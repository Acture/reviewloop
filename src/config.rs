use crate::{backend::cspaper, model::ReviewOptions};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
};

/// Wrapper that redacts the inner value in `Debug` output. Used for
/// passwords / OAuth client secrets so a future `tracing::warn!("{cfg:?}")`
/// or panic dump cannot accidentally leak credentials.
#[derive(Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Redacted<T>(pub T);

impl<T> std::fmt::Debug for Redacted<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<redacted>")
    }
}

impl<T> std::ops::Deref for Redacted<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> From<T> for Redacted<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

const GLOBAL_CONFIG_FILE: &str = "config.toml";
const LEGACY_GLOBAL_CONFIG_FILE: &str = "reviewloop.toml";
const PROJECT_CONFIG_FILE: &str = "reviewloop.toml";

#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub config: Config,
    pub global_path: Option<PathBuf>,
    pub project_path: Option<PathBuf>,
    pub legacy_global_path: Option<PathBuf>,
    pub compat_notice: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub project_id: String,
    pub core: CoreConfig,
    pub logging: LoggingConfig,
    pub polling: PollingConfig,
    pub retention: RetentionConfig,
    pub trigger: TriggerConfig,
    pub providers: ProvidersConfig,
    pub papers: Vec<PaperConfig>,
    pub paper_watch: BTreeMap<String, bool>,
    pub paper_tag_triggers: BTreeMap<String, String>,
    pub imap: Option<ImapConfig>,
    pub gmail_oauth: Option<GmailOauthConfig>,
    pub notifications: NotificationsConfig,
    pub project_root: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        let global = GlobalConfigFile::default();
        let project = ProjectConfigFile::default();
        Self::from_parts(global, project, None)
    }
}

impl Config {
    pub fn load_runtime(
        explicit_project_path: Option<&Path>,
        require_project: bool,
    ) -> Result<Self> {
        Ok(Self::load_runtime_with_metadata(explicit_project_path, require_project)?.config)
    }

    pub fn load_runtime_with_metadata(
        explicit_project_path: Option<&Path>,
        require_project: bool,
    ) -> Result<LoadedConfig> {
        let global_path = Self::ensure_global_config_file()?;
        let legacy_global_path = Self::legacy_global_config_path().filter(|path| path.exists());
        let discovered_project_path = discover_project_config_path(explicit_project_path)?;

        let global = if let Some(path) = global_path.as_deref() {
            GlobalConfigFile::load(path)?
        } else {
            GlobalConfigFile::default()
        };
        global.validate()?;

        let project = if let Some(path) = discovered_project_path.as_deref() {
            if legacy_global_path.is_some() {
                return Err(anyhow!(
                    "legacy global config {} still carries project-owned fields while project config {} exists. run `reviewloop config migrate-project --project-id <id>` and remove the legacy file",
                    legacy_global_path
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_default(),
                    path.display()
                ));
            }
            let project = ProjectConfigFile::load(path)?;
            project.validate(true)?;
            project
        } else if let Some(path) = legacy_global_path.as_deref() {
            let legacy = LegacyConfig::load(path)?;
            let project = legacy.project_config();
            project.validate(require_project)?;
            let compat_notice = Some(format!(
                "using legacy project settings from {}. migrate them into {PROJECT_CONFIG_FILE} with `reviewloop config migrate-project --project-id <id>`",
                path.display()
            ));
            let config = Self::from_parts(global, project, None).with_env_secrets();
            config.validate_runtime(require_project)?;
            return Ok(LoadedConfig {
                config,
                global_path,
                project_path: None,
                legacy_global_path,
                compat_notice,
            });
        } else {
            let project = ProjectConfigFile::default();
            project.validate(require_project)?;
            project
        };

        let project_root = discovered_project_path
            .as_deref()
            .and_then(Path::parent)
            .map(Path::to_path_buf);
        let config = Self::from_parts(global, project, project_root).with_env_secrets();
        config.validate_runtime(require_project)?;
        Ok(LoadedConfig {
            config,
            global_path,
            project_path: discovered_project_path,
            legacy_global_path,
            compat_notice: None,
        })
    }

    pub fn global_config_path() -> Option<PathBuf> {
        default_global_config_path().map(|dir| dir.join(GLOBAL_CONFIG_FILE))
    }

    pub fn legacy_global_config_path() -> Option<PathBuf> {
        default_global_config_path().map(|dir| dir.join(LEGACY_GLOBAL_CONFIG_FILE))
    }

    pub fn ensure_global_config_dir() -> Result<Option<PathBuf>> {
        let Some(path) = Self::global_config_path() else {
            return Ok(None);
        };
        let Some(parent) = path.parent() else {
            return Ok(None);
        };
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create global config dir: {}", parent.display()))?;
        Ok(Some(parent.to_path_buf()))
    }

    pub fn ensure_global_config_file() -> Result<Option<PathBuf>> {
        let Some(path) = Self::global_config_path() else {
            return Ok(None);
        };
        Self::ensure_global_config_dir()?;
        if path.exists() {
            return Ok(Some(path));
        }

        if let Some(legacy_path) = Self::legacy_global_config_path().filter(|p| p.exists()) {
            let legacy = LegacyConfig::load(&legacy_path)?;
            let global = legacy.global_config();
            global.save(&path)?;
            return Ok(Some(path));
        }

        GlobalConfigFile::default().save(&path)?;
        Ok(Some(path))
    }

    pub fn global_data_dir() -> Option<PathBuf> {
        default_global_data_dir()
    }

    pub fn ensure_global_data_dir() -> Result<Option<PathBuf>> {
        let Some(path) = Self::global_data_dir() else {
            return Ok(None);
        };
        fs::create_dir_all(&path)
            .with_context(|| format!("failed to create global data dir: {}", path.display()))?;
        Ok(Some(path))
    }

    pub fn load_project(path: &Path) -> Result<ProjectConfigFile> {
        ProjectConfigFile::load(path)
    }

    pub fn load_legacy_global(path: &Path) -> Result<LegacyConfig> {
        LegacyConfig::load(path)
    }

    pub fn state_dir(&self) -> PathBuf {
        PathBuf::from(&self.core.state_dir)
    }

    /// Returns the path where the widget state JSON file should be written,
    /// or `None` if `core.widget_state_enabled` is `false`.
    ///
    /// The directory defaults to `core.state_dir`; override with
    /// `core.widget_state_dir` (global config only; no per-project V1 support).
    pub fn widget_state_path(&self) -> Option<PathBuf> {
        if !self.core.widget_state_enabled {
            return None;
        }
        let dir = self
            .core
            .widget_state_dir
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| self.state_dir());
        Some(dir.join("widget-state.json"))
    }

    pub fn db_in_memory(&self) -> bool {
        self.core.db_path.trim().eq_ignore_ascii_case(":memory:")
    }

    pub fn db_path(&self) -> Option<PathBuf> {
        if self.db_in_memory() {
            None
        } else {
            Some(PathBuf::from(&self.core.db_path))
        }
    }

    pub fn find_paper(&self, paper_id: &str) -> Option<&PaperConfig> {
        self.papers.iter().find(|p| p.id == paper_id)
    }

    pub fn first_paper_for_backend(&self, backend: &str) -> Option<&PaperConfig> {
        self.papers.iter().find(|p| p.backend == backend)
    }

    pub fn is_paper_watched(&self, paper_id: &str) -> bool {
        self.paper_watch.get(paper_id).copied().unwrap_or(true)
    }

    pub fn set_paper_watch(&mut self, paper_id: &str, enabled: bool) {
        self.paper_watch.insert(paper_id.to_string(), enabled);
    }

    pub fn paper_tag_trigger(&self, paper_id: &str) -> Option<&str> {
        self.paper_tag_triggers.get(paper_id).map(String::as_str)
    }

    pub fn set_paper_tag_trigger(&mut self, paper_id: &str, trigger: Option<String>) {
        match trigger {
            Some(trigger) => {
                self.paper_tag_triggers
                    .insert(paper_id.to_string(), trigger);
            }
            None => {
                self.paper_tag_triggers.remove(paper_id);
            }
        }
    }

    /// Resolve the venue used when submitting / referencing this specific paper.
    ///
    /// The resolution chain is fully config-driven, with no hardcoded fallback:
    /// `paper.venue → project.providers.stanford.venue → global.providers.stanford.venue`.
    /// `Config::from_parts` materializes the second-and-third merge into
    /// `self.providers.stanford.venue`, so this only needs to combine the
    /// per-paper override with the merged project/global default.
    ///
    /// For cspaper the venue is the review template (`agent_id`): the per-paper
    /// override, then `providers.cspaper.agent_id` (project over global). Other
    /// backends consult only the per-paper override and return `None` if unset.
    pub fn venue_for(&self, paper: &PaperConfig) -> Option<String> {
        let per_paper = paper
            .venue
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string);
        if per_paper.is_some() {
            return per_paper;
        }
        match paper.backend.as_str() {
            "stanford" => self
                .providers
                .stanford
                .venue
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_string),
            cspaper::BACKEND => self
                .providers
                .cspaper
                .agent_id
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_string),
            _ => None,
        }
    }

    /// The setting `paper`'s provider still needs before a review of it can
    /// be submitted, with a message naming it, or `None` when nothing is
    /// missing. Every enqueue path checks this, so a job that could only fail
    /// is never queued.
    pub fn missing_provider_setting(&self, paper: &PaperConfig) -> Option<(&'static str, String)> {
        if paper.backend != cspaper::BACKEND {
            return None;
        }
        if self
            .providers
            .cspaper
            .api_key
            .as_ref()
            .is_none_or(|key| key.trim().is_empty())
        {
            return Some((
                "api_key",
                "no CSPaper API key configured for backend=cspaper".to_string(),
            ));
        }
        if self.venue_for(paper).is_none() {
            return Some((
                "agent_id",
                format!(
                    "no CSPaper review template (agent_id) configured for paper {}",
                    paper.id
                ),
            ));
        }
        None
    }

    /// Provider options beyond the venue that a new review of `paper` is
    /// requested with. Part of the request identity, so every enqueue path
    /// resolves them here. CSPaper always records the desk-rejection setting
    /// explicitly, so an unset value and the provider default compare equal.
    pub fn review_options_for(&self, paper: &PaperConfig) -> ReviewOptions {
        match paper.backend.as_str() {
            cspaper::BACKEND => ReviewOptions::default().with(
                cspaper::DESK_REJECTION_ENABLED,
                self.providers.cspaper.desk_rejection_enabled.to_string(),
            ),
            _ => ReviewOptions::default(),
        }
    }

    /// Fills machine-level secrets that the global config leaves unset from
    /// the environment. Only the runtime loader calls this, so configs built
    /// in code never pick up the caller's environment.
    fn with_env_secrets(mut self) -> Self {
        self.providers.cspaper.api_key = secret_or_env(
            self.providers.cspaper.api_key.take(),
            env::var(CSPAPER_API_KEY_ENV).ok(),
        );
        self
    }

    #[cfg(test)]
    pub(crate) fn merge_for_tests(global: GlobalConfigFile, project: ProjectConfigFile) -> Self {
        Self::from_parts(global, project, None)
    }

    /// The default backend for papers that omit `backend` in the project file
    /// AND when `project.default_backend` is also unset.
    pub const DEFAULT_BACKEND: &'static str = "stanford";

    /// Resolve a [`PaperConfigFile`] (TOML form) into a runtime [`PaperConfig`].
    /// Backend falls back to `default_backend`, then to [`Self::DEFAULT_BACKEND`].
    fn resolve_paper(file: PaperConfigFile, default_backend: &str) -> PaperConfig {
        let backend = file
            .backend
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| default_backend.to_string());
        PaperConfig {
            id: file.id,
            pdf_path: file.pdf_path,
            backend,
            venue: file.venue,
        }
    }

    fn from_parts(
        global: GlobalConfigFile,
        mut project: ProjectConfigFile,
        project_root: Option<PathBuf>,
    ) -> Self {
        if let Some(root) = project_root.as_deref() {
            for paper in &mut project.papers {
                paper.pdf_path = resolve_project_relative_path(root, &paper.pdf_path)
                    .to_string_lossy()
                    .to_string();
            }
            project.trigger.git.repo_dir =
                resolve_project_relative_path(root, &project.trigger.git.repo_dir)
                    .to_string_lossy()
                    .to_string();
        }

        let default_backend = project
            .default_backend
            .clone()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| Self::DEFAULT_BACKEND.to_string());
        let papers: Vec<PaperConfig> = project
            .papers
            .into_iter()
            .map(|file| Self::resolve_paper(file, &default_backend))
            .collect();

        // Merge: project Option<T> overrides global concrete value.
        let mut core = global.core;
        if let Some(hours) = project.core.review_timeout_hours {
            core.review_timeout_hours = hours;
        }
        // Project proxy list replaces global when non-empty.
        if let Some(proxies) = project.core.proxies {
            if !proxies.is_empty() {
                core.proxies = proxies;
            }
        }

        let trigger = TriggerConfig {
            git: GitTriggerConfig {
                enabled: project.trigger.git.enabled,
                tag_pattern: merge_optional_string(
                    project.trigger.git.tag_pattern,
                    global.trigger.git.tag_pattern,
                ),
                repo_dir: project.trigger.git.repo_dir,
                auto_create_tags_on_pdf_change: project.trigger.git.auto_create_tags_on_pdf_change,
                auto_delete_processed_tags: project.trigger.git.auto_delete_processed_tags,
            },
            pdf: PdfTriggerConfig {
                enabled: project.trigger.pdf.enabled,
                auto_submit_on_change: project
                    .trigger
                    .pdf
                    .auto_submit_on_change
                    .unwrap_or(global.trigger.pdf.auto_submit_on_change),
                max_scan_papers: project
                    .trigger
                    .pdf
                    .max_scan_papers
                    .unwrap_or(global.trigger.pdf.max_scan_papers),
            },
        };

        let provider_email = merge_optional_string(
            project.providers.stanford.email,
            global.providers.stanford.email,
        );
        let provider_fallback_script = merge_optional_string(
            project.providers.stanford.fallback_script,
            global.providers.stanford.fallback_script,
        );
        let provider_fallback_script = if let Some(root) = project_root.as_deref() {
            resolve_project_relative_path(root, &provider_fallback_script)
                .to_string_lossy()
                .to_string()
        } else {
            provider_fallback_script
        };

        Self {
            project_id: project.project_id,
            core,
            logging: global.logging,
            polling: global.polling,
            retention: global.retention,
            trigger,
            providers: ProvidersConfig {
                stanford: StanfordProviderConfig {
                    base_url: global.providers.stanford.base_url,
                    fallback_mode: global.providers.stanford.fallback_mode,
                    fallback_script: provider_fallback_script,
                    email: provider_email,
                    // Project venue overrides global venue. Per-paper overrides
                    // are applied later in `Config::venue_for`.
                    venue: project
                        .providers
                        .stanford
                        .venue
                        .or(global.providers.stanford.venue),
                },
                cspaper: CspaperProviderConfig {
                    base_url: global.providers.cspaper.base_url,
                    api_key: secret_or_env(global.providers.cspaper.api_key, None),
                    agent_id: non_blank(project.providers.cspaper.agent_id)
                        .or_else(|| non_blank(global.providers.cspaper.agent_id)),
                    desk_rejection_enabled: project
                        .providers
                        .cspaper
                        .desk_rejection_enabled
                        .unwrap_or(global.providers.cspaper.desk_rejection_enabled),
                },
            },
            papers,
            paper_watch: project.paper_watch,
            paper_tag_triggers: project.paper_tag_triggers,
            imap: global.imap,
            gmail_oauth: global.gmail_oauth,
            notifications: NotificationsConfig {
                enabled: project
                    .notifications
                    .enabled
                    .unwrap_or(global.notifications.enabled),
                summary_only: project
                    .notifications
                    .summary_only
                    .unwrap_or(global.notifications.summary_only),
            },
            project_root,
        }
    }
}

/// Returns `project` when it carries a non-empty value, otherwise `global`.
/// Used for fields that live in the global config but accept per-project
/// overrides.
fn merge_optional_string(project: Option<String>, global: String) -> String {
    project
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or(global)
}

fn non_blank(value: Option<String>) -> Option<String> {
    value
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Environment variable holding the CSPaper organisation API key when
/// `providers.cspaper.api_key` is unset in the global config.
pub const CSPAPER_API_KEY_ENV: &str = "REVIEWLOOP_CSPAPER_API_KEY";

/// A configured secret wins; the environment fills it only when the config
/// leaves it blank (the same precedence as the Gmail client credentials).
fn secret_or_env(
    configured: Option<Redacted<String>>,
    env: Option<String>,
) -> Option<Redacted<String>> {
    let non_blank = |value: String| {
        let value = value.trim().to_string();
        (!value.is_empty()).then_some(Redacted(value))
    };
    configured
        .and_then(|secret| non_blank(secret.0))
        .or_else(|| env.and_then(non_blank))
}

/// Provider base URLs receive credentials or manuscripts, so they must be
/// `https://`; plain `http://` is accepted only for a loopback host (local
/// tests and mocks).
fn validate_provider_base_url(field: &str, raw: &str) -> Result<()> {
    let invalid = || {
        anyhow!(
            "{field} must be https:// (or http://localhost / http://127.0.0.1 for local testing); got {raw}"
        )
    };
    let url = reqwest::Url::parse(raw).map_err(|_| invalid())?;
    let loopback = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    match url.scheme() {
        "https" if url.host_str().is_some() => Ok(()),
        "http" if loopback => Ok(()),
        _ => Err(invalid()),
    }
}

impl Config {
    fn validate_runtime(&self, require_project: bool) -> Result<()> {
        if self.core.db_path.trim().is_empty() {
            return Err(anyhow!("core.db_path must not be empty"));
        }
        if self.core.max_concurrency == 0 {
            return Err(anyhow!("core.max_concurrency must be >= 1"));
        }
        if self.core.max_submissions_per_tick == 0 {
            return Err(anyhow!("core.max_submissions_per_tick must be >= 1"));
        }
        if self.polling.schedule_minutes.is_empty() {
            return Err(anyhow!("polling.schedule_minutes cannot be empty"));
        }
        if self.retention.prune_every_ticks == 0 {
            return Err(anyhow!("retention.prune_every_ticks must be >= 1"));
        }
        if self.trigger.pdf.max_scan_papers == 0 {
            return Err(anyhow!("trigger.pdf.max_scan_papers must be >= 1"));
        }
        if let Some(imap) = &self.imap
            && imap.max_messages_per_poll == 0
        {
            return Err(anyhow!("imap.max_messages_per_poll must be >= 1"));
        }
        if let Some(gmail) = &self.gmail_oauth
            && gmail.max_messages_per_poll == 0
        {
            return Err(anyhow!("gmail_oauth.max_messages_per_poll must be >= 1"));
        }
        if require_project && self.project_id.trim().is_empty() {
            return Err(anyhow!(
                "project config is required here. create {} with project_id or run `reviewloop init project --project-id <id>`",
                PROJECT_CONFIG_FILE
            ));
        }
        self.validate_base_url()?;
        self.validate_fallback_script()?;
        Ok(())
    }

    /// O9: provider base URLs must be `https://`, with `http://localhost` and
    /// `http://127.0.0.1` whitelisted for local tests.
    fn validate_base_url(&self) -> Result<()> {
        validate_provider_base_url(
            "providers.stanford.base_url",
            &self.providers.stanford.base_url,
        )?;
        validate_provider_base_url(
            "providers.cspaper.base_url",
            &self.providers.cspaper.base_url,
        )
    }

    /// O8: Validate that `providers.stanford.fallback_script` does not escape
    /// the project root via `..` traversal when it is a relative path.
    fn validate_fallback_script(&self) -> Result<()> {
        let script_str = &self.providers.stanford.fallback_script;
        let path = Path::new(script_str);
        if path.is_absolute() {
            // Absolute paths are an explicit user choice; trust them.
            return Ok(());
        }
        let Some(root) = self.project_root.as_deref() else {
            // Relative path with no project root — the script can never be
            // cleanly resolved, but only surface an error if the path looks
            // like it might escape (contains `..`).
            if script_str.contains("..") {
                return Err(anyhow!(
                    "providers.stanford.fallback_script is relative ({}) but no project \
                     root is set; either pin an absolute path in global config or run \
                     from a directory with a reviewloop.toml",
                    script_str
                ));
            }
            return Ok(());
        };
        // If the script doesn't exist yet (fresh checkout), skip traversal
        // check — the fallback won't be invoked anyway.
        if !path.exists() {
            return Ok(());
        }
        let canonical_root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
        let canonical_script = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        if !canonical_script.starts_with(&canonical_root) {
            return Err(anyhow!(
                "providers.stanford.fallback_script ({}) resolves outside project \
                 root ({}); refusing to execute. set an absolute path in global \
                 config if this is intentional.",
                canonical_script.display(),
                canonical_root.display()
            ));
        }
        Ok(())
    }

    /// Stricter validation for configs loaded from outside the current working
    /// directory (for example, via the project registry). Rejects high-risk
    /// values while warning on lower-confidence suspicious settings.
    pub fn validate_for_foreign_load(&self) -> Result<()> {
        let script = self.providers.stanford.fallback_script.trim();
        if !script.is_empty() {
            let path = Path::new(script);
            if path.is_absolute() {
                let home = home_dir_for_security()?;
                if !path_is_within_dir(path, &home) {
                    anyhow::bail!(
                        "registered config has fallback_script outside HOME: {}; refusing to load (security)",
                        script
                    );
                }
            }
        }

        if let Some(dir) = self.core.widget_state_dir.as_deref().map(str::trim)
            && !dir.is_empty()
        {
            let path = Path::new(dir);
            if path.is_absolute() {
                match home_dir_for_security() {
                    Ok(home) if !path_is_within_dir(path, &home) => {
                        tracing::warn!(
                            path = %dir,
                            "registered config has widget_state_dir outside HOME; allowing but logging"
                        );
                    }
                    Err(err) => {
                        tracing::warn!(
                            path = %dir,
                            error = %err,
                            "registered config has absolute widget_state_dir but HOME could not be verified; allowing but logging"
                        );
                    }
                    _ => {}
                }
            }
        }

        for (index, url) in self.core.proxies.iter().enumerate() {
            if proxy_url_has_embedded_credentials(url) {
                tracing::warn!(
                    proxy_index = index,
                    "registered config has proxy URL with embedded credentials; \
                     this could leak via debug logs. consider env var or keychain instead"
                );
            }
        }

        Ok(())
    }
}

fn home_dir_for_security() -> Result<PathBuf> {
    let home = env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("HOME unavailable"))?;
    if !home.is_absolute() {
        anyhow::bail!("HOME must be an absolute path");
    }
    Ok(home)
}

fn path_is_within_dir(path: &Path, dir: &Path) -> bool {
    if let (Ok(canonical_path), Ok(canonical_dir)) = (path.canonicalize(), dir.canonicalize()) {
        return canonical_path.starts_with(canonical_dir);
    }

    normalize_for_security(path).starts_with(normalize_for_security(dir))
}

fn normalize_for_security(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::Normal(part) => normalized.push(part),
            std::path::Component::Prefix(_) | std::path::Component::RootDir => {
                normalized.push(component.as_os_str());
            }
        }
    }
    normalized
}

fn proxy_url_has_embedded_credentials(url: &str) -> bool {
    let Some((_, rest)) = url.split_once("://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    authority.contains('@')
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GlobalConfigFile {
    pub core: CoreConfig,
    pub logging: LoggingConfig,
    pub polling: PollingConfig,
    pub retention: RetentionConfig,
    pub trigger: GlobalTriggerConfig,
    pub providers: GlobalProvidersConfig,
    pub imap: Option<ImapConfig>,
    pub gmail_oauth: Option<GmailOauthConfig>,
    pub notifications: GlobalNotificationsConfig,
}

impl Default for GlobalConfigFile {
    fn default() -> Self {
        Self {
            core: CoreConfig::default(),
            logging: LoggingConfig::default(),
            polling: PollingConfig::default(),
            retention: RetentionConfig::default(),
            trigger: GlobalTriggerConfig::default(),
            providers: GlobalProvidersConfig::default(),
            imap: Some(ImapConfig::default()),
            gmail_oauth: Some(GmailOauthConfig::default()),
            notifications: GlobalNotificationsConfig::default(),
        }
    }
}

impl GlobalConfigFile {
    pub fn load(path: &Path) -> Result<Self> {
        load_toml_file(path)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        save_toml_file(path, self)
    }

    pub fn validate(&self) -> Result<()> {
        if !matches!(self.logging.output.as_str(), "stdout" | "stderr" | "file") {
            return Err(anyhow!(
                "logging.output must be one of: stdout | stderr | file"
            ));
        }
        if self.logging.output == "file"
            && self
                .logging
                .file_path
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
        {
            return Err(anyhow!(
                "logging.file_path is required when logging.output = \"file\""
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectConfigFile {
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_backend: Option<String>,
    pub core: ProjectCoreOverrides,
    pub notifications: ProjectNotificationsConfig,
    pub trigger: ProjectTriggerConfig,
    pub providers: ProjectProvidersConfig,
    pub papers: Vec<PaperConfigFile>,
    pub paper_watch: BTreeMap<String, bool>,
    pub paper_tag_triggers: BTreeMap<String, String>,
}

/// Project-side override slots for fields whose defaults live in the global
/// config. Every field is `Option<T>`; `None` means "inherit global".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectCoreOverrides {
    /// Override for `global.core.review_timeout_hours`. The runtime
    /// [`CoreConfig::review_timeout_hours`] resolves to this value when set,
    /// otherwise to the global value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_timeout_hours: Option<u64>,
    /// Per-project proxy list. When non-empty, replaces (not merges with)
    /// the global `core.proxies` list. An empty project list means "inherit
    /// global"; use `[""]` tricks are not needed — just omit the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxies: Option<Vec<String>>,
}

impl ProjectConfigFile {
    /// A project file is meant to be committed, so an API key in it is
    /// refused before parsing, naming its line but never quoting it.
    pub fn load(path: &Path) -> Result<Self> {
        let raw = read_config_file(path)?;
        refuse_project_api_key(path, &raw)?;
        parse_toml(path, &raw)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        save_toml_file(path, self)
    }

    pub fn validate(&self, require_project: bool) -> Result<()> {
        if require_project && self.project_id.trim().is_empty() {
            return Err(anyhow!("project_id must not be empty"));
        }
        if self.trigger.pdf.max_scan_papers == Some(0) {
            return Err(anyhow!("trigger.pdf.max_scan_papers must be >= 1"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LegacyConfig {
    pub core: CoreConfig,
    pub logging: LoggingConfig,
    pub polling: PollingConfig,
    pub retention: RetentionConfig,
    pub trigger: TriggerConfig,
    pub providers: ProvidersConfig,
    pub papers: Vec<PaperConfigFile>,
    pub paper_watch: BTreeMap<String, bool>,
    pub paper_tag_triggers: BTreeMap<String, String>,
    pub imap: Option<ImapConfig>,
    pub gmail_oauth: Option<GmailOauthConfig>,
}

impl Default for LegacyConfig {
    fn default() -> Self {
        Self {
            core: CoreConfig::default(),
            logging: LoggingConfig::default(),
            polling: PollingConfig::default(),
            retention: RetentionConfig::default(),
            trigger: TriggerConfig::default(),
            providers: ProvidersConfig::default(),
            papers: Vec::new(),
            paper_watch: BTreeMap::new(),
            paper_tag_triggers: BTreeMap::new(),
            imap: Some(ImapConfig::default()),
            gmail_oauth: Some(GmailOauthConfig::default()),
        }
    }
}

impl LegacyConfig {
    pub fn load(path: &Path) -> Result<Self> {
        load_toml_file(path)
    }

    pub fn global_config(&self) -> GlobalConfigFile {
        GlobalConfigFile {
            core: self.core.clone(),
            logging: self.logging.clone(),
            polling: self.polling.clone(),
            retention: self.retention.clone(),
            // Legacy values went through a single trigger struct that conflated
            // global defaults and project overrides. Migration parks the legacy
            // trigger values fully on the project side (see project_config()),
            // so the migrated global trigger gets stock defaults.
            trigger: GlobalTriggerConfig::default(),
            providers: GlobalProvidersConfig {
                stanford: GlobalStanfordProviderConfig {
                    base_url: self.providers.stanford.base_url.clone(),
                    fallback_mode: self.providers.stanford.fallback_mode.clone(),
                    fallback_script: self.providers.stanford.fallback_script.clone(),
                    email: self.providers.stanford.email.clone(),
                    // Legacy global venue stayed empty; the per-project venue
                    // (now migrated to project_config below) carries the value.
                    venue: None,
                },
                cspaper: GlobalCspaperProviderConfig::default(),
            },
            imap: self.imap.clone(),
            gmail_oauth: self.gmail_oauth.clone(),
            notifications: GlobalNotificationsConfig::default(),
        }
    }

    pub fn project_config(&self) -> ProjectConfigFile {
        // Migration: legacy single-file configs put the trigger fields inline.
        // We materialize the whole legacy trigger as project-side overrides so
        // the migrated project matches the legacy runtime behavior exactly,
        // even when the legacy values diverged from current global defaults.
        let legacy = self.trigger.clone();
        ProjectConfigFile {
            project_id: String::new(),
            default_backend: None,
            core: ProjectCoreOverrides::default(),
            notifications: ProjectNotificationsConfig::default(),
            trigger: ProjectTriggerConfig {
                git: ProjectGitTriggerConfig {
                    enabled: legacy.git.enabled,
                    tag_pattern: Some(legacy.git.tag_pattern),
                    repo_dir: legacy.git.repo_dir,
                    auto_create_tags_on_pdf_change: legacy.git.auto_create_tags_on_pdf_change,
                    auto_delete_processed_tags: legacy.git.auto_delete_processed_tags,
                },
                pdf: ProjectPdfTriggerConfig {
                    enabled: legacy.pdf.enabled,
                    auto_submit_on_change: Some(legacy.pdf.auto_submit_on_change),
                    max_scan_papers: Some(legacy.pdf.max_scan_papers),
                },
            },
            providers: ProjectProvidersConfig {
                stanford: ProjectStanfordProviderConfig {
                    email: None,
                    fallback_script: None,
                    venue: self.providers.stanford.venue.clone(),
                },
                cspaper: ProjectCspaperProviderConfig::default(),
            },
            papers: self.papers.clone(),
            paper_watch: self.paper_watch.clone(),
            paper_tag_triggers: self.paper_tag_triggers.clone(),
        }
    }
}

fn load_toml_file<T>(path: &Path) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    parse_toml(path, &read_config_file(path)?)
}

fn read_config_file(path: &Path) -> Result<String> {
    fs::read_to_string(path).with_context(|| format!("failed to read config: {}", path.display()))
}

/// The error names the line and column but never quotes the file: the toml
/// crate's own report prints the offending source line, which may hold a
/// secret (an API key, an IMAP password).
fn parse_toml<T>(path: &Path, raw: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    toml::from_str(raw).map_err(|err| {
        let location = err
            .span()
            .and_then(|span| line_and_column(raw, span.start))
            .map(|(line, column)| format!(" at line {line}, column {column}"))
            .unwrap_or_default();
        anyhow!(
            "failed to parse TOML config: {}{location}: {}",
            path.display(),
            scrub_values(err.message())
        )
    })
}

/// serde's type and value errors quote the offending value (`invalid type:
/// string "csp_live_...", expected a boolean`); keep the kind, drop the value.
fn scrub_values(message: &str) -> String {
    let value = regex::Regex::new(
        r#"(invalid (?:type|value): [a-z ]*?|unknown variant )("(?:[^"\\]|\\.)*"|`[^`]*`)"#,
    )
    .expect("valid value regex");
    value.replace_all(message, "$1<value>").into_owned()
}

/// 1-based line and column of byte `offset` in `raw`.
fn line_and_column(raw: &str, offset: usize) -> Option<(usize, usize)> {
    let before = raw.get(..offset)?;
    let line_start = before.rfind('\n').map_or(0, |newline| newline + 1);
    Some((
        before.matches('\n').count() + 1,
        before[line_start..].chars().count() + 1,
    ))
}

/// Refuses an `api_key` anywhere in a project file, and anything shaped like a
/// CSPaper key even in a comment or under another name. Scans the raw text, so
/// a line the TOML parser would reject (an unquoted or unterminated key) is
/// refused here too instead of being quoted by the parse error.
fn refuse_project_api_key(path: &Path, raw: &str) -> Result<()> {
    let key_line = regex::Regex::new(r#"(?m)^[^#\n]*\bapi_key["']?\s*=|csp_live_"#)
        .expect("valid api_key regex");
    let Some(found) = key_line.find(raw) else {
        return Ok(());
    };
    let line = raw[..found.start()].matches('\n').count() + 1;
    let global = Config::global_config_path().map_or_else(
        || format!("~/.config/reviewloop/{GLOBAL_CONFIG_FILE}"),
        |path| path.display().to_string(),
    );
    Err(anyhow!(
        "project config {} holds an API key (line {line}); remove it: providers.cspaper.api_key \
         belongs only in the global config {global} or the {CSPAPER_API_KEY_ENV} environment \
         variable, never in a file that may be committed",
        path.display()
    ))
}

fn save_toml_file<T>(path: &Path, value: &T) -> Result<()>
where
    T: Serialize,
{
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create config parent directory: {}",
                parent.display()
            )
        })?;
    }
    let content = toml::to_string_pretty(value)?;

    // Atomic write: write to a sibling temp file, fsync it, then rename over the
    // target. Rename on POSIX (and reasonably-modern Windows NTFS) is atomic
    // within a single filesystem, so a crash either leaves the original file
    // intact or replaces it cleanly with the new contents.
    let tmp_name = format!(
        ".{}.tmp.{}",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("config"),
        std::process::id()
    );
    let tmp_path = path.with_file_name(tmp_name);
    {
        #[cfg(unix)]
        let mut f = {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp_path)
                .with_context(|| format!("failed to create temp config: {}", tmp_path.display()))?
        };
        #[cfg(not(unix))]
        let mut f = std::fs::File::create(&tmp_path)
            .with_context(|| format!("failed to create temp config: {}", tmp_path.display()))?;
        use std::io::Write;
        f.write_all(content.as_bytes())
            .with_context(|| format!("failed to write temp config: {}", tmp_path.display()))?;
        f.sync_all()
            .with_context(|| format!("failed to fsync temp config: {}", tmp_path.display()))?;
    }
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "failed to atomically rename {} -> {}",
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn discover_project_config_path(explicit_path: Option<&Path>) -> Result<Option<PathBuf>> {
    if let Some(path) = explicit_path {
        if let Err(err) = fs::metadata(path) {
            if err.kind() == std::io::ErrorKind::NotFound {
                return Err(err)
                    .with_context(|| format!("project config file not found: {}", path.display()));
            }
            return Err(err).with_context(|| {
                format!("failed to access project config file: {}", path.display())
            });
        }
        return Ok(Some(path.to_path_buf()));
    }

    let cwd = env::current_dir().context("failed to resolve current working directory")?;
    let git_root = find_git_root(&cwd);
    let mut current = cwd.as_path();

    loop {
        let candidate = current.join(PROJECT_CONFIG_FILE);
        if candidate.exists() {
            return Ok(Some(candidate));
        }
        if git_root.as_deref() == Some(current) {
            break;
        }
        let Some(parent) = current.parent() else {
            break;
        };
        current = parent;
    }

    Ok(None)
}

pub fn default_project_config_path() -> Result<PathBuf> {
    let cwd = env::current_dir().context("failed to resolve current working directory")?;
    Ok(find_git_root(&cwd).unwrap_or(cwd).join(PROJECT_CONFIG_FILE))
}

pub fn find_git_root(start: &Path) -> Option<PathBuf> {
    let mut current = start.to_path_buf();
    loop {
        if current.join(".git").exists() {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

fn resolve_project_relative_path(project_root: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        project_root.join(path)
    }
}

fn default_global_config_path() -> Option<PathBuf> {
    if let Some(xdg) = env::var_os("XDG_CONFIG_HOME") {
        return Some(PathBuf::from(xdg).join("reviewloop"));
    }

    #[cfg(windows)]
    {
        if let Some(appdata) = env::var_os("APPDATA") {
            return Some(PathBuf::from(appdata).join("reviewloop"));
        }
    }

    env::var_os("HOME").map(|home| PathBuf::from(home).join(".config").join("reviewloop"))
}

fn default_global_data_dir() -> Option<PathBuf> {
    if let Some(custom) = env::var_os("REVIEWLOOP_STATE_DIR") {
        return Some(PathBuf::from(custom));
    }

    #[cfg(windows)]
    {
        if let Some(local_app_data) = env::var_os("LOCALAPPDATA") {
            return Some(PathBuf::from(local_app_data).join("review_loop"));
        }
    }

    env::var_os("HOME").map(|home| PathBuf::from(home).join(".review_loop"))
}

fn default_db_path() -> String {
    let base = default_global_data_dir().unwrap_or_else(|| PathBuf::from(".reviewloop"));
    base.join("reviewloop.db").to_string_lossy().to_string()
}

fn default_state_dir() -> String {
    default_global_data_dir()
        .unwrap_or_else(|| PathBuf::from(".reviewloop"))
        .to_string_lossy()
        .to_string()
}

fn default_log_path() -> String {
    PathBuf::from(default_state_dir())
        .join("reviewloop.log")
        .to_string_lossy()
        .to_string()
}

fn default_widget_state_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CoreConfig {
    pub state_dir: String,
    pub db_path: String,
    pub max_concurrency: usize,
    pub max_submissions_per_tick: usize,
    pub review_timeout_hours: u64,
    /// HTTP / SOCKS proxy URLs that all outbound requests rotate through.
    /// Empty list = direct connection (no proxy). Each entry is a full URL
    /// like `"http://user:pass@proxy.example.com:8080"` or `"socks5://..."`.
    /// Credentials in the URL are not logged — only the count is reported.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proxies: Vec<String>,
    /// Whether to write a widget state JSON file on every tick.
    /// Defaults to `true`. Set to `false` to disable (e.g. on non-macOS hosts).
    #[serde(default = "default_widget_state_enabled")]
    pub widget_state_enabled: bool,
    /// Directory in which to write `widget-state.json`.
    /// `None` (the default) resolves to `state_dir`.
    /// Per-project override is not supported in V1; this is a global-only field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub widget_state_dir: Option<String>,
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            state_dir: default_state_dir(),
            db_path: default_db_path(),
            max_concurrency: 2,
            max_submissions_per_tick: 1,
            review_timeout_hours: 48,
            proxies: Vec::new(),
            widget_state_enabled: default_widget_state_enabled(),
            widget_state_dir: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    pub level: String,
    pub output: String,
    pub file_path: Option<String>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            output: "stdout".to_string(),
            file_path: Some(default_log_path()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PollingConfig {
    pub schedule_minutes: Vec<u64>,
    pub jitter_percent: u8,
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            schedule_minutes: vec![1, 2, 5, 10, 20, 40],
            jitter_percent: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetentionConfig {
    pub enabled: bool,
    pub prune_every_ticks: u64,
    pub email_tokens_days: u64,
    pub seen_tags_days: u64,
    pub events_days: u64,
    pub terminal_jobs_days: u64,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            prune_every_ticks: 20,
            email_tokens_days: 30,
            seen_tags_days: 90,
            events_days: 30,
            terminal_jobs_days: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct TriggerConfig {
    pub git: GitTriggerConfig,
    pub pdf: PdfTriggerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GitTriggerConfig {
    pub enabled: bool,
    pub tag_pattern: String,
    pub repo_dir: String,
    pub auto_create_tags_on_pdf_change: bool,
    pub auto_delete_processed_tags: bool,
}

impl Default for GitTriggerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tag_pattern: "review-<backend>/<paper-id>/*".to_string(),
            repo_dir: ".".to_string(),
            auto_create_tags_on_pdf_change: false,
            auto_delete_processed_tags: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PdfTriggerConfig {
    pub enabled: bool,
    pub auto_submit_on_change: bool,
    pub max_scan_papers: usize,
}

impl Default for PdfTriggerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            auto_submit_on_change: false,
            max_scan_papers: 10,
        }
    }
}

// ===== On-disk: GLOBAL trigger defaults =====
//
// Only contains fields that have a sensible machine-wide default and may be
// overridden per project. Other trigger fields (like git.repo_dir or the
// auto-tag toggles) live exclusively on the project side because they are
// inherently per-repo decisions.

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GlobalTriggerConfig {
    pub git: GlobalGitTriggerConfig,
    pub pdf: GlobalPdfTriggerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GlobalGitTriggerConfig {
    pub tag_pattern: String,
}

impl Default for GlobalGitTriggerConfig {
    fn default() -> Self {
        Self {
            tag_pattern: GitTriggerConfig::default().tag_pattern,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GlobalPdfTriggerConfig {
    pub auto_submit_on_change: bool,
    pub max_scan_papers: usize,
}

impl Default for GlobalPdfTriggerConfig {
    fn default() -> Self {
        let pdf = PdfTriggerConfig::default();
        Self {
            auto_submit_on_change: pdf.auto_submit_on_change,
            max_scan_papers: pdf.max_scan_papers,
        }
    }
}

// ===== On-disk: PROJECT trigger overrides =====
//
// Concrete fields stay (project-only knobs); the three overridable defaults
// from GlobalTriggerConfig become Option<T>: `None` means "inherit global".

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectTriggerConfig {
    pub git: ProjectGitTriggerConfig,
    pub pdf: ProjectPdfTriggerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectGitTriggerConfig {
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag_pattern: Option<String>,
    pub repo_dir: String,
    pub auto_create_tags_on_pdf_change: bool,
    pub auto_delete_processed_tags: bool,
}

impl Default for ProjectGitTriggerConfig {
    fn default() -> Self {
        let git = GitTriggerConfig::default();
        Self {
            enabled: git.enabled,
            tag_pattern: None,
            repo_dir: git.repo_dir,
            auto_create_tags_on_pdf_change: git.auto_create_tags_on_pdf_change,
            auto_delete_processed_tags: git.auto_delete_processed_tags,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectPdfTriggerConfig {
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_submit_on_change: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_scan_papers: Option<usize>,
}

impl Default for ProjectPdfTriggerConfig {
    fn default() -> Self {
        let pdf = PdfTriggerConfig::default();
        Self {
            enabled: pdf.enabled,
            auto_submit_on_change: None,
            max_scan_papers: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ProvidersConfig {
    pub stanford: StanfordProviderConfig,
    pub cspaper: CspaperProviderConfig,
}

/// Runtime CSPaper settings, merged from the global and project files.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CspaperProviderConfig {
    pub base_url: String,
    /// Organisation API key from the global config or
    /// [`CSPAPER_API_KEY_ENV`]. Never serialized: no file written from a
    /// runtime config may carry it.
    #[serde(skip)]
    pub api_key: Option<Redacted<String>>,
    /// Default review template (CSPaper `agent_id`), project value over
    /// global. A paper's `venue` overrides it; see [`Config::venue_for`].
    pub agent_id: Option<String>,
    pub desk_rejection_enabled: bool,
}

impl Default for CspaperProviderConfig {
    fn default() -> Self {
        let global = GlobalCspaperProviderConfig::default();
        Self {
            base_url: global.base_url,
            api_key: None,
            agent_id: None,
            desk_rejection_enabled: global.desk_rejection_enabled,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StanfordProviderConfig {
    pub base_url: String,
    pub fallback_mode: String,
    pub fallback_script: String,
    pub email: String,
    pub venue: Option<String>,
}

impl Default for StanfordProviderConfig {
    fn default() -> Self {
        Self {
            base_url: "https://paperreview.ai".to_string(),
            fallback_mode: "node_playwright".to_string(),
            fallback_script: "tools/paperreview_fallback.mjs".to_string(),
            email: String::new(),
            venue: Some("ICLR".to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct GlobalProvidersConfig {
    pub stanford: GlobalStanfordProviderConfig,
    pub cspaper: GlobalCspaperProviderConfig,
}

/// Machine-level CSPaper settings. The API key and base URL live only here:
/// the project file has no field for them, so `deny_unknown_fields` rejects
/// a key (or a base URL that would redirect it) placed in `reviewloop.toml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GlobalCspaperProviderConfig {
    pub base_url: String,
    /// Organisation API key (`csp_live_...`). When unset,
    /// [`CSPAPER_API_KEY_ENV`] is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<Redacted<String>>,
    /// Default review template (`agent_id`, e.g. `ICLR_main_2026_1`). No
    /// built-in default: the template decides what the review means.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// CSPaper's desk-rejection screening (topic fit, minimum quality,
    /// prompt injection) before the review. The provider default is `true`;
    /// `false` always yields a full, scored review.
    pub desk_rejection_enabled: bool,
}

impl Default for GlobalCspaperProviderConfig {
    fn default() -> Self {
        Self {
            base_url: "https://cspaper.org".to_string(),
            api_key: None,
            agent_id: None,
            desk_rejection_enabled: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GlobalStanfordProviderConfig {
    pub base_url: String,
    pub fallback_mode: String,
    pub fallback_script: String,
    pub email: String,
    /// The default venue used when neither the paper nor the project specifies
    /// one. Lives in the global config so users can change "ICLR" once and
    /// have every project pick it up. The runtime `Config` flattens the chain
    /// `paper.venue → project.providers.stanford.venue → this` into
    /// `Config.providers.stanford.venue`.
    pub venue: Option<String>,
}

impl Default for GlobalStanfordProviderConfig {
    fn default() -> Self {
        let base = StanfordProviderConfig::default();
        Self {
            base_url: base.base_url,
            fallback_mode: base.fallback_mode,
            fallback_script: base.fallback_script,
            email: base.email,
            venue: Some("ICLR".to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectProvidersConfig {
    pub stanford: ProjectStanfordProviderConfig,
    #[serde(skip_serializing_if = "ProjectCspaperProviderConfig::is_empty")]
    pub cspaper: ProjectCspaperProviderConfig,
}

/// Per-project CSPaper review choices. Deliberately no `api_key` or
/// `base_url`: see [`GlobalCspaperProviderConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectCspaperProviderConfig {
    /// Overrides `global.providers.cspaper.agent_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// Overrides `global.providers.cspaper.desk_rejection_enabled`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desk_rejection_enabled: Option<bool>,
}

impl ProjectCspaperProviderConfig {
    fn is_empty(&self) -> bool {
        self.agent_id.is_none() && self.desk_rejection_enabled.is_none()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectStanfordProviderConfig {
    /// Per-project submitter email. When set, overrides
    /// `global.providers.stanford.email`. When `None` (or empty), the global
    /// value is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    /// Per-project Playwright fallback script path. Overrides
    /// `global.providers.stanford.fallback_script` when set; the project
    /// path is resolved relative to the project root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_script: Option<String>,
    /// Per-project default venue. When set, overrides
    /// `global.providers.stanford.venue`. When `None`, the global value is
    /// used (which itself defaults to "ICLR" but is user-overridable in
    /// `~/.config/reviewloop/config.toml`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub venue: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperConfig {
    pub id: String,
    pub pdf_path: String,
    pub backend: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub venue: Option<String>,
}

/// On-disk representation of a paper inside `ProjectConfigFile.papers`.
///
/// `backend` is optional because it can fall back to `project.default_backend`,
/// which itself ultimately falls back to `"stanford"`. The runtime
/// [`PaperConfig`] always has a concrete `backend`; resolution happens in
/// [`Config::from_parts`].
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PaperConfigFile {
    pub id: String,
    pub pdf_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub venue: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ImapConfig {
    pub enabled: bool,
    pub server: String,
    pub port: u16,
    pub username: String,
    pub password: Redacted<String>,
    pub folder: String,
    pub poll_seconds: u64,
    pub mark_seen: bool,
    pub max_lookback_hours: u64,
    pub max_messages_per_poll: usize,
    pub header_first: bool,
    pub backend_header_patterns: BTreeMap<String, String>,
    pub backend_patterns: BTreeMap<String, String>,
}

impl Default for ImapConfig {
    fn default() -> Self {
        let mut backend_header_patterns = BTreeMap::new();
        backend_header_patterns.insert(
            "stanford".to_string(),
            r"(?is)(from:\s*.*mail\.paperreview\.ai|subject:\s*.*paper review is ready)"
                .to_string(),
        );

        let mut backend_patterns = BTreeMap::new();
        backend_patterns.insert(
            "stanford".to_string(),
            r"https?://paperreview\.ai/review\?token=([A-Za-z0-9_-]+)".to_string(),
        );

        Self {
            enabled: false,
            server: "imap.gmail.com".to_string(),
            port: 993,
            username: String::new(),
            password: Redacted::default(),
            folder: "INBOX".to_string(),
            poll_seconds: 300,
            mark_seen: true,
            max_lookback_hours: 72,
            max_messages_per_poll: 50,
            header_first: true,
            backend_header_patterns,
            backend_patterns,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GmailOauthConfig {
    pub enabled: bool,
    pub client_id: String,
    pub client_secret: Redacted<String>,
    pub token_store_path: Option<String>,
    pub poll_seconds: u64,
    pub mark_seen: bool,
    pub max_lookback_hours: u64,
    pub max_messages_per_poll: usize,
    pub header_first: bool,
    pub backend_header_patterns: BTreeMap<String, String>,
    pub backend_patterns: BTreeMap<String, String>,
}

impl Default for GmailOauthConfig {
    fn default() -> Self {
        let mut backend_header_patterns = BTreeMap::new();
        backend_header_patterns.insert(
            "stanford".to_string(),
            r"(?is)(from:\s*.*mail\.paperreview\.ai|subject:\s*.*paper review is ready)"
                .to_string(),
        );

        let mut backend_patterns = BTreeMap::new();
        backend_patterns.insert(
            "stanford".to_string(),
            r"https?://paperreview\.ai/review\?token=([A-Za-z0-9_-]+)".to_string(),
        );

        Self {
            enabled: false,
            client_id: String::new(),
            client_secret: Redacted::default(),
            token_store_path: None,
            poll_seconds: 300,
            mark_seen: true,
            max_lookback_hours: 72,
            max_messages_per_poll: 50,
            header_first: true,
            backend_header_patterns,
            backend_patterns,
        }
    }
}

/// Runtime notifications config (merged from global + project override).
#[derive(Debug, Clone)]
pub struct NotificationsConfig {
    pub enabled: bool,
    pub summary_only: bool,
}

/// Global on-disk notifications defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GlobalNotificationsConfig {
    pub enabled: bool,
    pub summary_only: bool,
}

impl Default for GlobalNotificationsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            summary_only: false,
        }
    }
}

/// Per-project notification overrides. `None` means "inherit global default".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectNotificationsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_only: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::{
        CSPAPER_API_KEY_ENV, Config, GlobalConfigFile, LegacyConfig, PaperConfig, PaperConfigFile,
        ProjectConfigFile, Redacted, default_project_config_path, find_git_root,
        home_dir_for_security, secret_or_env,
    };
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn save_toml_file_roundtrips_and_leaves_no_tmp_files() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("reviewloop.toml");
        let original = ProjectConfigFile {
            project_id: "my-project".to_string(),
            papers: vec![],
            ..ProjectConfigFile::default()
        };
        original.save(&path).expect("save");

        // No .tmp.* sibling should remain after a successful save.
        let leftover: Vec<_> = fs::read_dir(tmp.path())
            .expect("read dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(
            leftover.is_empty(),
            "temp file must not linger: {leftover:?}"
        );

        // The written file must round-trip correctly.
        let loaded = ProjectConfigFile::load(&path).expect("load");
        assert_eq!(loaded.project_id, original.project_id);

        // Save a second time to confirm idempotency.
        original.save(&path).expect("second save");
        let loaded2 = ProjectConfigFile::load(&path).expect("load2");
        assert_eq!(loaded2.project_id, original.project_id);
    }

    #[cfg(unix)]
    #[test]
    fn config_file_is_0o600_after_atomic_write() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("reviewloop.toml");
        let cfg = ProjectConfigFile {
            project_id: "private-project".to_string(),
            ..ProjectConfigFile::default()
        };

        cfg.save(&path).expect("save");

        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "config file must be 0o600 after save");
    }

    #[test]
    fn defaults_start_polling_within_one_minute() {
        let cfg = Config::default();
        // First poll happens within ~1 minute so users see fast feedback after
        // submit; later attempts fall back over minutes (Phase 1 default change).
        assert_eq!(cfg.polling.schedule_minutes, vec![1, 2, 5, 10, 20, 40]);
        assert_eq!(cfg.trigger.git.repo_dir, ".");
        assert_eq!(cfg.core.max_submissions_per_tick, 1);
        assert!(cfg.project_id.is_empty());
        assert!(cfg.papers.is_empty());
    }

    #[test]
    fn email_ingestion_is_disabled_by_default() {
        // Email ingestion is opt-in: empty / unconfigured installations should
        // not silently try to log into IMAP or Gmail OAuth.
        let cfg = Config::default();
        let imap = cfg.imap.as_ref().expect("imap default config exists");
        assert!(
            !imap.enabled,
            "imap should be opt-in (Experimental), default disabled"
        );
        let gmail = cfg
            .gmail_oauth
            .as_ref()
            .expect("gmail oauth default config exists");
        assert!(
            !gmail.enabled,
            "gmail_oauth should be opt-in (Experimental), default disabled"
        );
    }

    #[test]
    fn global_config_rejects_project_fields() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("config.toml");
        fs::write(
            &path,
            r#"
papers = []

[core]
db_path = "db.sqlite"
"#,
        )
        .expect("write");
        assert!(GlobalConfigFile::load(&path).is_err());
    }

    #[test]
    fn project_config_rejects_global_fields() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("reviewloop.toml");
        fs::write(
            &path,
            r#"
project_id = "paper-a"

[core]
db_path = "db.sqlite"
"#,
        )
        .expect("write");
        assert!(ProjectConfigFile::load(&path).is_err());
    }

    #[test]
    fn legacy_split_preserves_global_and_project_fields() {
        let legacy = LegacyConfig::default();
        let global = legacy.global_config();
        let project = legacy.project_config();
        assert!(project.project_id.is_empty());
        assert_eq!(global.providers.stanford.base_url, "https://paperreview.ai");
        assert_eq!(project.providers.stanford.venue.as_deref(), Some("ICLR"));
    }

    #[test]
    fn finds_git_root_when_present() {
        let tmp = TempDir::new().expect("tempdir");
        fs::create_dir_all(tmp.path().join(".git")).expect("git dir");
        fs::create_dir_all(tmp.path().join("a/b")).expect("nested");
        let nested = tmp.path().join("a/b");
        assert_eq!(find_git_root(&nested).as_deref(), Some(tmp.path()));
    }

    #[test]
    fn default_project_path_uses_cwd_or_git_root() {
        let tmp = TempDir::new().expect("tempdir");
        fs::create_dir_all(tmp.path().join(".git")).expect("git dir");
        fs::create_dir_all(tmp.path().join("nested")).expect("nested");
        let old = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(tmp.path().join("nested")).expect("set cwd");
        let path = default_project_config_path().expect("path");
        std::env::set_current_dir(old).expect("restore cwd");
        assert_eq!(
            path.file_name().and_then(|name| name.to_str()),
            Some("reviewloop.toml")
        );
        assert_eq!(
            path.parent()
                .expect("project parent")
                .canonicalize()
                .expect("canonical project parent"),
            tmp.path().canonicalize().expect("canonical tempdir")
        );
    }

    fn paper_file(id: &str, backend: Option<&str>, venue: Option<&str>) -> PaperConfigFile {
        PaperConfigFile {
            id: id.to_string(),
            pdf_path: format!("{id}.pdf"),
            backend: backend.map(str::to_string),
            venue: venue.map(str::to_string),
        }
    }

    fn project_with(papers: Vec<PaperConfigFile>) -> ProjectConfigFile {
        ProjectConfigFile {
            project_id: "p".to_string(),
            papers,
            ..ProjectConfigFile::default()
        }
    }

    #[test]
    fn paper_backend_falls_back_to_default_backend_then_stanford() {
        // No explicit backend, no default_backend -> Config::DEFAULT_BACKEND
        let cfg = Config::merge_for_tests(
            GlobalConfigFile::default(),
            project_with(vec![paper_file("a", None, None)]),
        );
        assert_eq!(cfg.papers[0].backend, Config::DEFAULT_BACKEND);

        // No explicit backend, project sets default_backend -> uses it
        let mut project = project_with(vec![paper_file("b", None, None)]);
        project.default_backend = Some("custom".to_string());
        let cfg = Config::merge_for_tests(GlobalConfigFile::default(), project);
        assert_eq!(cfg.papers[0].backend, "custom");

        // Explicit backend wins over default_backend
        let mut project = project_with(vec![paper_file("c", Some("explicit"), None)]);
        project.default_backend = Some("ignored".to_string());
        let cfg = Config::merge_for_tests(GlobalConfigFile::default(), project);
        assert_eq!(cfg.papers[0].backend, "explicit");

        // Empty/whitespace explicit backend treated as missing
        let mut project = project_with(vec![paper_file("d", Some("   "), None)]);
        project.default_backend = Some("filled".to_string());
        let cfg = Config::merge_for_tests(GlobalConfigFile::default(), project);
        assert_eq!(cfg.papers[0].backend, "filled");
    }

    #[test]
    fn venue_for_resolves_per_paper_then_project_then_global() {
        // Per-paper venue wins
        let cfg = Config::merge_for_tests(
            GlobalConfigFile::default(),
            project_with(vec![paper_file(
                "a",
                Some("stanford"),
                Some("NeurIPS workshop"),
            )]),
        );
        assert_eq!(
            cfg.venue_for(&cfg.papers[0]),
            Some("NeurIPS workshop".to_string())
        );

        // No per-paper venue, project default applies for stanford
        let mut project = project_with(vec![paper_file("b", Some("stanford"), None)]);
        project.providers.stanford.venue = Some("CVPR".to_string());
        let cfg = Config::merge_for_tests(GlobalConfigFile::default(), project);
        assert_eq!(cfg.venue_for(&cfg.papers[0]), Some("CVPR".to_string()));

        // No per-paper, no project venue, stanford -> falls back to global
        // default which ships as "ICLR" (but is user-overridable in the
        // global config file -- see test below).
        let mut project = project_with(vec![paper_file("c", Some("stanford"), None)]);
        project.providers.stanford.venue = None;
        let cfg = Config::merge_for_tests(GlobalConfigFile::default(), project);
        assert_eq!(cfg.venue_for(&cfg.papers[0]), Some("ICLR".to_string()));

        // Empty per-paper venue is ignored, project default takes over
        let mut project = project_with(vec![paper_file("d", Some("stanford"), Some("   "))]);
        project.providers.stanford.venue = Some("ACL".to_string());
        let cfg = Config::merge_for_tests(GlobalConfigFile::default(), project);
        assert_eq!(cfg.venue_for(&cfg.papers[0]), Some("ACL".to_string()));

        // Non-stanford backend, no per-paper -> None
        let cfg = Config::merge_for_tests(
            GlobalConfigFile::default(),
            project_with(vec![paper_file("e", Some("custom"), None)]),
        );
        assert_eq!(cfg.venue_for(&cfg.papers[0]), None);

        // Non-stanford backend, with per-paper -> per-paper
        let cfg = Config::merge_for_tests(
            GlobalConfigFile::default(),
            project_with(vec![paper_file("f", Some("custom"), Some("Foo"))]),
        );
        assert_eq!(cfg.venue_for(&cfg.papers[0]), Some("Foo".to_string()));
    }

    #[test]
    fn venue_global_default_is_user_overridable() {
        // Regression guard for the original "ICLR is hardcoded in code" smell:
        // the global default venue MUST be a config value, not a string literal
        // baked into Config::venue_for. A user (or a future Stanford default
        // change) can flip it via ~/.config/reviewloop/config.toml without a
        // recompile, and project-level / per-paper overrides still win.
        let mut global = GlobalConfigFile::default();
        global.providers.stanford.venue = Some("NeurIPS".to_string());

        // Bare project: inherits the new global default.
        let cfg = Config::merge_for_tests(
            global.clone(),
            project_with(vec![paper_file("a", Some("stanford"), None)]),
        );
        assert_eq!(cfg.venue_for(&cfg.papers[0]), Some("NeurIPS".to_string()));

        // Project override still wins over global.
        let mut project = project_with(vec![paper_file("b", Some("stanford"), None)]);
        project.providers.stanford.venue = Some("CVPR".to_string());
        let cfg = Config::merge_for_tests(global.clone(), project);
        assert_eq!(cfg.venue_for(&cfg.papers[0]), Some("CVPR".to_string()));

        // Per-paper override still wins over project + global.
        let cfg = Config::merge_for_tests(
            global,
            project_with(vec![paper_file("c", Some("stanford"), Some("ACL"))]),
        );
        assert_eq!(cfg.venue_for(&cfg.papers[0]), Some("ACL".to_string()));
    }

    #[test]
    fn venue_returns_none_when_no_default_anywhere() {
        // When the global default is explicitly cleared and nothing project-
        // or paper-level fills in, venue_for is None. The submit path treats
        // this as "no venue", which Stanford backend serializes as empty
        // string -- not great UX, but the behavior is observable + testable
        // rather than masked by an invisible "ICLR" default.
        let mut global = GlobalConfigFile::default();
        global.providers.stanford.venue = None;
        let cfg = Config::merge_for_tests(
            global,
            project_with(vec![paper_file("a", Some("stanford"), None)]),
        );
        assert_eq!(cfg.venue_for(&cfg.papers[0]), None);
    }

    #[test]
    fn paper_runtime_struct_keeps_pdf_path() {
        let project = project_with(vec![PaperConfigFile {
            id: "main".to_string(),
            pdf_path: "build/main.pdf".to_string(),
            backend: Some("stanford".to_string()),
            venue: Some("ICLR".to_string()),
        }]);
        let cfg = Config::merge_for_tests(GlobalConfigFile::default(), project);
        let resolved: &PaperConfig = &cfg.papers[0];
        assert_eq!(resolved.id, "main");
        assert_eq!(resolved.pdf_path, "build/main.pdf");
        assert_eq!(resolved.backend, "stanford");
        assert_eq!(resolved.venue.as_deref(), Some("ICLR"));
    }

    #[test]
    fn legacy_papers_round_trip_through_project_config() {
        // Existing legacy single-file configs continue to deserialize / migrate:
        // backend stays explicit, venue is migrated to project-default (per-paper venue
        // is a new field and not present in legacy files).
        let mut legacy = LegacyConfig::default();
        legacy.papers.push(PaperConfigFile {
            id: "main".to_string(),
            pdf_path: "main.pdf".to_string(),
            backend: Some("stanford".to_string()),
            venue: None,
        });
        let project = legacy.project_config();
        assert_eq!(project.papers.len(), 1);
        assert_eq!(project.papers[0].backend.as_deref(), Some("stanford"));
        assert_eq!(project.providers.stanford.venue.as_deref(), Some("ICLR"));
        // None of the new override slots should fire on migration: the legacy
        // values stay where they were (in global), not duplicated into project.
        assert_eq!(project.providers.stanford.email, None);
        assert_eq!(project.providers.stanford.fallback_script, None);
        assert_eq!(project.core.review_timeout_hours, None);
    }

    #[test]
    fn provider_email_uses_project_override_then_global() {
        // No project override -> global value flows through.
        let mut global = GlobalConfigFile::default();
        global.providers.stanford.email = "global@example.edu".to_string();
        let cfg = Config::merge_for_tests(global.clone(), project_with(vec![]));
        assert_eq!(cfg.providers.stanford.email, "global@example.edu");

        // Project override wins.
        let mut project = project_with(vec![]);
        project.providers.stanford.email = Some("project@example.edu".to_string());
        let cfg = Config::merge_for_tests(global.clone(), project);
        assert_eq!(cfg.providers.stanford.email, "project@example.edu");

        // Empty/whitespace project override falls back to global.
        let mut project = project_with(vec![]);
        project.providers.stanford.email = Some("   ".to_string());
        let cfg = Config::merge_for_tests(global, project);
        assert_eq!(cfg.providers.stanford.email, "global@example.edu");
    }

    #[test]
    fn provider_fallback_script_uses_project_override_then_global() {
        let mut global = GlobalConfigFile::default();
        global.providers.stanford.fallback_script = "tools/global.mjs".to_string();
        let cfg = Config::merge_for_tests(global.clone(), project_with(vec![]));
        assert_eq!(cfg.providers.stanford.fallback_script, "tools/global.mjs");

        let mut project = project_with(vec![]);
        project.providers.stanford.fallback_script = Some("tools/project.mjs".to_string());
        let cfg = Config::merge_for_tests(global, project);
        assert_eq!(cfg.providers.stanford.fallback_script, "tools/project.mjs");
    }

    #[test]
    fn core_review_timeout_uses_project_override_then_global() {
        let mut global = GlobalConfigFile::default();
        global.core.review_timeout_hours = 48;
        let cfg = Config::merge_for_tests(global.clone(), project_with(vec![]));
        assert_eq!(cfg.core.review_timeout_hours, 48);

        let mut project = project_with(vec![]);
        project.core.review_timeout_hours = Some(12);
        let cfg = Config::merge_for_tests(global, project);
        assert_eq!(cfg.core.review_timeout_hours, 12);
    }

    #[test]
    fn trigger_tag_pattern_uses_project_override_then_global() {
        let mut global = GlobalConfigFile::default();
        global.trigger.git.tag_pattern = "global-pattern/*".to_string();
        // No project override -> global default flows through
        let cfg = Config::merge_for_tests(global.clone(), project_with(vec![]));
        assert_eq!(cfg.trigger.git.tag_pattern, "global-pattern/*");

        // Project override wins
        let mut project = project_with(vec![]);
        project.trigger.git.tag_pattern = Some("project-pattern/*".to_string());
        let cfg = Config::merge_for_tests(global.clone(), project);
        assert_eq!(cfg.trigger.git.tag_pattern, "project-pattern/*");

        // Empty/whitespace project override falls back to global
        let mut project = project_with(vec![]);
        project.trigger.git.tag_pattern = Some("   ".to_string());
        let cfg = Config::merge_for_tests(global, project);
        assert_eq!(cfg.trigger.git.tag_pattern, "global-pattern/*");
    }

    #[test]
    fn trigger_pdf_prefs_use_project_overrides_then_global() {
        let mut global = GlobalConfigFile::default();
        global.trigger.pdf.auto_submit_on_change = true;
        global.trigger.pdf.max_scan_papers = 25;

        // No project overrides -> global defaults flow through
        let cfg = Config::merge_for_tests(global.clone(), project_with(vec![]));
        assert!(cfg.trigger.pdf.auto_submit_on_change);
        assert_eq!(cfg.trigger.pdf.max_scan_papers, 25);

        // Project overrides win
        let mut project = project_with(vec![]);
        project.trigger.pdf.auto_submit_on_change = Some(false);
        project.trigger.pdf.max_scan_papers = Some(7);
        let cfg = Config::merge_for_tests(global, project);
        assert!(!cfg.trigger.pdf.auto_submit_on_change);
        assert_eq!(cfg.trigger.pdf.max_scan_papers, 7);
    }

    #[test]
    fn trigger_project_only_fields_pass_through_unchanged() {
        // git.enabled, repo_dir, auto_create_tags, auto_delete_processed_tags,
        // pdf.enabled live exclusively on the project side -- no global default.
        let mut project = project_with(vec![]);
        project.trigger.git.enabled = false;
        project.trigger.git.repo_dir = "/tmp/repo".to_string();
        project.trigger.git.auto_create_tags_on_pdf_change = true;
        project.trigger.git.auto_delete_processed_tags = true;
        project.trigger.pdf.enabled = false;
        let cfg = Config::merge_for_tests(GlobalConfigFile::default(), project);
        assert!(!cfg.trigger.git.enabled);
        assert_eq!(cfg.trigger.git.repo_dir, "/tmp/repo");
        assert!(cfg.trigger.git.auto_create_tags_on_pdf_change);
        assert!(cfg.trigger.git.auto_delete_processed_tags);
        assert!(!cfg.trigger.pdf.enabled);
    }

    #[test]
    fn legacy_config_migrates_trigger_fully_to_project_side() {
        // Legacy single-file configs put trigger settings in one shared struct.
        // Migration must preserve those exact values, even when they differ
        // from the new global defaults, so the upgraded user sees no behavior
        // change. Achieved by parking the legacy trigger as project overrides.
        let mut legacy = LegacyConfig::default();
        legacy.trigger.git.tag_pattern = "legacy-style/<paper-id>/*".to_string();
        legacy.trigger.pdf.auto_submit_on_change = true;
        legacy.trigger.pdf.max_scan_papers = 99;

        let migrated_project = legacy.project_config();
        assert_eq!(
            migrated_project.trigger.git.tag_pattern.as_deref(),
            Some("legacy-style/<paper-id>/*")
        );
        assert_eq!(
            migrated_project.trigger.pdf.auto_submit_on_change,
            Some(true)
        );
        assert_eq!(migrated_project.trigger.pdf.max_scan_papers, Some(99));

        // And the migrated global trigger is plain defaults -- the project
        // overrides carry the actual values so the merged Config matches
        // the legacy runtime exactly.
        let migrated_global = legacy.global_config();
        let cfg = Config::merge_for_tests(migrated_global, migrated_project);
        assert_eq!(cfg.trigger.git.tag_pattern, "legacy-style/<paper-id>/*");
        assert!(cfg.trigger.pdf.auto_submit_on_change);
        assert_eq!(cfg.trigger.pdf.max_scan_papers, 99);
    }

    #[test]
    fn notifications_default_enabled() {
        let cfg = Config::default();
        assert!(cfg.notifications.enabled);
        assert!(!cfg.notifications.summary_only);
    }

    #[test]
    fn notifications_use_project_override_then_global() {
        // No project override -> inherits global
        let mut global = GlobalConfigFile::default();
        global.notifications.enabled = true;
        global.notifications.summary_only = false;
        let cfg = Config::merge_for_tests(global.clone(), project_with(vec![]));
        assert!(cfg.notifications.enabled);
        assert!(!cfg.notifications.summary_only);

        // Project disables notifications
        let mut project = project_with(vec![]);
        project.notifications.enabled = Some(false);
        let cfg = Config::merge_for_tests(global.clone(), project);
        assert!(!cfg.notifications.enabled);

        // Project enables summary_only
        let mut project = project_with(vec![]);
        project.notifications.summary_only = Some(true);
        let cfg = Config::merge_for_tests(global.clone(), project);
        assert!(cfg.notifications.summary_only);

        // Global disabled, project re-enables
        let mut global2 = GlobalConfigFile::default();
        global2.notifications.enabled = false;
        let mut project = project_with(vec![]);
        project.notifications.enabled = Some(true);
        let cfg = Config::merge_for_tests(global2, project);
        assert!(cfg.notifications.enabled);
    }

    // ──────────────────────────────────────────────────────────────────────
    // O9: base_url validation
    // ──────────────────────────────────────────────────────────────────────

    #[test]
    fn base_url_https_passes() {
        let mut cfg = Config::default();
        cfg.providers.stanford.base_url = "https://paperreview.ai".to_string();
        assert!(cfg.validate_base_url().is_ok());
    }

    #[test]
    fn base_url_http_fails() {
        let mut cfg = Config::default();
        cfg.providers.stanford.base_url = "http://paperreview.ai".to_string();
        assert!(cfg.validate_base_url().is_err());
    }

    #[test]
    fn base_url_localhost_http_passes() {
        let mut cfg = Config::default();
        cfg.providers.stanford.base_url = "http://localhost:8080".to_string();
        assert!(cfg.validate_base_url().is_ok());
    }

    #[test]
    fn base_url_127_0_0_1_http_passes() {
        let mut cfg = Config::default();
        cfg.providers.stanford.base_url = "http://127.0.0.1:9000".to_string();
        assert!(cfg.validate_base_url().is_ok());
    }

    // ──────────────────────────────────────────────────────────────────────
    // O8: fallback_script path traversal validation
    // ──────────────────────────────────────────────────────────────────────

    #[test]
    fn fallback_script_absolute_always_passes() {
        let mut cfg = Config::default();
        cfg.providers.stanford.fallback_script = "/usr/local/bin/fallback.mjs".to_string();
        cfg.project_root = None;
        assert!(cfg.validate_fallback_script().is_ok());
    }

    #[test]
    fn fallback_script_relative_with_dotdot_and_no_root_fails() {
        let mut cfg = Config::default();
        cfg.providers.stanford.fallback_script = "../../etc/passwd".to_string();
        cfg.project_root = None;
        assert!(cfg.validate_fallback_script().is_err());
    }

    #[test]
    fn fallback_script_relative_no_dotdot_no_root_passes() {
        let mut cfg = Config::default();
        cfg.providers.stanford.fallback_script = "tools/fallback.mjs".to_string();
        cfg.project_root = None;
        // Relative without `..` and no project root is fine (script won't resolve
        // but also won't be invoked).
        assert!(cfg.validate_fallback_script().is_ok());
    }

    #[test]
    fn foreign_load_rejects_absolute_fallback_script_outside_home() {
        let mut cfg = Config::default();
        cfg.providers.stanford.fallback_script = "/tmp/evil/script.js".to_string();

        let err = cfg
            .validate_for_foreign_load()
            .expect_err("absolute fallback_script outside HOME must be rejected")
            .to_string();
        assert!(
            err.contains("fallback_script outside HOME"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn foreign_load_allows_relative_fallback_script() {
        let mut cfg = Config::default();
        cfg.providers.stanford.fallback_script = "tools/fallback.mjs".to_string();

        assert!(cfg.validate_for_foreign_load().is_ok());
    }

    #[test]
    fn foreign_load_allows_absolute_fallback_script_under_home() {
        let mut cfg = Config::default();
        let script = home_dir_for_security()
            .expect("HOME is required for this test")
            .join(".reviewloop")
            .join("fallback.mjs");
        cfg.providers.stanford.fallback_script = script.to_string_lossy().to_string();

        assert!(cfg.validate_for_foreign_load().is_ok());
    }

    // ──────────────────────────────────────────────────────────────────────
    // L1: Redacted<T> Debug impl
    // ──────────────────────────────────────────────────────────────────────

    #[test]
    fn redacted_debug_hides_value() {
        let secret: Redacted<String> = Redacted::from("hunter2".to_string());
        assert_eq!(format!("{:?}", secret), "<redacted>");
    }

    #[test]
    fn redacted_deref_gives_inner() {
        let s: Redacted<String> = Redacted::from("hello".to_string());
        assert_eq!(s.as_str(), "hello");
        assert!(!s.trim().is_empty());
    }

    // ──────────────────────────────────────────────────────────────────────
    // OSS-353: CSPaper provider settings
    // ──────────────────────────────────────────────────────────────────────

    const CSPAPER_KEY: &str = "csp_live_X";

    fn write_config(dir: &TempDir, name: &str, body: &str) -> std::path::PathBuf {
        let path = dir.path().join(name);
        fs::write(&path, body).expect("write config");
        path
    }

    fn cspaper_paper(venue: Option<&str>) -> PaperConfigFile {
        paper_file("main", Some("cspaper"), venue)
    }

    fn global_with_key(key: &str) -> GlobalConfigFile {
        let mut global = GlobalConfigFile::default();
        global.providers.cspaper.api_key = Some(Redacted(key.to_string()));
        global
    }

    fn table_at<'a>(table: &'a toml::Table, path: &[&str]) -> &'a toml::Table {
        path.iter().fold(table, |table, key| {
            table
                .get(*key)
                .and_then(toml::Value::as_table)
                .unwrap_or_else(|| panic!("missing table {key} in {path:?}"))
        })
    }

    /// Asserts a config error neither quotes `secret` in its message nor in
    /// its debug chain.
    fn assert_no_echo(err: &anyhow::Error, secret: &str) {
        let display = format!("{err:#}");
        let debug = format!("{err:?}");
        assert!(
            !display.contains(secret),
            "error echoes the secret: {display}"
        );
        assert!(
            !debug.contains(secret),
            "error chain echoes the secret: {debug}"
        );
    }

    #[test]
    fn global_default_writes_cspaper_table_without_api_key() {
        let raw = toml::to_string_pretty(&GlobalConfigFile::default()).expect("serialize");
        assert!(raw.contains("[providers.cspaper]"), "{raw}");
        assert!(!raw.contains("api_key"), "{raw}");

        let parsed: toml::Table = toml::from_str(&raw).expect("reparse");
        let cspaper = table_at(&parsed, &["providers", "cspaper"]);
        assert_eq!(
            cspaper.get("base_url").and_then(toml::Value::as_str),
            Some("https://cspaper.org")
        );
        assert_eq!(
            cspaper
                .get("desk_rejection_enabled")
                .and_then(toml::Value::as_bool),
            Some(true)
        );
        // No built-in template: the user must choose what the review means.
        assert!(!cspaper.contains_key("agent_id"), "{raw}");
    }

    #[test]
    fn global_api_key_round_trips_through_save_and_load() {
        let tmp = TempDir::new().expect("tempdir");
        let path = write_config(
            &tmp,
            "config.toml",
            &format!(
                "[providers.cspaper]\napi_key = \"{CSPAPER_KEY}\"\nagent_id = \"ICLR_main_2026_1\"\n"
            ),
        );
        let loaded = GlobalConfigFile::load(&path).expect("load global");
        let cspaper = &loaded.providers.cspaper;
        assert_eq!(
            cspaper.api_key.as_deref().map(String::as_str),
            Some(CSPAPER_KEY)
        );
        assert_eq!(cspaper.agent_id.as_deref(), Some("ICLR_main_2026_1"));
        assert_eq!(cspaper.base_url, "https://cspaper.org");
        assert!(cspaper.desk_rejection_enabled);

        // The global file is the key's home, so saving keeps it.
        let saved = tmp.path().join("saved.toml");
        loaded.save(&saved).expect("save global");
        let raw = fs::read_to_string(&saved).expect("read saved");
        assert!(
            raw.contains(&format!("api_key = \"{CSPAPER_KEY}\"")),
            "{raw}"
        );
        let reloaded = GlobalConfigFile::load(&saved).expect("reload global");
        assert_eq!(
            reloaded.providers.cspaper.api_key,
            loaded.providers.cspaper.api_key
        );
        assert_eq!(
            reloaded.providers.cspaper.agent_id,
            loaded.providers.cspaper.agent_id
        );

        let cfg = Config::merge_for_tests(reloaded, project_with(vec![]));
        assert_eq!(
            cfg.providers.cspaper.api_key.as_deref().map(String::as_str),
            Some(CSPAPER_KEY)
        );
    }

    #[test]
    fn project_config_refuses_api_key_without_echoing_it() {
        let tmp = TempDir::new().expect("tempdir");
        // Each places the key differently; the unquoted and unterminated
        // forms are TOML syntax errors whose parser report would quote it.
        let bodies = [
            format!("project_id = \"p\"\n\n[providers.cspaper]\napi_key = \"{CSPAPER_KEY}\"\n"),
            format!("project_id = \"p\"\n\n[providers.cspaper]\napi_key = {CSPAPER_KEY}\n"),
            format!("project_id = \"p\"\n\n[providers.cspaper]\napi_key = \"{CSPAPER_KEY}\n"),
            format!("project_id = \"p\"\nproviders.cspaper.api_key = \"{CSPAPER_KEY}\"\n"),
            format!(
                "project_id = \"p\"\n[providers]\ncspaper = {{ api_key = \"{CSPAPER_KEY}\" }}\n"
            ),
            format!("project_id = \"p\"\n[providers.cspaper]\n\"api_key\" = '{CSPAPER_KEY}'\n"),
            format!("project_id = \"p\"\n[providers.stanford]\napi_key = \"{CSPAPER_KEY}\"\n"),
            format!(
                "project_id = \"p\"\n[[papers]]\nid = \"a\"\npdf_path = \"a.pdf\"\napi_key = \"{CSPAPER_KEY}\"\n"
            ),
        ];
        for body in bodies {
            let path = write_config(&tmp, "reviewloop.toml", &body);
            let err = ProjectConfigFile::load(&path).expect_err("api_key in a project file");
            assert_no_echo(&err, CSPAPER_KEY);
            let msg = format!("{err:#}");
            assert!(msg.contains("holds an API key"), "{msg}");
            assert!(msg.contains("providers.cspaper.api_key"), "{msg}");
            assert!(msg.contains("global config"), "{msg}");
            assert!(msg.contains(CSPAPER_API_KEY_ENV), "{msg}");
            assert!(msg.contains(&path.display().to_string()), "{msg}");
        }

        // The error names the line holding the key.
        let path = write_config(
            &tmp,
            "reviewloop.toml",
            &format!("project_id = \"p\"\n\n[providers.cspaper]\napi_key = \"{CSPAPER_KEY}\"\n"),
        );
        let msg = format!("{:#}", ProjectConfigFile::load(&path).expect_err("refused"));
        assert!(msg.contains("(line 4)"), "{msg}");
    }

    #[test]
    fn project_config_allows_commented_api_key_and_agent_settings() {
        let tmp = TempDir::new().expect("tempdir");
        let path = write_config(
            &tmp,
            "reviewloop.toml",
            "project_id = \"p\"\n\n[providers.cspaper]\n# api_key lives in the global config\n\
             agent_id = \"ICLR_main_2026_1\"\ndesk_rejection_enabled = false\n",
        );
        let project = ProjectConfigFile::load(&path).expect("load project");
        assert_eq!(
            project.providers.cspaper.agent_id.as_deref(),
            Some("ICLR_main_2026_1")
        );
        assert_eq!(
            project.providers.cspaper.desk_rejection_enabled,
            Some(false)
        );
    }

    #[test]
    fn project_config_refuses_cspaper_base_url() {
        let tmp = TempDir::new().expect("tempdir");
        let path = write_config(
            &tmp,
            "reviewloop.toml",
            "project_id = \"p\"\n\n[providers.cspaper]\nbase_url = \"https://key-sink.example\"\n",
        );
        let err = ProjectConfigFile::load(&path).expect_err("base_url in a project file");
        let msg = format!("{err:#}");
        assert!(msg.contains("unknown field `base_url`"), "{msg}");
        assert!(msg.contains("line 4, column 1"), "{msg}");
        assert_no_echo(&err, "key-sink.example");
    }

    #[test]
    fn config_parse_errors_never_quote_the_file() {
        let tmp = TempDir::new().expect("tempdir");

        // A misnamed key is an unknown field: the field is named, the line is not quoted.
        let path = write_config(
            &tmp,
            "reviewloop.toml",
            // Not key-shaped, so the unknown-field path (not the key scan) reports it.
            "project_id = \"p\"\n[providers.cspaper]\napikey = \"not-a-key-shape-secret\"\n",
        );
        let err = ProjectConfigFile::load(&path).expect_err("unknown field");
        assert_no_echo(&err, "not-a-key-shape-secret");
        assert!(
            format!("{err:#}").contains("unknown field `apikey`"),
            "{err:#}"
        );

        // A syntax error in the global file, where secrets do belong.
        let path = write_config(&tmp, "config.toml", "[imap]\npassword = hunter2\n");
        let err = GlobalConfigFile::load(&path).expect_err("syntax error");
        assert_no_echo(&err, "hunter2");
        let msg = format!("{err:#}");
        assert!(msg.contains("failed to parse TOML config"), "{msg}");
        assert!(msg.contains(&path.display().to_string()), "{msg}");
        assert!(msg.contains("line 2, column 12"), "{msg}");
    }

    #[test]
    fn config_type_errors_name_the_kind_but_not_the_value() {
        let tmp = TempDir::new().expect("tempdir");
        for (name, body, secret, kind) in [
            (
                "config.toml",
                format!("[providers]\ncspaper = \"{CSPAPER_KEY}\"\n"),
                CSPAPER_KEY,
                "invalid type: string <value>",
            ),
            (
                "config.toml",
                "[imap]\npassword = 12345678\n".to_string(),
                "12345678",
                "invalid type: integer <value>",
            ),
            (
                "reviewloop.toml",
                "project_id = \"p\"\n[providers.cspaper]\ndesk_rejection_enabled = \"hunter2secret\"\n"
                    .to_string(),
                "hunter2secret",
                "invalid type: string <value>",
            ),
        ] {
            let path = write_config(&tmp, name, &body);
            let err = if name == "config.toml" {
                GlobalConfigFile::load(&path).map(drop)
            } else {
                ProjectConfigFile::load(&path).map(drop)
            }
            .expect_err("type error");
            assert_no_echo(&err, secret);
            assert!(format!("{err:#}").contains(kind), "{err:#}");
        }
    }

    #[test]
    fn project_file_refuses_a_cspaper_key_anywhere() {
        let tmp = TempDir::new().expect("tempdir");
        for (body, line) in [
            (format!("project_id = \"p\"\n# old key: {CSPAPER_KEY}\n"), 2),
            (
                format!("project_id = \"p\"\n[providers.cspaper]\nagent_id = \"{CSPAPER_KEY}\"\n"),
                3,
            ),
        ] {
            let path = write_config(&tmp, "reviewloop.toml", &body);
            let err = ProjectConfigFile::load(&path).expect_err("key-shaped text refused");
            assert_no_echo(&err, CSPAPER_KEY);
            assert!(
                format!("{err:#}").contains(&format!("(line {line})")),
                "{err:#}"
            );
        }
    }

    #[test]
    fn debug_output_never_contains_cspaper_api_key() {
        let global = global_with_key(CSPAPER_KEY);
        assert!(!format!("{global:?}").contains(CSPAPER_KEY));
        let cfg = Config::merge_for_tests(global, project_with(vec![cspaper_paper(None)]));
        assert!(cfg.providers.cspaper.api_key.is_some());
        assert!(!format!("{cfg:?}").contains(CSPAPER_KEY));
        assert!(!format!("{cfg:#?}").contains(CSPAPER_KEY));
    }

    #[test]
    fn runtime_providers_serialization_skips_api_key() {
        let cfg = Config::merge_for_tests(global_with_key(CSPAPER_KEY), project_with(vec![]));
        assert!(cfg.providers.cspaper.api_key.is_some());
        let as_toml = toml::to_string_pretty(&cfg.providers).expect("toml");
        let as_json = serde_json::to_string(&cfg.providers).expect("json");
        for raw in [as_toml, as_json] {
            assert!(!raw.contains(CSPAPER_KEY), "{raw}");
            assert!(!raw.contains("api_key"), "{raw}");
        }
    }

    #[test]
    fn config_default_never_reads_cspaper_key_from_environment() {
        // Only the runtime loader consults the environment.
        assert!(Config::default().providers.cspaper.api_key.is_none());
    }

    #[test]
    fn cspaper_agent_id_resolves_paper_then_project_then_global() {
        assert_eq!(GlobalConfigFile::default().providers.cspaper.agent_id, None);
        let cfg = Config::merge_for_tests(
            GlobalConfigFile::default(),
            project_with(vec![cspaper_paper(None)]),
        );
        assert_eq!(cfg.venue_for(&cfg.papers[0]), None);

        let mut global = GlobalConfigFile::default();
        global.providers.cspaper.agent_id = Some("GLOBAL_T".to_string());
        let resolve = |project_agent: Option<&str>, paper_venue: Option<&str>| {
            let mut project = project_with(vec![cspaper_paper(paper_venue)]);
            project.providers.cspaper.agent_id = project_agent.map(str::to_string);
            let cfg = Config::merge_for_tests(global.clone(), project);
            cfg.venue_for(&cfg.papers[0])
        };
        assert_eq!(
            resolve(Some("PROJECT_T"), Some("PAPER_T")).as_deref(),
            Some("PAPER_T")
        );
        assert_eq!(
            resolve(Some("PROJECT_T"), Some("   ")).as_deref(),
            Some("PROJECT_T")
        );
        assert_eq!(
            resolve(Some("PROJECT_T"), None).as_deref(),
            Some("PROJECT_T")
        );
        assert_eq!(resolve(Some("   "), None).as_deref(), Some("GLOBAL_T"));
        assert_eq!(resolve(None, None).as_deref(), Some("GLOBAL_T"));

        // The stanford venue never leaks into a cspaper paper's template.
        let mut project = project_with(vec![cspaper_paper(None)]);
        project.providers.stanford.venue = Some("CVPR".to_string());
        let cfg = Config::merge_for_tests(GlobalConfigFile::default(), project);
        assert_eq!(cfg.venue_for(&cfg.papers[0]), None);
    }

    #[test]
    fn cspaper_desk_rejection_project_override_beats_global() {
        let resolve = |global_value: bool, project_value: Option<bool>| {
            let mut global = GlobalConfigFile::default();
            global.providers.cspaper.desk_rejection_enabled = global_value;
            let mut project = project_with(vec![]);
            project.providers.cspaper.desk_rejection_enabled = project_value;
            Config::merge_for_tests(global, project)
                .providers
                .cspaper
                .desk_rejection_enabled
        };
        assert!(resolve(true, None));
        assert!(!resolve(false, None));
        assert!(!resolve(true, Some(false)));
        assert!(resolve(false, Some(true)));
    }

    #[test]
    fn review_options_for_names_cspaper_desk_rejection_only() {
        let mut project = project_with(vec![
            paper_file("s", Some("stanford"), None),
            cspaper_paper(None),
        ]);
        let cfg = Config::merge_for_tests(GlobalConfigFile::default(), project.clone());
        let stanford = cfg.review_options_for(&cfg.papers[0]);
        assert!(stanford.is_empty());
        assert_eq!(stanford.canonical(), None);
        assert_eq!(
            cfg.review_options_for(&cfg.papers[1])
                .canonical()
                .as_deref(),
            Some(r#"{"desk_rejection_enabled":"true"}"#)
        );

        project.providers.cspaper.desk_rejection_enabled = Some(false);
        let cfg = Config::merge_for_tests(GlobalConfigFile::default(), project);
        let cspaper = cfg.review_options_for(&cfg.papers[1]);
        assert_eq!(cspaper.get("desk_rejection_enabled"), Some("false"));
        assert_eq!(
            cspaper.canonical().as_deref(),
            Some(r#"{"desk_rejection_enabled":"false"}"#)
        );
        assert!(cfg.review_options_for(&cfg.papers[0]).is_empty());
    }

    #[test]
    fn default_provider_base_urls_validate() {
        assert!(Config::default().validate_base_url().is_ok());
    }

    #[test]
    fn provider_base_urls_require_https_or_loopback_http() {
        type SetBaseUrl = fn(&mut Config, &str);
        let fields: [(&str, SetBaseUrl); 2] = [
            ("providers.stanford.base_url", |cfg, url| {
                cfg.providers.stanford.base_url = url.to_string();
            }),
            ("providers.cspaper.base_url", |cfg, url| {
                cfg.providers.cspaper.base_url = url.to_string();
            }),
        ];
        let accepted = [
            "https://paperreview.ai",
            "https://cspaper.org",
            "http://localhost:8080",
            "http://127.0.0.1:9",
            "http://[::1]:8080",
        ];
        let rejected = [
            "http://example.com",
            "http://localhost.evil",
            "http://127.0.0.1.evil.example",
            "garbage",
            "",
            "ftp://cspaper.org",
            "file:///etc/passwd",
        ];
        for (field, set) in fields {
            for url in accepted {
                let mut cfg = Config::default();
                set(&mut cfg, url);
                assert!(
                    cfg.validate_base_url().is_ok(),
                    "{field} = {url:?} must be accepted"
                );
            }
            for url in rejected {
                let mut cfg = Config::default();
                set(&mut cfg, url);
                let Err(err) = cfg.validate_base_url() else {
                    panic!("{field} = {url:?} must be rejected");
                };
                assert!(err.to_string().contains(field), "{err}");
            }
        }
    }

    #[test]
    fn secret_or_env_prefers_non_blank_config_then_env() {
        let secret = |value: &str| Some(Redacted(value.to_string()));
        let resolve = |configured, env: Option<&str>| {
            secret_or_env(configured, env.map(str::to_string)).map(|secret| secret.0)
        };
        assert_eq!(
            resolve(secret("from-config"), Some("from-env")).as_deref(),
            Some("from-config")
        );
        assert_eq!(
            resolve(secret("  padded  "), None).as_deref(),
            Some("padded")
        );
        assert_eq!(
            resolve(secret("   "), Some("from-env")).as_deref(),
            Some("from-env")
        );
        assert_eq!(resolve(None, Some("from-env")).as_deref(), Some("from-env"));
        assert_eq!(resolve(secret(""), Some("   ")), None);
        assert_eq!(resolve(None, Some("")), None);
        assert_eq!(resolve(None, None), None);
    }
}

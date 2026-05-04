use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
};

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
            let config = Self::from_parts(global, project, None);
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
        let config = Self::from_parts(global, project, project_root);
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

    pub fn effective_stanford_venue(&self) -> String {
        self.providers
            .stanford
            .venue
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .unwrap_or("ICLR")
            .to_string()
    }

    /// Resolve the venue used when submitting / referencing this specific paper.
    /// For `stanford`, falls back to the project default and finally to "ICLR";
    /// for other backends, just returns the per-paper override (or `None`).
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
            "stanford" => Some(self.effective_stanford_venue()),
            _ => None,
        }
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
                    venue: project.providers.stanford.venue,
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
        Ok(())
    }
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
}

impl ProjectConfigFile {
    pub fn load(path: &Path) -> Result<Self> {
        load_toml_file(path)
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
                },
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
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read config: {}", path.display()))?;
    toml::from_str(&raw).with_context(|| format!("failed to parse TOML config: {}", path.display()))
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
    fs::write(path, content)
        .with_context(|| format!("failed to write config file: {}", path.display()))
}

fn discover_project_config_path(explicit_path: Option<&Path>) -> Result<Option<PathBuf>> {
    if let Some(path) = explicit_path {
        if !path.exists() {
            return Err(anyhow!("project config file not found: {}", path.display()));
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CoreConfig {
    pub state_dir: String,
    pub db_path: String,
    pub max_concurrency: usize,
    pub max_submissions_per_tick: usize,
    pub review_timeout_hours: u64,
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            state_dir: default_state_dir(),
            db_path: default_db_path(),
            max_concurrency: 2,
            max_submissions_per_tick: 1,
            review_timeout_hours: 48,
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
            schedule_minutes: vec![10, 20, 40, 60],
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GlobalStanfordProviderConfig {
    pub base_url: String,
    pub fallback_mode: String,
    pub fallback_script: String,
    pub email: String,
}

impl Default for GlobalStanfordProviderConfig {
    fn default() -> Self {
        let base = StanfordProviderConfig::default();
        Self {
            base_url: base.base_url,
            fallback_mode: base.fallback_mode,
            fallback_script: base.fallback_script,
            email: base.email,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectProvidersConfig {
    pub stanford: ProjectStanfordProviderConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub venue: Option<String>,
}

impl Default for ProjectStanfordProviderConfig {
    fn default() -> Self {
        Self {
            email: None,
            fallback_script: None,
            venue: Some("ICLR".to_string()),
        }
    }
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
    pub password: String,
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
            enabled: true,
            server: "imap.gmail.com".to_string(),
            port: 993,
            username: String::new(),
            password: String::new(),
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
    pub client_secret: String,
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
            enabled: true,
            client_id: String::new(),
            client_secret: String::new(),
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
        Config, GlobalConfigFile, LegacyConfig, PaperConfig, PaperConfigFile, ProjectConfigFile,
        default_project_config_path, find_git_root,
    };
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn defaults_start_polling_at_ten_minutes() {
        let cfg = Config::default();
        assert_eq!(cfg.polling.schedule_minutes, vec![10, 20, 40, 60]);
        assert_eq!(cfg.trigger.git.repo_dir, ".");
        assert_eq!(cfg.core.max_submissions_per_tick, 1);
        assert!(cfg.project_id.is_empty());
        assert!(cfg.papers.is_empty());
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
    fn venue_for_resolves_per_paper_then_project_then_iclr() {
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

        // No per-paper, no project venue, stanford -> falls back to "ICLR"
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
}

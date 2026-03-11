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

    pub fn save_project(&self, path: &Path) -> Result<()> {
        self.project_file().save(path)
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

    pub fn project_file(&self) -> ProjectConfigFile {
        ProjectConfigFile {
            project_id: self.project_id.clone(),
            trigger: self.trigger.clone(),
            providers: ProjectProvidersConfig {
                stanford: ProjectStanfordProviderConfig {
                    venue: self.providers.stanford.venue.clone(),
                },
            },
            papers: self.papers.clone(),
            paper_watch: self.paper_watch.clone(),
            paper_tag_triggers: self.paper_tag_triggers.clone(),
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

        Self {
            project_id: project.project_id,
            core: global.core,
            logging: global.logging,
            polling: global.polling,
            retention: global.retention,
            trigger: project.trigger,
            providers: ProvidersConfig {
                stanford: StanfordProviderConfig {
                    base_url: global.providers.stanford.base_url,
                    fallback_mode: global.providers.stanford.fallback_mode,
                    fallback_script: global.providers.stanford.fallback_script,
                    email: global.providers.stanford.email,
                    venue: project.providers.stanford.venue,
                },
            },
            papers: project.papers,
            paper_watch: project.paper_watch,
            paper_tag_triggers: project.paper_tag_triggers,
            imap: global.imap,
            gmail_oauth: global.gmail_oauth,
            project_root,
        }
    }

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
                "project config is required here. create {} with project_id or run `reviewloop config migrate-project --project-id <id>`",
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
    pub providers: GlobalProvidersConfig,
    pub imap: Option<ImapConfig>,
    pub gmail_oauth: Option<GmailOauthConfig>,
}

impl Default for GlobalConfigFile {
    fn default() -> Self {
        Self {
            core: CoreConfig::default(),
            logging: LoggingConfig::default(),
            polling: PollingConfig::default(),
            retention: RetentionConfig::default(),
            providers: GlobalProvidersConfig::default(),
            imap: Some(ImapConfig::default()),
            gmail_oauth: Some(GmailOauthConfig::default()),
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
    pub trigger: TriggerConfig,
    pub providers: ProjectProvidersConfig,
    pub papers: Vec<PaperConfig>,
    pub paper_watch: BTreeMap<String, bool>,
    pub paper_tag_triggers: BTreeMap<String, String>,
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
        if self.trigger.pdf.max_scan_papers == 0 {
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
    pub papers: Vec<PaperConfig>,
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
        }
    }

    pub fn project_config(&self) -> ProjectConfigFile {
        ProjectConfigFile {
            project_id: String::new(),
            trigger: self.trigger.clone(),
            providers: ProjectProvidersConfig {
                stanford: ProjectStanfordProviderConfig {
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
    pub venue: Option<String>,
}

impl Default for ProjectStanfordProviderConfig {
    fn default() -> Self {
        Self {
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

#[cfg(test)]
mod tests {
    use super::{
        Config, GlobalConfigFile, LegacyConfig, ProjectConfigFile, default_project_config_path,
        find_git_root,
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
}

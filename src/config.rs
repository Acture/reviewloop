use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub config: Config,
    pub layers: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub core: CoreConfig,
    pub logging: LoggingConfig,
    pub polling: PollingConfig,
    pub retention: RetentionConfig,
    pub trigger: TriggerConfig,
    pub providers: ProvidersConfig,
    pub papers: Vec<PaperConfig>,
    pub imap: Option<ImapConfig>,
    pub gmail_oauth: Option<GmailOauthConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            core: CoreConfig::default(),
            logging: LoggingConfig::default(),
            polling: PollingConfig::default(),
            retention: RetentionConfig::default(),
            trigger: TriggerConfig::default(),
            providers: ProvidersConfig::default(),
            papers: vec![PaperConfig {
                id: "main".to_string(),
                pdf_path: "paper/main.pdf".to_string(),
                backend: "stanford".to_string(),
            }],
            imap: Some(ImapConfig::default()),
            gmail_oauth: Some(GmailOauthConfig::default()),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        Self::load_from_paths(&[path.to_path_buf()])
    }

    pub fn load_layered(explicit_path: Option<&Path>) -> Result<Self> {
        Ok(Self::load_layered_with_metadata(explicit_path)?.config)
    }

    pub fn load_layered_with_metadata(explicit_path: Option<&Path>) -> Result<LoadedConfig> {
        let layers = resolve_layered_paths(explicit_path)?;
        let config = Self::load_from_paths(&layers)?;
        Ok(LoadedConfig { config, layers })
    }

    pub fn global_config_path() -> Option<PathBuf> {
        default_global_config_path()
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

    pub fn validate(&self) -> Result<()> {
        if self.core.db_path.trim().is_empty() {
            return Err(anyhow!("core.db_path must not be empty"));
        }
        if self.core.max_concurrency == 0 {
            return Err(anyhow!("core.max_concurrency must be >= 1"));
        }
        if self.core.max_submissions_per_tick == 0 {
            return Err(anyhow!("core.max_submissions_per_tick must be >= 1"));
        }
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
        if self.papers.is_empty() {
            return Err(anyhow!("papers[] must contain at least one paper"));
        }
        Ok(())
    }

    pub fn save_template(path: &Path) -> Result<()> {
        let content = toml::to_string_pretty(&Config::default())?;
        fs::write(path, content)
            .with_context(|| format!("failed to write config template: {}", path.display()))
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

    fn load_from_paths(paths: &[PathBuf]) -> Result<Self> {
        if paths.is_empty() {
            return Err(anyhow!("no config paths provided"));
        }

        let mut merged = toml::Value::Table(toml::map::Map::new());
        for path in paths {
            let raw = fs::read_to_string(path)
                .with_context(|| format!("failed to read config: {}", path.display()))?;
            let parsed: toml::Value = toml::from_str(&raw)
                .with_context(|| format!("failed to parse TOML config: {}", path.display()))?;
            if !parsed.is_table() {
                return Err(anyhow!(
                    "config root must be a TOML table: {}",
                    path.display()
                ));
            }
            merge_toml_values(&mut merged, parsed);
        }

        let cfg: Config = merged
            .try_into()
            .context("failed to deserialize merged config")?;
        cfg.validate()?;
        Ok(cfg)
    }
}

fn resolve_layered_paths(explicit_path: Option<&Path>) -> Result<Vec<PathBuf>> {
    let mut layers = Vec::new();
    let mut looked = Vec::new();

    if let Some(global) = Config::global_config_path() {
        push_unique(&mut looked, global.clone());
        if global.exists() {
            push_unique(&mut layers, global);
        }
    }

    let local = PathBuf::from("reviewloop.toml");
    push_unique(&mut looked, local.clone());
    if local.exists() {
        push_unique(&mut layers, local);
    }

    if let Some(path) = explicit_path {
        let explicit = path.to_path_buf();
        push_unique(&mut looked, explicit.clone());
        if !explicit.exists() {
            return Err(anyhow!("config file not found: {}", explicit.display()));
        }
        push_unique(&mut layers, explicit);
    }

    if layers.is_empty() {
        let looked_text = looked
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(anyhow!(
            "no config file found (looked for: {}). run `reviewloop init` or pass --config <path>",
            looked_text
        ));
    }

    Ok(layers)
}

fn default_global_config_path() -> Option<PathBuf> {
    if let Some(xdg) = env::var_os("XDG_CONFIG_HOME") {
        return Some(
            PathBuf::from(xdg)
                .join("reviewloop")
                .join("reviewloop.toml"),
        );
    }

    #[cfg(windows)]
    {
        if let Some(appdata) = env::var_os("APPDATA") {
            return Some(
                PathBuf::from(appdata)
                    .join("reviewloop")
                    .join("reviewloop.toml"),
            );
        }
    }

    env::var_os("HOME").map(|home| {
        PathBuf::from(home)
            .join(".config")
            .join("reviewloop")
            .join("reviewloop.toml")
    })
}

fn default_global_data_dir() -> Option<PathBuf> {
    if let Some(xdg) = env::var_os("XDG_STATE_HOME") {
        return Some(PathBuf::from(xdg).join("reviewloop"));
    }

    #[cfg(windows)]
    {
        if let Some(local_app_data) = env::var_os("LOCALAPPDATA") {
            return Some(PathBuf::from(local_app_data).join("reviewloop"));
        }
    }

    env::var_os("HOME").map(|home| {
        PathBuf::from(home)
            .join(".local")
            .join("state")
            .join("reviewloop")
    })
}

fn default_db_path() -> String {
    let base = default_global_data_dir().unwrap_or_else(|| PathBuf::from(".reviewloop"));
    base.join("reviewloop.db").to_string_lossy().to_string()
}

fn push_unique(paths: &mut Vec<PathBuf>, candidate: PathBuf) {
    if !paths.iter().any(|p| p == &candidate) {
        paths.push(candidate);
    }
}

fn merge_toml_values(base: &mut toml::Value, overlay: toml::Value) {
    match (base, overlay) {
        (toml::Value::Table(base_table), toml::Value::Table(overlay_table)) => {
            for (key, value) in overlay_table {
                if let Some(base_value) = base_table.get_mut(&key) {
                    merge_toml_values(base_value, value);
                } else {
                    base_table.insert(key, value);
                }
            }
        }
        (base_slot, overlay_value) => {
            *base_slot = overlay_value;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
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
            state_dir: ".reviewloop".to_string(),
            db_path: default_db_path(),
            max_concurrency: 2,
            max_submissions_per_tick: 1,
            review_timeout_hours: 48,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
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
            file_path: Some(".reviewloop/reviewloop.log".to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
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
#[serde(default)]
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
#[serde(default)]
pub struct TriggerConfig {
    pub git: GitTriggerConfig,
    pub pdf: PdfTriggerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
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
#[serde(default)]
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
            email: "".to_string(),
            venue: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperConfig {
    pub id: String,
    pub pdf_path: String,
    pub backend: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
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
            username: "".to_string(),
            password: "".to_string(),
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
#[serde(default)]
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
            enabled: false,
            client_id: "".to_string(),
            client_secret: "".to_string(),
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
    use super::Config;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn defaults_start_polling_at_ten_minutes() {
        let cfg = Config::default();
        assert_eq!(cfg.polling.schedule_minutes, vec![10, 20, 40, 60]);
        assert_eq!(cfg.trigger.git.repo_dir, ".");
        assert_eq!(cfg.core.max_submissions_per_tick, 1);
        assert!(!cfg.core.db_path.trim().is_empty());
        assert!(cfg.retention.enabled);
        assert_eq!(cfg.retention.prune_every_ticks, 20);
        assert_eq!(cfg.trigger.pdf.max_scan_papers, 10);
        assert!(!cfg.trigger.git.auto_create_tags_on_pdf_change);
        assert!(!cfg.trigger.git.auto_delete_processed_tags);
        assert_eq!(cfg.logging.output, "stdout");
    }

    #[test]
    fn default_imap_has_stanford_pattern() {
        let cfg = Config::default();
        let imap = cfg.imap.expect("imap config should exist by default");
        assert!(imap.backend_patterns.contains_key("stanford"));
        assert!(imap.backend_header_patterns.contains_key("stanford"));
        assert!(imap.header_first);
        assert_eq!(imap.max_lookback_hours, 72);
        assert_eq!(imap.max_messages_per_poll, 50);
    }

    #[test]
    fn default_gmail_oauth_has_stanford_pattern() {
        let cfg = Config::default();
        let gmail = cfg
            .gmail_oauth
            .expect("gmail_oauth config should exist by default");
        assert!(gmail.backend_patterns.contains_key("stanford"));
        assert!(gmail.backend_header_patterns.contains_key("stanford"));
        assert!(gmail.header_first);
        assert_eq!(gmail.max_lookback_hours, 72);
        assert_eq!(gmail.max_messages_per_poll, 50);
        assert!(!gmail.enabled);
    }

    #[test]
    fn validate_rejects_zero_concurrency() {
        let mut cfg = Config::default();
        cfg.core.max_concurrency = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_poll_schedule() {
        let mut cfg = Config::default();
        cfg.polling.schedule_minutes.clear();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_submissions_per_tick() {
        let mut cfg = Config::default();
        cfg.core.max_submissions_per_tick = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_db_path() {
        let mut cfg = Config::default();
        cfg.core.db_path = " ".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_pdf_scan_limit() {
        let mut cfg = Config::default();
        cfg.trigger.pdf.max_scan_papers = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_retention_tick_interval() {
        let mut cfg = Config::default();
        cfg.retention.prune_every_ticks = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_invalid_logging_output() {
        let mut cfg = Config::default();
        cfg.logging.output = "syslog".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_requires_file_path_for_file_output() {
        let mut cfg = Config::default();
        cfg.logging.output = "file".to_string();
        cfg.logging.file_path = Some("".to_string());
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_imap_max_messages_per_poll() {
        let mut cfg = Config::default();
        if let Some(imap) = cfg.imap.as_mut() {
            imap.max_messages_per_poll = 0;
        }
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_gmail_oauth_max_messages_per_poll() {
        let mut cfg = Config::default();
        if let Some(gmail) = cfg.gmail_oauth.as_mut() {
            gmail.max_messages_per_poll = 0;
        }
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn layered_merge_respects_precedence_and_nested_fields() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let global = tmp.path().join("global.toml");
        let local = tmp.path().join("local.toml");
        let override_cfg = tmp.path().join("override.toml");

        fs::write(
            &global,
            r#"
[core]
max_concurrency = 1

[providers.stanford]
email = "global@example.edu"
venue = "acl2026"

[trigger.git]
enabled = true
repo_dir = "/global/repo"
"#,
        )
        .expect("failed to write global config");

        fs::write(
            &local,
            r#"
[core]
max_concurrency = 2

[trigger.git]
repo_dir = "/local/repo"
"#,
        )
        .expect("failed to write local config");

        fs::write(
            &override_cfg,
            r#"
[core]
max_concurrency = 3

[providers.stanford]
email = "override@example.edu"
"#,
        )
        .expect("failed to write override config");

        let cfg = Config::load_from_paths(&[global, local, override_cfg])
            .expect("failed to load layered config");

        assert_eq!(cfg.core.max_concurrency, 3);
        assert_eq!(cfg.providers.stanford.email, "override@example.edu");
        assert_eq!(cfg.providers.stanford.venue.as_deref(), Some("acl2026"));
        assert!(cfg.trigger.git.enabled);
        assert_eq!(cfg.trigger.git.repo_dir, "/local/repo");
    }

    #[test]
    fn load_from_paths_requires_at_least_one_path() {
        assert!(Config::load_from_paths(&[]).is_err());
    }
}

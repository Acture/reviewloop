use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub core: CoreConfig,
    pub polling: PollingConfig,
    pub trigger: TriggerConfig,
    pub providers: ProvidersConfig,
    pub papers: Vec<PaperConfig>,
    pub imap: Option<ImapConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            core: CoreConfig::default(),
            polling: PollingConfig::default(),
            trigger: TriggerConfig::default(),
            providers: ProvidersConfig::default(),
            papers: vec![PaperConfig {
                id: "main".to_string(),
                pdf_path: "paper/main.pdf".to_string(),
                backend: "stanford".to_string(),
            }],
            imap: Some(ImapConfig::default()),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config: {}", path.display()))?;
        let cfg: Config = toml::from_str(&raw)
            .with_context(|| format!("failed to parse TOML config: {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.core.max_concurrency == 0 {
            return Err(anyhow!("core.max_concurrency must be >= 1"));
        }
        if self.polling.schedule_minutes.is_empty() {
            return Err(anyhow!("polling.schedule_minutes cannot be empty"));
        }
        if self.providers.stanford.email.trim().is_empty() {
            return Err(anyhow!(
                "providers.stanford.email is required for stanford backend"
            ));
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

    pub fn find_paper(&self, paper_id: &str) -> Option<&PaperConfig> {
        self.papers.iter().find(|p| p.id == paper_id)
    }

    pub fn first_paper_for_backend(&self, backend: &str) -> Option<&PaperConfig> {
        self.papers.iter().find(|p| p.backend == backend)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CoreConfig {
    pub state_dir: String,
    pub max_concurrency: usize,
    pub review_timeout_hours: u64,
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            state_dir: ".reviewloop".to_string(),
            max_concurrency: 2,
            review_timeout_hours: 48,
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
pub struct TriggerConfig {
    pub git: GitTriggerConfig,
    pub pdf: PdfTriggerConfig,
}

impl Default for TriggerConfig {
    fn default() -> Self {
        Self {
            git: GitTriggerConfig::default(),
            pdf: PdfTriggerConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GitTriggerConfig {
    pub enabled: bool,
    pub tag_pattern: String,
}

impl Default for GitTriggerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tag_pattern: "review-<backend>/<paper-id>/*".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PdfTriggerConfig {
    pub enabled: bool,
    pub auto_submit_on_change: bool,
}

impl Default for PdfTriggerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            auto_submit_on_change: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProvidersConfig {
    pub stanford: StanfordProviderConfig,
}

impl Default for ProvidersConfig {
    fn default() -> Self {
        Self {
            stanford: StanfordProviderConfig::default(),
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
            email: "your.email@example.edu".to_string(),
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
    pub backend_patterns: BTreeMap<String, String>,
}

impl Default for ImapConfig {
    fn default() -> Self {
        let mut backend_patterns = BTreeMap::new();
        backend_patterns.insert(
            "stanford".to_string(),
            r"https?://paperreview\.ai/review\?token=([A-Za-z0-9_-]+)".to_string(),
        );

        Self {
            enabled: false,
            server: "imap.gmail.com".to_string(),
            port: 993,
            username: "".to_string(),
            password: "".to_string(),
            folder: "INBOX".to_string(),
            poll_seconds: 300,
            mark_seen: true,
            backend_patterns,
        }
    }
}

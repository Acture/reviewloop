use crate::config::Config;
use anyhow::{Context, Result, anyhow};
use std::{path::PathBuf, sync::OnceLock};
use tracing::dispatcher;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::LevelFilter;

static LOGGING_INITIALIZED: OnceLock<()> = OnceLock::new();
static FILE_GUARD: OnceLock<WorkerGuard> = OnceLock::new();

pub fn init_logging(config: &Config, force_stderr: bool) -> Result<()> {
    if LOGGING_INITIALIZED.get().is_some() || dispatcher::has_been_set() {
        return Ok(());
    }

    let level: LevelFilter = config
        .logging
        .level
        .parse()
        .map_err(|_| anyhow!("invalid logging.level: {}", config.logging.level))?;

    let output = if force_stderr {
        "stderr".to_string()
    } else {
        config.logging.output.clone()
    };

    match output.as_str() {
        "stdout" => {
            tracing_subscriber::fmt()
                .with_max_level(level)
                .with_target(true)
                .with_writer(std::io::stdout)
                .try_init()
                .map_err(|e| anyhow!(e.to_string()))?;
        }
        "stderr" => {
            tracing_subscriber::fmt()
                .with_max_level(level)
                .with_target(true)
                .with_writer(std::io::stderr)
                .try_init()
                .map_err(|e| anyhow!(e.to_string()))?;
        }
        "file" => {
            let path = PathBuf::from(config.logging.file_path.clone().unwrap_or_else(|| {
                config
                    .state_dir()
                    .join("reviewloop.log")
                    .to_string_lossy()
                    .to_string()
            }));
            let parent = path
                .parent()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."));
            let filename = path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| anyhow!("invalid logging.file_path: {}", path.display()))?
                .to_string();

            std::fs::create_dir_all(&parent)
                .with_context(|| format!("failed to create log directory: {}", parent.display()))?;

            let appender = tracing_appender::rolling::never(parent, filename);
            let (non_blocking, guard) = tracing_appender::non_blocking(appender);

            tracing_subscriber::fmt()
                .with_max_level(level)
                .with_target(true)
                .with_ansi(false)
                .with_writer(non_blocking)
                .try_init()
                .map_err(|e| anyhow!(e.to_string()))?;

            let _ = FILE_GUARD.set(guard);
        }
        other => {
            return Err(anyhow!(
                "logging.output must be stdout | stderr | file, got: {other}"
            ));
        }
    }

    let _ = LOGGING_INITIALIZED.set(());
    Ok(())
}

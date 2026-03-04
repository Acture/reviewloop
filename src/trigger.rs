use crate::{
    config::{Config, PaperConfig},
    db::Db,
    model::{JobStatus, NewJob},
    util::sha256_file,
};
use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::json;
use std::{path::Path, process::Command};
use tracing::warn;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedTag {
    pub backend: String,
    pub paper_id: Option<String>,
}

pub fn parse_review_tag(tag: &str) -> Option<ParsedTag> {
    // Canonical: review-<backend>/<paper-id>/<anything>
    // Shorthand: review-<backend>/<anything>
    if !tag.starts_with("review-") {
        return None;
    }

    let body = tag.trim_start_matches("review-");
    let parts: Vec<&str> = body.split('/').collect();
    if parts.len() < 2 {
        return None;
    }

    let backend = parts[0].trim();
    if backend.is_empty() {
        return None;
    }

    let paper_id = if parts.len() >= 3 {
        Some(parts[1].trim().to_string())
    } else {
        None
    };

    Some(ParsedTag {
        backend: backend.to_string(),
        paper_id,
    })
}

pub fn run_git_tag_trigger(config: &Config, db: &Db) -> Result<()> {
    if !config.trigger.git.enabled {
        return Ok(());
    }
    let repo_dir = config.trigger.git.repo_dir.trim();
    let repo_dir = if repo_dir.is_empty() { "." } else { repo_dir };

    let output = Command::new("git")
        .args(["-C", repo_dir, "tag", "--list", "review-*"])
        .output()
        .with_context(|| format!("failed to list git tags in repo_dir={repo_dir}"))?;

    if !output.status.success() {
        return Ok(());
    }

    let tags = String::from_utf8_lossy(&output.stdout);
    for tag in tags.lines().map(str::trim).filter(|v| !v.is_empty()) {
        let commit = resolve_tag_commit(repo_dir, tag).unwrap_or_else(|| "unknown".to_string());
        let processed = process_tag_entry(config, db, tag, &commit)?;
        if processed && config.trigger.git.auto_delete_processed_tags {
            if let Err(err) = delete_local_tag(repo_dir, tag) {
                warn!(tag, error = %err, "failed to auto-delete processed git tag");
            }
        }
    }

    Ok(())
}

pub fn run_pdf_trigger(config: &Config, db: &Db) -> Result<()> {
    if !config.trigger.pdf.enabled {
        return Ok(());
    }

    for paper in config
        .papers
        .iter()
        .take(config.trigger.pdf.max_scan_papers)
    {
        let path = Path::new(&paper.pdf_path);
        if !path.exists() {
            continue;
        }

        let hash = sha256_file(path)?;
        if db.has_duplicate_guard(&paper.backend, &hash)? {
            continue;
        }

        let latest_hash = db.latest_hash_for_paper(&paper.id, &paper.backend)?;
        if latest_hash.as_deref() == Some(hash.as_str()) {
            continue;
        }

        let status = if config.trigger.pdf.auto_submit_on_change {
            JobStatus::Queued
        } else {
            JobStatus::PendingApproval
        };

        let (auto_tag, auto_commit) = match maybe_create_auto_tag(config, paper) {
            Ok(v) => v.unwrap_or((None, None)),
            Err(err) => {
                warn!(
                    paper_id = %paper.id,
                    backend = %paper.backend,
                    error = %err,
                    "failed to create auto git tag; continuing without git tag metadata"
                );
                (None, None)
            }
        };

        enqueue_for_paper(
            config,
            db,
            paper,
            status,
            auto_tag,
            auto_commit,
            "pdf_change_trigger",
        )?;
    }

    Ok(())
}

fn select_paper<'a>(config: &'a Config, parsed: &ParsedTag) -> Option<&'a PaperConfig> {
    if let Some(paper_id) = &parsed.paper_id {
        if let Some(paper) = config.find_paper(paper_id) {
            if paper.backend == parsed.backend {
                return Some(paper);
            }
        }
    }

    config.first_paper_for_backend(&parsed.backend)
}

fn resolve_tag_commit(repo_dir: &str, tag: &str) -> Option<String> {
    let output = Command::new("git")
        .args(["-C", repo_dir, "rev-list", "-n", "1", tag])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let commit = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if commit.is_empty() {
        None
    } else {
        Some(commit)
    }
}

fn process_tag_entry(config: &Config, db: &Db, tag: &str, commit: &str) -> Result<bool> {
    if db.is_tag_seen(tag)? {
        return Ok(false);
    }

    if let Some(parsed) = parse_review_tag(tag) {
        if let Some(paper) = select_paper(config, &parsed) {
            enqueue_for_paper(
                config,
                db,
                paper,
                JobStatus::Queued,
                Some(tag.to_string()),
                Some(commit.to_string()),
                "git_tag_trigger",
            )?;
        }
    }

    db.mark_tag_seen(tag, commit)?;
    Ok(true)
}

fn maybe_create_auto_tag(
    config: &Config,
    paper: &PaperConfig,
) -> Result<Option<(Option<String>, Option<String>)>> {
    if !config.trigger.git.auto_create_tags_on_pdf_change {
        return Ok(None);
    }

    let repo_dir = config.trigger.git.repo_dir.trim();
    let repo_dir = if repo_dir.is_empty() { "." } else { repo_dir };
    let tag = format!(
        "review-{}/{}/auto-{}",
        paper.backend,
        paper.id,
        Utc::now().timestamp_millis()
    );

    let output = Command::new("git")
        .args(["-C", repo_dir, "tag", &tag])
        .output()
        .with_context(|| format!("failed to create auto git tag: {tag}"))?;

    if !output.status.success() {
        anyhow::bail!(
            "auto git tag command failed for {tag}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let commit = resolve_tag_commit(repo_dir, &tag);
    Ok(Some((Some(tag), commit)))
}

fn delete_local_tag(repo_dir: &str, tag: &str) -> Result<()> {
    let output = Command::new("git")
        .args(["-C", repo_dir, "tag", "-d", tag])
        .output()
        .with_context(|| format!("failed to run git tag -d for tag={tag}"))?;
    if !output.status.success() {
        anyhow::bail!(
            "git tag -d failed for {tag}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

fn enqueue_for_paper(
    config: &Config,
    db: &Db,
    paper: &PaperConfig,
    status: JobStatus,
    git_tag: Option<String>,
    git_commit: Option<String>,
    source: &str,
) -> Result<()> {
    let pdf_hash = sha256_file(Path::new(&paper.pdf_path))?;

    if db.has_duplicate_guard(&paper.backend, &pdf_hash)? {
        return Ok(());
    }

    let job = db.create_job(&NewJob {
        paper_id: paper.id.clone(),
        backend: paper.backend.clone(),
        pdf_path: paper.pdf_path.clone(),
        pdf_hash,
        status,
        email: provider_email(config, &paper.backend),
        venue: provider_venue(config, &paper.backend),
        git_tag,
        git_commit,
        next_poll_at: None,
    })?;

    db.add_event(
        Some(&job.id),
        "job_enqueued",
        json!({
            "source": source,
            "status": job.status.as_str(),
            "paper_id": job.paper_id,
            "backend": job.backend,
        }),
    )?;

    Ok(())
}

fn provider_email(config: &Config, backend: &str) -> String {
    match backend {
        "stanford" => config.providers.stanford.email.clone(),
        _ => String::new(),
    }
}

fn provider_venue(config: &Config, backend: &str) -> Option<String> {
    match backend {
        "stanford" => config.providers.stanford.venue.clone(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_review_tag, process_tag_entry};
    use crate::{
        config::{Config, PaperConfig},
        db::Db,
        model::JobStatus,
    };
    use anyhow::Context;
    use std::{fs, path::Path};

    fn setup_simulation_context() -> anyhow::Result<(tempfile::TempDir, Config, Db)> {
        let tmp = tempfile::tempdir()?;
        let state_dir = tmp.path().join("state");
        fs::create_dir_all(&state_dir)?;

        let pdf_path = tmp.path().join("main.pdf");
        fs::write(&pdf_path, b"%PDF-1.4\n%%EOF\n")?;

        let mut config = Config::default();
        config.core.state_dir = state_dir.to_string_lossy().to_string();
        config.trigger.git.enabled = false;
        config.trigger.pdf.enabled = false;
        config.providers.stanford.email = "test@example.edu".to_string();
        config.papers = vec![PaperConfig {
            id: "main".to_string(),
            pdf_path: pdf_path.to_string_lossy().to_string(),
            backend: "stanford".to_string(),
        }];

        let db = Db::new(Path::new(&config.core.state_dir));
        db.init_schema()?;
        Ok((tmp, config, db))
    }

    #[test]
    fn parses_full_tag_format() {
        let parsed = parse_review_tag("review-stanford/main/v1").unwrap();
        assert_eq!(parsed.backend, "stanford");
        assert_eq!(parsed.paper_id.as_deref(), Some("main"));
    }

    #[test]
    fn parses_shorthand_tag_format() {
        let parsed = parse_review_tag("review-stanford/v2").unwrap();
        assert_eq!(parsed.backend, "stanford");
        assert_eq!(parsed.paper_id, None);
    }

    #[test]
    fn rejects_non_review_tag() {
        assert!(parse_review_tag("v1.2.3").is_none());
    }

    #[test]
    fn simulated_tag_entry_enqueues_once_and_deduplicates() -> anyhow::Result<()> {
        let (_tmp, config, db) = setup_simulation_context()?;
        let tag = "review-stanford/main/sim";

        let processed = process_tag_entry(&config, &db, tag, "deadbeef")?;
        assert!(processed);

        let job = db
            .find_latest_open_job_for_paper("main")?
            .context("expected queued job for simulated tag")?;
        assert_eq!(job.status, JobStatus::Queued);
        assert_eq!(job.git_tag.as_deref(), Some(tag));
        assert_eq!(job.git_commit.as_deref(), Some("deadbeef"));
        assert!(db.is_tag_seen(tag)?);

        let processed = process_tag_entry(&config, &db, tag, "deadbeef")?;
        assert!(!processed);
        let rows = db.list_status_views(Some("main"))?;
        assert_eq!(
            rows.len(),
            1,
            "simulated duplicate tag should not enqueue twice"
        );

        Ok(())
    }
}

use crate::{
    config::{Config, PaperConfig},
    db::Db,
    model::{JobStatus, NewJob},
    util::sha256_file,
};
use anyhow::{Context, Result};
use serde_json::json;
use std::{path::Path, process::Command};

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

    let output = Command::new("git")
        .args(["tag", "--list", "review-*"])
        .output()
        .context("failed to list git tags")?;

    if !output.status.success() {
        return Ok(());
    }

    let tags = String::from_utf8_lossy(&output.stdout);
    for tag in tags.lines().map(str::trim).filter(|v| !v.is_empty()) {
        if db.is_tag_seen(tag)? {
            continue;
        }

        let commit = resolve_tag_commit(tag).unwrap_or_else(|| "unknown".to_string());
        if let Some(parsed) = parse_review_tag(tag) {
            if let Some(paper) = select_paper(config, &parsed) {
                enqueue_for_paper(
                    config,
                    db,
                    paper,
                    JobStatus::Queued,
                    Some(tag.to_string()),
                    Some(commit.clone()),
                    "git_tag_trigger",
                )?;
            }
        }

        db.mark_tag_seen(tag, &commit)?;
    }

    Ok(())
}

pub fn run_pdf_trigger(config: &Config, db: &Db) -> Result<()> {
    if !config.trigger.pdf.enabled {
        return Ok(());
    }

    for paper in &config.papers {
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

        enqueue_for_paper(config, db, paper, status, None, None, "pdf_change_trigger")?;
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

fn resolve_tag_commit(tag: &str) -> Option<String> {
    let output = Command::new("git")
        .args(["rev-list", "-n", "1", tag])
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
    use super::parse_review_tag;

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
}

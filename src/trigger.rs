use crate::{
    config::{Config, PaperConfig},
    db::Db,
    email_account::resolve_submission_email,
    model::{Job, JobStatus, NewJob},
    util::sha256_file,
};
use anyhow::{Context, Result};
use chrono::Utc;
use regex::Regex;
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
        if processed
            && config.trigger.git.auto_delete_processed_tags
            && let Err(err) = delete_local_tag(repo_dir, tag)
        {
            warn!(tag, error = %err, "failed to auto-delete processed git tag");
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
        .filter(|paper| config.is_paper_watched(&paper.id))
        .take(config.trigger.pdf.max_scan_papers)
    {
        let path = Path::new(&paper.pdf_path);
        if !path.exists() {
            continue;
        }

        let hash = sha256_file(path)?;
        let (version_source, version_key) = version_identity(None, &hash);
        if let Some(existing) = db.find_duplicate_covering_job(
            &config.project_id,
            &paper.id,
            &paper.backend,
            &hash,
            &version_key,
        )? {
            record_duplicate_skip(DuplicateSkipContext {
                config,
                db,
                paper,
                pdf_hash: &hash,
                version_source: &version_source,
                version_key: &version_key,
                existing: &existing,
                source: "pdf_trigger",
            })?;
            continue;
        }

        let latest_hash =
            db.latest_hash_for_paper(&config.project_id, &paper.id, &paper.backend)?;
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
    if let Some(paper_id) = &parsed.paper_id
        && let Some(paper) = config.find_paper(paper_id)
        && paper.backend == parsed.backend
    {
        return Some(paper);
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
    let scoped_tag = scoped_tag_name(&config.project_id, tag);
    if db.is_tag_seen(&scoped_tag)? {
        return Ok(false);
    }

    if let Some(paper) = select_paper_for_tag(config, tag) {
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

    db.mark_tag_seen(&scoped_tag, commit)?;
    Ok(true)
}

fn select_paper_for_tag<'a>(config: &'a Config, tag: &str) -> Option<&'a PaperConfig> {
    if let Some(parsed) = parse_review_tag(tag)
        && let Some(paper) = select_paper(config, &parsed)
    {
        return Some(paper);
    }

    config.papers.iter().find(|paper| {
        config
            .paper_tag_trigger(&paper.id)
            .is_some_and(|pattern| matches_tag_pattern(pattern, tag))
    })
}

fn matches_tag_pattern(pattern: &str, tag: &str) -> bool {
    let trimmed = pattern.trim();
    if trimmed.is_empty() {
        return false;
    }

    let mut regex_pattern = String::from("^");
    for part in trimmed.split('*') {
        regex_pattern.push_str(&regex::escape(part));
        regex_pattern.push_str(".*");
    }
    if !trimmed.ends_with('*') {
        regex_pattern.truncate(regex_pattern.len().saturating_sub(2));
    }
    regex_pattern.push('$');

    Regex::new(&regex_pattern)
        .map(|re| re.is_match(tag))
        .unwrap_or(false)
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
    let (version_source, version_key) = version_identity(git_commit.as_deref(), &pdf_hash);

    if let Some(existing) = db.find_duplicate_covering_job(
        &config.project_id,
        &paper.id,
        &paper.backend,
        &pdf_hash,
        &version_key,
    )? {
        record_duplicate_skip(DuplicateSkipContext {
            config,
            db,
            paper,
            pdf_hash: &pdf_hash,
            version_source: &version_source,
            version_key: &version_key,
            existing: &existing,
            source,
        })?;
        return Ok(());
    }

    let job = db.create_job(&NewJob {
        project_id: config.project_id.clone(),
        paper_id: paper.id.clone(),
        backend: paper.backend.clone(),
        pdf_path: paper.pdf_path.clone(),
        pdf_hash,
        status,
        email: provider_email(config, &paper.backend)?,
        venue: provider_venue(config, &paper.backend),
        git_tag,
        git_commit,
        next_poll_at: None,
    })?;

    db.add_event(
        None,
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

fn scoped_tag_name(project_id: &str, tag: &str) -> String {
    format!("{project_id}::{tag}")
}

fn version_identity(git_commit: Option<&str>, pdf_hash: &str) -> (String, String) {
    if let Some(commit) = git_commit.map(str::trim).filter(|value| !value.is_empty()) {
        ("git_commit".to_string(), commit.to_string())
    } else {
        ("pdf_hash".to_string(), pdf_hash.to_string())
    }
}

struct DuplicateSkipContext<'a> {
    config: &'a Config,
    db: &'a Db,
    paper: &'a PaperConfig,
    pdf_hash: &'a str,
    version_source: &'a str,
    version_key: &'a str,
    existing: &'a Job,
    source: &'a str,
}

fn record_duplicate_skip(ctx: DuplicateSkipContext<'_>) -> Result<()> {
    warn!(
        project_id = %ctx.config.project_id,
        paper_id = %ctx.paper.id,
        backend = %ctx.paper.backend,
        source = %ctx.source,
        existing_job_id = %ctx.existing.id,
        existing_status = %ctx.existing.status.as_str(),
        "skipped duplicate trigger enqueue"
    );
    ctx.db.add_event(
        Some(&ctx.config.project_id),
        None,
        "duplicate_skipped",
        json!({
            "project_id": ctx.config.project_id,
            "paper_id": ctx.paper.id,
            "backend": ctx.paper.backend,
            "pdf_hash": ctx.pdf_hash,
            "version_no": ctx.existing.version_no,
            "round_no": ctx.existing.round_no,
            "version_source": ctx.version_source,
            "version_key": ctx.version_key,
            "existing_job_id": ctx.existing.id,
            "existing_job_status": ctx.existing.status.as_str(),
            "source": ctx.source
        }),
    )?;
    Ok(())
}

fn provider_email(config: &Config, backend: &str) -> Result<String> {
    resolve_submission_email(config, backend, None)
}

fn provider_venue(config: &Config, backend: &str) -> Option<String> {
    match backend {
        "stanford" => Some(config.effective_stanford_venue()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{matches_tag_pattern, parse_review_tag, process_tag_entry, run_pdf_trigger};
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

        let mut config = Config {
            project_id: "project-main".to_string(),
            ..Config::default()
        };
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
            .find_latest_open_job_for_paper(&config.project_id, "main")?
            .context("expected queued job for simulated tag")?;
        assert_eq!(job.status, JobStatus::Queued);
        assert_eq!(job.git_tag.as_deref(), Some(tag));
        assert_eq!(job.git_commit.as_deref(), Some("deadbeef"));
        assert!(db.is_tag_seen(&format!("{}::{}", config.project_id, tag))?);

        let processed = process_tag_entry(&config, &db, tag, "deadbeef")?;
        assert!(!processed);
        let rows = db.list_status_views(&config.project_id, Some("main"))?;
        assert_eq!(
            rows.len(),
            1,
            "simulated duplicate tag should not enqueue twice"
        );

        Ok(())
    }

    #[test]
    fn simulated_custom_tag_trigger_enqueues_target_paper() -> anyhow::Result<()> {
        let (_tmp, mut config, db) = setup_simulation_context()?;
        config.set_paper_tag_trigger("main", Some("custom/main/*".to_string()));

        let processed = process_tag_entry(&config, &db, "custom/main/v3", "beadfeed")?;
        assert!(processed);

        let job = db
            .find_latest_open_job_for_paper(&config.project_id, "main")?
            .context("expected queued job for custom tag trigger")?;
        assert_eq!(job.status, JobStatus::Queued);
        assert_eq!(job.git_tag.as_deref(), Some("custom/main/v3"));
        assert_eq!(job.git_commit.as_deref(), Some("beadfeed"));
        Ok(())
    }

    #[test]
    fn pattern_match_supports_wildcard() {
        assert!(matches_tag_pattern(
            "review-stanford/main/*",
            "review-stanford/main/v1"
        ));
        assert!(matches_tag_pattern("custom-*", "custom-build-123"));
        assert!(!matches_tag_pattern(
            "review-stanford/main/*",
            "review-stanford/other/v1"
        ));
    }

    #[test]
    fn pdf_trigger_skips_unwatched_paper() -> anyhow::Result<()> {
        let (_tmp, mut config, db) = setup_simulation_context()?;
        config.trigger.pdf.enabled = true;
        config.set_paper_watch("main", false);

        run_pdf_trigger(&config, &db)?;
        assert!(
            db.list_status_views(&config.project_id, Some("main"))?
                .is_empty()
        );
        Ok(())
    }
}

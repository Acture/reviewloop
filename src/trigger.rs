use crate::{
    config::{Config, PaperConfig},
    db::Db,
    email_account::resolve_submission_email,
    model::{
        EnqueueConflict, EnqueueMode, EnqueueOutcome, EnqueueRequest, Job, JobPdf, JobStatus,
        NewJob, ReviewIdentity,
    },
    submission_input::prepare_input,
    util::{git_in, sha256_file},
};
use anyhow::{Context, Result};
use chrono::Utc;
use regex::Regex;
use serde_json::json;
use std::{
    collections::HashSet,
    path::Path,
    sync::{Mutex, OnceLock},
};
use tracing::{info, warn};

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

    let output = git_in(repo_dir)
        .args(["tag", "--list", "review-*"])
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

// Per-process set of paper IDs for which a missing-PDF warning has already
// been emitted.  We log the warning and write the `pdf_missing` event only
// once per paper per process lifetime (option b from the design notes) to
// avoid spamming the event table every 30-second tick.
//
// Trade-off: if the file reappears and then goes missing again without a
// daemon restart, the second disappearance will be silent.  Acceptable given
// the use-case (reorganised repo); a `daemon stop && daemon start` resets the
// set.
static PDF_MISSING_WARNED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

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
            let guard = PDF_MISSING_WARNED.get_or_init(|| Mutex::new(HashSet::new()));
            let mut seen = guard.lock().unwrap_or_else(|e| e.into_inner());
            if seen.insert(paper.id.clone()) {
                tracing::warn!(
                    paper_id = %paper.id,
                    path = %paper.pdf_path,
                    "configured PDF not found; skipping until file appears"
                );
                db.add_event(
                    Some(&config.project_id),
                    None,
                    "pdf_missing",
                    json!({"paper_id": paper.id, "path": paper.pdf_path}),
                )?;
            }
            continue;
        }

        let hash = sha256_file(path)?;
        // Content an earlier job without git metadata covers. Checked before
        // the auto tag below; enqueue repeats the check atomically. This
        // records a duplicate_skipped event on every tick while the file stays
        // covered, and daemon health currently reads those events as its tick
        // heartbeat, so the skip must stay ahead of the unchanged-file check.
        let identity = ReviewIdentity::new(
            &paper.id,
            &paper.backend,
            &hash,
            provider_venue(config, paper).as_deref(),
            &config.review_options_for(paper),
            None,
        );
        if let Some(existing) = db.find_duplicate_covering_job(&config.project_id, &identity)? {
            warn_duplicate(&config.project_id, &existing, "pdf_change_trigger");
            db.record_duplicate_skip(
                &config.project_id,
                &identity,
                &existing,
                "pdf_change_trigger",
            )?;
            continue;
        }

        // Unchanged since the paper's latest job, whatever became of it: a
        // failed job is not resubmitted just because the file is still there.
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

        enqueue_trigger_request(
            db,
            EnqueueRequest {
                job: new_trigger_job(config, paper, status, auto_tag, auto_commit)?,
                request_key: None,
                mode: EnqueueMode::Deduplicate,
                source: "pdf_change_trigger".to_string(),
            },
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
    let output = git_in(repo_dir)
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

fn process_tag_entry(config: &Config, db: &Db, tag: &str, commit: &str) -> Result<bool> {
    let scoped_tag = scoped_tag_name(&config.project_id, tag);
    if db.is_tag_seen(&scoped_tag)? {
        return Ok(false);
    }

    if let Some(paper) = select_paper_for_tag(config, tag) {
        // The tag is the request: if marking it seen below is lost (crash,
        // retention pruning), replaying it returns the job it already made,
        // as long as that job has not been pruned itself.
        let request = EnqueueRequest {
            job: new_trigger_job(
                config,
                paper,
                JobStatus::Queued,
                Some(tag.to_string()),
                Some(commit.to_string()),
            )?,
            request_key: Some(format!("git-tag:{tag}@{commit}")),
            mode: EnqueueMode::Deduplicate,
            source: "git_tag_trigger".to_string(),
        };
        if let Err(err) = enqueue_trigger_request(db, request) {
            // The tag already produced a job, but the manuscript or venue has
            // changed since. Treat it as processed instead of failing every tick.
            let conflict = err.downcast::<EnqueueConflict>()?;
            warn!(
                tag,
                existing_job_id = %conflict.existing_job_id,
                error = %conflict,
                "git tag was already enqueued with different content; not enqueueing again"
            );
            db.add_event(
                Some(&config.project_id),
                Some(&conflict.existing_job_id),
                "enqueue_conflict",
                json!({
                    "source": "git_tag_trigger",
                    "paper_id": paper.id,
                    "request_key": conflict.request_key,
                    "mismatches": conflict.mismatches,
                }),
            )?;
        }
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

    let output = git_in(repo_dir)
        .args(["tag", &tag])
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
    let output = git_in(repo_dir)
        .args(["tag", "-d", tag])
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

fn new_trigger_job(
    config: &Config,
    paper: &PaperConfig,
    status: JobStatus,
    git_tag: Option<String>,
    git_commit: Option<String>,
) -> Result<NewJob> {
    Ok(NewJob {
        project_id: config.project_id.clone(),
        paper_id: paper.id.clone(),
        backend: paper.backend.clone(),
        pdf: JobPdf::Pinned(prepare_input(
            &config.state_dir(),
            Path::new(&paper.pdf_path),
        )?),
        status,
        email: provider_email(config, &paper.backend)?,
        venue: provider_venue(config, paper),
        review_options: config.review_options_for(paper),
        git_tag,
        git_commit,
        next_poll_at: None,
    })
}

fn enqueue_trigger_request(db: &Db, request: EnqueueRequest) -> Result<()> {
    match db.enqueue(&request)? {
        EnqueueOutcome::Created(job) => info!(
            project_id = %job.project_id,
            paper_id = %job.paper_id,
            job_id = %job.id,
            source = %request.source,
            version_no = job.version_no,
            round_no = job.round_no,
            "trigger enqueued job"
        ),
        EnqueueOutcome::Existing { job, .. } => {
            warn_duplicate(&request.job.project_id, &job, &request.source)
        }
    }
    Ok(())
}

fn scoped_tag_name(project_id: &str, tag: &str) -> String {
    format!("{project_id}::{tag}")
}

fn warn_duplicate(project_id: &str, existing: &Job, source: &str) {
    warn!(
        project_id = %project_id,
        paper_id = %existing.paper_id,
        backend = %existing.backend,
        source = %source,
        existing_job_id = %existing.id,
        existing_status = %existing.status.as_str(),
        "skipped duplicate trigger enqueue"
    );
}

fn provider_email(config: &Config, backend: &str) -> Result<String> {
    resolve_submission_email(config, backend, None)
}

fn provider_venue(config: &Config, paper: &PaperConfig) -> Option<String> {
    config.venue_for(paper)
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
            venue: None,
        }];

        let db = Db::new(Path::new(&config.core.state_dir));
        db.ensure_schema()?;
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
        let snapshot = job.snapshot_path.as_deref().context("job must be pinned")?;
        assert_eq!(crate::util::sha256_file(Path::new(snapshot))?, job.pdf_hash);
        assert_eq!(fs::read(snapshot)?, fs::read(&config.papers[0].pdf_path)?);
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

    /// Losing the seen-tag record (crash before it is written, or retention
    /// pruning) must not turn a tag whose job failed into a second job.
    #[test]
    fn replayed_tag_returns_its_job_after_seen_record_is_lost() -> anyhow::Result<()> {
        let (_tmp, config, db) = setup_simulation_context()?;
        let tag = "review-stanford/main/replay";
        assert!(process_tag_entry(&config, &db, tag, "c0ffee")?);
        let job = db
            .find_latest_open_job_for_paper(&config.project_id, "main")?
            .context("expected job for tag")?;
        db.update_job_state_unchecked(&job.id, JobStatus::Failed, None, Some(None), None)?;
        forget_seen_tags(&db)?;

        assert!(process_tag_entry(&config, &db, tag, "c0ffee")?);

        let rows = db.list_status_views(&config.project_id, Some("main"))?;
        assert_eq!(rows.len(), 1, "replayed tag must not enqueue again");
        assert!(db.is_tag_seen(&format!("{}::{}", config.project_id, tag))?);
        Ok(())
    }

    /// A replayed tag whose manuscript has changed since it was enqueued is
    /// marked processed and reported, rather than failing every tick.
    #[test]
    fn replayed_tag_with_changed_manuscript_is_reported_not_enqueued() -> anyhow::Result<()> {
        let (_tmp, config, db) = setup_simulation_context()?;
        let tag = "review-stanford/main/changed";
        assert!(process_tag_entry(&config, &db, tag, "c0ffee")?);
        forget_seen_tags(&db)?;
        fs::write(&config.papers[0].pdf_path, b"%PDF-1.4\n% edited\n%%EOF\n")?;

        assert!(process_tag_entry(&config, &db, tag, "c0ffee")?);

        assert_eq!(
            db.list_status_views(&config.project_id, Some("main"))?
                .len(),
            1
        );
        let event = db
            .most_recent_event_of_type(&config.project_id, "enqueue_conflict")?
            .context("expected enqueue_conflict event")?;
        assert_eq!(event.payload["mismatches"][0]["field"], "pdf_hash");
        assert!(db.is_tag_seen(&format!("{}::{}", config.project_id, tag))?);
        Ok(())
    }

    /// An unchanged PDF is enqueued once, however many ticks see it.
    #[test]
    fn pdf_trigger_enqueues_unchanged_pdf_once() -> anyhow::Result<()> {
        let (_tmp, mut config, db) = setup_simulation_context()?;
        config.trigger.pdf.enabled = true;

        for _ in 0..3 {
            run_pdf_trigger(&config, &db)?;
        }

        assert_eq!(
            db.list_status_views(&config.project_id, Some("main"))?
                .len(),
            1
        );
        Ok(())
    }

    /// Reverting the PDF to content an earlier job still covers is skipped and
    /// recorded as a duplicate instead of enqueueing a second review.
    #[test]
    fn pdf_trigger_skips_reverted_pdf_covered_by_earlier_job() -> anyhow::Result<()> {
        let (_tmp, mut config, db) = setup_simulation_context()?;
        config.trigger.pdf.enabled = true;
        let pdf = config.papers[0].pdf_path.clone();
        let original = fs::read(&pdf)?;

        run_pdf_trigger(&config, &db)?;
        fs::write(&pdf, b"%PDF-1.4\n% revision 2\n%%EOF\n")?;
        run_pdf_trigger(&config, &db)?;
        fs::write(&pdf, original)?;
        run_pdf_trigger(&config, &db)?;

        assert_eq!(
            db.list_status_views(&config.project_id, Some("main"))?
                .len(),
            2
        );
        let skipped = db
            .most_recent_event_of_type(&config.project_id, "duplicate_skipped")?
            .context("expected duplicate_skipped event")?;
        assert_eq!(skipped.payload["source"], "pdf_change_trigger");
        Ok(())
    }

    fn forget_seen_tags(db: &Db) -> anyhow::Result<()> {
        let conn = rusqlite::Connection::open(&db.path)?;
        conn.execute("DELETE FROM seen_tags", [])?;
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

    /// U4 regression: a missing PDF must emit a `pdf_missing` event exactly
    /// once per paper per process lifetime, no matter how many ticks fire.
    #[test]
    fn pdf_trigger_emits_pdf_missing_event_once_for_missing_file() -> anyhow::Result<()> {
        // Use a project/paper ID that is unique to this test to avoid
        // colliding with the process-global PDF_MISSING_WARNED HashSet that
        // other tests might have already populated.
        let tmp = tempfile::tempdir()?;
        let state_dir = tmp.path().join("state");
        fs::create_dir_all(&state_dir)?;

        let mut config = Config {
            project_id: "proj-u4-pdf-missing".to_string(),
            ..Config::default()
        };
        config.core.state_dir = state_dir.to_string_lossy().to_string();
        config.trigger.pdf.enabled = true;
        config.providers.stanford.email = "test@example.edu".to_string();

        // Point to a path that will never exist.
        let missing = tmp.path().join("does-not-exist-u4.pdf");
        config.papers = vec![PaperConfig {
            id: "u4-missing-paper".to_string(),
            pdf_path: missing.to_string_lossy().to_string(),
            backend: "stanford".to_string(),
            venue: None,
        }];

        let db = Db::new(Path::new(&config.core.state_dir));
        db.ensure_schema()?;

        // Simulate three daemon ticks.
        run_pdf_trigger(&config, &db)?;
        run_pdf_trigger(&config, &db)?;
        run_pdf_trigger(&config, &db)?;

        // The `pdf_missing` event must have been recorded.
        let ev = db
            .most_recent_event_of_type(&config.project_id, "pdf_missing")?
            .expect("expected a pdf_missing event after ticks with missing file");
        assert_eq!(
            ev.payload.get("paper_id").and_then(|v| v.as_str()),
            Some("u4-missing-paper"),
        );

        // Only one event must have been written (HashSet dedup).
        let timeline = db.list_timeline_events(&config.project_id, "u4-missing-paper")?;
        let missing_events: Vec<_> = timeline
            .iter()
            .filter(|e| e.event_type == "pdf_missing")
            .collect();
        assert_eq!(
            missing_events.len(),
            1,
            "pdf_missing must be written exactly once per process lifetime per paper; got {}",
            missing_events.len()
        );

        Ok(())
    }
}

//! Widget state snapshot writer for macOS WidgetKit integration.
//!
//! Every supervisor tick writes one small JSON document for the whole
//! machine, which the Swift WidgetKit extension reads to render the
//! home-screen widget: every project's jobs, the supervisor's state, and
//! each registered project's health.
//!
//! The JSON schema is shared with the Swift side; add fields only, and do
//! **not** rename or retype existing ones without coordinating (see
//! `docs/widget-schema.md`).
//!
//! ## `completed_today` note
//! "Completed today" is defined as jobs whose `updated_at` falls on the
//! current **UTC** calendar date. Local-timezone date is acceptable for V1
//! (documented here so W2B can decide whether to call it out in the UI).

use crate::{
    db::Db,
    supervisor::{ProjectState, SupervisorState},
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::{collections::BTreeMap, fs, io::Write, path::Path};

// ---------------------------------------------------------------------------
// Tick-health thresholds (seconds).  Mirrored from `cmd_daemon_status` so
// both the CLI status output and the widget file always agree.
// ---------------------------------------------------------------------------

/// Ticks younger than this are considered healthy.
pub const TICK_HEALTH_NORMAL_SECS: i64 = 60;
/// Ticks between NORMAL and STUCK thresholds are "stale".
pub const TICK_HEALTH_STUCK_SECS: i64 = 300;

/// Compute tick-health label from the age of the last tick (in seconds).
/// Mirrors the logic in `cmd_daemon_status`.
pub fn tick_health_label(last_tick_at: Option<DateTime<Utc>>) -> &'static str {
    match last_tick_at {
        None => "unknown",
        Some(ts) => {
            let age = (Utc::now() - ts).num_seconds();
            if age < TICK_HEALTH_NORMAL_SECS {
                "normal"
            } else if age < TICK_HEALTH_STUCK_SECS {
                "stale"
            } else {
                "stuck"
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Schema structs — field names MUST match the frozen JSON contract.
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct WidgetState {
    pub schema_version: u32,
    pub generated_at: String,
    /// Always `""`: the document covers every project on the machine.
    pub project_id: String,
    pub summary: WidgetSummary,
    pub active_jobs: Vec<WidgetActiveJob>,
    pub recent_failures: Vec<WidgetFailure>,
    /// The supervisor's latest tick; never null in a written document.
    pub last_tick_at: String,
    pub last_tick_error: Option<WidgetTickError>,
    pub tick_health: &'static str,
    pub supervisor: WidgetSupervisor,
    pub projects: Vec<WidgetProject>,
}

#[derive(Debug, Serialize)]
pub struct WidgetSummary {
    pub active_count: usize,
    pub failed_recent_24h: usize,
    pub completed_today: usize,
}

#[derive(Debug, Serialize)]
pub struct WidgetActiveJob {
    pub project_id: String,
    pub paper_id: String,
    pub status: String,
    pub attempt: u32,
    pub next_poll_at: Option<String>,
    pub started_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct WidgetFailure {
    pub project_id: String,
    pub paper_id: String,
    pub status: String,
    pub last_error: String,
    pub occurred_at: String,
}

#[derive(Debug, Serialize)]
pub struct WidgetTickError {
    pub at: String,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct WidgetSupervisor {
    pub state: SupervisorState,
    pub pid: Option<u32>,
    pub started_at: Option<String>,
    pub paused_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct WidgetProject {
    pub project_id: String,
    pub enabled: bool,
    pub state: ProjectState,
    pub active_count: usize,
    pub last_run_at: Option<String>,
    pub last_ok_at: Option<String>,
    pub last_error: Option<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Truncate `s` to at most `max_chars` Unicode scalar values (char-boundary
/// safe). Mirrors `truncate_chars` used elsewhere in the codebase.
fn truncate_chars(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        Some((byte_idx, _)) => &s[..byte_idx],
        None => s,
    }
}

fn fmt_rfc3339(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

// ---------------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------------

/// The machine's document as of `now`, from the database alone.
pub fn build_fleet(db: &Db, now: DateTime<Utc>) -> Result<WidgetState> {
    let supervisor = db
        .supervisor_record()
        .context("failed to read the supervisor row")?;
    let last_tick_at = supervisor.last_tick_at.unwrap_or(now);
    let last_tick_error = supervisor
        .current_tick_error()
        .map(|message| WidgetTickError {
            at: fmt_rfc3339(last_tick_at),
            message: message.to_string(),
        });

    // --- active jobs (capped at 10, sorted by next_poll_at ASC, None first) ---
    let mut raw_active = db
        .list_active_jobs_all()
        .context("failed to read active jobs")?;
    raw_active.sort_by(|a, b| match (a.next_poll_at, b.next_poll_at) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(ta), Some(tb)) => ta.cmp(&tb),
    });
    let mut active_per_project: BTreeMap<&str, usize> = BTreeMap::new();
    for job in &raw_active {
        *active_per_project
            .entry(job.project_id.as_str())
            .or_default() += 1;
    }
    let active_jobs: Vec<WidgetActiveJob> = raw_active
        .iter()
        .take(10)
        .map(|j| WidgetActiveJob {
            project_id: j.project_id.clone(),
            paper_id: j.paper_id.clone(),
            status: j.status.as_str().to_string(),
            attempt: j.attempt,
            next_poll_at: j.next_poll_at.map(fmt_rfc3339),
            started_at: j.started_at.map(fmt_rfc3339),
        })
        .collect();

    // --- recent failures (newest 5 across projects, at most 5 per project) ---
    // The query already excludes cancellations.
    let mut raw_failures = db
        .list_failed_jobs_all_per_project(5)
        .context("failed to read failed jobs")?;
    raw_failures.sort_by_key(|job| std::cmp::Reverse(job.updated_at));
    let cutoff_24h = now - chrono::Duration::hours(24);
    let failed_recent_24h = raw_failures
        .iter()
        .filter(|j| j.updated_at >= cutoff_24h)
        .count();
    let recent_failures: Vec<WidgetFailure> = raw_failures
        .iter()
        .take(5)
        .map(|j| {
            let raw_err = j.last_error.as_deref().unwrap_or("(unknown error)");
            WidgetFailure {
                project_id: j.project_id.clone(),
                paper_id: j.paper_id.clone(),
                status: j.status.as_str().to_string(),
                last_error: truncate_chars(raw_err, 80).to_string(),
                occurred_at: fmt_rfc3339(j.updated_at),
            }
        })
        .collect();

    // completed_today: COMPLETED jobs whose updated_at is on today's UTC date.
    let completed_today = db
        .count_completed_on(&now.format("%Y-%m-%d").to_string())
        .context("failed to count completed-today jobs")?;

    let projects = db
        .list_registered_projects()
        .context("failed to read the project registry")?
        .into_iter()
        .map(|project| WidgetProject {
            enabled: project.enabled,
            state: ProjectState::of(&project),
            active_count: active_per_project
                .get(project.project_id.as_str())
                .copied()
                .unwrap_or_default(),
            last_run_at: project.health.last_run_at.map(fmt_rfc3339),
            last_ok_at: project.health.last_ok_at.map(fmt_rfc3339),
            last_error: project
                .health
                .last_error
                .as_deref()
                .map(|error| truncate_chars(error, 80).to_string()),
            project_id: project.project_id,
        })
        .collect();

    Ok(WidgetState {
        schema_version: 1,
        generated_at: fmt_rfc3339(now),
        project_id: String::new(),
        summary: WidgetSummary {
            active_count: raw_active.len(),
            failed_recent_24h,
            completed_today,
        },
        active_jobs,
        recent_failures,
        last_tick_at: fmt_rfc3339(last_tick_at),
        last_tick_error,
        tick_health: tick_health_label(Some(last_tick_at)),
        supervisor: WidgetSupervisor {
            state: SupervisorState::of(&supervisor, now),
            pid: supervisor.pid,
            started_at: supervisor.started_at.map(fmt_rfc3339),
            paused_at: supervisor.paused_at.map(fmt_rfc3339),
        },
        projects,
    })
}

/// Write `state` to `path` atomically via a `.tmp.<pid>` sibling + rename.
/// Mirrors the pattern used by `save_toml_file` in `config.rs`.
pub fn write_atomically(path: &Path, state: &WidgetState) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create widget state directory: {}",
                parent.display()
            )
        })?;
    }

    let content =
        serde_json::to_string_pretty(state).context("failed to serialize widget state")?;

    let tmp_name = format!(
        ".{}.tmp.{}",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("widget-state.json"),
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
                .with_context(|| {
                    format!(
                        "failed to create temp widget state file: {}",
                        tmp_path.display()
                    )
                })?
        };
        #[cfg(not(unix))]
        let mut f = std::fs::File::create(&tmp_path).with_context(|| {
            format!(
                "failed to create temp widget state file: {}",
                tmp_path.display()
            )
        })?;
        f.write_all(content.as_bytes()).with_context(|| {
            format!(
                "failed to write temp widget state file: {}",
                tmp_path.display()
            )
        })?;
        f.sync_all().with_context(|| {
            format!(
                "failed to fsync temp widget state file: {}",
                tmp_path.display()
            )
        })?;
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

/// Build the machine's document and write it atomically to `path`.
pub fn write_fleet(db: &Db, path: &Path, now: DateTime<Utc>) -> Result<()> {
    write_atomically(path, &build_fleet(db, now)?)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Config,
        db::Db,
        model::{JobPdf, JobStatus, NewJob},
    };
    use chrono::TimeZone;
    use std::path::Path;

    /// Create a fresh, schema-initialized in-memory DB for each test.
    /// Each test gets a unique name to avoid SQLite shared-cache collisions.
    fn make_db(name: &str) -> Db {
        let db = Db::new_in_memory(name).expect("new_in_memory");
        db.ensure_schema().expect("ensure_schema");
        db
    }

    /// Create a job in the given status with an optional last_error.
    /// Uses `create_job` (inserts as the requested status) then
    /// `update_job_state_unchecked` to set status + last_error.
    fn make_job(
        db: &Db,
        project_id: &str,
        paper_id: &str,
        status: JobStatus,
        last_error: Option<&str>,
    ) {
        let new_job = NewJob {
            project_id: project_id.to_string(),
            paper_id: paper_id.to_string(),
            backend: "test".to_string(),
            pdf: JobPdf::Unpinned {
                pdf_path: "/dev/null".to_string(),
                pdf_hash: "deadbeef".to_string(),
            },
            status: JobStatus::Queued,
            email: "t@example.com".to_string(),
            venue: None,
            review_options: Default::default(),
            git_tag: None,
            git_commit: None,
            next_poll_at: None,
        };
        let job = db.create_job(&new_job).expect("create_job");
        if status != JobStatus::Queued || last_error.is_some() {
            db.update_job_state_unchecked(
                &job.id,
                status,
                None,
                None,
                Some(last_error.map(str::to_string)),
            )
            .expect("update_job_state_unchecked");
        }
    }

    #[test]
    fn fleet_document_covers_every_project_and_the_supervisor() {
        let db = make_db("widget-fleet");
        let now = Utc::now();
        db.insert_project_registration("alpha", Path::new("/repos/alpha/reviewloop.toml"), now)
            .unwrap();
        db.enable_project(
            "alpha",
            Path::new("/repos/alpha/reviewloop.toml"),
            Some(Path::new("/repos/alpha/reviewloop.toml")),
            now,
        )
        .unwrap();
        db.record_project_health("alpha", now, Some("pdf trigger: boom"))
            .unwrap();
        db.insert_project_registration("beta", Path::new("/repos/beta/reviewloop.toml"), now)
            .unwrap();
        db.record_supervisor_start(7, Path::new("/state"), "0.0.0", now)
            .unwrap();
        db.record_supervisor_tick(7, now, Some("email token ingestion: offline"))
            .unwrap();

        make_job(&db, "alpha", "main", JobStatus::Processing, None);
        make_job(&db, "beta", "main", JobStatus::Queued, None);
        make_job(
            &db,
            "beta",
            "draft",
            JobStatus::Failed,
            Some("network timeout"),
        );
        // A cancellation is not a failure.
        make_job(
            &db,
            "beta",
            "old",
            JobStatus::Failed,
            Some("cancelled by user"),
        );
        make_job(&db, "alpha", "done", JobStatus::Completed, None);

        let state = build_fleet(&db, now).expect("build");
        assert_eq!(state.schema_version, 1);
        assert_eq!(state.project_id, "");
        assert_eq!(state.summary.active_count, 2);
        assert_eq!(state.summary.failed_recent_24h, 1);
        assert_eq!(state.summary.completed_today, 1);
        let active: Vec<(&str, &str)> = state
            .active_jobs
            .iter()
            .map(|job| (job.project_id.as_str(), job.paper_id.as_str()))
            .collect();
        assert!(active.contains(&("alpha", "main")) && active.contains(&("beta", "main")));
        assert_eq!(state.recent_failures.len(), 1);
        assert_eq!(state.recent_failures[0].project_id, "beta");

        assert_eq!(state.supervisor.state, SupervisorState::Running);
        assert_eq!(state.supervisor.pid, Some(7));
        assert_eq!(state.tick_health, "normal");
        assert_eq!(
            state
                .last_tick_error
                .as_ref()
                .map(|error| error.message.as_str()),
            Some("email token ingestion: offline")
        );

        let projects: Vec<(&str, bool, ProjectState, usize)> = state
            .projects
            .iter()
            .map(|p| (p.project_id.as_str(), p.enabled, p.state, p.active_count))
            .collect();
        assert_eq!(
            projects,
            [
                ("alpha", true, ProjectState::Error, 1),
                ("beta", false, ProjectState::Disabled, 1),
            ]
        );
        assert_eq!(
            state.projects[0].last_error.as_deref(),
            Some("pdf trigger: boom")
        );
    }

    /// The Swift widget decodes `last_tick_at` as a non-optional date, so a
    /// written document always carries one, even before the first tick.
    #[test]
    fn last_tick_at_is_never_null() {
        let db = make_db("widget-no-tick");
        let now = Utc.with_ymd_and_hms(2026, 10, 10, 8, 0, 0).unwrap();
        let state = build_fleet(&db, now).expect("build");
        assert_eq!(state.last_tick_at, "2026-10-10T08:00:00Z");
        assert_eq!(state.supervisor.state, SupervisorState::Stopped);
        let json = serde_json::to_value(&state).unwrap();
        assert!(json["last_tick_at"].is_string());
        assert_eq!(json["projects"], serde_json::json!([]));
    }

    #[test]
    fn widget_state_v1_serializes_to_documented_shape() {
        let at = |h, m, s| fmt_rfc3339(Utc.with_ymd_and_hms(2026, 5, 6, h, m, s).unwrap());
        let state = WidgetState {
            schema_version: 1,
            generated_at: at(12, 0, 0),
            project_id: String::new(),
            summary: WidgetSummary {
                active_count: 2,
                failed_recent_24h: 1,
                completed_today: 3,
            },
            active_jobs: vec![WidgetActiveJob {
                project_id: "thesis".to_string(),
                paper_id: "paper-a".to_string(),
                status: "PROCESSING".to_string(),
                attempt: 2,
                next_poll_at: Some(at(12, 5, 0)),
                started_at: Some(at(11, 50, 0)),
            }],
            recent_failures: vec![WidgetFailure {
                project_id: "thesis".to_string(),
                paper_id: "paper-b".to_string(),
                status: "FAILED".to_string(),
                last_error: "rate limit exceeded".to_string(),
                occurred_at: at(11, 55, 0),
            }],
            last_tick_at: at(11, 59, 50),
            last_tick_error: Some(WidgetTickError {
                at: at(11, 59, 50),
                message: "email token ingestion: offline".to_string(),
            }),
            tick_health: "normal",
            supervisor: WidgetSupervisor {
                state: SupervisorState::Running,
                pid: Some(4242),
                started_at: Some(at(9, 0, 0)),
                paused_at: None,
            },
            projects: vec![WidgetProject {
                project_id: "thesis".to_string(),
                enabled: true,
                state: ProjectState::Ok,
                active_count: 2,
                last_run_at: Some(at(11, 59, 50)),
                last_ok_at: Some(at(11, 59, 50)),
                last_error: None,
            }],
        };
        let json = serde_json::to_string_pretty(&state).expect("serialise");
        let expected = r#"{
  "schema_version": 1,
  "generated_at": "2026-05-06T12:00:00Z",
  "project_id": "",
  "summary": {
    "active_count": 2,
    "failed_recent_24h": 1,
    "completed_today": 3
  },
  "active_jobs": [
    {
      "project_id": "thesis",
      "paper_id": "paper-a",
      "status": "PROCESSING",
      "attempt": 2,
      "next_poll_at": "2026-05-06T12:05:00Z",
      "started_at": "2026-05-06T11:50:00Z"
    }
  ],
  "recent_failures": [
    {
      "project_id": "thesis",
      "paper_id": "paper-b",
      "status": "FAILED",
      "last_error": "rate limit exceeded",
      "occurred_at": "2026-05-06T11:55:00Z"
    }
  ],
  "last_tick_at": "2026-05-06T11:59:50Z",
  "last_tick_error": {
    "at": "2026-05-06T11:59:50Z",
    "message": "email token ingestion: offline"
  },
  "tick_health": "normal",
  "supervisor": {
    "state": "running",
    "pid": 4242,
    "started_at": "2026-05-06T09:00:00Z",
    "paused_at": null
  },
  "projects": [
    {
      "project_id": "thesis",
      "enabled": true,
      "state": "ok",
      "active_count": 2,
      "last_run_at": "2026-05-06T11:59:50Z",
      "last_ok_at": "2026-05-06T11:59:50Z",
      "last_error": null
    }
  ]
}"#;
        assert_eq!(
            json, expected,
            "widget JSON shape changed; update docs/widget-schema.md (bump schema_version for a breaking change)"
        );
        let documented = include_str!("../docs/widget-schema.md");
        assert!(
            documented.contains(expected),
            "docs/widget-schema.md must show this exact sample document"
        );
    }

    #[test]
    fn write_atomically_round_trips() {
        let db = make_db("widget-roundtrip");
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("widget-state.json");
        write_fleet(&db, &path, Utc::now()).expect("write_fleet");

        let raw = std::fs::read_to_string(&path).expect("read back");
        let v: serde_json::Value = serde_json::from_str(&raw).expect("parse json");

        // Verify all required top-level keys are present.
        for key in &[
            "schema_version",
            "generated_at",
            "project_id",
            "summary",
            "active_jobs",
            "recent_failures",
            "last_tick_at",
            "last_tick_error",
            "tick_health",
            "supervisor",
            "projects",
        ] {
            assert!(v.get(key).is_some(), "missing key: {key}");
        }
        assert_eq!(v["schema_version"], 1);

        // No leftover .tmp.* files should remain after a successful write.
        let leftover: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(leftover.is_empty(), "leftover tmp files: {leftover:?}");
    }

    #[cfg(unix)]
    #[test]
    fn widget_state_file_is_0o600_after_atomic_write() {
        use std::os::unix::fs::PermissionsExt;

        let db = make_db("widget-mode");
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("widget-state.json");

        write_fleet(&db, &path, Utc::now()).expect("write_fleet");

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "widget state must be 0o600 after write");
    }

    #[test]
    fn disabled_returns_none_path() {
        let mut cfg = Config::default();
        cfg.core.widget_state_enabled = false;
        assert!(
            cfg.widget_state_path().is_none(),
            "should be None when disabled"
        );
    }

    #[test]
    fn truncates_long_last_error_at_80_chars() {
        // Use a multi-byte character near the boundary to verify char-boundary
        // safety — 'á' is 2 bytes in UTF-8.
        let long: String = "á".repeat(50) + &"x".repeat(50); // 100 Unicode chars
        let truncated = truncate_chars(&long, 80);
        assert_eq!(truncated.chars().count(), 80, "should be exactly 80 chars");
        // No panic means the slice is at a valid UTF-8 boundary.
        assert!(std::str::from_utf8(truncated.as_bytes()).is_ok());
    }
}

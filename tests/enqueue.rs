//! Acceptance tests for `Db::enqueue`: request idempotency, coverage dedupe and
//! version/round allocation, all exercised against a temporary SQLite file
//! through independent connections.

use anyhow::Result;
use chrono::Utc;
use reviewloop::{
    db::Db,
    model::{
        EnqueueConflict, EnqueueMode, EnqueueOutcome, EnqueueRequest, ExistingReason, Job, JobPdf,
        JobStatus, NewJob,
    },
};
use rusqlite::{Connection, params};
use std::{
    path::PathBuf,
    sync::{Arc, Barrier},
    thread,
};

const PROJECT: &str = "project-enqueue";
const WORKERS: usize = 8;

struct Ctx {
    _tmp: tempfile::TempDir,
    path: PathBuf,
    db: Db,
}

impl Ctx {
    fn new() -> Result<Self> {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("reviewloop.db");
        let db = Db::new_file(path.clone());
        db.ensure_schema()?;
        Ok(Self {
            _tmp: tmp,
            path,
            db,
        })
    }

    /// A separate handle on the same file, as another process would open it.
    fn open(&self) -> Db {
        Db::new_file(self.path.clone())
    }

    fn sql(&self) -> Result<Connection> {
        Ok(Connection::open(&self.path)?)
    }

    fn count(&self, sql: &str) -> Result<i64> {
        Ok(self.sql()?.query_row(sql, [], |row| row.get(0))?)
    }

    fn jobs(&self) -> Result<i64> {
        self.count("SELECT COUNT(*) FROM jobs")
    }

    fn events(&self, event_type: &str) -> Result<i64> {
        Ok(self.sql()?.query_row(
            "SELECT COUNT(*) FROM events WHERE event_type = ?1",
            params![event_type],
            |row| row.get(0),
        )?)
    }
}

fn new_job(hash: &str, venue: Option<&str>) -> NewJob {
    NewJob {
        project_id: PROJECT.to_string(),
        paper_id: "main".to_string(),
        backend: "stanford".to_string(),
        pdf: JobPdf::Unpinned {
            pdf_path: "paper/main.pdf".to_string(),
            pdf_hash: hash.to_string(),
        },
        status: JobStatus::Queued,
        email: "author@example.edu".to_string(),
        venue: venue.map(str::to_string),
        git_tag: None,
        git_commit: None,
        next_poll_at: None,
    }
}

fn request(job: NewJob, key: Option<&str>, mode: EnqueueMode) -> EnqueueRequest {
    EnqueueRequest {
        job,
        request_key: key.map(str::to_string),
        mode,
        source: "test".to_string(),
    }
}

fn created(outcome: EnqueueOutcome) -> Job {
    match outcome {
        EnqueueOutcome::Created(job) => job,
        other => panic!("expected Created, got {other:?}"),
    }
}

fn existing(outcome: EnqueueOutcome, expected: ExistingReason) -> Job {
    match outcome {
        EnqueueOutcome::Existing { job, reason } if reason == expected => job,
        other => panic!("expected Existing({expected:?}), got {other:?}"),
    }
}

fn conflict(result: Result<EnqueueOutcome>) -> EnqueueConflict {
    match result {
        Ok(outcome) => panic!("expected a conflict, got {outcome:?}"),
        Err(err) => err
            .downcast::<EnqueueConflict>()
            .unwrap_or_else(|err| panic!("expected EnqueueConflict, got {err:#}")),
    }
}

/// Run `WORKERS` enqueues at once, each on its own connection.
fn race(ctx: &Ctx, requests: Vec<EnqueueRequest>) -> Vec<EnqueueOutcome> {
    let barrier = Arc::new(Barrier::new(requests.len()));
    thread::scope(|scope| {
        let handles: Vec<_> = requests
            .into_iter()
            .map(|request| {
                let db = ctx.open();
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    db.enqueue(&request).expect("concurrent enqueue")
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("enqueue thread panicked"))
            .collect()
    })
}

#[test]
fn concurrent_same_key_creates_one_job_and_one_event() -> Result<()> {
    let ctx = Ctx::new()?;
    let requests = (0..WORKERS)
        .map(|_| {
            request(
                new_job("h1", None),
                Some("agent-req-1"),
                EnqueueMode::Deduplicate,
            )
        })
        .collect();

    let outcomes = race(&ctx, requests);

    let created_ids: Vec<_> = outcomes
        .iter()
        .filter(|o| o.is_created())
        .map(|o| o.job().id.clone())
        .collect();
    assert_eq!(created_ids.len(), 1, "exactly one caller creates the job");
    for outcome in &outcomes {
        assert_eq!(outcome.job().id, created_ids[0]);
        if let EnqueueOutcome::Existing { reason, .. } = outcome {
            assert_eq!(*reason, ExistingReason::RequestReplay);
        }
    }
    assert_eq!(ctx.jobs()?, 1);
    assert_eq!(ctx.events("job_enqueued")?, 1);
    assert_eq!(ctx.count("SELECT COUNT(*) FROM enqueue_requests")?, 1);
    Ok(())
}

#[test]
fn concurrent_keyless_duplicates_are_covered_by_one_job() -> Result<()> {
    let ctx = Ctx::new()?;
    let requests = (0..WORKERS)
        .map(|_| request(new_job("h1", Some("ICLR")), None, EnqueueMode::Deduplicate))
        .collect();

    let outcomes = race(&ctx, requests);

    assert_eq!(outcomes.iter().filter(|o| o.is_created()).count(), 1);
    let covered = outcomes
        .iter()
        .filter(|o| {
            matches!(
                o,
                EnqueueOutcome::Existing {
                    reason: ExistingReason::Covered,
                    ..
                }
            )
        })
        .count();
    assert_eq!(covered, WORKERS - 1);
    assert_eq!(ctx.jobs()?, 1);
    assert_eq!(ctx.events("job_enqueued")?, 1);
    assert_eq!(ctx.events("duplicate_skipped")?, (WORKERS - 1) as i64);
    Ok(())
}

#[test]
fn concurrent_new_rounds_get_distinct_rounds() -> Result<()> {
    let ctx = Ctx::new()?;
    let requests = (0..WORKERS)
        .map(|i| {
            request(
                new_job("h1", None),
                Some(&format!("rerun-{i}")),
                EnqueueMode::NewRound,
            )
        })
        .collect();

    let outcomes = race(&ctx, requests);

    assert!(outcomes.iter().all(EnqueueOutcome::is_created));
    let mut rounds: Vec<u32> = outcomes.iter().map(|o| o.job().round_no).collect();
    rounds.sort_unstable();
    assert_eq!(rounds, (1..=WORKERS as u32).collect::<Vec<_>>());
    assert!(outcomes.iter().all(|o| o.job().version_no == 1));
    assert_eq!(ctx.events("job_enqueued")?, WORKERS as i64);
    Ok(())
}

#[test]
fn concurrent_new_versions_get_distinct_version_numbers() -> Result<()> {
    let ctx = Ctx::new()?;
    let requests = (0..WORKERS)
        .map(|i| {
            request(
                new_job(&format!("h{i}"), None),
                None,
                EnqueueMode::Deduplicate,
            )
        })
        .collect();

    let outcomes = race(&ctx, requests);

    let mut versions: Vec<u32> = outcomes.iter().map(|o| o.job().version_no).collect();
    versions.sort_unstable();
    assert_eq!(versions, (1..=WORKERS as u32).collect::<Vec<_>>());
    assert!(outcomes.iter().all(|o| o.job().round_no == 1));
    Ok(())
}

#[test]
fn failed_transaction_writes_nothing_and_retry_succeeds_once() -> Result<()> {
    let ctx = Ctx::new()?;
    // Fail the last write of the transaction, after the job row and key binding.
    ctx.sql()?.execute_batch(
        "CREATE TRIGGER fail_enqueue_event BEFORE INSERT ON events
         WHEN NEW.event_type = 'job_enqueued'
         BEGIN SELECT RAISE(ABORT, 'injected failure'); END;",
    )?;
    let req = request(
        new_job("h1", None),
        Some("retry-me"),
        EnqueueMode::Deduplicate,
    );

    let err = ctx
        .db
        .enqueue(&req)
        .expect_err("injected failure must surface");
    assert!(format!("{err:#}").contains("injected failure"), "{err:#}");
    assert_eq!(ctx.jobs()?, 0, "rolled back: no job");
    assert_eq!(ctx.count("SELECT COUNT(*) FROM enqueue_requests")?, 0);
    assert_eq!(ctx.count("SELECT COUNT(*) FROM events")?, 0);

    ctx.sql()?
        .execute_batch("DROP TRIGGER fail_enqueue_event;")?;
    let job = created(ctx.open().enqueue(&req)?);
    assert_eq!((job.version_no, job.round_no), (1, 1));
    let replay = existing(ctx.db.enqueue(&req)?, ExistingReason::RequestReplay);
    assert_eq!(replay.id, job.id);
    assert_eq!(ctx.jobs()?, 1);
    assert_eq!(ctx.events("job_enqueued")?, 1);
    Ok(())
}

#[test]
fn replay_returns_finished_job_without_new_round() -> Result<()> {
    let ctx = Ctx::new()?;
    for (key, terminal) in [
        ("done", JobStatus::Completed),
        ("broke", JobStatus::Failed),
        ("slow", JobStatus::Timeout),
    ] {
        let req = request(new_job(key, None), Some(key), EnqueueMode::Deduplicate);
        let job = created(ctx.db.enqueue(&req)?);
        ctx.db
            .update_job_state_unchecked(&job.id, terminal, None, Some(None), None)?;

        let replay = existing(ctx.db.enqueue(&req)?, ExistingReason::RequestReplay);
        assert_eq!(replay.id, job.id, "{terminal:?} job is returned again");
        assert_eq!(replay.status, terminal);
        assert_eq!(replay.round_no, job.round_no);
    }
    assert_eq!(ctx.jobs()?, 3);
    assert_eq!(ctx.events("job_enqueued")?, 3);
    assert_eq!(
        ctx.events("duplicate_skipped")?,
        0,
        "replays write no events"
    );
    Ok(())
}

#[test]
fn replay_does_not_touch_the_existing_job() -> Result<()> {
    let ctx = Ctx::new()?;
    let req = request(new_job("h1", None), Some("k"), EnqueueMode::Deduplicate);
    let job = created(ctx.db.enqueue(&req)?);
    let cooldown = Utc::now() + chrono::Duration::hours(2);
    ctx.db.update_job_state(
        &job.id,
        JobStatus::Processing,
        Some(3),
        Some(Some(cooldown)),
        Some(Some("rate limited".to_string())),
    )?;
    let before = ctx.db.get_job(&job.id)?.expect("job");

    existing(ctx.db.enqueue(&req)?, ExistingReason::RequestReplay);
    let covered = request(new_job("h1", None), Some("other"), EnqueueMode::Deduplicate);
    existing(ctx.db.enqueue(&covered)?, ExistingReason::Covered);

    let after = ctx.db.get_job(&job.id)?.expect("job");
    assert_eq!(after.attempt, 3);
    assert_eq!(after.next_poll_at, before.next_poll_at);
    assert_eq!(after.last_error, before.last_error);
    assert_eq!(after.updated_at, before.updated_at);
    Ok(())
}

#[test]
fn same_key_with_different_content_conflicts_and_writes_nothing() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = created(ctx.db.enqueue(&request(
        new_job("h1", Some("ICLR")),
        Some("k"),
        EnqueueMode::Deduplicate,
    ))?);
    let events_before = ctx.count("SELECT COUNT(*) FROM events")?;

    let err = conflict(ctx.db.enqueue(&request(
        new_job("h2", Some("ICLR")),
        Some("k"),
        EnqueueMode::Deduplicate,
    )));

    assert_eq!(err.existing_job_id, job.id);
    assert_eq!(err.request_key, "k");
    let fields: Vec<_> = err.mismatches.iter().map(|m| m.field).collect();
    assert_eq!(fields, ["pdf_hash", "version_key"]);
    assert_eq!(err.mismatches[0].recorded.as_deref(), Some("h1"));
    assert_eq!(err.mismatches[0].requested.as_deref(), Some("h2"));
    let message = err.to_string();
    assert!(
        message.contains(&job.id) && message.contains("new request key"),
        "{message}"
    );
    assert_eq!(ctx.jobs()?, 1);
    assert_eq!(ctx.count("SELECT COUNT(*) FROM events")?, events_before);
    Ok(())
}

#[test]
fn request_details_outside_the_review_identity_do_not_conflict() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = created(ctx.db.enqueue(&request(
        new_job("h1", Some("ICLR")),
        Some("k"),
        EnqueueMode::Deduplicate,
    ))?);

    let mut moved = new_job("h1", Some("  ICLR "));
    moved.pdf = JobPdf::Unpinned {
        pdf_path: "build/renamed.pdf".to_string(),
        pdf_hash: "h1".to_string(),
    };
    moved.email = "coauthor@example.edu".to_string();
    // The key owns its job whatever mode the replay asks for.
    let replay = existing(
        ctx.db
            .enqueue(&request(moved, Some("k"), EnqueueMode::NewRound))?,
        ExistingReason::RequestReplay,
    );
    assert_eq!(replay.id, job.id);
    assert_eq!(ctx.jobs()?, 1);
    Ok(())
}

#[test]
fn venue_is_part_of_coverage_and_request_identity() -> Result<()> {
    let ctx = Ctx::new()?;
    let iclr = created(ctx.db.enqueue(&request(
        new_job("h1", Some("ICLR")),
        Some("iclr"),
        EnqueueMode::Deduplicate,
    ))?);
    assert_eq!(iclr.venue.as_deref(), Some("ICLR"));

    let neurips = created(ctx.db.enqueue(&request(
        new_job("h1", Some("NeurIPS")),
        None,
        EnqueueMode::Deduplicate,
    ))?);
    assert_ne!(
        neurips.id, iclr.id,
        "same manuscript, other venue: new review"
    );
    assert_eq!((neurips.version_no, neurips.round_no), (1, 2));

    let padded = existing(
        ctx.db.enqueue(&request(
            new_job("h1", Some(" NeurIPS ")),
            None,
            EnqueueMode::Deduplicate,
        ))?,
        ExistingReason::Covered,
    );
    assert_eq!(padded.id, neurips.id, "venues compare after trimming");

    let err = conflict(ctx.db.enqueue(&request(
        new_job("h1", Some("NeurIPS")),
        Some("iclr"),
        EnqueueMode::Deduplicate,
    )));
    assert_eq!(err.mismatches.len(), 1);
    assert_eq!(err.mismatches[0].field, "venue");
    assert_eq!(ctx.jobs()?, 2);
    Ok(())
}

#[test]
fn blank_venue_matches_legacy_rows_without_venue() -> Result<()> {
    let ctx = Ctx::new()?;
    let legacy = ctx.db.create_job(&new_job("h1", None))?;
    ctx.sql()?.execute(
        "UPDATE jobs SET venue = '' WHERE id = ?1",
        params![legacy.id],
    )?;

    let covered = existing(
        ctx.db.enqueue(&request(
            new_job("h1", Some("   ")),
            None,
            EnqueueMode::Deduplicate,
        ))?,
        ExistingReason::Covered,
    );
    assert_eq!(covered.id, legacy.id);
    Ok(())
}

#[test]
fn explicit_new_round_needs_a_new_key() -> Result<()> {
    let ctx = Ctx::new()?;
    let first = created(ctx.db.enqueue(&request(
        new_job("h1", None),
        Some("review-1"),
        EnqueueMode::Deduplicate,
    ))?);
    ctx.db
        .update_job_state_unchecked(&first.id, JobStatus::Completed, None, Some(None), None)?;

    let covered = existing(
        ctx.db.enqueue(&request(
            new_job("h1", None),
            Some("review-1b"),
            EnqueueMode::Deduplicate,
        ))?,
        ExistingReason::Covered,
    );
    assert_eq!(
        covered.id, first.id,
        "completed review covers a plain request"
    );

    let second_req = request(new_job("h1", None), Some("review-2"), EnqueueMode::NewRound);
    let second = created(ctx.db.enqueue(&second_req)?);
    assert_eq!((second.version_no, second.round_no), (1, 2));

    let replay = existing(ctx.db.enqueue(&second_req)?, ExistingReason::RequestReplay);
    assert_eq!(
        replay.id, second.id,
        "replaying the round's key starts no round 3"
    );

    let third = created(ctx.db.enqueue(&request(
        new_job("h1", None),
        Some("review-3"),
        EnqueueMode::NewRound,
    ))?);
    assert_eq!(third.round_no, 3);
    assert_eq!(ctx.jobs()?, 3);
    assert_eq!(ctx.events("job_enqueued")?, 3);
    Ok(())
}

#[test]
fn failed_attempt_gives_its_round_back() -> Result<()> {
    let ctx = Ctx::new()?;
    let failed = created(ctx.db.enqueue(&request(
        new_job("h1", None),
        Some("a"),
        EnqueueMode::Deduplicate,
    ))?);
    ctx.db
        .update_job_state_unchecked(&failed.id, JobStatus::Failed, None, Some(None), None)?;

    let retry = created(ctx.db.enqueue(&request(
        new_job("h1", None),
        Some("b"),
        EnqueueMode::Deduplicate,
    ))?);
    assert_eq!((retry.version_no, retry.round_no), (1, 1));
    Ok(())
}

#[test]
fn request_keys_are_scoped_to_their_project() -> Result<()> {
    let ctx = Ctx::new()?;
    let ours = created(ctx.db.enqueue(&request(
        new_job("h1", None),
        Some("k"),
        EnqueueMode::Deduplicate,
    ))?);
    let mut theirs = new_job("h2", None);
    theirs.project_id = "other-project".to_string();
    let theirs = created(
        ctx.db
            .enqueue(&request(theirs, Some("k"), EnqueueMode::Deduplicate))?,
    );
    assert_ne!(ours.id, theirs.id);
    assert_eq!((theirs.version_no, theirs.round_no), (1, 1));
    Ok(())
}

#[test]
fn blank_request_key_is_rejected() -> Result<()> {
    let ctx = Ctx::new()?;
    let err = ctx
        .db
        .enqueue(&request(
            new_job("h1", None),
            Some("  "),
            EnqueueMode::Deduplicate,
        ))
        .expect_err("blank key");
    assert!(err.to_string().contains("request key"), "{err:#}");
    assert_eq!(ctx.jobs()?, 0);
    Ok(())
}

#[test]
fn purging_a_paper_releases_its_request_keys() -> Result<()> {
    let ctx = Ctx::new()?;
    let req = request(new_job("h1", None), Some("k"), EnqueueMode::Deduplicate);
    let job = created(ctx.db.enqueue(&req)?);

    ctx.db.purge_paper_history(PROJECT, "main")?;
    assert_eq!(ctx.count("SELECT COUNT(*) FROM enqueue_requests")?, 0);

    let again = created(ctx.db.enqueue(&req)?);
    assert_ne!(again.id, job.id);
    Ok(())
}

#[test]
fn pruning_terminal_jobs_releases_their_request_keys() -> Result<()> {
    let ctx = Ctx::new()?;
    let done = created(ctx.db.enqueue(&request(
        new_job("h1", None),
        Some("old"),
        EnqueueMode::Deduplicate,
    ))?);
    ctx.db
        .update_job_state_unchecked(&done.id, JobStatus::Completed, None, Some(None), None)?;
    let live = created(ctx.db.enqueue(&request(
        new_job("h2", None),
        Some("live"),
        EnqueueMode::Deduplicate,
    ))?);

    let retention = reviewloop::config::RetentionConfig {
        enabled: true,
        terminal_jobs_days: 1,
        ..Default::default()
    };
    let report = ctx
        .db
        .prune_retention(&retention, Utc::now() + chrono::Duration::days(3))?;
    assert_eq!(report.jobs, 1);

    let keys: Vec<String> = {
        let conn = ctx.sql()?;
        let mut stmt = conn.prepare("SELECT job_id FROM enqueue_requests")?;
        stmt.query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?
    };
    assert_eq!(keys, [live.id]);
    Ok(())
}

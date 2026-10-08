//! OSS-337 acceptance (1) and (6): competing claims on one job yield exactly one owner,
//! and normal retries still respect the existing cooldown.
//!
//! Concurrency runs against a file-backed SQLite database with one `Db` handle per OS
//! thread (`Db` is `!Sync`); elapsed time is simulated by passing a later `now`.

mod common;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use common::{
    Answer, Ctx, FetchResult, MockBackend, POLL_TTL, PROJECT, SUBMIT_TTL, SubmitResult, in_order,
    receipt,
};
use reviewloop::{
    backend::BackendError,
    db::{ClaimTiming, Db, Lease},
    model::{EventRecord, Job, JobStatus, SubmitChannel, SubmitStage, WorkKind},
    worker::{self, Attempt},
};
use serde_json::Value;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    path::Path,
    sync::Barrier,
    thread,
};

const CONTENDERS: usize = 8;
const ROUNDS: usize = 20;
const DAEMON_JOBS: usize = 40;
/// Distinct values so a wrong schedule index shows up as a wrong delay.
const SCHEDULE_MINUTES: [u64; 4] = [3, 7, 13, 29];
const RATE_LIMIT_MESSAGE: &str = "slow down";

/// The shared project with this suite's schedule and two daemon submission slots.
fn claims_ctx() -> Result<Ctx> {
    let mut ctx = Ctx::new()?;
    ctx.config.core.max_concurrency = 2;
    ctx.config.polling.schedule_minutes = SCHEDULE_MINUTES.to_vec();
    Ok(ctx)
}

fn payload_str<'a>(payload: &'a Value, key: &str) -> Option<&'a str> {
    payload.get(key).and_then(Value::as_str)
}

fn assert_within(value: DateTime<Utc>, from: DateTime<Utc>, to: DateTime<Utc>, what: &str) {
    assert!(
        from <= value && value <= to,
        "{what}: expected {value} within [{from}, {to}]"
    );
}

/// Primary backend whose submits answer `replies` in order.
fn submits(replies: impl IntoIterator<Item = SubmitResult>) -> MockBackend {
    MockBackend::default().on_submit(in_order(replies.into_iter().map(Answer::Now).collect()))
}

/// Backend whose fetches answer `replies` in order.
fn fetches(replies: impl IntoIterator<Item = FetchResult>) -> MockBackend {
    MockBackend::default().on_fetch(in_order(replies.into_iter().map(Answer::Now).collect()))
}

fn rate_limited(retry_after: Option<Duration>) -> BackendError {
    BackendError::RateLimited {
        message: RATE_LIMIT_MESSAGE.to_string(),
        retry_after,
    }
}

/// `CONTENDERS` threads, each with its own handle on the database file, race
/// `claim_job(.., ClaimTiming::Now, ..)` on every job in turn; a barrier releases them
/// together for each job. Returns every contender's result, grouped per job.
fn race_claims(
    db_path: &Path,
    job_ids: &[String],
    kind: WorkKind,
    now: DateTime<Utc>,
    ttl: Duration,
) -> Result<Vec<Vec<Option<Lease>>>> {
    let barrier = Barrier::new(CONTENDERS);
    let per_thread: Vec<Vec<Result<Option<Lease>>>> = thread::scope(|scope| {
        let handles: Vec<_> = (0..CONTENDERS)
            .map(|_| {
                scope.spawn(|| -> Vec<Result<Option<Lease>>> {
                    let db = Db::new_file(db_path.to_path_buf());
                    // No early return: a contender that errors still meets every barrier.
                    job_ids
                        .iter()
                        .map(|job_id| {
                            barrier.wait();
                            db.claim_job(job_id, kind, ClaimTiming::Now, now, ttl)
                        })
                        .collect()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("contender thread panicked"))
            .collect()
    });

    let mut per_job: Vec<Vec<Option<Lease>>> = vec![Vec::with_capacity(CONTENDERS); job_ids.len()];
    for results in per_thread {
        for (slot, result) in per_job.iter_mut().zip(results) {
            slot.push(result?);
        }
    }
    Ok(per_job)
}

/// Asserts exactly one contender won each job and that the row records that winner.
/// Returns the winners.
fn assert_single_winners(
    ctx: &Ctx,
    job_ids: &[String],
    outcomes: Vec<Vec<Option<Lease>>>,
    kind: WorkKind,
    now: DateTime<Utc>,
    ttl: Duration,
) -> Result<Vec<Lease>> {
    let mut winners = Vec::with_capacity(job_ids.len());
    for (job_id, results) in job_ids.iter().zip(outcomes) {
        assert_eq!(results.len(), CONTENDERS);
        let mut won: Vec<Lease> = results.into_iter().flatten().collect();
        assert_eq!(
            won.len(),
            1,
            "job {job_id}: expected one of {CONTENDERS} {kind:?} claims to win, got owners {:?}",
            won.iter().map(|lease| &lease.owner).collect::<Vec<_>>()
        );
        let lease = won.remove(0);
        assert_eq!(lease.kind, kind);
        assert_eq!(&lease.job.id, job_id);
        assert_eq!(lease.expires_at, now + ttl);
        assert_eq!(lease.job.lease_owner.as_deref(), Some(lease.owner.as_str()));
        assert_eq!(lease.job.lease_expires_at, Some(now + ttl));

        let stored = ctx.job(job_id)?;
        assert_eq!(
            stored.lease_owner.as_deref(),
            Some(lease.owner.as_str()),
            "job {job_id}: the stored owner must be the one winner"
        );
        assert_eq!(stored.lease_expires_at, Some(now + ttl));
        assert_eq!(stored.attempt, 0);
        assert!(
            !ctx.event_types(job_id)?
                .iter()
                .any(|t| t == "submit_claim_taken_over"),
            "job {job_id}: a first claim takes nothing over"
        );
        winners.push(lease);
    }

    let owners: HashSet<&str> = winners.iter().map(|lease| lease.owner.as_str()).collect();
    assert_eq!(
        owners.len(),
        winners.len(),
        "every winning claim gets a fresh owner id"
    );
    Ok(winners)
}

#[test]
fn competing_submit_claims_yield_exactly_one_owner() -> Result<()> {
    let ctx = claims_ctx()?;
    let job_ids = (0..ROUNDS)
        .map(|_| Ok(ctx.create_queued_job()?.id))
        .collect::<Result<Vec<_>>>()?;
    let now = Utc::now();

    let outcomes = race_claims(&ctx.db.path, &job_ids, WorkKind::Submit, now, SUBMIT_TTL)?;
    let winners =
        assert_single_winners(&ctx, &job_ids, outcomes, WorkKind::Submit, now, SUBMIT_TTL)?;

    for lease in &winners {
        assert_eq!(lease.job.status, JobStatus::Queued);
        assert_eq!(lease.job.submit_stage, Some(SubmitStage::Claimed));
        let stored = ctx.job(&lease.job.id)?;
        assert_eq!(stored.status, JobStatus::Queued, "claiming sends nothing");
        assert_eq!(stored.submit_stage, Some(SubmitStage::Claimed));
        assert_eq!(stored.token, None);
        // A late contender is refused while the winner's lease is live.
        assert!(
            ctx.db
                .claim_job(
                    &lease.job.id,
                    WorkKind::Submit,
                    ClaimTiming::Now,
                    now + Duration::minutes(29),
                    SUBMIT_TTL
                )?
                .is_none()
        );
    }
    assert!(
        ctx.ready_ids(now)?.is_empty(),
        "every claimed job is hidden from the ready listing"
    );
    Ok(())
}

#[test]
fn competing_poll_claims_yield_exactly_one_owner() -> Result<()> {
    let ctx = claims_ctx()?;
    let now = Utc::now();
    let due = now - Duration::minutes(1);
    let job_ids = (0..ROUNDS)
        .map(|i| Ok(ctx.create_processing_job(&format!("tok-poll-{i}"), due)?.id))
        .collect::<Result<Vec<_>>>()?;

    let outcomes = race_claims(&ctx.db.path, &job_ids, WorkKind::Poll, now, POLL_TTL)?;
    let winners = assert_single_winners(&ctx, &job_ids, outcomes, WorkKind::Poll, now, POLL_TTL)?;

    for (i, lease) in winners.iter().enumerate() {
        let token = format!("tok-poll-{i}");
        assert_eq!(lease.job.status, JobStatus::Processing);
        assert_eq!(
            lease.job.submit_stage, None,
            "a poll claim sets no submit stage"
        );
        let stored = ctx.job(&lease.job.id)?;
        assert_eq!(stored.status, JobStatus::Processing);
        assert_eq!(stored.submit_stage, None);
        assert_eq!(stored.token.as_deref(), Some(token.as_str()));
        assert_eq!(
            stored.next_poll_at,
            Some(due),
            "claiming keeps the schedule"
        );
    }
    assert!(
        ctx.due_ids(now)?.is_empty(),
        "every claimed job is hidden from the due-poll listing"
    );
    Ok(())
}

type Listing = fn(&Db, &str, usize, DateTime<Utc>) -> Result<Vec<Job>>;

#[derive(Default)]
struct DaemonRun {
    listed: Vec<String>,
    claimed: Vec<Lease>,
    refused: Vec<String>,
}

/// Order in which daemon B walks its listing; daemon A always walks it as listed.
#[derive(Debug, Clone, Copy)]
enum WalkOrder {
    /// Both walk oldest-first, as `process_submissions` does: the slower daemon tends
    /// to find every job already taken.
    Same,
    /// B walks newest-first, so the two meet and contend mid-listing.
    Opposite,
}

/// Two daemon passes on separate threads and handles: both list ready work at `now`,
/// wait until the other has listed too (so each acts on a listing the other makes
/// stale), then claim every listed job with `ClaimTiming::WhenDue`.
fn run_two_daemons(
    db_path: &Path,
    list: Listing,
    kind: WorkKind,
    now: DateTime<Utc>,
    ttl: Duration,
    order: WalkOrder,
) -> Result<[DaemonRun; 2]> {
    let start = Barrier::new(2);
    let listed = Barrier::new(2);
    let daemon = |reverse: bool| {
        let (start, listed) = (&start, &listed);
        move || -> Result<DaemonRun> {
            let db = Db::new_file(db_path.to_path_buf());
            start.wait();
            let jobs = list(&db, PROJECT, 1000, now);
            listed.wait();
            let mut run = DaemonRun {
                listed: jobs?.into_iter().map(|job| job.id).collect(),
                ..DaemonRun::default()
            };
            let mut walk: Vec<&String> = run.listed.iter().collect();
            if reverse {
                walk.reverse();
            }
            for job_id in walk {
                match db.claim_job(job_id, kind, ClaimTiming::WhenDue, now, ttl)? {
                    Some(lease) => run.claimed.push(lease),
                    None => run.refused.push(job_id.clone()),
                }
            }
            Ok(run)
        }
    };
    let (a, b) = thread::scope(|scope| {
        let a = scope.spawn(daemon(false));
        let b = scope.spawn(daemon(matches!(order, WalkOrder::Opposite)));
        (
            a.join().expect("daemon A panicked"),
            b.join().expect("daemon B panicked"),
        )
    });
    Ok([a?, b?])
}

fn assert_daemons_split_work(
    ctx: &Ctx,
    all: &BTreeSet<String>,
    runs: &[DaemonRun; 2],
) -> Result<()> {
    let claimed: Vec<BTreeSet<String>> = runs
        .iter()
        .map(|run| {
            run.claimed
                .iter()
                .map(|lease| lease.job.id.clone())
                .collect()
        })
        .collect();
    for (name, run, mine, theirs) in [
        ("A", &runs[0], &claimed[0], &claimed[1]),
        ("B", &runs[1], &claimed[1], &claimed[0]),
    ] {
        assert_eq!(
            run.listed.len(),
            all.len(),
            "daemon {name} lists each job once"
        );
        assert_eq!(
            &run.listed.iter().cloned().collect::<BTreeSet<_>>(),
            all,
            "daemon {name} listed before any claim, so its listing holds every job"
        );
        assert_eq!(
            mine.len(),
            run.claimed.len(),
            "daemon {name} claims no job twice"
        );
        assert_eq!(
            run.refused.iter().cloned().collect::<BTreeSet<_>>(),
            *theirs,
            "daemon {name} is refused exactly the jobs the other daemon holds"
        );
    }
    assert!(
        claimed[0].is_disjoint(&claimed[1]),
        "no job is claimed by both daemons: {:?}",
        claimed[0].intersection(&claimed[1]).collect::<Vec<_>>()
    );
    assert_eq!(
        &claimed[0]
            .union(&claimed[1])
            .cloned()
            .collect::<BTreeSet<_>>(),
        all,
        "every ready job is claimed by one daemon"
    );
    for lease in runs.iter().flat_map(|run| &run.claimed) {
        assert_eq!(
            ctx.job(&lease.job.id)?.lease_owner.as_deref(),
            Some(lease.owner.as_str())
        );
    }
    Ok(())
}

#[test]
fn two_daemons_claim_each_ready_queued_job_exactly_once() -> Result<()> {
    let ctx = claims_ctx()?;
    let all = (0..DAEMON_JOBS)
        .map(|_| Ok(ctx.create_queued_job()?.id))
        .collect::<Result<BTreeSet<_>>>()?;
    let start = Utc::now();

    // The second pass runs once the first pass's unused claims have lapsed, so both
    // daemons also race to take over the same expired claims.
    let mut previous: Option<HashMap<String, String>> = None;
    for (pass, order) in [WalkOrder::Same, WalkOrder::Opposite]
        .into_iter()
        .enumerate()
    {
        let now = start + SUBMIT_TTL * pass as i32;
        assert_eq!(ctx.ready_ids(now)?, all, "pass {pass}: every job is ready");
        let runs = run_two_daemons(
            &ctx.db.path,
            Db::list_ready_queued,
            WorkKind::Submit,
            now,
            SUBMIT_TTL,
            order,
        )?;
        assert_daemons_split_work(&ctx, &all, &runs)?;

        let owners: HashMap<String, String> = runs
            .iter()
            .flat_map(|run| &run.claimed)
            .map(|lease| (lease.job.id.clone(), lease.owner.clone()))
            .collect();
        for job_id in &all {
            let stored = ctx.job(job_id)?;
            assert_eq!(stored.status, JobStatus::Queued);
            assert_eq!(stored.submit_stage, Some(SubmitStage::Claimed));
            assert_eq!(stored.lease_expires_at, Some(now + SUBMIT_TTL));
            let takeovers: Vec<EventRecord> = ctx
                .events(job_id)?
                .into_iter()
                .filter(|event| event.event_type == "submit_claim_taken_over")
                .collect();
            match &previous {
                None => assert!(takeovers.is_empty(), "pass {pass}: {takeovers:?}"),
                Some(previous) => {
                    assert_eq!(
                        takeovers.len(),
                        1,
                        "pass {pass}: only the winning daemon records a takeover: {takeovers:?}"
                    );
                    assert_eq!(
                        payload_str(&takeovers[0].payload, "previous_owner"),
                        Some(previous[job_id].as_str())
                    );
                    assert_eq!(
                        payload_str(&takeovers[0].payload, "owner"),
                        Some(owners[job_id].as_str())
                    );
                }
            }
        }
        assert!(ctx.ready_ids(now)?.is_empty(), "pass {pass}: all claimed");
        previous = Some(owners);
    }
    Ok(())
}

#[test]
fn two_daemons_claim_each_due_processing_job_exactly_once() -> Result<()> {
    let ctx = claims_ctx()?;
    let now = Utc::now();
    let all = (0..DAEMON_JOBS)
        .map(|i| {
            Ok(ctx
                .create_processing_job(&format!("tok-daemon-{i}"), now - Duration::minutes(1))?
                .id)
        })
        .collect::<Result<BTreeSet<_>>>()?;

    for (pass, order) in [WalkOrder::Same, WalkOrder::Opposite]
        .into_iter()
        .enumerate()
    {
        let now = now + POLL_TTL * pass as i32;
        assert_eq!(ctx.due_ids(now)?, all, "pass {pass}: every job is due");
        let runs = run_two_daemons(
            &ctx.db.path,
            Db::list_due_processing,
            WorkKind::Poll,
            now,
            POLL_TTL,
            order,
        )?;
        assert_daemons_split_work(&ctx, &all, &runs)?;
        for job_id in &all {
            let stored = ctx.job(job_id)?;
            assert_eq!(stored.status, JobStatus::Processing);
            assert_eq!(stored.submit_stage, None);
            assert_eq!(stored.lease_expires_at, Some(now + POLL_TTL));
        }
        assert!(ctx.due_ids(now)?.is_empty(), "pass {pass}: all claimed");
    }
    Ok(())
}

#[test]
fn ready_listing_excludes_live_submit_lease_and_readmits_it_once_expired() -> Result<()> {
    let ctx = claims_ctx()?;
    let job = ctx.create_queued_job()?;
    let now = Utc::now();
    assert!(ctx.ready_ids(now)?.contains(&job.id));

    let first = ctx
        .db
        .claim_job(&job.id, WorkKind::Submit, ClaimTiming::Now, now, SUBMIT_TTL)?
        .context("an unclaimed QUEUED job is claimable")?;
    assert!(!ctx.ready_ids(now)?.contains(&job.id));
    assert!(
        !ctx.ready_ids(first.expires_at - Duration::seconds(1))?
            .contains(&job.id)
    );
    for timing in [ClaimTiming::WhenDue, ClaimTiming::Now] {
        assert!(
            ctx.db
                .claim_job(
                    &job.id,
                    WorkKind::Submit,
                    timing,
                    first.expires_at - Duration::seconds(1),
                    SUBMIT_TTL
                )?
                .is_none(),
            "{timing:?} claim must be refused while the lease is live"
        );
    }
    // The SQL listing and the claim agree that the lease ends at `expires_at`.
    assert!(ctx.ready_ids(first.expires_at)?.contains(&job.id));

    let later = now + Duration::minutes(31);
    assert!(ctx.ready_ids(later)?.contains(&job.id));
    let second = ctx
        .db
        .claim_job(
            &job.id,
            WorkKind::Submit,
            ClaimTiming::WhenDue,
            later,
            SUBMIT_TTL,
        )?
        .context("an expired pre-dispatch claim can be taken over")?;
    assert_ne!(second.owner, first.owner);
    let stored = ctx.job(&job.id)?;
    assert_eq!(stored.status, JobStatus::Queued);
    assert_eq!(stored.submit_stage, Some(SubmitStage::Claimed));
    assert_eq!(stored.lease_owner.as_deref(), Some(second.owner.as_str()));
    assert_eq!(stored.lease_expires_at, Some(later + SUBMIT_TTL));
    assert!(!ctx.ready_ids(later)?.contains(&job.id));

    let events = ctx.events(&job.id)?;
    let takeovers: Vec<&EventRecord> = events
        .iter()
        .filter(|event| event.event_type == "submit_claim_taken_over")
        .collect();
    assert_eq!(takeovers.len(), 1, "events: {events:?}");
    assert_eq!(
        payload_str(&takeovers[0].payload, "previous_owner"),
        Some(first.owner.as_str())
    );
    assert_eq!(
        payload_str(&takeovers[0].payload, "owner"),
        Some(second.owner.as_str())
    );

    // The displaced owner can neither dispatch nor release.
    let mut stale = first.clone();
    assert!(!ctx.db.begin_submit_dispatch(
        &mut stale,
        SubmitChannel::Primary,
        later,
        SUBMIT_TTL
    )?);
    assert!(!ctx.db.release_lease(&first)?);
    let stored = ctx.job(&job.id)?;
    assert_eq!(stored.status, JobStatus::Queued);
    assert_eq!(stored.lease_owner.as_deref(), Some(second.owner.as_str()));
    assert!(
        !ctx.event_types(&job.id)?
            .iter()
            .any(|t| t == "submit_dispatched")
    );

    // The current owner releases; the job is ready again at once, cooldown untouched.
    assert!(ctx.db.release_lease(&second)?);
    let stored = ctx.job(&job.id)?;
    assert_eq!(stored.status, JobStatus::Queued);
    assert_eq!(stored.lease_owner, None);
    assert_eq!(stored.lease_expires_at, None);
    assert_eq!(stored.submit_stage, None);
    assert_eq!(stored.next_poll_at, None);
    assert!(ctx.ready_ids(later)?.contains(&job.id));
    Ok(())
}

#[test]
fn due_poll_listing_excludes_live_poll_lease_and_readmits_it_once_expired() -> Result<()> {
    let ctx = claims_ctx()?;
    let now = Utc::now();
    let job = ctx.create_processing_job("tok-listing", now - Duration::minutes(1))?;
    assert!(ctx.due_ids(now)?.contains(&job.id));

    let lease = ctx
        .db
        .claim_job(&job.id, WorkKind::Poll, ClaimTiming::WhenDue, now, POLL_TTL)?
        .context("a due PROCESSING job is claimable")?;
    assert!(!ctx.due_ids(now)?.contains(&job.id));
    assert!(
        !ctx.due_ids(lease.expires_at - Duration::seconds(1))?
            .contains(&job.id)
    );
    assert!(ctx.due_ids(lease.expires_at)?.contains(&job.id));

    let later = now + Duration::minutes(11);
    let next = ctx
        .db
        .claim_job(
            &job.id,
            WorkKind::Poll,
            ClaimTiming::WhenDue,
            later,
            POLL_TTL,
        )?
        .context("an expired poll lease can be taken over")?;
    assert_ne!(next.owner, lease.owner);
    assert_eq!(
        ctx.job(&job.id)?.lease_owner.as_deref(),
        Some(next.owner.as_str())
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_worker_submits_dispatch_exactly_once() -> Result<()> {
    let ctx = claims_ctx()?;
    let job = ctx.create_queued_job()?;
    let backend = MockBackend::default()
        .on_submit(in_order(vec![Answer::OnRelease(Ok(receipt("tok-race")))]));
    let db_a = Db::new_file(ctx.db.path.clone());
    let db_b = Db::new_file(ctx.db.path.clone());

    // Worker A dispatches and parks inside the backend; worker B tries meanwhile.
    let (a, b) = tokio::join!(
        worker::submit_job_with_backend(&ctx.config, &db_a, &job.id, &backend),
        async {
            backend.entered.notified().await;
            let in_flight = ctx.job(&job.id)?;
            let b = worker::submit_job_with_backend(&ctx.config, &db_b, &job.id, &backend).await;
            backend.release.notify_one();
            Ok::<_, anyhow::Error>((in_flight, b?))
        }
    );
    let (in_flight, b) = b?;
    assert_eq!(a?, Attempt::Ran);
    assert_eq!(b, Attempt::NotClaimed);
    assert_eq!(
        backend.submit_count(),
        1,
        "the provider sees one submission"
    );

    assert_eq!(in_flight.status, JobStatus::Submitted);
    assert_eq!(in_flight.submit_stage, Some(SubmitStage::Dispatched));
    assert!(in_flight.lease_owner.is_some());

    let stored = ctx.job(&job.id)?;
    assert_eq!(stored.status, JobStatus::Processing);
    assert_eq!(stored.token.as_deref(), Some("tok-race"));
    assert_eq!(stored.lease_owner, None);
    assert_eq!(stored.submit_stage, None);
    assert_eq!(
        ctx.event_types(&job.id)?,
        ["submit_dispatched", "submitted"],
        "one dispatch, one receipt"
    );
    Ok(())
}

/// Asserts a rate-limited submit left the job QUEUED, unleased, at `attempt`, due within
/// `[from, to]`, and that its last two events are the dispatch and the rate limit.
/// Returns the scheduled `next_poll_at`.
fn assert_submit_cooled_down(
    ctx: &Ctx,
    job_id: &str,
    attempt: u32,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    source: &str,
) -> Result<DateTime<Utc>> {
    let stored = ctx.job(job_id)?;
    assert_eq!(stored.status, JobStatus::Queued);
    assert_eq!(stored.attempt, attempt);
    assert_eq!(stored.submit_stage, None);
    assert_eq!(stored.lease_owner, None);
    assert_eq!(stored.lease_expires_at, None);
    assert_eq!(stored.token, None);
    assert!(!stored.fallback_used);
    assert_eq!(stored.last_error.as_deref(), Some(RATE_LIMIT_MESSAGE));
    let next = stored.next_poll_at.context("rate limit sets a cooldown")?;
    assert_within(next, from, to, "submit next_poll_at");

    let events = ctx.events(job_id)?;
    let [.., dispatched, limited] = events.as_slice() else {
        panic!("expected dispatch and rate-limit events, got {events:?}");
    };
    assert_eq!(dispatched.event_type, "submit_dispatched");
    assert_eq!(payload_str(&dispatched.payload, "channel"), Some("primary"));
    assert!(payload_str(&dispatched.payload, "owner").is_some_and(|o| !o.is_empty()));
    assert_eq!(limited.event_type, "submit_rate_limited");
    assert_eq!(
        payload_str(&limited.payload, "message"),
        Some(RATE_LIMIT_MESSAGE)
    );
    assert_eq!(
        payload_str(&limited.payload, "retry_after_source"),
        Some(source)
    );
    assert_eq!(
        payload_str(&limited.payload, "next_poll_at"),
        Some(next.to_rfc3339().as_str())
    );
    Ok(next)
}

/// Asserts the daemon's claim and listing honour the cooldown ending at `next`.
fn assert_submit_cooldown_gates_daemon(ctx: &Ctx, job_id: &str, next: DateTime<Utc>) -> Result<()> {
    let claim = |at: DateTime<Utc>| {
        ctx.db.claim_job(
            job_id,
            WorkKind::Submit,
            ClaimTiming::WhenDue,
            at,
            SUBMIT_TTL,
        )
    };
    assert!(claim(Utc::now())?.is_none(), "cooling down: not claimable");
    assert!(
        ctx.ready_ids(Utc::now())?.is_empty(),
        "cooling down: not listed"
    );
    assert!(claim(next - Duration::seconds(1))?.is_none());
    assert!(!ctx.ready_ids(next - Duration::seconds(1))?.contains(job_id));
    // Due from `next_poll_at` on, by both the listing and the claim.
    assert!(ctx.ready_ids(next)?.contains(job_id));
    let at_boundary = claim(next)?.context("claimable once next_poll_at is reached")?;
    assert!(ctx.db.release_lease(&at_boundary)?);

    let due = claim(next + Duration::seconds(1))?.context("claimable after the cooldown")?;
    assert_eq!(due.job.status, JobStatus::Queued);
    assert_eq!(due.job.submit_stage, Some(SubmitStage::Claimed));
    assert_eq!(
        due.job.next_poll_at,
        Some(next),
        "claiming keeps the schedule"
    );
    assert!(ctx.db.release_lease(&due)?);
    Ok(())
}

#[tokio::test]
async fn rate_limited_submit_with_retry_after_stays_queued_until_cooldown_passes() -> Result<()> {
    let ctx = claims_ctx()?;
    let job = ctx.create_queued_job()?;
    let retry_after = Duration::minutes(10);
    let backend = submits([Err(rate_limited(Some(retry_after)))]);

    let before = Utc::now();
    let attempt = worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend).await?;
    let after = Utc::now();
    assert_eq!(attempt, Attempt::Ran);
    assert_eq!(backend.submit_count(), 1);
    let next = assert_submit_cooled_down(
        &ctx,
        &job.id,
        1,
        before + retry_after,
        after + retry_after,
        "server",
    )?;
    assert_eq!(ctx.event_types(&job.id)?.len(), 2);

    // A daemon pass during the cooldown leaves the job alone (nothing is dispatched).
    worker::process_submissions(&ctx.config, &ctx.db).await?;
    let untouched = ctx.job(&job.id)?;
    assert_eq!(untouched.status, JobStatus::Queued);
    assert_eq!(untouched.attempt, 1);
    assert_eq!(untouched.next_poll_at, Some(next));
    assert_eq!(untouched.lease_owner, None);
    assert_eq!(ctx.event_types(&job.id)?.len(), 2);

    // An explicit CLI submit bypasses the cooldown.
    let manual = ctx
        .db
        .claim_job(
            &job.id,
            WorkKind::Submit,
            ClaimTiming::Now,
            Utc::now(),
            SUBMIT_TTL,
        )?
        .context("ClaimTiming::Now ignores the cooldown")?;
    assert_eq!(manual.job.submit_stage, Some(SubmitStage::Claimed));
    assert!(ctx.db.release_lease(&manual)?);

    assert_submit_cooldown_gates_daemon(&ctx, &job.id, next)
}

#[tokio::test]
async fn rate_limited_submit_without_retry_after_follows_polling_schedule() -> Result<()> {
    let ctx = claims_ctx()?;
    let job = ctx.create_queued_job()?;
    let backend = submits([Err(rate_limited(None)), Err(rate_limited(None))]);
    // The existing cadence indexes the schedule by the new attempt number.
    let delay = |attempt: usize| Duration::minutes(SCHEDULE_MINUTES[attempt] as i64);

    let before = Utc::now();
    worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend).await?;
    let after = Utc::now();
    let first = assert_submit_cooled_down(
        &ctx,
        &job.id,
        1,
        before + delay(1),
        after + delay(1),
        "schedule",
    )?;
    assert!(
        ctx.db
            .claim_job(
                &job.id,
                WorkKind::Submit,
                ClaimTiming::WhenDue,
                Utc::now(),
                SUBMIT_TTL
            )?
            .is_none()
    );

    // A second, explicit attempt is rate limited again and backs off further.
    let before = Utc::now();
    let attempt = worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend).await?;
    let after = Utc::now();
    assert_eq!(attempt, Attempt::Ran);
    assert_eq!(backend.submit_count(), 2);
    let second = assert_submit_cooled_down(
        &ctx,
        &job.id,
        2,
        before + delay(2),
        after + delay(2),
        "schedule",
    )?;
    assert!(second > first);
    assert_eq!(
        ctx.event_types(&job.id)?,
        [
            "submit_dispatched",
            "submit_rate_limited",
            "submit_dispatched",
            "submit_rate_limited"
        ]
    );

    assert_submit_cooldown_gates_daemon(&ctx, &job.id, second)
}

#[tokio::test]
async fn rate_limited_poll_respects_cooldown() -> Result<()> {
    let ctx = claims_ctx()?;
    let token = "tok-poll-cooldown";
    let job = ctx.create_processing_job(token, Utc::now() - Duration::minutes(1))?;
    let retry_after = Duration::minutes(10);
    let backend = fetches([
        Err(rate_limited(Some(retry_after))),
        Err(rate_limited(None)),
    ]);

    let before = Utc::now();
    let attempt = worker::poll_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend).await?;
    let after = Utc::now();
    assert_eq!(attempt, Attempt::Ran);
    assert_eq!(backend.fetch_count(), 1);

    let stored = ctx.job(&job.id)?;
    assert_eq!(stored.status, JobStatus::Processing);
    assert_eq!(stored.token.as_deref(), Some(token));
    assert_eq!(stored.attempt, 1);
    assert_eq!(stored.lease_owner, None);
    assert_eq!(stored.lease_expires_at, None);
    assert_eq!(stored.submit_stage, None);
    assert_eq!(stored.last_error.as_deref(), Some(RATE_LIMIT_MESSAGE));
    let next = stored.next_poll_at.context("rate limit sets a cooldown")?;
    assert_within(
        next,
        before + retry_after,
        after + retry_after,
        "poll next_poll_at",
    );
    let events = ctx.events(&job.id)?;
    let limited = events.last().context("poll records an event")?;
    assert_eq!(limited.event_type, "poll_rate_limited");
    assert_eq!(
        payload_str(&limited.payload, "retry_after_source"),
        Some("server")
    );
    assert_eq!(
        payload_str(&limited.payload, "next_poll_at"),
        Some(next.to_rfc3339().as_str())
    );

    let claim = |at: DateTime<Utc>| {
        ctx.db
            .claim_job(&job.id, WorkKind::Poll, ClaimTiming::WhenDue, at, POLL_TTL)
    };
    assert!(claim(Utc::now())?.is_none());
    assert!(ctx.due_ids(Utc::now())?.is_empty());
    let event_count = events.len();
    worker::process_polls(&ctx.config, &ctx.db).await?;
    let untouched = ctx.job(&job.id)?;
    assert_eq!(untouched.attempt, 1);
    assert_eq!(untouched.next_poll_at, Some(next));
    assert_eq!(ctx.events(&job.id)?.len(), event_count);
    assert!(claim(next - Duration::seconds(1))?.is_none());
    assert!(ctx.due_ids(next)?.contains(&job.id));
    let due = claim(next + Duration::seconds(1))?.context("claimable after the cooldown")?;
    assert!(ctx.db.release_lease(&due)?);

    // Without Retry-After the poll backs off on the schedule, indexed by the new attempt.
    let before = Utc::now();
    worker::poll_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend).await?;
    let after = Utc::now();
    let stored = ctx.job(&job.id)?;
    assert_eq!(stored.attempt, 2);
    let delay = Duration::minutes(SCHEDULE_MINUTES[2] as i64);
    let next = stored.next_poll_at.context("rate limit sets a cooldown")?;
    assert_within(
        next,
        before + delay,
        after + delay,
        "poll schedule next_poll_at",
    );
    let events = ctx.events(&job.id)?;
    let limited = events.last().context("poll records an event")?;
    assert_eq!(limited.event_type, "poll_rate_limited");
    assert_eq!(
        payload_str(&limited.payload, "retry_after_source"),
        Some("schedule")
    );
    assert!(claim(next - Duration::seconds(1))?.is_none());
    assert!(claim(next)?.is_some());
    Ok(())
}

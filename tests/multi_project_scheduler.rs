//! OSS-338: one scheduler tick over several projects. Every project is
//! served, the provider budget is machine-wide (it never grows with the
//! number of projects), projects take turns across ticks, and one project's
//! failure never blocks another's work.

mod common;

use anyhow::Result;
use chrono::{Duration, Utc};
use common::{Answer, Fleet, FleetBackends, MockBackend, load_job, ready_review, receipt};
use reviewloop::{
    backend::BackendError,
    model::JobStatus,
    worker::{self, Attempt, RoundRobin, Scheduler, TickBudget, TickReport},
};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
};

fn accepting_backend() -> MockBackend {
    let issued = AtomicUsize::new(0);
    MockBackend::default()
        .on_submit(move || {
            let n = issued.fetch_add(1, Ordering::SeqCst);
            Answer::Now(Ok(receipt(&format!("tok-{n}"))))
        })
        .on_fetch(|| Answer::Now(Ok(ready_review())))
}

fn never() -> bool {
    false
}

async fn tick(
    fleet: &Fleet,
    backends: &FleetBackends,
    turns: &mut RoundRobin,
    number: u64,
) -> TickReport {
    let scheduler = Scheduler {
        db: &fleet.db,
        backends,
        budget: TickBudget::from_config(&fleet.machine),
        stop: &never,
    };
    scheduler
        .tick(&fleet.machine, &fleet.configs(), Some(number), turns)
        .await
}

fn counts(callers: Vec<String>) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for caller in callers {
        *counts.entry(caller).or_default() += 1;
    }
    counts
}

#[tokio::test]
async fn every_project_is_submitted_and_polled() -> Result<()> {
    let mut fleet = Fleet::new(&["alpha", "beta", "gamma"])?;
    fleet.set_budget(3, 3);
    let backends = FleetBackends::new(accepting_backend());
    let queued: Vec<_> = ["alpha", "beta", "gamma"]
        .into_iter()
        .map(|id| fleet.queue_job(id))
        .collect::<Result<_>>()?;

    let report = tick(&fleet, &backends, &mut RoundRobin::default(), 1).await;
    assert!(report.errors().is_empty(), "{:?}", report.errors());
    assert_eq!(report.submits_sent, 3);
    assert_eq!(backends.callers("submit"), ["alpha", "beta", "gamma"]);
    for job in &queued {
        assert_eq!(load_job(&fleet.db, &job.id)?.status, JobStatus::Processing);
    }

    // Due polls of every project complete.
    let past = Utc::now() - Duration::minutes(1);
    let polled: Vec<_> = ["alpha", "beta", "gamma"]
        .into_iter()
        .map(|id| fleet.processing_job(id, &format!("tok-{id}"), past))
        .collect::<Result<_>>()?;
    backends.clear();
    let report = tick(&fleet, &backends, &mut RoundRobin::default(), 2).await;
    assert!(report.errors().is_empty(), "{:?}", report.errors());
    assert_eq!(report.polls_sent, 3);
    assert_eq!(backends.callers("fetch"), ["alpha", "beta", "gamma"]);
    for job in &polled {
        assert_eq!(load_job(&fleet.db, &job.id)?.status, JobStatus::Completed);
    }
    Ok(())
}

/// The acceptance check: four projects with work waiting still get one
/// machine-wide budget per tick, not four.
#[tokio::test]
async fn the_budget_does_not_grow_with_the_number_of_projects() -> Result<()> {
    let ids = ["a", "b", "c", "d"];
    let mut fleet = Fleet::new(&ids)?;
    fleet.set_budget(1, 2);
    let backends = FleetBackends::new(accepting_backend());
    let past = Utc::now() - Duration::minutes(1);
    for id in ids {
        fleet.queue_job(id)?;
        fleet.queue_job(id)?;
        fleet.processing_job(id, &format!("tok-{id}-1"), past)?;
        fleet.processing_job(id, &format!("tok-{id}-2"), past)?;
    }

    let report = tick(&fleet, &backends, &mut RoundRobin::default(), 1).await;
    assert!(report.errors().is_empty(), "{:?}", report.errors());
    assert_eq!(report.submits_sent, 1, "max_submissions_per_tick = 1");
    assert_eq!(report.polls_sent, 2, "max_concurrency = 2");
    assert_eq!(backends.mock.submit_count(), 1);
    assert_eq!(backends.mock.fetch_count(), 2);
    // The two polls went to two different projects.
    assert_eq!(counts(backends.callers("fetch")).len(), 2);
    Ok(())
}

#[tokio::test]
async fn projects_take_turns_across_ticks() -> Result<()> {
    let mut fleet = Fleet::new(&["a", "b", "c"])?;
    fleet.set_budget(1, 1);
    let backends = FleetBackends::new(accepting_backend());
    for id in ["a", "b", "c"] {
        for _ in 0..3 {
            fleet.queue_job(id)?;
        }
    }

    let mut turns = RoundRobin::default();
    for number in 1..=6 {
        tick(&fleet, &backends, &mut turns, number).await;
    }
    assert_eq!(
        backends.callers("submit"),
        ["a", "b", "c", "a", "b", "c"],
        "each project gets a turn before any gets a second"
    );

    // With work in only two of three projects, they still alternate rather
    // than the one after the idle project taking two turns in three.
    let mut fleet = Fleet::new(&["a", "b", "c"])?;
    fleet.set_budget(1, 1);
    let backends = FleetBackends::new(accepting_backend());
    for id in ["a", "c"] {
        for _ in 0..3 {
            fleet.queue_job(id)?;
        }
    }
    let mut turns = RoundRobin::default();
    for number in 1..=4 {
        tick(&fleet, &backends, &mut turns, number).await;
    }
    assert_eq!(backends.callers("submit"), ["a", "c", "a", "c"]);
    Ok(())
}

/// A project whose backend cannot be built fails alone: its claim is
/// released, it spends none of the budget, and the next project submits in
/// the same tick.
#[tokio::test]
async fn a_broken_project_neither_blocks_others_nor_spends_their_budget() -> Result<()> {
    let mut fleet = Fleet::new(&["broken", "healthy"])?;
    fleet.set_budget(1, 1);
    let backends = FleetBackends::new(accepting_backend());
    backends.break_project("broken");
    let stuck = fleet.queue_job("broken")?;
    let served = fleet.queue_job("healthy")?;

    let report = tick(&fleet, &backends, &mut RoundRobin::default(), 1).await;
    assert_eq!(report.submits_sent, 1);
    assert_eq!(backends.callers("submit"), ["healthy"]);
    assert_eq!(
        load_job(&fleet.db, &served.id)?.status,
        JobStatus::Processing
    );
    let stuck = load_job(&fleet.db, &stuck.id)?;
    assert_eq!(stuck.status, JobStatus::Queued);
    assert_eq!(stuck.lease_owner, None, "the failed claim was released");
    assert!(report.projects["healthy"].is_empty());
    let errors = &report.projects["broken"];
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0].contains("no backend for project broken"),
        "{errors:?}"
    );
    Ok(())
}

/// A provider that is down for one project's request is that job's outcome
/// (rescheduled or failed), not an error that stops anyone else.
#[tokio::test]
async fn a_network_failure_is_a_job_outcome_not_a_fleet_failure() -> Result<()> {
    let mut fleet = Fleet::new(&["a", "b"])?;
    fleet.set_budget(2, 2);
    let first = AtomicUsize::new(0);
    let backends = FleetBackends::new(MockBackend::default().on_submit(move || {
        if first.fetch_add(1, Ordering::SeqCst) == 0 {
            Answer::Now(Err(BackendError::RateLimited {
                message: "slow down".to_string(),
                retry_after: None,
            }))
        } else {
            Answer::Now(Ok(receipt("tok-b")))
        }
    }));
    let limited = fleet.queue_job("a")?;
    let accepted = fleet.queue_job("b")?;

    let report = tick(&fleet, &backends, &mut RoundRobin::default(), 1).await;
    assert!(report.errors().is_empty(), "{:?}", report.errors());
    assert_eq!(backends.callers("submit"), ["a", "b"]);
    let limited = load_job(&fleet.db, &limited.id)?;
    assert_eq!(limited.status, JobStatus::Queued);
    assert!(limited.next_poll_at.is_some(), "rescheduled on its own");
    assert_eq!(
        load_job(&fleet.db, &accepted.id)?.status,
        JobStatus::Processing
    );
    Ok(())
}

/// A pause (or shutdown) mid-tick lets the call in flight finish and sends
/// nothing more.
#[tokio::test]
async fn stop_ends_provider_work_for_the_rest_of_the_tick() -> Result<()> {
    let mut fleet = Fleet::new(&["a", "b"])?;
    fleet.set_budget(2, 2);
    let backends = FleetBackends::new(accepting_backend());
    fleet.queue_job("a")?;
    fleet.queue_job("b")?;

    let checks = AtomicUsize::new(0);
    let stop_after_first = || checks.fetch_add(1, Ordering::SeqCst) >= 1;
    let scheduler = Scheduler {
        db: &fleet.db,
        backends: &backends,
        budget: TickBudget::from_config(&fleet.machine),
        stop: &stop_after_first,
    };
    let report = scheduler
        .tick(
            &fleet.machine,
            &fleet.configs(),
            Some(1),
            &mut RoundRobin::default(),
        )
        .await;
    assert!(report.stopped_early);
    assert_eq!(report.submits_sent, 1);
    assert_eq!(report.polls_sent, 0);
    assert_eq!(backends.callers("submit"), ["a"]);
    Ok(())
}

/// `reviewloop run` without a supervisor: its job alone, never ahead of
/// the job's schedule, never another job of the project or the machine.
#[tokio::test]
async fn advance_job_moves_only_its_job_by_its_schedule() -> Result<()> {
    let fleet = Fleet::new(&["mine", "other"])?;
    let backends = FleetBackends::new(accepting_backend());
    let mine = fleet.queue_job("mine")?;
    let sibling = fleet.queue_job("mine")?;
    let elsewhere = fleet.queue_job("other")?;
    let config = fleet.config("mine");

    assert_eq!(
        worker::advance_job(config, &fleet.db, &backends, &mine.id).await?,
        Attempt::Ran
    );
    let submitted = load_job(&fleet.db, &mine.id)?;
    assert_eq!(submitted.status, JobStatus::Processing);
    assert!(submitted.next_poll_at.expect("scheduled") > Utc::now());
    // Not due yet: nothing is sent.
    assert_eq!(
        worker::advance_job(config, &fleet.db, &backends, &mine.id).await?,
        Attempt::NotClaimed
    );
    assert_eq!(backends.mock.fetch_count(), 0);

    fleet
        .db
        .pull_poll_forward(&mine.id, Utc::now() - Duration::seconds(1))?;
    assert_eq!(
        worker::advance_job(config, &fleet.db, &backends, &mine.id).await?,
        Attempt::Ran
    );
    assert_eq!(load_job(&fleet.db, &mine.id)?.status, JobStatus::Completed);
    assert_eq!(backends.callers("submit"), ["mine"]);
    assert_eq!(load_job(&fleet.db, &sibling.id)?.status, JobStatus::Queued);
    assert_eq!(
        load_job(&fleet.db, &elsewhere.id)?.status,
        JobStatus::Queued
    );

    // Another project's job is refused, not run with this config.
    assert!(
        worker::advance_job(config, &fleet.db, &backends, &elsewhere.id)
            .await
            .is_err()
    );
    Ok(())
}

/// A poll whose local work fails after the provider answered (here: the
/// artifact directory cannot be created) still spends budget, so it cannot
/// push the machine past `max_concurrency` calls per tick.
#[tokio::test]
async fn a_call_that_fails_after_reaching_the_provider_spends_budget() -> Result<()> {
    let mut fleet = Fleet::new(&["a", "b"])?;
    fleet.set_budget(2, 2);
    let backends = FleetBackends::new(accepting_backend());
    let past = Utc::now() - Duration::minutes(1);
    let broken = fleet.processing_job("a", "tok-a", past)?;
    for n in 0..4 {
        fleet.processing_job("b", &format!("tok-b{n}"), past)?;
    }
    let artifacts = fleet.state_dir.join("artifacts");
    std::fs::create_dir_all(&artifacts)?;
    std::fs::write(artifacts.join(&broken.id), "not a directory")?;

    let mut turns = RoundRobin::default();
    for number in 1..=2 {
        backends.clear();
        let report = tick(&fleet, &backends, &mut turns, number).await;
        assert_eq!(report.polls_sent, 2, "tick {number}");
        assert_eq!(
            backends.callers("fetch").len(),
            2,
            "tick {number}: {:?}",
            backends.calls()
        );
    }
    Ok(())
}

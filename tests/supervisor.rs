//! OSS-338: the machine supervisor runs only explicitly enabled projects, a
//! disabled project gets no new trigger jobs, one project's broken config
//! fails alone, pause persists and stops all work, a restart settles the
//! submissions a crash interrupted, a second supervisor is refused, and the
//! one-time migration of a single-project install enables only its project.

mod common;

use anyhow::Result;
use chrono::{Duration, Utc};
use common::{
    Answer, Fleet, FleetBackends, MockBackend, SUBMIT_TTL, fleet_project_toml, load_job,
    ready_review, receipt,
};
use reviewloop::{
    config::MachineConfig,
    db::{ClaimTiming, Db},
    model::{JobStatus, SubmitChannel, SubmitStage, WorkKind},
    registry,
    supervisor::{
        LegacyAdoption, ProjectState, StorageMoved, Supervisor, SupervisorLock, SupervisorMemory,
        SupervisorState, TickOutcome, adopt_legacy_binding,
    },
};
use std::{
    fs,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration as StdDuration,
};

const PID: u32 = 4242;

fn accepting_backend() -> MockBackend {
    let issued = AtomicUsize::new(0);
    MockBackend::default()
        .on_submit(move || {
            let n = issued.fetch_add(1, Ordering::SeqCst);
            Answer::Now(Ok(receipt(&format!("tok-{n}"))))
        })
        .on_fetch(|| Answer::Now(Ok(ready_review())))
}

fn supervisor<'a>(
    fleet: &'a Fleet,
    backends: &'a FleetBackends,
    load: &'a dyn Fn() -> Result<MachineConfig>,
) -> Supervisor<'a> {
    Supervisor {
        db: &fleet.db,
        backends,
        load_machine: load,
        interval: StdDuration::from_secs(3600),
        panel: false,
    }
}

async fn tick(
    supervisor: &Supervisor<'_>,
    memory: &mut SupervisorMemory,
    number: u64,
) -> TickOutcome {
    supervisor
        .tick_once(PID, number, memory, &AtomicBool::new(false))
        .await
        .expect("a machine-level tick failure")
}

fn enable(fleet: &Fleet, id: &str) {
    registry::enable(&fleet.db, id, &fleet.config_path(id), false, Utc::now())
        .expect("enable project");
}

fn jobs_of(db: &Db, project_id: &str) -> Vec<reviewloop::model::Job> {
    db.list_project_jobs(project_id, None, false, 100)
        .expect("list jobs")
        .into_iter()
        .map(|(job, _)| job)
        .collect()
}

#[tokio::test]
async fn only_enabled_projects_are_supervised() -> Result<()> {
    let mut fleet = Fleet::new(&["alpha", "beta", "gamma"])?;
    fleet.set_budget(3, 3);
    let widget_dir = fleet.tmp.path().join("widget");
    fleet.machine.core.widget_state_enabled = true;
    fleet.machine.core.widget_state_dir = Some(widget_dir.to_string_lossy().to_string());
    let backends = FleetBackends::new(accepting_backend());
    let load = || Ok(fleet.machine_config());
    let sup = supervisor(&fleet, &backends, &load);
    // Loading a config registers a project; only `enable` makes it run.
    registry::register_seen(&fleet.db, "beta", &fleet.config_path("beta"), Utc::now())?;
    enable(&fleet, "alpha");
    enable(&fleet, "gamma");
    let jobs: Vec<_> = ["alpha", "beta", "gamma"]
        .into_iter()
        .map(|id| fleet.queue_job(id))
        .collect::<Result<_>>()?;

    let TickOutcome::Ran(report) = tick(&sup, &mut SupervisorMemory::default(), 1).await else {
        panic!("not paused");
    };
    assert!(report.errors().is_empty(), "{:?}", report.errors());
    assert_eq!(backends.callers("submit"), ["alpha", "gamma"]);
    assert_eq!(load_job(&fleet.db, &jobs[1].id)?.status, JobStatus::Queued);

    let state = |id: &str| -> Result<ProjectState> {
        Ok(ProjectState::of(
            &fleet.db.get_registered_project(id)?.expect("registered"),
        ))
    };
    assert_eq!(state("alpha")?, ProjectState::Ok);
    assert_eq!(state("beta")?, ProjectState::Disabled);
    assert_eq!(state("gamma")?, ProjectState::Ok);

    // The tick published one document for the whole machine.
    let widget: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(widget_dir.join("widget-state.json"))?)?;
    assert_eq!(widget["project_id"], "");
    assert_eq!(widget["summary"]["active_count"], 3);
    let enabled: Vec<&str> = widget["projects"]
        .as_array()
        .expect("projects")
        .iter()
        .filter(|project| project["enabled"] == true)
        .map(|project| project["project_id"].as_str().expect("id"))
        .collect();
    assert_eq!(enabled, ["alpha", "gamma"]);
    Ok(())
}

/// The acceptance check for disable: a changed PDF of a disabled project
/// produces no new job; enabling it again picks the change up.
#[tokio::test]
async fn disabling_a_project_stops_new_trigger_jobs() -> Result<()> {
    let mut fleet = Fleet::new(&["watched"])?;
    fleet.set_budget(1, 1);
    fs::write(
        &fleet.project("watched").config_path,
        fleet_project_toml("watched", true),
    )?;
    let backends = FleetBackends::new(accepting_backend());
    let load = || Ok(fleet.machine_config());
    let sup = supervisor(&fleet, &backends, &load);
    let mut memory = SupervisorMemory::default();
    enable(&fleet, "watched");

    tick(&sup, &mut memory, 1).await;
    assert_eq!(
        jobs_of(&fleet.db, "watched").len(),
        1,
        "the PDF trigger enqueued"
    );

    registry::disable(&fleet.db, "watched", Utc::now())?;
    let pdf = &fleet.project("watched").pdf_path;
    fs::write(pdf, "%PDF-1.4\n% revised\n%%EOF\n")?;
    tick(&sup, &mut memory, 2).await;
    tick(&sup, &mut memory, 3).await;
    assert_eq!(
        jobs_of(&fleet.db, "watched").len(),
        1,
        "a disabled project's trigger must not enqueue"
    );

    enable(&fleet, "watched");
    tick(&sup, &mut memory, 4).await;
    assert_eq!(jobs_of(&fleet.db, "watched").len(), 2);
    Ok(())
}

#[tokio::test]
async fn a_broken_project_config_fails_alone_and_is_reported_once() -> Result<()> {
    let mut fleet = Fleet::new(&["broken", "healthy"])?;
    fleet.set_budget(2, 2);
    let backends = FleetBackends::new(accepting_backend());
    let load = || Ok(fleet.machine_config());
    let sup = supervisor(&fleet, &backends, &load);
    let mut memory = SupervisorMemory::default();
    enable(&fleet, "broken");
    enable(&fleet, "healthy");
    fleet.queue_job("broken")?;
    fleet.queue_job("healthy")?;
    // A machine-wide setting in a project file is refused by name.
    fs::write(
        &fleet.project("broken").config_path,
        "project_id = \"broken\"\n\n[core]\ndb_path = \"/elsewhere.db\"\n",
    )?;

    for number in 1..=2 {
        let TickOutcome::Ran(report) = tick(&sup, &mut memory, number).await else {
            panic!("not paused");
        };
        assert!(report.projects["healthy"].is_empty());
        assert!(
            report.projects["broken"][0].contains("core.db_path"),
            "{:?}",
            report.projects
        );
    }
    assert_eq!(backends.callers("submit"), ["healthy"]);
    let broken = fleet
        .db
        .get_registered_project("broken")?
        .expect("registered");
    assert_eq!(ProjectState::of(&broken), ProjectState::Error);
    assert!(
        broken
            .health
            .last_error
            .as_deref()
            .unwrap()
            .contains("machine-wide")
    );
    let failures = fleet
        .db
        .list_recent_events_of_type("broken", "tick_failed", 10)?;
    assert_eq!(failures.len(), 1, "one event per new error, not per tick");

    // Fixed: the next tick runs it and clears the error.
    fs::write(
        &fleet.project("broken").config_path,
        fleet_project_toml("broken", false),
    )?;
    tick(&sup, &mut memory, 3).await;
    assert_eq!(backends.callers("submit"), ["healthy", "broken"]);
    let broken = fleet
        .db
        .get_registered_project("broken")?
        .expect("registered");
    assert_eq!(ProjectState::of(&broken), ProjectState::Ok);
    Ok(())
}

/// A project whose file now declares another id is not run under the old one.
#[tokio::test]
async fn a_config_that_changed_its_project_id_is_not_run() -> Result<()> {
    let mut fleet = Fleet::new(&["old-id"])?;
    fleet.set_budget(1, 1);
    let backends = FleetBackends::new(accepting_backend());
    let load = || Ok(fleet.machine_config());
    let sup = supervisor(&fleet, &backends, &load);
    enable(&fleet, "old-id");
    fleet.queue_job("old-id")?;
    fs::write(
        &fleet.project("old-id").config_path,
        fleet_project_toml("new-id", false),
    )?;

    let TickOutcome::Ran(report) = tick(&sup, &mut SupervisorMemory::default(), 1).await else {
        panic!("not paused");
    };
    assert!(report.projects["old-id"][0].contains("now declares project"));
    assert!(backends.calls().is_empty());
    Ok(())
}

#[tokio::test]
async fn pause_persists_and_stops_triggers_and_providers() -> Result<()> {
    let mut fleet = Fleet::new(&["p"])?;
    fleet.set_budget(1, 1);
    fs::write(
        &fleet.project("p").config_path,
        fleet_project_toml("p", true),
    )?;
    let backends = FleetBackends::new(accepting_backend());
    let load = || Ok(fleet.machine_config());
    enable(&fleet, "p");
    fleet.db.set_supervisor_paused(true, Utc::now())?;

    let sup = supervisor(&fleet, &backends, &load);
    assert!(matches!(
        tick(&sup, &mut SupervisorMemory::default(), 1).await,
        TickOutcome::Paused
    ));
    // A restarted supervisor (fresh memory) is still paused.
    let restarted = supervisor(&fleet, &backends, &load);
    assert!(matches!(
        tick(&restarted, &mut SupervisorMemory::default(), 1).await,
        TickOutcome::Paused
    ));
    assert!(
        jobs_of(&fleet.db, "p").is_empty(),
        "no trigger ran while paused"
    );
    assert!(backends.calls().is_empty());

    fleet.db.set_supervisor_paused(false, Utc::now())?;
    tick(&restarted, &mut SupervisorMemory::default(), 2).await;
    assert_eq!(jobs_of(&fleet.db, "p").len(), 1);
    assert_eq!(backends.callers("submit"), ["p"]);
    Ok(())
}

/// The acceptance check for restart recovery: a supervisor that died mid-
/// submission leaves leases behind; the next one settles them. A claim that
/// never dispatched goes back to the queue and is submitted; a dispatched
/// one whose outcome is unknown is parked, never sent twice.
#[tokio::test]
async fn a_restarted_supervisor_settles_interrupted_submissions() -> Result<()> {
    let mut fleet = Fleet::new(&["p"])?;
    fleet.set_budget(1, 1);
    let backends = FleetBackends::new(accepting_backend());
    let load = || Ok(fleet.machine_config());
    enable(&fleet, "p");
    let crashed_at = Utc::now() - SUBMIT_TTL - Duration::minutes(1);

    let dispatched = fleet.queue_job("p")?;
    let mut lease = fleet
        .db
        .claim_job(
            &dispatched.id,
            WorkKind::Submit,
            ClaimTiming::Now,
            crashed_at,
            SUBMIT_TTL,
        )?
        .expect("claim");
    assert!(fleet.db.begin_submit_dispatch(
        &mut lease,
        SubmitChannel::Primary,
        crashed_at,
        SUBMIT_TTL
    )?);
    let claimed = fleet.queue_job("p")?;
    fleet
        .db
        .claim_job(
            &claimed.id,
            WorkKind::Submit,
            ClaimTiming::Now,
            crashed_at,
            SUBMIT_TTL,
        )?
        .expect("claim");

    let sup = supervisor(&fleet, &backends, &load);
    tick(&sup, &mut SupervisorMemory::default(), 1).await;

    let dispatched = load_job(&fleet.db, &dispatched.id)?;
    assert_eq!(dispatched.status, JobStatus::Submitted);
    assert_eq!(dispatched.submit_stage, Some(SubmitStage::Uncertain));
    let claimed = load_job(&fleet.db, &claimed.id)?;
    assert_eq!(
        claimed.status,
        JobStatus::Processing,
        "requeued and submitted"
    );
    assert_eq!(
        backends.mock.submit_count(),
        1,
        "the uncertain job was not resent"
    );
    Ok(())
}

#[tokio::test]
async fn a_second_supervisor_is_refused_and_a_clean_stop_releases_the_lock() -> Result<()> {
    let fleet = Fleet::new(&["p"])?;
    let backends = FleetBackends::new(accepting_backend());
    let load = || Ok(fleet.machine_config());
    let first = supervisor(&fleet, &backends, &load);
    let second = supervisor(&fleet, &backends, &load);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();

    let (first_result, second_result) = tokio::join!(
        first.run(async move {
            let _ = stopped.await;
        }),
        async {
            wait_until(|| {
                fleet
                    .db
                    .supervisor_record()
                    .is_ok_and(|record| record.started_at.is_some())
            })
            .await;
            let refused = second.run(std::future::pending()).await;
            let _ = stop.send(());
            refused
        }
    );
    first_result?;
    let err = second_result.expect_err("the second supervisor must not start");
    assert!(format!("{err:#}").contains("already running"), "{err:#}");

    let record = fleet.db.supervisor_record()?;
    assert!(record.stopped_at.is_some());
    assert_eq!(
        SupervisorState::of(&record, Utc::now()),
        SupervisorState::Stopped
    );
    SupervisorLock::acquire(&fleet.state_dir).expect("released on stop");
    Ok(())
}

/// One database takes one supervisor, even from another state dir.
#[tokio::test]
async fn a_database_supervised_from_another_state_dir_is_refused() -> Result<()> {
    let fleet = Fleet::new(&["p"])?;
    let backends = FleetBackends::new(accepting_backend());
    let load = || Ok(fleet.machine_config());
    fleet.db.claim_supervisor(
        1,
        &fleet.tmp.path().join("other-state"),
        "0.0.0",
        Utc::now(),
        |_| false,
    )?;

    let err = supervisor(&fleet, &backends, &load)
        .run(std::future::pending())
        .await
        .expect_err("must refuse");
    assert!(format!("{err:#}").contains("already supervised"), "{err:#}");
    Ok(())
}

/// Enabling a project wakes a sleeping supervisor long before its next
/// scheduled tick; shutdown ends the loop.
#[tokio::test]
async fn enabling_wakes_the_supervisor_and_shutdown_stops_it() -> Result<()> {
    let mut fleet = Fleet::new(&["p"])?;
    fleet.set_budget(1, 1);
    let backends = FleetBackends::new(accepting_backend());
    let load = || Ok(fleet.machine_config());
    let sup = supervisor(&fleet, &backends, &load);
    let job = fleet.queue_job("p")?;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();

    let (result, ()) = tokio::join!(
        sup.run(async move {
            let _ = stopped.await;
        }),
        async {
            wait_until(|| {
                fleet
                    .db
                    .supervisor_record()
                    .is_ok_and(|record| record.last_tick_at.is_some())
            })
            .await;
            enable(&fleet, "p");
            wait_until(|| {
                load_job(&fleet.db, &job.id).is_ok_and(|job| job.status == JobStatus::Processing)
            })
            .await;
            let _ = stop.send(());
        }
    );
    result?;
    assert_eq!(backends.callers("submit"), ["p"]);
    Ok(())
}

#[tokio::test]
async fn a_global_config_that_moves_the_database_stops_the_supervisor() -> Result<()> {
    let fleet = Fleet::new(&["p"])?;
    let backends = FleetBackends::new(accepting_backend());
    let load = || {
        let mut global = fleet.global();
        global.core.db_path = fleet
            .tmp
            .path()
            .join("moved.db")
            .to_string_lossy()
            .to_string();
        MachineConfig::from_global(global)
    };
    let err = supervisor(&fleet, &backends, &load)
        .tick_once(
            PID,
            1,
            &mut SupervisorMemory::default(),
            &AtomicBool::new(false),
        )
        .await
        .expect_err("storage moved");
    assert!(err.is::<StorageMoved>(), "{err:#}");
    Ok(())
}

/// The acceptance check for migrating a single-project install: the bound
/// project keeps running; no other registered project starts; an explicit
/// later disable is never undone by the old plist's `--config`.
#[test]
fn migrating_a_single_project_install_enables_only_its_project() -> Result<()> {
    let fleet = Fleet::new(&["bound", "other"])?;
    let machine = fleet.machine_config();
    registry::register_seen(&fleet.db, "other", &fleet.config_path("other"), Utc::now())?;

    let (id, adoption) = adopt_legacy_binding(
        &fleet.db,
        &machine,
        &fleet.project("bound").config_path,
        Utc::now(),
    )?;
    assert_eq!(
        (id.as_str(), adoption),
        ("bound", LegacyAdoption::Enabled { moved_from: None })
    );
    let enabled: Vec<String> = fleet
        .db
        .list_registered_projects()?
        .into_iter()
        .filter(|project| project.enabled)
        .map(|project| project.project_id)
        .collect();
    assert_eq!(enabled, ["bound"]);

    let again = adopt_legacy_binding(
        &fleet.db,
        &machine,
        &fleet.project("bound").config_path,
        Utc::now(),
    )?;
    assert_eq!(again.1, LegacyAdoption::AlreadyDecided { enabled: true });
    registry::disable(&fleet.db, "bound", Utc::now())?;
    let after_disable = adopt_legacy_binding(
        &fleet.db,
        &machine,
        &fleet.project("bound").config_path,
        Utc::now(),
    )?;
    assert_eq!(
        after_disable.1,
        LegacyAdoption::AlreadyDecided { enabled: false }
    );
    Ok(())
}

/// Before OSS-338 the registry kept whichever clone loaded last, so the row
/// can point at another worktree than the one the daemon was bound to. The
/// undecided registration follows the explicit binding; a decided one stays.
#[test]
fn the_binding_wins_over_an_undecided_registration_of_another_clone() -> Result<()> {
    let fleet = Fleet::new(&["p"])?;
    let machine = fleet.machine_config();
    let clone = fleet.tmp.path().join("clone");
    fs::create_dir_all(&clone)?;
    fs::write(
        clone.join("reviewloop.toml"),
        fleet_project_toml("p", false),
    )?;
    fs::copy(&fleet.project("p").pdf_path, clone.join("paper.pdf"))?;
    let clone_config = fs::canonicalize(clone.join("reviewloop.toml"))?;
    registry::register_seen(&fleet.db, "p", &clone_config, Utc::now())?;

    let (_, adoption) = adopt_legacy_binding(
        &fleet.db,
        &machine,
        &fleet.project("p").config_path,
        Utc::now(),
    )?;
    assert_eq!(
        adoption,
        LegacyAdoption::Enabled {
            moved_from: Some(clone_config.clone())
        }
    );
    let row = fleet.db.get_registered_project("p")?.expect("row");
    assert!(row.enabled);
    assert_eq!(row.config_path, fleet.config_path("p"));

    // Once decided, a binding to the other clone changes nothing.
    let (_, again) = adopt_legacy_binding(&fleet.db, &machine, &clone_config, Utc::now())?;
    assert_eq!(again, LegacyAdoption::AlreadyDecided { enabled: true });
    assert_eq!(
        fleet
            .db
            .get_registered_project("p")?
            .expect("row")
            .config_path,
        fleet.config_path("p")
    );
    Ok(())
}

/// A tick that cannot load the global config is reported once (event and
/// tick error) and kept in the widget; a clean stop marks the widget stopped.
#[tokio::test]
async fn a_machine_failure_is_reported_and_a_stop_reaches_the_widget() -> Result<()> {
    let mut fleet = Fleet::new(&["p"])?;
    let widget_dir = fleet.tmp.path().join("widget");
    fleet.machine.core.widget_state_enabled = true;
    fleet.machine.core.widget_state_dir = Some(widget_dir.to_string_lossy().to_string());
    let backends = FleetBackends::new(accepting_backend());
    let loads = AtomicUsize::new(0);
    let load = || {
        if loads.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(fleet.machine_config())
        } else {
            anyhow::bail!("global config: invalid TOML at line 3")
        }
    };
    let mut sup = supervisor(&fleet, &backends, &load);
    sup.interval = StdDuration::from_millis(20);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let widget = || -> Option<serde_json::Value> {
        serde_json::from_str(&fs::read_to_string(widget_dir.join("widget-state.json")).ok()?).ok()
    };

    let (result, ()) = tokio::join!(
        sup.run(async move {
            let _ = stopped.await;
        }),
        async {
            wait_until(|| {
                widget().is_some_and(|doc| {
                    doc["last_tick_error"]["message"]
                        .as_str()
                        .is_some_and(|message| message.contains("invalid TOML"))
                })
            })
            .await;
            // Several failing ticks pass before the stop.
            wait_until(|| loads.load(Ordering::SeqCst) >= 4).await;
            let _ = stop.send(());
        }
    );
    result?;
    let failures = fleet.db.list_recent_events_of_type("", "tick_failed", 10)?;
    assert_eq!(failures.len(), 1, "one event per new error, not per tick");
    assert!(
        fleet
            .db
            .supervisor_record()?
            .current_tick_error()
            .is_some_and(|error| error.contains("invalid TOML"))
    );
    assert_eq!(widget().expect("widget")["supervisor"]["state"], "stopped");
    Ok(())
}

async fn wait_until(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(StdDuration::from_secs(20), async {
        while !ready() {
            tokio::time::sleep(StdDuration::from_millis(20)).await;
        }
    })
    .await
    .expect("condition not reached in time");
}

/// Another live supervisor took the control row (this one's heartbeat went
/// stale across a sleep): this one stops instead of running beside it.
#[tokio::test]
async fn a_displaced_supervisor_stops() -> Result<()> {
    let fleet = Fleet::new(&["p"])?;
    let backends = FleetBackends::new(accepting_backend());
    let load = || Ok(fleet.machine_config());
    fleet.db.claim_supervisor(
        PID + 1,
        &fleet.tmp.path().join("elsewhere"),
        "0.0.0",
        Utc::now(),
        |_| false,
    )?;
    let err = supervisor(&fleet, &backends, &load)
        .tick_once(
            PID,
            1,
            &mut SupervisorMemory::default(),
            &AtomicBool::new(false),
        )
        .await
        .expect_err("displaced");
    assert!(err.is::<reviewloop::supervisor::Displaced>(), "{err:#}");
    Ok(())
}

/// v0.2.1 wrote the fully resolved config path into the plist. When the
/// repository's `reviewloop.toml` is a symlink (into a dotfiles repo, say),
/// the binding names the target; the project stays at the link, with the
/// repository as its root.
#[cfg(unix)]
#[test]
fn a_binding_to_a_symlink_target_keeps_the_link() -> Result<()> {
    let fleet = Fleet::new(&["p"])?;
    let machine = fleet.machine_config();
    let dotfiles = fleet.tmp.path().join("dotfiles");
    fs::create_dir_all(&dotfiles)?;
    let link = fleet.project("p").config_path.clone();
    fs::rename(&link, dotfiles.join("reviewloop.toml"))?;
    std::os::unix::fs::symlink(dotfiles.join("reviewloop.toml"), &link)?;
    let link = reviewloop::config::canonical_config_path(&link)?;
    registry::register_seen(&fleet.db, "p", &link, Utc::now())?;

    let target = fs::canonicalize(dotfiles.join("reviewloop.toml"))?;
    let (_, adoption) = adopt_legacy_binding(&fleet.db, &machine, &target, Utc::now())?;
    assert_eq!(adoption, LegacyAdoption::Enabled { moved_from: None });
    let row = fleet.db.get_registered_project("p")?.expect("row");
    assert!(row.enabled);
    assert_eq!(row.config_path, link);
    assert_eq!(
        machine.project(&row.config_path)?.project_root.as_deref(),
        link.parent()
    );
    Ok(())
}

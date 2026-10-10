use crate::{
    config::Config,
    db::Db,
    supervisor::{ProjectState, TickOutcome},
    worker::TickBudget,
};
use anyhow::Result;
use chrono::Utc;
use std::io::{Write, stdout};

/// The foreground supervisor's terminal panel, repainted after every tick:
/// one row per registered project plus the machine-wide budget.
pub fn render_supervisor_panel(
    machine: &Config,
    db: &Db,
    tick: u64,
    outcome: &TickOutcome,
) -> Result<()> {
    let projects = db.list_registered_projects()?;
    let budget = TickBudget::from_config(machine);

    // Clear screen and paint a lightweight terminal panel.
    print!("\x1B[2J\x1B[H");
    println!("ReviewLoop Supervisor (foreground)");
    println!("time: {}", Utc::now().to_rfc3339());
    println!(
        "tick: {tick}{}",
        if matches!(outcome, TickOutcome::Paused) {
            " (paused: `reviewloop daemon resume` to continue)"
        } else {
            ""
        }
    );
    println!();

    let (enabled, disabled): (Vec<_>, Vec<_>) =
        projects.iter().partition(|project| project.enabled);
    println!("Projects ({} enabled)", enabled.len());
    if enabled.is_empty() {
        println!("- none: run `reviewloop project enable` in a project repo");
    }
    for project in &enabled {
        let counts = db.status_counts(&project.project_id)?;
        let count = |status: &str| counts.get(status).copied().unwrap_or(0);
        let failed = count("FAILED") + count("FAILED_NEEDS_MANUAL") + count("TIMEOUT");
        println!(
            "- {} [{}] pending approval {} · queued {} · submitted {} · processing {} · completed {} · failed {}",
            project.project_id,
            ProjectState::of(project).as_str(),
            count("PENDING_APPROVAL"),
            count("QUEUED"),
            count("SUBMITTED"),
            count("PROCESSING"),
            count("COMPLETED"),
            failed
        );
        if let Some(error) = &project.health.last_error {
            println!("    error: {error}");
        }
    }
    if !disabled.is_empty() {
        println!("({} registered project(s) disabled)", disabled.len());
    }
    println!();

    println!("Machine-wide budget per tick");
    println!("- submissions : {}", budget.submits);
    println!("- polls       : {}", budget.polls);
    println!(
        "- poll schedule (min) : {:?}",
        machine.polling.schedule_minutes
    );
    println!();

    println!("Last Tick");
    match outcome {
        TickOutcome::Paused => println!("- paused: no triggers, submissions or polls"),
        TickOutcome::Ran(report) => {
            println!(
                "- sent: {} submission(s), {} poll(s)",
                report.submits_sent, report.polls_sent
            );
            if report.machine_errors.is_empty() {
                println!("- machine errors: none");
            }
            for error in &report.machine_errors {
                println!("- machine error: {error}");
            }
        }
    }
    println!();
    println!("Press Ctrl+C to stop the supervisor.");

    stdout().flush()?;
    Ok(())
}

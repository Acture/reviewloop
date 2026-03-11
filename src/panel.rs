use crate::{config::Config, db::Db};
use anyhow::Result;
use chrono::Utc;
use std::io::{Write, stdout};

pub fn render_tick_panel(
    config: &Config,
    db: &Db,
    tick: u64,
    last_tick_error: Option<&str>,
) -> Result<()> {
    let counts = db.status_counts(&config.project_id)?;

    let pending = counts.get("PENDING_APPROVAL").copied().unwrap_or(0);
    let queued = counts.get("QUEUED").copied().unwrap_or(0);
    let submitted = counts.get("SUBMITTED").copied().unwrap_or(0);
    let processing = counts.get("PROCESSING").copied().unwrap_or(0);
    let completed = counts.get("COMPLETED").copied().unwrap_or(0);
    let failed = counts.get("FAILED").copied().unwrap_or(0)
        + counts.get("FAILED_NEEDS_MANUAL").copied().unwrap_or(0)
        + counts.get("TIMEOUT").copied().unwrap_or(0);

    // Clear screen and paint a lightweight terminal panel.
    print!("\x1B[2J\x1B[H");
    println!("ReviewLoop Monitor (foreground)");
    println!("time: {}", Utc::now().to_rfc3339());
    println!("tick: {tick}");
    println!("project_id: {}", config.project_id);
    println!("backend: stanford (paperreview.ai)");
    println!();
    println!("Jobs");
    println!("- pending approval : {pending}");
    println!("- queued           : {queued}");
    println!("- submitted        : {submitted}");
    println!("- processing       : {processing}");
    println!("- completed        : {completed}");
    println!("- failed/timeout   : {failed}");
    println!();
    println!("Guardrails");
    println!(
        "- max submissions/tick : {}",
        config.core.max_submissions_per_tick
    );
    println!("- max concurrency      : {}", config.core.max_concurrency);
    println!(
        "- pdf scan papers/tick : {}",
        config.trigger.pdf.max_scan_papers
    );
    println!(
        "- poll schedule (min)  : {:?}",
        config.polling.schedule_minutes
    );
    println!();

    if let Some(err) = last_tick_error {
        println!("Last Tick Error");
        println!("- {err}");
    } else {
        println!("Last Tick Error");
        println!("- none");
    }
    println!();
    println!("Press Ctrl+C to stop daemon.");

    stdout().flush()?;
    Ok(())
}

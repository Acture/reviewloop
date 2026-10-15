//! `reviewloop retry --all-failed` through the real CLI, in an isolated home,
//! config dir and state dir.

use anyhow::Result;
use reviewloop::{
    db::Db,
    model::{Job, JobPdf, JobStatus, NewJob},
};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

struct Machine {
    _tmp: tempfile::TempDir,
    root: PathBuf,
}

impl Machine {
    fn new() -> Result<Self> {
        let tmp = tempfile::tempdir()?;
        let root = tmp.path().canonicalize()?;
        fs::create_dir_all(root.join("home"))?;
        Ok(Self { _tmp: tmp, root })
    }

    fn state_dir(&self) -> PathBuf {
        self.root.join("state")
    }

    /// Run `reviewloop args` from `cwd`.
    fn run(&self, cwd: &Path, args: &[&str]) -> Result<Output> {
        Ok(Command::new(env!("CARGO_BIN_EXE_reviewloop"))
            .args(args)
            .current_dir(cwd)
            .env("HOME", self.root.join("home"))
            .env("XDG_CONFIG_HOME", self.root.join("xdg"))
            .env("REVIEWLOOP_STATE_DIR", self.state_dir())
            .env_remove("REVIEWLOOP_CSPAPER_API_KEY")
            .env_remove("RUST_LOG")
            .output()?)
    }

    fn ok(&self, cwd: &Path, args: &[&str]) -> Result<String> {
        let output = self.run(cwd, args)?;
        assert!(
            output.status.success(),
            "reviewloop {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(String::from_utf8(output.stdout)?)
    }

    /// A repository with a `reviewloop.toml` declaring `project_id`.
    fn repo(&self, name: &str, project_id: &str) -> Result<PathBuf> {
        let dir = self.root.join(name);
        fs::create_dir_all(dir.join(".git"))?;
        fs::write(
            dir.join("reviewloop.toml"),
            format!("project_id = \"{project_id}\"\n"),
        )?;
        Ok(dir)
    }

    /// The database the CLI uses, for seeding and inspection.
    fn db(&self) -> Result<Db> {
        fs::create_dir_all(self.state_dir())?;
        let db = Db::new(&self.state_dir());
        db.ensure_schema()?;
        Ok(db)
    }
}

/// A job of `project_id` for `paper` on stanford, created in `status`.
fn job(db: &Db, project_id: &str, paper: &str, status: JobStatus) -> Result<Job> {
    // Created rows take their `created_at` from the wall clock; keep them apart.
    std::thread::sleep(std::time::Duration::from_millis(5));
    db.create_job(&NewJob {
        project_id: project_id.to_string(),
        paper_id: paper.to_string(),
        backend: "stanford".to_string(),
        pdf: JobPdf::Unpinned {
            pdf_path: "paper.pdf".to_string(),
            pdf_hash: format!("{project_id}-{paper}-{}", status.as_str()),
        },
        status,
        email: "author@example.edu".to_string(),
        venue: None,
        review_options: Default::default(),
        git_tag: None,
        git_commit: None,
        next_poll_at: None,
    })
}

#[test]
fn retry_all_failed_requeues_the_projects_failed_jobs_and_lists_the_skipped() -> Result<()> {
    let machine = Machine::new()?;
    let empty = machine.repo("empty", "empty")?;
    let out = machine.ok(&empty, &["retry", "--all-failed"])?;
    assert!(out.contains("No failed jobs in project empty"), "{out}");

    let repo = machine.repo("thesis", "thesis")?;
    let db = machine.db()?;
    let superseded = job(&db, "thesis", "main", JobStatus::Failed)?;
    let newest = job(&db, "thesis", "main", JobStatus::Timeout)?;
    let needs_manual = job(&db, "thesis", "appendix", JobStatus::FailedNeedsManual)?;
    let foreign = job(&db, "other", "main", JobStatus::Failed)?;

    let out = machine.ok(&repo, &["retry", "--all-failed"])?;
    for expected in [
        format!(
            "Re-queued job {} (paper main): it joins the end of the queue and is submitted in turn",
            newest.id
        ),
        format!(
            "Skipped job {} (paper main, FAILED): superseded by newer job {} (TIMEOUT)",
            superseded.id, newest.id
        ),
        format!(
            "Skipped job {} (paper appendix, FAILED_NEEDS_MANUAL): it needs attention first, and its last error says how to recover: fix the cause and run `reviewloop retry --job-id {}`, or request a new review with `reviewloop submit --paper-id appendix --force`",
            needs_manual.id, needs_manual.id
        ),
        "Retried 1 failed job(s) of project thesis; skipped 2".to_string(),
        "worker: ".to_string(),
    ] {
        assert!(out.contains(&expected), "missing {expected:?} in:\n{out}");
    }

    let requeued = db.get_job(&newest.id)?.expect("job");
    assert_eq!(requeued.status, JobStatus::Queued);
    assert!(requeued.next_poll_at.is_some(), "its place: the retry");
    for job in [&superseded, &needs_manual, &foreign] {
        let after = db.get_job(&job.id)?.expect("job");
        assert_eq!(
            (after.status, after.updated_at),
            (job.status, job.updated_at)
        );
    }
    Ok(())
}

#[test]
fn retry_all_failed_takes_no_job_reference_and_no_now() -> Result<()> {
    let machine = Machine::new()?;
    let repo = machine.repo("thesis", "thesis")?;
    for args in [
        &["retry", "--all-failed", "--now"][..],
        &["retry", "--all-failed", "--include-failed"],
        &["retry", "--all-failed", "--job-id", "some-job"],
        &["retry", "--all-failed", "--paper-id", "main"],
        &["retry"],
    ] {
        let output = machine.run(&repo, args)?;
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("cannot be used with")
                || stderr.contains("the following required arguments were not provided"),
            "{args:?}: {stderr}"
        );
    }
    Ok(())
}

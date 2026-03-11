use anyhow::{Context, Result};
use reviewloop::{
    config::{Config, PaperConfig},
    db::Db,
    model::JobStatus,
    trigger::{run_git_tag_trigger, run_pdf_trigger},
};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

struct GitTriggerTestContext {
    _tmp: tempfile::TempDir,
    repo_dir: PathBuf,
    config: Config,
    db: Db,
}

impl GitTriggerTestContext {
    fn new() -> Result<Self> {
        if !git_available() {
            anyhow::bail!("git is not available in PATH");
        }

        let tmp = tempfile::tempdir()?;
        let repo_dir = tmp.path().join("repo");
        fs::create_dir_all(&repo_dir)?;

        run_git(&repo_dir, &["init"])?;
        run_git(&repo_dir, &["config", "user.email", "test@example.com"])?;
        run_git(&repo_dir, &["config", "user.name", "ReviewLoop Test"])?;

        let pdf_main = repo_dir.join("main.pdf");
        fs::write(&pdf_main, b"%PDF-1.4\n%%EOF\n")?;
        run_git(&repo_dir, &["add", "."])?;
        run_git(&repo_dir, &["commit", "-m", "initial"])?;

        let state_dir = tmp.path().join("state");
        fs::create_dir_all(&state_dir)?;

        let mut config = Config {
            project_id: "project-git-trigger".to_string(),
            ..Config::default()
        };
        config.core.state_dir = state_dir.to_string_lossy().to_string();
        config.trigger.git.enabled = true;
        config.trigger.git.repo_dir = repo_dir.to_string_lossy().to_string();
        config.trigger.pdf.enabled = false;
        config.imap = None;
        config.providers.stanford.email = "test@example.edu".to_string();
        config.papers = vec![PaperConfig {
            id: "main".to_string(),
            pdf_path: pdf_main.to_string_lossy().to_string(),
            backend: "stanford".to_string(),
        }];

        let db = Db::new(Path::new(&config.core.state_dir));
        db.init_schema()?;

        Ok(Self {
            _tmp: tmp,
            repo_dir,
            config,
            db,
        })
    }

    fn add_second_paper(&mut self, paper_id: &str) -> Result<()> {
        let path = self.repo_dir.join(format!("{paper_id}.pdf"));
        fs::write(&path, b"%PDF-1.4\n%%EOF\n")?;
        run_git(&self.repo_dir, &["add", "."])?;
        run_git(&self.repo_dir, &["commit", "-m", "add second paper"])?;
        self.config.papers.push(PaperConfig {
            id: paper_id.to_string(),
            pdf_path: path.to_string_lossy().to_string(),
            backend: "stanford".to_string(),
        });
        Ok(())
    }

    fn create_tag(&self, tag: &str) -> Result<()> {
        run_git(&self.repo_dir, &["tag", tag])
    }
}

fn git_available() -> bool {
    Command::new("git")
        .arg("--version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}

fn run_git(repo_dir: &Path, args: &[&str]) -> Result<()> {
    let output = Command::new("git")
        .args([
            "-C",
            repo_dir.to_string_lossy().as_ref(),
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .output()
        .with_context(|| format!("failed to execute git command: {:?}", args))?;

    if !output.status.success() {
        anyhow::bail!(
            "git command failed: {:?}\nstdout: {}\nstderr: {}",
            args,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(())
}

#[test]
fn git_trigger_enqueues_job_and_deduplicates_seen_tag() -> Result<()> {
    let ctx = GitTriggerTestContext::new()?;
    ctx.create_tag("review-stanford/main/v1")?;

    run_git_tag_trigger(&ctx.config, &ctx.db)?;

    let job = ctx
        .db
        .find_latest_open_job_for_paper(&ctx.config.project_id, "main")?
        .context("expected a queued job")?;
    assert_eq!(job.status, JobStatus::Queued);
    assert_eq!(job.git_tag.as_deref(), Some("review-stanford/main/v1"));
    assert!(job.git_commit.as_deref().unwrap_or_default().len() >= 7);

    run_git_tag_trigger(&ctx.config, &ctx.db)?;
    let rows = ctx
        .db
        .list_status_views(&ctx.config.project_id, Some("main"))?;
    assert_eq!(
        rows.len(),
        1,
        "tag should only enqueue once due to seen_tags"
    );

    Ok(())
}

#[test]
fn git_trigger_shorthand_tag_routes_to_first_backend_paper() -> Result<()> {
    let mut ctx = GitTriggerTestContext::new()?;
    ctx.add_second_paper("aux")?;
    ctx.create_tag("review-stanford/v2")?;

    run_git_tag_trigger(&ctx.config, &ctx.db)?;

    let job = ctx
        .db
        .find_latest_open_job_for_paper(&ctx.config.project_id, "main")?
        .context("expected shorthand tag to map to first stanford paper")?;
    assert_eq!(job.paper_id, "main");
    assert_eq!(job.git_tag.as_deref(), Some("review-stanford/v2"));

    Ok(())
}

#[test]
fn git_trigger_ignores_tags_without_matching_backend() -> Result<()> {
    let ctx = GitTriggerTestContext::new()?;
    ctx.create_tag("review-other/main/v1")?;

    run_git_tag_trigger(&ctx.config, &ctx.db)?;

    let jobs = ctx.db.list_status_views(&ctx.config.project_id, None)?;
    assert!(
        jobs.is_empty(),
        "unknown backend tags must not enqueue jobs"
    );

    Ok(())
}

#[test]
fn git_trigger_auto_delete_processed_tag_removes_local_tag() -> Result<()> {
    let mut ctx = GitTriggerTestContext::new()?;
    ctx.config.trigger.git.auto_delete_processed_tags = true;
    ctx.create_tag("review-stanford/main/cleanup")?;

    run_git_tag_trigger(&ctx.config, &ctx.db)?;

    let output = Command::new("git")
        .args([
            "-C",
            ctx.repo_dir.to_string_lossy().as_ref(),
            "tag",
            "--list",
        ])
        .output()?;
    let tags = String::from_utf8_lossy(&output.stdout);
    assert!(
        !tags
            .lines()
            .any(|t| t.trim() == "review-stanford/main/cleanup"),
        "processed tag should be deleted when auto_delete_processed_tags is enabled"
    );
    Ok(())
}

#[test]
fn pdf_trigger_auto_create_tag_records_git_metadata_on_job() -> Result<()> {
    let mut ctx = GitTriggerTestContext::new()?;
    ctx.config.trigger.git.auto_create_tags_on_pdf_change = true;
    ctx.config.trigger.pdf.enabled = true;

    run_pdf_trigger(&ctx.config, &ctx.db)?;

    let job = ctx
        .db
        .find_latest_open_job_for_paper(&ctx.config.project_id, "main")?
        .context("expected job from pdf trigger")?;
    let tag = job
        .git_tag
        .clone()
        .context("expected auto-created git tag")?;
    assert!(tag.starts_with("review-stanford/main/auto-"));
    assert!(job.git_commit.as_deref().unwrap_or_default().len() >= 7);

    let output = Command::new("git")
        .args([
            "-C",
            ctx.repo_dir.to_string_lossy().as_ref(),
            "tag",
            "--list",
        ])
        .output()?;
    let tags = String::from_utf8_lossy(&output.stdout);
    assert!(
        tags.lines().any(|t| t.trim() == tag),
        "auto-created tag should exist in git repository"
    );

    Ok(())
}

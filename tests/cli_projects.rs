//! OSS-338 through the real CLI, in an isolated home, config dir and state
//! dir: `project enable / disable / list`, the duplicate-clone conflict,
//! `daemon status` and `daemon pause / resume` without a running supervisor,
//! and the refusal of a machine-wide setting in a project file.

use anyhow::Result;
use serde_json::Value;
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

    /// Run `reviewloop args` from `cwd`.
    fn run(&self, cwd: &Path, args: &[&str]) -> Result<Output> {
        Ok(Command::new(env!("CARGO_BIN_EXE_reviewloop"))
            .args(args)
            .current_dir(cwd)
            .env("HOME", self.root.join("home"))
            .env("XDG_CONFIG_HOME", self.root.join("xdg"))
            .env("REVIEWLOOP_STATE_DIR", self.root.join("state"))
            .env_remove("REVIEWLOOP_CSPAPER_API_KEY")
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

    fn json(&self, cwd: &Path, args: &[&str]) -> Result<Value> {
        Ok(serde_json::from_str(&self.ok(cwd, args)?)?)
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
}

fn project<'a>(list: &'a Value, id: &str) -> &'a Value {
    list["projects"]
        .as_array()
        .expect("projects")
        .iter()
        .find(|project| project["project_id"] == id)
        .unwrap_or_else(|| panic!("no project {id} in {list}"))
}

#[test]
fn enable_disable_and_list_through_the_cli() -> Result<()> {
    let machine = Machine::new()?;
    let repo = machine.repo("thesis", "thesis")?;
    let config = repo.join("reviewloop.toml").canonicalize()?;

    // Loading the config registers it, but nothing runs it yet.
    machine.ok(&repo, &["status"])?;
    let list = machine.json(&machine.root, &["project", "list", "--json"])?;
    assert_eq!(project(&list, "thesis")["enabled"], false);
    assert_eq!(list["supervisor"]["state"], "stopped");

    let enabled = machine.ok(&repo, &["project", "enable"])?;
    assert!(enabled.contains("Enabled project thesis"), "{enabled}");
    assert!(enabled.contains("no supervisor is running"), "{enabled}");
    let list = machine.json(&machine.root, &["project", "list", "--json"])?;
    let thesis = project(&list, "thesis");
    assert_eq!(thesis["enabled"], true);
    assert_eq!(thesis["state"], "pending");
    assert_eq!(thesis["config_path"], config.display().to_string());

    let disabled = machine.ok(
        &machine.root,
        &["project", "disable", "--project-id", "thesis"],
    )?;
    assert!(disabled.contains("Disabled project thesis"), "{disabled}");
    let list = machine.json(&machine.root, &["project", "list", "--json"])?;
    assert_eq!(project(&list, "thesis")["enabled"], false);

    // Enable by id from anywhere, once registered.
    machine.ok(
        &machine.root,
        &["project", "enable", "--project-id", "thesis"],
    )?;
    let unknown = machine.run(
        &machine.root,
        &["project", "disable", "--project-id", "nope"],
    )?;
    assert_eq!(unknown.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&unknown.stderr).contains("not registered"));
    Ok(())
}

/// Two clones declaring one project_id: the second never silently takes the
/// registration; enabling it needs --replace.
#[test]
fn a_second_clone_is_reported_not_registered_over_the_first() -> Result<()> {
    let machine = Machine::new()?;
    let first = machine.repo("first", "paper")?;
    let second = machine.repo("second", "paper")?;
    machine.ok(&first, &["project", "enable"])?;

    let status = machine.run(&second, &["status"])?;
    assert!(status.status.success());
    let note = String::from_utf8_lossy(&status.stderr);
    assert_eq!(
        note.lines()
            .filter(|line| line.starts_with("note:"))
            .count(),
        1,
        "one line about the other clone: {note}"
    );
    assert!(note.contains("registered at"), "{note}");

    let refused = machine.run(&second, &["project", "enable"])?;
    assert_eq!(refused.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("--replace"), "{stderr}");
    let list = machine.json(&machine.root, &["project", "list", "--json"])?;
    assert_eq!(
        project(&list, "paper")["config_path"],
        first
            .join("reviewloop.toml")
            .canonicalize()?
            .display()
            .to_string()
    );

    let moved = machine.ok(&second, &["project", "enable", "--replace"])?;
    assert!(moved.contains("moved from"), "{moved}");
    let list = machine.json(&machine.root, &["project", "list", "--json"])?;
    assert_eq!(
        project(&list, "paper")["config_path"],
        second
            .join("reviewloop.toml")
            .canonicalize()?
            .display()
            .to_string()
    );
    Ok(())
}

#[test]
fn daemon_status_and_pause_work_without_a_running_supervisor() -> Result<()> {
    let machine = Machine::new()?;
    let repo = machine.repo("thesis", "thesis")?;
    machine.ok(&repo, &["project", "enable"])?;

    let status = machine.json(&repo, &["daemon", "status", "--json"])?;
    assert_eq!(status["supervisor"]["state"], "stopped");
    assert_eq!(
        status["current_project"]["availability"],
        "supervisor_stopped"
    );
    assert_eq!(project(&status, "thesis")["enabled"], true);
    assert_eq!(status["budget"]["submissions_per_tick"], 1);

    let paused = machine.ok(&machine.root, &["daemon", "pause"])?;
    assert!(paused.contains("will start paused"), "{paused}");
    let status = machine.json(&machine.root, &["daemon", "status", "--json"])?;
    assert!(status["supervisor"]["paused_at"].is_string());
    let human = machine.ok(&machine.root, &["daemon", "status"])?;
    assert!(human.contains("it will start paused"), "{human}");

    let resumed = machine.ok(&machine.root, &["daemon", "resume"])?;
    assert!(resumed.contains("Supervisor resumed"), "{resumed}");
    let status = machine.json(&machine.root, &["daemon", "status", "--json"])?;
    assert!(status["supervisor"]["paused_at"].is_null());
    Ok(())
}

/// A project file cannot move the database: the setting is refused by name.
#[test]
fn a_project_file_that_sets_the_database_is_refused() -> Result<()> {
    let machine = Machine::new()?;
    let repo = machine.repo("thesis", "thesis")?;
    fs::write(
        repo.join("reviewloop.toml"),
        "project_id = \"thesis\"\n\n[core]\ndb_path = \"/tmp/other.db\"\n",
    )?;
    let output = machine.run(&repo, &["status"])?;
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("core.db_path"), "{stderr}");
    assert!(stderr.contains("machine-wide"), "{stderr}");
    Ok(())
}

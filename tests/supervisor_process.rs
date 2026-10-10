//! OSS-338, as real processes: while one `reviewloop daemon run` supervises a
//! state dir, a second one is refused; once the first dies (even by SIGKILL,
//! without a clean stop) a new one starts at once, since the OS lock dies
//! with its holder.

use anyhow::{Context, Result};
use reviewloop::db::Db;
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

/// A supervisor process, killed when dropped.
struct Supervisor(Child);

impl Drop for Supervisor {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A home, global config dir and state dir of its own, so the test never
/// touches the developer's real ones.
struct Machine {
    _tmp: tempfile::TempDir,
    root: PathBuf,
}

impl Machine {
    fn new() -> Result<Self> {
        let tmp = tempfile::tempdir()?;
        let root = tmp.path().canonicalize()?;
        std::fs::create_dir_all(root.join("home"))?;
        Ok(Self { _tmp: tmp, root })
    }

    fn state_dir(&self) -> PathBuf {
        self.root.join("state")
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_reviewloop"));
        command
            .args(["daemon", "run", "--panel", "false"])
            .current_dir(&self.root)
            .env("HOME", self.root.join("home"))
            .env("XDG_CONFIG_HOME", self.root.join("xdg"))
            .env("REVIEWLOOP_STATE_DIR", self.state_dir())
            .env_remove("REVIEWLOOP_CSPAPER_API_KEY")
            .stdin(Stdio::null());
        command
    }

    fn start(&self) -> Result<Supervisor> {
        let child = self
            .command()
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("spawning reviewloop daemon run")?;
        let supervisor = Supervisor(child);
        let pid = supervisor.0.id();
        wait_for(|| recorded_pid(&self.state_dir()) == Some(pid))
            .with_context(|| format!("supervisor {pid} never recorded its start"))?;
        Ok(supervisor)
    }
}

fn recorded_pid(state_dir: &Path) -> Option<u32> {
    let path = state_dir.join("reviewloop.db");
    if !path.exists() {
        return None;
    }
    Db::new_file(path).supervisor_record().ok()?.pid
}

fn wait_for(mut ready: impl FnMut() -> bool) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready() {
        anyhow::ensure!(Instant::now() < deadline, "timed out");
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

#[test]
fn one_supervisor_per_state_dir_and_a_killed_one_is_replaced() -> Result<()> {
    let machine = Machine::new()?;
    let mut first = machine.start()?;

    let refused = machine.command().output()?;
    assert_eq!(refused.status.code(), Some(1), "{refused:?}");
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("already running"), "{stderr}");
    assert!(
        stderr.contains(&format!("pid {}", first.0.id())),
        "names the holder: {stderr}"
    );
    assert!(first.0.try_wait()?.is_none(), "the first one keeps running");

    // SIGKILL: no clean stop, yet the replacement starts right away.
    first.0.kill()?;
    first.0.wait()?;
    let second = machine.start()?;
    assert_eq!(recorded_pid(&machine.state_dir()), Some(second.0.id()));
    Ok(())
}

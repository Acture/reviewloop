//! Project registry rules: which `reviewloop.toml` backs each `project_id`,
//! and which projects the machine supervisor runs.
//!
//! Loading a config registers it but never enables it. A registration only
//! moves to another file implicitly when the project is disabled and its
//! registered file is gone or now declares another project; a second live
//! file declaring the same `project_id` (another clone, a worktree) is a
//! conflict, never a silent overwrite. Only `project enable` (with
//! `--replace` for a live conflict) moves an enabled project.

pub use crate::config::canonical_config_path;
use crate::{db::Db, model::RegisteredProject};
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::{
    fs,
    path::{Path, PathBuf},
};

/// What a registered config path holds now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigFileState {
    /// The file parses and declares this `project_id`.
    Declares(String),
    Missing,
    /// The file exists but cannot be read or parsed; treated as live.
    Unreadable(String),
}

impl ConfigFileState {
    pub fn probe(path: &Path) -> Self {
        let raw = match fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Self::Missing,
            Err(err) => return Self::Unreadable(err.to_string()),
        };
        match raw.parse::<toml::Table>() {
            Ok(table) => Self::Declares(
                table
                    .get("project_id")
                    .and_then(toml::Value::as_str)
                    .unwrap_or_default()
                    .trim()
                    .to_string(),
            ),
            // Never quote the parser's message: it can echo a secret's line.
            Err(_) => Self::Unreadable("not valid TOML".to_string()),
        }
    }

    /// Whether this file still backs `project_id` (or might: unreadable).
    fn backs(&self, project_id: &str) -> bool {
        match self {
            Self::Declares(declared) => declared == project_id,
            Self::Missing => false,
            Self::Unreadable(_) => true,
        }
    }
}

/// Whether two config paths name the same config, in canonical form (see
/// [`canonical_config_path`]): two repositories symlinking one file are two
/// configs, since each has its own project root. Paths that no longer
/// resolve compare as written.
pub fn same_config_file(a: &Path, b: &Path) -> bool {
    match (canonical_config_path(a), canonical_config_path(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => a == b,
    }
}

/// Another live config already holds the registration of `project_id`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "project {project_id} is registered at {}{}, not {}",
    .registered.display(),
    if *.registered_enabled { " (enabled)" } else { "" },
    .requested.display()
)]
pub struct RegistryConflict {
    pub project_id: String,
    pub registered: PathBuf,
    pub requested: PathBuf,
    pub registered_enabled: bool,
    pub registered_state: ConfigFileState,
}

/// What [`register_seen`] did with a config the CLI loaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Registration {
    Inserted,
    Refreshed,
    /// The disabled registration pointed at a stale file and now points here.
    Repointed {
        from: PathBuf,
    },
    /// The registration stays where it was.
    Kept(RegistryConflict),
}

/// Record that the CLI loaded `project_id` from `config_path` (canonical).
pub fn register_seen(
    db: &Db,
    project_id: &str,
    config_path: &Path,
    now: DateTime<Utc>,
) -> Result<Registration> {
    if db.insert_project_registration(project_id, config_path, now)? {
        return Ok(Registration::Inserted);
    }
    let Some(row) = db.get_registered_project(project_id)? else {
        // Removed between the insert and the read: register again next time.
        return Ok(Registration::Inserted);
    };
    if same_config_file(&row.config_path, config_path) {
        db.touch_project_registration(project_id, &row.config_path, config_path, now)?;
        return Ok(Registration::Refreshed);
    }
    let state = ConfigFileState::probe(&row.config_path);
    if !row.enabled
        && !state.backs(project_id)
        && db.repoint_disabled_project(project_id, &row.config_path, config_path, now)?
    {
        return Ok(Registration::Repointed {
            from: row.config_path,
        });
    }
    Ok(Registration::Kept(conflict(&row, config_path, state)))
}

fn conflict(row: &RegisteredProject, requested: &Path, state: ConfigFileState) -> RegistryConflict {
    RegistryConflict {
        project_id: row.project_id.clone(),
        registered: row.config_path.clone(),
        requested: requested.to_path_buf(),
        registered_enabled: row.enabled,
        registered_state: state,
    }
}

/// Why `project enable` refused.
#[derive(Debug, thiserror::Error)]
pub enum EnableError {
    /// Another live config declares the same `project_id`.
    #[error(transparent)]
    Conflict(RegistryConflict),
    /// This file already backs another enabled project.
    #[error(
        "{} is enabled as project {enabled_as}; disable it first with `reviewloop project disable --project-id {enabled_as}`",
        .config_path.display()
    )]
    FileEnabledAs {
        config_path: PathBuf,
        enabled_as: String,
    },
    /// Another process changed the registration while this one checked it.
    #[error("the registration of project {project_id} changed concurrently; run the command again")]
    Concurrent { project_id: String },
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

/// What [`enable`] changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Enabled {
    /// It was already enabled at this file; nothing but `last_seen_at` changed.
    pub already: bool,
    /// The registration this replaced, when it pointed at another file.
    pub moved_from: Option<PathBuf>,
}

/// Enable `project_id` at `config_path` (canonical, already loaded and
/// validated by the caller). `replace` moves a registration whose file is
/// still live; a stale one moves either way, since enabling is explicit.
pub fn enable(
    db: &Db,
    project_id: &str,
    config_path: &Path,
    replace: bool,
    now: DateTime<Utc>,
) -> Result<Enabled, EnableError> {
    enable_with(db, project_id, config_path, replace, false, now)
}

/// [`enable`] with `replace`, but only while nobody has enabled or disabled
/// the project: what an old single-project install's binding may do. `None`
/// when a decision exists (checked in the same transaction as the write).
pub fn enable_if_undecided(
    db: &Db,
    project_id: &str,
    config_path: &Path,
    now: DateTime<Utc>,
) -> Result<Option<Enabled>, EnableError> {
    if db
        .get_registered_project(project_id)?
        .is_some_and(|row| row.enabled_changed_at.is_some())
    {
        return Ok(None);
    }
    match enable_with(db, project_id, config_path, true, true, now) {
        Err(EnableError::Concurrent { .. })
            if db
                .get_registered_project(project_id)?
                .is_some_and(|row| row.enabled_changed_at.is_some()) =>
        {
            Ok(None)
        }
        other => other.map(Some),
    }
}

fn enable_with(
    db: &Db,
    project_id: &str,
    config_path: &Path,
    replace: bool,
    undecided_only: bool,
    now: DateTime<Utc>,
) -> Result<Enabled, EnableError> {
    let projects = db.list_registered_projects()?;
    if let Some(other) = projects.iter().find(|row| {
        row.enabled
            && row.project_id != project_id
            && same_config_file(&row.config_path, config_path)
    }) {
        return Err(EnableError::FileEnabledAs {
            config_path: config_path.to_path_buf(),
            enabled_as: other.project_id.clone(),
        });
    }
    let existing = projects.iter().find(|row| row.project_id == project_id);
    // Already enabled here: nothing to decide, and its health stays.
    if let Some(row) = existing
        && row.enabled
        && !undecided_only
        && same_config_file(&row.config_path, config_path)
    {
        db.touch_project_registration(project_id, &row.config_path, config_path, now)?;
        return Ok(Enabled {
            already: true,
            moved_from: None,
        });
    }
    let mut moved_from = None;
    if let Some(row) = existing
        && !same_config_file(&row.config_path, config_path)
    {
        let state = ConfigFileState::probe(&row.config_path);
        if state.backs(project_id) && !replace {
            return Err(EnableError::Conflict(conflict(row, config_path, state)));
        }
        moved_from = Some(row.config_path.clone());
    }
    let expected = existing.map(|row| row.config_path.as_path());
    if !db.enable_project(project_id, config_path, expected, undecided_only, now)? {
        return Err(EnableError::Concurrent {
            project_id: project_id.to_string(),
        });
    }
    Ok(Enabled {
        already: existing.is_some_and(|row| row.enabled) && moved_from.is_none(),
        moved_from,
    })
}

/// What [`disable`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disabled {
    NotRegistered,
    WasEnabled,
    AlreadyDisabled,
}

pub fn disable(db: &Db, project_id: &str, now: DateTime<Utc>) -> Result<Disabled> {
    Ok(match db.disable_project(project_id, now)? {
        None => Disabled::NotRegistered,
        Some(true) => Disabled::WasEnabled,
        Some(false) => Disabled::AlreadyDisabled,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    struct Fixture {
        tmp: TempDir,
        db: Db,
    }

    impl Fixture {
        fn new() -> Self {
            let tmp = TempDir::new().unwrap();
            let db = Db::new_file(tmp.path().join("reviewloop.db"));
            db.ensure_schema().unwrap();
            Self { tmp, db }
        }

        /// A canonical config path in `dir` declaring `project_id`.
        fn config(&self, dir: &str, project_id: &str) -> PathBuf {
            let dir = self.tmp.path().join(dir);
            fs::create_dir_all(&dir).unwrap();
            let path = dir.join("reviewloop.toml");
            fs::write(&path, format!("project_id = \"{project_id}\"\n")).unwrap();
            canonical_config_path(&path).unwrap()
        }

        fn row(&self, project_id: &str) -> RegisteredProject {
            self.db.get_registered_project(project_id).unwrap().unwrap()
        }
    }

    #[test]
    fn registering_never_enables() {
        let fx = Fixture::new();
        let path = fx.config("a", "p");
        assert_eq!(
            register_seen(&fx.db, "p", &path, Utc::now()).unwrap(),
            Registration::Inserted
        );
        let row = fx.row("p");
        assert!(!row.enabled);
        assert_eq!(row.enabled_changed_at, None);
        assert_eq!(
            register_seen(&fx.db, "p", &path, Utc::now()).unwrap(),
            Registration::Refreshed
        );
    }

    #[test]
    fn a_second_live_config_for_the_same_id_is_kept_out() {
        let fx = Fixture::new();
        let first = fx.config("first", "p");
        let second = fx.config("second", "p");
        register_seen(&fx.db, "p", &first, Utc::now()).unwrap();

        let Registration::Kept(conflict) = register_seen(&fx.db, "p", &second, Utc::now()).unwrap()
        else {
            panic!("a live duplicate must not take over the registration");
        };
        assert_eq!(conflict.registered, first);
        assert_eq!(conflict.requested, second);
        assert_eq!(fx.row("p").config_path, first);
    }

    #[test]
    fn a_stale_disabled_registration_follows_the_project() {
        let fx = Fixture::new();
        let old = fx.config("old", "p");
        register_seen(&fx.db, "p", &old, Utc::now()).unwrap();
        fs::remove_file(&old).unwrap();
        let new = fx.config("new", "p");

        assert_eq!(
            register_seen(&fx.db, "p", &new, Utc::now()).unwrap(),
            Registration::Repointed { from: old.clone() }
        );
        assert_eq!(fx.row("p").config_path, new);

        // A file that now declares another project is stale for this one too.
        let third = fx.config("third", "p");
        fs::write(&new, "project_id = \"renamed\"\n").unwrap();
        assert_eq!(
            register_seen(&fx.db, "p", &third, Utc::now()).unwrap(),
            Registration::Repointed { from: new }
        );
    }

    #[test]
    fn an_enabled_registration_never_moves_implicitly() {
        let fx = Fixture::new();
        let old = fx.config("old", "p");
        enable(&fx.db, "p", &old, false, Utc::now()).unwrap();
        fs::remove_file(&old).unwrap();
        let new = fx.config("new", "p");

        let Registration::Kept(conflict) = register_seen(&fx.db, "p", &new, Utc::now()).unwrap()
        else {
            panic!("an enabled project moves only through `project enable`");
        };
        assert!(conflict.registered_enabled);
        assert_eq!(conflict.registered_state, ConfigFileState::Missing);
        assert_eq!(fx.row("p").config_path, old);

        // Enabling at the new file moves it, since the old one is gone.
        let enabled = enable(&fx.db, "p", &new, false, Utc::now()).unwrap();
        assert_eq!(enabled.moved_from, Some(old));
        assert_eq!(fx.row("p").config_path, new);
    }

    #[test]
    fn enabling_over_a_live_duplicate_needs_replace() {
        let fx = Fixture::new();
        let first = fx.config("first", "p");
        let second = fx.config("second", "p");
        enable(&fx.db, "p", &first, false, Utc::now()).unwrap();

        let err = enable(&fx.db, "p", &second, false, Utc::now()).unwrap_err();
        assert!(matches!(err, EnableError::Conflict(_)), "{err}");
        assert_eq!(fx.row("p").config_path, first);

        let enabled = enable(&fx.db, "p", &second, true, Utc::now()).unwrap();
        assert_eq!(enabled.moved_from, Some(first));
        let row = fx.row("p");
        assert_eq!(row.config_path, second);
        assert!(row.enabled);
    }

    #[test]
    fn one_file_backs_at_most_one_enabled_project() {
        let fx = Fixture::new();
        let path = fx.config("a", "old-id");
        enable(&fx.db, "old-id", &path, false, Utc::now()).unwrap();
        fs::write(&path, "project_id = \"new-id\"\n").unwrap();

        let err = enable(&fx.db, "new-id", &path, false, Utc::now()).unwrap_err();
        assert!(
            matches!(&err, EnableError::FileEnabledAs { enabled_as, .. } if enabled_as == "old-id"),
            "{err}"
        );
        disable(&fx.db, "old-id", Utc::now()).unwrap();
        enable(&fx.db, "new-id", &path, false, Utc::now()).unwrap();
    }

    #[test]
    fn enable_reports_an_already_enabled_project_and_keeps_its_health() {
        let fx = Fixture::new();
        let path = fx.config("a", "p");
        assert!(
            !enable(&fx.db, "p", &path, false, Utc::now())
                .unwrap()
                .already
        );
        fx.db.record_project_health("p", Utc::now(), None).unwrap();
        let before = fx.row("p");
        assert!(
            enable(&fx.db, "p", &path, false, Utc::now())
                .unwrap()
                .already
        );
        let after = fx.row("p");
        assert_eq!(
            after.health, before.health,
            "a no-op enable keeps the last pass"
        );
        assert_eq!(after.enabled_changed_at, before.enabled_changed_at);
    }

    #[test]
    fn disable_records_an_explicit_decision() {
        let fx = Fixture::new();
        assert_eq!(
            disable(&fx.db, "p", Utc::now()).unwrap(),
            Disabled::NotRegistered
        );
        let path = fx.config("a", "p");
        register_seen(&fx.db, "p", &path, Utc::now()).unwrap();
        assert_eq!(
            disable(&fx.db, "p", Utc::now()).unwrap(),
            Disabled::AlreadyDisabled
        );
        // Decided now, so the legacy install migration leaves it alone.
        assert!(fx.row("p").enabled_changed_at.is_some());
        assert!(!fx.row("p").enabled);
    }

    #[test]
    fn enable_and_disable_wake_the_supervisor() {
        let fx = Fixture::new();
        let path = fx.config("a", "p");
        let before = fx.db.supervisor_record().unwrap().control_version;
        enable(&fx.db, "p", &path, false, Utc::now()).unwrap();
        let enabled = fx.db.supervisor_record().unwrap().control_version;
        assert!(enabled > before);
        disable(&fx.db, "p", Utc::now()).unwrap();
        assert!(fx.db.supervisor_record().unwrap().control_version > enabled);
    }

    #[test]
    fn same_config_file_sees_through_symlinks_and_relative_spellings() {
        let fx = Fixture::new();
        let path = fx.config("real", "p");
        #[cfg(unix)]
        {
            let link = fx.tmp.path().join("link");
            std::os::unix::fs::symlink(fx.tmp.path().join("real"), &link).unwrap();
            assert!(same_config_file(&link.join("reviewloop.toml"), &path));
        }
        let dotted = fx.tmp.path().join("real/../real/reviewloop.toml");
        assert!(same_config_file(&dotted, &path));
    }
}

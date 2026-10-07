//! Immutable PDF snapshots pinned to review jobs.
//!
//! Every job uploads a content-addressed copy of its PDF taken at enqueue
//! time, so editing or deleting the source file, or repointing the paper in
//! config, cannot change what a later (re)submission sends. Snapshots live at
//! `<state_dir>/snapshots/<sha256>/<source file name>`; the file name is kept
//! because the backend forwards it to the review service.

use crate::{model::Job, util::sha256_file};
use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};
use uuid::Uuid;

const SNAPSHOTS_DIR: &str = "snapshots";
const STAGING_DIR: &str = ".staging";

/// Unreferenced snapshots younger than this survive garbage collection. A
/// concurrent enqueue in another process (e.g. `reviewloop submit` while the
/// daemon prunes) publishes its snapshot before inserting the job row.
pub const SNAPSHOT_GC_GRACE: Duration = Duration::from_secs(60 * 60);

/// A PDF copied into the snapshot store, ready to be pinned to a new job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedInput {
    /// Path the bytes were copied from (the paper's configured `pdf_path`).
    pub source_path: PathBuf,
    /// Immutable copy that every submission of the job uploads.
    pub snapshot_path: PathBuf,
    /// SHA-256 of the snapshot bytes (not of the source, which may have moved on).
    pub sha256: String,
}

/// What the submit path should upload for a job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobInput {
    /// The job's snapshot exists and still matches its recorded hash.
    Ready(PathBuf),
    /// The job had no usable snapshot, but its source still matched the
    /// recorded hash and has just been snapshotted. Persist the new path.
    Backfilled(PreparedInput),
    /// Neither the snapshot nor the source matches the recorded hash; the job
    /// must not be submitted.
    Blocked { reason: String },
}

pub fn snapshots_root(state_dir: &Path) -> PathBuf {
    state_dir.join(SNAPSHOTS_DIR)
}

/// Copy `source` into the snapshot store, hashing the copied bytes in the same
/// pass, and publish it atomically: bytes are staged and fsynced under
/// `snapshots/.staging/`, then renamed into place, so a failure never leaves a
/// partial file at a snapshot path.
pub fn prepare_input(state_dir: &Path, source: &Path) -> Result<PreparedInput> {
    let file_name = source
        .file_name()
        .with_context(|| format!("PDF path has no file name: {}", source.display()))?;
    let root = snapshots_root(state_dir);
    let staging_dir = root.join(STAGING_DIR);
    create_private_dir(&staging_dir)
        .with_context(|| format!("failed to create {}", staging_dir.display()))?;

    let staged = staging_dir.join(format!("{}.part", Uuid::new_v4()));
    let sha256 = match copy_and_hash(source, &staged) {
        Ok(sha256) => sha256,
        Err(err) => {
            // The partial copy is useless; GC would also reap it after the grace window.
            let _ = fs::remove_file(&staged);
            return Err(err);
        }
    };

    let snapshot_dir = root.join(&sha256);
    create_private_dir(&snapshot_dir)
        .with_context(|| format!("failed to create {}", snapshot_dir.display()))?;
    let snapshot_path = snapshot_dir.join(file_name);

    if snapshot_path.exists() && sha256_file(&snapshot_path)? == sha256 {
        fs::remove_file(&staged)
            .with_context(|| format!("failed to remove {}", staged.display()))?;
        // Refresh the mtime so GC's grace window covers the job about to reference it.
        File::options()
            .write(true)
            .open(&snapshot_path)
            .and_then(|file| file.set_modified(SystemTime::now()))
            .with_context(|| format!("failed to touch {}", snapshot_path.display()))?;
    } else {
        fs::rename(&staged, &snapshot_path).with_context(|| {
            format!(
                "failed to publish snapshot {} -> {}",
                staged.display(),
                snapshot_path.display()
            )
        })?;
    }

    tracing::info!(
        source = %source.display(),
        snapshot = %snapshot_path.display(),
        sha256 = %sha256,
        "prepared PDF snapshot"
    );
    Ok(PreparedInput {
        source_path: source.to_path_buf(),
        snapshot_path,
        sha256,
    })
}

// Snapshots are unpublished manuscripts: keep them owner-only (Unix), like
// the database and config files.
fn create_private_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)
    }
    #[cfg(not(unix))]
    fs::create_dir_all(path)
}

fn create_private_file(path: &Path) -> io::Result<File> {
    let mut options = File::options();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

fn copy_and_hash(source: &Path, dest: &Path) -> Result<String> {
    let mut reader =
        File::open(source).with_context(|| format!("failed to open PDF: {}", source.display()))?;
    let mut writer = create_private_file(dest)
        .with_context(|| format!("failed to create staging file: {}", dest.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("failed to read PDF: {}", source.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        writer
            .write_all(&buffer[..read])
            .with_context(|| format!("failed to write staging file: {}", dest.display()))?;
    }
    writer
        .sync_all()
        .with_context(|| format!("failed to sync staging file: {}", dest.display()))?;
    Ok(format!("{:x}", hasher.finalize()))
}

/// Decide what to upload for `job`. The snapshot is re-hashed on every call,
/// so a corrupted or tampered snapshot is caught before upload. A job without
/// a usable snapshot (created before snapshots existed, or whose snapshot was
/// lost) is backfilled from its own `pdf_path` only if that file still hashes
/// to the job's recorded `pdf_hash`.
pub fn resolve_job_input(state_dir: &Path, job: &Job) -> Result<JobInput> {
    let snapshot_problem = match job.snapshot_path.as_deref().map(Path::new) {
        Some(snapshot) if !snapshot.exists() => {
            format!("snapshot {} is missing", snapshot.display())
        }
        Some(snapshot) => {
            let actual = sha256_file(snapshot)?;
            if actual == job.pdf_hash {
                return Ok(JobInput::Ready(snapshot.to_path_buf()));
            }
            format!("snapshot {} hashes to {actual}", snapshot.display())
        }
        None => "job has no snapshot".to_string(),
    };

    let source = Path::new(&job.pdf_path);
    if !source.exists() {
        return Ok(JobInput::Blocked {
            reason: format!(
                "{snapshot_problem} and source PDF {} no longer exists",
                source.display()
            ),
        });
    }
    let source_hash = sha256_file(source)?;
    if source_hash != job.pdf_hash {
        return Ok(JobInput::Blocked {
            reason: format!(
                "{snapshot_problem} and source PDF {} changed since enqueue (now {source_hash})",
                source.display()
            ),
        });
    }

    let prepared = prepare_input(state_dir, source)?;
    if prepared.sha256 != job.pdf_hash {
        return Ok(JobInput::Blocked {
            reason: format!(
                "{snapshot_problem} and source PDF {} changed while being snapshotted (now {})",
                source.display(),
                prepared.sha256
            ),
        });
    }
    Ok(JobInput::Backfilled(prepared))
}

/// Remove snapshot directories whose hash no job references, plus abandoned
/// staging files. Anything modified within `grace` is kept. Returns the number
/// of entries removed.
pub fn prune_unreferenced_snapshots(
    state_dir: &Path,
    referenced_hashes: &HashSet<String>,
    grace: Duration,
) -> Result<usize> {
    let root = snapshots_root(state_dir);
    if !root.exists() {
        return Ok(0);
    }
    let cutoff = SystemTime::now() - grace;
    let mut removed = 0;

    for entry in
        fs::read_dir(&root).with_context(|| format!("failed to read {}", root.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == STAGING_DIR {
            for staged in fs::read_dir(&path)? {
                let staged = staged?.path();
                let modified = fs::metadata(&staged).and_then(|meta| meta.modified());
                let Some(modified) = skip_vanished(modified)? else {
                    continue;
                };
                if modified < cutoff
                    && skip_vanished(fs::remove_file(&staged))
                        .with_context(|| format!("failed to remove {}", staged.display()))?
                        .is_some()
                {
                    removed += 1;
                }
            }
            continue;
        }
        if referenced_hashes.contains(&name) || !entry.file_type()?.is_dir() {
            continue;
        }
        let Some(newest) = skip_vanished(newest_mtime(&path))? else {
            continue;
        };
        if newest < cutoff
            && skip_vanished(fs::remove_dir_all(&path))
                .with_context(|| format!("failed to remove {}", path.display()))?
                .is_some()
        {
            removed += 1;
        }
    }

    Ok(removed)
}

/// Map `NotFound` to `None`: during a sweep, a concurrent `prepare_input` may
/// rename its staging file and another process's GC may delete the same entry
/// first. Either way the entry is already gone, so the sweep carries on.
fn skip_vanished<T>(result: io::Result<T>) -> io::Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

fn newest_mtime(dir: &Path) -> io::Result<SystemTime> {
    let mut newest = fs::metadata(dir)?.modified()?;
    for entry in fs::read_dir(dir)? {
        newest = newest.max(entry?.metadata()?.modified()?);
    }
    Ok(newest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::JobStatus;
    use chrono::Utc;

    const ORIGINAL: &[u8] = b"%PDF-1.4\n%original\n%%EOF\n";
    const EDITED: &[u8] = b"%PDF-1.4\n%edited after enqueue\n%%EOF\n";

    fn sha(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn job(source: &Path, pdf_hash: &str, snapshot_path: Option<&Path>) -> Job {
        Job {
            id: "job-1".to_string(),
            project_id: "p".to_string(),
            paper_id: "main".to_string(),
            backend: "stanford".to_string(),
            pdf_path: source.to_string_lossy().into_owned(),
            pdf_hash: pdf_hash.to_string(),
            snapshot_path: snapshot_path.map(|p| p.to_string_lossy().into_owned()),
            status: JobStatus::Queued,
            token: None,
            email: "a@b.c".to_string(),
            venue: None,
            git_tag: None,
            git_commit: None,
            version_no: 1,
            round_no: 1,
            version_source: "pdf_hash".to_string(),
            version_key: pdf_hash.to_string(),
            attempt: 0,
            started_at: None,
            next_poll_at: None,
            last_error: None,
            fallback_used: false,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn age(path: &Path, by: Duration) {
        File::open(path)
            .and_then(|f| f.set_modified(SystemTime::now() - by))
            .expect("set mtime");
    }

    #[test]
    fn prepare_copies_bytes_to_content_addressed_path_keeping_file_name() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let source = tmp.path().join("main.pdf");
        fs::write(&source, ORIGINAL)?;

        let input = prepare_input(tmp.path(), &source)?;

        assert_eq!(input.sha256, sha(ORIGINAL));
        assert_eq!(input.source_path, source);
        assert_eq!(
            input.snapshot_path,
            snapshots_root(tmp.path())
                .join(sha(ORIGINAL))
                .join("main.pdf")
        );
        assert_eq!(fs::read(&input.snapshot_path)?, ORIGINAL);
        let staging = snapshots_root(tmp.path()).join(STAGING_DIR);
        assert_eq!(fs::read_dir(staging)?.count(), 0, "staging must be empty");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn snapshots_are_owner_only() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir()?;
        let source = tmp.path().join("main.pdf");
        fs::write(&source, ORIGINAL)?;

        let input = prepare_input(tmp.path(), &source)?;

        let mode =
            |path: &Path| -> Result<u32> { Ok(fs::metadata(path)?.permissions().mode() & 0o777) };
        assert_eq!(mode(&input.snapshot_path)?, 0o600);
        assert_eq!(mode(input.snapshot_path.parent().unwrap())?, 0o700);
        assert_eq!(mode(&snapshots_root(tmp.path()))?, 0o700);
        assert_eq!(mode(&snapshots_root(tmp.path()).join(STAGING_DIR))?, 0o700);
        Ok(())
    }

    #[test]
    fn snapshot_is_unaffected_by_later_source_edits() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let source = tmp.path().join("main.pdf");
        fs::write(&source, ORIGINAL)?;
        let first = prepare_input(tmp.path(), &source)?;

        fs::write(&source, EDITED)?;
        let second = prepare_input(tmp.path(), &source)?;

        assert_ne!(first.snapshot_path, second.snapshot_path);
        assert_eq!(fs::read(&first.snapshot_path)?, ORIGINAL);
        assert_eq!(fs::read(&second.snapshot_path)?, EDITED);
        Ok(())
    }

    #[test]
    fn prepare_reuses_identical_snapshot_and_repairs_corrupted_one() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let source = tmp.path().join("main.pdf");
        fs::write(&source, ORIGINAL)?;
        let first = prepare_input(tmp.path(), &source)?;
        let again = prepare_input(tmp.path(), &source)?;
        assert_eq!(first, again);

        fs::write(&first.snapshot_path, b"corrupted")?;
        let repaired = prepare_input(tmp.path(), &source)?;
        assert_eq!(repaired.snapshot_path, first.snapshot_path);
        assert_eq!(fs::read(&repaired.snapshot_path)?, ORIGINAL);
        Ok(())
    }

    #[test]
    fn failed_prepare_publishes_nothing() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let missing = tmp.path().join("missing.pdf");
        assert!(prepare_input(tmp.path(), &missing).is_err());
        let root = snapshots_root(tmp.path());
        assert_eq!(fs::read_dir(root.join(STAGING_DIR))?.count(), 0);
        assert_eq!(fs::read_dir(&root)?.count(), 1, "only the staging dir");

        // Snapshot store unusable (a regular file where the directory should be).
        let blocked_state = tmp.path().join("blocked");
        fs::create_dir_all(&blocked_state)?;
        fs::write(snapshots_root(&blocked_state), b"not a directory")?;
        let source = tmp.path().join("main.pdf");
        fs::write(&source, ORIGINAL)?;
        assert!(prepare_input(&blocked_state, &source).is_err());
        Ok(())
    }

    #[test]
    fn resolve_uses_matching_snapshot_even_after_source_is_gone() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let source = tmp.path().join("main.pdf");
        fs::write(&source, ORIGINAL)?;
        let input = prepare_input(tmp.path(), &source)?;
        fs::remove_file(&source)?;

        let resolved = resolve_job_input(
            tmp.path(),
            &job(&source, &input.sha256, Some(&input.snapshot_path)),
        )?;
        assert_eq!(resolved, JobInput::Ready(input.snapshot_path));
        Ok(())
    }

    #[test]
    fn resolve_backfills_legacy_job_only_when_source_still_matches() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let source = tmp.path().join("main.pdf");
        fs::write(&source, ORIGINAL)?;

        let JobInput::Backfilled(input) =
            resolve_job_input(tmp.path(), &job(&source, &sha(ORIGINAL), None))?
        else {
            panic!("expected backfill for a matching legacy source");
        };
        assert_eq!(input.sha256, sha(ORIGINAL));
        assert_eq!(fs::read(&input.snapshot_path)?, ORIGINAL);

        fs::write(&source, EDITED)?;
        let resolved = resolve_job_input(tmp.path(), &job(&source, &sha(ORIGINAL), None))?;
        let JobInput::Blocked { reason } = resolved else {
            panic!("expected block for an edited legacy source, got {resolved:?}");
        };
        assert!(reason.contains("changed since enqueue"), "{reason}");
        assert!(reason.contains(&sha(EDITED)), "{reason}");

        fs::remove_file(&source)?;
        let resolved = resolve_job_input(tmp.path(), &job(&source, &sha(ORIGINAL), None))?;
        let JobInput::Blocked { reason } = resolved else {
            panic!("expected block for a deleted legacy source, got {resolved:?}");
        };
        assert!(reason.contains("no longer exists"), "{reason}");
        Ok(())
    }

    #[test]
    fn resolve_rejects_tampered_snapshot_unless_source_can_rebuild_it() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let source = tmp.path().join("main.pdf");
        fs::write(&source, ORIGINAL)?;
        let input = prepare_input(tmp.path(), &source)?;
        let pinned = job(&source, &input.sha256, Some(&input.snapshot_path));

        fs::write(&input.snapshot_path, EDITED)?;
        let JobInput::Backfilled(rebuilt) = resolve_job_input(tmp.path(), &pinned)? else {
            panic!("expected rebuild from the unchanged source");
        };
        assert_eq!(fs::read(&rebuilt.snapshot_path)?, ORIGINAL);

        fs::write(&input.snapshot_path, EDITED)?;
        fs::write(&source, EDITED)?;
        let resolved = resolve_job_input(tmp.path(), &pinned)?;
        let JobInput::Blocked { reason } = resolved else {
            panic!("expected block when neither copy matches, got {resolved:?}");
        };
        assert!(reason.contains("hashes to"), "{reason}");
        Ok(())
    }

    #[test]
    fn gc_keeps_referenced_and_recent_snapshots() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let make = |name: &str, bytes: &[u8]| -> Result<PreparedInput> {
            let source = tmp.path().join(name);
            fs::write(&source, bytes)?;
            prepare_input(tmp.path(), &source)
        };
        let referenced = make("referenced.pdf", b"%PDF referenced")?;
        let old_orphan = make("old.pdf", b"%PDF old orphan")?;
        let fresh_orphan = make("fresh.pdf", b"%PDF fresh orphan")?;
        let hour = Duration::from_secs(3600);
        for input in [&referenced, &old_orphan] {
            age(&input.snapshot_path, 2 * hour);
            age(input.snapshot_path.parent().unwrap(), 2 * hour);
        }
        let staging = snapshots_root(tmp.path()).join(STAGING_DIR);
        let old_part = staging.join("old.part");
        let fresh_part = staging.join("fresh.part");
        fs::write(&old_part, b"partial")?;
        fs::write(&fresh_part, b"partial")?;
        age(&old_part, 2 * hour);

        let refs = HashSet::from([referenced.sha256.clone()]);
        let removed = prune_unreferenced_snapshots(tmp.path(), &refs, hour)?;

        assert_eq!(removed, 2);
        assert!(referenced.snapshot_path.exists());
        assert!(!old_orphan.snapshot_path.parent().unwrap().exists());
        assert!(fresh_orphan.snapshot_path.exists());
        assert!(!old_part.exists());
        assert!(fresh_part.exists());
        Ok(())
    }

    #[test]
    fn gc_treats_vanished_entries_as_removed_but_surfaces_other_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("raced-away.part");
        assert!(skip_vanished(fs::metadata(&missing)).unwrap().is_none());
        assert!(
            skip_vanished(fs::remove_dir_all(&missing))
                .unwrap()
                .is_none()
        );

        let not_a_dir = tmp.path().join("file");
        fs::write(&not_a_dir, b"x").unwrap();
        assert!(skip_vanished(fs::read_dir(&not_a_dir)).is_err());
    }

    #[test]
    fn gc_without_snapshot_store_is_a_no_op() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let removed = prune_unreferenced_snapshots(tmp.path(), &HashSet::new(), Duration::ZERO)?;
        assert_eq!(removed, 0);
        Ok(())
    }
}

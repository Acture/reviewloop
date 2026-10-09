//! Operation-level tests for `reviewloop::application`: a temporary config,
//! state directory and SQLite database, no MCP server and no provider.

use anyhow::Result;
use chrono::{Duration, Utc};
use reviewloop::{
    application::{
        Approval, CancelRequest, Eligibility, JobListQuery, JobPhase, JobRef, OpError, Operation,
        RequestDisposition, RequestOrigin, RetryAction, RetryRequest, ReviewOps, ReviewPart,
        ReviewQuery, ReviewRequest, ReviewRequestOutcome,
    },
    artifact::write_review_artifacts,
    backend::{cspaper, provider_source},
    config::{CSPAPER_API_KEY_ENV, Config, PaperConfig, Redacted},
    db::Db,
    model::{EnqueueConflict, ExistingReason, Job, JobPdf, JobStatus, NewJob},
    util::sha256_file,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{collections::BTreeSet, fs, path::Path};

const TOKEN: &str = "tok-0123456789abcdef";
/// CSPaper organisation key; must never reach a DTO, error view or event.
const CSPAPER_KEY: &str = "csp-org-key-5f0e1d2c3b4a";
const CSPAPER_PAPER: &str = "cs";
const AGENT_ID: &str = "ICLR_main_2026_1";
const OTHER_AGENT_ID: &str = "NeurIPS_main_2026_1";

struct Fixture {
    tmp: tempfile::TempDir,
    config: Config,
    db: Db,
}

impl Fixture {
    fn new() -> Result<Self> {
        let tmp = tempfile::tempdir()?;
        let state_dir = tmp.path().join("state");
        fs::create_dir_all(&state_dir)?;
        let pdf_path = tmp.path().join("paper.pdf");
        fs::write(&pdf_path, b"%PDF-1.4\n%%EOF\n")?;

        let mut config = Config {
            project_id: "project-ops".to_string(),
            ..Config::default()
        };
        config.core.state_dir = state_dir.to_string_lossy().to_string();
        config.trigger.git.enabled = false;
        config.trigger.pdf.enabled = false;
        config.imap = None;
        config.gmail_oauth = None;
        config.providers.stanford.email = "test@example.edu".to_string();
        config.papers = vec![PaperConfig {
            id: "main".to_string(),
            pdf_path: pdf_path.to_string_lossy().to_string(),
            backend: "stanford".to_string(),
            venue: None,
        }];

        let db = Db::new(&state_dir);
        db.ensure_schema()?;
        Ok(Self { tmp, config, db })
    }

    fn ops(&self) -> ReviewOps<'_> {
        ReviewOps::new(&self.config, &self.db)
    }

    fn pdf_path(&self) -> &Path {
        Path::new(&self.config.papers[0].pdf_path)
    }

    fn request(&self, force: bool, approval: Approval) -> Result<ReviewRequestOutcome, OpError> {
        self.keyed_request(None, force, approval)
    }

    fn keyed_request(
        &self,
        request_key: Option<&str>,
        force: bool,
        approval: Approval,
    ) -> Result<ReviewRequestOutcome, OpError> {
        self.ops().request_review(&ReviewRequest {
            paper_id: "main".to_string(),
            request_key: request_key.map(str::to_string),
            force,
            approval,
            origin: RequestOrigin::Submit,
        })
    }

    /// Add paper [`CSPAPER_PAPER`] on the cspaper backend, with its own PDF and no
    /// per-paper template; the provider settings stay unconfigured.
    fn add_cspaper_paper(&mut self) -> Result<()> {
        let pdf_path = self.tmp.path().join("cspaper.pdf");
        fs::write(&pdf_path, b"%PDF-1.4\n% cspaper draft\n%%EOF\n")?;
        self.config.papers.push(PaperConfig {
            id: CSPAPER_PAPER.to_string(),
            pdf_path: pdf_path.to_string_lossy().to_string(),
            backend: cspaper::BACKEND.to_string(),
            venue: None,
        });
        Ok(())
    }

    /// The API key and default template a cspaper request needs.
    fn configure_cspaper(&mut self) {
        self.config.providers.cspaper.api_key = Some(Redacted(CSPAPER_KEY.to_string()));
        self.config.providers.cspaper.agent_id = Some(AGENT_ID.to_string());
    }

    fn cspaper_request(&self, request_key: Option<&str>) -> Result<ReviewRequestOutcome, OpError> {
        self.ops().request_review(&ReviewRequest {
            paper_id: CSPAPER_PAPER.to_string(),
            request_key: request_key.map(str::to_string),
            force: false,
            approval: Approval::Granted,
            origin: RequestOrigin::Submit,
        })
    }

    fn cspaper_events(&self) -> Result<Vec<Value>> {
        Ok(self
            .db
            .list_timeline_events(&self.config.project_id, CSPAPER_PAPER)?
            .into_iter()
            .map(|event| event.payload)
            .collect())
    }

    fn insert_job(&self, project_id: &str, status: JobStatus, hash: &str) -> Result<Job> {
        self.db.create_job(&NewJob {
            project_id: project_id.to_string(),
            paper_id: "main".to_string(),
            backend: "stanford".to_string(),
            pdf: JobPdf::Unpinned {
                pdf_path: self.config.papers[0].pdf_path.clone(),
                pdf_hash: hash.to_string(),
            },
            status,
            email: "test@example.edu".to_string(),
            venue: None,
            review_options: Default::default(),
            git_tag: None,
            git_commit: None,
            next_poll_at: None,
        })
    }

    fn job(&self, status: JobStatus, hash: &str) -> Result<Job> {
        self.insert_job(&self.config.project_id, status, hash)
    }

    /// A job the provider accepted: PROCESSING with `TOKEN`.
    fn submitted_job(&self, hash: &str) -> Result<Job> {
        let job = self.job(JobStatus::Queued, hash)?;
        self.db
            .attach_token_to_job(&job.id, TOKEN, Utc::now() + Duration::hours(1))?;
        Ok(self.db.get_job(&job.id)?.expect("job exists"))
    }

    /// A job whose review was stored the way the worker stores it.
    fn completed_job(&self, raw: &Value) -> Result<Job> {
        let job = self.submitted_job("hash-completed")?;
        let (_, summary_md, _) = write_review_artifacts(
            &self.config.state_dir(),
            &job,
            TOKEN,
            raw,
            &provider_source(&self.config, &job.backend),
        )?;
        self.db
            .upsert_review(&job.id, TOKEN, &raw.to_string(), &summary_md)?;
        self.db
            .update_job_state(&job.id, JobStatus::Completed, None, Some(None), Some(None))?;
        Ok(self.db.get_job(&job.id)?.expect("job exists"))
    }

    fn events(&self, event_type: &str) -> Result<Vec<Value>> {
        Ok(self
            .db
            .list_timeline_events(&self.config.project_id, "main")?
            .into_iter()
            .filter(|event| event.event_type == event_type)
            .map(|event| event.payload)
            .collect())
    }
}

fn unscoped(config: &Config) -> Config {
    Config {
        project_id: String::new(),
        ..config.clone()
    }
}

/// Serialize `value` and fail if any object key is `token` or the token text
/// appears anywhere.
fn assert_token_free<T: Serialize>(value: &T) {
    fn walk(value: &Value) {
        match value {
            Value::Object(map) => {
                assert!(!map.contains_key("token"), "DTO has a token key: {value}");
                map.values().for_each(walk);
            }
            Value::Array(items) => items.iter().for_each(walk),
            _ => {}
        }
    }
    let json = serde_json::to_value(value).expect("serializable");
    walk(&json);
    assert!(!json.to_string().contains(TOKEN), "token leaked: {json}");
}

/// Serialize `value` and fail if the CSPaper API key appears anywhere.
fn assert_key_free<T: Serialize>(value: &T) {
    let json = serde_json::to_string(value).expect("serializable");
    assert!(
        !json.contains(CSPAPER_KEY),
        "CSPaper API key leaked: {json}"
    );
}

fn desk_rejection(enabled: bool) -> Value {
    json!({ "desk_rejection_enabled": enabled.to_string() })
}

fn review_json() -> Value {
    json!({
        "title": "A Paper",
        "numerical_score": 6.5,
        "token": TOKEN,
        "link": format!("https://paperreview.ai/review/{TOKEN}"),
        "sections": {
            "weaknesses": "Small evaluation.",
            "summary": "Studies loops.",
            "strengths": format!("Clear writing. ref {TOKEN}"),
            "zz_extra": "Appendix notes.",
            "not_text": 3
        }
    })
}

// ---- discovery ----

#[test]
fn list_projects_reports_registry_and_current_project() -> Result<()> {
    let fx = Fixture::new()?;
    let present = fx.tmp.path().join("reviewloop.toml");
    fs::write(&present, "project_id = \"project-ops\"\n")?;
    fx.db.register_project_config("project-ops", &present)?;
    fx.db
        .register_project_config("other", &fx.tmp.path().join("gone/reviewloop.toml"))?;

    let projects = fx.ops().list_projects()?;
    let summary: Vec<_> = projects
        .iter()
        .map(|p| (p.project_id.as_str(), p.config_present, p.current))
        .collect();
    assert_eq!(
        summary,
        vec![("other", false, false), ("project-ops", true, true)]
    );
    Ok(())
}

#[test]
fn list_papers_reports_effective_settings() -> Result<()> {
    let mut fx = Fixture::new()?;
    fx.config.papers.push(PaperConfig {
        id: "draft".to_string(),
        pdf_path: fx.tmp.path().join("missing.pdf").display().to_string(),
        backend: "stanford".to_string(),
        venue: Some("NeurIPS".to_string()),
    });
    fx.config.set_paper_watch("draft", false);

    let papers = fx.ops().list_papers()?;
    assert_eq!(papers.len(), 2);
    assert_eq!(papers[0].paper_id, "main");
    assert!(papers[0].pdf_present && papers[0].watched);
    assert_eq!(papers[0].venue, fx.config.venue_for(&fx.config.papers[0]));
    assert_eq!(papers[1].venue.as_deref(), Some("NeurIPS"));
    assert!(!papers[1].pdf_present && !papers[1].watched);

    let config = unscoped(&fx.config);
    let err = ReviewOps::new(&config, &fx.db).list_papers().unwrap_err();
    assert_eq!(err.code(), "project_required");
    Ok(())
}

// ---- request_review ----

#[test]
fn request_review_enqueues_without_submitting() -> Result<()> {
    let fx = Fixture::new()?;
    let outcome = fx.request(false, Approval::Granted)?;

    assert_eq!(outcome.disposition, RequestDisposition::Created);
    let job = &outcome.job;
    assert_eq!(job.status, JobStatus::Queued);
    assert_eq!(job.phase, JobPhase::Queued);
    assert!(!job.has_token && !job.terminal && !job.review_available);
    assert_eq!(job.next_poll_at, None);
    assert_eq!(job.version_no, 1);
    assert_eq!(job.round_no, 1);

    let hash = sha256_file(fx.pdf_path())?;
    assert_eq!(outcome.input.pdf_hash, hash);
    assert_eq!(outcome.input.version_source, "pdf_hash");
    assert_eq!(outcome.input.version_key, hash);
    assert_eq!(job.pdf_hash, hash);

    // The job is pinned to a snapshot that later edits of the source leave alone.
    let snapshot = job.snapshot_path.clone().expect("request pins a snapshot");
    assert_eq!(
        outcome.input.snapshot_path.as_deref(),
        Some(snapshot.as_str())
    );
    assert_eq!(job.pdf_path, fx.config.papers[0].pdf_path);
    fs::write(
        fx.pdf_path(),
        b"%PDF-1.4\n% edited after the request\n%%EOF\n",
    )?;
    assert_eq!(sha256_file(Path::new(&snapshot))?, hash);

    assert_eq!(outcome.reason, None);
    let enqueued = fx.events("job_enqueued")?;
    assert_eq!(enqueued.len(), 1);
    assert_eq!(enqueued[0]["source"], json!("manual_submit"));
    assert_eq!(enqueued[0]["enqueue_mode"], json!("deduplicate"));
    assert_eq!(enqueued[0]["request_key"], Value::Null);
    Ok(())
}

#[test]
fn request_review_returns_the_job_covering_the_same_pdf() -> Result<()> {
    let fx = Fixture::new()?;
    let first = fx.request(false, Approval::Granted)?;
    let second = fx.request(false, Approval::Granted)?;

    assert_eq!(second.disposition, RequestDisposition::Existing);
    assert_eq!(second.reason, Some(ExistingReason::Covered));
    assert_eq!(second.job.job_id, first.job.job_id);
    assert_eq!(
        fx.ops().list_jobs(&JobListQuery::default())?.jobs.len(),
        1,
        "a repeated request must not store a second job"
    );
    let skipped = fx.events("duplicate_skipped")?;
    assert_eq!(skipped.len(), 1);
    assert_eq!(skipped[0]["existing_job_id"], json!(first.job.job_id));
    assert_eq!(skipped[0]["source"], json!("manual_submit"));
    Ok(())
}

#[test]
fn request_key_replay_returns_the_bound_job_even_after_it_ended() -> Result<()> {
    let fx = Fixture::new()?;
    let first = fx.keyed_request(Some("req-1"), false, Approval::Granted)?;
    fx.db.update_job_state_unchecked(
        &first.job.job_id,
        JobStatus::Failed,
        None,
        Some(None),
        Some(Some("boom".to_string())),
    )?;

    let replay = fx.keyed_request(Some("req-1"), false, Approval::Granted)?;
    assert_eq!(replay.disposition, RequestDisposition::Existing);
    assert_eq!(replay.reason, Some(ExistingReason::RequestReplay));
    assert_eq!(replay.job.job_id, first.job.job_id);
    assert_eq!(replay.job.status, JobStatus::Failed);

    let fresh = fx.keyed_request(Some("req-2"), false, Approval::Granted)?;
    assert_eq!(fresh.disposition, RequestDisposition::Created);
    assert_ne!(fresh.job.job_id, first.job.job_id);
    Ok(())
}

#[test]
fn request_key_reused_for_other_content_is_a_conflict() -> Result<()> {
    let fx = Fixture::new()?;
    let first = fx.keyed_request(Some("req-1"), false, Approval::Granted)?;
    fs::write(fx.pdf_path(), b"%PDF-1.4\n% revised\n%%EOF\n")?;

    let err = fx
        .keyed_request(Some("req-1"), false, Approval::Granted)
        .unwrap_err();
    assert_eq!(err.code(), "request_conflict");
    let view = err.view();
    assert_eq!(view.details["existing_job_id"], json!(first.job.job_id));
    assert_eq!(view.details["mismatches"][0]["field"], json!("pdf_hash"));
    assert!(
        view.recovery
            .is_some_and(|hint| hint.contains(&first.job.job_id))
    );
    assert_eq!(fx.ops().list_jobs(&JobListQuery::default())?.jobs.len(), 1);

    let blank = fx
        .keyed_request(Some("  "), false, Approval::Granted)
        .unwrap_err();
    assert_eq!(blank.code(), "invalid_request");
    Ok(())
}

#[test]
fn request_review_after_a_pdf_change_creates_a_new_version() -> Result<()> {
    let fx = Fixture::new()?;
    let first = fx.request(false, Approval::Granted)?;
    fs::write(fx.pdf_path(), b"%PDF-1.4\n% revised\n%%EOF\n")?;
    let second = fx.request(false, Approval::Granted)?;

    assert_eq!(second.disposition, RequestDisposition::Created);
    assert_ne!(second.job.job_id, first.job.job_id);
    assert_ne!(second.input.pdf_hash, first.input.pdf_hash);
    assert_eq!(second.job.version_no, 2);
    Ok(())
}

#[test]
fn forced_request_creates_a_job_and_clears_sibling_cooldowns() -> Result<()> {
    let fx = Fixture::new()?;
    let stuck = fx.job(JobStatus::Processing, "old-hash")?;
    fx.db.update_job_state(
        &stuck.id,
        JobStatus::Processing,
        Some(3),
        Some(Some(Utc::now() + Duration::hours(2))),
        None,
    )?;
    let done = fx.job(JobStatus::Completed, "done-hash")?;
    let first = fx.request(false, Approval::Granted)?;

    let forced = fx.request(true, Approval::Granted)?;
    assert_eq!(forced.disposition, RequestDisposition::Created);
    assert_ne!(forced.job.job_id, first.job.job_id);

    let stuck = fx.db.get_job(&stuck.id)?.expect("job");
    assert_eq!(stuck.status, JobStatus::Processing);
    assert_eq!(stuck.attempt, 0);
    assert_eq!(stuck.next_poll_at, None);
    let done_after = fx.db.get_job(&done.id)?.expect("job");
    assert_eq!(
        done_after.updated_at, done.updated_at,
        "completed jobs keep their state"
    );

    let cleared = fx.events("force_clear_cooldown")?;
    assert_eq!(cleared.len(), 2, "the stuck job and the first request");
    assert!(
        cleared
            .iter()
            .all(|p| p["from_command"] == "submit --force")
    );
    Ok(())
}

#[test]
fn forced_request_without_siblings_only_creates_the_job() -> Result<()> {
    let fx = Fixture::new()?;
    let outcome = fx.request(true, Approval::Granted)?;
    assert_eq!(outcome.disposition, RequestDisposition::Created);
    assert!(fx.events("force_clear_cooldown")?.is_empty());
    Ok(())
}

#[test]
fn request_requiring_approval_waits_for_approve() -> Result<()> {
    let fx = Fixture::new()?;
    let outcome = fx.ops().request_review(&ReviewRequest {
        paper_id: "main".to_string(),
        request_key: None,
        force: false,
        approval: Approval::Required,
        origin: RequestOrigin::Agent,
    })?;
    assert_eq!(outcome.job.status, JobStatus::PendingApproval);
    assert_eq!(outcome.job.phase, JobPhase::AwaitingApproval);
    let enqueued = fx.events("job_enqueued")?;
    assert_eq!(enqueued.len(), 1);
    assert_eq!(enqueued[0]["source"], json!("agent_request"));
    assert_eq!(enqueued[0]["status"], json!("PENDING_APPROVAL"));

    let approved = fx.ops().approve_job(&JobRef::Paper("main".to_string()))?;
    assert_eq!(approved.previous_status, JobStatus::PendingApproval);
    assert_eq!(approved.job.job_id, outcome.job.job_id);
    assert_eq!(approved.job.status, JobStatus::Queued);
    assert_eq!(fx.events("approved")?, vec![json!({})]);

    let again = fx
        .ops()
        .approve_job(&JobRef::Id(outcome.job.job_id.clone()))
        .unwrap_err();
    assert_eq!(again.code(), "invalid_state");
    assert_eq!(
        again.to_string(),
        format!(
            "job {} is in status QUEUED, only PENDING_APPROVAL can be approved",
            outcome.job.job_id
        )
    );
    Ok(())
}

#[test]
fn request_review_reports_typed_errors() -> Result<()> {
    let mut fx = Fixture::new()?;
    let missing = fx
        .ops()
        .request_review(&ReviewRequest {
            paper_id: "nope".to_string(),
            request_key: None,
            force: false,
            approval: Approval::Granted,
            origin: RequestOrigin::Submit,
        })
        .unwrap_err();
    assert_eq!(missing.code(), "paper_not_found");
    assert!(matches!(&missing, OpError::PaperNotFound { known, .. } if known == &["main"]));
    let message = missing.to_string();
    assert!(message.contains("paper_id not found: nope"), "{message}");
    assert!(message.contains("known paper_ids: main"), "{message}");
    assert!(
        message.contains("reviewloop paper add --paper-id nope"),
        "{message}"
    );

    fx.config.providers.stanford.email.clear();
    let no_email = fx.request(false, Approval::Granted).unwrap_err();
    assert_eq!(no_email.code(), "submitter_email_unavailable");
    assert!(
        no_email
            .to_string()
            .contains("no email available for backend=stanford")
    );

    fs::remove_file(fx.pdf_path())?;
    let no_pdf = fx.request(false, Approval::Granted).unwrap_err();
    assert_eq!(no_pdf.code(), "pdf_not_found");
    assert!(no_pdf.to_string().starts_with("pdf file not found: "));

    let config = unscoped(&fx.config);
    let no_project = ReviewOps::new(&config, &fx.db)
        .request_review(&ReviewRequest {
            paper_id: "main".to_string(),
            request_key: None,
            force: false,
            approval: Approval::Granted,
            origin: RequestOrigin::Submit,
        })
        .unwrap_err();
    assert_eq!(no_project.code(), "project_required");
    assert!(
        fx.ops()
            .list_jobs(&JobListQuery::default())?
            .jobs
            .is_empty()
    );
    Ok(())
}

#[test]
fn paper_not_found_without_papers_suggests_adding_one() {
    let config = Config::default();
    let message = OpError::paper_not_found("myid", &config).to_string();
    assert!(message.contains("paper_id not found: myid"), "{message}");
    assert!(message.contains("no papers configured yet"), "{message}");
    assert!(
        message.contains("reviewloop paper add --paper-id myid"),
        "{message}"
    );
}

// ---- cspaper ----

#[test]
fn cspaper_request_without_provider_settings_is_refused_before_enqueue() -> Result<()> {
    let mut fx = Fixture::new()?;
    fx.add_cspaper_paper()?;

    // The key is checked first, whether or not a template is configured.
    for agent_id in [None, Some(AGENT_ID)] {
        fx.config.providers.cspaper.agent_id = agent_id.map(str::to_string);
        let err = fx.cspaper_request(Some("cs-1")).unwrap_err();
        assert_eq!(err.code(), "provider_not_configured");
        assert!(matches!(
            &err,
            OpError::ProviderNotConfigured {
                setting: "api_key",
                ..
            }
        ));
        let view = err.view();
        assert_eq!(
            view.details,
            json!({ "backend": "cspaper", "setting": "api_key" })
        );
        assert!(
            view.recovery
                .as_deref()
                .is_some_and(|hint| hint.contains(CSPAPER_API_KEY_ENV)),
            "{view:?}"
        );
    }

    fx.config.providers.cspaper.api_key = Some(Redacted(CSPAPER_KEY.to_string()));
    fx.config.providers.cspaper.agent_id = None;
    let err = fx.cspaper_request(Some("cs-1")).unwrap_err();
    assert_eq!(err.code(), "provider_not_configured");
    let view = err.view();
    assert_eq!(
        view.details,
        json!({ "backend": "cspaper", "setting": "agent_id" })
    );
    assert!(view.message.contains(CSPAPER_PAPER), "{view:?}");
    assert!(
        view.recovery
            .as_deref()
            .is_some_and(|hint| hint.contains("agent_id")),
        "{view:?}"
    );
    assert_key_free(&view);
    assert!(!format!("{err:?}").contains(CSPAPER_KEY), "{err:?}");

    // Refused requests enqueue nothing and leave their request key unbound.
    assert!(
        fx.ops()
            .list_jobs(&JobListQuery::default())?
            .jobs
            .is_empty()
    );
    assert!(fx.cspaper_events()?.is_empty());

    // A per-paper venue is a template too.
    fx.config.papers[1].venue = Some(OTHER_AGENT_ID.to_string());
    let outcome = fx.cspaper_request(Some("cs-1"))?;
    assert_eq!(outcome.disposition, RequestDisposition::Created);
    assert_eq!(outcome.job.venue.as_deref(), Some(OTHER_AGENT_ID));
    assert_key_free(&outcome);
    Ok(())
}

#[test]
fn cspaper_request_records_template_and_desk_rejection_option() -> Result<()> {
    let mut fx = Fixture::new()?;
    fx.add_cspaper_paper()?;
    fx.configure_cspaper();

    let outcome = fx.cspaper_request(None)?;
    assert_eq!(outcome.disposition, RequestDisposition::Created);
    let job = &outcome.job;
    assert_eq!(job.backend, cspaper::BACKEND);
    assert_eq!(job.status, JobStatus::Queued);
    assert_eq!(job.venue.as_deref(), Some(AGENT_ID));
    let job_json = serde_json::to_value(job)?;
    assert_eq!(job_json["review_options"], desk_rejection(true));
    let input = serde_json::to_value(&outcome.input)?;
    assert_eq!(input["backend"], json!(cspaper::BACKEND));
    assert_eq!(input["venue"], json!(AGENT_ID));
    assert_eq!(input["review_options"], desk_rejection(true));

    // CSPaper authenticates by key, so the job carries no submitter email.
    let stored = fx.db.get_job(&job.job_id)?.expect("job");
    assert_eq!(stored.email, "");
    assert_eq!(
        stored.review_options.get(cspaper::DESK_REJECTION_ENABLED),
        Some("true")
    );

    let events = fx.cspaper_events()?;
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0]["backend"], json!(cspaper::BACKEND));
    assert_eq!(events[0]["venue"], json!(AGENT_ID));
    assert_eq!(events[0]["review_options"], desk_rejection(true));

    // Without desk-rejection screening it is another review of the same manuscript.
    fx.config.providers.cspaper.desk_rejection_enabled = false;
    let unscreened = fx.cspaper_request(None)?;
    assert_eq!(unscreened.disposition, RequestDisposition::Created);
    assert_ne!(unscreened.job.job_id, job.job_id);
    assert_eq!(unscreened.job.round_no, 2);
    assert_eq!(
        serde_json::to_value(&unscreened.job)?["review_options"],
        desk_rejection(false)
    );

    assert_key_free(&outcome);
    assert_key_free(&unscreened);
    assert_key_free(&fx.ops().get_job(&job.job_id)?);
    assert_key_free(&fx.ops().list_jobs(&JobListQuery::default())?);
    assert_key_free(&fx.cspaper_events()?);
    Ok(())
}

#[test]
fn list_papers_reports_cspaper_template_and_options() -> Result<()> {
    let mut fx = Fixture::new()?;
    fx.add_cspaper_paper()?;
    fx.configure_cspaper();
    fx.config.providers.cspaper.desk_rejection_enabled = false;

    let papers = fx.ops().list_papers()?;
    let json = serde_json::to_value(&papers)?;
    assert_eq!(json[0]["paper_id"], json!("main"));
    assert_eq!(json[0]["review_options"], json!({}));
    assert_eq!(json[1]["paper_id"], json!(CSPAPER_PAPER));
    assert_eq!(json[1]["backend"], json!(cspaper::BACKEND));
    assert_eq!(json[1]["venue"], json!(AGENT_ID));
    assert_eq!(json[1]["review_options"], desk_rejection(false));
    assert_key_free(&papers);

    // A Stanford request records no options anywhere.
    let stanford = serde_json::to_value(fx.request(false, Approval::Granted)?)?;
    assert_eq!(stanford["job"]["review_options"], json!({}));
    assert_eq!(stanford["input"]["review_options"], json!({}));
    let job_id = stanford["job"]["job_id"].as_str().expect("job_id");
    assert_eq!(
        serde_json::to_value(fx.ops().get_job(job_id)?)?["review_options"],
        json!({})
    );
    Ok(())
}

#[test]
fn cspaper_request_key_replays_until_the_template_or_options_change() -> Result<()> {
    let mut fx = Fixture::new()?;
    fx.add_cspaper_paper()?;
    fx.configure_cspaper();

    let first = fx.cspaper_request(Some("cs-req"))?;
    assert_eq!(first.disposition, RequestDisposition::Created);
    let replay = fx.cspaper_request(Some("cs-req"))?;
    assert_eq!(replay.disposition, RequestDisposition::Existing);
    assert_eq!(replay.reason, Some(ExistingReason::RequestReplay));
    assert_eq!(replay.job.job_id, first.job.job_id);

    fx.config.providers.cspaper.agent_id = Some(OTHER_AGENT_ID.to_string());
    let switched = fx.cspaper_request(Some("cs-req")).unwrap_err();
    assert_eq!(switched.code(), "request_conflict");
    let view = switched.view();
    assert_eq!(view.details["existing_job_id"], json!(first.job.job_id));
    assert_eq!(
        view.details["mismatches"],
        json!([{ "field": "venue", "recorded": AGENT_ID, "requested": OTHER_AGENT_ID }])
    );
    assert_key_free(&view);

    // The other template under a new key is a new review.
    let other = fx.cspaper_request(Some("cs-req-neurips"))?;
    assert_eq!(other.disposition, RequestDisposition::Created);
    assert_ne!(other.job.job_id, first.job.job_id);
    assert_eq!(other.job.venue.as_deref(), Some(OTHER_AGENT_ID));

    fx.config.providers.cspaper.agent_id = Some(AGENT_ID.to_string());
    fx.config.providers.cspaper.desk_rejection_enabled = false;
    let flipped = fx.cspaper_request(Some("cs-req")).unwrap_err();
    assert_eq!(flipped.code(), "request_conflict");
    let view = flipped.view();
    assert_eq!(
        view.details["mismatches"],
        json!([{
            "field": "review_options",
            "recorded": r#"{"desk_rejection_enabled":"true"}"#,
            "requested": r#"{"desk_rejection_enabled":"false"}"#,
        }])
    );
    assert_key_free(&view);

    assert_eq!(fx.ops().list_jobs(&JobListQuery::default())?.jobs.len(), 2);
    assert_key_free(&first);
    assert_key_free(&replay);
    assert_key_free(&other);
    Ok(())
}

// ---- job references ----

#[test]
fn paper_reference_with_no_eligible_job_names_the_statuses() -> Result<()> {
    let fx = Fixture::new()?;
    fx.job(JobStatus::Queued, "a")?;
    let err = fx
        .ops()
        .find_job(&JobRef::Paper("main".to_string()), Eligibility::APPROVE)
        .unwrap_err();
    assert_eq!(err.code(), "no_eligible_job");
    assert_eq!(
        err.to_string(),
        "no approve-eligible job for paper_id=main (looking for statuses: PENDING_APPROVAL)"
    );
    Ok(())
}

#[test]
fn paper_reference_resolves_a_single_eligible_job() -> Result<()> {
    let fx = Fixture::new()?;
    fx.job(JobStatus::Completed, "a")?;
    let processing = fx.job(JobStatus::Processing, "b")?;
    let complete = Eligibility {
        action: "complete",
        statuses: &[JobStatus::Processing, JobStatus::Submitted],
    };
    let found = fx
        .ops()
        .find_job(&JobRef::Paper("main".to_string()), complete)?;
    assert_eq!(found.id, processing.id);
    Ok(())
}

#[test]
fn paper_reference_matching_several_jobs_lists_candidates() -> Result<()> {
    let fx = Fixture::new()?;
    fx.job(JobStatus::PendingApproval, "a")?;
    fx.job(JobStatus::PendingApproval, "b")?;
    let err = fx
        .ops()
        .find_job(&JobRef::Paper("main".to_string()), Eligibility::APPROVE)
        .unwrap_err();
    assert_eq!(err.code(), "ambiguous_job");
    assert!(matches!(&err, OpError::AmbiguousJob { candidates, .. } if candidates.len() == 2));
    let message = err.to_string();
    assert!(
        message.contains("multiple jobs match paper_id=main for approve"),
        "{message}"
    );
    assert!(message.contains("pass --job-id explicitly"), "{message}");
    assert!(message.contains("candidates:"), "{message}");

    let details = err.view().details;
    let candidates = details["candidates"].as_array().expect("candidates");
    assert_eq!(candidates.len(), 2);
    assert!(
        candidates
            .iter()
            .all(|c| c["status"] == "PENDING_APPROVAL" && c["job_id"].is_string())
    );
    Ok(())
}

#[test]
fn retry_paper_reference_matches_failed_jobs_only_when_asked() -> Result<()> {
    let fx = Fixture::new()?;
    let failed = fx.job(JobStatus::Failed, "a")?;
    let paper = JobRef::Paper("main".to_string());

    let narrow = fx
        .ops()
        .find_job(&paper, Eligibility::retry(false))
        .unwrap_err();
    assert!(
        narrow.to_string().contains("no retry-eligible job"),
        "{narrow}"
    );
    assert_eq!(
        fx.ops().find_job(&paper, Eligibility::retry(true))?.id,
        failed.id
    );

    let queued = fx.job(JobStatus::Queued, "b")?;
    assert_eq!(
        fx.ops().find_job(&paper, Eligibility::retry(false))?.id,
        queued.id
    );
    Ok(())
}

#[test]
fn job_ids_are_scoped_to_the_project_unless_unscoped() -> Result<()> {
    let fx = Fixture::new()?;
    let foreign = fx.insert_job("other-project", JobStatus::Queued, "a")?;

    let err = fx.ops().get_job(&foreign.id).unwrap_err();
    assert_eq!(err.code(), "job_not_found");
    assert_eq!(err.to_string(), format!("job not found: {}", foreign.id));

    let config = unscoped(&fx.config);
    let any = ReviewOps::new(&config, &fx.db);
    assert_eq!(any.get_job(&foreign.id)?.project_id, "other-project");
    let paper = any
        .find_job(&JobRef::Paper("main".to_string()), Eligibility::CANCEL)
        .unwrap_err();
    assert_eq!(paper.code(), "project_required");
    Ok(())
}

// ---- get_job / list_jobs ----

#[test]
fn get_job_reads_the_database_and_hides_the_token() -> Result<()> {
    let fx = Fixture::new()?;
    let job = fx.submitted_job("a")?;
    fx.db.update_job_state(
        &job.id,
        JobStatus::Processing,
        None,
        None,
        Some(Some(format!(
            "network error: https://paperreview.ai/api/review/{TOKEN}"
        ))),
    )?;

    let view = fx.ops().get_job(&job.id)?;
    assert_eq!(view.status, JobStatus::Processing);
    assert_eq!(view.phase, JobPhase::Submitted);
    assert!(view.has_token);
    assert_eq!(
        view.last_error.as_deref(),
        Some("network error: https://paperreview.ai/api/review/[redacted]")
    );
    assert_eq!(view.next_poll_at, job.next_poll_at);
    assert_token_free(&view);
    Ok(())
}

#[test]
fn list_jobs_filters_active_jobs_and_reports_truncation() -> Result<()> {
    let fx = Fixture::new()?;
    let completed = fx.completed_job(&review_json())?;
    let queued = fx.job(JobStatus::Queued, "q")?;
    let pending = fx.job(JobStatus::PendingApproval, "p")?;

    let all = fx.ops().list_jobs(&JobListQuery::default())?;
    let ids: Vec<_> = all.jobs.iter().map(|job| job.job_id.clone()).collect();
    assert_eq!(
        ids,
        vec![pending.id.clone(), queued.id.clone(), completed.id.clone()]
    );
    assert!(!all.truncated);
    assert!(all.jobs[2].review_available);
    assert!(all.jobs[2].review_completed_at.is_some());
    assert_token_free(&all);

    let active = fx.ops().list_jobs(&JobListQuery {
        active_only: true,
        ..JobListQuery::default()
    })?;
    assert_eq!(active.jobs.len(), 2);

    let capped = fx.ops().list_jobs(&JobListQuery {
        limit: Some(1),
        ..JobListQuery::default()
    })?;
    assert_eq!(capped.jobs.len(), 1);
    assert!(capped.truncated);
    Ok(())
}

// ---- get_review ----

#[test]
fn get_review_before_completion_is_not_available() -> Result<()> {
    let fx = Fixture::new()?;
    let job = fx.submitted_job("a")?;
    let err = fx
        .ops()
        .get_review(&ReviewQuery {
            job_id: job.id.clone(),
            part: ReviewPart::Markdown,
        })
        .unwrap_err();
    assert_eq!(err.code(), "review_not_available");
    assert!(matches!(
        err,
        OpError::ReviewNotAvailable {
            status: JobStatus::Processing,
            ..
        }
    ));
    assert!(err.recovery().is_some_and(|hint| hint.starts_with("wait")));

    let failed = fx.job(JobStatus::Timeout, "b")?;
    let ended = fx
        .ops()
        .get_review(&ReviewQuery {
            job_id: failed.id.clone(),
            part: ReviewPart::Markdown,
        })
        .unwrap_err();
    assert!(
        ended
            .recovery()
            .is_some_and(|hint| hint.starts_with("the job ended without a review"))
    );
    Ok(())
}

#[test]
fn get_review_redacts_the_token_the_review_was_fetched_with() -> Result<()> {
    let fx = Fixture::new()?;
    let job = fx.completed_job(&review_json())?;
    let old_token = "old-token-5555";
    let raw = json!({
        "title": "A Paper",
        "content": format!("fetched via {old_token}"),
        "token": old_token,
    });
    fx.db
        .upsert_review(&job.id, old_token, &raw.to_string(), "unused")?;

    for part in [ReviewPart::Markdown, ReviewPart::Raw] {
        let view = fx.ops().get_review(&ReviewQuery {
            job_id: job.id.clone(),
            part,
        })?;
        let json = serde_json::to_string(&view)?;
        assert!(!json.contains(old_token), "old token leaked: {json}");
        assert!(!json.contains(TOKEN), "token leaked: {json}");
    }
    Ok(())
}

#[test]
fn get_review_returns_requested_parts_without_the_token() -> Result<()> {
    let fx = Fixture::new()?;
    let job = fx.completed_job(&review_json())?;
    let query = |part| ReviewQuery {
        job_id: job.id.clone(),
        part,
    };

    let summary = fx.ops().get_review(&query(ReviewPart::Summary))?;
    assert_eq!(summary.job.phase, JobPhase::Completed);
    assert_eq!(summary.score.as_deref(), Some("6.5"));
    assert_eq!(summary.title.as_deref(), Some("A Paper"));
    assert_eq!(
        summary.sections,
        vec!["summary", "strengths", "weaknesses", "zz_extra"]
    );
    assert!(summary.markdown.is_none() && summary.section.is_none() && summary.raw.is_none());
    let artifacts = &summary.artifacts;
    assert!(
        artifacts
            .review_md
            .as_deref()
            .is_some_and(|p| p.ends_with("review.md"))
    );
    assert!(
        artifacts
            .review_json
            .as_deref()
            .is_some_and(|p| p.ends_with("review.json"))
    );
    assert!(!serde_json::to_string(&summary)?.contains("meta.json"));

    let markdown = fx.ops().get_review(&query(ReviewPart::Markdown))?;
    let text = markdown.markdown.as_deref().expect("markdown");
    assert!(text.starts_with("# Review Summary"), "{text}");
    assert!(text.contains("Clear writing. ref [redacted]"), "{text}");

    let section = fx
        .ops()
        .get_review(&query(ReviewPart::Section("strengths".to_string())))?;
    assert_eq!(
        section.section.map(|s| s.text).as_deref(),
        Some("Clear writing. ref [redacted]")
    );

    let raw = fx.ops().get_review(&query(ReviewPart::Raw))?;
    let raw_json = raw.raw.as_ref().expect("raw");
    assert_eq!(
        raw_json["link"],
        json!("https://paperreview.ai/review/[redacted]")
    );

    for view in [&summary, &markdown, &raw] {
        let json = serde_json::to_value(view)?;
        assert!(!json.to_string().contains(TOKEN), "token leaked: {json}");
    }

    let missing = fx
        .ops()
        .get_review(&query(ReviewPart::Section("questions".to_string())))
        .unwrap_err();
    assert_eq!(missing.code(), "section_not_found");
    assert!(matches!(missing, OpError::SectionNotFound { available, .. } if available.len() == 4));
    Ok(())
}

// ---- retry ----

#[test]
fn retry_requeues_a_tokenless_job_for_submission() -> Result<()> {
    let fx = Fixture::new()?;
    let job = fx.job(JobStatus::Failed, "a")?;
    let outcome = fx.ops().retry_job(&RetryRequest {
        job: JobRef::Id(job.id.clone()),
        force: false,
        include_failed: false,
        caller_executes: false,
    })?;
    assert_eq!(outcome.action, RetryAction::SubmissionQueued);
    assert_eq!(outcome.previous_status, JobStatus::Failed);
    assert_eq!(outcome.job.status, JobStatus::Queued);
    assert_eq!(outcome.job.attempt, 0);
    assert_eq!(outcome.job.next_poll_at, None);
    assert_eq!(fx.events("retried")?, vec![json!({})]);
    Ok(())
}

#[test]
fn retry_reschedules_a_token_backed_job_for_polling() -> Result<()> {
    let fx = Fixture::new()?;
    let job = fx.submitted_job("a")?;
    fx.db
        .update_job_state(&job.id, JobStatus::Timeout, Some(4), Some(None), None)?;
    let before = Utc::now();
    let outcome = fx.ops().retry_job(&RetryRequest {
        job: JobRef::Id(job.id.clone()),
        force: false,
        include_failed: false,
        caller_executes: false,
    })?;
    assert_eq!(outcome.action, RetryAction::PollScheduled);
    assert_eq!(outcome.job.status, JobStatus::Processing);
    assert_eq!(outcome.job.attempt, 0);
    assert!(outcome.job.next_poll_at.is_some_and(|at| at > before));
    assert_token_free(&outcome);
    Ok(())
}

#[test]
fn forced_retry_makes_the_job_due_now() -> Result<()> {
    let fx = Fixture::new()?;
    let failed = fx.job(JobStatus::FailedNeedsManual, "a")?;
    let submit = fx.ops().retry_job(&RetryRequest {
        job: JobRef::Id(failed.id.clone()),
        force: true,
        include_failed: false,
        caller_executes: false,
    })?;
    assert_eq!(submit.action, RetryAction::SubmitNow);
    assert_eq!(submit.job.status, JobStatus::Queued);

    let processing = fx.submitted_job("b")?;
    let poll = fx.ops().retry_job(&RetryRequest {
        job: JobRef::Id(processing.id.clone()),
        force: true,
        include_failed: false,
        caller_executes: false,
    })?;
    assert_eq!(poll.action, RetryAction::PollNow);
    assert!(poll.job.next_poll_at.is_some_and(|at| at <= Utc::now()));

    let modes: Vec<_> = fx
        .events("manual_rate_limit_override")?
        .into_iter()
        .map(|payload| payload["mode"].clone())
        .collect();
    assert_eq!(modes, vec![json!("submit"), json!("poll")]);
    Ok(())
}

#[test]
fn forced_retry_rejects_statuses_it_cannot_act_on() -> Result<()> {
    let fx = Fixture::new()?;
    let completed = fx.job(JobStatus::Completed, "a")?;
    let err = fx
        .ops()
        .retry_job(&RetryRequest {
            job: JobRef::Id(completed.id.clone()),
            force: true,
            include_failed: false,
            caller_executes: false,
        })
        .unwrap_err();
    assert_eq!(err.code(), "invalid_state");
    assert_eq!(
        err.to_string(),
        "--force for tokenless jobs only supports QUEUED/SUBMITTED/FAILED/FAILED_NEEDS_MANUAL/TIMEOUT"
    );
    assert_eq!(
        fx.db.get_job(&completed.id)?.expect("job").status,
        JobStatus::Completed
    );
    Ok(())
}

#[test]
fn retry_never_bypasses_approval() -> Result<()> {
    let fx = Fixture::new()?;
    let pending = fx.job(JobStatus::PendingApproval, "a")?;
    for force in [false, true] {
        let err = fx
            .ops()
            .retry_job(&RetryRequest {
                job: JobRef::Id(pending.id.clone()),
                force,
                include_failed: false,
                caller_executes: false,
            })
            .unwrap_err();
        assert_eq!(err.code(), "invalid_state");
        assert_eq!(
            err.to_string(),
            format!(
                "job {} is in status PENDING_APPROVAL; approve it instead of retrying",
                pending.id
            )
        );
        assert_eq!(
            err.recovery().as_deref(),
            Some("approve the job with approve_job")
        );
    }
    assert_eq!(
        fx.db.get_job(&pending.id)?.expect("job").status,
        JobStatus::PendingApproval
    );
    assert!(fx.events("retried")?.is_empty());
    Ok(())
}

#[test]
fn forced_retry_executed_by_the_caller_keeps_the_poll_schedule() -> Result<()> {
    let fx = Fixture::new()?;
    let job = fx.submitted_job("a")?;
    let outcome = fx.ops().retry_job(&RetryRequest {
        job: JobRef::Id(job.id.clone()),
        force: true,
        include_failed: false,
        caller_executes: true,
    })?;
    assert_eq!(outcome.action, RetryAction::PollNow);
    let after = fx.db.get_job(&job.id)?.expect("job");
    assert_eq!(after.next_poll_at, job.next_poll_at);
    assert_eq!(after.updated_at, job.updated_at);
    assert_eq!(fx.events("manual_rate_limit_override")?.len(), 1);
    Ok(())
}

#[test]
fn retry_refuses_a_job_from_another_projects_config() -> Result<()> {
    let fx = Fixture::new()?;
    let foreign = fx.insert_job("other-project", JobStatus::Failed, "a")?;
    let config = unscoped(&fx.config);
    let err = ReviewOps::new(&config, &fx.db)
        .retry_job(&RetryRequest {
            job: JobRef::Id(foreign.id.clone()),
            force: false,
            include_failed: false,
            caller_executes: false,
        })
        .unwrap_err();
    assert_eq!(err.code(), "project_mismatch");
    assert_eq!(
        err.to_string(),
        format!(
            "job {} belongs to project other-project; retry it with that project's config",
            foreign.id
        )
    );
    let renamed = OpError::ProjectMismatch {
        job_id: "j".into(),
        job_project_id: "a".into(),
        context_project_id: "b".into(),
    };
    assert_eq!(
        renamed.to_string(),
        "job j belongs to project a, but the loaded config declares project b"
    );
    assert_eq!(
        fx.db.get_job(&foreign.id)?.expect("job").status,
        JobStatus::Failed
    );
    Ok(())
}

// ---- cancel ----

#[test]
fn cancel_marks_the_job_cancelled_locally() -> Result<()> {
    let fx = Fixture::new()?;
    let job = fx.submitted_job("a")?;
    let outcome = fx.ops().cancel_job(&CancelRequest {
        job: JobRef::Paper("main".to_string()),
        reason: Some("wrong draft".to_string()),
    })?;
    assert_eq!(outcome.previous_status, JobStatus::Processing);
    assert_eq!(outcome.job.status, JobStatus::Failed);
    assert_eq!(outcome.job.phase, JobPhase::Cancelled);
    assert!(
        outcome.job.has_token,
        "the provider may still hold the submission"
    );
    assert_eq!(
        fx.db.get_job(&job.id)?.expect("job").last_error.as_deref(),
        Some("cancelled by user: wrong draft")
    );
    assert_eq!(
        fx.events("cancelled")?,
        vec![json!({
            "reason": "wrong draft",
            "previous_status": "PROCESSING",
            "previous_submit_stage": null,
            "lease_was_active": false,
        })]
    );

    let again = fx
        .ops()
        .cancel_job(&CancelRequest {
            job: JobRef::Id(job.id.clone()),
            reason: None,
        })
        .unwrap_err();
    assert_eq!(again.code(), "invalid_state");
    assert_eq!(
        again.to_string(),
        format!(
            "job {} is already in terminal status FAILED; cannot cancel",
            job.id
        )
    );

    let by_paper = fx
        .ops()
        .cancel_job(&CancelRequest {
            job: JobRef::Paper("main".to_string()),
            reason: None,
        })
        .unwrap_err();
    assert_eq!(by_paper.code(), "no_eligible_job");
    Ok(())
}

#[test]
fn unscoped_cancel_reaches_any_project() -> Result<()> {
    let fx = Fixture::new()?;
    let foreign = fx.insert_job("other-project", JobStatus::PendingApproval, "a")?;
    let config = unscoped(&fx.config);
    let outcome = ReviewOps::new(&config, &fx.db).cancel_job(&CancelRequest {
        job: JobRef::Id(foreign.id.clone()),
        reason: None,
    })?;
    assert_eq!(outcome.job.phase, JobPhase::Cancelled);
    assert_eq!(
        fx.db
            .get_job(&foreign.id)?
            .expect("job")
            .last_error
            .as_deref(),
        Some("cancelled by user")
    );
    Ok(())
}

// ---- contract ----

#[test]
fn operations_map_to_unique_documented_tools() -> Result<()> {
    let doc = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/review-operations.md"),
    )?;
    let names: BTreeSet<_> = Operation::ALL.iter().map(|op| op.tool_name()).collect();
    assert_eq!(
        names.len(),
        Operation::ALL.len(),
        "tool names must be unique"
    );
    for op in Operation::ALL {
        let row = format!("| `{}` | `ReviewOps::{}` |", op.tool_name(), op.tool_name());
        assert!(doc.contains(&row), "docs must map {op:?}: {row}");
    }
    let read_only: Vec<_> = Operation::ALL
        .iter()
        .filter(|op| op.read_only())
        .map(|op| op.tool_name())
        .collect();
    assert_eq!(
        read_only,
        vec![
            "list_projects",
            "list_papers",
            "get_job",
            "list_jobs",
            "get_review"
        ]
    );
    Ok(())
}

#[test]
fn request_review_rejects_a_pdf_the_provider_cannot_accept() -> Result<()> {
    let fx = Fixture::new()?;
    let mut oversized = b"%PDF-1.4\n".to_vec();
    oversized.resize(10 * 1024 * 1024 + 1, b' ');
    fs::write(fx.pdf_path(), &oversized)?;

    let rejected = fx.request(false, Approval::Granted).unwrap_err();
    assert_eq!(rejected.code(), "input_rejected");
    let message = rejected.to_string();
    assert!(message.contains("10 MiB"), "{message}");
    assert_eq!(
        serde_json::to_value(rejected.view())?["details"],
        json!({ "paper_id": "main", "backend": "stanford" })
    );
    assert!(
        fx.ops()
            .list_jobs(&JobListQuery::default())?
            .jobs
            .is_empty()
    );

    fs::write(fx.pdf_path(), b"PK\x03\x04 a zip, not a pdf")?;
    let not_pdf = fx.request(false, Approval::Granted).unwrap_err();
    assert_eq!(not_pdf.code(), "input_rejected");
    assert!(not_pdf.to_string().contains("not a PDF"), "{not_pdf}");
    Ok(())
}

#[test]
fn request_review_reports_the_provider_review_coverage() -> Result<()> {
    let fx = Fixture::new()?;
    assert!(
        fx.request(false, Approval::Granted)?
            .input
            .notices
            .is_empty()
    );

    let long = Fixture::new()?;
    fs::write(
        long.pdf_path(),
        format!("%PDF-1.4\n{}%%EOF\n", "<< /Type /Page >>\n".repeat(16)),
    )?;
    let outcome = long.request(false, Approval::Granted)?;
    assert_eq!(
        outcome.input.notices.len(),
        1,
        "{:?}",
        outcome.input.notices
    );
    assert!(
        outcome.input.notices[0].contains("first 15 pages"),
        "{:?}",
        outcome.input.notices
    );
    assert!(
        outcome.input.notices[0].contains("16"),
        "{:?}",
        outcome.input.notices
    );
    Ok(())
}

#[test]
fn every_error_code_is_documented_with_a_view() -> Result<()> {
    let doc = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/review-operations.md"),
    )?;
    let errors = [
        OpError::ProjectRequired,
        OpError::ProjectMismatch {
            job_id: "j".into(),
            job_project_id: "a".into(),
            context_project_id: "b".into(),
        },
        OpError::InvalidRequest {
            field: "request_key",
            message: "blank".into(),
        },
        OpError::RequestConflict(EnqueueConflict {
            project_id: "p".into(),
            request_key: "k".into(),
            existing_job_id: "j".into(),
            mismatches: vec![],
        }),
        OpError::PaperNotFound {
            paper_id: "p".into(),
            known: vec![],
        },
        OpError::PdfNotFound {
            paper_id: "p".into(),
            path: "/x.pdf".into(),
        },
        OpError::SubmitterEmailUnavailable {
            backend: "stanford".into(),
            detail: "none".into(),
        },
        OpError::InputRejected {
            paper_id: "p".into(),
            backend: "stanford".into(),
            reason: "too big".into(),
        },
        OpError::ProviderNotConfigured {
            backend: "cspaper".into(),
            setting: "api_key",
            message: "none".into(),
        },
        OpError::JobNotFound { job_id: "j".into() },
        OpError::NoEligibleJob {
            paper_id: "p".into(),
            action: "approve",
            statuses: Eligibility::APPROVE.statuses,
        },
        OpError::AmbiguousJob {
            paper_id: "p".into(),
            action: "approve",
            candidates: vec![],
        },
        OpError::InvalidState {
            job_id: "j".into(),
            status: JobStatus::Completed,
            operation: Operation::CancelJob,
            message: "m".into(),
        },
        OpError::ReviewNotAvailable {
            job_id: "j".into(),
            status: JobStatus::Queued,
        },
        OpError::SectionNotFound {
            job_id: "j".into(),
            section: "s".into(),
            available: vec![],
        },
        OpError::Internal(anyhow::anyhow!("disk full")),
    ];
    let codes: BTreeSet<_> = errors.iter().map(OpError::code).collect();
    assert_eq!(codes.len(), errors.len(), "error codes must be unique");
    for error in &errors {
        let row = format!("| `{}` |", error.code());
        assert!(doc.contains(&row), "docs must describe {}", error.code());
        let view = error.view();
        assert_eq!(view.recovery.is_none(), error.code() == "internal");
    }

    assert_eq!(
        serde_json::to_value(OpError::JobNotFound { job_id: "j".into() }.view())?,
        json!({
            "code": "job_not_found",
            "message": "job not found: j",
            "recovery": "check the job_id; list_jobs shows this project's jobs",
            "details": { "job_id": "j" }
        })
    );
    Ok(())
}

#[test]
fn job_view_serializes_the_documented_fields() -> Result<()> {
    let fx = Fixture::new()?;
    let view = fx.request(false, Approval::Granted)?.job;
    let json = serde_json::to_value(&view)?;
    let keys: Vec<_> = json.as_object().expect("object").keys().cloned().collect();
    let doc = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/review-operations.md"),
    )?;
    for key in &keys {
        assert!(
            doc.contains(&format!("| `{key}` |")),
            "docs must describe JobView.{key}"
        );
    }
    assert_eq!(json["status"], json!("QUEUED"));
    assert_eq!(json["phase"], json!("queued"));
    assert!(
        json["created_at"]
            .as_str()
            .is_some_and(|at| at.ends_with('Z'))
    );
    Ok(())
}

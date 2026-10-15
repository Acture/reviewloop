//! OSS-339: `reviewloop mcp` as a standard stdio MCP server, driven through
//! the real binary in an isolated home, config dir and state dir. An
//! independent MCP client (the official Rust SDK) discovers and calls the
//! tools; every successful result is validated against the tool's
//! `outputSchema`. One test speaks raw JSON-RPC to check the stdio framing;
//! one carries a review from request to result through the machine
//! supervisor and a mock provider.

mod common;

use anyhow::{Context, Result};
use common::{Answer, FleetBackends, MockBackend, ready_review, receipt};
use reviewloop::{
    artifact::write_review_artifacts,
    backend::provider_source,
    config::{Config, GlobalConfigFile, MachineConfig},
    db::Db,
    model::{JobPdf, JobStatus, NewJob},
    supervisor::{Supervisor, SupervisorMemory},
    util::sha256_file,
};
use rmcp::{
    RoleClient, ServiceError, ServiceExt,
    model::{CallToolRequestParams, CallToolResult, ErrorCode, Tool},
    service::RunningService,
    transport::TokioChildProcess,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::AtomicBool,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
};

/// readOnly, destructive, idempotent, openWorld.
type Hints = (bool, bool, bool, bool);

/// docs/review-operations.md "Tools", for a server with approval required:
/// (tool, arguments, hints).
const CONTRACT: [(&str, &[&str], Hints); 12] = [
    ("list_projects", &[], (true, false, true, false)),
    ("list_papers", &["project_id"], (true, false, true, false)),
    (
        "request_review",
        &["paper_id", "request_key", "force", "project_id"],
        (false, false, false, false),
    ),
    (
        "get_job",
        &["job_id", "project_id"],
        (true, false, true, false),
    ),
    (
        "list_jobs",
        &["paper_id", "active_only", "limit", "project_id"],
        (true, false, true, false),
    ),
    (
        "get_review",
        &["job_id", "part", "section", "project_id"],
        (true, false, true, false),
    ),
    (
        "approve_job",
        &["job_id", "paper_id", "project_id"],
        (false, false, false, true),
    ),
    (
        "retry_job",
        &[
            "job_id",
            "paper_id",
            "include_failed",
            "force",
            "project_id",
        ],
        (false, false, false, true),
    ),
    (
        "get_worker_status",
        &["project_id"],
        (true, false, true, false),
    ),
    (
        "enable_project",
        &["project_id", "replace"],
        (false, false, true, true),
    ),
    (
        "disable_project",
        &["project_id"],
        (false, false, true, false),
    ),
    (
        "cancel_job",
        &["job_id", "paper_id", "reason", "project_id"],
        (false, true, false, false),
    ),
];
const TOKEN: &str = "tok-mcp-0123456789abcdef";
const PDF: &[u8] = b"%PDF-1.4\n%%EOF\n";

struct Machine {
    _tmp: tempfile::TempDir,
    root: PathBuf,
}

impl Machine {
    fn new() -> Result<Self> {
        Self::with_global_config("")
    }

    /// A machine whose global config holds the Stanford submitter email plus
    /// `extra`.
    fn with_global_config(extra: &str) -> Result<Self> {
        let tmp = tempfile::tempdir()?;
        let root = tmp.path().canonicalize()?;
        fs::create_dir_all(root.join("home"))?;
        fs::create_dir_all(root.join("xdg/reviewloop"))?;
        fs::write(
            root.join("xdg/reviewloop/config.toml"),
            format!("{extra}\n[providers.stanford]\nemail = \"agent@example.edu\"\n"),
        )?;
        Ok(Self { _tmp: tmp, root })
    }

    fn state_dir(&self) -> PathBuf {
        self.root.join("state")
    }

    /// A repository with one stanford paper `main` at `paper.pdf`.
    fn repo(&self, name: &str, project_id: &str) -> Result<PathBuf> {
        let dir = self.root.join(name);
        fs::create_dir_all(dir.join(".git"))?;
        fs::write(dir.join("paper.pdf"), PDF)?;
        fs::write(
            dir.join("reviewloop.toml"),
            project_toml(project_id, &["main"]),
        )?;
        Ok(dir)
    }

    fn command(&self, cwd: &Path, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_reviewloop"));
        command
            .args(args)
            .current_dir(cwd)
            .env("HOME", self.root.join("home"))
            .env("XDG_CONFIG_HOME", self.root.join("xdg"))
            .env("REVIEWLOOP_STATE_DIR", self.state_dir())
            .env_remove("REVIEWLOOP_CSPAPER_API_KEY")
            .env_remove("RUST_LOG");
        command
    }

    /// Run a CLI command to completion (to register a project).
    async fn cli(&self, cwd: &Path, args: &[&str]) -> Result<()> {
        let output = self.command(cwd, args).output().await?;
        anyhow::ensure!(
            output.status.success(),
            "reviewloop {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    /// `reviewloop mcp <args>` started from `cwd`, connected to an MCP client.
    async fn connect(&self, cwd: &Path, args: &[&str]) -> Result<Session> {
        let mut command = self.command(cwd, &["mcp"]);
        command.args(args);
        Session::start(command).await
    }

    /// The database the server uses, for seeding and inspection.
    fn db(&self) -> Result<Db> {
        fs::create_dir_all(self.state_dir())?;
        let db = Db::new(&self.state_dir());
        db.ensure_schema()?;
        Ok(db)
    }
}

fn project_toml(project_id: &str, papers: &[&str]) -> String {
    let mut toml = format!("project_id = \"{project_id}\"\n");
    for paper in papers {
        toml.push_str(&format!(
            "\n[[papers]]\nid = \"{paper}\"\npdf_path = \"paper.pdf\"\nbackend = \"stanford\"\n"
        ));
    }
    toml
}

struct Session {
    client: RunningService<RoleClient, ()>,
    tools: BTreeMap<String, Tool>,
}

impl Session {
    /// Start the server `command` runs and connect an MCP client to it.
    async fn start(command: Command) -> Result<Self> {
        let (transport, _) = TokioChildProcess::builder(command)
            .stderr(Stdio::null())
            .spawn()?;
        let client = ().serve(transport).await?;
        let tools = client
            .list_all_tools()
            .await?
            .into_iter()
            .map(|tool| (tool.name.to_string(), tool))
            .collect();
        Ok(Self { client, tools })
    }

    async fn raw_call(&self, name: &str, arguments: Value) -> Result<CallToolResult, ServiceError> {
        let arguments = arguments.as_object().cloned().unwrap_or_default();
        self.client
            .call_tool(CallToolRequestParams::new(name.to_string()).with_arguments(arguments))
            .await
    }

    /// Call a tool that must succeed; returns its validated structured result.
    async fn ok(&self, name: &str, arguments: Value) -> Result<Value> {
        let result = self.raw_call(name, arguments.clone()).await?;
        anyhow::ensure!(
            result.is_error != Some(true),
            "{name}({arguments}) failed: {}",
            text(&result)
        );
        let structured = result
            .structured_content
            .clone()
            .with_context(|| format!("{name} returned no structuredContent"))?;
        // The text block carries the same JSON for clients without
        // structured content.
        let from_text: Value = serde_json::from_str(&text(&result))?;
        assert_eq!(from_text, structured, "{name}: text and structured differ");

        let schema = Value::Object(
            self.tools[name]
                .output_schema
                .as_deref()
                .cloned()
                .with_context(|| format!("{name} has no outputSchema"))?,
        );
        let validator = jsonschema::validator_for(&schema)?;
        let errors: Vec<String> = validator
            .iter_errors(&structured)
            .map(|err| format!("{} at {}", err, err.instance_path()))
            .collect();
        assert!(
            errors.is_empty(),
            "{name} result does not match its outputSchema: {errors:#?}\n{structured:#}"
        );
        Ok(structured)
    }

    /// Call a tool that must fail with a tool error; returns the error JSON.
    async fn err(&self, name: &str, arguments: Value) -> Result<Value> {
        let result = self.raw_call(name, arguments.clone()).await?;
        assert_eq!(
            result.is_error,
            Some(true),
            "{name}({arguments}) should fail: {}",
            text(&result)
        );
        assert!(
            result.structured_content.is_none(),
            "error results carry no structuredContent"
        );
        let error: Value = serde_json::from_str(&text(&result))?;
        for key in ["code", "message", "recovery", "details"] {
            assert!(error.get(key).is_some(), "error lacks {key}: {error}");
        }
        Ok(error)
    }

    async fn close(self) -> Result<()> {
        self.client.cancel().await?;
        Ok(())
    }
}

fn text(result: &CallToolResult) -> String {
    result
        .content
        .iter()
        .filter_map(|block| block.as_text())
        .map(|block| block.text.clone())
        .collect()
}

/// Assert no key `$ref`, `$defs` or `$schema` appears, so clients without
/// reference resolution or a 2020-12 meta-schema accept the schema.
fn assert_self_contained(name: &str, schema: &Value) {
    match schema {
        Value::Object(map) => {
            for key in ["$ref", "$defs", "$schema", "definitions"] {
                assert!(!map.contains_key(key), "{name} schema has {key}");
            }
            map.values()
                .for_each(|value| assert_self_contained(name, value));
        }
        Value::Array(items) => items
            .iter()
            .for_each(|value| assert_self_contained(name, value)),
        _ => {}
    }
}

fn required(schema: &Value) -> BTreeSet<&str> {
    schema["required"]
        .as_array()
        .map(|items| items.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}

#[tokio::test]
async fn lists_the_contract_tools_with_schemas_and_annotations() -> Result<()> {
    let machine = Machine::new()?;
    let repo = machine.repo("thesis", "thesis")?;
    let session = machine.connect(&repo, &[]).await?;

    let info = session.client.peer_info().context("server info")?;
    let implementation = info.server_info.as_ref().context("serverInfo")?;
    assert_eq!(implementation.name, "reviewloop");
    assert_eq!(implementation.version, env!("CARGO_PKG_VERSION"));
    assert!(info.capabilities.tools.is_some());
    let instructions = info.instructions.as_deref().unwrap_or_default();
    assert!(instructions.contains("approve_job"), "{instructions}");

    let names: BTreeSet<&str> = session.tools.keys().map(String::as_str).collect();
    assert_eq!(names, BTreeSet::from(CONTRACT.map(|(name, ..)| name)));

    for (name, tool) in &session.tools {
        let input = Value::Object(tool.input_schema.as_ref().clone());
        let output = Value::Object(
            tool.output_schema
                .as_deref()
                .cloned()
                .expect("outputSchema"),
        );
        assert_eq!(input["type"], "object", "{name}");
        assert_eq!(
            input["additionalProperties"], false,
            "{name} input is closed"
        );
        assert_eq!(output["type"], "object", "{name}");
        assert_self_contained(name, &input);
        assert_self_contained(name, &output);
        assert!(
            tool.description.as_deref().is_some_and(|d| d.len() > 40),
            "{name}"
        );
    }

    // Exactly the documented arguments and hints: only cancelling ends a job;
    // approving and retrying lead to the provider.
    for (name, arguments, (read_only, destructive, idempotent, open_world)) in CONTRACT {
        let tool = &session.tools[name];
        let properties: BTreeSet<&str> = tool
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .map(|properties| properties.keys().map(String::as_str).collect())
            .unwrap_or_default();
        assert_eq!(
            properties,
            BTreeSet::from_iter(arguments.iter().copied()),
            "{name}"
        );
        let annotations = tool.annotations.as_ref().expect("annotations");
        assert_eq!(annotations.read_only_hint, Some(read_only), "{name}");
        assert_eq!(annotations.destructive_hint, Some(destructive), "{name}");
        assert_eq!(annotations.idempotent_hint, Some(idempotent), "{name}");
        assert_eq!(annotations.open_world_hint, Some(open_world), "{name}");
    }

    let request = Value::Object(
        session.tools["request_review"]
            .input_schema
            .as_ref()
            .clone(),
    );
    assert_eq!(required(&request), BTreeSet::from(["paper_id"]));
    assert_eq!(request["properties"]["request_key"]["type"], "string");
    assert_eq!(request["properties"]["force"]["type"], "boolean");
    let get_review = Value::Object(session.tools["get_review"].input_schema.as_ref().clone());
    assert_eq!(
        get_review["properties"]["part"]["type"], "string",
        "{get_review:#}"
    );
    assert_eq!(
        get_review["properties"]["part"]["enum"],
        json!(["summary", "markdown", "section", "raw"])
    );
    // Optional arguments are left out, never null.
    for (name, tool) in &session.tools {
        let properties = tool
            .input_schema
            .get("properties")
            .and_then(Value::as_object);
        for (field, property) in properties.into_iter().flatten() {
            let schema = property.to_string();
            assert!(
                !schema.contains("\"null\""),
                "{name}.{field} admits null: {schema}"
            );
            assert!(property.get("anyOf").is_none(), "{name}.{field}: {schema}");
            let description = property["description"].as_str().unwrap_or_default();
            assert!(!description.is_empty(), "{name}.{field} is undocumented");
            assert!(
                !description.replace("\n\n", "").contains('\n'),
                "{name}.{field} description is hard-wrapped: {description:?}"
            );
        }
    }

    // JobView fields are always present; absent optionals are null.
    let job = Value::Object(
        session.tools["get_job"]
            .output_schema
            .as_deref()
            .cloned()
            .unwrap(),
    );
    for field in [
        "next_poll_at",
        "submit_stage",
        "last_error",
        "snapshot_path",
        "venue",
    ] {
        assert!(
            required(&job).contains(field),
            "{field} not required: {job:#}"
        );
    }
    let statuses: BTreeSet<&str> = job["properties"]["status"]["enum"]
        .as_array()
        .expect("status enum")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert_eq!(
        statuses,
        BTreeSet::from([
            "PENDING_APPROVAL",
            "QUEUED",
            "SUBMITTED",
            "PROCESSING",
            "COMPLETED",
            "FAILED",
            "FAILED_NEEDS_MANUAL",
            "TIMEOUT"
        ])
    );
    session.close().await
}

#[tokio::test]
async fn an_agent_requests_approves_cancels_and_retries_a_review() -> Result<()> {
    let machine = Machine::new()?;
    let repo = machine.repo("thesis", "thesis")?;
    let session = machine.connect(&repo, &[]).await?;

    let papers = session.ok("list_papers", json!({})).await?;
    assert_eq!(papers["project_id"], "thesis");
    assert_eq!(papers["papers"][0]["paper_id"], "main");
    assert_eq!(papers["papers"][0]["pdf_present"], true);

    let request = json!({"paper_id": "main", "request_key": "session-1/main"});
    let created = session.ok("request_review", request.clone()).await?;
    assert_eq!(created["disposition"], "created");
    assert_eq!(created["reason"], Value::Null);
    let job_id = created["job"]["job_id"]
        .as_str()
        .expect("job_id")
        .to_string();
    assert_eq!(created["job"]["status"], "PENDING_APPROVAL");
    assert_eq!(created["job"]["phase"], "awaiting_approval");
    assert_eq!(
        created["input"]["pdf_hash"],
        sha256_file(&repo.join("paper.pdf"))?
    );

    // A retried call with the same key returns the same job.
    let replay = session.ok("request_review", request).await?;
    assert_eq!(replay["disposition"], "existing");
    assert_eq!(replay["reason"], "request_replay");
    assert_eq!(replay["job"]["job_id"], job_id.as_str());
    // So does a keyless request for the same manuscript.
    let covered = session
        .ok("request_review", json!({"paper_id": "main"}))
        .await?;
    assert_eq!(covered["reason"], "covered");
    assert_eq!(covered["job"]["job_id"], job_id.as_str());

    let job = session.ok("get_job", json!({"job_id": job_id})).await?;
    assert_eq!(job["status"], "PENDING_APPROVAL");
    assert_eq!(job["has_token"], false);
    let jobs = session
        .ok("list_jobs", json!({"active_only": true, "limit": 10}))
        .await?;
    assert_eq!(jobs["jobs"].as_array().map(Vec::len), Some(1));
    assert_eq!(jobs["truncated"], false);

    let not_yet = session.err("get_review", json!({"job_id": job_id})).await?;
    assert_eq!(not_yet["code"], "review_not_available");
    assert_eq!(not_yet["details"]["status"], "PENDING_APPROVAL");
    let pending = session.err("retry_job", json!({"job_id": job_id})).await?;
    assert_eq!(pending["code"], "invalid_state");
    assert!(
        pending["recovery"]
            .as_str()
            .unwrap_or_default()
            .contains("approve_job"),
        "{pending}"
    );

    let approved = session
        .ok("approve_job", json!({"paper_id": "main"}))
        .await?;
    assert_eq!(approved["previous_status"], "PENDING_APPROVAL");
    assert_eq!(approved["job"]["status"], "QUEUED");
    assert_eq!(approved["job"]["phase"], "queued");

    // Registered by starting the server, never enabled.
    let worker = session.ok("get_worker_status", json!({})).await?;
    assert_eq!(worker["supervisor"]["state"], "stopped");
    assert_eq!(worker["project"]["availability"], "project_disabled");

    let cancelled = session
        .ok(
            "cancel_job",
            json!({"job_id": job_id, "reason": "wrong draft"}),
        )
        .await?;
    assert_eq!(cancelled["previous_status"], "QUEUED");
    assert_eq!(cancelled["job"]["phase"], "cancelled");
    assert_eq!(cancelled["job"]["terminal"], true);

    // Submitting again sends the manuscript again: back to approval.
    let retried = session
        .ok(
            "retry_job",
            json!({"paper_id": "main", "include_failed": true}),
        )
        .await?;
    assert_eq!(retried["action"], "awaiting_approval");
    assert_eq!(retried["previous_status"], "FAILED");
    assert_eq!(retried["job"]["status"], "PENDING_APPROVAL");
    let approved = session
        .ok("approve_job", json!({"paper_id": "main"}))
        .await?;
    assert_eq!(approved["job"]["status"], "QUEUED");

    let enabled = session.ok("enable_project", json!({})).await?;
    assert_eq!(enabled["project"]["enabled"], true);
    assert_eq!(enabled["changed"], true);
    assert_eq!(
        enabled["project"]["config_path"],
        repo.join("reviewloop.toml")
            .canonicalize()?
            .display()
            .to_string()
    );
    assert_eq!(
        enabled["worker"]["project"]["availability"],
        "supervisor_stopped"
    );
    let disabled = session.ok("disable_project", json!({})).await?;
    assert_eq!(disabled["project"]["enabled"], false);
    let projects = session.ok("list_projects", json!({})).await?;
    assert_eq!(projects["projects"][0]["project_id"], "thesis");
    assert_eq!(projects["projects"][0]["current"], true);
    session.close().await?;

    // The request is recorded as an agent's.
    let events = machine.db()?.list_timeline_events("thesis", "main")?;
    let enqueued: Vec<&Value> = events
        .iter()
        .filter(|event| event.event_type == "job_enqueued")
        .map(|event| &event.payload)
        .collect();
    assert_eq!(enqueued.len(), 1, "{enqueued:?}");
    assert_eq!(enqueued[0]["source"], "agent_request");
    assert_eq!(enqueued[0]["request_key"], "session-1/main");
    Ok(())
}

#[tokio::test]
async fn failures_are_tool_errors_with_codes_and_recovery() -> Result<()> {
    let machine = Machine::new()?;
    let repo = machine.repo("thesis", "thesis")?;
    let session = machine.connect(&repo, &[]).await?;

    let unknown_paper = session
        .err("request_review", json!({"paper_id": "nope"}))
        .await?;
    assert_eq!(unknown_paper["code"], "paper_not_found");
    assert_eq!(unknown_paper["details"]["known"], json!(["main"]));

    for arguments in [
        json!({}),
        json!({"paper_id": "main", "unexpected": 1}),
        json!({"paper_id": 7}),
    ] {
        let invalid = session.err("request_review", arguments).await?;
        assert_eq!(invalid["code"], "invalid_request", "{invalid}");
        assert_eq!(invalid["details"]["field"], "arguments");
    }
    for arguments in [json!({}), json!({"job_id": "x", "paper_id": "main"})] {
        let invalid = session.err("approve_job", arguments).await?;
        assert_eq!(invalid["code"], "invalid_request");
        assert_eq!(invalid["details"]["field"], "job_id");
    }
    let mixed = session
        .err(
            "get_review",
            json!({"job_id": "x", "part": "raw", "section": "summary"}),
        )
        .await?;
    assert_eq!(mixed["details"]["field"], "section");
    let nameless = session
        .err("get_review", json!({"job_id": "x", "part": "section"}))
        .await?;
    assert_eq!(nameless["details"]["field"], "section");

    let missing = session.err("get_job", json!({"job_id": "missing"})).await?;
    assert_eq!(missing["code"], "job_not_found");
    let unregistered = session
        .err("list_papers", json!({"project_id": "ghost"}))
        .await?;
    assert_eq!(unregistered["code"], "project_not_registered");
    let blank = session
        .err("list_papers", json!({"project_id": " "}))
        .await?;
    assert_eq!(blank["details"]["field"], "project_id");

    // An unknown tool is a protocol error, not a tool result.
    match session.raw_call("submit_everything", json!({})).await {
        Err(ServiceError::McpError(error)) => assert_eq!(error.code, ErrorCode::INVALID_PARAMS),
        other => panic!("expected a protocol error, got {other:?}"),
    }

    // The session survives every failure.
    session.ok("list_projects", json!({})).await?;
    session.close().await
}

#[tokio::test]
async fn get_review_returns_each_part_without_the_token() -> Result<()> {
    let machine = Machine::new()?;
    let repo = machine.repo("thesis", "thesis")?;
    let db = machine.db()?;
    let mut config = Config {
        project_id: "thesis".to_string(),
        ..Config::default()
    };
    config.core.state_dir = machine.state_dir().display().to_string();
    let pdf_path = repo.join("paper.pdf");
    let job = db.create_job(&NewJob {
        project_id: "thesis".to_string(),
        paper_id: "main".to_string(),
        backend: "stanford".to_string(),
        pdf: JobPdf::Unpinned {
            pdf_path: pdf_path.display().to_string(),
            pdf_hash: sha256_file(&pdf_path)?,
        },
        status: JobStatus::Queued,
        email: "agent@example.edu".to_string(),
        venue: None,
        review_options: Default::default(),
        git_tag: None,
        git_commit: None,
        next_poll_at: None,
    })?;
    db.attach_token_to_job(&job.id, TOKEN, chrono::Utc::now())?;
    let job = db.get_job(&job.id)?.expect("job");
    let raw = json!({
        "title": "A Paper",
        "numerical_score": 6.5,
        "token": TOKEN,
        "sections": {
            "summary": "Studies loops.",
            "strengths": format!("Clear writing. ref {TOKEN}"),
            "weaknesses": "Small evaluation."
        }
    });
    let (_, markdown, _) = write_review_artifacts(
        &machine.state_dir(),
        &job,
        TOKEN,
        &raw,
        &provider_source(&config, "stanford"),
    )?;
    db.upsert_review(&job.id, TOKEN, &raw.to_string(), &markdown)?;
    db.update_job_state(&job.id, JobStatus::Completed, None, Some(None), Some(None))?;

    let session = machine.connect(&repo, &[]).await?;
    let id = job.id.as_str();
    let full = session.ok("get_review", json!({"job_id": id})).await?;
    assert_eq!(full["score"], "6.5");
    assert_eq!(full["title"], "A Paper");
    assert_eq!(
        full["sections"],
        json!(["summary", "strengths", "weaknesses"])
    );
    assert!(
        full["markdown"]
            .as_str()
            .unwrap_or_default()
            .contains("Studies loops.")
    );
    assert_eq!(full["job"]["phase"], "completed");
    assert_eq!(full["job"]["review_available"], true);

    let summary = session
        .ok("get_review", json!({"job_id": id, "part": "summary"}))
        .await?;
    assert_eq!(summary["markdown"], Value::Null);
    let section = session
        .ok("get_review", json!({"job_id": id, "section": "strengths"}))
        .await?;
    assert_eq!(section["section"]["name"], "strengths");
    assert!(
        section["section"]["text"]
            .as_str()
            .unwrap_or_default()
            .contains("[redacted]")
    );
    let raw = session
        .ok("get_review", json!({"job_id": id, "part": "raw"}))
        .await?;
    assert_eq!(raw["raw"]["token"], "[redacted]");
    let unknown = session
        .err("get_review", json!({"job_id": id, "section": "nope"}))
        .await?;
    assert_eq!(unknown["code"], "section_not_found");

    // Nothing the client received names the token.
    for arguments in [
        json!({"job_id": id}),
        json!({"job_id": id, "part": "raw"}),
        json!({"job_id": id, "section": "strengths"}),
    ] {
        let result = session.raw_call("get_review", arguments).await?;
        assert!(!serde_json::to_string(&result)?.contains(TOKEN));
    }
    // review.json is stored unredacted; it is never listed.
    assert!(full["artifacts"]["review_md"].is_string());
    assert!(!full.to_string().contains("review.json"));
    let job = session.ok("get_job", json!({"job_id": id})).await?;
    assert_eq!(job["has_token"], true);
    assert!(!job.to_string().contains(TOKEN));
    session.close().await
}

#[tokio::test]
async fn an_unscoped_server_acts_on_registered_projects_by_id() -> Result<()> {
    let machine = Machine::new()?;
    let alpha = machine.repo("alpha", "alpha")?;
    let beta = machine.repo("beta", "beta")?;
    machine.cli(&alpha, &["status"]).await?;
    machine.cli(&beta, &["status"]).await?;
    let outside = machine.root.join("elsewhere");
    fs::create_dir_all(&outside)?;
    let session = machine.connect(&outside, &[]).await?;

    let projects = session.ok("list_projects", json!({})).await?;
    let ids: Vec<&str> = projects["projects"]
        .as_array()
        .expect("projects")
        .iter()
        .map(|project| {
            assert_eq!(project["current"], false);
            project["project_id"].as_str().unwrap_or_default()
        })
        .collect();
    assert_eq!(ids, ["alpha", "beta"]);

    let scoped = session.err("list_papers", json!({})).await?;
    assert_eq!(scoped["code"], "project_required");
    let worker = session.ok("get_worker_status", json!({})).await?;
    assert_eq!(worker["project"], Value::Null);

    let created = session
        .ok(
            "request_review",
            json!({"project_id": "beta", "paper_id": "main"}),
        )
        .await?;
    assert_eq!(created["job"]["project_id"], "beta");
    let job_id = created["job"]["job_id"]
        .as_str()
        .expect("job_id")
        .to_string();
    // By id, an unscoped server finds any project's job.
    session.ok("get_job", json!({"job_id": job_id})).await?;
    session.ok("cancel_job", json!({"job_id": job_id})).await?;
    // Cancelled before approval, a retry does not send it either.
    let retried = session.ok("retry_job", json!({"job_id": job_id})).await?;
    assert_eq!(retried["action"], "awaiting_approval");
    assert_eq!(retried["job"]["status"], "PENDING_APPROVAL");
    assert_eq!(retried["job"]["project_id"], "beta");
    let scoped_job = session
        .err("get_job", json!({"job_id": job_id, "project_id": "alpha"}))
        .await?;
    assert_eq!(scoped_job["code"], "job_not_found");

    // Config edits apply to the next call, without a restart.
    fs::write(
        beta.join("reviewloop.toml"),
        project_toml("beta", &["main", "appendix"]),
    )?;
    let papers = session
        .ok("list_papers", json!({"project_id": "beta"}))
        .await?;
    assert_eq!(papers["papers"].as_array().map(Vec::len), Some(2));

    fs::write(alpha.join("reviewloop.toml"), "project_id = [broken\n")?;
    let broken = session
        .err("list_papers", json!({"project_id": "alpha"}))
        .await?;
    assert_eq!(broken["code"], "config_unavailable");
    assert_eq!(broken["details"]["project_id"], "alpha");
    // Disabling by id never needs the project's config.
    let disabled = session
        .ok("disable_project", json!({"project_id": "alpha"}))
        .await?;
    assert_eq!(disabled["project"]["enabled"], false);
    session
        .ok("list_papers", json!({"project_id": "beta"}))
        .await?;
    session.close().await
}

/// Some clients send every optional argument, as `null` when unset.
#[tokio::test]
async fn an_explicit_null_reads_as_an_omitted_argument() -> Result<()> {
    let machine = Machine::new()?;
    let repo = machine.repo("thesis", "thesis")?;
    let session = machine.connect(&repo, &[]).await?;

    let created = session
        .ok(
            "request_review",
            json!({"paper_id": "main", "request_key": null, "force": null, "project_id": null}),
        )
        .await?;
    assert_eq!(created["disposition"], "created");
    let jobs = session
        .ok(
            "list_jobs",
            json!({"paper_id": null, "active_only": null, "limit": null, "project_id": null}),
        )
        .await?;
    assert_eq!(jobs["jobs"].as_array().map(Vec::len), Some(1));
    // Parsed, then refused for the job's state, not the arguments.
    let pending = session
        .err(
            "retry_job",
            json!({"job_id": jobs["jobs"][0]["job_id"], "paper_id": null, "include_failed": null, "force": null}),
        )
        .await?;
    assert_eq!(pending["code"], "invalid_state", "{pending}");
    let enabled = session
        .ok("enable_project", json!({"replace": null}))
        .await?;
    assert_eq!(enabled["project"]["enabled"], true);
    // Wrong types are still refused.
    let invalid = session
        .err(
            "request_review",
            json!({"paper_id": "main", "force": "yes"}),
        )
        .await?;
    assert_eq!(invalid["code"], "invalid_request");
    session.close().await
}

/// A call naming the startup project's id acts on that project only while
/// its config still declares it.
#[tokio::test]
async fn naming_the_startup_project_needs_its_config_to_declare_it() -> Result<()> {
    let machine = Machine::new()?;
    let repo = machine.repo("thesis", "thesis")?;
    let session = machine.connect(&repo, &[]).await?;
    let job = session
        .ok("request_review", json!({"paper_id": "main"}))
        .await?;
    let job_id = job["job"]["job_id"].as_str().expect("job_id").to_string();

    fs::write(
        repo.join("reviewloop.toml"),
        project_toml("renamed", &["main"]),
    )?;
    let renamed = session
        .err(
            "request_review",
            json!({"project_id": "thesis", "paper_id": "main", "force": true}),
        )
        .await?;
    assert_eq!(renamed["code"], "config_unavailable", "{renamed}");
    assert_eq!(renamed["details"]["project_id"], "thesis");

    // Without a config at all, a job id scoped to the project is not looked
    // up across every project.
    fs::remove_file(repo.join("reviewloop.toml"))?;
    let gone = session
        .err(
            "cancel_job",
            json!({"project_id": "thesis", "job_id": job_id}),
        )
        .await?;
    assert_eq!(gone["code"], "config_unavailable", "{gone}");
    session.close().await?;

    let db = machine.db()?;
    assert_eq!(
        db.get_job(&job_id)?.expect("job").status,
        JobStatus::PendingApproval
    );
    let renamed_jobs = db.list_timeline_events("renamed", "main")?;
    assert!(renamed_jobs.is_empty(), "{renamed_jobs:?}");
    Ok(())
}

/// The jobs live in the database, not in the MCP session: a new session
/// (after a disconnect or a host restart) picks them up again.
#[tokio::test]
async fn a_new_session_resumes_the_jobs_of_a_closed_one() -> Result<()> {
    let machine = Machine::new()?;
    let repo = machine.repo("thesis", "thesis")?;
    let request = json!({"paper_id": "main", "request_key": "session-1/main"});

    let first = machine.connect(&repo, &[]).await?;
    let created = first.ok("request_review", request.clone()).await?;
    let job_id = created["job"]["job_id"]
        .as_str()
        .expect("job_id")
        .to_string();
    first.close().await?;

    let second = machine.connect(&repo, &[]).await?;
    let replay = second.ok("request_review", request).await?;
    assert_eq!(replay["disposition"], "existing");
    assert_eq!(replay["job"]["job_id"], job_id.as_str());
    let job = second.ok("get_job", json!({"job_id": job_id})).await?;
    assert_eq!(job["status"], "PENDING_APPROVAL");
    second.ok("approve_job", json!({"job_id": job_id})).await?;
    second.close().await?;

    // Nor does a server killed right after answering a call (the host
    // crashed): the call's effect is already committed.
    let mut child = machine
        .command(&repo, &["mcp"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let mut stdin = child.stdin.take().context("stdin")?;
    let mut stdout = BufReader::new(child.stdout.take().context("stdout")?).lines();
    for request in [
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "crashing-host", "version": "0"}
        }}),
        json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
            "name": "cancel_job", "arguments": {"job_id": job_id, "reason": "host crash test"}
        }}),
    ] {
        stdin.write_all(format!("{request}\n").as_bytes()).await?;
    }
    stdin.flush().await?;
    let answered = async {
        loop {
            let line = stdout.next_line().await?.context("stdout closed early")?;
            let message: Value = serde_json::from_str(&line)?;
            if message["id"] == 2 {
                return anyhow::Ok(message);
            }
        }
    };
    let cancelled = tokio::time::timeout(Duration::from_secs(60), answered).await??;
    assert_eq!(cancelled["result"]["isError"], false, "{cancelled}");
    child.kill().await?;

    let third = machine.connect(&repo, &[]).await?;
    let jobs = third.ok("list_jobs", json!({"paper_id": "main"})).await?;
    assert_eq!(jobs["jobs"].as_array().map(Vec::len), Some(1));
    assert_eq!(jobs["jobs"][0]["job_id"], job_id.as_str());
    assert_eq!(jobs["jobs"][0]["status"], "FAILED");
    assert_eq!(jobs["jobs"][0]["phase"], "cancelled");
    third.close().await
}

/// The whole agent flow against a mock provider: the MCP server only stores
/// and reads jobs; the machine supervisor (here ticked in-process on the same
/// database, with a scripted provider) submits the approved job and collects
/// its review, which the agent then reads over MCP.
#[tokio::test]
async fn a_review_travels_from_request_to_result_through_the_supervisor() -> Result<()> {
    let machine = Machine::new()?;
    let state_dir = machine.state_dir();
    // The server and the supervisor share the database, named explicitly so
    // nothing falls back to a default path outside the test's root. Polls
    // run as soon as a job is submitted, so two ticks finish it.
    let global_toml = format!(
        "[core]\nstate_dir = {state:?}\ndb_path = {db:?}\n\n[polling]\nschedule_minutes = [0]\njitter_percent = 0\n\n[providers.stanford]\nemail = \"agent@example.edu\"\n",
        state = state_dir.display().to_string(),
        db = state_dir.join("reviewloop.db").display().to_string(),
    );
    fs::write(
        machine.root.join("xdg/reviewloop/config.toml"),
        &global_toml,
    )?;
    let repo = machine.repo("thesis", "thesis")?;
    let session = machine.connect(&repo, &[]).await?;

    let load = || {
        let mut global: GlobalConfigFile = toml::from_str(&global_toml)?;
        global.notifications.enabled = false;
        MachineConfig::from_global(global)
    };
    let backends = FleetBackends::new(
        MockBackend::default()
            .on_submit(|| Answer::Now(Ok(receipt(TOKEN))))
            .on_fetch(|| Answer::Now(Ok(ready_review()))),
    );
    let db = machine.db()?;
    let supervisor = Supervisor {
        db: &db,
        backends: &backends,
        load_machine: &load,
        interval: Duration::from_secs(3600),
        panel: false,
    };
    let mut memory = SupervisorMemory::default();
    let mut number = 0;
    let mut tick = async || {
        number += 1;
        supervisor
            .tick_once(4242, number, &mut memory, &AtomicBool::new(false))
            .await
            .map(|_| ())
    };

    let request = json!({"paper_id": "main", "request_key": "session-1/main"});
    let created = session.ok("request_review", request.clone()).await?;
    let job_id = created["job"]["job_id"]
        .as_str()
        .expect("job_id")
        .to_string();
    session.ok("enable_project", json!({})).await?;
    // Enabled, but not approved: nothing is sent.
    tick().await?;
    assert!(backends.calls().is_empty(), "{:?}", backends.calls());
    let job = session.ok("get_job", json!({"job_id": job_id})).await?;
    assert_eq!(job["status"], "PENDING_APPROVAL");

    session.ok("approve_job", json!({"job_id": job_id})).await?;
    tick().await?;
    let job = session.ok("get_job", json!({"job_id": job_id})).await?;
    assert_eq!(job["has_token"], true, "{job:#}");
    assert_eq!(backends.callers("submit"), ["thesis"]);
    if job["status"] != "COMPLETED" {
        assert_eq!(job["status"], "PROCESSING");
        tick().await?;
    }

    let job = session.ok("get_job", json!({"job_id": job_id})).await?;
    assert_eq!(job["status"], "COMPLETED", "{job:#}");
    assert_eq!(job["phase"], "completed");
    assert_eq!(job["review_available"], true);
    assert_eq!(backends.callers("submit"), ["thesis"], "submitted once");
    let review = session.ok("get_review", json!({"job_id": job_id})).await?;
    assert_eq!(review["title"], "Sample Paper");
    assert!(
        review["markdown"]
            .as_str()
            .unwrap_or_default()
            .contains("Solid work"),
        "{review:#}"
    );
    // The provider's token never reaches the agent.
    for (name, arguments) in [
        ("get_job", json!({"job_id": job_id})),
        ("get_review", json!({"job_id": job_id, "part": "raw"})),
        ("list_jobs", json!({})),
    ] {
        let result = session.raw_call(name, arguments).await?;
        assert!(!serde_json::to_string(&result)?.contains(TOKEN), "{name}");
    }

    // Repeating the request returns the finished job instead of a new round.
    let replay = session.ok("request_review", request).await?;
    assert_eq!(replay["disposition"], "existing");
    assert_eq!(replay["job"]["job_id"], job_id.as_str());
    assert_eq!(replay["job"]["status"], "COMPLETED");
    tick().await?;
    assert_eq!(backends.callers("submit"), ["thesis"]);
    assert!(
        state_dir.join("artifacts").join(&job_id).is_dir(),
        "the review is archived"
    );
    session.close().await
}

/// A server started in one project reports another project's job id as not
/// found, for every job-id tool, and leaves the job untouched.
#[tokio::test]
async fn a_scoped_server_never_reaches_another_projects_job() -> Result<()> {
    let machine = Machine::new()?;
    let alpha = machine.repo("alpha", "alpha")?;
    let beta = machine.repo("beta", "beta")?;
    machine.cli(&alpha, &["status"]).await?;
    let in_beta = machine.connect(&beta, &[]).await?;
    let created = in_beta
        .ok("request_review", json!({"paper_id": "main"}))
        .await?;
    let job_id = created["job"]["job_id"]
        .as_str()
        .expect("job_id")
        .to_string();
    in_beta.close().await?;

    let in_alpha = machine.connect(&alpha, &[]).await?;
    for tool in [
        "get_job",
        "get_review",
        "approve_job",
        "cancel_job",
        "retry_job",
    ] {
        let err = in_alpha.err(tool, json!({"job_id": job_id})).await?;
        assert_eq!(err["code"], "job_not_found", "{tool}: {err}");
    }
    let jobs = in_alpha.ok("list_jobs", json!({})).await?;
    assert_eq!(jobs["jobs"], json!([]));
    let papers = in_alpha.ok("list_papers", json!({})).await?;
    assert_eq!(papers["project_id"], "alpha");
    // Enabling another project leaves this server's default where it is.
    let enabled = in_alpha
        .ok("enable_project", json!({"project_id": "beta"}))
        .await?;
    assert_eq!(enabled["project"]["project_id"], "beta");
    assert_eq!(enabled["project"]["current"], false);
    let enabled = in_alpha.ok("enable_project", json!({})).await?;
    assert_eq!(enabled["project"]["current"], true);
    in_alpha.close().await?;
    assert_eq!(
        machine.db()?.get_job(&job_id)?.expect("job").status,
        JobStatus::PendingApproval
    );
    Ok(())
}

/// Calls that name no project act on the startup project's own file only:
/// once it declares another project or is gone, they fail instead of moving
/// to another clone of the project, or to every project. Naming the id may
/// reach the registered clone.
#[tokio::test]
async fn a_server_never_drifts_from_its_startup_project() -> Result<()> {
    let machine = Machine::new()?;
    let beta = machine.repo("beta", "beta")?;
    // Two clones of `thesis`; the registry keeps the one seen first.
    let clone_b = machine.repo("thesis-b", "thesis")?;
    fs::write(
        clone_b.join("reviewloop.toml"),
        project_toml("thesis", &["main", "b_only"]),
    )?;
    machine.cli(&clone_b, &["status"]).await?;
    let clone_a = machine.repo("thesis-a", "thesis")?;
    let in_beta = machine.connect(&beta, &[]).await?;
    let created = in_beta
        .ok("request_review", json!({"paper_id": "main"}))
        .await?;
    let job_id = created["job"]["job_id"]
        .as_str()
        .expect("job_id")
        .to_string();
    in_beta.close().await?;

    let session = machine.connect(&clone_a, &[]).await?;
    let papers = session.ok("list_papers", json!({})).await?;
    assert_eq!(papers["papers"].as_array().map(Vec::len), Some(1));

    fs::write(
        clone_a.join("reviewloop.toml"),
        project_toml("renamed", &["main"]),
    )?;
    for (tool, arguments) in [
        ("list_papers", json!({})),
        ("request_review", json!({"paper_id": "main"})),
        ("enable_project", json!({})),
    ] {
        let renamed = session.err(tool, arguments).await?;
        assert_eq!(renamed["code"], "config_unavailable", "{tool}: {renamed}");
        assert_eq!(renamed["details"]["project_id"], "thesis", "{tool}");
    }
    // Named, the id reaches the registered clone.
    let named = session
        .ok("list_papers", json!({"project_id": "thesis"}))
        .await?;
    assert_eq!(named["papers"][1]["paper_id"], "b_only");

    fs::remove_file(clone_a.join("reviewloop.toml"))?;
    for (tool, arguments) in [
        ("get_job", json!({"job_id": job_id})),
        ("cancel_job", json!({"job_id": job_id})),
        ("list_papers", json!({})),
        ("disable_project", json!({})),
    ] {
        let gone = session.err(tool, arguments).await?;
        assert_eq!(gone["code"], "config_unavailable", "{tool}: {gone}");
        assert_eq!(gone["details"]["project_id"], "thesis", "{tool}");
    }
    let named = session
        .ok("list_papers", json!({"project_id": "thesis"}))
        .await?;
    assert_eq!(named["papers"][1]["paper_id"], "b_only");
    // Other projects stay reachable by id.
    session
        .ok("get_job", json!({"job_id": job_id, "project_id": "beta"}))
        .await?;
    session.close().await?;
    let db = machine.db()?;
    assert_eq!(
        db.get_job(&job_id)?.expect("job").status,
        JobStatus::PendingApproval
    );
    assert!(db.list_timeline_events("thesis", "main")?.is_empty());
    Ok(())
}

/// The server keeps the file it started with: a `reviewloop.toml` that
/// appears closer to its working directory, even for the same project, is
/// not picked up.
#[tokio::test]
async fn a_server_keeps_the_project_file_it_started_with() -> Result<()> {
    let machine = Machine::new()?;
    let repo = machine.repo("thesis", "thesis")?;
    let sub = repo.join("sections");
    fs::create_dir_all(&sub)?;
    let session = machine.connect(&sub, &[]).await?;
    let papers = session.ok("list_papers", json!({})).await?;
    assert_eq!(papers["papers"][0]["paper_id"], "main");

    fs::write(
        sub.join("reviewloop.toml"),
        project_toml("thesis", &["nested"]),
    )?;
    let papers = session.ok("list_papers", json!({})).await?;
    assert_eq!(papers["papers"][0]["paper_id"], "main");
    assert_eq!(papers["papers"].as_array().map(Vec::len), Some(1));
    session.close().await
}

/// `reviewloop --config <file> mcp`, for hosts that start servers in an
/// arbitrary directory: the named project, reloaded every call.
#[tokio::test]
async fn a_server_started_elsewhere_serves_the_project_of_its_config() -> Result<()> {
    let machine = Machine::new()?;
    let repo = machine.repo("thesis", "thesis")?;
    let config = repo.join("reviewloop.toml");
    let outside = machine.root.join("elsewhere");
    fs::create_dir_all(&outside)?;
    let config_arg = config.display().to_string();
    let session =
        Session::start(machine.command(&outside, &["--config", &config_arg, "mcp"])).await?;

    let papers = session.ok("list_papers", json!({})).await?;
    assert_eq!(papers["project_id"], "thesis");
    fs::write(&config, project_toml("thesis", &["main", "second"]))?;
    let papers = session.ok("list_papers", json!({})).await?;
    assert_eq!(papers["papers"][1]["paper_id"], "second");
    let enabled = session.ok("enable_project", json!({})).await?;
    assert_eq!(
        enabled["project"]["config_path"],
        config.canonicalize()?.display().to_string()
    );
    assert_eq!(enabled["project"]["current"], true);
    session.close().await
}

#[tokio::test]
async fn list_jobs_takes_a_whole_limit_within_its_range() -> Result<()> {
    let machine = Machine::new()?;
    let repo = machine.repo("thesis", "thesis")?;
    let session = machine.connect(&repo, &[]).await?;
    session
        .ok("request_review", json!({"paper_id": "main"}))
        .await?;
    // JSON Schema's `integer` admits 10.0; so does the server.
    for limit in [json!(1), json!(10.0), json!(200)] {
        let jobs = session.ok("list_jobs", json!({"limit": limit})).await?;
        assert_eq!(jobs["jobs"].as_array().map(Vec::len), Some(1), "{limit}");
    }
    for limit in [json!(0), json!(201)] {
        let err = session.err("list_jobs", json!({"limit": limit})).await?;
        assert_eq!(err["code"], "invalid_request", "{limit}");
        assert_eq!(err["details"]["field"], "limit", "{limit}");
    }
    for limit in [json!(10.5), json!(-1), json!("10")] {
        let err = session.err("list_jobs", json!({"limit": limit})).await?;
        assert_eq!(err["details"]["field"], "arguments", "{limit}");
    }
    session.close().await
}

/// A host that starts the server and closes stdin before the handshake (a
/// probe, a cancelled start) sees a clean exit.
#[tokio::test]
async fn closing_stdin_before_initializing_exits_cleanly() -> Result<()> {
    let machine = Machine::new()?;
    let repo = machine.repo("thesis", "thesis")?;
    let output = machine
        .command(&repo, &["mcp"])
        .stdin(Stdio::null())
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
    Ok(())
}

#[tokio::test]
async fn approval_granted_queues_requests_at_once() -> Result<()> {
    let machine = Machine::new()?;
    let repo = machine.repo("thesis", "thesis")?;
    let session = machine.connect(&repo, &["--approval", "granted"]).await?;
    let annotations = session.tools["request_review"]
        .annotations
        .clone()
        .expect("annotations");
    assert_eq!(annotations.open_world_hint, Some(true));
    let info = session.client.peer_info().context("server info")?;
    assert!(
        !info
            .instructions
            .as_deref()
            .unwrap_or_default()
            .contains("approve_job")
    );

    let created = session
        .ok("request_review", json!({"paper_id": "main"}))
        .await?;
    assert_eq!(created["job"]["status"], "QUEUED");
    assert_eq!(created["job"]["phase"], "queued");
    session.close().await
}

/// Raw JSON-RPC over the pipes: stdout holds one MCP message per line and
/// nothing else, even with logging configured to stdout at debug level; the
/// log goes to stderr.
#[tokio::test]
async fn stdout_carries_only_newline_delimited_json_rpc() -> Result<()> {
    let machine = Machine::with_global_config("[logging]\nlevel = \"debug\"\noutput = \"stdout\"")?;
    let repo = machine.repo("thesis", "thesis")?;
    let mut child = machine
        .command(&repo, &["mcp"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut stdin = child.stdin.take().context("stdin")?;
    let mut stdout = BufReader::new(child.stdout.take().context("stdout")?).lines();
    let mut stderr = child.stderr.take().context("stderr")?;
    // Drained concurrently: a debug log can outgrow the pipe buffer.
    let log = tokio::spawn(async move {
        let mut log = String::new();
        stderr.read_to_string(&mut log).await.map(|_| log)
    });

    let requests = [
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "raw-test", "version": "0"}
        }}),
        json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
        json!({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
            "name": "request_review", "arguments": {"paper_id": "main"}
        }}),
        json!({"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {
            "name": "get_job", "arguments": {"job_id": "missing"}
        }}),
        json!({"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {
            "name": "no_such_tool", "arguments": {}
        }}),
        // Not `{name, arguments?: object}`: the SDK cannot route it.
        json!({"jsonrpc": "2.0", "id": 6, "method": "tools/call", "params": {
            "name": "get_job", "arguments": "oops"
        }}),
    ];
    for request in &requests {
        stdin.write_all(format!("{request}\n").as_bytes()).await?;
    }
    stdin.flush().await?;

    let mut responses = BTreeMap::new();
    let read_all = async {
        while responses.len() < 6 {
            let line = stdout.next_line().await?.context("stdout closed early")?;
            let message: Value = serde_json::from_str(&line)
                .with_context(|| format!("stdout line is not JSON: {line:?}"))?;
            assert_eq!(message["jsonrpc"], "2.0", "{line}");
            if let Some(id) = message["id"].as_u64() {
                responses.insert(id, message);
            }
        }
        anyhow::Ok(())
    };
    tokio::time::timeout(Duration::from_secs(60), read_all).await??;

    assert_eq!(responses[&1]["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(responses[&1]["result"]["serverInfo"]["name"], "reviewloop");
    assert_eq!(
        responses[&2]["result"]["tools"].as_array().map(Vec::len),
        Some(CONTRACT.len())
    );
    let created = &responses[&3]["result"];
    assert_eq!(created["isError"], false);
    assert_eq!(created["structuredContent"]["disposition"], "created");
    assert_eq!(created["content"][0]["type"], "text");
    let missing = &responses[&4]["result"];
    assert_eq!(missing["isError"], true);
    let error: Value =
        serde_json::from_str(missing["content"][0]["text"].as_str().unwrap_or("{}"))?;
    assert_eq!(error["code"], "job_not_found");
    assert_eq!(responses[&5]["error"]["code"], -32602);
    assert_eq!(responses[&6]["error"]["code"], -32601);

    // Closing stdin ends the session cleanly, with nothing more on stdout.
    drop(stdin);
    let status = tokio::time::timeout(Duration::from_secs(30), child.wait()).await??;
    assert!(status.success(), "exit status {status}");
    while let Some(line) = stdout.next_line().await? {
        let message: Value = serde_json::from_str(&line)
            .with_context(|| format!("stdout line is not JSON: {line:?}"))?;
        assert_eq!(message["jsonrpc"], "2.0");
    }
    let log = log.await??;
    assert!(log.contains("serving MCP on stdio"), "stderr: {log}");
    assert!(log.contains("review request enqueued"), "stderr: {log}");
    Ok(())
}

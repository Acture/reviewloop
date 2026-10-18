//! The example transcripts: every call and result against the live tool
//! schemas, and every expectation against the rows of the skill that the
//! transcript selects and the skill's consent, request-key, pacing and
//! version rules.

use crate::{
    contract::Contract,
    markdown::{Skill, row, spans_of, table},
};
use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Duration, Utc};
use regex::Regex;
use serde_json::{Map, Value};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::LazyLock,
};

/// The OSS-340 situations the examples must cover.
pub const REQUIRED: [&str; 7] = [
    "first-request",
    "duplicate-request",
    "resume-existing-job",
    "pdf-changed",
    "approval-needed",
    "outcome-unknown",
    "review-ready",
];
/// Sections that are one table each: an expectation names a row of them,
/// never the whole table.
const TABLE_SECTIONS: [&str; 3] = ["states", "codes", "worker-availability"];
const LIFECYCLE: &str = "references/lifecycle.md";
const ERRORS: &str = "references/errors.md";

static ASK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bask\b|tell the user|give the user").unwrap());
static TOOL_WORD: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\b[a-z]+_[a-z_]+\b").unwrap());
static ANCHOR_LINK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\]\(#([a-z0-9-]+)\)").unwrap());
static REVIEWLOOP_COMMAND: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"reviewloop [a-z][a-z -]*[a-z-]").unwrap());

#[derive(Clone)]
pub struct Example {
    pub name: String,
    pub path: PathBuf,
    pub value: Value,
}

pub fn examples(skill: &Skill) -> Result<Vec<Example>> {
    skill
        .texts
        .iter()
        .filter(|(path, _)| {
            path.starts_with("examples") && path.extension().is_some_and(|ext| ext == "json")
        })
        .map(|(path, text)| {
            Ok(Example {
                name: path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .context("name")?
                    .to_string(),
                path: path.clone(),
                value: serde_json::from_str(text).with_context(|| path.display().to_string())?,
            })
        })
        .collect()
}

/// The job state of a `JobView`, as references/lifecycle.md names it.
pub fn state_of(job: &Value) -> Result<&'static str> {
    let status = job["status"].as_str().context("job without status")?;
    Ok(match status {
        "PENDING_APPROVAL" => "awaiting-approval",
        "QUEUED" => "queued",
        "PROCESSING" => "processing",
        "COMPLETED" => "completed",
        "FAILED_NEEDS_MANUAL" => "needs-manual",
        "SUBMITTED" if job["submit_stage"] != "UNCERTAIN" => "in-flight",
        "SUBMITTED" if job["has_token"] == true => "receipt-saved",
        "SUBMITTED" => "outcome-unknown",
        "FAILED" if job["phase"] == "cancelled" => "cancelled",
        "FAILED" | "TIMEOUT" => "failed",
        other => bail!("unknown status {other}"),
    })
}

/// Add `additionalProperties: false` to every object schema with
/// `properties` that leaves it open, so a fixture cannot carry a field the
/// tool never returns.
fn strict(schema: &Value) -> Value {
    match schema {
        Value::Object(map) => {
            let mut map: Map<String, Value> = map
                .iter()
                .map(|(key, value)| (key.clone(), strict(value)))
                .collect();
            if map.contains_key("properties") && !map.contains_key("additionalProperties") {
                map.insert("additionalProperties".to_string(), Value::Bool(false));
            }
            Value::Object(map)
        }
        Value::Array(items) => Value::Array(items.iter().map(strict).collect()),
        other => other.clone(),
    }
}

fn validate(schema: &Value, instance: &Value, what: &str) -> Result<()> {
    let validator = jsonschema::validator_for(schema)?;
    let errors: Vec<String> = validator
        .iter_errors(instance)
        .map(|err| format!("{err} at {}", err.instance_path()))
        .collect();
    ensure!(errors.is_empty(), "{what}: {errors:#?}");
    Ok(())
}

/// The job a tool result is about, if any.
fn job_of(result: &Value) -> Option<&Value> {
    if result.get("job_id").is_some() {
        return Some(result);
    }
    if let Some(job) = result.get("job") {
        return Some(job);
    }
    result.get("jobs").and_then(|jobs| jobs.get(0))
}

fn timestamp(value: &Value) -> Result<DateTime<Utc>> {
    let text = value.as_str().context("timestamp")?;
    Ok(DateTime::parse_from_rfc3339(text)?.with_timezone(&Utc))
}

/// Every string value in a document.
fn strings(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(text) => out.push(text.clone()),
        Value::Array(items) => items.iter().for_each(|item| strings(item, out)),
        Value::Object(map) => map.values().for_each(|item| strings(item, out)),
        _ => {}
    }
}

/// Times and counters the server would write: `created_at` ≤ `started_at`
/// ≤ `updated_at`, a completed job polled at least once, a created job
/// stamped at the moment of the request.
fn realistic(job: &Value, now: DateTime<Utc>, created: bool) -> Result<()> {
    let created_at = timestamp(&job["created_at"])?;
    let updated_at = timestamp(&job["updated_at"])?;
    ensure!(created_at <= updated_at, "updated before created");
    if !job["started_at"].is_null() {
        let started_at = timestamp(&job["started_at"])?;
        ensure!(
            created_at <= started_at && started_at <= updated_at,
            "started_at outside created_at..updated_at"
        );
    }
    ensure!(updated_at <= now, "updated after context.now");
    if job["status"] == "COMPLETED" {
        ensure!(
            job["attempt"].as_u64() >= Some(1),
            "a completed job was polled"
        );
    }
    if created {
        ensure!(
            now - created_at <= Duration::minutes(1),
            "a created job is stamped at the request"
        );
    }
    Ok(())
}

/// What the transcript ends in.
struct Ending<'a> {
    /// A lifecycle state, an error code, or `none` before any job.
    state: String,
    job: Option<&'a Value>,
    last: &'a Value,
}

/// Run `example` against the contract and the skill.
pub fn check_example(example: &Example, contract: &Contract, skill: &Skill) -> Result<()> {
    let value = &example.value;
    ensure!(value["scenario"] == example.name.as_str(), "scenario");
    let context = &value["context"];
    let approval = context["server_approval"]
        .as_str()
        .context("server_approval")?;
    let now = timestamp(&context["now"])?;
    let consent = Consent {
        send: context["consent_to_send"]
            .as_bool()
            .context("consent_to_send")?,
        watch: context["consent_to_watch"]
            .as_bool()
            .context("consent_to_watch")?,
        cancel: context["cancel_requested"]
            .as_bool()
            .context("cancel_requested")?,
    };
    let new_round = context["new_round_requested"]
        .as_bool()
        .context("new_round_requested")?;
    let remembered = &context["remembered"];
    let transcript = value["transcript"].as_array().context("transcript")?;
    ensure!(!transcript.is_empty(), "empty transcript");

    let mut grounded = Vec::new();
    strings(remembered, &mut grounded);
    let mut keys: BTreeSet<String> = remembered["request_key"]
        .as_str()
        .map(str::to_string)
        .into_iter()
        .collect();
    let mut job: Option<&Value> = None;
    for (index, entry) in transcript.iter().enumerate() {
        let call = entry["call"].as_str().context("call")?;
        let at = |what: &str| format!("transcript[{index}] {call}: {what}");
        let tool = contract.tool(approval, call)?;
        let arguments = &entry["arguments"];
        validate(
            &Value::Object(tool.input_schema.as_ref().clone()),
            arguments,
            &at("arguments"),
        )?;
        let outcomes = ["result", "error", "lost"]
            .iter()
            .filter(|key| entry.get(**key).is_some())
            .count();
        ensure!(
            outcomes == 1,
            "{}",
            at("exactly one of result, error, lost")
        );
        ensure!(
            consent.allows(contract, approval, call, job)?,
            "{}",
            at("a call that sends, watches or cancels, without consent")
        );

        // Request keys: a new job needs an unused key, a replay or a
        // conflict a used one, and a lost response the same call again.
        let key = arguments["request_key"].as_str();
        let seen = key.is_some_and(|key| keys.contains(key));
        if entry.get("lost").is_some() {
            let next = transcript
                .get(index + 1)
                .context(at("lost, then nothing"))?;
            ensure!(
                next["call"] == call && next["arguments"] == *arguments,
                "{}",
                at("a lost call is repeated with the same arguments")
            );
            if call == "request_review" {
                ensure!(
                    next["result"]["disposition"] == "existing"
                        && next["result"]["reason"] == "request_replay",
                    "{}",
                    at("the repeated request is a replay")
                );
            }
        }
        if let Some(result) = entry.get("result") {
            let output = Value::Object(
                tool.output_schema
                    .as_deref()
                    .cloned()
                    .context("outputSchema")?,
            );
            validate(&strict(&output), result, &at("result"))?;
            strings(result, &mut grounded);
            if call == "request_review" {
                match (&result["disposition"], &result["reason"]) {
                    (Value::String(d), _) if d == "created" => {
                        ensure!(key.is_some() && !seen, "{}", at("created needs a new key"));
                    }
                    (_, Value::String(r)) if r == "request_replay" => {
                        ensure!(seen, "{}", at("a replay reuses a key"));
                    }
                    _ => {}
                }
            }
            if let Some(found) = job_of(result) {
                realistic(found, now, result["disposition"] == "created")
                    .with_context(|| at("job"))?;
                job = Some(found);
            }
        } else if let Some(error) = entry.get("error") {
            check_error(error, contract).with_context(|| at("error"))?;
            if error["code"] == "request_conflict" {
                ensure!(seen, "{}", at("a conflict reuses a key"));
            }
            strings(error, &mut grounded);
        }
        keys.extend(key.map(str::to_string));
    }

    let last = transcript.last().context("transcript")?;
    let ending = Ending {
        state: match (&last["error"]["code"], job) {
            (Value::String(code), _) => code.clone(),
            (_, Some(job)) => state_of(job)?.to_string(),
            _ => "none".to_string(),
        },
        job,
        last,
    };
    let expect = &value["expect"];
    ensure!(
        expect["state"] == ending.state.as_str(),
        "state is {}",
        ending.state
    );
    let guidance = guidance(&ending, expect, skill)?;
    let never = never(expect, &ending, contract, approval, consent, skill)?;
    check_next(&NextContext {
        expect,
        ending: &ending,
        contract,
        approval,
        consent,
        new_round,
        now,
        remembered,
        keys: &keys,
        guidance: &guidance,
        never: &never,
        grounded: &grounded,
    })?;

    for told in expect["tell"]
        .as_array()
        .context("tell")?
        .iter()
        .filter_map(Value::as_str)
    {
        ensure!(
            grounded.iter().any(|text| text.contains(told)) || guidance.contains(told),
            "tell {told:?} is in no result and no rule this transcript selects"
        );
    }
    Ok(())
}

/// A retry of a job the provider holds only polls it: it sends nothing.
fn polls_only(call: &str, job: Option<&Value>) -> bool {
    call == "retry_job" && job.is_some_and(|job| job["has_token"] == true)
}

/// What the user agreed to. Each needs its own yes: consent to send a paper
/// is not consent to watch the project or to cancel a job.
#[derive(Clone, Copy)]
struct Consent {
    send: bool,
    watch: bool,
    cancel: bool,
}

impl Consent {
    /// Whether the user's consent covers calling `call` now, on `job`.
    fn allows(
        self,
        contract: &Contract,
        approval: &str,
        call: &str,
        job: Option<&Value>,
    ) -> Result<bool> {
        Ok(match call {
            "watch_project" => self.watch,
            "cancel_job" => self.cancel,
            _ => !contract.needs_consent(approval, call)? || self.send || polls_only(call, job),
        })
    }
}

/// An error document: the four keys, a documented code, and exactly the
/// detail fields the contract lists for that code.
fn check_error(error: &Value, contract: &Contract) -> Result<()> {
    let keys: BTreeSet<&str> = error
        .as_object()
        .context("error")?
        .keys()
        .map(String::as_str)
        .collect();
    ensure!(
        keys == BTreeSet::from(["code", "message", "recovery", "details"]),
        "error keys {keys:?}"
    );
    let code = error["code"].as_str().context("code")?;
    let fields = contract
        .details
        .get(code)
        .with_context(|| format!("undocumented code {code}"))?;
    let details: BTreeSet<String> = error["details"]
        .as_object()
        .context("details")?
        .keys()
        .cloned()
        .collect();
    ensure!(
        &details == fields,
        "{code} details {details:?}, contract {fields:?}"
    );
    for mismatch in error["details"]["mismatches"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let keys: BTreeSet<&str> = mismatch
            .as_object()
            .context("mismatch")?
            .keys()
            .map(String::as_str)
            .collect();
        ensure!(
            keys == BTreeSet::from(["field", "recorded", "requested"]),
            "mismatch keys {keys:?}"
        );
    }
    Ok(())
}

/// The text of the skill that decides this ending: the state's row, the
/// sections it links, the worker-availability rows `get_worker_status`
/// selected, the retry row `retry_job` selected, and the `rule` section
/// unless that is a whole table.
fn guidance(ending: &Ending, expect: &Value, skill: &Skill) -> Result<String> {
    let mut parts = Vec::new();
    let lifecycle = skill.text(LIFECYCLE)?;
    let states = table(lifecycle, "states")?;
    let codes = table(skill.text(ERRORS)?, "codes")?;
    if ending.state != "none" {
        let found = row(&states, &ending.state)
            .or_else(|| row(&codes, &ending.state))
            .with_context(|| format!("no row for {}", ending.state))?;
        let next_step = found.last().context("row")?;
        parts.push(next_step.clone());
        for anchor in ANCHOR_LINK.captures_iter(next_step) {
            parts.push(skill.section(LIFECYCLE, &anchor[1])?);
        }
    }
    let result = &ending.last["result"];
    match ending.last["call"].as_str() {
        Some("get_worker_status") => {
            let rows = table(lifecycle, "worker-availability")?;
            let availability = result["project"]["availability"]
                .as_str()
                .context("availability")?;
            let mut selected = BTreeSet::from([availability.to_string()]);
            // `availability` names the first blocker only; the skill also
            // reads the supervisor's state.
            match result["supervisor"]["state"].as_str() {
                Some("stopped") => selected.insert("supervisor_stopped".into()),
                Some("paused") => selected.insert("supervisor_paused".into()),
                _ => false,
            };
            let section = skill.section(LIFECYCLE, "worker-availability")?;
            let blocks: Vec<&str> = section.split("\n\n").collect();
            // The hand-off before the table ("Tell the user the fix …").
            parts.push(
                blocks
                    .get(1)
                    .context("worker-availability intro")?
                    .to_string(),
            );
            for key in &selected {
                parts.push(row(&rows, key).context(key.clone())?.join(" | "));
            }
            // What watching sends, only where watching is the fix.
            if selected.contains("project_unwatched") || selected.contains("project_not_registered")
            {
                let watch = blocks
                    .iter()
                    .find(|block| block.starts_with("`watch_project` does"))
                    .context("watching paragraph")?;
                parts.push(watch.to_string());
            }
        }
        Some("retry_job") => {
            let rows = table(lifecycle, "retry")?;
            let action = result["action"].as_str().context("action")?;
            let found = rows
                .iter()
                .find(|row| spans_of(&row[0]).iter().any(|span| span == action))
                .with_context(|| format!("no retry row for {action}"))?;
            parts.push(found.join(" | "));
        }
        _ => {}
    }
    let rule = expect["rule"].as_str().context("rule")?;
    let (file, anchor) = skill.resolve(Path::new("SKILL.md"), rule)?;
    let anchor = anchor.context("rule names a section")?;
    if !TABLE_SECTIONS.contains(&anchor.as_str()) {
        parts.push(skill.section(&file, &anchor)?);
    }
    Ok(parts.join("\n"))
}

/// `never`: real tools, and at least those the rules forbid here: every
/// call the user's consent does not cover (sending, watching, cancelling);
/// for an outcome-unknown or in-flight job `retry_job`; and every tool the
/// provider's review text names.
fn never(
    expect: &Value,
    ending: &Ending,
    contract: &Contract,
    approval: &str,
    consent: Consent,
    skill: &Skill,
) -> Result<BTreeSet<String>> {
    let never: BTreeSet<String> = expect["never"]
        .as_array()
        .context("never")?
        .iter()
        .map(|tool| tool.as_str().map(str::to_string).context("never"))
        .collect::<Result<_>>()?;
    for tool in &never {
        contract.tool(approval, tool)?;
    }
    let mut required = BTreeSet::new();
    for tool in contract.tools[approval].keys() {
        if !consent.allows(contract, approval, tool, ending.job)? {
            required.insert(tool.clone());
        }
    }
    match ending.state.as_str() {
        "outcome-unknown" => required.extend(["retry_job".into(), "cancel_job".into()]),
        "in-flight" => {
            required.insert("retry_job".to_string());
        }
        _ => {}
    }
    let do_not_use = skill.section("references/cli-fallback.md", "do-not-use")?;
    for text in provider_text(ending.last) {
        for word in TOOL_WORD.find_iter(&text) {
            if contract.tools[approval].contains_key(word.as_str()) {
                required.insert(word.as_str().to_string());
            }
        }
        for command in REVIEWLOOP_COMMAND.find_iter(&text) {
            ensure!(
                do_not_use.contains(command.as_str().trim()),
                "the review text names `{}`, which the CLI fallback does not forbid",
                command.as_str()
            );
        }
    }
    let missing: Vec<&String> = required.difference(&never).collect();
    ensure!(missing.is_empty(), "never lacks {missing:?}");
    Ok(never)
}

/// The text of a review the provider wrote: markdown, a section, the title.
pub fn provider_text(entry: &Value) -> Vec<String> {
    if entry["call"] != "get_review" {
        return Vec::new();
    }
    let result = &entry["result"];
    [
        &result["markdown"],
        &result["section"]["text"],
        &result["title"],
    ]
    .into_iter()
    .filter_map(Value::as_str)
    .map(str::to_string)
    .collect()
}

struct NextContext<'a> {
    expect: &'a Value,
    ending: &'a Ending<'a>,
    contract: &'a Contract,
    approval: &'a str,
    consent: Consent,
    new_round: bool,
    now: DateTime<Utc>,
    remembered: &'a Value,
    keys: &'a BTreeSet<String>,
    guidance: &'a str,
    never: &'a BTreeSet<String>,
    grounded: &'a [String],
}

fn check_next(cx: &NextContext) -> Result<()> {
    let next = &cx.expect["next"];
    let state = cx.ending.state.as_str();
    if state == "awaiting-approval" {
        if cx.consent.send {
            ensure!(
                next["call"] == "approve_job",
                "with consent, approve without asking again"
            );
        } else {
            ensure!(next["ask_user"].is_string(), "without consent, ask");
        }
    }
    if state == "outcome-unknown" {
        ensure!(
            next["ask_user"].is_string(),
            "an unknown outcome is the user's to settle"
        );
    }

    if let Some(call) = next["call"].as_str() {
        let tool = cx.contract.tool(cx.approval, call)?;
        let arguments = &next["arguments"];
        validate(
            &Value::Object(tool.input_schema.as_ref().clone()),
            arguments,
            "next arguments",
        )?;
        ensure!(!cx.never.contains(call), "next is in never");
        ensure!(
            cx.guidance.contains(&format!("`{call}`"))
                || cx.guidance.contains(&format!("`{call}(")),
            "the rows this transcript selects do not name {call}"
        );
        ensure!(
            cx.consent
                .allows(cx.contract, cx.approval, call, cx.ending.job)?,
            "{call} without the user's consent to it"
        );
        if let Some(job_id) = arguments["job_id"].as_str() {
            ensure!(
                cx.grounded.iter().any(|text| text == job_id),
                "next names a job id no result gave"
            );
            let job = cx.ending.job.context("job")?;
            if job["job_id"] == job_id {
                match call {
                    "approve_job" => {
                        ensure!(job["status"] == "PENDING_APPROVAL", "approve a pending job")
                    }
                    "get_review" => {
                        ensure!(job["review_available"] == true, "read a stored review")
                    }
                    "retry_job" => ensure!(
                        !(job["status"] == "SUBMITTED" && job["has_token"] != true),
                        "retry of a SUBMITTED job without a receipt"
                    ),
                    _ => {}
                }
            }
        }
        if call == "request_review" {
            let key = arguments["request_key"]
                .as_str()
                .context("request_review without request_key")?;
            let force = arguments["force"].as_bool().unwrap_or(false);
            ensure!(
                !force || cx.new_round,
                "force without an explicit new round"
            );
            if force || state == "request_conflict" {
                ensure!(
                    !cx.keys.contains(key),
                    "a new round or version reuses a request key"
                );
            }
        }
    } else if let Some(question) = next["ask_user"].as_str() {
        ensure!(!question.is_empty(), "empty ask_user");
        ensure!(
            ASK.is_match(cx.guidance),
            "the rows do not hand this to the user"
        );
        for span in spans_of(question) {
            ensure!(
                cx.guidance.contains(&span),
                "ask_user names `{span}`, which the selected rows do not"
            );
        }
    } else if next["end_turn"] == true {
        let job = cx.ending.job.context("end_turn without a job")?;
        let pace = format!("{} seconds", crate::MIN_CHECK_INTERVAL.num_seconds());
        ensure!(cx.guidance.contains("Wait"), "the row does not say to wait");
        let earliest = match job["next_poll_at"].as_str() {
            Some(_) if timestamp(&job["next_poll_at"])? > cx.now => {
                timestamp(&job["next_poll_at"])?
            }
            _ => {
                ensure!(
                    cx.guidance.contains(&pace),
                    "the row does not state the {pace} pace"
                );
                cx.now + crate::MIN_CHECK_INTERVAL
            }
        };
        ensure!(
            timestamp(&next["check_again_at"])? == earliest,
            "check_again_at should be {earliest}"
        );
    } else if next["report"] == true {
        // A report follows a full read of the review, and says which
        // version it covers when that is not the remembered one.
        let last = cx.ending.last;
        ensure!(
            last["call"] == "get_review",
            "report before reading the review"
        );
        let review = &last["result"];
        ensure!(
            review["markdown"].is_string() || review["section"].is_object(),
            "report from a summary only"
        );
        let hash = review["job"]["pdf_hash"].as_str().context("pdf_hash")?;
        if cx.remembered["pdf_hash"].as_str() != Some(hash) {
            let tell: Vec<&str> = cx.expect["tell"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect();
            ensure!(
                tell.contains(&hash),
                "an unconfirmed version is reported by its pdf_hash"
            );
        }
    } else {
        bail!("next is none of call, ask_user, end_turn, report");
    }
    Ok(())
}

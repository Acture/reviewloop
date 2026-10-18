//! OSS-340: the shared agent skill (`integrations/reviewloop/skills/reviewloop`)
//! against what it builds on.
//!
//! - The frontmatter is what the Agent Skills spec and Codex's skill
//!   validator accept; links and anchors resolve inside the skill, and every
//!   shipped file is reachable from SKILL.md.
//! - Tools, arguments, fields (as full dotted paths), statuses, error codes
//!   and their detail fields resolve in the live `reviewloop mcp` schemas,
//!   the CLI's JSON or docs/review-operations.md; CLI-only keys stay in the
//!   CLI fallback.
//! - The job-state tables cover every job, availability and retry action the
//!   schemas allow, each exactly once.
//! - Every CLI command and flag the skill or an example names exists in
//!   `--help`, and its `--json` commands print nothing but JSON.
//! - Every example transcript validates against the live tool schemas; its
//!   expected next step is named by the rows of the skill the transcript
//!   selects and obeys the consent, request-key, pacing and version rules.
//! - Mutations of each kind of rule are rejected.
//!
//! No agent runs here: these checks cover the skill text and its examples,
//! not how a host follows them.

mod cli;
mod contract;
mod examples;
mod markdown;
mod names;
mod structure;

use anyhow::Result;
use chrono::Duration;
use contract::{Contract, Machine, repo};
use examples::{Example, REQUIRED, check_example, examples, provider_text};
use markdown::Skill;
use serde_json::{Value, json};
use std::path::PathBuf;

const SKILL: &str = "integrations/reviewloop/skills/reviewloop";

/// The supervisor's tick: the shortest interval between checks of a job
/// whose `next_poll_at` is not in the future.
pub const MIN_CHECK_INTERVAL: Duration =
    Duration::seconds(reviewloop::supervisor::TICK_INTERVAL.as_secs() as i64);

fn skill() -> Result<Skill> {
    Skill::load(&repo().join(SKILL))
}

fn directory() -> String {
    PathBuf::from(SKILL)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string()
}

#[test]
fn frontmatter_is_valid_for_agent_skill_hosts() -> Result<()> {
    structure::check_frontmatter(&skill()?, &directory())
}

#[test]
fn links_resolve_and_every_file_is_reachable() -> Result<()> {
    structure::check_links(&skill()?)
}

#[tokio::test]
async fn names_match_the_live_contract() -> Result<()> {
    let contract = Contract::load(&Machine::new()?).await?;
    names::check_names(&skill()?, &contract)
}

#[tokio::test]
async fn lifecycle_tables_cover_every_state() -> Result<()> {
    let contract = Contract::load(&Machine::new()?).await?;
    names::check_lifecycle(&skill()?, &contract)
}

#[tokio::test]
async fn cli_commands_and_flags_exist() -> Result<()> {
    cli::check_cli(&skill()?, &Machine::new()?).await
}

#[tokio::test]
async fn examples_follow_the_contract_and_the_rules() -> Result<()> {
    let contract = Contract::load(&Machine::new()?).await?;
    let skill = skill()?;
    let examples = examples(&skill)?;
    for required in REQUIRED {
        assert!(
            examples.iter().any(|example| example.name == required),
            "no example {required}"
        );
    }
    for example in &examples {
        check_example(example, &contract, &skill)
            .map_err(|err| err.context(example.path.display().to_string()))?;
    }
    // The prompt-injection case is covered: some review text names a tool.
    assert!(
        examples.iter().any(|example| {
            example.value["transcript"]
                .as_array()
                .into_iter()
                .flatten()
                .flat_map(provider_text)
                .any(|text| text.contains("approve_job"))
        }),
        "no example quotes an instruction inside a review"
    );
    Ok(())
}

// ---- the checks reject what they exist to catch ----

fn example(examples: &[Example], name: &str) -> Example {
    examples
        .iter()
        .find(|example| example.name == name)
        .unwrap_or_else(|| panic!("no example {name}"))
        .clone()
}

fn edit(mut example: Example, change: impl FnOnce(&mut Value)) -> Example {
    change(&mut example.value);
    example
}

fn with_text(skill: &Skill, file: &str, change: impl FnOnce(&str) -> String) -> Skill {
    let mut skill = skill.clone();
    let path = PathBuf::from(file);
    let text = change(&skill.texts[&path]);
    skill.texts.insert(path, text);
    skill
}

#[tokio::test]
async fn mutations_are_rejected() -> Result<()> {
    let machine = Machine::new()?;
    let contract = Contract::load(&machine).await?;
    let skill = skill()?;
    let all = examples(&skill)?;
    let job = "5b0b8f1e-3c55-4b3f-9a52-1d3f2b7c9e10";

    let broken_examples = [
        (
            "asking again with consent",
            edit(example(&all, "first-request"), |v| {
                v["expect"]["next"] = json!({"ask_user": "Send it?"});
            }),
        ),
        (
            "a read where the rules approve",
            edit(example(&all, "first-request"), |v| {
                v["expect"]["next"] = json!({"call": "get_job", "arguments": {"job_id": job}});
            }),
        ),
        (
            "a call the selected row does not name",
            edit(example(&all, "covered-request"), |v| {
                v["expect"]["next"] = json!({"call": "get_worker_status", "arguments": {}});
            }),
        ),
        (
            "a fix for another availability",
            edit(example(&all, "worker-not-ready"), |v| {
                v["transcript"][1]["result"]["project"]["availability"] =
                    json!("supervisor_paused");
                v["transcript"][1]["result"]["supervisor"]["state"] = json!("paused");
            }),
        ),
        (
            "a new key after a lost response",
            edit(example(&all, "duplicate-request"), |v| {
                v["transcript"][2]["arguments"]["request_key"] = json!("conv-7f3a/main-2");
            }),
        ),
        (
            "a retry of an unknown outcome",
            edit(example(&all, "outcome-unknown"), |v| {
                v["expect"]["next"] = json!({"call": "retry_job", "arguments": {"job_id": job}});
            }),
        ),
        (
            "a sending call left out of never without consent",
            edit(example(&all, "approval-needed"), |v| {
                v["expect"]["never"] = json!(["approve_job", "request_review"]);
            }),
        ),
        (
            "a tool the review text names left out of never",
            // With consent to send, only the review text can require approve_job.
            edit(example(&all, "review-ready"), |v| {
                v["context"]["consent_to_send"] = json!(true);
                v["expect"]["never"] =
                    json!(["retry_job", "request_review", "cancel_job", "watch_project"]);
            }),
        ),
        (
            "a check before next_poll_at",
            edit(example(&all, "resume-existing-job"), |v| {
                v["expect"]["next"]["check_again_at"] = json!("2026-10-18T12:05:00Z");
            }),
        ),
        (
            "the conflicting key again",
            edit(example(&all, "pdf-changed"), |v| {
                v["expect"]["next"]["arguments"]["request_key"] = json!("conv-7f3a/main");
            }),
        ),
        (
            "force without an explicit new round",
            edit(example(&all, "new-round"), |v| {
                v["context"]["new_round_requested"] = json!(false);
            }),
        ),
        (
            "a request that sends without consent",
            edit(example(&all, "granted-no-consent"), |v| {
                v["expect"]["next"] = json!({"call": "request_review", "arguments": {"paper_id": "main", "request_key": "conv-1/main"}});
            }),
        ),
        (
            "a retry that resends without consent",
            // Still TIMEOUT (state failed), so only the consent rule applies.
            edit(example(&all, "failed-retry"), |v| {
                v["transcript"][0]["result"]["has_token"] = json!(false);
                v["context"]["consent_to_send"] = json!(false);
                v["expect"]["never"] = json!([
                    "request_review",
                    "approve_job",
                    "cancel_job",
                    "watch_project"
                ]);
            }),
        ),
        (
            "watching where the supervisor is the fix",
            edit(example(&all, "submit-stalled"), |v| {
                v["expect"]["next"] = json!({"ask_user": "Run `reviewloop project watch`."});
                v["expect"]["tell"] = json!(["reviewloop project watch"]);
            }),
        ),
        (
            "watching without consent to watch",
            edit(example(&all, "worker-not-ready"), |v| {
                v["expect"]["next"] = json!({"call": "watch_project", "arguments": {}});
                v["expect"]["never"] =
                    json!(["retry_job", "request_review", "approve_job", "cancel_job"]);
            }),
        ),
        (
            "cancelling without being asked",
            edit(example(&all, "duplicate-request"), |v| {
                v["expect"]["next"] = json!({"call": "cancel_job", "arguments": {"job_id": job}});
                v["expect"]["never"] = json!([
                    "approve_job",
                    "request_review",
                    "retry_job",
                    "watch_project"
                ]);
            }),
        ),
        (
            "another version reported as the requested one",
            edit(example(&all, "review-ready"), |v| {
                v["context"]["remembered"]["pdf_hash"] = json!("0".repeat(64));
            }),
        ),
        (
            "a field the tool never returns",
            edit(example(&all, "failed-retry"), |v| {
                v["transcript"][0]["result"]["token"] = json!("tok");
            }),
        ),
        (
            "a detail field the code does not have",
            edit(example(&all, "pdf-changed"), |v| {
                v["transcript"][0]["error"]["details"]["job_id"] = json!(job);
            }),
        ),
        (
            "a created job stamped hours ago",
            edit(example(&all, "first-request"), |v| {
                v["transcript"][2]["result"]["job"]["created_at"] =
                    json!("2026-10-18T09:00:00.000000Z");
            }),
        ),
    ];
    for (what, broken) in broken_examples {
        let err = check_example(&broken, &contract, &skill).expect_err(what);
        eprintln!("{what}: {err:#}");
    }

    let lifecycle = "references/lifecycle.md";
    let broken_skills = [
        (
            "receipt rows swapped",
            with_text(&skill, lifecycle, |text| {
                text.replace("`has_token` false |", "`has_token` TMP |")
                    .replace("`has_token` true |", "`has_token` false |")
                    .replace("`has_token` TMP |", "`has_token` true |")
            }),
        ),
        (
            "in-flight without its negation",
            with_text(&skill, lifecycle, |text| {
                text.replace(
                    "`submit_stage` not `UNCERTAIN`",
                    "`submit_stage` `UNCERTAIN`",
                )
            }),
        ),
        (
            "a made-up field",
            with_text(&skill, "SKILL.md", |text| {
                format!("{text}\nCheck `next_check_at`.\n")
            }),
        ),
        (
            "a made-up path",
            with_text(&skill, "SKILL.md", |text| {
                format!("{text}\nReport `job.notices`.\n")
            }),
        ),
        (
            "a made-up detail",
            with_text(&skill, "SKILL.md", |text| {
                format!("{text}\nRead `details.pdf_hash`.\n")
            }),
        ),
        (
            "a CLI key in the tool steps",
            with_text(&skill, "SKILL.md", |text| {
                format!("{text}\nRemember the `token_masked`.\n")
            }),
        ),
        (
            "a made-up argument",
            with_text(&skill, "SKILL.md", |text| {
                text.replace("`get_review(job_id, part)`", "`get_review(job_id, format)`")
            }),
        ),
    ];
    for (what, broken) in broken_skills {
        let err = names::check_lifecycle(&broken, &contract)
            .and_then(|()| names::check_names(&broken, &contract))
            .expect_err(what);
        eprintln!("{what}: {err:#}");
    }

    let broken_shapes = [
        (
            "a name without the separator space",
            with_text(&skill, "SKILL.md", |text| {
                text.replacen("name: reviewloop", "name:reviewloop", 1)
            }),
        ),
        (
            "a description that is a YAML comment",
            with_text(&skill, "SKILL.md", |text| {
                text.replacen("description: Request", "description: #Request", 1)
            }),
        ),
        (
            "a broken link",
            with_text(&skill, "SKILL.md", |text| {
                text.replace("(references/errors.md)", "(references/error-codes.md)")
            }),
        ),
        (
            "a titled link",
            with_text(&skill, "SKILL.md", |text| {
                text.replace("(references/errors.md)", "(references/errors.md \"codes\")")
            }),
        ),
        (
            "a duplicate heading",
            with_text(&skill, lifecycle, |text| format!("{text}\n## Retry\n")),
        ),
    ];
    for (what, broken) in broken_shapes {
        let err = structure::check_frontmatter(&broken, &directory())
            .and_then(|()| structure::check_links(&broken))
            .expect_err(what);
        eprintln!("{what}: {err:#}");
    }
    let mut orphan = skill.clone();
    orphan.files.insert(PathBuf::from("references/orphan.md"));
    orphan
        .texts
        .insert(PathBuf::from("references/orphan.md"), "# Orphan\n".into());
    assert!(
        structure::check_links(&orphan).is_err(),
        "an orphan file was accepted"
    );

    for (what, line) in [
        (
            "a flag the command lacks",
            "`reviewloop submit --paper-id <paper_id> --json`",
        ),
        (
            "a command that does not exist",
            "`reviewloop review show --job-id <job_id>`",
        ),
    ] {
        let broken = with_text(&skill, "references/cli-fallback.md", |text| {
            format!("{text}\n{line}\n")
        });
        let err = cli::check_cli(&broken, &machine).await.expect_err(what);
        eprintln!("{what}: {err:#}");
    }
    Ok(())
}

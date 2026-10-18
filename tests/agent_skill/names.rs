//! Every name the skill uses exists in the contract, and the tables of
//! references/lifecycle.md cover every job, availability and retry action
//! the server can report, each exactly once.

use crate::{
    MIN_CHECK_INTERVAL,
    cli::cli_command,
    contract::{Contract, DOTTED, SNAKE, UPPER},
    examples::{examples, state_of},
    markdown::{CODE_SPAN, Skill, code_spans, row, spans_of, table},
};
use anyhow::{Context, Result, bail, ensure};
use regex::Regex;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::LazyLock,
};

static CALL: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^([a-z_]+)\(([a-z_, ]*)\)$").unwrap());
const FILE_EXTENSIONS: [&str; 4] = ["md", "json", "toml", "pdf"];

/// The keys an example uses outside tool arguments, results and errors,
/// and its approval modes: the vocabulary of examples/README.md.
fn fixture_words(value: &Value, words: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                words.insert(key.clone());
                match key.as_str() {
                    "arguments" | "result" | "error" => {}
                    "server_approval" => words.extend(value.as_str().map(str::to_string)),
                    _ => fixture_words(value, words),
                }
            }
        }
        Value::Array(items) => items.iter().for_each(|item| fixture_words(item, words)),
        _ => {}
    }
}

pub fn check_names(skill: &Skill, contract: &Contract) -> Result<()> {
    let tools: BTreeSet<&String> = contract.tools["required"].keys().collect();
    let skill_spans: BTreeSet<String> = code_spans(skill.text("SKILL.md")?).into_iter().collect();
    for tool in &tools {
        ensure!(
            skill_spans.contains(tool.as_str()),
            "SKILL.md never names tool {tool}"
        );
    }

    let mut fixture = BTreeSet::from(["none".to_string()]);
    for example in examples(skill)? {
        fixture_words(&example.value, &mut fixture);
    }
    let states: BTreeSet<String> = table(skill.text("references/lifecycle.md")?, "states")?
        .iter()
        .map(|row| row[0].trim_matches('`').to_string())
        .collect();
    let detail_fields: BTreeSet<&String> = contract.details.values().flatten().collect();

    let mut unknown = Vec::new();
    for (file, text) in skill.markdown() {
        for span in code_spans(text) {
            let at = format!("{}: `{span}`", file.display());
            if cli_command(&span).is_some()
                || states.contains(&span)
                || (file.starts_with("examples") && fixture.contains(&span))
            {
                continue;
            }
            if let Some(call) = CALL.captures(&span) {
                let arguments = contract.arguments(&call[1]).with_context(|| at.clone())?;
                for argument in call[2].split(',').map(str::trim).filter(|a| !a.is_empty()) {
                    if !arguments.contains(argument) {
                        unknown.push(format!("{at}: {} has no argument {argument}", &call[1]));
                    }
                }
            } else if let Some(prefixed) = span.strip_prefix("mcp__") {
                let tool = prefixed.rsplit("__").next().unwrap_or(prefixed);
                if !tools.contains(&tool.to_string()) {
                    unknown.push(format!("{at}: no tool {tool}"));
                }
            } else if SNAKE.is_match(&span) {
                if !contract.words.contains(&span) {
                    unknown.push(at);
                } else if contract.cli_only.contains(&span)
                    && file != Path::new("references/cli-fallback.md")
                {
                    unknown.push(format!("{at}: a CLI JSON key outside the CLI fallback"));
                }
            } else if UPPER.is_match(&span) {
                if !contract.upper.contains(&span) {
                    unknown.push(at);
                }
            } else if DOTTED.is_match(&span) {
                let file_name = FILE_EXTENSIONS
                    .iter()
                    .any(|ext| span.ends_with(&format!(".{ext}")));
                let known = match span.strip_prefix("details.") {
                    Some(field) => detail_fields.contains(&field.to_string()),
                    None => contract.knows_path(&span),
                };
                if !file_name && !known {
                    unknown.push(at);
                }
            }
        }
    }
    ensure!(
        unknown.is_empty(),
        "names outside the contract:\n{}",
        unknown.join("\n")
    );

    // The error table covers exactly the contract's codes, and each row
    // names only its own code's detail fields.
    let rows = table(skill.text("references/errors.md")?, "codes")?;
    let codes: BTreeSet<String> = rows
        .iter()
        .map(|row| row[0].trim_matches('`').to_string())
        .collect();
    ensure!(
        codes == contract.error_codes,
        "references/errors.md codes {codes:?}"
    );
    for row in &rows {
        let code = row[0].trim_matches('`');
        for span in row.iter().flat_map(|cell| spans_of(cell)) {
            if let Some(field) = span.strip_prefix("details.") {
                ensure!(
                    contract.details[code].contains(field),
                    "the {code} row names `{span}`, which {code} has no detail for"
                );
            }
        }
    }
    Ok(())
}

/// One condition of a `#states` Job cell: `` `field` `A` or `B` ``,
/// `` `field` not `A` `` or `` `field` true ``.
struct Clause {
    field: String,
    negated: bool,
    expected: Expected,
}

enum Expected {
    Bool(bool),
    OneOf(BTreeSet<String>),
}

fn clauses(cell: &str) -> Result<Vec<Clause>> {
    cell.split(", ")
        .map(|clause| {
            let spans = spans_of(clause);
            let (field, values) = spans
                .split_first()
                .with_context(|| format!("clause {clause:?}"))?;
            let words: Vec<String> = CODE_SPAN
                .replace_all(clause, " ")
                .split_whitespace()
                .map(str::to_string)
                .collect();
            let negated = words.iter().any(|word| word == "not");
            let expected = match words
                .iter()
                .find(|word| *word == "true" || *word == "false")
            {
                Some(word) => Expected::Bool(word == "true"),
                None if !values.is_empty() => Expected::OneOf(values.iter().cloned().collect()),
                None => bail!("clause {clause:?} names no value"),
            };
            Ok(Clause {
                field: field.clone(),
                negated,
                expected,
            })
        })
        .collect()
}

fn holds(clause: &Clause, job: &Value) -> bool {
    let matched = match &clause.expected {
        Expected::Bool(value) => job[&clause.field] == *value,
        Expected::OneOf(values) => job[&clause.field]
            .as_str()
            .is_some_and(|value| values.contains(value)),
    };
    matched != clause.negated
}

/// The phases the server derives from a status.
fn phases(status: &str) -> &'static [&'static str] {
    match status {
        "PENDING_APPROVAL" => &["awaiting_approval"],
        "QUEUED" => &["queued"],
        "SUBMITTED" | "PROCESSING" => &["submitted"],
        "COMPLETED" => &["completed"],
        "FAILED" => &["failed", "cancelled"],
        _ => &["failed"],
    }
}

pub fn check_lifecycle(skill: &Skill, contract: &Contract) -> Result<()> {
    let lifecycle = skill.text("references/lifecycle.md")?;
    let rows = table(lifecycle, "states")?;
    let parsed: BTreeMap<String, Vec<Clause>> = rows
        .iter()
        .map(|row| Ok((row[0].trim_matches('`').to_string(), clauses(&row[1])?)))
        .collect::<Result<_>>()?;

    // Every job the schema allows matches exactly one row, the row of its state.
    let phase_values = contract.values("get_job", "/properties/phase")?;
    let mut stages: Vec<Value> = contract
        .values("get_job", "/properties/submit_stage")?
        .into_iter()
        .map(Value::from)
        .collect();
    stages.push(Value::Null);
    let mut reached = BTreeSet::new();
    for status in contract.values("get_job", "/properties/status")? {
        for phase in phases(&status) {
            ensure!(
                phase_values.contains(*phase),
                "phase {phase} is not in the schema"
            );
            for stage in &stages {
                for token in [false, true] {
                    let job = json!({"status": status, "phase": phase, "submit_stage": stage, "has_token": token});
                    let matches: Vec<&String> = parsed
                        .iter()
                        .filter(|(_, clauses)| clauses.iter().all(|clause| holds(clause, &job)))
                        .map(|(key, _)| key)
                        .collect();
                    let state = state_of(&job)?;
                    ensure!(
                        matches == [state],
                        "{job} matches rows {matches:?}, its state is {state}"
                    );
                    reached.insert(state.to_string());
                }
            }
        }
    }
    let keys: BTreeSet<String> = parsed.keys().cloned().collect();
    ensure!(
        keys == reached,
        "#states rows {keys:?}, reachable states {reached:?}"
    );

    let availability: BTreeSet<String> = table(lifecycle, "worker-availability")?
        .iter()
        .map(|row| row[0].trim_matches('`').to_string())
        .collect();
    ensure!(
        availability
            == contract.values(
                "get_worker_status",
                "/properties/project/properties/availability"
            )?,
        "#worker-availability rows {availability:?}"
    );
    let actions: BTreeSet<String> = table(lifecycle, "retry")?
        .iter()
        .flat_map(|row| spans_of(&row[0]))
        .collect();
    ensure!(
        actions == contract.values("retry_job", "/properties/action")?,
        "#retry rows {actions:?}"
    );

    // The pace the examples assume is the one the skill states.
    let pace = format!("{} seconds", MIN_CHECK_INTERVAL.num_seconds());
    let queued = row(&rows, "queued").context("queued row")?;
    ensure!(
        queued.last().is_some_and(|cell| cell.contains(&pace)),
        "the queued row states no {pace}"
    );
    ensure!(
        skill
            .section("SKILL.md", "5-follow-the-job")?
            .contains(&format!("at least {pace}")),
        "SKILL.md step 5 states no {pace} pace"
    );
    Ok(())
}

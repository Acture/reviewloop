//! Every `reviewloop …` command the skill names exists with its flags, and
//! its `--json` commands print nothing but JSON.

use crate::{
    contract::{Machine, cli_json},
    examples::examples,
    markdown::{Skill, code_spans, spans_of},
};
use anyhow::{Context, Result, ensure};
use regex::Regex;
use serde_json::Value;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::LazyLock,
};

/// An option line of clap's help: `  -x, --flag` or `      --flag`.
static HELP_FLAG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?m)^  (?:-[A-Za-z], |    )(--[a-z][a-z0-9-]*)").unwrap());
static HELP_ALIASES: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[aliases: ([^\]]+)\]").unwrap());
static SUBCOMMAND: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[a-z][a-z-]*$").unwrap());
static LONE_FLAG: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(--[a-z][a-z0-9-]*)$").unwrap());

pub struct CliCommand {
    /// Subcommands, such as `["daemon", "status"]`.
    pub path: Vec<String>,
    /// Flags given before the subcommands (`--config`).
    pub global_flags: Vec<String>,
    pub flags: Vec<String>,
    pub text: String,
}

/// Parse a `reviewloop …` span; `None` for anything else.
pub fn cli_command(span: &str) -> Option<CliCommand> {
    let mut tokens = span.split_whitespace().peekable();
    if tokens.next() != Some("reviewloop") {
        return None;
    }
    let mut command = CliCommand {
        path: Vec::new(),
        global_flags: Vec::new(),
        flags: Vec::new(),
        text: span.to_string(),
    };
    while let Some(token) = tokens.next() {
        let token = token.trim_end_matches([']', ')', ',', '|']);
        if let Some(name) = token.strip_prefix("--") {
            let flag = format!("--{}", name.split('=').next().unwrap_or(name));
            if command.path.is_empty() {
                if flag == "--config" {
                    tokens.next();
                }
                command.global_flags.push(flag);
            } else {
                command.flags.push(flag);
            }
        } else if command.flags.is_empty() && SUBCOMMAND.is_match(token) {
            command.path.push(token.to_string());
        }
    }
    Some(command)
}

/// The flags `reviewloop <path> --help` lists under Options, aliases
/// included; an error when there is no such command.
async fn defined_flags(machine: &Machine, path: &[String]) -> Result<BTreeSet<String>> {
    let mut args: Vec<&str> = path.iter().map(String::as_str).collect();
    args.push("--help");
    let output = machine.run(&machine.repo(), &args).await?;
    ensure!(
        output.status.success(),
        "reviewloop {} --help failed: no such command",
        path.join(" ")
    );
    let help = String::from_utf8(output.stdout)?;
    let (_, options) = help
        .split_once("\nOptions:\n")
        .with_context(|| format!("reviewloop {} --help has no Options", path.join(" ")))?;
    let mut flags: BTreeSet<String> = HELP_FLAG
        .captures_iter(options)
        .map(|c| c[1].to_string())
        .collect();
    for aliases in HELP_ALIASES.captures_iter(options) {
        flags.extend(aliases[1].split(',').map(|alias| alias.trim().to_string()));
    }
    Ok(flags)
}

/// The spans to check, with where they come from: the Markdown files, and
/// the strings an example expects the answer to carry.
fn spans(skill: &Skill) -> Result<Vec<(PathBuf, String)>> {
    let mut spans: Vec<(PathBuf, String)> = skill
        .markdown()
        .flat_map(|(path, text)| {
            code_spans(text)
                .into_iter()
                .map(move |span| (path.clone(), span))
        })
        .collect();
    for example in examples(skill)? {
        let expect = &example.value["expect"];
        let mut texts: Vec<&str> = expect["tell"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        texts.extend(expect["next"]["ask_user"].as_str());
        for text in texts {
            let mut found = spans_of(text);
            if text.starts_with("reviewloop ") {
                found.push(text.to_string());
            }
            spans.extend(found.into_iter().map(|span| (example.path.clone(), span)));
        }
    }
    Ok(spans)
}

pub async fn check_cli(skill: &Skill, machine: &Machine) -> Result<()> {
    let mut commands = Vec::new();
    let mut lone_flags = Vec::new();
    for (file, span) in spans(skill)? {
        if let Some(command) = cli_command(&span) {
            commands.push((file, command));
        } else if let Some(flag) = LONE_FLAG.captures(&span) {
            lone_flags.push((file, flag[1].to_string()));
        }
    }
    ensure!(
        !commands.is_empty(),
        "the skill names no reviewloop command"
    );

    let top = defined_flags(machine, &[]).await?;
    let mut known = top.clone();
    for (file, command) in &commands {
        let at = || format!("{}: `{}`", file.display(), command.text);
        let flags = defined_flags(machine, &command.path)
            .await
            .with_context(at)?;
        for flag in &command.global_flags {
            ensure!(top.contains(flag), "{}: no global flag {flag}", at());
        }
        for flag in &command.flags {
            ensure!(
                flags.contains(flag),
                "{}: reviewloop {} has no {flag}",
                at(),
                command.path.join(" ")
            );
        }
        known.extend(flags);

        // A JSON recipe prints nothing but the document, with the default
        // logging configuration (logs to stdout), in a repository that loads
        // and in one whose config does not (which logs a warning).
        if command.flags.iter().any(|flag| flag == "--json") {
            let text = command.text.replace("<paper_id>", "main");
            let args: Vec<&str> = text.split_whitespace().skip(1).collect();
            cli_json(machine, &machine.repo(), &args)
                .await
                .with_context(at)?;
            json_or_failure(machine, &machine.broken_repo(), &args)
                .await
                .with_context(at)?;
        }
    }
    for (file, flag) in lone_flags {
        ensure!(
            known.contains(&flag),
            "{}: `{flag}` is not a flag of any command the skill names",
            file.display()
        );
    }
    Ok(())
}

/// A command that succeeds prints only JSON; one that fails prints its error
/// on stderr.
async fn json_or_failure(machine: &Machine, cwd: &Path, args: &[&str]) -> Result<()> {
    let output = machine.run(cwd, args).await?;
    if output.status.success() {
        let stdout = String::from_utf8(output.stdout)?;
        serde_json::from_str::<Value>(&stdout).with_context(|| {
            format!(
                "reviewloop {} printed more than JSON:\n{stdout}",
                args.join(" ")
            )
        })?;
    }
    Ok(())
}

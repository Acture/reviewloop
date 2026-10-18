//! The skill's shape: frontmatter that the Agent Skills spec and Codex's
//! skill validator accept, and links that resolve inside the skill, from
//! which every shipped file is reachable.

use crate::markdown::{Skill, anchors, frontmatter, links};
use anyhow::{Result, ensure};
use regex::Regex;
use std::{collections::BTreeSet, path::PathBuf};

/// Agent Skills frontmatter keys that Codex's bundled validator also accepts.
const FRONTMATTER_KEYS: [&str; 5] = [
    "name",
    "description",
    "license",
    "allowed-tools",
    "metadata",
];

pub fn check_frontmatter(skill: &Skill, directory: &str) -> Result<()> {
    let (fields, body) = frontmatter(skill.text("SKILL.md")?)?;
    for key in fields.keys() {
        let top = key.split('.').next().unwrap_or(key);
        ensure!(
            FRONTMATTER_KEYS.contains(&top),
            "unexpected frontmatter key {key}"
        );
    }

    // 1-64 lowercase letters, digits and single hyphens, equal to the
    // directory name.
    let name = fields.get("name").map(String::as_str).unwrap_or_default();
    ensure!(
        Regex::new(r"^[a-z0-9]+(-[a-z0-9]+)*$")?.is_match(name) && name.len() <= 64,
        "invalid name {name:?}"
    );
    ensure!(name == directory, "name {name:?} is not the directory name");

    // One plain YAML scalar of 1-1024 characters without angle brackets
    // (Codex's validator), so every host reads the same single line.
    let description = fields
        .get("description")
        .map(String::as_str)
        .unwrap_or_default();
    ensure!(
        description.chars().count() <= 1024,
        "description over 1024 characters"
    );
    ensure!(
        !description.contains(['<', '>']),
        "angle brackets in description"
    );
    ensure!(
        description.starts_with(|c: char| c.is_ascii_alphanumeric())
            && !description.contains(": ")
            && !description.contains(" #")
            && !description.contains('\t')
            && !description.ends_with(':'),
        "description must stay a plain YAML scalar: {description}"
    );
    for trigger in ["review", "ReviewLoop", "MCP", "CLI"] {
        ensure!(
            description.contains(trigger),
            "description lacks {trigger:?}"
        );
    }

    // The spec's budget for the body loaded on activation.
    ensure!(
        body.lines().count() <= 500,
        "SKILL.md body is over 500 lines"
    );
    Ok(())
}

/// Every link and anchor in every Markdown file resolves inside the skill,
/// headings are unique per file, and every shipped file is reachable from
/// SKILL.md by following links.
pub fn check_links(skill: &Skill) -> Result<()> {
    for (file, text) in skill.markdown() {
        anchors(text).map_err(|err| err.context(file.display().to_string()))?;
        for target in links(text).map_err(|err| err.context(file.display().to_string()))? {
            skill.resolve(file, &target)?;
        }
    }
    let start = PathBuf::from("SKILL.md");
    let mut reached = BTreeSet::from([start.clone()]);
    let mut queue = vec![start];
    while let Some(file) = queue.pop() {
        if file.extension().is_none_or(|ext| ext != "md") {
            continue;
        }
        for target in links(skill.text(&file)?)? {
            let (path, _) = skill.resolve(&file, &target)?;
            if reached.insert(path.clone()) {
                queue.push(path);
            }
        }
    }
    let unreached: Vec<&PathBuf> = skill.files.difference(&reached).collect();
    ensure!(
        unreached.is_empty(),
        "not reachable from SKILL.md: {unreached:?}"
    );
    for reference in [
        "references/lifecycle.md",
        "references/errors.md",
        "references/cli-fallback.md",
        "examples/README.md",
    ] {
        ensure!(
            links(skill.text("SKILL.md")?)?
                .iter()
                .any(|target| target.split('#').next() == Some(reference)),
            "SKILL.md does not link {reference}"
        );
    }
    Ok(())
}

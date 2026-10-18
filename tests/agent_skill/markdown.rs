//! The skill's files as data: Markdown code spans, headings, sections,
//! tables and links, and the frontmatter of SKILL.md.

use anyhow::{Context, Result, bail, ensure};
use regex::Regex;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
    sync::LazyLock,
};

pub static CODE_SPAN: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"`([^`]+)`").unwrap());
static LINK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\[[^\]]*\]\(([^)\s]+)\)").unwrap());
/// Link forms `LINK` does not read: a titled link, a reference link, a
/// reference definition.
static UNREAD_LINK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"\]\([^)\s]+\s+["']|\]\[|^\s{0,3}\[[^\]]+\]:\s"#).unwrap());
static HEADING: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^ {0,3}(#{1,6})\s+(.*?)\s*#*\s*$").unwrap());

/// Every file the skill ships, relative to its root (dotfiles such as
/// `.DS_Store` are not shipped), with the text of its Markdown and JSON files.
#[derive(Clone)]
pub struct Skill {
    pub files: BTreeSet<PathBuf>,
    pub texts: BTreeMap<PathBuf, String>,
}

impl Skill {
    pub fn load(root: &Path) -> Result<Self> {
        fn walk(root: &Path, dir: &Path, skill: &mut Skill) -> Result<()> {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                if entry.file_name().to_string_lossy().starts_with('.') {
                    continue;
                }
                let path = entry.path();
                if path.is_dir() {
                    walk(root, &path, skill)?;
                    continue;
                }
                let relative = path.strip_prefix(root)?.to_path_buf();
                if relative
                    .extension()
                    .is_some_and(|ext| ext == "md" || ext == "json")
                {
                    let text = fs::read_to_string(&path)
                        .with_context(|| format!("read {}", relative.display()))?;
                    skill.texts.insert(relative.clone(), text);
                }
                skill.files.insert(relative);
            }
            Ok(())
        }
        let mut skill = Skill {
            files: BTreeSet::new(),
            texts: BTreeMap::new(),
        };
        walk(root, root, &mut skill)?;
        Ok(skill)
    }

    pub fn text(&self, path: impl AsRef<Path>) -> Result<&str> {
        let path = path.as_ref();
        self.texts
            .get(path)
            .map(String::as_str)
            .with_context(|| format!("no file {}", path.display()))
    }

    pub fn markdown(&self) -> impl Iterator<Item = (&PathBuf, &str)> {
        self.texts
            .iter()
            .filter(|(path, _)| path.extension().is_some_and(|ext| ext == "md"))
            .map(|(path, text)| (path, text.as_str()))
    }

    /// The section of `file` under `#anchor`.
    pub fn section(&self, file: impl AsRef<Path>, anchor: &str) -> Result<String> {
        let file = file.as_ref();
        section(self.text(file)?, anchor)
            .with_context(|| format!("{} has no section #{anchor}", file.display()))
    }

    /// A link target (`path`, `path#anchor` or `#anchor`) resolved from the
    /// skill file `from`: the file it names, relative to the skill root, and
    /// the anchor, which must exist.
    pub fn resolve(&self, from: &Path, target: &str) -> Result<(PathBuf, Option<String>)> {
        let (path, anchor) = match target.split_once('#') {
            Some((path, anchor)) => (path, Some(anchor.to_string())),
            None => (target, None),
        };
        let file = if path.is_empty() {
            from.to_path_buf()
        } else {
            let mut resolved = PathBuf::new();
            for part in from
                .parent()
                .unwrap_or(Path::new(""))
                .join(path)
                .components()
            {
                match part {
                    Component::ParentDir => ensure!(
                        resolved.pop(),
                        "{} links to {target}, outside the skill",
                        from.display()
                    ),
                    Component::CurDir => {}
                    other => resolved.push(other),
                }
            }
            resolved
        };
        ensure!(
            self.files.contains(&file),
            "{} links to {target}, but {} does not exist",
            from.display(),
            file.display()
        );
        if let Some(anchor) = &anchor {
            ensure!(
                anchors(self.text(&file)?)?.contains(anchor),
                "{} links to {target}, but {} has no heading #{anchor}",
                from.display(),
                file.display()
            );
        }
        Ok((file, anchor))
    }
}

/// Lines outside fenced blocks, and the lines inside them.
pub fn split_fences(text: &str) -> (Vec<(usize, &str)>, Vec<&str>) {
    let (mut prose, mut fenced, mut in_fence) = (Vec::new(), Vec::new(), false);
    for (index, line) in text.lines().enumerate() {
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
        } else if in_fence {
            fenced.push(line);
        } else {
            prose.push((index, line));
        }
    }
    (prose, fenced)
}

/// Inline code spans outside fenced blocks, plus every fenced line.
pub fn code_spans(text: &str) -> Vec<String> {
    let (prose, fenced) = split_fences(text);
    let mut spans: Vec<String> = prose
        .iter()
        .flat_map(|(_, line)| {
            CODE_SPAN
                .captures_iter(line)
                .map(|c| c[1].trim().to_string())
        })
        .collect();
    spans.extend(fenced.iter().map(|line| line.trim().to_string()));
    spans
}

/// The code spans of one line or cell.
pub fn spans_of(text: &str) -> Vec<String> {
    CODE_SPAN
        .captures_iter(text)
        .map(|c| c[1].to_string())
        .collect()
}

/// GitHub's heading anchor: lower case, punctuation dropped, spaces to hyphens.
pub fn slug(heading: &str) -> String {
    heading
        .trim()
        .to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric() || matches!(c, ' ' | '-' | '_'))
        .map(|c| if c == ' ' { '-' } else { c })
        .collect()
}

/// Headings outside fenced blocks: line index, level, anchor.
fn headings(text: &str) -> Vec<(usize, usize, String)> {
    split_fences(text)
        .0
        .iter()
        .filter_map(|(index, line)| {
            HEADING
                .captures(line)
                .map(|c| (*index, c[1].len(), slug(&c[2])))
        })
        .collect()
}

/// The anchors of a file's headings, which must be unique.
pub fn anchors(text: &str) -> Result<BTreeSet<String>> {
    let mut anchors = BTreeSet::new();
    for (_, _, anchor) in headings(text) {
        ensure!(
            anchors.insert(anchor.clone()),
            "duplicate heading #{anchor}"
        );
    }
    Ok(anchors)
}

/// The section under the heading `#anchor`, down to the next heading of the
/// same or a higher level.
pub fn section(text: &str, anchor: &str) -> Option<String> {
    let lines: Vec<&str> = text.lines().collect();
    let headings = headings(text);
    let at = headings.iter().position(|(_, _, a)| a == anchor)?;
    let (start, depth, _) = headings[at];
    let end = headings[at + 1..]
        .iter()
        .find(|(_, level, _)| *level <= depth)
        .map_or(lines.len(), |(index, _, _)| *index);
    Some(lines[start..end].join("\n"))
}

/// Rows of the first table under `#anchor`, as trimmed cells.
pub fn table(text: &str, anchor: &str) -> Result<Vec<Vec<String>>> {
    let section = section(text, anchor).with_context(|| format!("no section #{anchor}"))?;
    let rows: Vec<Vec<String>> = section
        .lines()
        .skip_while(|line| !line.starts_with('|'))
        .take_while(|line| line.starts_with('|'))
        .skip(2)
        .map(|line| {
            line.trim_matches('|')
                .split(" | ")
                .map(|cell| cell.trim().to_string())
                .collect()
        })
        .collect();
    ensure!(!rows.is_empty(), "#{anchor} has no table");
    Ok(rows)
}

/// The table row whose first cell is `` `key` ``.
pub fn row<'a>(rows: &'a [Vec<String>], key: &str) -> Option<&'a Vec<String>> {
    rows.iter().find(|row| row[0].trim_matches('`') == key)
}

/// Relative link targets outside code, fenced blocks and URLs. A link form
/// this parser cannot read is an error, not a silent skip.
pub fn links(text: &str) -> Result<Vec<String>> {
    let mut targets = Vec::new();
    for (index, line) in split_fences(text).0 {
        let line = CODE_SPAN.replace_all(line, "");
        if UNREAD_LINK.is_match(&line) {
            bail!("line {}: use plain [text](target) links: {line}", index + 1);
        }
        targets.extend(
            LINK.captures_iter(&line)
                .map(|c| c[1].trim_matches(['<', '>']).to_string())
                .filter(|target| !target.contains("://") && !target.starts_with("mailto:")),
        );
    }
    Ok(targets)
}

/// SKILL.md's frontmatter as `key` → value (`map.key` for a nested map),
/// and the body after it. Accepts only what a YAML loader reads the same
/// way: `key: value` lines, and `key:` opening a map of indented
/// `key: value` lines; no key twice.
pub fn frontmatter(text: &str) -> Result<(BTreeMap<String, String>, &str)> {
    let rest = text
        .strip_prefix("---\n")
        .context("SKILL.md must start with ---")?;
    let (head, body) = rest
        .split_once("\n---\n")
        .context("unterminated frontmatter")?;
    let mut fields = BTreeMap::new();
    let mut parent: Option<String> = None;
    for line in head.lines() {
        let (key, value) = if let Some(nested) = line.strip_prefix("  ") {
            let parent = parent.as_ref().context("indented line outside a map")?;
            let (key, value) = nested
                .split_once(": ")
                .with_context(|| format!("{line:?} is not key: value"))?;
            (format!("{parent}.{key}"), value.to_string())
        } else if let Some(key) = line.strip_suffix(':') {
            parent = Some(key.to_string());
            (key.to_string(), String::new())
        } else {
            parent = None;
            let (key, value) = line
                .split_once(": ")
                .with_context(|| format!("{line:?} is not key: value"))?;
            (key.to_string(), value.to_string())
        };
        ensure!(
            fields.insert(key.clone(), value).is_none(),
            "duplicate frontmatter key {key}"
        );
    }
    Ok((fields, body))
}

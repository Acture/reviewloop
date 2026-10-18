//! The live contract the skill names: the tools `reviewloop mcp` lists in
//! both approval modes, the JSON the CLI prints, and the tables of
//! docs/review-operations.md.

use crate::markdown::{code_spans, spans_of, table};
use anyhow::{Context, Result, ensure};
use regex::Regex;
use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, Tool},
    transport::TokioChildProcess,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::{Output, Stdio},
    sync::LazyLock,
};
use tokio::process::Command;

pub static SNAKE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[a-z][a-z0-9_]*$").unwrap());
pub static UPPER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[A-Z][A-Z0-9_]*$").unwrap());
pub static DOTTED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)+$").unwrap());

const CONTRACT: &str = "docs/review-operations.md";

pub fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// An isolated home, global config and state directory, with a repository
/// `my-paper` holding one stanford paper `main`, and a repository `broken`
/// whose `reviewloop.toml` does not parse (a path that logs a warning).
pub struct Machine {
    _tmp: tempfile::TempDir,
    root: PathBuf,
}

impl Machine {
    pub fn new() -> Result<Self> {
        let tmp = tempfile::tempdir()?;
        let root = tmp.path().canonicalize()?;
        fs::create_dir_all(root.join("home"))?;
        fs::create_dir_all(root.join("xdg/reviewloop"))?;
        fs::write(
            root.join("xdg/reviewloop/config.toml"),
            "[providers.stanford]\nemail = \"agent@example.edu\"\n",
        )?;
        let repo = root.join("my-paper");
        fs::create_dir_all(repo.join(".git"))?;
        fs::write(repo.join("main.pdf"), b"%PDF-1.4\n%%EOF\n")?;
        fs::write(
            repo.join("reviewloop.toml"),
            "project_id = \"my-paper\"\n\n[[papers]]\nid = \"main\"\npdf_path = \"main.pdf\"\nbackend = \"stanford\"\n",
        )?;
        let broken = root.join("broken");
        fs::create_dir_all(broken.join(".git"))?;
        fs::write(broken.join("reviewloop.toml"), "project_id = [\n")?;
        Ok(Self { _tmp: tmp, root })
    }

    pub fn repo(&self) -> PathBuf {
        self.root.join("my-paper")
    }

    pub fn broken_repo(&self) -> PathBuf {
        self.root.join("broken")
    }

    pub fn command(&self, cwd: &Path, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_reviewloop"));
        command
            .args(args)
            .current_dir(cwd)
            .env("HOME", self.root.join("home"))
            .env("XDG_CONFIG_HOME", self.root.join("xdg"))
            .env("REVIEWLOOP_STATE_DIR", self.root.join("state"))
            .env_remove("REVIEWLOOP_CSPAPER_API_KEY")
            .env_remove("RUST_LOG");
        command
    }

    pub async fn run(&self, cwd: &Path, args: &[&str]) -> Result<Output> {
        Ok(self.command(cwd, args).output().await?)
    }

    /// The tools `reviewloop mcp --approval <approval>` lists, by name. With
    /// approval required, the session also requests a review of `main`, so
    /// the CLI has a job row to print (nothing is sent).
    async fn tools(&self, approval: &str) -> Result<BTreeMap<String, Tool>> {
        let command = self.command(&self.repo(), &["mcp", "--approval", approval]);
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
        if approval == "required" {
            let arguments = json!({"paper_id": "main", "request_key": "contract/main"});
            let result = client
                .call_tool(
                    CallToolRequestParams::new("request_review")
                        .with_arguments(arguments.as_object().cloned().unwrap_or_default()),
                )
                .await?;
            ensure!(
                result.is_error != Some(true),
                "request_review failed: {result:?}"
            );
        }
        client.cancel().await?;
        Ok(tools)
    }
}

/// Run a CLI command from `cwd` that must print one JSON document on stdout.
pub async fn cli_json(machine: &Machine, cwd: &Path, args: &[&str]) -> Result<Value> {
    let output = machine.run(cwd, args).await?;
    ensure!(
        output.status.success(),
        "reviewloop {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    serde_json::from_str(&stdout).with_context(|| {
        format!(
            "reviewloop {} printed more than JSON:\n{stdout}",
            args.join(" ")
        )
    })
}

pub struct Contract {
    /// By approval mode (`required`, `granted`), then tool name.
    pub tools: BTreeMap<String, BTreeMap<String, Tool>>,
    /// Single names: tools, arguments, result fields, enum values, the code
    /// spans of docs/review-operations.md, error codes, error detail fields
    /// and the keys of the CLI's JSON.
    pub words: BTreeSet<String>,
    /// Statuses, submit stages and the upper-case spans of the contract.
    pub upper: BTreeSet<String>,
    /// Full dotted paths: tool result fields, CLI JSON keys, the dotted
    /// spans of the contract (`providers.cspaper.api_key`) and
    /// `details.<field>`.
    pub paths: BTreeSet<String>,
    pub error_codes: BTreeSet<String>,
    /// Error code → the fields of its `details`.
    pub details: BTreeMap<String, BTreeSet<String>>,
    /// Keys only the CLI's JSON has (`id`, `token_masked`, `artifact_dir`).
    pub cli_only: BTreeSet<String>,
}

impl Contract {
    pub async fn load(machine: &Machine) -> Result<Self> {
        let mut tools = BTreeMap::new();
        for approval in ["required", "granted"] {
            tools.insert(approval.to_string(), machine.tools(approval).await?);
        }
        let mut schema_words = BTreeSet::new();
        let mut upper = BTreeSet::new();
        let mut paths = BTreeSet::new();
        for tool in tools["required"].values() {
            schema_words.insert(tool.name.to_string());
            let input = Value::Object(tool.input_schema.as_ref().clone());
            collect_schema_words(&input, &mut schema_words, &mut upper);
            if let Some(output) = &tool.output_schema {
                let output = Value::Object(output.as_ref().clone());
                collect_schema_words(&output, &mut schema_words, &mut upper);
                collect_paths(&output, "", &mut paths);
            }
        }

        let doc = fs::read_to_string(repo().join(CONTRACT))?;
        let mut words = schema_words.clone();
        for span in code_spans(&doc) {
            let span = span.trim_matches('"');
            if SNAKE.is_match(span) {
                words.insert(span.to_string());
            } else if DOTTED.is_match(span) {
                paths.insert(span.to_string());
            } else if UPPER.is_match(span) {
                upper.insert(span.to_string());
            }
        }
        let mut error_codes = BTreeSet::new();
        let mut details = BTreeMap::new();
        for row in table(&doc, "errors")? {
            let code = row[0].trim_matches('`').to_string();
            let fields = detail_fields(&row[2]);
            paths.extend(fields.iter().map(|field| format!("details.{field}")));
            words.extend(fields.iter().cloned());
            error_codes.insert(code.clone());
            details.insert(code, fields);
        }
        words.extend(error_codes.iter().cloned());

        let mut cli_keys = BTreeSet::new();
        for args in [
            &["status", "--json"][..],
            &["project", "list", "--json"],
            &["daemon", "status", "--json"],
        ] {
            let document = cli_json(machine, &machine.repo(), args).await?;
            collect_keys(&document, "", &mut cli_keys, &mut paths);
        }
        let detail_words: BTreeSet<&String> = details.values().flatten().collect();
        let cli_only = cli_keys
            .iter()
            .filter(|key| {
                !schema_words.contains(*key)
                    && !error_codes.contains(*key)
                    && !detail_words.contains(key)
            })
            .cloned()
            .collect();
        words.extend(cli_keys);
        Ok(Self {
            tools,
            words,
            upper,
            paths,
            error_codes,
            details,
            cli_only,
        })
    }

    pub fn tool(&self, approval: &str, name: &str) -> Result<&Tool> {
        self.tools
            .get(approval)
            .with_context(|| format!("no approval mode {approval}"))?
            .get(name)
            .with_context(|| format!("no tool {name}"))
    }

    pub fn arguments(&self, name: &str) -> Result<BTreeSet<String>> {
        let schema = self.tool("required", name)?.input_schema.as_ref();
        Ok(schema
            .get("properties")
            .and_then(Value::as_object)
            .map(|properties| properties.keys().cloned().collect())
            .unwrap_or_default())
    }

    /// A dotted name is a full path, or ends one at a segment boundary.
    pub fn knows_path(&self, dotted: &str) -> bool {
        let suffix = format!(".{dotted}");
        self.paths
            .iter()
            .any(|path| path == dotted || path.ends_with(&suffix))
    }

    /// Whether calling `name` leads to the provider or ends a job.
    pub fn needs_consent(&self, approval: &str, name: &str) -> Result<bool> {
        let hints = self
            .tool(approval, name)?
            .annotations
            .as_ref()
            .context("annotations")?;
        Ok(hints.open_world_hint == Some(true) || hints.destructive_hint == Some(true))
    }

    /// The enum values of the output property of `tool` at the JSON
    /// `pointer` into its outputSchema, such as `/properties/status`.
    pub fn values(&self, tool: &str, pointer: &str) -> Result<BTreeSet<String>> {
        let output = self
            .tool("required", tool)?
            .output_schema
            .as_deref()
            .cloned()
            .context("outputSchema")?;
        let schema = Value::Object(output)
            .pointer(pointer)
            .cloned()
            .with_context(|| format!("{tool} outputSchema has no {pointer}"))?;
        let (mut lower, mut upper) = (BTreeSet::new(), BTreeSet::new());
        collect_schema_words(&schema, &mut lower, &mut upper);
        lower.extend(upper);
        Ok(lower)
    }
}

/// The detail fields a row of the contract's error table lists after
/// "Details:", leaving out what parentheses explain about them.
fn detail_fields(when: &str) -> BTreeSet<String> {
    let Some((_, listed)) = when.split_once("Details:") else {
        return BTreeSet::new();
    };
    let mut top = String::new();
    let (mut depth, mut in_code) = (0usize, false);
    for c in listed.chars() {
        match c {
            '`' => in_code = !in_code,
            '(' if !in_code => depth += 1,
            ')' if !in_code => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0 && c != ')' {
            top.push(c);
        }
    }
    spans_of(&top)
        .into_iter()
        .filter(|span| SNAKE.is_match(span))
        .collect()
}

/// Property names and enum values (`enum` lists and `const`s) of a schema.
pub fn collect_schema_words(
    schema: &Value,
    words: &mut BTreeSet<String>,
    upper: &mut BTreeSet<String>,
) {
    match schema {
        Value::Object(map) => {
            if let Some(Value::Object(properties)) = map.get("properties") {
                words.extend(properties.keys().cloned());
            }
            let values = map
                .get("enum")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .chain(map.get("const"));
            for value in values.filter_map(Value::as_str) {
                if UPPER.is_match(value) {
                    upper.insert(value.to_string());
                } else {
                    words.insert(value.to_string());
                }
            }
            for (key, value) in map {
                if key != "description" {
                    collect_schema_words(value, words, upper);
                }
            }
        }
        Value::Array(items) => items
            .iter()
            .for_each(|item| collect_schema_words(item, words, upper)),
        _ => {}
    }
}

/// The dotted paths of a schema's properties, through `anyOf`, `oneOf`,
/// `allOf` and `items`.
fn collect_paths(schema: &Value, prefix: &str, paths: &mut BTreeSet<String>) {
    let Value::Object(map) = schema else {
        return;
    };
    if let Some(Value::Object(properties)) = map.get("properties") {
        for (key, property) in properties {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            collect_paths(property, &path, paths);
            paths.insert(path);
        }
    }
    for key in ["anyOf", "oneOf", "allOf"] {
        for branch in map.get(key).and_then(Value::as_array).into_iter().flatten() {
            collect_paths(branch, prefix, paths);
        }
    }
    if let Some(items) = map.get("items") {
        collect_paths(items, prefix, paths);
    }
}

/// The keys of a JSON document, and their dotted paths (arrays flattened).
fn collect_keys(
    value: &Value,
    prefix: &str,
    keys: &mut BTreeSet<String>,
    paths: &mut BTreeSet<String>,
) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                keys.insert(key.clone());
                collect_keys(value, &path, keys, paths);
                paths.insert(path);
            }
        }
        Value::Array(items) => items
            .iter()
            .for_each(|item| collect_keys(item, prefix, keys, paths)),
        _ => {}
    }
}

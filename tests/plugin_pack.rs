//! OSS-341: Codex and Claude Code plugin package.
//!
//! The skill under `integrations/reviewloop/skills/reviewloop` is the only
//! copy. The packager copies it, and the packaged bytes must match. Manifest
//! paths stay inside the plugin root. The launcher finds an installed
//! `reviewloop` without a GUI host's PATH. The stdio smoke lists tools and
//! does not request a review.

use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command as TokioCommand,
    time::{Duration, timeout},
};

const TOOLS: [&str; 12] = [
    "list_projects",
    "list_papers",
    "request_review",
    "get_job",
    "list_jobs",
    "get_review",
    "approve_job",
    "retry_job",
    "get_worker_status",
    "watch_project",
    "unwatch_project",
    "cancel_job",
];

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn plugin_src() -> PathBuf {
    repo().join("integrations/reviewloop")
}

fn package_version() -> Result<String> {
    let text = fs::read_to_string(repo().join("Cargo.toml"))?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("version = \"") {
            let Some(version) = rest.strip_suffix('"') else {
                continue;
            };
            return Ok(version.to_string());
        }
    }
    bail!("Cargo.toml has no package version");
}

fn read_json(path: &Path) -> Result<Value> {
    let text = fs::read_to_string(path).with_context(|| path.display().to_string())?;
    Ok(serde_json::from_str(&text)?)
}

fn files(root: &Path) -> Result<BTreeMap<PathBuf, Vec<u8>>> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let kind = entry.file_type()?;
            ensure!(
                !kind.is_symlink(),
                "symlink in plugin tree: {}",
                path.display()
            );
            if kind.is_dir() {
                walk(root, &path, out)?;
            } else if kind.is_file() {
                let rel = path.strip_prefix(root)?.to_path_buf();
                out.insert(rel, fs::read(path)?);
            }
        }
        Ok(())
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out)?;
    Ok(out)
}

fn pack_to(out: &Path) -> Result<()> {
    let status = Command::new(repo().join("scripts/package-plugin.sh"))
        .arg(out)
        .status()
        .context("spawn package-plugin.sh")?;
    ensure!(status.success(), "package-plugin.sh failed: {status}");
    Ok(())
}

fn assert_inside(root: &Path, raw: &str) -> Result<()> {
    ensure!(!raw.contains(".."), "path escapes the plugin root: {raw}");
    let relative = if let Some(rest) = raw.strip_prefix("${CLAUDE_PLUGIN_ROOT}/") {
        rest
    } else if let Some(rest) = raw.strip_prefix("${PLUGIN_ROOT}/") {
        rest
    } else if let Some(rest) = raw.strip_prefix("./") {
        rest
    } else {
        bail!("path is not inside the plugin root: {raw}");
    };
    ensure!(
        !relative.starts_with('/') && !relative.is_empty(),
        "path is not inside the plugin root: {raw}"
    );
    let path = root.join(relative);
    ensure!(path.exists(), "missing plugin path {raw}");
    let canon_root = root.canonicalize()?;
    let canon = path.canonicalize()?;
    ensure!(
        canon.starts_with(&canon_root),
        "{raw} resolves outside the plugin root"
    );
    Ok(())
}

fn string_field<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("missing string {key}"))
}

#[test]
fn package_matches_the_single_skill_source() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let out = tmp.path().join("reviewloop-plugin");
    pack_to(&out)?;

    let source = files(&plugin_src().join("skills/reviewloop"))?;
    let packaged = files(&out.join("skills/reviewloop"))?;
    ensure!(
        source == packaged,
        "packaged skill differs from the source tree"
    );
    let skill_files: Vec<_> = files(&out)?
        .into_keys()
        .filter(|path| path.file_name().is_some_and(|name| name == "SKILL.md"))
        .collect();
    ensure!(
        skill_files == vec![PathBuf::from("skills/reviewloop/SKILL.md")],
        "unexpected SKILL.md files: {skill_files:?}"
    );

    let readme = fs::read(out.join("README.md"))?;
    let docs = fs::read(repo().join("docs/plugins.md"))?;
    ensure!(readme == docs, "packaged README is not docs/plugins.md");
    ensure!(
        out.join("bin/reviewloop-mcp").metadata()?.is_file(),
        "launcher missing"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(out.join("bin/reviewloop-mcp"))?
            .permissions()
            .mode();
        ensure!(mode & 0o111 != 0, "launcher is not executable");
    }
    Ok(())
}

#[test]
fn manifests_stay_inside_the_plugin_and_name_stdio() -> Result<()> {
    let root = plugin_src();
    let version = package_version()?;
    for manifest in [
        "plugin.json",
        ".claude-plugin/plugin.json",
        ".codex-plugin/plugin.json",
    ] {
        let value = read_json(&root.join(manifest))?;
        ensure!(
            string_field(&value, "name")? == "reviewloop",
            "{manifest} name"
        );
        ensure!(
            string_field(&value, "version")? == version,
            "{manifest} version drifted from Cargo.toml"
        );
    }

    let portable = read_json(&root.join("plugin.json"))?;
    ensure!(portable["$schema"] == "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json");
    let mcp = read_json(&root.join("mcp.json"))?;
    ensure!(mcp["$schema"] == "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json");
    ensure!(mcp["mcpServers"]["reviewloop"]["type"] == "stdio");
    assert_inside(
        &root,
        string_field(&mcp["mcpServers"]["reviewloop"], "command")?,
    )?;

    let claude = read_json(&root.join(".claude-plugin/plugin.json"))?;
    assert_inside(&root, string_field(&claude, "skills")?)?;
    assert_inside(&root, string_field(&claude, "mcpServers")?)?;
    let claude_mcp = read_json(&root.join("mcp/claude.json"))?;
    assert_inside(
        &root,
        string_field(&claude_mcp["mcpServers"]["reviewloop"], "command")?,
    )?;

    let codex = read_json(&root.join(".codex-plugin/plugin.json"))?;
    assert_inside(&root, string_field(&codex, "skills")?)?;
    assert_inside(&root, string_field(&codex, "mcpServers")?)?;
    let codex_mcp = read_json(&root.join("mcp/codex.json"))?;
    assert_inside(
        &root,
        string_field(&codex_mcp["mcpServers"]["reviewloop"], "command")?,
    )?;

    for path in ["mcp.json", "mcp/claude.json", "mcp/codex.json"] {
        let text = fs::read_to_string(root.join(path))?;
        ensure!(!text.contains("\"url\""), "{path} declares a remote URL");
        ensure!(
            !text.contains("streamable-http") && !text.contains("\"sse\""),
            "{path} is not local stdio"
        );
    }

    let generic = read_json(&repo().join("docs/plugins/stdio.mcp.json"))?;
    let command = string_field(&generic["mcpServers"]["reviewloop"], "command")?;
    ensure!(
        command.starts_with('/'),
        "generic config must name an absolute binary, got {command}"
    );
    ensure!(generic["mcpServers"]["reviewloop"]["args"] == serde_json::json!(["mcp"]));
    ensure!(!generic.to_string().contains("url"));
    Ok(())
}

fn run_launcher(home: &Path, bin: Option<&Path>, path: &str) -> std::process::Output {
    let mut command = Command::new(plugin_src().join("bin/reviewloop-mcp"));
    command
        .env("HOME", home)
        .env("PATH", path)
        .env_remove("REVIEWLOOP_CSPAPER_API_KEY")
        .env_remove("REVIEWLOOP_CSPAPER_COOKIE");
    match bin {
        Some(bin) => {
            command.env("REVIEWLOOP_BIN", bin);
        }
        None => {
            command.env_remove("REVIEWLOOP_BIN");
        }
    }
    command.output().expect("spawn launcher")
}

#[test]
fn launcher_reports_a_missing_binary_without_using_path() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let output = run_launcher(tmp.path(), None, "/usr/bin:/bin");
    ensure!(!output.status.success(), "missing binary exited 0");
    let err = String::from_utf8(output.stderr)?;
    ensure!(
        output.stdout.is_empty(),
        "stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    ensure!(err.contains("not found"), "{err}");
    ensure!(err.contains("/opt/homebrew/bin"), "{err}");
    ensure!(err.contains("$HOME/.cargo/bin"), "{err}");
    ensure!(err.contains("does not inherit a terminal PATH"), "{err}");
    ensure!(
        err.contains("remote HTTP MCP endpoint is not available"),
        "{err}"
    );
    ensure!(err.contains("0.0.1"), "{err}");
    Ok(())
}

#[test]
fn launcher_rejects_a_binary_without_mcp() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let fake_dir = tmp.path().join("prefix");
    fs::create_dir_all(&fake_dir)?;
    let fake = fake_dir.join("reviewloop");
    fs::write(
        &fake,
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo reviewloop 0.2.1; exit 0; fi\necho unsupported >&2\nexit 2\n",
    )?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&fake)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&fake, perms)?;
    }
    let output = run_launcher(tmp.path(), Some(&fake), "/usr/bin:/bin");
    ensure!(!output.status.success(), "unsupported binary exited 0");
    let err = String::from_utf8(output.stderr)?;
    ensure!(err.contains("does not provide the mcp command"), "{err}");
    ensure!(err.contains("0.2.1"), "{err}");
    ensure!(
        err.contains("remote HTTP MCP endpoint is not available"),
        "{err}"
    );
    Ok(())
}

#[tokio::test]
async fn packaged_launcher_lists_tools_over_stdio() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let out = tmp.path().join("reviewloop-plugin");
    pack_to(&out)?;
    let home = tmp.path().join("home");
    let xdg = tmp.path().join("xdg");
    let state = tmp.path().join("state");
    fs::create_dir_all(&home)?;

    let mut child = TokioCommand::new(out.join("bin/reviewloop-mcp"))
        .current_dir(tmp.path())
        .env("HOME", &home)
        .env("XDG_CONFIG_HOME", &xdg)
        .env("REVIEWLOOP_STATE_DIR", &state)
        .env("REVIEWLOOP_BIN", env!("CARGO_BIN_EXE_reviewloop"))
        .env("PATH", "/usr/bin:/bin")
        .env_remove("REVIEWLOOP_CSPAPER_API_KEY")
        .env_remove("REVIEWLOOP_CSPAPER_COOKIE")
        .env_remove("RUST_LOG")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("spawn packaged launcher")?;
    let mut stdin = child.stdin.take().context("stdin")?;
    let mut stdout = BufReader::new(child.stdout.take().context("stdout")?).lines();
    let mut stderr = child.stderr.take().context("stderr")?;
    let log = tokio::spawn(async move {
        let mut log = String::new();
        stderr.read_to_string(&mut log).await.map(|_| log)
    });

    let requests = [
        serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "plugin-pack-smoke", "version": "0"}
        }}),
        serde_json::json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        serde_json::json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
    ];
    for request in &requests {
        ensure!(
            request["method"] != "tools/call",
            "smoke must not call a tool"
        );
        stdin.write_all(format!("{request}\n").as_bytes()).await?;
    }
    stdin.flush().await?;

    let mut responses = BTreeMap::new();
    let read_all = async {
        while responses.len() < 2 {
            let line = stdout.next_line().await?.context("stdout closed early")?;
            let message: Value = serde_json::from_str(&line)
                .with_context(|| format!("stdout line is not JSON: {line:?}"))?;
            ensure!(message["jsonrpc"] == "2.0", "{line}");
            if let Some(id) = message["id"].as_u64() {
                responses.insert(id, message);
            }
        }
        anyhow::Ok(())
    };
    timeout(Duration::from_secs(60), read_all).await??;
    drop(stdin);

    ensure!(responses[&1]["result"]["serverInfo"]["name"] == "reviewloop");
    ensure!(responses[&1]["result"]["serverInfo"]["version"] == package_version()?);
    let names: Vec<&str> = responses[&2]["result"]["tools"]
        .as_array()
        .context("tools")?
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect();
    ensure!(names == TOOLS, "tools: {names:?}");

    let status = timeout(Duration::from_secs(30), child.wait()).await??;
    ensure!(status.success(), "launcher exit {status}");
    let log = log.await??;
    ensure!(log.contains("serving MCP on stdio"), "stderr: {log}");
    ensure!(
        !log.contains("review request enqueued"),
        "smoke submitted work: {log}"
    );
    Ok(())
}

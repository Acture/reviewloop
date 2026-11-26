# ReviewLoop plugins

The plugin root is [`integrations/reviewloop/`](../integrations/reviewloop). It
ships one Agent Skill, [`skills/reviewloop/`](../integrations/reviewloop/skills/reviewloop/SKILL.md),
and a local stdio MCP launcher. Codex and Claude Code each have a manifest.
The skill files are that directory and nowhere else; the packager copies them
and checks the copy against the source.

`scripts/package-plugin.sh` writes a directory you can load or archive. It
copies this document in as the package `README.md`. It does not publish a
marketplace entry or a release.

## ReviewLoop binary

The plugin does not embed `reviewloop`. Install the binary first.

Supported builds are **ReviewLoop 0.0.1 and later on the restarted release
line**, the builds that provide `reviewloop mcp`. Historical 0.2.x crates do
not. An unpinned `cargo install reviewloop` can still select 0.2.x, so pin
the version.

Homebrew (Apple Silicon prefix `/opt/homebrew/bin`, Intel Homebrew prefix
`/usr/local/bin`):

```sh
brew tap acture/ac
brew install reviewloop
```

Cargo (`$HOME/.cargo/bin`):

```sh
cargo install reviewloop --version 0.0.1 --locked
```

Confirm the installed binary with the absolute path, not a shell alias:

```sh
/opt/homebrew/bin/reviewloop mcp --help
/opt/homebrew/bin/reviewloop --version
```

### How the plugin finds the binary

`bin/reviewloop-mcp` is the process the plugin starts. It does not assume a
GUI host inherited a terminal `PATH`.

Search order:

1. `REVIEWLOOP_BIN`, when set. The file must be executable. No other path is tried.
2. `$HOME/.cargo/bin/reviewloop`
3. `$HOME/.local/bin/reviewloop`
4. `/opt/homebrew/bin/reviewloop`
5. `/usr/local/bin/reviewloop`
6. `/home/linuxbrew/.linuxbrew/bin/reviewloop`
7. `reviewloop` on the process `PATH`

The chosen binary must accept `reviewloop mcp --help`. Otherwise the launcher
exits before speaking MCP and writes the reason to stderr: the path it found,
the version it reported, the supported release line, and the install commands
above. A missing binary names the prefixes it searched and says a GUI host
does not inherit a terminal PATH.

The launcher then execs `reviewloop mcp` plus any extra arguments. The plugin
configs pass none, so approval stays required. A generic config may pass
`--approval granted`; that queues a request for the supervisor, which can
send the manuscript.

This package starts a **local stdio** server only. ReviewLoop does not serve
MCP over HTTP. A remote endpoint would be a later capability.

## Generic stdio config

Use this when the host has MCP settings and no ReviewLoop plugin. Replace
`command` with the absolute path from `command -v reviewloop` in a terminal
that can see the binary. The sample uses the Apple Silicon Homebrew prefix.

[`stdio.mcp.json`](plugins/stdio.mcp.json):

```json
{
  "mcpServers": {
    "reviewloop": {
      "command": "/opt/homebrew/bin/reviewloop",
      "args": ["mcp"]
    }
  }
}
```

VS Code (`.vscode/mcp.json`) uses the same absolute command:

```json
{
  "servers": {
    "reviewloop": {
      "type": "stdio",
      "command": "/opt/homebrew/bin/reviewloop",
      "args": ["mcp"]
    }
  }
}
```

A terminal-integrated host that already has `reviewloop` on its own PATH can
store `command` as `reviewloop`. A GUI host that was not started from that
shell often cannot. Do not put a `url` in this config.

Claude Code and Codex can also register the server from a terminal, which
stores the command the same way:

```sh
claude mcp add reviewloop -- /opt/homebrew/bin/reviewloop mcp
codex mcp add reviewloop -- /opt/homebrew/bin/reviewloop mcp
```

Remove a server registered that way with `claude mcp remove reviewloop` or
`codex mcp remove reviewloop`.

## Plugin manifests

| Host | Manifest | MCP config |
| --- | --- | --- |
| Agent Plugins (current Codex / ChatGPT packaging) | `plugin.json` | `mcp.json`, command `./bin/reviewloop-mcp` |
| Claude Code | `.claude-plugin/plugin.json` | `mcp/claude.json`, command `${CLAUDE_PLUGIN_ROOT}/bin/reviewloop-mcp` |
| Codex compatibility overlay | `.codex-plugin/plugin.json` | `mcp/codex.json`, command `./bin/reviewloop-mcp` |

Every component path starts inside the plugin root. `mcp.json` declares
`"type": "stdio"`. None of the MCP configs has a URL.

Current Codex reads `plugin.json` and `mcp.json`. When `extensions.com.openai`
is present, Codex uses it instead of `.codex-plugin/plugin.json`. The overlay
remains for clients that still load that file.

## Install the plugin

These steps load the local directory. They do not add this repository to a
public marketplace.

Claude Code, for one session, from a checkout:

```sh
claude --plugin-dir /absolute/path/to/reviewloop/integrations/reviewloop
```

Quit that session to unload it. A plugin installed from a marketplace you
configured yourself is removed with:

```sh
claude plugin uninstall reviewloop
```

Codex installs from a marketplace source. This repository does not register
one. To try a private local marketplace, point its `source.path` at
`integrations/reviewloop` (the path starts with `./` and stays inside the
marketplace root), then:

```sh
codex plugin marketplace add /absolute/path/to/that-marketplace-root
codex plugin add reviewloop@<marketplace-name>
```

Remove it with:

```sh
codex plugin remove reviewloop
codex plugin marketplace remove <marketplace-name>
```

Loading the skill without the plugin still works: copy or symlink
`integrations/reviewloop/skills/reviewloop` to `~/.claude/skills/reviewloop`
or `~/.agents/skills/reviewloop`. That copy is not the source. The source
stays in this repository. Remove the copy to uninstall that skill. The
symlink or copy does not start the MCP server; add the generic stdio config
as well.

## Package

```sh
./scripts/package-plugin.sh
./scripts/package-plugin.sh --check
```

The default output is `dist/reviewloop-plugin/`, which git ignores. `--check`
packs to a temporary directory, deletes it, and exits non-zero unless:

- the packaged `skills/reviewloop` tree matches the repository source
- the package contains one `SKILL.md`
- the package contains no symlinks
- manifest paths do not contain `..`
- the packaged `README.md` is this document

`cargo test --test plugin_pack` runs those checks and a mock stdio smoke:
the launcher is given the test binary, then the test speaks `initialize` and
`tools/list` and stops. It does not call `request_review` and does not
contact Stanford or CSPaper.

## What a local check does not cover

`claude plugin validate` checks the manifest files. It does not load the
plugin in the Claude Code or Codex GUI, and it does not prove the host
resolved `./bin/reviewloop-mcp` or `${CLAUDE_PLUGIN_ROOT}` the way a desktop
session will. Those GUI loads remain a manual acceptance step.

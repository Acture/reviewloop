# ReviewLoop

[![CI](https://github.com/Acture/reviewloop/actions/workflows/ci.yml/badge.svg)](https://github.com/Acture/reviewloop/actions/workflows/ci.yml)
[![Release](https://github.com/Acture/reviewloop/actions/workflows/release.yml/badge.svg)](https://github.com/Acture/reviewloop/actions/workflows/release.yml)
[![Rust](https://img.shields.io/badge/rust-1.89%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-AGPL--3.0--or--later-blue.svg)](LICENSE)

> A production-minded Rust CLI/daemon for AI paper review submission and retrieval: `paperreview.ai` (Stanford) and CSPaper Agentic Review.

Most paper review automation breaks in boring ways: duplicate submissions, lost tokens, noisy polling, and zero traceability.

**ReviewLoop** gives you a durable loop with guardrails:
- Queue reviews from Git tags or PDF hash changes
- Persist every transition in SQLite
- Pull `paperreview.ai` tokens from Gmail OAuth or IMAP
- Write reproducible artifacts (`review.json`, `review.md`, `meta.json`)
- Recover from failures with explicit retries and fallback submission

## Why This Project Exists

Reviewing pipelines are usually a pile of scripts plus cron plus hope.
ReviewLoop is built for the opposite:
- predictable state transitions
- low default provider pressure
- human approval gates where it matters
- clear local evidence of what happened and why

If you want reliable, low-drama automation for `paperreview.ai` or CSPaper, this is the tool.

## 1-Minute Quick Start

```bash
# 1) one-time machine setup
reviewloop init

# 2) for any repo, one-time project setup
reviewloop init project --project-id main

# 2.5) one-time: configure submitter email (Stanford backend; CSPaper needs
#      an API key and a review template instead, see "CSPaper Backend")
# Edit ~/.config/reviewloop/config.toml and add:
#
#   [providers.stanford]
#   email = "you@example.edu"
#
# (reviewloop config init creates the file if it doesn't exist yet; re-running it
#  is a no-op when the file already exists, so edit the file directly.)

# 3) submit + watch a paper come back, all at once
reviewloop run paper/main.pdf
```

`reviewloop run` registers the paper if it isn't already in the project config, submits
it immediately with force, then drives a live polling loop until the review lands.

Exit codes: `0` = review complete, `2` = terminal failure or a submission that needs manual reconciliation, `130` = Ctrl+C.

> **The Stanford backend requires a submitter email.** Set `providers.stanford.email` in
> `~/.config/reviewloop/config.toml` (step 2.5 above) or run
> `reviewloop email login --provider google` to use OAuth.
> Email/OAuth is also needed if you submitted via the paperreview.ai website
> and want reviewloop to ingest tokens from your inbox.
> See [Optional: email token ingestion](#email-token-ingestion-experimental-opt-in) below.
> The CSPaper backend needs no email; see [CSPaper Backend](#cspaper-backend).

Optional flags:

```bash
reviewloop run paper/main.pdf \
  --paper-id main \        # override the default (filename stem)
  --backend stanford \     # override the default backend (stanford | cspaper)
  --no-watch \             # register the paper without PDF-change watching
  --tag-trigger "review-stanford/main/*" \  # custom tag trigger
  --quiet                  # suppress live status; print only the final line
```

## Long-running Setup (Multiple Papers, Automation)

For daemon-based automation with multiple papers and Git-tag triggers:

```bash
# register a paper (uses the project-level venue from reviewloop.toml)
reviewloop paper add \
  --paper-id main \
  --path paper/main.pdf

# register a second paper targeting a different venue (per-paper override)
reviewloop paper add \
  --paper-id camera_ready \
  --path build/camera_ready.pdf \
  --venue NeurIPS \
  --tag-trigger "custom-review/camera_ready/*"

# watch this project: the machine supervisor submits and polls for it (once per project repo)
reviewloop project watch

# install and start the supervisor (macOS, once per machine)
reviewloop daemon install
```

The supervisor runs every 30 seconds, handles triggers, submissions, polls, retries, token
ingestion and retention pruning for every watched project. Use `reviewloop daemon status`,
`reviewloop status` and `reviewloop check` to monitor it.

## Deployment model: one supervisor, explicitly watched projects

One **supervisor** (`reviewloop daemon run`, installed on macOS as the launchd
agent `ai.reviewloop.daemon`) runs every project you **watch** on the machine:

- **Watch explicitly.** Any command that loads a project's `reviewloop.toml`
  registers it (`reviewloop project list` shows the registry), but only
  `reviewloop project watch` makes the supervisor run it: its git-tag and
  PDF triggers, queue, polls, timeouts and recovery. `reviewloop project
  unwatch` stops that; the project's jobs keep their state and explicit
  commands (`submit`, `check`, `retry`, …) still act on them.
- **One registration per `project_id`.** Paths are stored canonical. A
  second clone or worktree declaring the same `project_id` never takes the
  registration over silently: commands there print one note, and watching it
  needs `reviewloop project watch --replace`.
- **One supervisor per state dir.** A second `daemon run` on the same state
  directory (or database) is refused; a crashed one is replaced at once.
- **One budget for the machine.** `core.max_submissions_per_tick` and
  `core.max_concurrency` are shared by every project and both providers, so
  watching more projects never adds provider load; projects take turns, and
  one project's broken config or failing request never stops the others
  (`reviewloop daemon status` shows each project's last error).
- **Pause** (`reviewloop daemon pause`) is persistent: the service stays
  loaded but nothing is submitted, polled or triggered until `reviewloop
  daemon resume`, across restarts.
- **Config changes** apply on the next tick; a project whose config stops
  loading is skipped and reported until fixed.
- **No supervisor?** Commands still work; `submit`, `status`, `approve` and
  `retry` say whether anything will move the job without you, instead of
  reporting a queued job as submitted. `reviewloop run` then checks its own
  job by its schedule.
- The shared SQLite database (`~/.review_loop/reviewloop.db` by default)
  holds every project's jobs; the menu bar app (`reviewloop-bar`) and the
  widget show all of them.

Upgrading from a single-project install: the old launchd service passes
`--config <project>`; the supervisor keeps that project watched (unless you
unwatched it since) and watches nothing else. Run `reviewloop daemon install`
once to rewrite the service.

## Installation

Version numbering restarts at **0.0.1**. Earlier 0.1.x and 0.2.x releases remain
historical releases. See the [release notes](docs/releases/0.0.1.md) for scope
and upgrade details.
The version reset does not reset databases or review history.

### Homebrew (recommended on macOS)

```bash
brew tap acture/ac
brew install reviewloop
```

Upgrade with `brew update && brew upgrade reviewloop` or
`reviewloop self-update --method brew --yes`. The formula uses `version_scheme 1`
so Homebrew recognizes the reset as an upgrade from historical 0.2.x versions.

### Cargo

```bash
cargo install reviewloop --version 0.0.1 --locked
```

Upgrade:

```bash
cargo install reviewloop --version 0.0.1 --locked --force
```

Pin the version during the reset: Cargo's default version selection still picks
the numerically higher historical release. Starting with 0.0.1,
`reviewloop self-update --method cargo --yes` instead reads GitHub's latest release
and installs that exact version. Existing installations need the explicit command
above once to get the new updater.

### Build From Source

```bash
git clone https://github.com/Acture/reviewloop.git
cd reviewloop
cargo build --release
./target/release/reviewloop --help
```

### Menu bar companion (optional, macOS)

`reviewloop-bar` is a menu-bar app that surfaces the current state of
your active jobs without keeping a terminal open. It is read-only
against the same SQLite database the daemon writes to and triggers
actions by spawning `reviewloop` subcommands.

Build and install separately:

```bash
cargo install --path . --bin reviewloop-bar --features bar
```

Run:

```bash
reviewloop-bar &
```

**v2 capabilities:**

- **Per-job submenus** — each active job (QUEUED / SUBMITTED /
  PROCESSING) gets its own submenu showing `paper_id · STATUS ·
  attempt=N · in Xs` with three actions: *Retry now*, *Open
  artifacts*, *Open log*.
- **Submit new…** — opens a native PDF file picker and spawns
  `reviewloop run <path>` in the background.
- **Pause / Resume daemon** — shells out to `reviewloop daemon pause`
  / `reviewloop daemon resume`, following the supervisor's persistent
  pause flag (Resume also reloads an unloaded launchd service).
- **Open Artifacts Folder** and **Open Daemon Log** — cross-platform
  (`open` / `xdg-open` / `explorer`).
- Menu is rebuilt every 5 s so the job list stays current without
  restarting the bar.

The bar is opt-in (gated behind the `bar` Cargo feature) so headless
servers and CI continue to build the standard `reviewloop` binary
without the GUI dependencies.

> **Note:** The menu bar companion has no automated integration tests of
> its GUI. Manual smoke-testing on macOS is the verification path; build it
> with `cargo test --features bar --bin reviewloop-bar` for its unit tests.

## CSPaper browser companion

[CSPaper Review Manager](https://github.com/Acture/cspaper-review-manager) maintains
review records on the logged-in CSPaper website. One TypeScript source builds a
Chromium extension and a userscript, with selection, sequential deletion,
per-record outcomes and coordination across tabs. It runs independently of the
Rust CLI; CSPaper review submission is a separate backend integration.

## Command Surface

Global usage:

```bash
reviewloop [--config /path/to/override.toml] <command>
```

Core commands:

```bash
reviewloop init
reviewloop init project --project-id <id> [--project-root <path>] [--force]
reviewloop run <pdf-path> [--paper-id <id>] [--backend <backend>] [--no-watch] [--tag-trigger "<pattern>"] [--quiet]
reviewloop paper add --paper-id <id> --path <pdf-or-build-artifact> [--backend <backend>] [--venue <venue>] [--no-watch] [--tag-trigger "<pattern>"] [--submit-now] [--no-submit-prompt]
reviewloop paper watch --paper-id <id>
reviewloop paper unwatch --paper-id <id>
reviewloop paper remove --paper-id <id> [--purge-history]
reviewloop project list [--json]
reviewloop project watch [--project-id <id>] [--replace]
reviewloop project unwatch [--project-id <id>]
reviewloop daemon run [--panel false]
reviewloop daemon install [--start true]
reviewloop daemon uninstall
reviewloop daemon status [--json]
reviewloop daemon pause
reviewloop daemon resume
reviewloop cancel (--job-id <job-id> | --paper-id <id>) [--reason <text>]
reviewloop submit --paper-id main [--force] [--request-key <key>]
reviewloop approve --job-id <job-id>
reviewloop import-token (--paper-id main | --job-id <job-id>) --token <token> [--source email]
reviewloop check [--job-id <job-id> | --paper-id <paper-id>] [--all-processing]
reviewloop status [--paper-id main] [--json] [--show-token] [--active]
reviewloop retry (--job-id <job-id> | --paper-id <id> [--include-failed]) [--now]
reviewloop retry --all-failed
reviewloop complete --job-id <job-id> [--summary-text <text> | --summary-url <url> | --empty-summary] [--score <value>]
reviewloop config init
reviewloop config init project --project-id <id> [--project-root <path>] [--force]
reviewloop config migrate-project --project-id <id> [--project-root <path>]
reviewloop email login --provider google
reviewloop email status
reviewloop cspaper usage [--json]
reviewloop email switch --account <account-id-or-email>
reviewloop email logout [--account <account-id-or-email>]
reviewloop self-update [--method auto|brew|cargo] [--yes] [--dry-run]
reviewloop mcp [--approval required|granted]
```

`paper watch` / `paper unwatch` turn PDF-change watching on or off for one
paper. On each pass the PDF trigger requests a review of a watched paper whose
PDF differs from the PDF of its latest review request, or that has none
(awaiting `approve` unless `trigger.pdf.auto_submit_on_change`): watching a
paper, or watching it again after editing its PDF, can request the current
file on the next pass. Papers are watched when registered; `--no-watch` on
`paper add` or `run` registers one unwatched. That takes effect only while the
project's PDF trigger is on (`trigger.pdf.enabled`, the default) and the
supervisor runs the project, which `project watch` / `project unwatch` decide;
`paper watch` says when either is off. Unwatching a paper leaves its git tags
and explicit commands (`submit`, `run`) working.

`paper add --agent-id <agent-id>` is an alias of `--venue`: for a `cspaper`
paper the venue is the CSPaper review template (see [CSPaper Backend](#cspaper-backend)).

`submit` does not enqueue a second job when one that is pending, in flight or
completed already covers the same manuscript bytes, backend, venue (for
CSPaper, the review template), review options (CSPaper's
`desk_rejection_enabled`) and version (the git commit for tag-triggered jobs,
otherwise the manuscript hash); it prints that job instead. `--force` asks for
a new review round regardless.
`--request-key <key>` makes the request idempotent: repeating it with the same
key returns the job it first resolved to, even after that job has finished
(until retention prunes it), and reusing the key for a different manuscript,
venue or review options fails with a conflict naming the existing job. Use a
new key for each new review round.

`retry` re-queues a failed or stalled job. By default the job joins the end of
the queue and the supervisor submits it in turn, within the machine's
submission budget; a job the provider already accepted (it has a receipt
token) is polled again on the polling schedule instead, its review timeout
restarted. `--now` attempts the submission or poll once, right away, in the
`retry` process itself, clearing any cooldown; it polls only a `PROCESSING`
job, so retry a `TIMEOUT` or `FAILED` job that has a receipt token without it.
`approve` also puts a job at the end of the queue.
`retry --all-failed` retries, oldest first, every `FAILED` or `TIMEOUT` job of
the current project that is the newest job of its paper and backend, each the
default way: jobs without a receipt token join the end of the queue in their
order, and jobs with one (every `TIMEOUT` job) are polled again on schedule.
It lists the jobs it skips with the reason: cancelled, `FAILED_NEEDS_MANUAL`
(its last error says how to recover: fix the cause and `retry --job-id`, or
request a new review with `submit --force`), or superseded by a newer job of
the same paper and backend, which it names.

`self-update` only replaces the executable. It does not delete:
- global config (`~/.config/reviewloop/config.toml`)
- global data directory (database, artifacts, logs)
- project-local configs

## Agent Integration (MCP)

`reviewloop mcp` is a standard stdio [Model Context Protocol](https://modelcontextprotocol.io)
server, so an AI agent (Claude Code, Codex, Claude Desktop, VS Code, GitHub Copilot, …)
can list papers, request reviews, follow jobs and read reviews as structured
results:

```bash
claude mcp add reviewloop -- reviewloop mcp   # Claude Code
codex mcp add reviewloop -- reviewloop mcp    # Codex
```

Clients configured with an `mcpServers` map (Claude Desktop, Cursor, GitHub
Copilot CLI):

```json
{ "mcpServers": { "reviewloop": { "command": "reviewloop", "args": ["mcp"] } } }
```

VS Code (`.vscode/mcp.json`):

```json
{ "servers": { "reviewloop": { "type": "stdio", "command": "reviewloop", "args": ["mcp"] } } }
```

- Calls act on the project of the server's working directory (or
  `reviewloop --config <path> mcp`); every tool except `list_projects` also
  takes a `project_id` from `list_projects`.
- Agent requests wait in `PENDING_APPROVAL` until `approve_job`, so sending a
  manuscript to the provider is a separate, visible step. A retry queues a job
  that was approved and then failed, timed out or stalled again directly; a
  cancelled or hand-completed job waits for `approve_job` again. Start the
  server with `--approval granted` to queue requests at once.
- Tools only read and change local state; the supervisor (`reviewloop project
  watch`, `reviewloop daemon install`) submits and polls. Failures come back
  as tool errors with a stable `code` and a `recovery` hint. Tokens are never
  returned.

The tools, their schemas and errors are specified in
[docs/review-operations.md](docs/review-operations.md#mcp-server).

### Agent skill

[`integrations/reviewloop/skills/reviewloop/`](integrations/reviewloop/skills/reviewloop/SKILL.md)
is the shared [Agent Skill](https://agentskills.io) for Codex, Claude Code
and other hosts that load skills. It tells the agent when and how to use the
tools: one request per manuscript version with a request key, the user's
consent before a manuscript is sent, checks paced by `next_poll_at`, stalled
jobs and the worker fix, resuming a job in a later session, submissions with
an unknown outcome, saying which manuscript version a review covers, and a
CLI fallback. Until host plugins package it, copy or symlink the directory
into the host's skills directory, such as `~/.claude/skills/reviewloop`
(Claude Code) or `~/.agents/skills/reviewloop` (Codex).
`cargo test --test agent_skill` checks it against the live tool schemas and
the CLI; it does not run an agent.

## Exit Codes

- `reviewloop run`: 0 = Completed, 2 = terminal failure or submission outcome unknown (needs reconciliation), 130 = Ctrl+C
- `reviewloop import-token`: 0 = token attached + poll success, 2 = poll resolved to failure
- All other commands: 0 = success, 1 = error

## Runtime Model

Supervisor tick interval: 30 seconds after the previous tick ends (pause,
resume, watch and unwatch wake it within 2 seconds).

Each tick reloads the global config and every watched project's config, then:
1. Per watched project, in turn: trigger scan (`git tags`, PDF hash changes),
   timeout marking, lease recovery (claims left behind by a crashed or killed
   worker)
2. Optional Gmail OAuth + IMAP token ingestion, once for the machine
   (tokens are bound to the job that holds them in any project)
3. Submission processing (`QUEUED -> SUBMITTED -> PROCESSING`), up to
   `core.max_submissions_per_tick` for the whole machine, one project per turn
4. Poll processing (`PROCESSING -> COMPLETED/FAILED/...`), up to
   `core.max_concurrency` for the whole machine, one project per turn
5. Retention pruning (every `retention.prune_every_ticks` ticks), the widget
   snapshot, and each project's health

Provider calls are sequential. A step or job that fails fails only its
project's pass; the error is in `reviewloop daemon status`.

### Job ownership and uncertain submissions

Every worker path — daemon tick, `submit`, `run`, `retry`, `check`,
`import-token` — first takes a time-limited lease on the job in one SQLite
transaction, so two processes never submit or poll the same job at once.
State changes are written only while the lease is still held, together
with their event; a result that arrives after the lease expired or was
revoked (cancel, `retry`, `complete`, an imported token) is rejected.

The submit stage is persisted before anything is sent:

- `QUEUED` + `CLAIMED`: a worker owns the job, nothing has been sent. If
  the worker dies here, the claim expires and the job is picked up again.
- `SUBMITTED` + `DISPATCHED`: the request may be on the wire.
- `SUBMITTED` + `UNCERTAIN`: the provider may have accepted the paper, but
  no receipt was saved (crash or timeout after sending; for Stanford an
  unreadable or 5xx `confirm-upload` response, for CSPaper an unreadable,
  5xx or `job_id`-less answer to the review request). ReviewLoop does
  **not** resubmit these or switch to the fallback, because neither
  provider gives an idempotency guarantee. `reviewloop status` shows the
  reason; settle it by letting email ingestion attach the token (Stanford
  only), running `reviewloop import-token --job-id <id> --token <token>`,
  resubmitting explicitly with `reviewloop retry --job-id <id> --now`, or
  `reviewloop cancel --job-id <id>`. CSPaper sends no email; see
  [Reconciling an uncertain CSPaper submission](#reconciling-an-uncertain-cspaper-submission).

Cancelling is local: it stops further processing and marks the job
`FAILED`, but does not withdraw a submission the provider already
received. A receipt that arrives after the cancel is kept on the job
(and in the `submit_receipt_after_lease_lost` event) for recovery.

Manual immediate poll:
- `reviewloop check --job-id <id>` forces one check now for that processing job (ignores `next_poll_at`)
- `reviewloop check --paper-id <paper-id>` checks the latest processing job for that paper
- `reviewloop check --all-processing` checks all current processing jobs

Output artifacts per completed job:
- `<state_dir>/artifacts/<job-id>/review.json`
- `<state_dir>/artifacts/<job-id>/review.md`
- `<state_dir>/artifacts/<job-id>/meta.json`

Each job also pins the exact PDF it uploads at `<state_dir>/snapshots/<sha256>/<file name>`, copied at enqueue; `meta.json` records it as `snapshot_path`.

## What Makes It Reliable

- **State machine, not ad-hoc scripts**: jobs move through explicit statuses (`PENDING_APPROVAL`, `QUEUED`, `PROCESSING`, `COMPLETED`, etc.)
- **Duplicate guard**: prevents repeated submissions for the same `project_id + paper_id + backend + pdf_hash + venue + review_options + version_key`
- **Load-aware polling**: default schedule starts at 10 minutes with jitter/cooldown behavior
- **Recovery built in**: every transition is evented, retries are explicit
- **Fallback path**: optional Node + Playwright submit path when the Stanford API flow fails

## Triggering Modes

### Git tag trigger

Supported patterns:
- `review-<backend>/<paper-id>/<anything>`
- `review-<backend>/<anything>` (uses the first configured paper of that backend)
- optional per-paper custom pattern via `paper add --tag-trigger "<pattern>"` (supports `*`)

Example:

```text
review-stanford/main/v1
```

### PDF change trigger

- Computes SHA256 for configured PDFs
- New hash enqueues job
- Default status is `PENDING_APPROVAL` (manual `approve` required)

## Email Token Ingestion (Experimental, opt-in)

ReviewLoop can attach review tokens from email to open jobs. Both
ingestion paths default to **disabled** because the regex / header
matching is heuristic and noisy when the inbox does not contain the
expected `paperreview.ai` mail. The Stanford backend already returns
the token directly from `confirm-upload`, so this path is mostly
useful as a backup for the Playwright fallback flow or for jobs
created out-of-band.

Email ingestion is Stanford-only. CSPaper sends no review email (its job id
arrives only in the submit response), so a token from mail is never attached
to a `cspaper` job, even when a pattern is configured under that name.

To turn either path on, set `enabled = true` explicitly in your config.

### IMAP mode (built in)

Default token pattern includes Stanford:

```toml
[imap]
enabled = true  # opt-in; default is false

[imap.backend_patterns]
stanford = "https?://paperreview\\.ai/review\\?token=([A-Za-z0-9_-]+)"
```

Recommended defaults:
- `imap.header_first = true` to scan headers first
- `imap.max_lookback_hours = 72`
- `imap.max_messages_per_poll = 50`

### Gmail OAuth mode

Configure:

```toml
[gmail_oauth]
enabled = true  # opt-in; default is false
client_id = "your-google-oauth-client-id"
client_secret = "your-google-oauth-client-secret"
token_store_path = "~/.review_loop/oauth/google_token.json" # optional
poll_seconds = 300
mark_seen = true
max_lookback_hours = 72
max_messages_per_poll = 50
header_first = true

[gmail_oauth.backend_header_patterns]
stanford = "(?is)(from:\\s*.*mail\\.paperreview\\.ai|subject:\\s*.*paper review is ready)"

[gmail_oauth.backend_patterns]
stanford = "https?://paperreview\\.ai/review\\?token=([A-Za-z0-9_-]+)"
```

You can also provide credentials via environment variables:
- `REVIEWLOOP_GMAIL_CLIENT_ID`
- `REVIEWLOOP_GMAIL_CLIENT_SECRET`

Credentials are resolved at **runtime only** (env var → `config.toml` field). They are
**not** baked into the binary at compile time, so every deployment must supply them via
one of the two mechanisms above. The old CI compile-time injection pattern
(`option_env!`) has been removed to prevent secrets from being embedded in binaries.

Then login:

```bash
reviewloop email login --provider google
```

`email login` will try to open your default browser automatically and wait in CLI for OAuth completion.
The daemon refreshes Gmail OAuth tokens when they are within five minutes of expiry.

ReviewLoop runs Gmail API polling first when available, then IMAP fallback.

## Configuration Highlights

### Proxy pool

Outbound HTTP requests (PDF upload, review fetch, Gmail API) can be routed
through a list of user-configured HTTP / SOCKS proxies. ReviewLoop uses
[`reqwest-middleware`](https://crates.io/crates/reqwest-middleware) for the
middleware framework; the rotation logic itself is a small in-house
middleware (∼90 lines) that does:

- **Round-robin** across the configured proxy URLs using an atomic counter,
  so concurrent requests spread across the pool.
- **Sequential failover** on transient connection errors: when a proxy
  refuses the connection, times out, or fails the TLS handshake, the
  request is retried against the next proxy in the rotation. HTTP
  responses (any 4xx / 5xx that completes a round-trip) are returned as
  the upstream service answered — the proxy is healthy, the upstream said
  no. Multipart uploads (the PDF upload and `confirm-upload`, which creates
  the submission) are streamed and sent exactly once through one proxy;
  they are never re-sent on failover.

> **Note on library choice**: [`reqwest-proxy-pool`](https://crates.io/crates/reqwest-proxy-pool)
> 0.4 was evaluated and found unsuitable: it supports only SOCKS5/SOCKS5H
> (no HTTP proxy) and only fetches its proxy list from remote URLs (no API
> for a user-supplied static list). The custom middleware avoids both
> limitations. Migration to upstream when it gains HTTP + static-list
> support is tracked separately.

Configure in global config:
```toml
[core]
proxies = [
    "http://user:pass@proxy1.example.com:8080",
    "socks5://user:pass@proxy2.example.com:1080",
]
```

Or per-project (overrides global, does not merge):
```toml
[core]
proxies = ["http://special-proxy.example.com:8080"]
```

Empty list (default) disables proxy routing — direct connections used.
Credentials embedded in proxy URLs are never written to logs; only the count
is reported.

**Tip — using Clash / Mihomo:** if you already run Clash locally, just point
ReviewLoop at its HTTP listener:

```toml
[core]
proxies = ["http://127.0.0.1:7890"]
```

Clash itself handles subscription URLs, real proxy rotation, health-check,
and protocol translation (VMess / Trojan / SS / etc.). ReviewLoop treats it
as a single stable upstream HTTP proxy.

**Limitations:**
- Bodies that cannot be cloned (streamed uploads from a file handle) fall
  back to a single-attempt path with no failover. The current PDF upload
  reads the file into memory before constructing the request body, so
  failover applies. Future streaming-upload paths would not.
- The OAuth2 token-exchange flow (`reviewloop email login --provider
  google`) uses only the **first** proxy in the list, because the `oauth2`
  crate requires a bare `reqwest::Client`. This affects only the initial
  one-time login; subsequent token refreshes go through the full pool.
- No active health-check probe / cooldown for known-bad proxies.
  Failover is per-request (next request again starts at round-robin
  position N+1 — a dead proxy is skipped at the moment of use, not
  blacklisted). Acceptable for small static lists; for large pools
  consider a managed service or Clash upstream.


ReviewLoop uses two config files with separate responsibilities:
- global config: `$XDG_CONFIG_HOME/reviewloop/config.toml` or `~/.config/reviewloop/config.toml`
- project config: `<repo-root>/reviewloop.toml`

There is no global-overrides-project merge chain. Instead:
- global config owns machine/user concerns such as `core.*`, `logging.*`, `polling.*`, `retention.*`, `imap.*`, `gmail_oauth.*`, Stanford provider connection defaults, and the CSPaper connection (`providers.cspaper.base_url`, `providers.cspaper.cookie` and `providers.cspaper.api_key`, which only the global config accepts)
- project config owns repo concerns such as `project_id`, `papers`, `paper_watch`, `paper_tag_triggers`, `trigger.*`, Stanford venue, and the CSPaper review choices (`providers.cspaper.agent_id`, `providers.cspaper.model` and `providers.cspaper.desk_rejection_enabled`, which override the global defaults)
- `--config /path/to/reviewloop.toml` explicitly points to a project config file
- `reviewloop init` initializes the global config/data paths
- `reviewloop init project --project-id <id>` initializes the current repo's project config
- `reviewloop daemon install` installs the one machine-wide supervisor; `reviewloop project watch` (from the repo, or `--config`) chooses the projects it runs
- a project config cannot set machine-wide settings (`core.db_path`, `core.state_dir`, the provider budget, `[polling]`, `[retention]`, `[logging]`, `[imap]`, `[gmail_oauth]`): such a file is refused by name

Project commands require a non-empty `project_id` in the project config. Jobs, events, dedupe, and status views are isolated inside the shared global DB by `project_id`.

Paper registration:
- start with an empty `papers[]`
- add papers through `reviewloop paper add ...`
- remove papers through `reviewloop paper remove --paper-id ...`
  - add `--purge-history` to also delete DB jobs/events/reviews and local artifacts for that paper
- turn PDF-change watching per paper on or off with `reviewloop paper watch --paper-id ...` / `reviewloop paper unwatch --paper-id ...` (stored in `paper_watch`)

Safe defaults:
- `core.max_concurrency = 2` (polls per tick, for the whole machine)
- `core.max_submissions_per_tick = 1` (submissions per tick, for the whole machine)
- `core.state_dir = "~/.review_loop"` (or `REVIEWLOOP_STATE_DIR` when set)
- `core.db_path = "~/.review_loop/reviewloop.db"` (or `<REVIEWLOOP_STATE_DIR>/reviewloop.db`)
- `core.review_timeout_hours = 48` (every job and backend, whatever its page count: provider processing time follows its load)
- `polling.schedule_minutes = [1, 2, 5, 10, 20, 40]` (first poll within ~1 minute, then back off)
- `polling.jitter_percent = 10`
- `retention.enabled = true`
- `retention.prune_every_ticks = 20` (10 minutes with 30s tick)
- `retention.email_tokens_days = 30`
- `retention.seen_tags_days = 90`
- `retention.events_days = 30`
- `retention.terminal_jobs_days = 0` (disabled by default)
- `trigger.pdf.auto_submit_on_change = false`
- `trigger.pdf.max_scan_papers = 10`
- `trigger.git.tag_pattern = "review-<backend>/<paper-id>/*"`
- `trigger.git.auto_create_tags_on_pdf_change = false`
- `trigger.git.auto_delete_processed_tags = false`

`providers.stanford` defaults:
- `base_url = "https://paperreview.ai"`
- `fallback_mode = "node_playwright"`
- `fallback_script = "tools/paperreview_fallback.mjs"`
- `email` optional (falls back to active email account)
- `venue = "ICLR"` (project config)

`providers.cspaper` defaults (see [CSPaper Backend](#cspaper-backend)):
- `base_url = "https://cspaper.org"` (global config only)
- `cookie` unset (global config only, or `REVIEWLOOP_CSPAPER_COOKIE`); preferred for new jobs
- `model` unset; selected with `reviewloop cspaper select`
- `api_key` unset (global config only, or `REVIEWLOOP_CSPAPER_API_KEY`)
- `agent_id` unset: there is no built-in review template
- `desk_rejection_enabled = true`

Logging:
- `logging.output = "stdout" | "stderr" | "file"`
- file mode default path: `<state_dir>/reviewloop.log`

## CSPaper Backend

`backend = "cspaper"` supports a personal website session using a Cookie,
and an optional organisation API key. Login requires no review template or
agent ID. A configured Cookie takes priority for new jobs; each job pins
its transport, template and model, and never falls back to another transport.

### Personal website session (Cookie first)

Log into <https://cspaper.org> in your browser. In DevTools → Network, reload
the Reviews page and copy the **Cookie request header value** of its
`/api/review/list` request into a local private file outside the repository.
Use the complete value (including chunked session cookies), not `Set-Cookie`
or an exported browser cookie database. Never put it in chat or a command argument.

```sh
reviewloop cspaper login --cookie-file /path/to/private/cspaper-cookie.txt
reviewloop cspaper auth-status
reviewloop cspaper agents
reviewloop cspaper select --agent ARD-Review
reviewloop paper add --paper-id main --path paper/main.pdf --backend cspaper
reviewloop submit --paper-id main
```

`login` checks the session with a read-only list request, then saves it in
the global config with mode `0600`. It does not submit a review or require
an API key, agent ID or model. `auth-status` is also read-only. The session
is not refreshed automatically: repeat login with a fresh Cookie when it expires.
`REVIEWLOOP_CSPAPER_COOKIE` fills an unset Cookie for the current process;
the installed supervisor needs the saved global value.

`agents` lists personal review templates and models from the live catalogue.
`select` accepts a unique name, abbreviation or exact ID. It selects the
sole model automatically; otherwise pass `--model vendor/model`. The
selection is saved as the machine default; project `providers.cspaper.model`
(`vendor` and `model`) and `agent_id`, and a paper's `venue`, may override it.
No dated template or model is built in. `select` does not submit anything.

The web adapter uploads the pinned PDF to `/api/review/register/pdf` and
polls `/api/review/<id>`. Its receipt is stored as `web:<id>`, so restarting
or changing credentials cannot turn a website job into an organisation job.
Cookie headers stay out of project files, jobs and archives. Templates that
do not support disabling desk rejection reject that choice before upload.
Web scores retain the provider's template-specific scale.

The website contract and validation boundary are recorded in
[CSPaper web transport](docs/providers/cspaper-web.md). Website endpoints
are not a published stable API. ReviewLoop does not automate browser login.

### Optional organisation API

With no Cookie configured, `backend = "cspaper"` uses the
[CSPaper Agentic Review](https://cspaper.org/platform/review) API. ReviewLoop
uploads the job's pinned PDF snapshot with `POST /api/platform/review`, keeps
the `job_id` CSPaper answers with as the job's token (so polling resumes after
a restart), and polls `GET /api/platform/reviews/<job_id>` until the review is
`COMPLETED` or `FAILED`.

#### API configuration

The API key and the base URL are machine settings and live only in the global
config:

```toml
# ~/.config/reviewloop/config.toml
[providers.cspaper]
api_key = "csp_live_..."            # or export REVIEWLOOP_CSPAPER_API_KEY
# base_url = "https://cspaper.org"  # the default
agent_id = "ICLR_main_2026_1"       # optional machine-wide default template
desk_rejection_enabled = true       # the default
```

- `api_key`: your organisation's API key. When it is unset or blank,
  `REVIEWLOOP_CSPAPER_API_KEY` is used; when both are set, the config value
  wins. It is sent only in the `X-API-Key` header and kept out of job rows,
  events, job errors and archives. The launchd supervisor does not inherit
  your shell's environment, so it needs the key in the global config;
  `reviewloop daemon install` warns, naming the watched projects that use
  CSPaper, when it is missing there.
- `base_url`: defaults to `https://cspaper.org`. It must be `https://` with a
  host (plain `http://` only for `localhost`, `127.0.0.1` or `[::1]`).
  ReviewLoop calls the live `/api/platform/...` paths, not the
  `/api/v1/platform/...` paths shown in CSPaper's examples README (on the live
  service those redirect to a sign-in page). Redirects are never followed, so
  the key cannot travel to another host.
- `agent_id`: the review template, e.g. `ICLR_main_2026_1`; CSPaper lists them
  at <https://cspaper.org/platform/review>. There is no built-in default,
  because the template decides what the review means. A paper's own `venue`
  (`paper add --agent-id`) overrides the project `agent_id`, which overrides
  the global one.
- `desk_rejection_enabled`: CSPaper's desk-rejection screening (topic fit,
  minimum quality, prompt injection) before the review; default `true`.
  `false` always yields a full, scored review. A project value overrides the
  global one.
- `monthly_allowance`: optional, global config only. Reviews per billing month
  the organisation expects to use (e.g. the volume agreed with CSPaper);
  `reviewloop cspaper usage` reports what is left of it.

The project file holds only the review choices:

```toml
# <repo>/reviewloop.toml
project_id = "main"
default_backend = "cspaper"   # optional: papers without a backend use cspaper

[providers.cspaper]
agent_id = "ICLR_main_2026_1"
desk_rejection_enabled = false
```

The key never goes in `reviewloop.toml`: a project file with an `api_key`, or
with `csp_live_` text anywhere (even in a comment), is refused before it is
parsed, and a `base_url` under `[providers.cspaper]` is a parse error. Config
errors name the line but never print it or the value. Keep the key in the
global config or the environment.

```bash
reviewloop paper add --paper-id main --path paper/main.pdf \
  --backend cspaper --agent-id ICLR_main_2026_1
reviewloop submit --paper-id main
```

`submit`, `run` and `paper add --submit-now` refuse a CSPaper paper up front,
queueing nothing, when no credential, template, or required web model is configured (error code
`provider_not_configured`). Git tag and PDF change triggers skip such a paper
and record one `provider_not_configured` event per paper and setting; a
skipped tag stays unprocessed, so it enqueues once the setting exists.

Unlike the Stanford backend, CSPaper needs no submitter email
(`providers.stanford.email` and `email login` are not used), has no browser
fallback, and has no page-scaled timeout: a `PROCESSING` job times out after
the flat `core.review_timeout_hours`. `reviewloop run` does not wait for a
token email on an uncertain CSPaper submission; it stops at once with exit
code 2.

### Usage

`cspaper usage` reports the organisation API account and requires its API
key. Its monthly allowance is not a personal website credit balance.
The separate local count includes both transports (`local_scope` in JSON);
only the organisation's remote list is used for its monthly allowance.

CSPaper bills organisation API keys by usage, not with credits: it sets the
platform account up by hand and invoices monthly for the reviews actually run,
at a rate scoped to the agreed volume ([Platform APIs](https://cspaper.org/articles/platform-apis)).
Its web Usage page needs a browser login, and no API returns a quota or
balance, so reviewloop counts reviews per billing month (UTC) itself.

`reviewloop cspaper usage [--month YYYY-MM] [--json]` reads CSPaper's job list
for the organisation with the API key (read-only, not usage) and splits the
month's reviews into those this machine's reviewloop tracks (matched by
CSPaper job id: submitted, or attached with `import-token`) and those from
other clients such as the web playground or other tools:

```text
CSPaper organisation reviews in 2026-10 (UTC), all clients: 12 (9 completed, 1 in progress, 2 failed)
  via reviewloop on this machine (submitted or imported): 7
  via other clients: 5
  monthly allowance 50, 38 left
All months: 140 reviews, 96 via reviewloop on this machine
CSPaper reviews from this machine in 2026-10: 6 completed, 1 in progress
```

"Left" needs `monthly_allowance` under `[providers.cspaper]` in the global
config, e.g. the volume agreed with CSPaper; without it the report says none
is set. Failed reviews are counted too, since whether CSPaper bills them is
not published. The month comes from each job's `created` time; a report notes
jobs without one, and a job list that could not be read to the end.

After a CSPaper submission, `submit` and `run` print the last line above (this
machine's reviews this month, from the local database across projects), and
`reviewloop status` repeats it for a project with CSPaper papers. It splits
reviews into completed, in progress and ended without a review, and lists
uncertain submissions separately: jobs parked `UNCERTAIN`, and jobs whose
submission outcome was unknown before they were cancelled (also while their
upload was in flight) or resubmitted with `retry`. The latter are known
from job events, kept for `retention.events_days` (default 30).

### Errors and outcomes

Submitting:

| CSPaper answer | Job becomes | What to do |
|---|---|---|
| No API key configured, or 401 / 403 | `FAILED_NEEDS_MANUAL`, with a notification | Nothing was created. Fix the key, then `reviewloop retry --job-id <id>`. |
| 400 (unknown template), 422 (incomplete request), any other 4xx; no template on the job; a file that is not a PDF | `FAILED` | Nothing was created. Fix the template or file, then request a new review with `reviewloop submit --paper-id <paper>`: `retry` resends what the job recorded. |
| 429 | `QUEUED`, retried after `Retry-After` (capped at 24 h), else on the polling schedule | Nothing. This assumes CSPaper throttles before creating a job; its documentation does not say. |
| Connection never established (including a TCP, proxy or TLS handshake still stalled after 30 s) | `FAILED` | Nothing reached CSPaper; safe to retry. |
| 5xx, 301 / 302 / 303, a lost or unreadable response, no answer within 20 minutes, or a 2xx receipt without a usable `job_id` | `SUBMITTED` + `UNCERTAIN`, with a notification; never resent | CSPaper may hold the review: [reconcile it](#reconciling-an-uncertain-cspaper-submission). |
| Any other 3xx (307 / 308 ask for the request to be resent elsewhere) | `FAILED` | The base URL points at the wrong host or path; check `providers.cspaper.base_url`. |

Polling:

| CSPaper answer | Job becomes | What to do |
|---|---|---|
| `PENDING` / `PROCESSING` | stays `PROCESSING` | Nothing. |
| `COMPLETED` | `COMPLETED`, artifacts written | Read the review. |
| `FAILED` | `FAILED_NEEDS_MANUAL` with CSPaper's `failed_reason`, and a notification | Polling again gives the same answer, so `retry` does not help: request a new review with `reviewloop submit --paper-id <paper> --force` (`--force` because an earlier review of the same manuscript would otherwise be returned instead). |
| 404 / 410 (unknown job, or one owned by another organisation's key) | `FAILED` (`invalid token`) | Check the job id and the key's organisation. A `FAILED` job cannot take a new token, so attach the right one to a new job: `reviewloop import-token --paper-id <paper> --token <cspaper job_id>`. |
| 401 / 403, 429, 5xx, or an unexpected payload | stays `PROCESSING`, polled again on the schedule (429: after `Retry-After`) | Fix the key if it was refused; the job times out after `core.review_timeout_hours` otherwise, and `reviewloop retry --job-id <job>` then polls it again with the timeout restarted. |

### Reconciling an uncertain CSPaper submission

CSPaper's submit request carries no idempotency key, so a submission whose
answer was lost is parked as `SUBMITTED` / `UNCERTAIN` and never resent
automatically. `reviewloop status` shows it, `reviewloop run` stops on it with
exit code 2, and no email will settle it. Settle it by hand:

1. Look for the paper in the CSPaper review list
   (<https://cspaper.org/reviews> for Cookie jobs;
   <https://cspaper.org/platform/review> for organisation jobs).
2. If it is there, attach its job id:
   `reviewloop import-token --job-id <id> --token web:<review-id>` for a Cookie
   job, or `--token <cspaper job_id>` for an organisation job.
   ReviewLoop polls it at once and continues normally.
3. If it is not there, resubmit with `reviewloop retry --job-id <id> --now`.
   This may create a duplicate review if CSPaper did receive the first one.
4. Or stop tracking it with `reviewloop cancel --job-id <id>`; a review
   CSPaper already created is not withdrawn.

### What is archived

For each completed review, `<state_dir>/artifacts/<job-id>/` holds:

Cookie reviews use the same three files: `review.json` includes `transport`
(`web`), `provider_job_id`, `title`, `venue`, `content`, `result_summary`,
`numerical_score` on its original scale, and the credential-scrubbed `provider_raw`.
The following field details describe organisation API reviews:

- `review.json`: the normalized review: `provider` (`"cspaper"`),
  `provider_job_id`, `title` (from `paper_meta.title`), `venue` and `agent_id`
  (the template), `finished_at`, `numerical_score`
  (`result_summary.overall_score`, else `mainScoreNorm`; numbers only),
  `desk_reject`, `result_summary` (decoded; kept as the raw string plus
  `result_summary_parse_error` when it is not valid JSON), `content` (the
  markdown review), and `provider_raw`, CSPaper's response verbatim.
- `review.md`: title, template and score, then the markdown review.
- `meta.json`: the job, paper and backend, `token` (the CSPaper job id), the
  template (`venue`), `review_options`, the manuscript version (`version_no`,
  `round_no`, `version_source`, `version_key`, `git_tag`, `git_commit`),
  `pdf_path`, `pdf_hash` and `snapshot_path`.

### Request identity

The template and the review options are part of a request's identity.
Switching `agent_id` or `desk_rejection_enabled` for an unchanged PDF asks for
a new review instead of returning the existing one, and replaying a
`--request-key` after changing either is a conflict. CSPaper jobs always
record `desk_rejection_enabled`, so leaving it unset and setting it to `true`
are the same request.
Cookie jobs also record `transport`, `model_vendor` and `model`; changing
these changes the request identity. Retrying uses the recorded choices.

### Limitations

- Uncertain submissions are not reconciled automatically.
- Organisation API templates are not checked before submission; an unknown one fails the job
  with CSPaper's 400.
- `core.max_concurrency` and `core.max_submissions_per_tick` are one budget
  for the machine, shared with Stanford jobs and every project, and the
  organisation key's quota is shared by every project that uses it.

## CI/CD and Release Flow

This repository ships with GitHub Actions for both quality gates and release automation.

### CI (`.github/workflows/ci.yml`)

On pull requests and pushes to `main/master`:
- `cargo fmt --all -- --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test --all-targets --locked`

Runs on both Ubuntu and macOS.

The same gate is shared locally via `./scripts/quality-gates.sh`.
To enable it in the standard `pre-commit` framework:
- `pre-commit install`
- commits will then run the repository-local `reviewloop quality gates` hook before creating the commit

### Release (`.github/workflows/release.yml`)

On tag push like `v0.0.1`:
1. Verify tag version matches `Cargo.toml`
2. Run quality gates again
3. Publish crate to crates.io
4. Update Homebrew tap formula in `Acture/homebrew-ac`
5. Create GitHub Release with generated notes

Required secrets:
- `CARGO_REGISTRY_TOKEN`
- `HOMEBREW_TAP_GITHUB_TOKEN`

Runtime secrets (must be provided via env or `config.toml` at runtime — not baked in at compile time):
- `REVIEWLOOP_GMAIL_CLIENT_ID`
- `REVIEWLOOP_GMAIL_CLIENT_SECRET`
- `REVIEWLOOP_CSPAPER_API_KEY` (CSPaper backend; `providers.cspaper.api_key` in the global config wins when both are set)
- `REVIEWLOOP_CSPAPER_COOKIE` (personal CSPaper web session; the saved global Cookie wins)

Optional repo variables:
- `HOMEBREW_TAP_REPO` (default: `Acture/homebrew-ac`)
- `HOMEBREW_FORMULA_PATH` (default: `packaging/homebrew/Formula/reviewloop.rb`)

## Fallback Requirements

The fallback exists for the Stanford backend only; CSPaper has none. It runs
only when the API submit failed in a way that proves the
provider did not accept the paper (an error before `confirm-upload`, or an
explicit rejection). An ambiguous failure marks the job `UNCERTAIN`
instead (see [Job ownership and uncertain submissions](#job-ownership-and-uncertain-submissions)).

When the fallback runs:
- Node.js must be available
- Playwright runtime dependencies must be installed
- script path defaults to `tools/paperreview_fallback.mjs`

The fallback receives `--base-url`, `--pdf`, `--filename`, `--email` and optionally
`--venue`: the same manuscript, upload name, email and venue as the API route.

A custom `fallback_script` prints one JSON line: `{"success": true, "token": "..."}`
on stdout, or on failure `{"success": false, "submitted": <bool>, "stage": "...",
"status": <http status>, "rate_limited": <bool>, "retry_after_secs": <n>, "rejected": <bool>,
"error": "..."}` on stderr with a non-zero exit. `rate_limited: true` requeues the job;
`"submitted": false` (confirm-upload never sent), or a 4xx `status` or `rejected: true`
reported for the `confirm` stage, is a definitive failure; any other failure is treated
as an unknown outcome. The full contract is in
[docs/providers/stanford.md](docs/providers/stanford.md#fallback).

## Responsible Use

ReviewLoop is intentionally conservative.

Please keep it that way:
- use it only for authorized submissions/retrieval
- keep concurrency and submit rate low unless provider approves otherwise
- do not aggressively shorten poll cadence
- respect provider Terms of Service and fair-use boundaries

## Current Scope

- Supported backends: `stanford` ([Stanford Agentic Reviewer](https://paperreview.ai), `paperreview.ai`)
  and `cspaper` (CSPaper Agentic Review, `cspaper.org`). The Stanford contract, limits
  (10 MiB, first 15 pages reviewed) and token handling are in
  [docs/providers/stanford.md](docs/providers/stanford.md)
- Database: SQLite (global state path by default, supports `:memory:`)
- Interface: CLI + daemon + stdio MCP server (`reviewloop mcp`)

## macOS Widget (preview)

The supervisor writes a small JSON status snapshot (`widget-state.json`) every
tick, covering every project on the machine (see
[docs/widget-schema.md](docs/widget-schema.md)). A separate macOS WidgetKit
extension reads that snapshot and renders a glance UI (active job count, recent
failures) in macOS desktop / Notification Center widgets.

**Platform**: macOS only. **Distribution**: opt-in via build — no signed binary is
distributed. You build the `.app` yourself with your Personal Team.

### Build & install

1. Install xcodegen: `brew install xcodegen`
2. `cd apple/ReviewLoopWidget && xcodegen generate`
3. Open `ReviewLoopWidget.xcodeproj` in Xcode 16+
4. Select your Personal Team for both `HostApp` and `Widget` targets in
   Signing & Capabilities
5. In both `.entitlements` files (`HostApp/HostApp.entitlements`,
   `Widget/Widget.entitlements`), change `group.ai.reviewloop.local` to
   `group.<your-bundle-prefix>.shared` (must match across both files)
6. Configure the daemon to write into the App Group container so the sandboxed
   widget can read it. Edit `~/.config/reviewloop/config.toml`:
   ```toml
   [core]
   widget_state_dir = "/Users/<you>/Library/Group Containers/group.<your-bundle-prefix>.shared"
   ```
7. In Xcode: ⌘R to build & launch the host app once. The host app is just a
   placeholder window; quit it.
8. Add the widget from the macOS desktop / Notification Center widget gallery
   (search for "ReviewLoop").

### Limitations

- Refresh ~5 minutes minimum (Apple WidgetKit budget); not a real-time dashboard.
- macOS 15+, Xcode 16+ required.
- You build the `.app` yourself with your Personal Team. No signed binary is
  distributed (~$99/yr Apple Developer fee not paid).
- Sandbox: the widget can only read the App Group container; you **must** configure
  `core.widget_state_dir` to match the App Group ID, or the widget will show
  "no data" indefinitely.
- Currently V1: small + medium widget sizes only; no Lock Screen /
  accessoryRectangular variants.

See [`apple/ReviewLoopWidget/README.md`](apple/ReviewLoopWidget/README.md) for
build details that may evolve.

## License

[GNU Affero General Public License, version 3 or any later version](LICENSE)
(`AGPL-3.0-or-later`).

## IMAP support

IMAP email ingestion is gated behind a Cargo feature and is **not compiled in by
default**. Default builds work without it — `reviewloop run` submits and polls
via the API directly.

To enable IMAP support:

```bash
cargo build --features imap
cargo install reviewloop --features imap
```

If `imap.enabled = true` appears in your config but the binary was built without
`--features imap`, a warning is logged at startup and IMAP polling is silently
skipped.

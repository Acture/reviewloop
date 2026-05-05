# ReviewLoop

[![CI](https://github.com/Acture/reviewloop/actions/workflows/ci.yml/badge.svg)](https://github.com/Acture/reviewloop/actions/workflows/ci.yml)
[![Release](https://github.com/Acture/reviewloop/actions/workflows/release.yml/badge.svg)](https://github.com/Acture/reviewloop/actions/workflows/release.yml)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/github/license/Acture/reviewloop)](LICENSE)

> A production-minded Rust CLI/daemon for `paperreview.ai` submission and review retrieval.

Most paper review automation breaks in boring ways: duplicate submissions, lost tokens, noisy polling, and zero traceability.

**ReviewLoop** gives you a durable loop with guardrails:
- Queue reviews from Git tags or PDF hash changes
- Persist every transition in SQLite
- Pull tokens from Gmail OAuth or IMAP
- Write reproducible artifacts (`review.json`, `review.md`, `meta.json`)
- Recover from failures with explicit retries and fallback submission

## Why This Project Exists

Reviewing pipelines are usually a pile of scripts plus cron plus hope.
ReviewLoop is built for the opposite:
- predictable state transitions
- low default provider pressure
- human approval gates where it matters
- clear local evidence of what happened and why

If you want reliable, low-drama automation for `paperreview.ai`, this is the tool.

## 1-Minute Quick Start

```bash
# 1) one-time machine setup
reviewloop init

# 2) for any repo, one-time project setup
reviewloop init project --project-id main

# 2.5) one-time: configure submitter email
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

Exit codes: `0` = review complete, `2` = terminal failure, `130` = Ctrl+C.

> **A submitter email is required.** Set `providers.stanford.email` in
> `~/.config/reviewloop/config.toml` (step 2.5 above) or run
> `reviewloop email login --provider google` to use OAuth.
> Email/OAuth is also needed if you submitted via the paperreview.ai website
> and want reviewloop to ingest tokens from your inbox.
> See [Optional: email token ingestion](#email-token-ingestion-experimental-opt-in) below.

Optional flags:

```bash
reviewloop run paper/main.pdf \
  --paper-id main \        # override the default (filename stem)
  --backend stanford \     # override the default backend
  --watch false \          # disable PDF-change watching for this paper
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

# install and start the background daemon (macOS)
reviewloop daemon install --start true
```

The daemon runs every 30 seconds, handles retries, token ingestion, and retention pruning
automatically. Use `reviewloop status` and `reviewloop check` to monitor it.

## Installation

### Homebrew (recommended on macOS)

```bash
# after public release
brew tap acture/ac
brew install reviewloop
```

Upgrade:

```bash
reviewloop self-update --yes
# or force Homebrew path
reviewloop self-update --method brew --yes
```

### Cargo

```bash
# after public release
cargo install reviewloop
```

Upgrade:

```bash
reviewloop self-update --yes
# or force Cargo path
reviewloop self-update --method cargo --yes
```

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
  / `reviewloop daemon resume` (macOS only; menu item is disabled on
  other platforms).
- **Open Artifacts Folder** and **Open Daemon Log** — cross-platform
  (`open` / `xdg-open` / `explorer`).
- Menu is rebuilt every 5 s so the job list stays current without
  restarting the bar.

The bar is opt-in (gated behind the `bar` Cargo feature) so headless
servers and CI continue to build the standard `reviewloop` binary
without the GUI dependencies.

> **Note:** The menu bar companion has no automated integration tests
> (it is GUI-bound). Manual smoke-testing on macOS is the verification
> path. Multi-project switching and "Retry Failed" enumeration are
> deferred to a future phase (they require new `Db` helpers).

## Command Surface

Global usage:

```bash
reviewloop [--config /path/to/override.toml] <command>
```

Core commands:

```bash
reviewloop init
reviewloop init project --project-id <id> [--project-root <path>] [--force]
reviewloop run <pdf-path> [--paper-id <id>] [--backend <backend>] [--watch true|false] [--tag-trigger "<pattern>"] [--quiet]
reviewloop paper add --paper-id <id> --path <pdf-or-build-artifact> [--backend <backend>] [--venue <venue>] [--watch true|false] [--tag-trigger "<pattern>"] [--submit-now] [--no-submit-prompt]
reviewloop paper watch --paper-id <id> --enabled <true|false>
reviewloop paper remove --paper-id <id> [--purge-history]
reviewloop daemon run
reviewloop daemon run --panel false
reviewloop daemon install [--start true]
reviewloop daemon uninstall
reviewloop daemon status
reviewloop submit --paper-id main [--force]
reviewloop approve --job-id <job-id>
reviewloop import-token --paper-id main --token <token> [--source email]
reviewloop check [--job-id <job-id> | --paper-id <paper-id>] [--all-processing]
reviewloop status [--paper-id main] [--json] [--show-token]
reviewloop retry --job-id <job-id> [--force]  # (was --override-rate-limit, deprecated since vNext)
reviewloop complete --job-id <job-id> [--summary-text <text> | --summary-url <url> | --empty-summary] [--score <value>]
reviewloop config init
reviewloop config init project --project-id <id> [--project-root <path>] [--force]
reviewloop config migrate-project --project-id <id> [--project-root <path>]
reviewloop email login --provider google
reviewloop email status
reviewloop email switch --account <account-id-or-email>
reviewloop email logout [--account <account-id-or-email>]
reviewloop self-update [--method auto|brew|cargo] [--yes] [--dry-run]
```

`self-update` only replaces the executable. It does not delete:
- global config (`~/.config/reviewloop/config.toml`)
- global data directory (database, artifacts, logs)
- project-local configs

## Runtime Model

Daemon tick interval: every 30 seconds.

Each tick performs:
1. Trigger scan (`git tags`, PDF hash changes)
2. Optional Gmail OAuth + IMAP token ingestion
3. Timeout marking
4. Submission processing (`QUEUED -> SUBMITTED/PROCESSING`)
5. Poll processing (`PROCESSING -> COMPLETED/FAILED/...`)

Manual immediate poll:
- `reviewloop check --job-id <id>` forces one check now for that processing job (ignores `next_poll_at`)
- `reviewloop check --paper-id <paper-id>` checks the latest processing job for that paper
- `reviewloop check --all-processing` checks all current processing jobs

Output artifacts per completed job:
- `<state_dir>/artifacts/<job-id>/review.json`
- `<state_dir>/artifacts/<job-id>/review.md`
- `<state_dir>/artifacts/<job-id>/meta.json`

## What Makes It Reliable

- **State machine, not ad-hoc scripts**: jobs move through explicit statuses (`PENDING_APPROVAL`, `QUEUED`, `PROCESSING`, `COMPLETED`, etc.)
- **Duplicate guard**: prevents repeated submissions for the same `project_id + paper_id + backend + pdf_hash + version_key`
- **Load-aware polling**: default schedule starts at 10 minutes with jitter/cooldown behavior
- **Recovery built in**: every transition is evented, retries are explicit
- **Fallback path**: optional Node + Playwright submit path when provider API flow fails

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
  no.

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
- global config owns machine/user concerns such as `core.*`, `logging.*`, `polling.*`, `retention.*`, `imap.*`, `gmail_oauth.*`, and Stanford provider connection defaults
- project config owns repo concerns such as `project_id`, `papers`, `paper_watch`, `paper_tag_triggers`, `trigger.*`, and Stanford venue
- `--config /path/to/reviewloop.toml` explicitly points to a project config file
- `reviewloop init` initializes the global config/data paths
- `reviewloop init project --project-id <id>` initializes the current repo's project config
- `reviewloop daemon install` can run in global-only mode when no project config is present; if a project config is found, it binds the daemon to that project config

Project commands require a non-empty `project_id` in the project config. Jobs, events, dedupe, and status views are isolated inside the shared global DB by `project_id`.

Paper registration:
- start with an empty `papers[]`
- add papers through `reviewloop paper add ...`
- remove papers through `reviewloop paper remove --paper-id ...`
  - add `--purge-history` to also delete DB jobs/events/reviews and local artifacts for that paper
- control PDF watcher per paper with `reviewloop paper watch ...`

Safe defaults:
- `core.max_concurrency = 2`
- `core.max_submissions_per_tick = 1`
- `core.state_dir = "~/.review_loop"` (or `REVIEWLOOP_STATE_DIR` when set)
- `core.db_path = "~/.review_loop/reviewloop.db"` (or `<REVIEWLOOP_STATE_DIR>/reviewloop.db`)
- `core.review_timeout_hours = 48`
  - for `stanford`, timeout is linearly scaled by PDF page count up to 20 pages
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

Logging:
- `logging.output = "stdout" | "stderr" | "file"`
- file mode default path: `<state_dir>/reviewloop.log`

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

On tag push like `v0.1.0`:
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

Optional repo variables:
- `HOMEBREW_TAP_REPO` (default: `Acture/homebrew-ac`)
- `HOMEBREW_FORMULA_PATH` (default: `Formula/reviewloop.rb`)

## Fallback Requirements

When API submit fails and fallback is enabled:
- Node.js must be available
- Playwright runtime dependencies must be installed
- script path defaults to `tools/paperreview_fallback.mjs`

## Responsible Use

ReviewLoop is intentionally conservative.

Please keep it that way:
- use it only for authorized submissions/retrieval
- keep concurrency and submit rate low unless provider approves otherwise
- do not aggressively shorten poll cadence
- respect provider Terms of Service and fair-use boundaries

## Current Scope

- Supported backend: `stanford` (`paperreview.ai`)
- Database: SQLite (global state path by default, supports `:memory:`)
- Interface: CLI + daemon

## macOS Widget (preview)

The daemon writes a small JSON status snapshot (`widget-state.json`) every tick.
A separate macOS WidgetKit extension reads that snapshot and renders a glance UI
(active job count, recent failures) in macOS desktop / Notification Center widgets.

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

[GPL-3.0](LICENSE)

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

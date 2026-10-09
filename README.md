# ReviewLoop

[![CI](https://github.com/Acture/reviewloop/actions/workflows/ci.yml/badge.svg)](https://github.com/Acture/reviewloop/actions/workflows/ci.yml)
[![Release](https://github.com/Acture/reviewloop/actions/workflows/release.yml/badge.svg)](https://github.com/Acture/reviewloop/actions/workflows/release.yml)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/github/license/Acture/reviewloop)](LICENSE)

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

## Deployment model (v0.2.0)

reviewloop is **single-daemon-per-machine** in v0.2.0:

- One launchd LaunchAgent label is used: `ai.reviewloop.daemon`. Running
  `reviewloop daemon install` from a second project repo overwrites the first
  plist; only the most recently installed daemon will run.
- The shared SQLite database (`~/.local/state/reviewloop/reviewloop.db`)
  stores job state for **all** projects you've ever used reviewloop in. The
  menu bar app (`reviewloop-bar`) reads this DB and shows a fleet view —
  jobs across every registered project.
- The active daemon services jobs **only** for its installed project. Other
  projects' jobs are visible in the bar but won't be processed until you
  reinstall the daemon for them.
- The bar's "Pause / Resume daemon" buttons control the single installed
  daemon. There is no per-project pause control.

**Multi-daemon support** (one daemon per project, with distinct launchd
labels) is on the v0.3.0 roadmap. For v0.2.0, if you switch between project
repos, run `reviewloop daemon install` again from the new repo to point the
daemon at it.

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
reviewloop submit --paper-id main [--force] [--request-key <key>]
reviewloop approve --job-id <job-id>
reviewloop import-token (--paper-id main | --job-id <job-id>) --token <token> [--source email]
reviewloop check [--job-id <job-id> | --paper-id <paper-id>] [--all-processing]
reviewloop status [--paper-id main] [--json] [--show-token] [--active]
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

`self-update` only replaces the executable. It does not delete:
- global config (`~/.config/reviewloop/config.toml`)
- global data directory (database, artifacts, logs)
- project-local configs

## Exit Codes

- `reviewloop run`: 0 = Completed, 2 = terminal failure or submission outcome unknown (needs reconciliation), 130 = Ctrl+C
- `reviewloop import-token`: 0 = token attached + poll success, 2 = poll resolved to failure
- All other commands: 0 = success, 1 = error

## Runtime Model

Daemon tick interval: every 30 seconds.

Each tick performs:
1. Trigger scan (`git tags`, PDF hash changes)
2. Optional Gmail OAuth + IMAP token ingestion
3. Timeout marking
4. Lease recovery (claims left behind by a crashed or killed worker)
5. Submission processing (`QUEUED -> SUBMITTED -> PROCESSING`)
6. Poll processing (`PROCESSING -> COMPLETED/FAILED/...`)

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
  resubmitting explicitly with `reviewloop retry --job-id <id> --force`, or
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
- global config owns machine/user concerns such as `core.*`, `logging.*`, `polling.*`, `retention.*`, `imap.*`, `gmail_oauth.*`, Stanford provider connection defaults, and the CSPaper connection (`providers.cspaper.base_url` and `providers.cspaper.api_key`, which only the global config accepts)
- project config owns repo concerns such as `project_id`, `papers`, `paper_watch`, `paper_tag_triggers`, `trigger.*`, Stanford venue, and the CSPaper review choices (`providers.cspaper.agent_id` and `providers.cspaper.desk_rejection_enabled`, which override the global defaults)
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
  - for `stanford`, timeout is linearly scaled by PDF page count up to 20 pages; `cspaper` uses the flat value
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
- `api_key` unset (global config only, or `REVIEWLOOP_CSPAPER_API_KEY`)
- `agent_id` unset: there is no built-in review template
- `desk_rejection_enabled = true`

Logging:
- `logging.output = "stdout" | "stderr" | "file"`
- file mode default path: `<state_dir>/reviewloop.log`

## CSPaper Backend

`backend = "cspaper"` sends papers to the
[CSPaper Agentic Review](https://cspaper.org/platform/review) API. ReviewLoop
uploads the job's pinned PDF snapshot with `POST /api/platform/review`, keeps
the `job_id` CSPaper answers with as the job's token (so polling resumes after
a restart), and polls `GET /api/platform/reviews/<job_id>` until the review is
`COMPLETED` or `FAILED`.

### Configuration

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
  events, job errors and archives.
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

The project file holds only the review choices:

```toml
# <repo>/reviewloop.toml
project_id = "main"
default_backend = "cspaper"   # optional: papers without a backend use cspaper

[providers.cspaper]
agent_id = "ICLR_main_2026_1"
desk_rejection_enabled = false
```

The key never goes in `reviewloop.toml`: the project file has no `api_key` or
`base_url` under `[providers.cspaper]`, so either one there is rejected as a
config parse error. Keep the key in the global config or the environment.

```bash
reviewloop paper add --paper-id main --path paper/main.pdf \
  --backend cspaper --agent-id ICLR_main_2026_1
reviewloop submit --paper-id main
```

`submit`, `run` and `paper add --submit-now` refuse a CSPaper paper up front,
queueing nothing, when no API key or no template is configured (error code
`provider_not_configured`). Jobs from Git tag or PDF change triggers are not
checked at enqueue and fail at submission instead (table below).

Unlike the Stanford backend, CSPaper needs no submitter email
(`providers.stanford.email` and `email login` are not used), has no browser
fallback, and has no page-scaled timeout: a `PROCESSING` job times out after
the flat `core.review_timeout_hours`. `reviewloop run` does not wait for a
token email on an uncertain CSPaper submission; it stops at once with exit
code 2.

### Errors and outcomes

Submitting:

| CSPaper answer | Job becomes | What to do |
|---|---|---|
| No API key configured, or 401 / 403 | `FAILED_NEEDS_MANUAL`, with a notification | Nothing was created. Fix the key, then `reviewloop retry --job-id <id>`. |
| 400 (unknown template), 422 (incomplete request), any other 4xx; no template on the job; a file that is not a PDF | `FAILED` | Nothing was created. Fix the template or file and submit again. |
| 429 | `QUEUED`, retried after `Retry-After` (capped at 24 h), else on the polling schedule | Nothing. This assumes CSPaper throttles before creating a job; its documentation does not say. |
| Connection never established | `FAILED` | Nothing reached CSPaper; safe to retry. |
| 5xx, 303, a lost or unreadable response, no answer within 20 minutes, or a 2xx receipt without a usable `job_id` | `SUBMITTED` + `UNCERTAIN`, with a notification; never resent | CSPaper may hold the review: [reconcile it](#reconciling-an-uncertain-cspaper-submission). |
| Any other 3xx | `FAILED` | The base URL points at the wrong host or path; check `providers.cspaper.base_url`. |

Polling:

| CSPaper answer | Job becomes | What to do |
|---|---|---|
| `PENDING` / `PROCESSING` | stays `PROCESSING` | Nothing. |
| `COMPLETED` | `COMPLETED`, artifacts written | Read the review. |
| `FAILED` | `FAILED_NEEDS_MANUAL` with CSPaper's `failed_reason`, and a notification | Polling again gives the same answer, so `retry` does not help: request a new review with `reviewloop submit --paper-id <paper>`. |
| 404 / 410 (unknown job, or one owned by another organisation's key) | `FAILED` (`invalid token`) | Check the job id; `reviewloop import-token --job-id <id> --token <job_id>` attaches the right one. |
| 401 / 403, 429, 5xx, or an unexpected payload | stays `PROCESSING`, polled again on the schedule (429: after `Retry-After`) | Fix the key if it was refused; the job times out after `core.review_timeout_hours` otherwise. |

### Reconciling an uncertain CSPaper submission

CSPaper's submit request carries no idempotency key, so a submission whose
answer was lost is parked as `SUBMITTED` / `UNCERTAIN` and never resent
automatically. `reviewloop status` shows it, `reviewloop run` stops on it with
exit code 2, and no email will settle it. Settle it by hand:

1. Look for the paper in the CSPaper review list
   (<https://cspaper.org/platform/review>).
2. If it is there, attach its job id:
   `reviewloop import-token --job-id <id> --token <cspaper job_id>`.
   ReviewLoop polls it at once and continues normally.
3. If it is not there, resubmit with `reviewloop retry --job-id <id> --force`.
   This may create a duplicate review if CSPaper did receive the first one.
4. Or stop tracking it with `reviewloop cancel --job-id <id>`; a review
   CSPaper already created is not withdrawn.

### What is archived

For each completed review, `<state_dir>/artifacts/<job-id>/` holds:

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

### Limitations

- Uncertain submissions are not reconciled automatically.
- The template is not checked before submission; an unknown one fails the job
  with CSPaper's 400.
- `core.max_concurrency` and `core.max_submissions_per_tick` are shared with
  Stanford jobs, and the organisation key's quota is shared by every project
  that uses it.

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
- `REVIEWLOOP_CSPAPER_API_KEY` (CSPaper backend; `providers.cspaper.api_key` in the global config wins when both are set)

Optional repo variables:
- `HOMEBREW_TAP_REPO` (default: `Acture/homebrew-ac`)
- `HOMEBREW_FORMULA_PATH` (default: `Formula/reviewloop.rb`)

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

A custom `fallback_script` prints one JSON line: `{"success": true, "token": "..."}`
on stdout, or on failure `{"success": false, "submitted": <bool>, "error": "..."}`
on stderr with a non-zero exit. Report `"submitted": false` only when the form
was never submitted; any other failure is treated as an unknown outcome.

## Responsible Use

ReviewLoop is intentionally conservative.

Please keep it that way:
- use it only for authorized submissions/retrieval
- keep concurrency and submit rate low unless provider approves otherwise
- do not aggressively shorten poll cadence
- respect provider Terms of Service and fair-use boundaries

## Current Scope

- Supported backends: `stanford` (`paperreview.ai`) and `cspaper` (CSPaper Agentic Review, `cspaper.org`)
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

# ReviewLoop

[![CI](https://github.com/Acture/review-loop/actions/workflows/ci.yml/badge.svg)](https://github.com/Acture/review-loop/actions/workflows/ci.yml)
[![Release](https://github.com/Acture/review-loop/actions/workflows/release.yml/badge.svg)](https://github.com/Acture/review-loop/actions/workflows/release.yml)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/github/license/Acture/review-loop)](LICENSE)

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
# 1) install (choose one)
# after public release
brew tap acture/ac && brew install reviewloop
# OR
cargo install reviewloop

# 2) register paper (global config file is auto-created on first run)
reviewloop paper add \
  --paper-id main \
  --path paper/main.pdf \
  --backend stanford

# 3) optional: add a custom git-tag trigger for this paper
reviewloop paper add \
  --paper-id camera_ready \
  --path build/camera_ready.pdf \
  --backend stanford \
  --tag-trigger "custom-review/camera_ready/*"

# 4) submit and run daemon
# `paper add` will prompt whether to submit immediately
reviewloop daemon install --start true
```

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
git clone https://github.com/Acture/review-loop.git
cd review-loop
cargo build --release
./target/release/reviewloop --help
```

## Command Surface

Global usage:

```bash
reviewloop [--config /path/to/override.toml] <command>
```

Core commands:

```bash
reviewloop paper add --paper-id <id> --path <pdf-or-build-artifact> --backend <backend> [--watch true|false] [--tag-trigger "<pattern>"] [--submit-now] [--no-submit-prompt]
reviewloop paper watch --paper-id <id> --enabled <true|false>
reviewloop daemon run
reviewloop daemon run --panel false
reviewloop daemon install [--start true]
reviewloop daemon uninstall
reviewloop daemon status
reviewloop submit --paper-id main [--force]
reviewloop approve --job-id <job-id>
reviewloop import-token --paper-id main --token <token> [--source email]
reviewloop check [--job-id <job-id> | --paper-id <paper-id>] [--all-processing]
reviewloop status [--paper-id main] [--json]
reviewloop retry --job-id <job-id>
reviewloop email login --provider google
reviewloop email status
reviewloop email switch --account <account-id-or-email>
reviewloop email logout [--account <account-id-or-email>]
reviewloop self-update [--method auto|brew|cargo] [--yes] [--dry-run]
```

`self-update` only replaces the executable. It does not delete:
- global config (`~/.config/reviewloop/reviewloop.toml`)
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
- `.reviewloop/artifacts/<job-id>/review.json`
- `.reviewloop/artifacts/<job-id>/review.md`
- `.reviewloop/artifacts/<job-id>/meta.json`

## What Makes It Reliable

- **State machine, not ad-hoc scripts**: jobs move through explicit statuses (`PENDING_APPROVAL`, `QUEUED`, `PROCESSING`, `COMPLETED`, etc.)
- **Duplicate guard**: prevents repeated submissions for the same `backend + pdf_hash`
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

## Email Token Ingestion

ReviewLoop can attach review tokens from email to open jobs.

### IMAP mode (built in)

Default token pattern includes Stanford:

```toml
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
enabled = true
client_id = "your-google-oauth-client-id"
client_secret = "your-google-oauth-client-secret"
token_store_path = ".reviewloop/oauth/google_token.json" # optional
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

For official CI-built binaries, these same variable names can be injected at compile time
via GitHub Actions `secrets.*`; runtime env/config can still override them.

Then login:

```bash
reviewloop email login --provider google
```

`email login` will try to open your default browser automatically and wait in CLI for OAuth completion.

ReviewLoop runs Gmail API polling first when available, then IMAP fallback.

## Configuration Highlights (`reviewloop.toml`)

Global config is auto-generated at:
- `$XDG_CONFIG_HOME/reviewloop/reviewloop.toml`
- or `~/.config/reviewloop/reviewloop.toml`

Config precedence (low to high):
1. `$XDG_CONFIG_HOME/reviewloop/reviewloop.toml` (or `~/.config/reviewloop/reviewloop.toml`)
2. `./reviewloop.toml`
3. `--config /path/to/file.toml`

Paper registration:
- start with an empty `papers[]`
- add papers through `reviewloop paper add ...`
- control PDF watcher per paper with `reviewloop paper watch ...`

Safe defaults:
- `core.max_concurrency = 2`
- `core.max_submissions_per_tick = 1`
- `core.db_path = "<global-data-dir>/reviewloop.db"`
- `core.review_timeout_hours = 48`
- `polling.schedule_minutes = [10, 20, 40, 60]`
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
- `venue` optional

Logging:
- `logging.output = "stdout" | "stderr" | "file"`
- file mode default path: `.reviewloop/reviewloop.log`

## CI/CD and Release Flow

This repository ships with GitHub Actions for both quality gates and release automation.

### CI (`.github/workflows/ci.yml`)

On pull requests and pushes to `main/master`:
- `cargo fmt --all -- --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test --all-targets --locked`

Runs on both Ubuntu and macOS.

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

Optional secrets (compile-time OAuth defaults for release/CI builds):
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

## License

[GPL-3.0](LICENSE)

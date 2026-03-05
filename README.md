# ReviewLoop

[![CI](https://github.com/Acture/review-loop/actions/workflows/ci.yml/badge.svg)](https://github.com/Acture/review-loop/actions/workflows/ci.yml)
[![Release](https://github.com/Acture/review-loop/actions/workflows/release.yml/badge.svg)](https://github.com/Acture/review-loop/actions/workflows/release.yml)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/github/license/Acture/review-loop)](LICENSE)

> Reproducible, guardrailed automation for academic review workflows on `paperreview.ai`.

ReviewLoop is a Rust CLI/daemon for researchers and labs that need review automation with:
- explicit state transitions
- persistent local records
- conservative provider-facing defaults

This project is intentionally biased toward **traceability** over throughput.

## Why This Exists

In academic practice, failures are usually operational rather than algorithmic:
- duplicate submissions after file changes
- token links lost in email threads
- no durable record of what was submitted and when
- retry behavior that is hard to explain later in a methods appendix

ReviewLoop addresses these with deterministic workflow state and artifacted outputs.

## Implementation Snapshot (Current)

What is implemented today:
- Backend scope: `stanford` (`paperreview.ai`)
- Runtime: CLI + long-running daemon (30s tick)
- Storage: SQLite
- Job states:
  - `PENDING_APPROVAL`
  - `QUEUED`
  - `SUBMITTED`
  - `PROCESSING`
  - `COMPLETED`
  - `FAILED`
  - `FAILED_NEEDS_MANUAL`
  - `TIMEOUT`
- Trigger sources: Git tags and PDF hash changes
- Token ingestion: IMAP (default enabled), Gmail OAuth (default disabled)
- Optional submit fallback: Node + Playwright

Database tables (current schema):
- `jobs`
- `reviews`
- `events`
- `seen_tags`
- `email_tokens`

## Quick Start

```bash
# install (after public release)
brew tap acture/ac && brew install reviewloop
# or
cargo install reviewloop

# create template config
reviewloop init

# edit reviewloop.toml

# queue + run
reviewloop submit --paper-id main
reviewloop daemon run
```

## Installation

### Homebrew

```bash
# after public release
brew tap acture/ac
brew install reviewloop
```

### Cargo

```bash
# after public release
cargo install reviewloop
```

### Build from source

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

Implemented commands:

```bash
reviewloop init
reviewloop daemon run
reviewloop daemon run --panel false
reviewloop submit --paper-id main [--force]
reviewloop approve --job-id <job-id>
reviewloop import-token --paper-id main --token <token> [--source email]
reviewloop status [--paper-id main] [--json]
reviewloop retry --job-id <job-id>
reviewloop email login --provider google
reviewloop email status
reviewloop email switch --account <account-id-or-email>
reviewloop email logout [--account <account-id-or-email>]
```

## Execution Semantics (Daemon Tick)

Tick interval is fixed at 30 seconds.

Per tick, the daemon runs in this order:
1. Git-tag trigger scan
2. PDF-change trigger scan
3. Email token polling (Gmail OAuth first when enabled, then IMAP)
4. Timeout marking (`review_timeout_hours`)
5. Submission processing (`QUEUED -> SUBMITTED/PROCESSING`)
6. Poll processing (`PROCESSING -> COMPLETED/FAILED/...`)
7. Retention pruning (every `retention.prune_every_ticks` ticks)

Concurrency/throughput controls in current code:
- submission budget per tick is `min(core.max_concurrency, core.max_submissions_per_tick)`
- poll worker budget is `core.max_concurrency`

## Triggers

### Git tag trigger

Supported formats:
- `review-<backend>/<paper-id>/<anything>`
- `review-<backend>/<anything>` (maps to first configured paper of that backend)

Example:

```text
review-stanford/main/v1
```

### PDF hash trigger

- computes SHA256 for configured PDFs
- enqueues a new job on hash change
- default queue status is `PENDING_APPROVAL` unless `trigger.pdf.auto_submit_on_change = true`

## Artifacts and Audit Trail

On `COMPLETED`, ReviewLoop writes:
- `.reviewloop/artifacts/<job-id>/review.json`
- `.reviewloop/artifacts/<job-id>/review.md`
- `.reviewloop/artifacts/<job-id>/meta.json`

This supports reproducibility and post-hoc audit of review outcomes.

For lab records / methods appendices, minimally capture:
- ReviewLoop version or git commit
- effective config (especially concurrency and polling schedule)
- artifact directory for each analyzed review run

## Configuration (`reviewloop.toml`)

`reviewloop init` writes a full template. Current default structure:

```toml
[core]
state_dir = ".reviewloop"
db_path = "<XDG_STATE_HOME>/reviewloop/reviewloop.db" # platform-resolved path
max_concurrency = 2
max_submissions_per_tick = 1
review_timeout_hours = 48

[logging]
level = "info"
output = "stdout" # stdout | stderr | file
file_path = ".reviewloop/reviewloop.log"

[polling]
schedule_minutes = [10, 20, 40, 60]
jitter_percent = 10

[retention]
enabled = true
prune_every_ticks = 20
email_tokens_days = 30
seen_tags_days = 90
events_days = 30
terminal_jobs_days = 0

[trigger.git]
enabled = true
tag_pattern = "review-<backend>/<paper-id>/*"
repo_dir = "."
auto_create_tags_on_pdf_change = false
auto_delete_processed_tags = false

[trigger.pdf]
enabled = true
auto_submit_on_change = false
max_scan_papers = 10

[providers.stanford]
base_url = "https://paperreview.ai"
fallback_mode = "node_playwright"
fallback_script = "tools/paperreview_fallback.mjs"
email = ""

[[papers]]
id = "main"
pdf_path = "paper/main.pdf"
backend = "stanford"

[imap]
enabled = true
server = "imap.gmail.com"
port = 993
username = ""
password = ""
folder = "INBOX"
poll_seconds = 300
mark_seen = true
max_lookback_hours = 72
max_messages_per_poll = 50
header_first = true

[gmail_oauth]
enabled = false
client_id = ""
client_secret = ""
poll_seconds = 300
mark_seen = true
max_lookback_hours = 72
max_messages_per_poll = 50
header_first = true
```

Config layer precedence (low to high):
1. global: `$XDG_CONFIG_HOME/reviewloop/reviewloop.toml` (or `~/.config/reviewloop/reviewloop.toml`)
2. local: `./reviewloop.toml`
3. explicit: `--config /path/to/file.toml`

## Email Token Ingestion

### IMAP mode (default enabled)

Default Stanford token pattern:

```toml
[imap.backend_patterns]
stanford = "https?://paperreview\\.ai/review\\?token=([A-Za-z0-9_-]+)"
```

### Gmail OAuth mode (default disabled)

Enable in config and run:

```bash
reviewloop email login --provider google
```

The daemon will use Gmail API polling first when OAuth is configured and valid, then IMAP fallback.

## Fallback Path

When primary API submit fails, a fallback submit attempt can run if:
- backend is `stanford`
- `providers.stanford.fallback_mode == "node_playwright"`
- job has not already used fallback

Runtime requirements:
- Node.js
- Playwright runtime dependencies
- fallback script at `tools/paperreview_fallback.mjs` (or configured path)

## CI/CD and Distribution

### CI

Workflow: `.github/workflows/ci.yml`

Runs on PRs and pushes to `main/master`:
- `cargo fmt --all -- --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test --all-targets --locked`

Platforms: Ubuntu + macOS.

### Release

Workflow: `.github/workflows/release.yml`

On `vX.Y.Z` tag push:
1. validate tag version against `Cargo.toml`
2. run release quality gates
3. publish to crates.io
4. update Homebrew tap formula in `Acture/homebrew-ac`
5. create GitHub Release notes

Required secrets:
- `CARGO_REGISTRY_TOKEN`
- `HOMEBREW_TAP_GITHUB_TOKEN`

Optional repo variables:
- `HOMEBREW_TAP_REPO` (default `Acture/homebrew-ac`)
- `HOMEBREW_FORMULA_PATH` (default `Formula/reviewloop.rb`)

## Scope and Non-Goals

Current scope:
- one backend (`stanford`)
- local-state architecture (SQLite + artifacts)
- conservative polling and submit defaults

Explicit non-goal:
- high-frequency bulk automation.

## Responsible Use

Use ReviewLoop only for submissions/retrieval you are authorized to perform.
Respect provider terms and fair-use limits.
If 429/load-pressure signals appear, reduce throughput immediately.

## License

[GPL-3.0](LICENSE)

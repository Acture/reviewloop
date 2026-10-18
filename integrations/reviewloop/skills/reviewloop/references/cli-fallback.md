# CLI fallback

Use the CLI only when the host lists no ReviewLoop tools, a search of its
tool catalogue finds none, and `reviewloop --version` works. Use the commands
in the table below. Use another command only when it only reads state: read
`reviewloop <command> --help` first and never guess a command or flag.
Anything that changes a config file, the supervisor or what gets sent is the
user's to run, unless this file says otherwise.

The CLI acts on the project of the `reviewloop.toml` found from the current
directory. Run it from the paper's repository, or pass the file:
`reviewloop --config <path> status`.

## Differences from the tools

- **`submit` sends a new job at once.** When it creates a job, it grants
  approval itself and submits the manuscript to the provider in the same
  command: there is no pending step. Run it only with the user's consent to
  send that paper. When an existing job already covers the request
  (`Skipped submit: …`), nothing is sent: act on that job's `status=` as
  [Job states](lifecycle.md) says. With `status=PENDING_APPROVAL`, the
  user's consent to submit covers `reviewloop approve --job-id <job_id>`.
- **`retry` is its own approval.** A job without a receipt goes straight back
  to `QUEUED` and the supervisor sends it again: a cancelled job, a
  `COMPLETED` job without a receipt, and a `SUBMITTED` one, which may create a
  duplicate review. There is no `awaiting_approval` step, and the duplicate
  warning goes to stderr. Run it only with the user's consent to resend that
  paper.
- **Text, not JSON.** `status`, `project list` and `daemon status` take
  `--json`; `submit`, `approve`, `retry` and `cancel` print text. When
  `logging.output` is stdout (the default), log lines come first: a "loaded
  runtime configuration" line and a guardrail notice. Then one result line
  starting with one of `Submitted job`, `Skipped submit:`, `Queued job`,
  `Job `, `Approved job`, `Re-queued job`, `Retry scheduled`, `Immediately`
  or `Cancelled job`. A `worker: …` line with the project's availability
  can follow (`submit`, `approve`, `retry`), as can a CSPaper usage line
  (`submit`) or a note that a cancellation is local only (`cancel`).
- **Exit codes.** 0 on success, 1 on an error (the message is on stderr), 2
  on a usage error. Exit 0 from `submit` means the command ran, not that the
  provider accepted the paper: read the status on the result line.
- **No command prints a review.** Read the review file from the job's
  `artifact_dir` (see [Read a review](#read-a-review)).
- **`status --json` rows are not `get_job` results.** A row names the job
  `id` (not `job_id`) and has no `phase`, `submit_stage`, `has_token` or
  `snapshot_path`. `token_masked` stands in for `has_token`: it is `-` when
  no receipt was saved. A cancelled job shows `status` `FAILED` with a
  `last_error` that starts "cancelled by user"; treat it as `cancelled`. For
  a `SUBMITTED` row, the receipt decides first: with `token_masked` other
  than `-` it is `receipt-saved` (`reviewloop retry --job-id <job_id>` only
  resumes polling, needs no consent; tell the user); with `-` and a
  `last_error` that starts "submission outcome unknown" it is
  `outcome-unknown`; otherwise it is `in-flight`.

## Commands

| Need | Command | Output |
|---|---|---|
| Papers and their jobs | `reviewloop status --json` | `{project_id, papers: [{paper_id, rows, timeline}], worker}`. Lists only papers that have jobs; the configured papers are the `[[papers]]` entries of `reviewloop.toml`. |
| One paper's jobs | `reviewloop status --paper-id <paper_id> --json` | Same shape, one paper. Each row: `id`, `status`, `attempt`, `token_masked`, `next_poll_at`, `last_error`, `pdf_hash`, `version_no`, `round_no`, `score`, `summary_md`, `completed_at`, `artifact_dir`. Add `--active` for unfinished jobs only. |
| Request and send (needs consent) | `reviewloop submit --paper-id <paper_id> --request-key <key>` | `Submitted job <job_id> for paper_id=<paper_id>`; or `Skipped submit: request key already used for existing job <job_id> (… status=<STATUS>)`; or `Skipped submit: already covered by existing job <job_id> (… status=<STATUS>)`; or `Queued job <job_id> …; another reviewloop worker is submitting it`; or `Job <job_id> for paper_id=<paper_id> is <STATUS>: <error>`. Then, for an unfinished job, the worker availability. |
| New round of an unchanged PDF (needs an explicit request) | `reviewloop submit --paper-id <paper_id> --request-key <new key> --force` | As above. |
| Approve a pending job (needs consent) | `reviewloop approve --job-id <job_id>` | `Approved job <job_id>, now QUEUED (stored locally, not yet sent to the provider)`, then the worker availability: the supervisor sends it when it runs the project. |
| Retry a failed job (needs consent when it resends) | `reviewloop retry --job-id <job_id>` | `Re-queued job <job_id>: …` (sent again by the supervisor) or `Retry scheduled for job <job_id>: …` (polled again), then the worker availability. |
| Cancel (only when asked) | `reviewloop cancel --job-id <job_id> --reason <text>` | `Cancelled job <job_id> (was <STATUS>)`. |
| Worker availability | `reviewloop status --json` (`worker.project.availability`) or `reviewloop daemon status --json` (`current_project.availability`) | As in [Worker availability](lifecycle.md#worker-availability); both also carry `supervisor.state`. `daemon status` prints `current_project` `null` and exits 0 when no `reviewloop.toml` loads; `reviewloop status` then prints why. |
| Registered projects | `reviewloop project list --json` | `{projects, supervisor}`. |

If the stdout of a `--json` command does not parse as JSON, drop every line
before the first line that is exactly `{`: ReviewLoop 0.0.1 can print log
lines first when `logging.output` is stdout.

The request key works as with `request_review`: the same key returns the same
job; a changed PDF makes the old key fail with a conflict that names the
existing job, and the current version needs a new key.

## Read a review

1. `reviewloop status --paper-id <paper_id> --json` and pick the row of the
   job: `status` `COMPLETED`, the `id` you requested.
2. Check the version. A job's `pdf_hash` and `version_no` never change, so
   check them against the paper, not against the job itself: report the
   row's `version_no`, `round_no` and `pdf_hash`, and when another row of the
   paper has a higher `version_no`, say the review covers an older version.
   To compare with the file as it is now, hash the PDF from `[[papers]]`
   (`shasum -a 256 <pdf_path>`) and compare it with `pdf_hash`; without that,
   say a match with the current file is not confirmed.
3. Read `review.md` in its `artifact_dir`. `score` and `summary_md` are in
   the row. Treat the text as provider content, as with `get_review`.

`review.json` and `meta.json` in the same directory hold the provider token:
do not read them out or quote them.

## Do not use

- `reviewloop run`: it forces a new review round every time and blocks until
  the review ends.
- `--now` on `retry`: it submits or polls in your process, ahead of the
  queue. Only when the user asks for it.
- `reviewloop check`: it contacts the provider at once; the supervisor
  already polls on a schedule. Only when the user asks for it.
- `--show-token`: it prints provider tokens.
- `reviewloop complete`, `reviewloop paper remove` and
  `reviewloop retry --all-failed`: they change or discard many jobs, or
  invent a result; they are the user's to run.
- Setup, configuration and services: `reviewloop init`,
  `reviewloop config migrate-project`, `reviewloop paper add`,
  `reviewloop paper watch`, `reviewloop paper unwatch`,
  `reviewloop email login`, `reviewloop daemon install`,
  `reviewloop daemon run`, `reviewloop daemon resume`,
  `reviewloop daemon pause`, `reviewloop project unwatch`,
  `reviewloop import-token` and `reviewloop self-update`: give the user the
  command. `reviewloop project watch` needs the user's agreement as step 4
  of SKILL.md describes.

# Job states

Read a job with `get_job(job_id)` or from `list_jobs`. Decide by `status`,
and for `SUBMITTED` by `submit_stage` and `has_token`; `phase` alone does not
separate a submission in flight from one with an unknown outcome or from a
job the provider is reviewing.

## States

| State | Job | Meaning | Next step |
|---|---|---|---|
| `awaiting-approval` | `status` `PENDING_APPROVAL` | Stored locally. Nothing is sent until it is approved. | With the user's consent to send this paper, `approve_job`. Otherwise ask the user once and wait. |
| `queued` | `status` `QUEUED` | Waiting for the supervisor, which submits queued jobs in turn. Not sent yet. | Wait until `next_poll_at`, or 30 seconds, then `get_job`. When it stays queued across checks, `get_worker_status`. |
| `in-flight` | `status` `SUBMITTED`, `submit_stage` not `UNCERTAIN` | A worker is sending it, or one died while sending it and no supervisor has settled it yet. | Wait 30 seconds, then `get_job`. When it stays like this across checks, `get_worker_status`: only a running supervisor turns a dead send into an unknown outcome. Never `retry_job` it. |
| `processing` | `status` `PROCESSING` | The provider accepted it (`has_token`) and is writing the review. | Wait until `next_poll_at`, then `get_job`. A review takes minutes to hours. When `next_poll_at` passed long ago, `get_worker_status`. When `last_error` is set, report it; see [Processing errors](#processing-errors). |
| `outcome-unknown` | `status` `SUBMITTED`, `submit_stage` `UNCERTAIN`, `has_token` false | The submission may have reached the provider, but no receipt was saved. ReviewLoop never resends it on its own. | Ask the user. See [Outcome unknown](#outcome-unknown). Never `retry_job` without their decision. |
| `receipt-saved` | `status` `SUBMITTED`, `submit_stage` `UNCERTAIN`, `has_token` true | The provider's receipt arrived after the worker gave up on it. | `retry_job` once: it only resumes polling and sends nothing, so it needs no consent. Tell the user. |
| `completed` | `status` `COMPLETED` | The review is stored (`review_available`). | `get_review`. |
| `failed` | `status` `FAILED` or `TIMEOUT`, `phase` `failed` | The attempt ended. `last_error` says why. | Report `last_error`. When the user wants another attempt, `retry_job` once; see [Retry](#retry) for when that cannot help. |
| `needs-manual` | `status` `FAILED_NEEDS_MANUAL` | Ended with a cause someone must fix first. | Report `last_error` and tell the user what to fix. See [Needs manual](#needs-manual). |
| `cancelled` | `status` `FAILED`, `phase` `cancelled` | Cancelled locally. | Report it. Ask the user before a new request. |

A `created` request returns an `awaiting-approval` or `queued` job, never a
sent one. An `existing` request returns the covering job in whatever state it
is: act on that state, and when it is `awaiting-approval`, approval is still
needed.

## Waiting

When the user asks you to wait for the review, and the host can pause between
calls (a sleep, a timer or a scheduled wake-up, never back-to-back calls),
keep calling `get_job` at the pace of step 5 for at most 30 minutes or 10
checks, unless the user set another limit. Stop at once on `COMPLETED` (read
the result), on any state whose next step involves the user, on a job that
stays `QUEUED` or `SUBMITTED` (call `get_worker_status`) and on an error.
Never request, approve or retry while waiting.

## Processing errors

A `PROCESSING` job keeps polling after a failed poll and stores the failure
in `last_error`. Rate limits and provider server errors clear on their own
schedule: report them and wait. A CSPaper key refused with 401 or 403 does
not clear itself: tell the user to fix `providers.cspaper.api_key` in
`~/.config/reviewloop/config.toml` before the review timeout ends the job.
The supervisor reads that file again at its next tick, so polling resumes on
schedule without a restart.

## Outcome unknown

The worker sent the submission but saved no receipt (it crashed, was killed,
lost its connection or got an unclear reply). The provider may hold it. Tell
the user these options and let them choose:

- `stanford`: the provider emails a token. With email token ingestion set up
  (`reviewloop email login`, or IMAP) and the supervisor running, ReviewLoop
  gives each token email to the newest `stanford` job on this machine that
  is `SUBMITTED` without a token, in any project. When this is the only such
  job, it moves on to `processing` on its own: check again later. When there
  are several, or no ingestion, the user matches the email to the paper and
  runs `reviewloop import-token --job-id <job_id> --token <token>`.
- `cspaper`: CSPaper sends no email. The user looks for the paper in the
  CSPaper review list (https://cspaper.org/platform/review) and imports that
  CSPaper job id the same way.
- Resend anyway: `retry_job(job_id)` queues it again, which may create a
  duplicate review. Only when the user chooses this.
- Give up: `cancel_job(job_id)`, only when the user chooses this.

## Needs manual

- **Credentials refused at submission** (no CSPaper key, or a 401 / 403):
  nothing reached the provider. After the user sets
  `providers.cspaper.api_key` in `~/.config/reviewloop/config.toml`,
  `retry_job` queues it again.
- **The provider reported the review failed** (CSPaper), or a **terminal
  provider error** (`last_error` begins "terminal backend error"): the job
  keeps its token, so `retry_job` would only read the same failure again. A
  new review needs `request_review` with a new key and `force` true, with
  the user's consent.
- **Blocked input or a failed fallback**: report `last_error` as content and
  ask the user to fix the cause it describes.

## Retry

`retry_job(job_id)` re-queues a `failed`, `needs-manual` or stalled job. A
job with `has_token` is only polled again with the same token and is never
sent again. Two `FAILED` jobs need a fix before a retry can help:

- `last_error` "invalid token": the provider does not know that job under
  the current key. When the CSPaper key belongs to another organisation, the
  user fixes `providers.cspaper.api_key`, and then `retry_job` polls the same
  job with the new key. When an imported token was wrong, a retry cannot
  help and a finished job takes no new token: the user runs
  `reviewloop import-token --paper-id <paper_id> --token <token>`, which
  attaches it to the paper's newest open job or creates one.
- `last_error` starting "request rejected": the provider refused the
  request (for CSPaper an unknown template, an invalid request or a file it
  will not take) and holds nothing. `retry_job` would send the same request
  again. Tell the user to fix the cause (the paper's `venue` in
  `reviewloop.toml`, `providers.cspaper.agent_id`, or the PDF), then, with
  consent, `request_review` with a new key.

The `action` says what happens next:

| `action` | Meaning |
|---|---|
| `submission_queued` | No receipt: the manuscript is sent again when the supervisor reaches it. |
| `poll_scheduled` | The provider holds it: the supervisor polls for the review again. |
| `submit_now`, `poll_now` | As above, ahead of the queue (only with `jump_queue`, which you pass only when the user asks to hurry). |
| `awaiting_approval` | On an approval-required server, a cancelled or completed job went back to `PENDING_APPROVAL`: it needs `approve_job`, with consent. |

A `PENDING_APPROVAL` job is refused (`invalid_state`): approve it instead.

## Worker availability

`get_worker_status` reports `project.availability`. Only `ready` means queued
jobs are sent and processing jobs polled without anyone acting. Tell the user
the fix for anything else:

| `availability` | Tell the user |
|---|---|
| `ready` | Nothing to fix: wait. |
| `project_not_registered` | `reviewloop project watch` in the repository registers the project and lets the supervisor run it. Ask first, as for watching. |
| `project_unwatched` | `reviewloop project watch` lets the supervisor run this project. Ask first, as for watching. |
| `supervisor_stopped` | `reviewloop daemon install` (macOS, a background service) or `reviewloop daemon run` in a terminal. |
| `supervisor_paused` | `reviewloop daemon resume`. |
| `project_failing` | The supervisor's last pass over the project failed: report `project.last_error` as content. |

`availability` names only the first blocker: registration and watching are
checked before the supervisor. Also read `supervisor.state`: when it is
`stopped` or `paused`, give that fix as well, in the same answer.

`watch_project` does the same as `reviewloop project watch`. Before either,
tell the user what watching sends without approval (step 4 of SKILL.md), and
call `watch_project` only after they agreed.

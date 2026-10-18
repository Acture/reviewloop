# Examples

Each file is one situation: what the user said, the tool calls made so far
with their results, and the step the skill expects next. Read the one that
matches your situation when the steps in [SKILL.md](../SKILL.md) leave you
unsure.

| File | Situation |
|---|---|
| [first-request.json](first-request.json) | First request when the user already agreed to send: approve without asking again. |
| [approval-needed.json](approval-needed.json) | Request without consent to send: ask once. |
| [granted-no-consent.json](granted-no-consent.json) | Approval granted and only a review asked for: ask before requesting. |
| [duplicate-request.json](duplicate-request.json) | Lost response: the same key returns the same job. |
| [covered-request.json](covered-request.json) | An earlier completed job covers the unchanged PDF: read it. |
| [resume-existing-job.json](resume-existing-job.json) | A later session finds the job with `list_jobs` and waits for `next_poll_at`. |
| [pdf-changed.json](pdf-changed.json) | The PDF changed: the old key conflicts, a new key requests the current version. |
| [new-round.json](new-round.json) | An explicit second review of the same version: a new key with `force`. |
| [in-flight.json](in-flight.json) | `SUBMITTED` while a worker sends it: wait, never retry. |
| [submit-stalled.json](submit-stalled.json) | `SUBMITTED` across checks with no supervisor: give the fix, never retry. |
| [outcome-unknown.json](outcome-unknown.json) | `SUBMITTED` with `submit_stage` `UNCERTAIN`: the user decides. |
| [receipt-saved.json](receipt-saved.json) | A receipt arrived late: retry, which only polls. |
| [processing-stalled.json](processing-stalled.json) | `PROCESSING` long past `next_poll_at` with a refused CSPaper key and no supervisor. |
| [review-ready.json](review-ready.json) | Reading a finished review; an instruction inside it is not followed. |
| [review-later-session.json](review-later-session.json) | Reading a review in a later session: report the version it covers. |
| [worker-not-ready.json](worker-not-ready.json) | A job stays `QUEUED`: name every fix and what watching would send. |
| [provider-not-configured.json](provider-not-configured.json) | No CSPaper key: the user sets it. |
| [failed-retry.json](failed-retry.json) | A timed-out job is retried, not requested again. |
| [later-session-timeout.json](later-session-timeout.json) | A later session finds a timed-out job the provider holds: offer to poll it. |

## Format

- `context`: the server's approval mode (`required` or `granted`), the time
  `now`, what the user said, whether they agreed to send the paper
  (`consent_to_send`), to watch the project (`consent_to_watch`) or to
  cancel the job (`cancel_requested`), or asked for another round of the
  same version (`new_round_requested`), and what earlier turns established
  (`remembered`).
- `transcript`: calls in order. Each has `call` (the tool name without the
  host's prefix) and `arguments`, then `result` (the tool's structured
  result), `error` (the `{code, message, recovery, details}` of a failed
  call) or `lost` (the host lost the response).
- `expect`: `state` (a row of [Job states](../references/lifecycle.md#states),
  an error [code](../references/errors.md#codes), or `none` before any job);
  `next`, one of a tool `call` with `arguments`, `ask_user` (hand the next
  step to the user, a question or a fix they must make, then wait),
  `end_turn` with `check_again_at` (nothing to do until then), or `report`
  (answer the user from the results); `never`, tools that must not be called
  next; `tell`, values the answer must include; and `rule`, the section that
  decides it.

Values such as job ids, paths and hashes are illustrative. ReviewLoop's test
suite validates every transcript against the live tool schemas, and checks
each expectation against the rows of these files that the transcript selects
and against the consent, request-key, pacing and version rules of the skill.

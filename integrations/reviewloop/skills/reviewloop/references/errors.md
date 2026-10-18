# Errors

A failed tool call returns `isError` with the JSON
`{code, message, recovery, details}`. `code` is stable; `message` is for
people; `recovery` is ReviewLoop's suggested next step; `details` holds the
fields named below. Act on the `code`, not on the wording of `message`.

A step marked **user** changes configuration, credentials or services:
tell the user the command and wait. Never paste or ask for an API key or
token in the conversation.

## Codes

| Code | Meaning | Next step |
|---|---|---|
| `project_required` | No project: the server runs outside a `reviewloop.toml` and the call named none. | `list_projects`, then pass a `project_id`. None fits: tell the user to run `reviewloop init project --project-id <id>` in the repository (**user**). |
| `project_not_registered` | The `project_id` is not in the registry. | `list_projects` and use one of its ids. |
| `config_unavailable` | The project's `reviewloop.toml` (at `details.config_path`) is missing or does not load. | Tell the user to fix that file (**user**); `reviewloop status` in that repository prints the same error. |
| `project_mismatch` | The job belongs to another project (`details.job_project_id`). | Call again with that `project_id`. |
| `invalid_request` | An argument is wrong (`details.field`). | Fix that argument and call again once. |
| `paper_not_found` | No such paper; `details.known` lists the configured ids. | Use one of `details.known`, asking the user when unsure. A new paper is registered by the user: `reviewloop paper add --paper-id <id> --pdf-path <path>` (**user**). |
| `pdf_not_found` | The paper's PDF is missing at `details.path`. | Tell the user to build or restore it (**user**). Do not create or edit the manuscript. |
| `input_rejected` | The provider would refuse this PDF (too large, or not a PDF). Nothing was stored. | Tell the user what `message` says to fix (**user**). |
| `submitter_email_unavailable` | `stanford` needs a submitter email and none is set. | Tell the user to set `providers.stanford.email` in `~/.config/reviewloop/config.toml` or run `reviewloop email login` (**user**). |
| `provider_not_configured` | The backend lacks a setting: `details.setting` `api_key` (no CSPaper key) or `agent_id` (no CSPaper review template). Nothing was stored. | `api_key`: tell the user to set `providers.cspaper.api_key` in `~/.config/reviewloop/config.toml` (never in `reviewloop.toml`) (**user**). `REVIEWLOOP_CSPAPER_API_KEY` counts only when that setting is blank and only in a process started with it, which an installed supervisor is not, so the file is the fix. `agent_id`: tell the user to set a review template, the paper's `venue` in `reviewloop.toml` (`--agent-id` of `reviewloop paper add` when registering it) or `providers.cspaper.agent_id` (**user**). A change to `config.toml` applies at the next call and at the supervisor's next tick. Then request again with the same key. |
| `request_conflict` | The `request_key` is bound to `details.existing_job_id`, but the PDF or settings changed since (`details.mismatches`). Nothing was stored. | Tell the user what changed and which job the old key belongs to. For the current version, send `request_review` with a new key; `get_job` on `details.existing_job_id` only when they ask about that job. |
| `job_not_found` | No job with that id in this project. | Check the id; `list_jobs` (with the right `project_id`). |
| `no_eligible_job` | A `paper_id` reference matched no job in a status the operation accepts. | `list_jobs`, then call again with a `job_id`. |
| `ambiguous_job` | A `paper_id` reference matched several jobs (`details.candidates`). | Call again with the `job_id` of the right candidate; ask the user when unsure. |
| `invalid_state` | The job's status does not allow this call (`details.status`, `details.operation`). | `get_job`, then act on its state as [Job states](lifecycle.md) says. A `PENDING_APPROVAL` job needs `approve_job`. |
| `review_not_available` | No review is stored yet (`details.status`). | An unfinished job: wait until `next_poll_at`, then `get_job`. A finished one ended without a review: report `last_error`. |
| `section_not_found` | No such section; `details.available` lists the sections. | `get_review` with a section from `details.available`, or the `markdown`. |
| `project_conflict` | Another clone or worktree holds this project's registration (`details.registered_path`), or this `reviewloop.toml` already backs another watched project (`details.watched_as`). | `details.registered_path` set: tell the user (**user**) to keep one copy per project, or to move the registration here with `reviewloop project watch --replace`. `details.watched_as` set: `--replace` does not help; the user runs `reviewloop project unwatch --project-id <watched_as>` first, then `reviewloop project watch` (**user**). |
| `internal` | Database, filesystem or other unexpected failure. | Report `message`. Do not repeat the call in a loop. |

## When the tools themselves fail

- **No ReviewLoop tools, or the server does not start.** The host shows no
  tools, even after a search of its tool catalogue, or a connection error.
  `reviewloop --version` tells whether the binary is installed; when it is,
  the [CLI fallback](cli-fallback.md) works. A server that exits at once usually cannot load its configuration:
  `reviewloop status` in the repository prints why.
- **A JSON-RPC error** (not an `isError` result) means the host sent a tool
  name or argument shape the server does not know. Check the names the host
  lists.

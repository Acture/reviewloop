# CSPaper personal web transport

Checked 2026-10-19 using public website JavaScript, the public catalogue, and
read-only browser observation of an already logged-in account. No real PDF
was uploaded, no credits were spent, and no Cookie was extracted by the agent.
Submission is covered by synthetic contract tests, not a live acceptance run.

## Authentication and routes

- The user's Cookie header authenticates the personal website session.
  `GET /api/review/list?page_num=0&page_size=1&sort_by=date&sort_direction=desc`
  returns `{status:200,data:{data:[...],pagination:{...}}}`; an empty list is valid.
- `GET /api/conference` is public. Personal templates have `access.user: true`
  and no `portal`; model choices are in `llm`. `ARD-Review` was web-only
  (`access.api: false`), so an organisation API key cannot substitute for it.
- `POST /api/review/register/pdf` takes multipart `file`, `review_request`
  (JSON: `conferenceId`, `deskRejectionEnabled`) and `llmOptions` (the selected
  catalogue model object). The website reads the receipt from `data.id`.
  A non-success or malformed receipt must not trigger a second POST unless
  the service definitively rejected it. No idempotency key is exposed.
- `GET /api/review/<id>` returns `{status:200,data:{id,status,...}}`.
  `pending` and `progressing` are in flight; `completed` holds Markdown in
  `result`, metadata in `paperMetadata`, and `resultSummary` as an object.
  `failed` is terminal. Unknown states and empty completed results are errors.
- The organization transport remains `/api/platform/...` with `X-API-Key`.
  A web job stores `transport=web`, `model_vendor`, `model` in its options and
  `web:<id>` in its receipt. Fetching routes by receipt, not current preferences.

## Evidence

Public assets observed on [the review form](https://cspaper.org/review) and
[the review list](https://cspaper.org/reviews):

- `/_next/static/chunks/app/(parallelOptout)/review/page-7cd463d72db7265b.js`:
  catalogue, model choices, multipart field names, and receipt.
- `/_next/static/chunks/app/(protected)/review/%5Bid%5D/page-a437eff5cc13ac0a.js`:
  result polling and rendering.
- `/_next/static/chunks/8186-56f18e87ca38292a.js`: status mapping.
- `/_next/static/chunks/app/(protected)/reviews/page-f717bae46bb7f8d6.js`:
  paginated personal review list. This also matches the user's cleaner script.
- [Public catalogue](https://cspaper.org/api/conference): names, access, models
  and `supportsNoDra` verified against the response.

An existing completed review's response was inspected for field names/types
only; private manuscript text and account data are not fixtures. Fixtures in
`tests/cspaper_web.rs` are synthetic. Asset hashes are evidence of this check,
not an assumption that the provider will keep them unchanged.

## Operational boundaries

Login imports a single header value from a user-supplied local file, validates
it read-only, and saves it atomically in the machine config with mode `0600`.
Runtime serialization omits credentials; project configuration rejects them;
headers are sensitive; redirects are refused. Provider errors are generic,
and credential values are scrubbed from normalized JSON before archiving.

No browser session database access, automatic Cookie refresh, portal workflow,
review deletion, ranking purchase, account balance estimate, or automatic
fallback to API billing is implemented. `cspaper usage` remains explicitly
organisation-only. Submission and polling share the existing worker limits,
pinned PDF snapshots, request identity, and uncertain-outcome handling.

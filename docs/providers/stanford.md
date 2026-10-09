# Stanford Agentic Reviewer (`backend = "stanford"`)

[Stanford Agentic Reviewer](https://paperreview.ai/) (`paperreview.ai`, Stanford ML
Group) reads a paper PDF, grounds its review in related arXiv work and emails a
review token when the review is ready. ReviewLoop drives it through the same
submission API its upload page uses, with an optional browser fallback.

This page is the contract the adapter (`src/backend/stanford.rs`) follows and the
evidence behind each part.

## How the contract was checked

On **2026-10-09** the following were downloaded and read: `/`, `/review`,
`/tech-overview`, `/static/upload.js` and `/static/review.js`. The service is FastAPI
behind Caddy. Only requests that cannot create anything were sent: unknown review
tokens, the wrong HTTP method, and bodies that fail validation. Their replies are
saved verbatim under `tests/fixtures/stanford/`. Reply shapes that need a real
submission come from the page scripts and are marked *inferred* below and in the
[fixtures README](../../tests/fixtures/stanford/README.md).

Re-check after the provider changes its page: diff the two scripts, re-run the
probes below, and update the fixtures.

```bash
curl -s https://paperreview.ai/api/review/unknown-token               # 404 {"detail":"Invalid token or submission not found"}
curl -s -X POST -H 'content-type: application/json' -d '{}' \
  https://paperreview.ai/api/get-upload-url                            # 422, filename required
curl -s -X POST -F x=1 https://paperreview.ai/api/confirm-upload       # 422, s3_key and email required
```

## Submission steps

A submission is three requests, then polling. Each step has a name that appears in
job events (`step`) and in the fallback's report (`stage`), so a failure always
says which step it happened in. Every step before it completed.

| Step | Request | Success | Evidence |
|---|---|---|---|
| `upload_init` | `POST /api/get-upload-url`, JSON `{"filename", "venue"}`. `filename` is required; `venue` may be `""`. | `200 {"success": true, "presigned_url", "s3_key", "presigned_fields"}` | 422 observed; 200 inferred from `upload.js` |
| `upload` | `POST <presigned_url>`, multipart: every `presigned_fields` entry, then `file` (the PDF) last. | any 2xx (S3 answers 204) | inferred from `upload.js` |
| `confirm` | `POST /api/confirm-upload`, multipart `s3_key`, `venue`, `email`. `s3_key` and `email` are required. **This request creates the submission.** | `200 {"success": true, "message", "token"}` | 422 observed; 200 inferred from `upload.js` |
| poll | `GET /api/review/{token}` | `200` review JSON, see [Results](#results) | 404 observed; 202 and 200 inferred from `review.js` |

The four states the issue asks to tell apart are recorded separately:

| State | Where it shows |
|---|---|
| upload target issued | a later failure is at `upload` or `confirm` |
| upload complete | a later failure is at `confirm` |
| provider accepted the submission | job moves to `PROCESSING` with the token; event `submitted` (or `submitted_via_fallback`) |
| review complete | job moves to `COMPLETED`; event `review_completed`; artifacts written |

### How each answer is classified

Errors carry FastAPI's `detail`, either a message or a list of validation errors
(`[{"loc": ["body", "email"], "msg": "Field required"}]`, shown as
`email: Field required`).

| Answer | `upload_init` / `upload` | `confirm` | poll |
|---|---|---|---|
| 2xx as above | next step | **accepted**: token saved, job `PROCESSING` | `200` with review content: **complete**; `202`: still processing |
| 429 | rate limited → `QUEUED`, retried after `Retry-After` (capped at 24 h) or the polling schedule | same; nothing was created | rate limited → next poll after `Retry-After` or schedule |
| other 4xx | definitive failure | definitive failure (rejected) | `404`: invalid token → `FAILED`; other: retried on schedule |
| 5xx | definitive failure | **outcome unknown** | retried on schedule, except a terminal generation failure → `FAILED_NEEDS_MANUAL` |
| 200 without the expected fields | definitive failure | `success: false`: rejected; no token or unparseable: **outcome unknown** | no `sections` or `content`: not a review, retried on schedule |
| no answer (timeout, dropped connection) | definitive failure | **outcome unknown** (a connection that never opened is definitive) | retried on schedule |

A definitive failure happened before anything existed at the provider, or the
provider refused the request, so the job is `FAILED`, or is handed to the fallback
when one is configured. An **outcome unknown** may have created a submission:
the job is parked `SUBMITTED` / `UNCERTAIN` and is never resent or handed to the
fallback automatically. The provider gives no idempotency key and its page asks
users not to resubmit. Reconcile it the way `reviewloop status` says: import the
token from the provider's email (`reviewloop import-token`) or retry by hand.
Email token ingestion attaches it automatically when enabled.

## Input limits and preflight

The upload page says **"Max 10MB • First 15 pages analyzed"** and its script rejects
files over `10 * 1024 * 1024` bytes. The tech overview adds that only
English-language papers are supported and that grounding in arXiv makes reviews
more accurate in fields like AI.

ReviewLoop checks the PDF before enqueueing (`request_review`, `reviewloop submit`)
and again on the pinned snapshot before every dispatch:

| Check | Result |
|---|---|
| larger than 10 MiB | rejected: `input_rejected` at enqueue; at submit `FAILED_NEEDS_MANUAL` with event `submit_input_rejected`, nothing sent, fallback not run |
| no `%PDF-` header in the first 1 KiB | rejected the same way |
| more than 15 pages (estimated) | accepted with a notice: `ManuscriptInput.notices` at enqueue, event `submit_input_notice` before dispatch, and a log warning |
| file name without `.pdf` | uploaded as `<name>.pdf` by both routes |

The page count is a heuristic (`/Type /Page` objects). It reads 0 for PDFs that keep
their page objects in compressed object streams, in which case no notice is given.
Language and field are not checked.

## Email, venue and score

- **Email** is required by `confirm-upload`; the provider mails the review link to
  it. Resolution at enqueue: `providers.stanford.email` from the project config, else
  the global config, else the active `reviewloop email` account; the job records it
  and every submission of the job sends that address. Without one, enqueueing fails
  with `submitter_email_unavailable`. The page warns that delivery to some addresses
  is unreliable, which is why the token from `confirm-upload` is saved at once.
- **Venue** is free text. The page lists ICLR, NeurIPS, ICML, CVPR, AAAI, IJCAI,
  ACL, EMNLP, OSDI, SOSP, VLDB and SIGMOD, and sends any other venue as typed under
  "Other". The API accepts any string; an empty venue is allowed. Resolution:
  `papers[].venue` → project `providers.stanford.venue` → global
  `providers.stanford.venue`. The job records the venue requested; a job stored
  without one is sent the venue configured at submission time.
- **Score**: `numerical_score` is fitted to ICLR 2025 reviews and, per the tech
  overview, shown only when the venue is ICLR. `review.md` prints it as
  `x/10 (ICLR-calibrated)`; it is absent or `null` for other venues.

## Configuration ownership

| Key | Owner | Default |
|---|---|---|
| `providers.stanford.base_url` | global | `https://paperreview.ai` (must be `https://`; `http://localhost` and `http://127.0.0.1` are allowed for testing) |
| `providers.stanford.fallback_mode` | global | `node_playwright`; `disabled` turns the fallback off |
| `providers.stanford.fallback_script` | global, project override | `tools/paperreview_fallback.mjs` |
| `providers.stanford.email` | global, project override | active email account |
| `providers.stanford.venue` | project (global fallback); `papers[].venue` per paper | none |

The backend identifier stays `stanford` everywhere: config sections, the job's
`backend`, tag triggers (`review-stanford/<paper-id>/*`) and the email token
patterns.

## Timeouts and rate limits

| Bound | Value | On expiry |
|---|---|---|
| `upload_init` request | 30 s | definitive failure |
| `upload` request | 5 min | definitive failure |
| `confirm` request | 5 min | outcome unknown |
| one poll request | 2 min | retried on schedule |
| one dispatch (primary or fallback) | 20 min | outcome unknown |
| review | `core.review_timeout_hours` (48 h) × min(pages, 15) / 15, at least 1 h; the full 48 h when the page count is unknown | `TIMEOUT` |

The review timeout stops scaling at 15 pages because later pages are not reviewed.
The page warns that reviews "can take hours or even longer" under load.

ReviewLoop sends at most `core.max_submissions_per_tick` (1) submissions per 30 s
tick and polls on `polling.schedule_minutes` with jitter. A 429 on any step is
honoured as described above; it never triggers the fallback.

## Results

A complete review is the `200` reply of `/api/review/{token}`, which carries
`title`, `venue`, `submission_date`, `sections` (`summary`, `strengths`,
`weaknesses`, `detailed_comments`, `questions`, `assessment`, and `full_review` when
the review could not be split), `numerical_score`, `content` (the whole review as
Markdown) and `has_feedback`. ReviewLoop writes `<state_dir>/artifacts/<job_id>/`:

| File | Content |
|---|---|
| `review.json` | the reply, verbatim |
| `review.md` | summary: title, venue, submission date, score, then the sections (or `content`) |
| `meta.json` | `job_id`, `paper_id`, `backend`, `token`, `generated_at`, `pdf_path`, `pdf_hash` and `snapshot_path` (the uploaded bytes), `provider` (`backend`, `name`, `base_url`) and `submission` (`channel`, `venue`, `version_no`, `round_no`, `submitted_at`, `provider_submission_date`) |

The review is stored against the job's pinned snapshot, so `meta.json.pdf_hash`
is the SHA-256 of the bytes the provider received, even if the source PDF changed
after enqueue. A restarted worker resumes polling with the saved token and never
submits again.

## Token handling

The token is the only key to a review. It is stored in the database (the job, its
review, and the events that record it) and in `meta.json`. It is kept out of:

- `reviewloop status` text and `--json` output: rows show `token_masked`, and event
  payloads and errors print `[redacted]`. `--show-token` reveals it.
- the shared operations (`get_job`, `get_review`, …): views never carry it.
- errors, logs and notifications: requests are described without their URL, since
  the review URL holds the token in its path.

If the database cannot save a receipt, the token is written to
`<state_dir>/recovery/receipt-<job_id>-<time>.json` (mode `0600`) and the error
names that file, not the token. Only if that write fails too does the error carry
the token, as the last place left to keep it.

## Fallback

With `fallback_mode = "node_playwright"`, a definitive primary failure hands the
attempt to `tools/paperreview_fallback.mjs`. It submits the same pinned PDF under the
same file name, email and venue through the provider's own upload form, and
watches the page's requests to report in the primary's terms:

```json
{"success": false, "stage": "confirm", "status": 422, "submitted": true, "error": "email: Field required"}
```

| Report | Treated as |
|---|---|
| `success: true` with `token` | accepted (`submitted_via_fallback`) |
| `rate_limited: true` (`retry_after_secs`) | rate limited → `QUEUED`; the fallback stays available |
| `submitted: false`, or a 4xx `status` | definitive → `FAILED_NEEDS_MANUAL`; the fallback stays available |
| anything else, or no report | outcome unknown → `SUBMITTED` / `UNCERTAIN` |

`submitted` turns true when the page sends `confirm-upload`. The fallback needs Node.js
and Playwright (`npm i playwright && npx playwright install chromium`). Its network
classification is covered by `shipped_fallback_script_classifies_outcomes_like_the_primary`.
The browser run itself is only exercised against the live page.

## Tests

| Case | Test |
|---|---|
| upload-init failure, upload failure, confirm rejection, validation detail | `tests/submit_outcome_http.rs` (`upload_init_failure_…`, `upload_failure_…`, `confirm_rejection_…`) |
| confirm without a valid receipt, lost response, timeouts | `confirm_200_…`, `confirm_response_lost_…`, `hung_upload_init_…`, `hung_confirm_…` |
| rate limits | `upload_init_rate_limit_…`, `confirm_429_…`, `integration_rate_limit_…`, `fallback_rate_limit_…` |
| processing, complete, invalid token, terminal failure | `tests/integration_mock_server.rs` (`integration_poll_…`) |
| preflight | `oversized_pdf_…`, `non_pdf_input_…`, `long_paper_…`, `request_review_rejects_…`, `request_review_reports_…` |
| restart resumes the same receipt; archive matches the uploaded snapshot | `restart_resumes_the_same_receipt_and_archives_the_uploaded_snapshot` |
| token stays out of errors and status | `poll_network_error_never_records_the_token`, `status_output_redacts_tokens_unless_shown`, `unsaved_receipt_goes_to_a_private_recovery_file_not_the_log` |
| primary and fallback send the same input | `primary_and_fallback_send_the_same_manuscript_email_and_venue` |

## Real-service acceptance

**Status: not yet run.** Mock results above do not stand in for it. It needs a
manuscript the owner has authorized for submission to paperreview.ai and a
submitter email whose inbox can be checked.

1. Pick the test PDF (English, under 10 MiB) and record its SHA-256.
2. `reviewloop submit --paper-id <id>`, then `reviewloop status --paper-id <id> --json`:
   record the job id, `submitted` event time and `pdf_hash`. Do not resubmit while
   the review is pending.
3. When the email arrives, confirm its token matches the job's
   (`reviewloop status --show-token`).
4. After `COMPLETED`, keep `review.json`, `review.md` and `meta.json`, and check
   `meta.json.pdf_hash` against step 1.
5. Compare the live `get-upload-url`, `confirm-upload` and review replies with the
   inferred fixtures, and replace those fixtures with sanitized live copies.

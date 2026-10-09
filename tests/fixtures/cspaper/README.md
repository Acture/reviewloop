# CSPaper API fixtures

Sanitized responses of the CSPaper Agentic Review API used by
`src/backend/cspaper.rs` unit tests and `tests/cspaper_backend.rs`. None of
them carries an API key, a real job id or a real manuscript.

Provenance (2026-10-09):

| File | Source |
| --- | --- |
| `error_401.json`, `error_403.json` | Verbatim live responses of `https://cspaper.org/api/platform/reviews/<id>` without a key and with an invalid key. |
| `submit_accepted.json` | The documented `202` body of `POST /api/platform/review` (platform page at `https://cspaper.org/platform/review`), with the job id from the official examples' README. |
| `review_*.json` | The documented `GET /api/platform/reviews/{job_id}` body (same page): `result` is markdown, `result_summary` a JSON-encoded string. Status values follow `common/review_status.py` of `https://github.com/cspaper/platform-examples`; `deskReject` and `failed_reason` come from that repository's poll script. Field values are invented. |
| `error_400.json`, `error_404.json`, `error_422.json` | Built from the documented status codes and the live error envelope `{"status":N,"data":{"message":..,"details":null}}`; the message texts are invented. |

None was captured from an authenticated run: no key was available. Record the
real asynchronous review acceptance separately once a user-authorized key and
test manuscript exist, and replace the reconstructed bodies with captured,
sanitized ones then.

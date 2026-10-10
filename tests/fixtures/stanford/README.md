# Stanford Agentic Reviewer fixtures

Sanitized provider replies. The adapter's unit tests (`src/backend/stanford.rs`) parse
every one of them through the types and helpers the live path uses, so a re-captured
fixture whose shape changed fails there. The mock providers in `tests/` build their
replies inline in the same shapes.

Checked against `https://paperreview.ai` on 2026-10-09; see
`docs/providers/stanford.md` for the full contract.

| File | Source | How it was obtained |
| --- | --- | --- |
| `review-404.json` | **observed** | `GET /api/review/<unknown token>` → 404, body verbatim |
| `get-upload-url-405.json` | **observed** | `GET /api/get-upload-url` → 405 (`allow: POST`), body verbatim |
| `get-upload-url-422.json` | **observed** | `POST /api/get-upload-url` with `{}` → 422, body verbatim |
| `confirm-upload-422.json` | **observed** | `POST /api/confirm-upload` with no `s3_key`/`email` → 422, body verbatim |
| `get-upload-url-200.json` | inferred | field names from `/static/upload.js`; bucket, key and signature values are placeholders |
| `confirm-upload-200.json` | inferred | `success`, `message`, `token` from `/static/upload.js`; values are placeholders |
| `rate-limit-429.json` | inferred | `detail` field from `/static/upload.js`; text is the script's own fallback message |
| `review-202.json` | inferred | `detail` field from `/static/review.js`; text is a placeholder |
| `review-200.json` | inferred | fields `/static/review.js` renders (`title`, `venue`, `submission_date`, `sections.*`, `numerical_score`, `content`, `has_feedback`); text is a placeholder |

Only requests that fail validation or name an unknown token were sent, so nothing was
uploaded or submitted. The inferred replies have not been seen from the live service;
the real-service acceptance run in `docs/providers/stanford.md` is what confirms them.

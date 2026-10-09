#!/usr/bin/env node

// Submit a paper through paperreview.ai's own upload form, for when the direct API
// route fails definitively. Prints one JSON report line: on stdout with the token when
// the provider accepted the paper, on stderr (exit 1) otherwise. The report uses the
// primary backend's terms, so reviewloop applies one outcome policy to both routes:
//
//   { success, token?, error?, stage, status?, submitted, rate_limited?, retry_after_secs?, rejected? }
//
// `stage` is the step reached (upload_init → upload → confirm) and `status` the HTTP
// status the provider gave it. `submitted` is true once confirm-upload, the request that
// creates the submission, has been sent: from then on only confirm's own answer counts.
// `rejected` marks a 2xx confirm answer with `success: false`. The token is read from
// confirm-upload's reply, never from the page, which renders a missing one as "undefined".

import { fileURLToPath } from 'node:url';
import { realpathSync } from 'node:fs';
import { readFile } from 'node:fs/promises';

const RANK = { upload_init: 1, upload: 2, confirm: 3 };

/** What the page did, from its network traffic. */
export function newFacts() {
  return {
    stage: null,
    confirmSent: false,
    responses: {},
    pending: [],
    confirmFailed: null,
    dialog: null,
  };
}

/** The submission step a request belongs to, or null for anything else the page sends. */
export function stageOf({ url, method, contentType }, apiOrigin) {
  if (method !== 'POST') return null;
  const target = new URL(url);
  if (target.origin === apiOrigin) {
    if (target.pathname === '/api/get-upload-url') return 'upload_init';
    if (target.pathname === '/api/confirm-upload') return 'confirm';
    return null;
  }
  // The presigned upload is the page's only cross-origin multipart POST; the analytics
  // beacons the live page sends (gtag) post without one.
  return (contentType ?? '').toLowerCase().startsWith('multipart/form-data') ? 'upload' : null;
}

/** A request for `stage` was sent. Steps only move forward, and stop at confirm. */
export function noteRequest(facts, stage) {
  if (!stage || facts.confirmSent || RANK[stage] < (RANK[facts.stage] ?? 0)) return;
  facts.stage = stage;
  if (stage === 'confirm') facts.confirmSent = true;
}

/** `stage` got an answer; returns the record its body is added to. */
export function noteResponse(facts, stage, { status, retryAfterSecs }) {
  if (!stage) return null;
  const reply = { status, retryAfterSecs, detail: null, success: null, token: null };
  facts.responses[stage] = reply;
  return reply;
}

function describeValidation(error) {
  const field = (error?.loc ?? []).filter((part) => part !== 'body').join('.');
  const message = error?.msg ?? String(error);
  return field ? `${field}: ${message}` : message;
}

/** Read the provider's JSON reply the way its page does (`detail`, else `message`). */
export function noteAnswer(reply, body) {
  const detail = body?.detail;
  reply.detail = typeof detail === 'string' ? detail
    : Array.isArray(detail) ? detail.map(describeValidation).join('; ')
    : typeof body?.message === 'string' ? body.message
    : null;
  reply.success = typeof body?.success === 'boolean' ? body.success : null;
  reply.token = typeof body?.token === 'string' && body.token.trim() ? body.token.trim() : null;
}

/** Turn what was observed into the report reviewloop reads. */
export function classify(facts, error) {
  // Once confirm-upload was sent, nothing else says whether the paper was taken.
  const stage = facts.confirmSent ? 'confirm' : facts.stage;
  const reply = stage ? facts.responses[stage] ?? null : null;
  const confirmed = stage === 'confirm' && reply !== null && reply.status >= 200 && reply.status < 300;
  if (confirmed && reply.success === true && reply.token) {
    return { success: true, token: reply.token, stage, submitted: true };
  }
  const report = { success: false, stage, submitted: facts.confirmSent };
  let message = facts.dialog ?? reply?.detail ?? facts.confirmFailed;
  if (reply) {
    report.status = reply.status;
    if (reply.status === 429) {
      report.rate_limited = true;
      if (Number.isFinite(reply.retryAfterSecs)) {
        report.retry_after_secs = reply.retryAfterSecs;
      }
    }
    if (confirmed && reply.success === false) {
      report.rejected = true;
    } else if (confirmed) {
      message = reply.success === true
        ? 'confirm-upload succeeded without a token'
        : `confirm-upload answered ${reply.status} without a readable receipt`;
    }
  }
  report.error = message ?? String(error ?? 'no answer from the provider');
  return report;
}

function getArg(name, required = false) {
  const idx = process.argv.indexOf(name);
  if (idx === -1) {
    if (required) {
      throw new Error(`missing required argument: ${name}`);
    }
    return null;
  }
  return process.argv[idx + 1] ?? null;
}

function retryAfterSecs(response) {
  const raw = response.headers()['retry-after'];
  const secs = raw === undefined ? NaN : Number.parseInt(raw, 10);
  return Number.isFinite(secs) ? secs : undefined;
}

/** Record each step's request and answer as the page's script makes them. */
function watch(page, facts, apiOrigin) {
  const stageFor = (request) => stageOf(
    { url: request.url(), method: request.method(), contentType: request.headers()['content-type'] },
    apiOrigin,
  );
  page.on('request', (request) => noteRequest(facts, stageFor(request)));
  page.on('requestfailed', (request) => {
    if (stageFor(request) === 'confirm') {
      facts.confirmFailed = `confirm-upload got no response: ${request.failure()?.errorText ?? 'unknown'}`;
    }
  });
  page.on('response', (response) => {
    const stage = stageFor(response.request());
    // Recorded at once, so the status is known before the page reacts to it.
    const reply = noteResponse(facts, stage, {
      status: response.status(),
      retryAfterSecs: retryAfterSecs(response),
    });
    if (reply && stage !== 'upload') {
      facts.pending.push(response.json().then(
        (body) => noteAnswer(reply, body),
        () => noteAnswer(reply, null),
      ));
    }
  });
  // The form reports client-side problems (size, missing file) with alert().
  page.on('dialog', async (dialog) => {
    facts.dialog = dialog.message();
    await dialog.dismiss().catch(() => {});
  });
}

async function main(facts) {
  const baseUrl = getArg('--base-url', true);
  const pdfPath = getArg('--pdf', true);
  const fileName = getArg('--filename') ?? pdfPath.split(/[\\/]/).pop();
  const email = getArg('--email', true);
  const venue = getArg('--venue', false);

  // Imported here so a missing playwright install is reported as a clean pre-submit failure.
  const { chromium } = await import('playwright');
  const browser = await chromium.launch({ headless: true });

  try {
    const page = await browser.newPage();
    watch(page, facts, new URL(baseUrl).origin);
    await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });

    await page.setInputFiles('#pdf', {
      name: fileName,
      mimeType: 'application/pdf',
      buffer: await readFile(pdfPath),
    });
    await page.fill('#email', email);

    if (venue && venue.trim()) {
      const listed = await page.$eval(
        '#venue',
        (select, v) => Array.from(select.options).some(o => o.value === v),
        venue,
      );
      if (listed) {
        await page.selectOption('#venue', venue);
      } else {
        await page.selectOption('#venue', 'Other');
        await page.fill('#customVenue', venue);
      }
    }

    await page.click('#submitBtn');
    // The form ends with either the token or an error box (red, or yellow for a rate limit).
    await page.waitForSelector('#tokenDisplay, #result.bg-red-50, #result.bg-yellow-50', {
      timeout: 6 * 60 * 1000,
    });
  } finally {
    // Reply bodies are read through the browser, so settle them before closing it.
    await Promise.allSettled(facts.pending);
    await browser.close().catch(() => {});
  }
  return classify(facts, null);
}

// Run when executed, not when imported (tests import the functions above). Node
// resolves symlinks in `import.meta.url`, so compare real paths: `/var` vs
// `/private/var` on macOS, or a Homebrew symlink, must not turn a run into a no-op.
const entry = process.argv[1];
if (entry && realpathSync(entry) === fileURLToPath(import.meta.url)) {
  const facts = newFacts();
  main(facts)
    .then((report) => {
      if (report.success) {
        console.log(JSON.stringify(report));
      } else {
        console.error(JSON.stringify(report));
        process.exit(1);
      }
    })
    .catch((error) => {
      console.error(JSON.stringify(classify(facts, error)));
      process.exit(1);
    });
}

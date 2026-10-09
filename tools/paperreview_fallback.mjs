#!/usr/bin/env node

// Submit a paper through paperreview.ai's own upload form, for when the direct API
// route fails definitively. Prints one JSON report line: on stdout with the token when
// the provider accepted the paper, on stderr (exit 1) otherwise. The report uses the
// primary backend's terms, so reviewloop applies one outcome policy to both routes:
//
//   { success, token?, error?, stage, status?, submitted, rate_limited?, retry_after_secs? }
//
// `stage` is the step reached (upload_init → upload → confirm) and `status` the HTTP
// status the provider gave it. `submitted` is true once confirm-upload, the request that
// creates the submission, has been sent: from then on only a definite answer is final.

import { fileURLToPath } from 'node:url';
import { realpathSync } from 'node:fs';
import { readFile } from 'node:fs/promises';

/** What the page did, from its network traffic. */
export function newFacts() {
  return {
    stage: null,
    confirmSent: false,
    responses: {},
    pending: [],
    confirmFailed: null,
    dialog: null,
    token: null,
  };
}

/** Turn what was observed into the report reviewloop reads. */
export function classify(facts, error) {
  if (facts.token) {
    return { success: true, token: facts.token, stage: 'confirm', submitted: true };
  }
  // Null when no request was made (for example playwright failed to load).
  const stage = facts.stage;
  const reply = stage ? facts.responses[stage] ?? null : null;
  const report = {
    success: false,
    stage,
    submitted: facts.confirmSent,
    error: facts.dialog ?? reply?.detail ?? facts.confirmFailed ?? String(error ?? 'no token shown'),
  };
  if (reply) {
    report.status = reply.status;
    if (reply.status === 429) {
      report.rate_limited = true;
      if (Number.isFinite(reply.retryAfterSecs)) {
        report.retry_after_secs = reply.retryAfterSecs;
      }
    }
  }
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

async function detailOf(response) {
  try {
    const body = await response.json();
    if (typeof body?.detail === 'string') return body.detail;
    if (Array.isArray(body?.detail)) return body.detail.map(d => d?.msg ?? String(d)).join('; ');
    return null;
  } catch {
    return null;
  }
}

function retryAfterSecs(response) {
  const raw = response.headers()['retry-after'];
  const secs = raw === undefined ? NaN : Number.parseInt(raw, 10);
  return Number.isFinite(secs) ? secs : undefined;
}

/** Record each step's request and answer as the page's script makes them. */
function watch(page, facts, apiOrigin) {
  const stageOf = (request) => {
    const url = new URL(request.url());
    if (url.origin === apiOrigin && url.pathname === '/api/get-upload-url') return 'upload_init';
    if (url.origin === apiOrigin && url.pathname === '/api/confirm-upload') return 'confirm';
    // The presigned upload target is the only POST the form sends elsewhere.
    if (url.origin !== apiOrigin && request.method() === 'POST') return 'upload';
    return null;
  };
  page.on('request', (request) => {
    const stage = stageOf(request);
    if (!stage) return;
    facts.stage = stage;
    if (stage === 'confirm') facts.confirmSent = true;
  });
  page.on('requestfailed', (request) => {
    if (stageOf(request) === 'confirm') {
      facts.confirmFailed = `confirm-upload got no response: ${request.failure()?.errorText ?? 'unknown'}`;
    }
  });
  page.on('response', (response) => {
    const stage = stageOf(response.request());
    if (!stage) return;
    // Recorded at once, so the status is known before the page reacts to it.
    const reply = { status: response.status(), detail: null, retryAfterSecs: retryAfterSecs(response) };
    facts.responses[stage] = reply;
    if (stage !== 'upload') {
      facts.pending.push(detailOf(response).then((detail) => { reply.detail = detail; }));
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
    // The form shows either the token or an error box (red, or yellow for a rate limit).
    await page.waitForSelector('#tokenDisplay, #result.bg-red-50, #result.bg-yellow-50', {
      timeout: 6 * 60 * 1000,
    });
    const token = (await page.textContent('#tokenDisplay').catch(() => null))?.trim();
    if (token) {
      facts.token = token;
    }
    await Promise.allSettled(facts.pending);
    return classify(facts, null);
  } finally {
    await browser.close().catch(() => {});
  }
}

// Run when executed, not when imported (tests import `classify`). Node resolves
// symlinks in `import.meta.url`, so compare real paths: `/var` vs `/private/var` on
// macOS, or a Homebrew symlink, must not turn a run into a silent no-op.
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

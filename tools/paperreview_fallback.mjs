#!/usr/bin/env node

// Flips just before the submit click. From then on the provider may hold the paper, so
// reviewloop treats any failure as "outcome unknown" instead of retrying.
let submitted = false;

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

async function main() {
  const baseUrl = getArg('--base-url', true);
  const pdfPath = getArg('--pdf', true);
  const email = getArg('--email', true);
  const venue = getArg('--venue', false);

  // Imported here so a missing playwright install is reported as a clean pre-submit failure.
  const { chromium } = await import('playwright');
  const browser = await chromium.launch({ headless: true });

  try {
    const page = await browser.newPage();
    await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });

    await page.setInputFiles('#pdf', pdfPath);
    await page.fill('#email', email);

    if (venue && venue.trim()) {
      const selected = await page.$eval(
        '#venue',
        (el, v) => {
          const select = el;
          const options = Array.from(select.options).map(o => o.value);
          return options.includes(v) ? v : null;
        },
        venue,
      );

      if (selected) {
        await page.selectOption('#venue', selected);
      } else {
        await page.selectOption('#venue', 'Other');
        await page.fill('#customVenue', venue);
      }
    }

    submitted = true;
    await page.click('#submitBtn');
    await page.waitForSelector('#tokenDisplay', { timeout: 120000 });

    const token = (await page.textContent('#tokenDisplay'))?.trim();
    if (!token) {
      throw new Error('tokenDisplay is empty');
    }

    console.log(JSON.stringify({ success: true, token }));
  } finally {
    await browser.close().catch(() => {});
  }
}

main().catch((error) => {
  console.error(JSON.stringify({ success: false, submitted, error: String(error) }));
  process.exit(1);
});

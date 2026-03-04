#!/usr/bin/env node

import { chromium } from 'playwright';

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

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  try {
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

    await page.click('#submitBtn');
    await page.waitForSelector('#tokenDisplay', { timeout: 120000 });

    const token = (await page.textContent('#tokenDisplay'))?.trim();
    if (!token) {
      throw new Error('tokenDisplay is empty');
    }

    console.log(JSON.stringify({ success: true, token }));
    await browser.close();
  } catch (error) {
    console.error(JSON.stringify({ success: false, error: String(error) }));
    await browser.close();
    process.exit(1);
  }
}

main().catch((error) => {
  console.error(JSON.stringify({ success: false, error: String(error) }));
  process.exit(1);
});

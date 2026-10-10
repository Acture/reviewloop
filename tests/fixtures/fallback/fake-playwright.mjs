// A stand-in for the `playwright` package, so tests can run the shipped fallback
// script without a browser. Its page replays the network exchanges of the scenario
// named in `scenario.json` (next to this file) when the form is submitted, and never
// shows a result, as a redesigned page would not.
import { readFileSync } from 'node:fs';

const scenario = JSON.parse(readFileSync(new URL('./scenario.json', import.meta.url), 'utf8'));

class Emitter {
  handlers = {};
  on(event, handler) {
    (this.handlers[event] ??= []).push(handler);
  }
  emit(event, value) {
    for (const handler of this.handlers[event] ?? []) handler(value);
  }
}

const ENDPOINTS = { upload_init: '/api/get-upload-url', confirm: '/api/confirm-upload' };

function exchange(apiOrigin, [stage, status, body]) {
  const url = stage === 'upload' ? 'https://bucket.example/' : apiOrigin + ENDPOINTS[stage];
  const contentType = stage === 'upload_init' ? 'application/json' : 'multipart/form-data; boundary=fake';
  const request = {
    url: () => url,
    method: () => 'POST',
    headers: () => ({ 'content-type': contentType }),
    failure: () => null,
  };
  const response = {
    request: () => request,
    status: () => status,
    headers: () => ({}),
    json: async () => {
      if (body === null) throw new Error('body is not JSON');
      return body;
    },
  };
  return { request, response };
}

export const chromium = {
  async launch() {
    return {
      async newPage() {
        const page = new Emitter();
        let apiOrigin = null;
        page.goto = async (url) => { apiOrigin = new URL(url).origin; };
        page.setInputFiles = async () => {};
        page.fill = async () => {};
        page.$eval = async () => true;
        page.selectOption = async () => {};
        page.click = async () => {
          for (const step of scenario) {
            const { request, response } = exchange(apiOrigin, step);
            page.emit('request', request);
            page.emit('response', response);
          }
        };
        page.waitForSelector = async () => {
          throw new Error('Timeout exceeded waiting for the result');
        };
        return page;
      },
      async close() {},
    };
  },
};

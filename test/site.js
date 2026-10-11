// velox-grid :: test/site.js — local fixture site for e2e tests. No deps.
import http from 'node:http';

export function startSite({ port = 0 } = {}) {
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://x');
    const p = url.pathname;
    if (p === '/simple') {
      res.writeHead(200, { 'content-type': 'text/html' });
      return res.end('<html><head><title>Grid Test</title></head><body><h1>Hello Grid</h1><ul><li class="item">alpha</li><li class="item">beta</li></ul><a href="/simple">self</a></body></html>');
    }
    if (p === '/slow') {
      return setTimeout(() => { res.writeHead(200, { 'content-type': 'text/html' }); res.end('<html><body><h1>slow page</h1></body></html>'); }, 3000);
    }
    res.writeHead(500, { 'content-type': 'text/plain' });
    res.end('nope');
  });
  return new Promise((resolveStart) => server.listen(port, '127.0.0.1', () => resolveStart({ server, url: 'http://127.0.0.1:' + server.address().port })));
}

export function startWebhookReceiver({ port = 0 } = {}) {
  const state = { requests: [], onRequest: null, server: null, url: '' };
  state.server = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8');
      let body = null;
      try { body = JSON.parse(raw); } catch {}
      state.requests.push({ url: req.url, headers: req.headers, raw, body });
      state.onRequest?.({ url: req.url, headers: req.headers, raw, body });
      res.writeHead(200); res.end('ok');
    });
  });
  return new Promise((resolveStart) => state.server.listen(port, '127.0.0.1', () => {
    state.url = 'http://127.0.0.1:' + state.server.address().port;
    resolveStart(state);
  }));
}

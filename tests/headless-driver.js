#!/usr/bin/env node
//
// Runs the Tinytest suite without a browser.
//
// `meteor test-packages` normally serves a browser-based reporter, and the
// official console driver (test-in-console) drives headless Chrome through
// puppeteer. Every test in this package is server-side, so neither is needed:
// this speaks raw DDP to the already-running test server, calls the
// `tinytest/run` method and prints the reports as they stream back.
//
//   meteor test-packages --release METEOR@2.15 --port 3199 ./   # in one shell
//   node tests/headless-driver.js                               # in another
//
// Exits non-zero if any test failed, so it can gate CI. ./test.sh --full does
// both halves for you.

const DEFAULT_URL = 'ws://localhost:3199/websocket';
const URL = process.argv[2] || process.env.TEST_URL || DEFAULT_URL;
const TIMEOUT_MS = Number(process.env.TEST_TIMEOUT_MS || 300000);

const RUN_ID = 'headless-' + Date.now();
const SUB = 'tinytest_results_subscription';
const COLL = 'tinytest_results_collection';

// --- socket: node's global WebSocket when available, else Meteor's bundled
// faye-websocket (searched in the local package cache).
function openSocket(url) {
  if (typeof WebSocket === 'function') {
    const ws = new WebSocket(url);
    return {
      onOpen: fn => ws.addEventListener('open', fn),
      onMessage: fn => ws.addEventListener('message', ev => fn(ev.data)),
      onClose: fn => ws.addEventListener('close', fn),
      onError: fn => ws.addEventListener('error', fn),
      send: data => ws.send(data),
      close: () => ws.close(),
    };
  }

  const fs = require('node:fs');
  const path = require('node:path');
  const root = path.join(process.env.HOME || '', '.meteor', 'packages', 'ddp-server');
  const version = fs.readdirSync(root).sort().pop();
  const Faye = require(path.join(root, version, 'npm', 'node_modules', 'faye-websocket'));
  const ws = new Faye.Client(url);
  return {
    onOpen: fn => ws.on('open', fn),
    onMessage: fn => ws.on('message', ev => fn(ev.data)),
    onClose: fn => ws.on('close', fn),
    onError: fn => ws.on('error', fn),
    send: data => ws.send(data),
    close: () => ws.close(),
  };
}

const socket = openSocket(URL);
const results = new Map();
let finished = false;

const send = msg => socket.send(JSON.stringify(msg));

function record(report) {
  if (!report || !report.test) return; // tinytest reports carry `test`, not `name`
  const key = (report.groupPath || []).join(' > ') + ' - ' + report.test;
  if (!results.has(key)) results.set(key, []);
  results.get(key).push(report);
}

function finish(forcedCode) {
  if (finished) return;
  finished = true;

  let failed = 0;
  console.log('');
  for (const [name, reports] of results) {
    const events = reports.reduce((all, r) => all.concat(r.events || []), []);
    const fails = events.filter(e => e.type === 'fail' || e.type === 'exception');
    if (fails.length) {
      failed++;
      console.log('FAIL  ' + name);
      fails.slice(0, 3).forEach(f => console.log('        ' + JSON.stringify(f.details || f).slice(0, 300)));
    } else {
      console.log('ok    ' + name + '   (' + events.filter(e => e.type === 'ok').length + ' assertions)');
    }
  }
  console.log('\n' + results.size + ' tests, ' + failed + ' failed');

  try {
    socket.close();
  } catch (e) {
    // Already closing or closed - we are exiting anyway, and swallowing this
    // must not mask the exit code that carries the actual test result.
  }
  process.exit(forcedCode != null ? forcedCode : failed ? 1 : 0);
}

socket.onOpen(() => send({ msg: 'connect', version: '1', support: ['1'] }));

socket.onMessage(raw => {
  let msg;
  try {
    msg = JSON.parse(raw);
  } catch (e) {
    return;
  }

  if (msg.msg === 'connected') {
    send({ msg: 'sub', id: 'results', name: SUB, params: [RUN_ID] });
    send({ msg: 'method', id: 'run', method: 'tinytest/run', params: [RUN_ID] });
    return;
  }

  if (msg.msg === 'ping') {
    send({ msg: 'pong', id: msg.id });
    return;
  }

  if ((msg.msg === 'added' || msg.msg === 'changed') && msg.collection === COLL) {
    const fields = msg.fields || {};
    for (const key of Object.keys(fields)) {
      // The server writes a `complete` key once every test has finished.
      if (key === 'complete') setTimeout(() => finish(), 300);
      else record(fields[key]);
    }
  }

  if (msg.msg === 'result' && msg.id === 'run' && msg.error) {
    console.log('tinytest/run failed: ' + JSON.stringify(msg.error));
    finish(1);
  }
});

socket.onError(err => {
  console.log('connection error: ' + (err && err.message ? err.message : err));
  console.log('is `meteor test-packages ./` running on ' + URL + ' ?');
  process.exit(2);
});

socket.onClose(() => {
  if (!finished) {
    console.log('connection closed before the run completed');
    process.exit(2);
  }
});

setTimeout(() => {
  console.log('\ntimed out after ' + TIMEOUT_MS + 'ms');
  finish(1);
}, TIMEOUT_MS);

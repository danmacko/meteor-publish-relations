// Harness for the fast unit layer.
//
// These tests load the package's server modules into a vm with Meteor stubbed
// out, so they run in plain node in milliseconds - no Meteor, no MongoDB, no
// DDP. That buys deterministic control over things the Tinytest layer cannot
// reach: what exactly sits in the deferred queue, what happens when a
// subscription is already deactivated, or how a burst of release() calls
// coalesces. The Tinytest suite in ../ still owns everything that needs a real
// server (DDP message order, observer lifecycle against Mongo).
//
// The loader strips ES module syntax rather than transpiling, which is enough
// for these files and keeps the layer dependency-free. If a module ever gains
// a runtime `import`, load it through Meteor instead of here.

// node: prefix - always the builtin, never a resolver hook or a cached entry.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const LIB = path.join(__dirname, '..', '..', 'lib', 'server');

const stripModuleSyntax = src =>
  src
    .replace(/^import .*$/gm, '')
    .replace(/^export default /gm, '')
    .replace(/^export /gm, '');

// Loads the given lib/server files (in dependency order) into one sandbox and
// returns it. `globals` is merged in first, so tests can supply their own
// Meteor stub.
function loadModules(relPaths, globals) {
  const src = relPaths.map(p => stripModuleSyntax(fs.readFileSync(path.join(LIB, p), 'utf8'))).join('\n');
  const sandbox = Object.assign({ console }, globals);
  vm.createContext(sandbox);
  // Re-export the declarations the tests need; `this` is the sandbox.
  const exposed = ['CursorJoin', 'CursorMethods', 'HandlerController', 'isPublishedInSub'];
  const tail = exposed.map(n => `try { this.${n} = ${n} } catch (e) {}`).join('\n');
  vm.runInContext(src + '\n' + tail, sandbox);
  return sandbox;
}

// A Meteor stub whose defer queue the test drives by hand.
function makeMeteorStub(extra) {
  const queue = [];
  const Meteor = Object.assign({ defer: fn => queue.push(fn), isDevelopment: true }, extra);
  // Runs queued callbacks until the queue drains (a restart may queue another).
  Meteor._flush = function (maxRounds = 20) {
    let rounds = 0;
    while (queue.length && rounds++ < maxRounds) queue.splice(0).forEach(fn => fn());
  };
  return Meteor;
}

// ---------------------------------------------------------------------------
// assertions
// ---------------------------------------------------------------------------

function createReporter(suiteName) {
  let failures = 0;
  let checks = 0;
  const lines = [];

  function check(label, actual, expected) {
    checks++;
    const a = JSON.stringify(actual);
    const e = JSON.stringify(expected);
    if (a === e) {
      lines.push('  ok    ' + label + '  ' + a);
    } else {
      failures++;
      lines.push('  FAIL  ' + label + '\n          got  ' + a + '\n          want ' + e);
    }
  }

  return {
    check,
    report() {
      console.log('\n' + suiteName);
      lines.forEach(l => console.log(l));
      return { failures, checks };
    },
  };
}

// ---------------------------------------------------------------------------
// fake mongo: cursors with live observers, enough for observeChanges semantics
// ---------------------------------------------------------------------------

function matches(doc, selector) {
  return Object.keys(selector).every(key => {
    const cond = selector[key];
    if (key === '$or') return cond.some(sub => matches(doc, sub));
    if (cond && typeof cond === 'object' && '$in' in cond) return cond.$in.indexOf(doc[key]) !== -1;
    return doc[key] === cond;
  });
}

class FakeDB {
  constructor() {
    this.colls = {};
    this.observers = [];
  }
  coll(name) {
    if (!this.colls[name]) this.colls[name] = new Map();
    return { _name: name, find: (selector = {}) => this._cursor(name, selector) };
  }
  _cursor(name, selector) {
    const db = this;
    return {
      _cursorDescription: { collectionName: name, selector },
      _getCollectionName: () => name,
      observeChanges(callbacks) {
        const observer = {
          coll: name,
          selector,
          callbacks,
          ids: new Set(),
          stopped: false,
          stop() {
            if (this.stopped) return;
            this.stopped = true;
            db.observers = db.observers.filter(o => o !== this);
          },
        };
        db.observers.push(observer);
        db.colls[name].forEach((doc, id) => {
          if (matches(doc, selector)) {
            observer.ids.add(id);
            callbacks.added(id, Object.assign({}, doc));
          }
        });
        return observer;
      },
    };
  }
  insert(name, doc) {
    this.coll(name);
    this.colls[name].set(doc._id, doc);
    this.observers.slice().forEach(o => {
      if (o.coll === name && matches(doc, o.selector)) {
        o.ids.add(doc._id);
        o.callbacks.added(doc._id, Object.assign({}, doc));
      }
    });
  }
  update(name, id, fields) {
    const doc = Object.assign(this.colls[name].get(id), fields);
    this.observers.slice().forEach(o => {
      if (o.coll !== name) return;
      const now = matches(doc, o.selector);
      const had = o.ids.has(id);
      if (now && had) o.callbacks.changed(id, fields);
      else if (now && !had) {
        o.ids.add(id);
        o.callbacks.added(id, Object.assign({}, doc));
      } else if (!now && had) {
        o.ids.delete(id);
        o.callbacks.removed(id);
      }
    });
  }
  remove(name, id) {
    this.colls[name].delete(id);
    this.observers.slice().forEach(o => {
      if (o.coll === name && o.ids.has(id)) {
        o.ids.delete(id);
        o.callbacks.removed(id);
      }
    });
  }
  live(name) {
    return this.observers.filter(o => o.coll === name);
  }
}

// ---------------------------------------------------------------------------
// fake Subscription, enforcing ddp-server's throwing semantics
// ---------------------------------------------------------------------------

const SERVER_MERGE = { useCollectionView: true, doAccountingForCollection: true };

// Mirrors MongoID.idStringify for the cases that matter here.
const idStringify = id => (typeof id === 'string' && id.length === 24 && /^[0-9a-f]*$/.test(id) ? '-' + id : id);

function makeSub(options = {}) {
  const strategy = options.strategy || SERVER_MERGE;
  const events = [];
  const sub = {
    _documents: new Map(),
    _idFilter: { idStringify },
    _session: { server: { getPublicationStrategy: () => strategy } },
    _deactivated: false,
    _isDeactivated() {
      return this._deactivated;
    },
    added(coll, id) {
      let set = this._documents.get(coll);
      if (!set) this._documents.set(coll, (set = new Set()));
      const key = idStringify(id);
      if (set.has(key)) {
        events.push(['changed', coll, id]);
        return;
      }
      set.add(key);
      events.push(['added', coll, id]);
    },
    changed(coll, id) {
      const set = this._documents.get(coll);
      if (strategy.useCollectionView && (!set || !set.has(idStringify(id)))) {
        throw new Error('Could not find element with id ' + id + ' to change');
      }
      events.push(['changed', coll, id]);
    },
    removed(coll, id) {
      const set = this._documents.get(coll);
      const key = idStringify(id);
      if (strategy.useCollectionView && (!set || !set.has(key))) {
        throw new Error('Removed nonexistent document ' + id);
      }
      if (set) set.delete(key);
      events.push(['removed', coll, id]);
    },
  };
  return { sub, events, isPublished: (coll, id) => !!(sub._documents.get(coll) || new Set()).has(idStringify(id)) };
}

module.exports = {
  loadModules,
  makeMeteorStub,
  createReporter,
  FakeDB,
  makeSub,
  idStringify,
  strategies: {
    SERVER_MERGE,
    NO_MERGE_NO_HISTORY: { useCollectionView: false, doAccountingForCollection: false },
  },
};

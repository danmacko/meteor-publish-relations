// CursorJoin in isolation: refcounting, the retraction queue and the deferred
// restart. Everything here is about state the Tinytest layer cannot inspect -
// what sits in the defer queue, and what happens when a release lands in the
// same tick as a push.

const { loadModules, makeMeteorStub, createReporter, makeSub, strategies } = require('./harness');

module.exports = function run() {
  const { check, report } = createReporter('unit: CursorJoin');

  // A join needs CursorMethods only as a prototype host, so stub it.
  function build(subOptions) {
    const Meteor = makeMeteorStub();
    const sandbox = loadModules(['cursor/published.js', 'cursor/join.js'], { Meteor, CursorMethods: class {} });
    const { sub, events, isPublished } = makeSub(subOptions);
    const cursorCalls = [];
    const methods = { sub, _registrySeq: 0, cursor: (...args) => cursorCalls.push(args) };
    const collection = { _name: 'authors', find: () => ({ _getCollectionName: () => 'authors' }) };
    const join = new sandbox.CursorJoin(methods, collection, {}, 'authors');
    return { join, sub, events, isPublished, cursorCalls, flush: Meteor._flush };
  }

  // Pushes ids as a given contributor, the way cursor.js frames each callback.
  function pushAs(join, contributorId, ...ids) {
    join.methods.sub._prContributorStack = join.methods.sub._prContributorStack || [];
    join.methods.sub._prContributorStack.push({ id: contributorId, parent: null });
    join.push(...ids);
    join.methods.sub._prContributorStack.pop();
  }

  // --- the retraction is deferred, and only touches published ids ----------
  {
    const t = build();
    t.sub.added('authors', 'a1'); // a1 was published; a2 never matched
    pushAs(t.join, 'c1', 'a1', 'a2');
    t.join.send();
    t.cursorCalls.length = 0;

    t.join.release('c1');
    check('retraction does not run inline', t.events.filter(e => e[0] === 'removed').length, 0);

    t.flush();
    check(
      'retracts the published id',
      t.events.filter(e => e[0] === 'removed').map(e => e[2]),
      ['a1']
    );
    check('restarts the observe once', t.cursorCalls.length, 1);
  }

  // --- ids are compared in their stringified form --------------------------
  {
    const objectLike = 'a1b2c3d4e5f6a1b2c3d4e5f6'; // 24 hex chars -> escaped in _documents
    const t = build();
    t.sub.added('authors', objectLike);
    pushAs(t.join, 'c1', objectLike);
    t.join.send();

    t.join.release('c1');
    t.flush();
    check(
      'object-id-shaped id is matched',
      t.events.filter(e => e[0] === 'removed').map(e => e[2]),
      [objectLike]
    );
  }

  // --- a duplicate release must not remove twice (the second throws) -------
  {
    const t = build();
    t.sub.added('authors', 'a1');
    pushAs(t.join, 'c1', 'a1');
    t.join.send();

    t.join.release('c1');
    pushAs(t.join, 'c2', 'a1');
    t.join.release('c2');
    t.flush();
    check('duplicate release removes once', t.events.filter(e => e[0] === 'removed').length, 1);
  }

  // --- an id re-joined before the restart runs is not retracted ------------
  {
    const t = build();
    t.sub.added('authors', 'a1');
    pushAs(t.join, 'c1', 'a1');
    t.join.send();

    t.join.release('c1');
    pushAs(t.join, 'c2', 'a1'); // another contributor picks it back up
    t.flush();
    check('re-joined id is not retracted', t.events.filter(e => e[0] === 'removed').length, 0);
    check('re-joined id stays in the $in', t.join.data, ['a1']);
  }

  // --- a stopped subscription neither retracts nor restarts ----------------
  {
    const t = build();
    t.sub.added('authors', 'a1');
    pushAs(t.join, 'c1', 'a1');
    t.join.sent = true;
    t.join.release('c1');
    t.cursorCalls.length = 0;
    t.sub._deactivated = true;

    t.flush();
    check('no retraction after teardown', t.events.filter(e => e[0] === 'removed').length, 0);
    check('no observe restart after teardown', t.cursorCalls.length, 0);
  }

  // --- without bookkeeping we never guess ---------------------------------
  {
    const t = build({ strategy: strategies.NO_MERGE_NO_HISTORY });
    pushAs(t.join, 'c1', 'a1', 'a2');
    t.join.send();
    t.join.release('c1');
    t.flush();
    check('no _documents proof -> no retraction', t.events.filter(e => e[0] === 'removed').length, 0);
  }

  // --- renamed/removed Meteor internals degrade, never crash ---------------
  {
    const t = build();
    t.sub.added('authors', 'a1');
    delete t.sub._session; // getPublicationStrategy gone
    delete t.sub._idFilter; // _idFilter gone -> identity fallback
    pushAs(t.join, 'c1', 'a1', 'a2');
    t.join.send();
    t.join.release('c1');
    t.flush();
    check(
      'survives renamed internals',
      t.events.filter(e => e[0] === 'removed').map(e => e[2]),
      ['a1']
    );
  }

  // --- a burst of releases coalesces into a single restart -----------------
  {
    const t = build();
    ['a1', 'a2', 'a3'].forEach(id => t.sub.added('authors', id));
    pushAs(t.join, 'c1', 'a1');
    pushAs(t.join, 'c2', 'a2');
    pushAs(t.join, 'c3', 'a3');
    t.join.send();
    t.cursorCalls.length = 0;

    t.join.release('c1');
    t.join.release('c2');
    t.join.release('c3');
    t.flush();
    check('three releases -> one restart', t.cursorCalls.length, 1);
    check('all three retracted', t.events.filter(e => e[0] === 'removed').length, 3);
  }

  return report();
};

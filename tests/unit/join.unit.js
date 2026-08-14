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
    const sandbox = loadModules(['cursor/published.js', 'cursor/contributor-context.js', 'cursor/join.js'], {
      Meteor,
      CursorMethods: class {},
    });
    const { sub, events, isPublished } = makeSub(subOptions);
    const cursorCalls = [];
    const methods = { sub, _registrySeq: 0, cursor: (...args) => cursorCalls.push(args) };
    const collection = { _name: 'authors', find: () => ({ _getCollectionName: () => 'authors' }) };
    const join = new sandbox.CursorJoin(methods, collection, {}, 'authors');
    // Pushes ids as a given contributor, the way cursor.js frames each callback.
    const pushAs = (contributorId, ...ids) =>
      sandbox.runInContributor({ id: contributorId, parent: null }, () => join.push(...ids));
    return { join, sub, events, isPublished, cursorCalls, pushAs, sandbox, Meteor, flush: Meteor._flush };
  }

  // --- the retraction is deferred, and only touches published ids ----------
  {
    const t = build();
    t.sub.added('authors', 'a1'); // a1 was published; a2 never matched
    t.pushAs('c1', 'a1', 'a2');
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
    t.pushAs('c1', objectLike);
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
    t.pushAs('c1', 'a1');
    t.join.send();

    t.join.release('c1');
    t.pushAs('c2', 'a1');
    t.join.release('c2');
    t.flush();
    check('duplicate release removes once', t.events.filter(e => e[0] === 'removed').length, 1);
  }

  // --- an id re-joined before the restart runs is not retracted ------------
  {
    const t = build();
    t.sub.added('authors', 'a1');
    t.pushAs('c1', 'a1');
    t.join.send();

    t.join.release('c1');
    t.pushAs('c2', 'a1'); // another contributor picks it back up
    t.flush();
    check('re-joined id is not retracted', t.events.filter(e => e[0] === 'removed').length, 0);
    check('re-joined id stays in the $in', t.join.data, ['a1']);
  }

  // --- a stopped subscription neither retracts nor restarts ----------------
  {
    const t = build();
    t.sub.added('authors', 'a1');
    t.pushAs('c1', 'a1');
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
    t.pushAs('c1', 'a1', 'a2');
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
    t.pushAs('c1', 'a1', 'a2');
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
    t.pushAs('c1', 'a1');
    t.pushAs('c2', 'a2');
    t.pushAs('c3', 'a3');
    t.join.send();
    t.cursorCalls.length = 0;

    t.join.release('c1');
    t.join.release('c2');
    t.join.release('c3');
    t.flush();
    check('three releases -> one restart', t.cursorCalls.length, 1);
    check('all three retracted', t.events.filter(e => e[0] === 'removed').length, 3);
  }

  // --- the contributor frame is scoped, never a shared stack ---------------
  {
    const t = build();
    const seen = [];
    t.sandbox.runInContributor({ id: 'outer', parent: null }, () => {
      seen.push(t.sandbox.currentContributor().id);
      t.sandbox.runInContributor({ id: 'inner', parent: 'outer' }, () => {
        seen.push(t.sandbox.currentContributor().id);
        seen.push(t.sandbox.currentContributor().parent);
      });
      seen.push(t.sandbox.currentContributor().id); // inner must not linger
    });
    seen.push(t.sandbox.currentContributor()); // nothing leaks out
    check('nested frames nest and restore', seen, ['outer', 'inner', 'outer', 'outer', null]);
  }

  // --- the deferred restart does not inherit the contributor that pushed ---
  {
    const t = build();
    t.join.send(); // sent, so the next push schedules a restart
    let frameDuringRestart = 'unset';
    t.join.methods.cursor = () => {
      frameDuringRestart = t.sandbox.currentContributor();
    };

    t.pushAs('c1', 'a1');
    t.flush();
    check('restart runs with no contributor frame', frameDuringRestart, null);
  }

  // --- a restart that throws is retried, not left dead ---------------------
  {
    const t = build();
    t.sub.added('authors', 'a1');
    t.pushAs('c1', 'a1');
    t.join.send();
    t.cursorCalls.length = 0;

    let failuresLeft = 2;
    const realCursor = t.join.methods.cursor;
    t.join.methods.cursor = (...args) => {
      if (failuresLeft-- > 0) throw new Error('transient mongo error');
      return realCursor(...args);
    };

    t.pushAs('c2', 'a2');
    t.flush();
    check('the failed attempt left no observe', t.cursorCalls.length, 0);
    check('a retry is scheduled', t.Meteor._pendingTimers(), 1);

    t.Meteor._flushTimers();
    check('the retry restarts the observe', t.cursorCalls.length, 1);
    check('recovery is silent', t.Meteor._debugs.length, 0);
  }

  // --- a retraction survives a failed restart, and is not sent twice -------
  {
    const t = build();
    t.sub.added('authors', 'a1');
    t.pushAs('c1', 'a1');
    t.join.send();
    t.cursorCalls.length = 0;

    let failed = false;
    const realCursor = t.join.methods.cursor;
    t.join.methods.cursor = (...args) => {
      if (failed) return realCursor(...args);
      failed = true;
      throw new Error('transient mongo error');
    };

    t.join.release('c1');
    t.flush();
    t.Meteor._flushTimers();
    check('the observe came back', t.cursorCalls.length, 1);
    check(
      'the retraction was sent exactly once',
      t.events.filter(e => e[0] === 'removed').map(e => e[2]),
      ['a1']
    );
  }

  // --- a restart that never succeeds gives up loudly ----------------------
  {
    const t = build();
    t.pushAs('c1', 'a1');
    t.join.send();
    t.cursorCalls.length = 0;
    t.join.methods.cursor = () => {
      throw new Error('mongo is down');
    };

    t.pushAs('c2', 'a2');
    t.flush();
    t.Meteor._flushTimers();

    check('no observe was created', t.cursorCalls.length, 0);
    check('no retry left dangling', t.Meteor._pendingTimers(), 0);
    check('gave up with an explicit log', t.Meteor._debugs.filter(d => d.indexOf('gave up restarting') !== -1).length, 1);
  }

  // --- the {$in} is a function of membership, not of push order ------------
  // Mongo keys its ObserveMultiplexer cache by the serialised cursor
  // description, so two joins holding the same members must produce a byte-equal
  // selector or they can never share an observer.
  {
    const a = build();
    const b = build();
    a.pushAs('c1', 'a3', 'a1', 'a2');
    b.pushAs('c1', 'a1', 'a2', 'a3');
    check('same members, different push order -> same selector', a.join._selector(), b.join._selector());
  }

  // --- and it survives churn, which is where the order actually drifts -----
  {
    const warm = build();
    const fresh = build();

    warm.pushAs('c1', 'a1');
    warm.pushAs('c2', 'a2');
    warm.pushAs('c3', 'a3');
    warm.join.release('c2'); // a2 leaves the middle of the array...
    warm.pushAs('c4', 'a2'); // ...and comes back appended at the end

    fresh.pushAs('c1', 'a1', 'a2', 'a3');

    check('a churned join matches a fresh one with the same members', warm.join._selector(), fresh.join._selector());
    check('and the underlying data really had drifted', warm.join.data, ['a1', 'a3', 'a2']);
  }

  // --- a custom selector gets the sorted array too --------------------------
  {
    const t = build();
    t.join.selector = _ids => ({ postId: _ids });
    t.pushAs('c1', 'a3', 'a1');
    check('custom selector receives sorted ids', t.join._selector(), { postId: { $in: ['a1', 'a3'] } });
  }

  return report();
};

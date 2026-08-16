// CursorJoin in isolation: contribution bookkeeping, the reconcile diff and the
// deferred restart. Everything here is about state the Tinytest layer cannot
// inspect - what sits in the defer queue, and what happens when a contributor
// is released in the same tick as a push.

const { loadModules, makeMeteorStub, createReporter, makeSub, strategies } = require('./harness');

module.exports = function run() {
  const { check, report } = createReporter('unit: CursorJoin');

  // A join needs CursorMethods only as a prototype host, so stub it. Contributors
  // are real HandlerControllers, because that is what the join keys by and what
  // drives release through stop().
  function build(subOptions) {
    const Meteor = makeMeteorStub();
    const sandbox = loadModules(
      ['cursor/published.js', 'cursor/contributor-context.js', 'handler_controller.js', 'cursor/join.js'],
      { Meteor, CursorMethods: class {} }
    );
    const { sub, events, isPublished } = makeSub(subOptions);
    const cursorCalls = [];
    const seqs = Object.create(null);
    const methods = {
      sub,
      cursor: (...args) => cursorCalls.push(args),
      _nextRegistryKey: (name, kind) => {
        const bucket = name + '#' + kind;
        const seq = seqs[bucket] || 0;
        seqs[bucket] = seq + 1;
        return bucket + seq;
      },
    };
    const collection = { _name: 'authors', find: () => ({ _getCollectionName: () => 'authors' }) };
    const join = new sandbox.CursorJoin(methods, collection, {}, 'authors');

    // Named contributors, so the tests read like the old string-keyed ones.
    // owner('a') is a root controller; owner('a', 'b') is 'b' nested under it,
    // which is exactly the tree cursor.js builds with handler.addBasic(id).
    const roots = new Map();
    const owner = (name, child) => {
      if (!roots.has(name)) roots.set(name, new sandbox.HandlerController());
      const root = roots.get(name);
      return child === undefined ? root : root.addBasic(child);
    };
    // Pushes ids as a given contributor, the way cursor.js frames each callback.
    const pushAs = (name, ...ids) => sandbox.runInContributor(owner(name), () => join.push(...ids));
    // A contributor's document leaves the result set: cursor.js stops its
    // controller, which is what releases the contribution.
    const stopOwner = (name, child) => owner(name, child).stop();

    return { join, sub, events, isPublished, cursorCalls, pushAs, owner, stopOwner, sandbox, Meteor, flush: Meteor._flush };
  }

  // --- the retraction is deferred, and only touches published ids ----------
  {
    const t = build();
    t.sub.added('authors', 'a1'); // a1 was published; a2 never matched
    t.pushAs('c1', 'a1', 'a2');
    t.join.send();
    t.cursorCalls.length = 0;

    t.stopOwner('c1');
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

    t.stopOwner('c1');
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

    t.stopOwner('c1');
    t.pushAs('c2', 'a1');
    t.stopOwner('c2');
    t.flush();
    check('duplicate release removes once', t.events.filter(e => e[0] === 'removed').length, 1);
  }

  // --- an id re-joined before the restart runs is not retracted ------------
  {
    const t = build();
    t.sub.added('authors', 'a1');
    t.pushAs('c1', 'a1');
    t.join.send();

    t.stopOwner('c1');
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
    t.stopOwner('c1');
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
    t.stopOwner('c1');
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
    t.stopOwner('c1');
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

    t.stopOwner('c1');
    t.stopOwner('c2');
    t.stopOwner('c3');
    t.flush();
    check('three releases -> one restart', t.cursorCalls.length, 1);
    check('all three retracted', t.events.filter(e => e[0] === 'removed').length, 3);
  }

  // --- a new contributor for an id already held does not restart -----------
  {
    const t = build();
    t.pushAs('c1', 'a1');
    t.join.send();
    t.cursorCalls.length = 0;

    t.pushAs('c2', 'a1'); // same id, second contributor -> union is unchanged
    t.flush();
    check('unchanged membership -> no restart', t.cursorCalls.length, 0);
    check('the contribution was still recorded', t.join.contributions.size, 2);

    t.stopOwner('c1'); // ...and it is what keeps the id alive now
    t.flush();
    check('still no restart, the id is still held', t.cursorCalls.length, 0);
    check('the id survives its first contributor', t.join.data, ['a1']);

    t.stopOwner('c2');
    t.flush();
    check('the last contributor leaving does restart', t.cursorCalls.length, 1);
    check('and empties the $in', t.join.data, []);
  }

  // --- a push from a callback that outlived its controller is dropped ------
  {
    const t = build();
    t.pushAs('c1', 'a1');
    t.join.send();
    t.cursorCalls.length = 0;

    const orphan = t.owner('c2');
    orphan.stop(); // its document left the result set while its callback yielded
    t.sandbox.runInContributor(orphan, () => t.join.push('a2'));
    t.flush();

    check('the orphaned push is not recorded', t.join.contributions.has(orphan), false);
    check('and never reaches the $in', t.join.data, ['a1']);
  }

  // --- the contributor is scoped, never a shared stack ---------------------
  {
    const t = build();
    const outer = t.owner('outer');
    const nested = t.owner('outer', 'child');
    const seen = [];
    t.sandbox.runInContributor(outer, () => {
      seen.push(t.sandbox.currentContributor() === outer);
      t.sandbox.runInContributor(nested, () => {
        seen.push(t.sandbox.currentContributor() === nested);
      });
      seen.push(t.sandbox.currentContributor() === outer); // nested must not linger
    });
    seen.push(t.sandbox.currentContributor()); // nothing leaks out
    check('nested contributors nest and restore', seen, [true, true, true, null]);
  }

  // --- stopping a parent releases its nested contributors too --------------
  {
    const t = build();
    ['a1', 'a2'].forEach(id => t.sub.added('authors', id));
    t.pushAs('parent', 'a1');
    t.sandbox.runInContributor(t.owner('parent', 'nested'), () => t.join.push('a2'));
    t.join.send();
    check('both contributions are in the $in', t.join.data.slice().sort(), ['a1', 'a2']);

    t.stopOwner('parent'); // the nested controller is a child, so stop() recurses
    t.flush();
    check('parent and nested ids both released', t.join.data, []);
    check(
      'both retracted',
      t.events.filter(e => e[0] === 'removed').map(e => e[2]).sort(),
      ['a1', 'a2']
    );
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
    t.sub.added('authors', 'a2');
    t.pushAs('c1', 'a1');
    t.pushAs('c2', 'a2'); // stays, so the join still has something to observe
    t.join.send();
    t.cursorCalls.length = 0;

    let failed = false;
    const realCursor = t.join.methods.cursor;
    t.join.methods.cursor = (...args) => {
      if (failed) return realCursor(...args);
      failed = true;
      throw new Error('transient mongo error');
    };

    t.stopOwner('c1');
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

  // --- a push of nothing leaves nothing behind -----------------------------
  // A changed callback is handed the update, not the document, so `push(doc.fk)`
  // is usually push(undefined). Recording a contributor for that would leave an
  // empty entry whose later removal schedules a reconcile that has nothing to
  // do - and, with no observer up yet, builds one over an empty {$in}.
  {
    const t = build();
    t.join.send(); // nothing collected, so no observer
    check('send builds nothing for an empty join', t.cursorCalls.length, 0);

    t.sandbox.runInContributor(t.owner('c1'), () => t.join.push(undefined));
    check('a push of nothing records no contributor', t.join.contributions.size, 0);

    t.stopOwner('c1');
    t.flush();
    check('and its contributor leaving builds nothing either', t.cursorCalls.length, 0);
    check('the $in is still empty', t.join.data, []);
  }

  // --- a failure in send() is retried, not thrown at the subscription ------
  {
    const t = build();
    t.pushAs('c1', 'a1');

    let failed = false;
    const realCursor = t.join.methods.cursor;
    t.join.methods.cursor = (...args) => {
      if (failed) return realCursor(...args);
      failed = true;
      throw new Error('transient mongo error');
    };

    t.join.send(); // must not propagate - that would nosub the whole publication
    check('the first attempt failed', failed, true);
    check('no observe yet', t.cursorCalls.length, 0);
    check('but a retry is scheduled', t.Meteor._pendingTimers(), 1);

    t.Meteor._flushTimers();
    check('and it comes up on the retry', t.cursorCalls.length, 1);
  }

  // --- a reconcile superseded mid-flight writes nothing back ---------------
  // _cursor() yields, so a push arriving during it schedules a second reconcile
  // that runs to completion in its own fiber. The first then wakes up holding a
  // handle set()'s latch has already discarded - and if it claims the observer
  // is live, a retry the second one scheduled will take the unchanged-membership
  // skip and the join never rebuilds. Reentrancy stands in for the fiber here.
  {
    const t = build();
    t.pushAs('c1', 'a1');
    t.join.send();
    t.cursorCalls.length = 0;

    const realCursor = t.join.methods.cursor;
    let reentered = false;

    t.join.methods.cursor = (...args) => {
      if (!reentered) {
        reentered = true;
        t.pushAs('c2', 'a2'); // lands while this _cursor() is still "yielding"
        t.join.methods.cursor = () => {
          throw new Error('transient mongo error');
        };
        t.flush(); // the second reconcile runs, fails, and schedules a retry
        t.join.methods.cursor = realCursor;
      }
      return realCursor(...args);
    };

    t.pushAs('c3', 'a3');
    t.flush();

    check('the superseded run claimed no observer', t.join._observerLive, false);
    check('the retry it scheduled is still pending', t.Meteor._pendingTimers(), 1);

    t.cursorCalls.length = 0;
    t.Meteor._flushTimers();
    check('and the retry does rebuild', t.cursorCalls.length, 1);
    check('leaving a live observer behind', t.join._observerLive, true);
  }

  // --- a join that gave up rebuilds on the next change ---------------------
  // The unchanged-membership skip has to key off "an observer is up", not off
  // the retry counter - _retryRestart zeroes that when it gives up.
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
    check('it gave up', t.Meteor._debugs.filter(d => d.indexOf('gave up restarting') !== -1).length, 1);

    // Mongo comes back. The union does not change - a2 is already a member -
    // but there is no observer, so this must NOT take the skip.
    t.join.methods.cursor = (...args) => t.cursorCalls.push(args);
    t.pushAs('c3', 'a2');
    t.flush();
    check('an observer-less join still rebuilds', t.cursorCalls.length, 1);
  }

  // --- the {$in} is a function of membership, not of push order ------------
  // Mongo keys its ObserveMultiplexer cache by the serialised cursor
  // description, so two joins holding the same members must produce a byte-equal
  // selector or they can never share an observer.
  {
    const a = build();
    const b = build();
    a.pushAs('c1', 'a3', 'a1', 'a2');
    a.join.send();
    b.pushAs('c1', 'a1', 'a2', 'a3');
    b.join.send();
    check('same members, different push order -> same selector', a.join._selector(), b.join._selector());
  }

  // --- and it survives churn, which is where the order actually drifts -----
  {
    const warm = build();
    const fresh = build();

    warm.pushAs('c1', 'a1');
    warm.pushAs('c2', 'a2');
    warm.pushAs('c3', 'a3');
    warm.join.send();
    warm.stopOwner('c2'); // a2's contributor goes away...
    warm.pushAs('c4', 'a2'); // ...and another picks it up, at the end of the union
    warm.flush();

    fresh.pushAs('c1', 'a1', 'a2', 'a3');
    fresh.join.send();

    check('a churned join matches a fresh one with the same members', warm.join._selector(), fresh.join._selector());
    check('and the underlying data really had drifted', warm.join.data, ['a1', 'a3', 'a2']);
  }

  // --- a custom selector gets the sorted array too --------------------------
  {
    const t = build();
    t.join.selector = _ids => ({ postId: _ids });
    t.pushAs('c1', 'a3', 'a1');
    t.join.send();
    check('custom selector receives sorted ids', t.join._selector(), { postId: { $in: ['a1', 'a3'] } });
  }

  return report();
};

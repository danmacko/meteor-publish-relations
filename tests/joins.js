import { Meteor } from 'meteor/meteor';
import { Tinytest } from 'meteor/tinytest';
import { Mongo } from 'meteor/mongo';
import { Random } from 'meteor/random';
import PublishRelations from 'meteor/danmacko:publish-relations';
import { Client } from './data';

// Regression coverage for the 3.0.x fixes. Each test drives a real DDP client
// and asserts on the raw messages, because everything being fixed here is
// about WHICH ddp messages a subscription produces over time.

// Fails the test if nothing happened within the timeout - without this a
// regression shows up as a suite that hangs instead of one that reports.
function deadline (test, done, label, ms = 10000) {
  const timer = Meteor.setTimeout(() => {
    test.fail({ message: 'timed out waiting for: ' + label });
    done();
  }, ms);
  return () => Meteor.clearTimeout(timer);
}

// Number of live Mongo queries touching a collection. Meteor keeps one
// ObserveMultiplexer per distinct cursor description and deletes it when its
// last handle stops, so this counts leaked observers directly - the failure
// mode this whole fork exists to fix, and the one thing a DDP client cannot
// see. Filtered by the (random) collection name so concurrent tests can't
// pollute the count.
function liveObservers (collectionName) {
  const mux = MongoInternals.defaultRemoteCollectionDriver().mongo._observeMultiplexers;
  return Object.keys(mux).filter(key => key.indexOf(collectionName) !== -1).length;
}

// Observers stop through a couple of callbacks; give them a tick to settle.
function settle (fn, ms = 1500) {
  Meteor.setTimeout(fn, ms);
}
// A write made from a DDP message handler cannot be awaited - the handler is not
// async and never will be - so it is fired and forgotten. Chained through here
// instead, so that several of them still land in the order they were written and
// a failure is reported as a failing test rather than an unhandled rejection.
let writeQueue = Promise.resolve();
function write (test, fn) {
  writeQueue = writeQueue
    .catch(() => {})
    .then(fn)
    .catch(error => test.fail({ message: 'write from a message handler failed: ' + error.message }));
}

Tinytest.addAsync('Join - publishes joined docs and retracts orphans', async function (test) {
  const posts = new Mongo.Collection(Random.id());
  const authorsName = Random.id();
  const authors = new Mongo.Collection(authorsName);
  const publish = Random.id();

  await authors.insertAsync({ _id: 'a1', name: 'Alice' });
  await authors.insertAsync({ _id: 'a2', name: 'Bob' });
  await posts.insertAsync({ _id: 'post1', authorId: 'a1' });
  await posts.insertAsync({ _id: 'post2', authorId: 'a2' });

  PublishRelations(publish, function () {
    const join = this.relations.join(authors);
    this.relations.cursor(posts.find(), function (id, doc) {
      join.push(doc.authorId);
    });
    join.send();

    return this.ready();
  });

  await new Promise(done => {
    const client = Client();
    const cancel = deadline(test, done, 'removed for the orphaned author');
    const added = {};
    let phase = 'initial';

    client._livedata_data = function (msg) {
      if (msg.msg === 'added' && msg.collection === authorsName) added[msg.id] = true;

      if (msg.msg === 'ready' && phase === 'initial') {
        phase = 'retracting';
        test.isTrue(added.a1, 'author a1 published through the join');
        test.isTrue(added.a2, 'author a2 published through the join');
        // last (only) contributor of a2 disappears -> a2 must leave the {$in}
        // AND be retracted from the client (the retraction was dead code before)
        write(test, () => posts.removeAsync('post2'));
      } else if (msg.msg === 'removed' && msg.collection === authorsName && phase === 'retracting') {
        phase = 'done';
        test.equal(msg.id, 'a2', 'only the orphaned joined doc is retracted');
        cancel();
        client.disconnect();
        done();
      }
    };

    client.subscribe(publish);
  });
});

Tinytest.addAsync('Join - a shared joined doc survives one contributor leaving', async function (test) {
  const posts = new Mongo.Collection(Random.id());
  const authorsName = Random.id();
  const authors = new Mongo.Collection(authorsName);
  const publish = Random.id();

  await authors.insertAsync({ _id: 'shared', name: 'Alice' });
  await authors.insertAsync({ _id: 'solo', name: 'Bob' });
  await posts.insertAsync({ _id: 'post1', authorId: 'shared' });
  await posts.insertAsync({ _id: 'post2', authorId: 'shared' }); // second contributor of the same author
  await posts.insertAsync({ _id: 'post3', authorId: 'solo' });

  PublishRelations(publish, function () {
    const join = this.relations.join(authors);
    this.relations.cursor(posts.find(), function (id, doc) {
      join.push(doc.authorId);
    });
    join.send();

    return this.ready();
  });

  await new Promise(done => {
    const client = Client();
    const cancel = deadline(test, done, 'removed for the solo author');
    let phase = 'initial';

    client._livedata_data = function (msg) {
      if (msg.msg === 'ready' && phase === 'initial') {
        phase = 'dropping';
        write(test, () => posts.removeAsync('post1')); // 'shared' still referenced by post2 -> refcount stays > 0
        write(test, () => posts.removeAsync('post3')); // 'solo' loses its only contributor
      } else if (msg.msg === 'removed' && msg.collection === authorsName && phase === 'dropping') {
        // The refcount must protect 'shared'; only 'solo' may be retracted.
        test.equal(msg.id, 'solo', 'refcounted join does not drop a still-referenced doc');
        phase = 'done';
        cancel();
        client.disconnect();
        done();
      }
    };

    client.subscribe(publish);
  });
});

Tinytest.addAsync('Join - two joins on one collection stay reactive', async function (test) {
  // One collection joined twice under different selectors - once by the parent
  // id, once by a value carried on the parent doc. Before the per-cursor
  // registry keys, the second join's observer stopped the first one (and vice
  // versa on every restart), so only one of them was live at a time.
  const posts = new Mongo.Collection(Random.id());
  const authorsName = Random.id();
  const authors = new Mongo.Collection(authorsName);
  const publish = Random.id();

  await posts.insertAsync({ _id: 'post1', pseudonym: 'ghost' });
  await authors.insertAsync({ _id: 'linked', postId: 'post1', pseudonym: 'none', v: 0 });
  await authors.insertAsync({ _id: 'pseudonymous', postId: 'none', pseudonym: 'ghost', v: 0 });

  PublishRelations(publish, function () {
    const byPost = this.relations.join(authors);
    byPost.selector = (_ids) => ({ postId: _ids });
    const byPseudonym = this.relations.join(authors);
    byPseudonym.selector = (_ids) => ({ pseudonym: _ids });

    this.relations.cursor(posts.find(), function (id, doc) {
      byPost.push(id);
      byPseudonym.push(doc.pseudonym);
    });
    byPost.send();
    byPseudonym.send();

    return this.ready();
  });

  await new Promise(done => {
    const client = Client();
    const cancel = deadline(test, done, 'changed from BOTH same-collection joins');
    const added = {};
    const changed = {};
    let phase = 'initial';

    client._livedata_data = function (msg) {
      if (msg.collection !== authorsName) {
        if (msg.msg === 'ready' && phase === 'initial') {
          phase = 'touching';
          test.isTrue(added.linked, 'doc matched by the first join is published');
          test.isTrue(added.pseudonymous, 'doc matched by the second join is published');
          // Touch both docs: each is matched by exactly one of the two joins, so
          // a changed for both proves both observers are alive at the same time.
          write(test, () => authors.updateAsync('linked', { $set: { v: 1 } }));
          write(test, () => authors.updateAsync('pseudonymous', { $set: { v: 1 } }));
        }
        return;
      }

      if (msg.msg === 'added') added[msg.id] = true;

      if (msg.msg === 'changed' && phase === 'touching') {
        changed[msg.id] = true;
        if (changed.linked && changed.pseudonymous) {
          phase = 'done';
          test.isTrue(true, 'both join observers are live simultaneously');
          cancel();
          client.disconnect();
          done();
        }
      }
    };

    client.subscribe(publish);
  });
});

Tinytest.addAsync('Cursor - two cursors on one collection stay reactive', async function (test) {
  // Same registry-key regression, but for two plain top-level cursors. These
  // are never restarted, so the loser used to stay dead for the whole
  // subscription.
  const postsName = Random.id();
  const posts = new Mongo.Collection(postsName);
  const publish = Random.id();

  await posts.insertAsync({ _id: 'a1', tag: 'a', v: 0 });
  await posts.insertAsync({ _id: 'b1', tag: 'b', v: 0 });

  PublishRelations(publish, function () {
    this.relations.cursor(posts.find({ tag: 'a' }));
    this.relations.cursor(posts.find({ tag: 'b' }));

    return this.ready();
  });

  await new Promise(done => {
    const client = Client();
    const cancel = deadline(test, done, 'changed from BOTH same-collection cursors');
    const added = {};
    const changed = {};
    let phase = 'initial';

    client._livedata_data = function (msg) {
      if (msg.msg === 'added' && msg.collection === postsName) added[msg.id] = true;

      if (msg.msg === 'ready' && phase === 'initial') {
        phase = 'touching';
        test.isTrue(added.a1 && added.b1, 'both cursors published their result set');
        write(test, () => posts.updateAsync('a1', { $set: { v: 1 } }));
        write(test, () => posts.updateAsync('b1', { $set: { v: 1 } }));
      } else if (msg.msg === 'changed' && msg.collection === postsName && phase === 'touching') {
        changed[msg.id] = true;
        if (changed.a1 && changed.b1) {
          phase = 'done';
          test.isTrue(true, 'both cursor observers are live simultaneously');
          cancel();
          client.disconnect();
          done();
        }
      }
    };

    client.subscribe(publish);
  });
});

Tinytest.addAsync('Join - custom selector never retracts wrongly', async function (test) {
  // With a custom selector the pushed values are foreign keys, not published
  // doc ids, so the retraction must find no proof in _documents and no-op
  // rather than removing a doc the client legitimately holds.
  const posts = new Mongo.Collection(Random.id());
  const authorsName = Random.id();
  const authors = new Mongo.Collection(authorsName);
  const publish = Random.id();

  await posts.insertAsync({ _id: 'post1' });
  await posts.insertAsync({ _id: 'post2' });
  await authors.insertAsync({ _id: 'a1', postId: 'post1' });
  await authors.insertAsync({ _id: 'a2', postId: 'post2' });

  PublishRelations(publish, function () {
    const join = this.relations.join(authors);
    join.selector = (_ids) => ({ postId: _ids });
    this.relations.cursor(posts.find(), function (id) {
      join.push(id);
    });
    join.send();

    return this.ready();
  });

  await new Promise(done => {
    const client = Client();
    const removed = [];
    let ready = false;

    client._livedata_data = function (msg) {
      if (msg.msg === 'removed' && msg.collection === authorsName) removed.push(msg.id);

      if (msg.msg === 'ready' && !ready) {
        ready = true;
        write(test, () => posts.removeAsync('post2')); // releases the join's foreign key 'post2'
        // No id pushed into this join is ever an authors _id, so nothing may be
        // retracted by id. Give the deferred restart time to run, then assert.
        Meteor.setTimeout(() => {
          test.equal(removed, [], 'custom-selector join retracts nothing by foreign key');
          client.disconnect();
          done();
        }, 2000);
      }
    };

    client.subscribe(publish);
  });
});

Tinytest.addAsync('Teardown - unsubscribing stops every observer', async function (test) {
  // The publication holds a parent observer plus a join observer. Both must be
  // gone once the client goes away; a leaked one survives until server restart.
  const postsName = Random.id();
  const authorsName = Random.id();
  const posts = new Mongo.Collection(postsName);
  const authors = new Mongo.Collection(authorsName);
  const publish = Random.id();

  await authors.insertAsync({ _id: 'a1', name: 'Alice' });
  await posts.insertAsync({ _id: 'post1', authorId: 'a1' });

  PublishRelations(publish, function () {
    const join = this.relations.join(authors);
    this.relations.cursor(posts.find(), function (id, doc) {
      join.push(doc.authorId);
    });
    join.send();

    return this.ready();
  });

  await new Promise(done => {
    const client = Client();
    const cancel = deadline(test, done, 'ready before teardown assertions');
    let ready = false;

    client._livedata_data = function (msg) {
      if (msg.msg !== 'ready' || ready) return;
      ready = true;
      cancel();

      test.isTrue(liveObservers(postsName) > 0, 'parent observer is live while subscribed');
      test.isTrue(liveObservers(authorsName) > 0, 'join observer is live while subscribed');

      client.disconnect();
      settle(() => {
        test.equal(liveObservers(postsName), 0, 'parent observer stopped on teardown');
        test.equal(liveObservers(authorsName), 0, 'join observer stopped on teardown');
        done();
      });
    };

    client.subscribe(publish);
  });
});

Tinytest.addAsync('Join - repeated restarts do not leak observers', async function (test) {
  // Every push() with new ids restarts the join's observe. The replaced one
  // must be stopped, or a busy subscription piles up observers against Mongo.
  const posts = new Mongo.Collection(Random.id());
  const authorsName = Random.id();
  const authors = new Mongo.Collection(authorsName);
  const publish = Random.id();

  for (let i = 0; i < 5; i++) await authors.insertAsync({ _id: 'a' + i, name: 'Author ' + i });
  await posts.insertAsync({ _id: 'post0', authorId: 'a0' });

  PublishRelations(publish, function () {
    const join = this.relations.join(authors);
    this.relations.cursor(posts.find(), function (id, doc) {
      join.push(doc.authorId);
    });
    join.send();

    return this.ready();
  });

  await new Promise(done => {
    const client = Client();
    const cancel = deadline(test, done, 'ready before restart assertions', 20000);
    let ready = false;

    client._livedata_data = function (msg) {
      if (msg.msg !== 'ready' || ready) return;
      ready = true;
      cancel();

      const baseline = liveObservers(authorsName);
      test.equal(baseline, 1, 'exactly one join observer at rest');

      // Each insert grows the {$in} and forces a restart.
      for (let i = 1; i < 5; i++) write(test, () => posts.insertAsync({ _id: 'post' + i, authorId: 'a' + i }));

      settle(() => {
        test.equal(liveObservers(authorsName), 1, 'restarts replace the observer instead of stacking');
        client.disconnect();
        settle(() => {
          test.equal(liveObservers(authorsName), 0, 'and the survivor is stopped on teardown');
          done();
        });
      }, 3000);
    };

    client.subscribe(publish);
  });
});

Tinytest.addAsync('Join - two subscriptions holding the same ids share one observer', async function (test) {
  // Mongo caches an ObserveMultiplexer per EJSON.stringify of the cursor
  // description, so the ORDER of the {$in} is part of the key. Two subscriptions
  // that arrived at the same members in a different order would each get their
  // own observer - one initial query and one oplog matcher per subscriber, for
  // a query they could have shared. The join sorts the $in so the key depends on
  // the membership alone; this asserts that, by pushing the same ids in
  // opposite orders and counting what Mongo ends up holding.
  const authorsName = Random.id();
  const authors = new Mongo.Collection(authorsName);
  const publish = Random.id();

  for (const id of ['a1', 'a2', 'a3']) await authors.insertAsync({ _id: id, name: id });

  PublishRelations(publish, function (reversed) {
    const join = this.relations.join(authors);
    const ids = reversed ? ['a3', 'a2', 'a1'] : ['a1', 'a2', 'a3'];
    ids.forEach(id => join.push(id));
    join.send();

    return this.ready();
  });

  await new Promise(done => {
    const first = Client();
    const second = Client();
    const cancel = deadline(test, done, 'both subscriptions ready', 20000);
    let ready = 0;

    const onReady = () => {
      if (++ready < 2) return;
      cancel();

      settle(() => {
        test.equal(liveObservers(authorsName), 1, 'same members, different push order -> one shared observer');
        first.disconnect();
        settle(() => {
          test.equal(liveObservers(authorsName), 1, 'one subscriber leaving does not stop the other');
          second.disconnect();
          settle(() => {
            test.equal(liveObservers(authorsName), 0, 'and the last one out stops it');
            done();
          });
        });
      });
    };

    first._livedata_data = msg => { if (msg.msg === 'ready') onReady(); };
    second._livedata_data = msg => { if (msg.msg === 'ready') onReady(); };

    first.subscribe(publish, false);
    second.subscribe(publish, true);
  });
});

Tinytest.addAsync('Nested cursor - re-runs do not leak observers', async function (test) {
  // A parent 'changed' re-runs its callback with a FRESH CursorMethods, so the
  // nested cursor's registry key must come out identical each time - otherwise
  // every parent update leaves another live observer behind.
  const postsName = Random.id();
  const commentsName = Random.id();
  const posts = new Mongo.Collection(postsName);
  const comments = new Mongo.Collection(commentsName);
  const publish = Random.id();

  await posts.insertAsync({ _id: 'post1', v: 0 });
  await comments.insertAsync({ _id: 'c1', postId: 'post1' });

  PublishRelations(publish, function () {
    this.relations.cursor(posts.find(), function (id, doc) {
      this.relations.cursor(comments.find({ postId: id }));
    });

    return this.ready();
  });

  await new Promise(done => {
    const client = Client();
    const cancel = deadline(test, done, 'ready before nested-cursor assertions', 20000);
    let ready = false;

    client._livedata_data = function (msg) {
      if (msg.msg !== 'ready' || ready) return;
      ready = true;
      cancel();

      const baseline = liveObservers(commentsName);
      test.equal(baseline, 1, 'one nested observer for the single parent');

      for (let i = 1; i <= 4; i++) write(test, () => posts.updateAsync('post1', { $set: { v: i } }));

      settle(() => {
        test.equal(liveObservers(commentsName), 1, 'parent re-runs replace the nested observer');
        client.disconnect();
        settle(() => {
          test.equal(liveObservers(commentsName), 0, 'nested observer stopped on teardown');
          done();
        });
      }, 3000);
    };

    client.subscribe(publish);
  });
});

Tinytest.addAsync('Join - parent leaving the window cascades to nested contributors', async function (test) {
  // The join refcounts by the INNERMOST contributor (the comment). When the outer
  // parent merely leaves the result set, its nested observer is stopped and
  // never fires removed - so the release must cascade, or the joined id stays
  // in the {$in} forever.
  const posts = new Mongo.Collection(Random.id());
  const comments = new Mongo.Collection(Random.id());
  const authorsName = Random.id();
  const authors = new Mongo.Collection(authorsName);
  const publish = Random.id();

  await authors.insertAsync({ _id: 'a1', name: 'Alice' });
  await posts.insertAsync({ _id: 'post1', active: true });
  await comments.insertAsync({ _id: 'c1', postId: 'post1', authorId: 'a1' });

  PublishRelations(publish, function () {
    const join = this.relations.join(authors);
    this.relations.cursor(posts.find({ active: true }), function (id, doc) {
      this.relations.cursor(comments.find({ postId: id }), function (commentId, comment) {
        join.push(comment.authorId);
      });
    });
    join.send();

    return this.ready();
  });

  await new Promise(done => {
    const client = Client();
    const cancel = deadline(test, done, 'removed for the cascade-released author');
    let phase = 'initial';

    client._livedata_data = function (msg) {
      if (msg.msg === 'ready' && phase === 'initial') {
        phase = 'dropping';
        // No document is deleted - post1 just stops matching the publication.
        write(test, () => posts.updateAsync('post1', { $set: { active: false } }));
      } else if (msg.msg === 'removed' && msg.collection === authorsName && phase === 'dropping') {
        phase = 'done';
        test.equal(msg.id, 'a1', 'nested contributor released through its parent');
        cancel();
        client.disconnect();
        done();
      }
    };

    client.subscribe(publish);
  });
});

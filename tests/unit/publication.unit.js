// The whole server stack (handler controller + cursor + join) driven against a
// fake mongo with live observers and a Subscription that throws exactly where
// ddp-server throws. Covers the overlap semantics that a DDP client cannot
// observe from outside: which observers are alive at a given moment, and which
// sub.added/changed/removed calls are suppressed.

const { loadModules, makeMeteorStub, createReporter, FakeDB, makeSub } = require('./harness');

const MODULES = [
  'cursor/published.js',
  'cursor/nonreactive/cursor.js',
  'handler_controller.js',
  'cursor/cursor.js',
  'cursor/utils.js',
  'cursor/join.js',
];

module.exports = function run() {
  const { check, report } = createReporter('unit: publication stack');

  function build() {
    const warnings = [];
    const Meteor = makeMeteorStub();
    const sandbox = loadModules(MODULES, {
      Meteor,
      console: Object.assign({}, console, { warn: (...a) => warnings.push(a.join(' ')) }),
    });
    const db = new FakeDB();
    const { sub, events, isPublished } = makeSub();

    function publish(fn) {
      const root = new sandbox.HandlerController();
      const cursors = new sandbox.CursorMethods(sub, root);
      fn.apply(cursors);
      return root;
    }

    return { db, sub, events, isPublished, publish, warnings, flush: Meteor._flush };
  }

  // === one collection joined twice under different selectors ===============
  {
    const t = build();
    const posts = t.db.coll('posts');
    const authors = t.db.coll('authors');
    t.db.insert('posts', { _id: 'post1', pseudonym: 'ghost' });
    t.db.insert('posts', { _id: 'post2', pseudonym: 'anon' });
    t.db.insert('authors', { _id: 'A1', postId: 'post1', pseudonym: 'ghost' }); // both joins
    t.db.insert('authors', { _id: 'A2', postId: 'post2', pseudonym: 'none' }); // first join only

    t.publish(function () {
      const byPost = this.join(authors);
      byPost.selector = _ids => ({ postId: _ids });
      const byPseudonym = this.join(authors);
      byPseudonym.selector = _ids => ({ pseudonym: _ids });
      this.cursor(posts.find({}), function (id, doc) {
        byPost.push(id);
        byPseudonym.push(doc.pseudonym);
      });
      byPost.send();
      byPseudonym.send();
    });

    check('both join observers are live', t.db.live('authors').length, 2);
    check('doc matched by both is published once', t.events.filter(e => e[0] === 'added' && e[2] === 'A1').length, 1);

    // Restarting one join must not disturb the other's observer.
    const before = t.db.live('authors').find(o => 'pseudonym' in o.selector);
    t.db.insert('authors', { _id: 'A3', postId: 'post3', pseudonym: 'none' });
    t.db.insert('posts', { _id: 'post3' }); // no pseudonym -> only the first join pushes
    t.flush();
    check(
      'other join observer untouched by the restart',
      before === t.db.live('authors').find(o => 'pseudonym' in o.selector),
      true
    );
    check('restarted join picked up the new doc', t.isPublished('authors', 'A3'), true);
    check('still exactly two observers', t.db.live('authors').length, 2);

    // A doc both observers match is deleted: exactly one removed, no throw.
    t.db.remove('authors', 'A1');
    check('overlapping delete removes once', t.events.filter(e => e[0] === 'removed' && e[2] === 'A1').length, 1);

    // Releasing a custom-selector join must not retract by foreign key.
    t.db.remove('posts', 'post2');
    t.flush();
    check('custom selector never false-retracts', t.isPublished('authors', 'A2'), true);
  }

  // === transient hide: one observer retracts what another still matches ====
  {
    const t = build();
    const posts = t.db.coll('posts');
    const authors = t.db.coll('authors');
    t.db.insert('posts', { _id: 'post1', pseudonym: 'ghost' });
    t.db.insert('authors', { _id: 'A1', postId: 'post1', pseudonym: 'ghost' }); // both joins

    t.publish(function () {
      const byPost = this.join(authors);
      byPost.selector = _ids => ({ postId: _ids });
      const byPseudonym = this.join(authors);
      byPseudonym.selector = _ids => ({ pseudonym: _ids });
      this.cursor(posts.find({}), function (id, doc) {
        byPost.push(id);
        byPseudonym.push(doc.pseudonym);
      });
      byPost.send();
      byPseudonym.send();
    });
    check('overlapping doc published', t.isPublished('authors', 'A1'), true);

    // Leaves the first join's selector; the second still matches it.
    t.db.update('authors', 'A1', { postId: 'gone' });
    check('doc is hidden, no throw', t.isPublished('authors', 'A1'), false);

    // The second observer now fires changed for a doc that is already gone.
    t.db.update('authors', 'A1', { note: 'edit' });
    check('changed for a hidden doc is suppressed', t.isPublished('authors', 'A1'), false);
    check('dev warning fired once', t.warnings.filter(w => w.indexOf("'authors'") !== -1).length, 1);

    // The hide heals when the surviving join restarts.
    t.db.insert('posts', { _id: 'post2', pseudonym: 'other' });
    t.flush();
    check('hidden doc restored by the next restart', t.isPublished('authors', 'A1'), true);
  }

  // === two plain top-level cursors on one collection =======================
  {
    const t = build();
    const posts = t.db.coll('posts');
    t.db.insert('posts', { _id: 'a1', tag: 'a' });
    t.db.insert('posts', { _id: 'b1', tag: 'b' });

    const root = t.publish(function () {
      this.cursor(posts.find({ tag: 'a' }));
      this.cursor(posts.find({ tag: 'b' }));
    });

    check('both cursors are live', t.db.live('posts').length, 2);
    check('both result sets published', [t.isPublished('posts', 'a1'), t.isPublished('posts', 'b1')], [true, true]);

    t.db.update('posts', 'a1', { title: 't' });
    check('first cursor is still reactive', t.events.filter(e => e[0] === 'changed' && e[2] === 'a1').length, 1);

    root.stop();
    check('teardown stops both', t.db.live('posts').length, 0);
  }

  // === default-selector join, end to end ==================================
  {
    const t = build();
    const posts = t.db.coll('posts');
    const authors = t.db.coll('authors');
    t.db.insert('authors', { _id: 'A1' });
    t.db.insert('authors', { _id: 'A2' });
    t.db.insert('posts', { _id: 'post1', authorId: 'A1' });
    t.db.insert('posts', { _id: 'post2', authorId: 'A2' });

    t.publish(function () {
      const join = this.join(authors);
      this.cursor(posts.find({}), function (id, doc) {
        join.push(doc.authorId);
      });
      join.send();
    });
    check('both joined docs published', [t.isPublished('authors', 'A1'), t.isPublished('authors', 'A2')], [true, true]);

    t.db.remove('posts', 'post2'); // last contributor of A2
    t.flush();
    check('orphaned joined doc retracted', t.isPublished('authors', 'A2'), false);
    check('still-referenced doc untouched', t.isPublished('authors', 'A1'), true);
    check('exactly one removed sent', t.events.filter(e => e[0] === 'removed' && e[1] === 'authors').length, 1);
    check('observe restarted with the smaller $in', t.db.live('authors')[0].selector._id.$in, ['A1']);
  }

  return report();
};

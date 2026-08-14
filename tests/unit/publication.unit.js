// The whole server stack (handler controller + cursor + join) driven against a
// fake mongo with live observers and a Subscription that throws exactly where
// ddp-server throws. Covers the overlap semantics that a DDP client cannot
// observe from outside: which observers are alive at a given moment, and which
// sub.added/changed/removed calls are suppressed.

const { loadModules, makeMeteorStub, createReporter, FakeDB, makeSub } = require('./harness');

const MODULES = [
  'cursor/published.js',
  'cursor/contributor-context.js',
  'cursor/nonreactive/cursor.js',
  'cursor/nonreactive/join.js',
  'handler_controller.js',
  'cursor/cursor.js',
  'cursor/utils.js',
  'cursor/observe.js',
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

  // === contributions are keyed per cursor, not by bare doc _id =============
  // Two collections whose docs share an _id both feed one join. Releasing by the
  // bare _id would let either removal drop the other's contribution.
  {
    const t = build();
    const posts = t.db.coll('posts');
    const tasks = t.db.coll('tasks');
    const authors = t.db.coll('authors');
    t.db.insert('authors', { _id: 'A1' });
    t.db.insert('posts', { _id: 'x1', authorId: 'A1' });
    t.db.insert('tasks', { _id: 'x1', authorId: 'A1' });

    t.publish(function () {
      const join = this.join(authors);
      this.cursor(posts.find({}), function (id, doc) {
        join.push(doc.authorId);
      });
      this.cursor(tasks.find({}), function (id, doc) {
        join.push(doc.authorId);
      });
      join.send();
    });
    check('joined doc published', t.isPublished('authors', 'A1'), true);

    t.db.remove('posts', 'x1'); // the task sharing the _id still references A1
    t.flush();
    check('a same-_id sibling keeps the joined doc', t.isPublished('authors', 'A1'), true);
    check('the $in still holds it', t.db.live('authors')[0].selector._id.$in, ['A1']);

    t.db.remove('tasks', 'x1'); // now the last contributor is gone
    t.flush();
    check('the last contributor leaving retracts it', t.isPublished('authors', 'A1'), false);
  }

  // === two overlapping cursors on ONE collection see the same _id ==========
  {
    const t = build();
    const posts = t.db.coll('posts');
    const authors = t.db.coll('authors');
    t.db.insert('authors', { _id: 'A1' });
    t.db.insert('posts', { _id: 'p1', tag: 'a', flagged: true, authorId: 'A1' });

    t.publish(function () {
      const join = this.join(authors);
      this.cursor(posts.find({ tag: 'a' }), function (id, doc) {
        join.push(doc.authorId);
      });
      this.cursor(posts.find({ flagged: true }), function (id, doc) {
        join.push(doc.authorId);
      });
      join.send();
    });
    check('joined doc published once', t.events.filter(e => e[0] === 'added' && e[2] === 'A1').length, 1);

    t.db.update('posts', 'p1', { tag: 'b' }); // leaves the first cursor, stays in the second
    t.flush();
    check('the still-matching cursor keeps the join alive', t.db.live('authors')[0].selector._id.$in, ['A1']);
  }

  // === registrations landing after teardown stay inert =====================
  {
    const t = build();
    const posts = t.db.coll('posts');
    const comments = t.db.coll('comments');
    t.db.insert('posts', { _id: 'p1' });
    t.db.insert('comments', { _id: 'c1', postId: 'p1' });

    let resume = null;
    const root = t.publish(function () {
      this.cursor(posts.find({}), function (id) {
        // models a callback that yields (a findOne) before opening its nested cursor
        resume = () => this.cursor(comments.find({ postId: id }));
      });
    });

    root.stop();
    check('teardown stopped the parent observer', t.db.live('posts').length, 0);

    resume(); // the suspended callback resumes after teardown
    check('a nested cursor opened after stop stays inert', t.db.live('comments').length, 0);
  }

  // === a top-level cursor opened after teardown stays inert ================
  {
    const t = build();
    const posts = t.db.coll('posts');
    t.db.insert('posts', { _id: 'p1' });

    let cursors = null;
    const root = t.publish(function () {
      cursors = this;
    });
    root.stop();

    cursors.cursor(posts.find({}));
    check('cursor() after stop creates nothing live', t.db.live('posts').length, 0);

    cursors.observeChanges(posts.find({}), { added() {}, changed() {}, removed() {} });
    check('observeChanges() after stop creates nothing live', t.db.live('posts').length, 0);
  }

  // === a changed callback only sees the changed fields =====================
  // Pins down why contributions are still append-only: observeChanges hands the
  // callback a delta, not the document, so a callback re-run cannot re-declare
  // what it contributes. Clearing the contribution before it runs would release
  // ids on every unrelated change (see design.md).
  {
    const t = build();
    const posts = t.db.coll('posts');
    const authors = t.db.coll('authors');
    t.db.insert('authors', { _id: 'A1' });
    t.db.insert('posts', { _id: 'post1', authorId: 'A1', title: 'a' });

    const seen = [];
    t.publish(function () {
      const join = this.join(authors);
      this.cursor(posts.find({}), function (id, doc, changed) {
        seen.push([changed === true, doc.authorId]);
        if (doc.authorId) join.push(doc.authorId);
      });
      join.send();
    });
    check('the linked doc is published', t.isPublished('authors', 'A1'), true);

    t.db.update('posts', 'post1', { title: 'b' }); // unrelated field
    t.flush();
    check('the callback saw only the delta', seen, [[false, 'A1'], [true, undefined]]);
    check('the unrelated change did not release the link', t.isPublished('authors', 'A1'), true);
    check('and the $in still holds it', t.db.live('authors')[0].selector._id.$in, ['A1']);
  }

  // === rebuilding a nested cursor keeps what it contributed ================
  // The common shape without an `if (!changed)` guard: the callback re-runs on
  // every parent update and re-opens its nested cursor. It is handed the update,
  // not the document, so the foreign key is usually undefined and the rebuilt
  // cursor declares nothing - releasing on the swap would drop the joined doc
  // from the client on any unrelated edit.
  {
    const t = build();
    const posts = t.db.coll('posts');
    const books = t.db.coll('books');
    const authors = t.db.coll('authors');
    t.db.insert('authors', { _id: 'A1' });
    t.db.insert('books', { _id: 'B1', authorId: 'A1' });
    t.db.insert('posts', { _id: 'post1', bookId: 'B1', title: 'a' });

    t.publish(function () {
      const join = this.join(authors);
      this.cursor(posts.find({}), function (id, doc) {
        this.cursor(books.find({ _id: doc.bookId }), function (bid, book) {
          join.push(book.authorId);
        });
      });
      join.send();
    });
    check('the joined doc is published', t.isPublished('authors', 'A1'), true);

    t.db.update('posts', 'post1', { title: 'b' }); // does not touch bookId
    t.flush();
    check('an unrelated parent edit keeps it', t.isPublished('authors', 'A1'), true);
    check('and the $in still holds it', t.db.live('authors')[0].selector._id.$in, ['A1']);

    t.db.remove('posts', 'post1'); // the parent really leaves
    t.flush();
    check('the parent leaving does release it', t.isPublished('authors', 'A1'), false);
    check('and empties the $in', t.db.live('authors')[0].selector._id.$in, []);
  }

  // === a skipped nested cursor must not shift the next one's key ===========
  // The guarded form the README recommends - `if (doc.someId) this.cursor(...)`
  // - means a callback creates a nested cursor only sometimes. Registry keys are
  // counted per collection so the second cursor still supersedes its own
  // observer rather than being handed the skipped one's slot.
  {
    const t = build();
    const posts = t.db.coll('posts');
    const books = t.db.coll('books');
    const authors = t.db.coll('authors');
    t.db.insert('books', { _id: 'B1' });
    t.db.insert('authors', { _id: 'A1' });
    t.db.insert('posts', { _id: 'post1', bookId: 'B1', authorId: 'A1' });

    t.publish(function () {
      this.cursor(posts.find({}), function (id, doc) {
        if (doc.bookId) this.cursor(books.find({ _id: doc.bookId }));
        if (doc.authorId) this.cursor(authors.find({ _id: doc.authorId }));
      });
    });
    check('both nested cursors are live', [t.db.live('books').length, t.db.live('authors').length], [1, 1]);

    // an update carrying only authorId: the books cursor is skipped this time
    t.db.update('posts', 'post1', { authorId: 'A1' });
    t.flush();
    check('the skipped one keeps its observer', t.db.live('books').length, 1);
    check('and the second superseded its own', t.db.live('authors').length, 1);
  }

  // === what an observe pushes belongs to whoever created it ================
  // Meteor delivers an observe's initial adds through a bindEnvironment-wrapped
  // task but every later event raw, so without an explicit binding the same
  // callback would be tracked or pinned for good depending on when a document
  // turned up. Here the second comment arrives after the observe was built.
  {
    const t = build();
    const posts = t.db.coll('posts');
    const comments = t.db.coll('comments');
    const authors = t.db.coll('authors');
    t.db.insert('authors', { _id: 'A1' });
    t.db.insert('authors', { _id: 'A2' });
    t.db.insert('posts', { _id: 'post1' });
    t.db.insert('comments', { _id: 'c1', postId: 'post1', authorId: 'A1' });

    t.publish(function () {
      const join = this.join(authors);
      this.cursor(posts.find({}), function (id) {
        this.observeChanges(comments.find({ postId: id }), {
          added(cid, comment) {
            join.push(comment.authorId);
          },
          changed() {},
          removed() {},
        });
      });
      join.send();
    });
    check('the observe publishes nothing itself', t.isPublished('comments', 'c1'), false);
    check('but its push reached the join', t.isPublished('authors', 'A1'), true);

    t.db.insert('comments', { _id: 'c2', postId: 'post1', authorId: 'A2' }); // a later event
    t.flush();
    check('a later push reaches the join too', t.isPublished('authors', 'A2'), true);

    t.db.remove('posts', 'post1'); // the contributor that created the observe leaves
    t.flush();
    check('both pushes are released with it', t.db.live('authors')[0].selector._id.$in, []);
  }

  // === joinNonreactive publishes once and observes nothing =================
  {
    const t = build();
    const posts = t.db.coll('posts');
    const authors = t.db.coll('authors');
    t.db.insert('authors', { _id: 'A1' });
    t.db.insert('authors', { _id: 'A2' });
    t.db.insert('posts', { _id: 'post1', authorId: 'A1' });

    const handed = [];
    let join = null;
    t.publish(function () {
      join = this.joinNonreactive(authors);
      join.selector = _id => {
        handed.push(_id.$in);
        return { _id: _id };
      };
      this.cursor(posts.find({}), function (id, doc) {
        join.push(doc.authorId);
      });
      join.send();
    });
    check('the joined doc is published', t.isPublished('authors', 'A1'), true);
    check('and no observer was created for it', t.db.live('authors').length, 0);

    join.push('A2'); // a late push has to publish on the spot
    check('a late push is published too', t.isPublished('authors', 'A2'), true);

    // The same {$in} shape both times, so a selector written for this.join fits.
    check('the selector is always handed an $in array', handed, [['A1'], ['A2']]);

    // ...and a copy of it: this.data keeps growing, the handed array must not.
    join.push('A3');
    check('the handed array is a copy', handed[0], ['A1']);
  }

  return report();
};

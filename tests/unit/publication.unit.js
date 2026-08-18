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

    // Mirrors publish_relations.js: the body runs with the subscription as
    // `this` and the package API in its own namespace on this.relations.
    function publish(fn) {
      const root = new sandbox.HandlerController();
      sub.relations = new sandbox.CursorMethods(sub, root);
      fn.apply(sub);
      return root;
    }

    return { db, sub, events, isPublished, publish, warnings, flush: Meteor._flush };
  }

  // The $in a join's live observer is currently built with, or null when it has
  // none. Reading it off the observer directly turns "the join lost its
  // observer" into a TypeError that takes the whole run down, instead of failing
  // the one check that is wrong.
  function joinedIds(t, collection) {
    const observer = t.db.live(collection)[0];
    return observer ? observer.selector._id.$in : null;
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
      const byPost = this.relations.join(authors);
      byPost.selector = _ids => ({ postId: _ids });
      const byPseudonym = this.relations.join(authors);
      byPseudonym.selector = _ids => ({ pseudonym: _ids });
      this.relations.cursor(posts.find({}), function (id, doc) {
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
      const byPost = this.relations.join(authors);
      byPost.selector = _ids => ({ postId: _ids });
      const byPseudonym = this.relations.join(authors);
      byPseudonym.selector = _ids => ({ pseudonym: _ids });
      this.relations.cursor(posts.find({}), function (id, doc) {
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
      this.relations.cursor(posts.find({ tag: 'a' }));
      this.relations.cursor(posts.find({ tag: 'b' }));
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
      const join = this.relations.join(authors);
      this.relations.cursor(posts.find({}), function (id, doc) {
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
    check('observe restarted with the smaller $in', joinedIds(t, 'authors'), ['A1']);
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
      const join = this.relations.join(authors);
      this.relations.cursor(posts.find({}), function (id, doc) {
        join.push(doc.authorId);
      });
      this.relations.cursor(tasks.find({}), function (id, doc) {
        join.push(doc.authorId);
      });
      join.send();
    });
    check('joined doc published', t.isPublished('authors', 'A1'), true);

    t.db.remove('posts', 'x1'); // the task sharing the _id still references A1
    t.flush();
    check('a same-_id sibling keeps the joined doc', t.isPublished('authors', 'A1'), true);
    check('the $in still holds it', joinedIds(t, 'authors'), ['A1']);

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
      const join = this.relations.join(authors);
      this.relations.cursor(posts.find({ tag: 'a' }), function (id, doc) {
        join.push(doc.authorId);
      });
      this.relations.cursor(posts.find({ flagged: true }), function (id, doc) {
        join.push(doc.authorId);
      });
      join.send();
    });
    check('joined doc published once', t.events.filter(e => e[0] === 'added' && e[2] === 'A1').length, 1);

    t.db.update('posts', 'p1', { tag: 'b' }); // leaves the first cursor, stays in the second
    t.flush();
    check('the still-matching cursor keeps the join alive', joinedIds(t, 'authors'), ['A1']);
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
      this.relations.cursor(posts.find({}), function (id) {
        // models a callback that yields (a findOne) before opening its nested cursor
        resume = () => this.relations.cursor(comments.find({ postId: id }));
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

    let relations = null;
    const root = t.publish(function () {
      relations = this.relations;
    });
    root.stop();

    relations.cursor(posts.find({}));
    check('cursor() after stop creates nothing live', t.db.live('posts').length, 0);

    relations.observeChanges(posts.find({}), { added() {}, changed() {}, removed() {} });
    check('observeChanges() after stop creates nothing live', t.db.live('posts').length, 0);
  }

  // === a changed callback only sees the changed fields =====================
  // Pins down why contributions are still append-only. Making a callback re-run
  // REPLACE what it contributes - clearing its set before the call - would fix
  // an id left pinned when a foreign key is re-pointed. It cannot be done while
  // observeChanges hands the callback a delta rather than the document: on any
  // update that does not touch the key the callback reads, it declares nothing
  // and a valid link is released. That is the worse of the two, so the pin
  // stays; unblocking it needs the callback to be handed the merged document.
  {
    const t = build();
    const posts = t.db.coll('posts');
    const authors = t.db.coll('authors');
    t.db.insert('authors', { _id: 'A1' });
    t.db.insert('posts', { _id: 'post1', authorId: 'A1', title: 'a' });

    const seen = [];
    t.publish(function () {
      const join = this.relations.join(authors);
      this.relations.cursor(posts.find({}), function (id, doc, changed) {
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
    check('and the $in still holds it', joinedIds(t, 'authors'), ['A1']);

    // The price of that, pinned: this update DOES carry the key, so the callback
    // declares A2 - and nothing says A1 is no longer declared, because a re-run
    // adds to what a callback contributes and never replaces it. The old id is
    // held until the contributing document itself leaves.
    t.db.insert('authors', { _id: 'A2' });
    t.db.update('posts', 'post1', { authorId: 'A2' });
    t.flush();
    check('the new link is published', t.isPublished('authors', 'A2'), true);
    check('and the old one is kept as well', joinedIds(t, 'authors'), ['A1', 'A2']);

    t.db.remove('posts', 'post1'); // the contributor leaves: both go
    t.flush();
    check('the pin ends with the contributor', t.db.live('authors').length, 0);
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
      const join = this.relations.join(authors);
      this.relations.cursor(posts.find({}), function (id, doc) {
        this.relations.cursor(books.find({ _id: doc.bookId }), function (bid, book) {
          join.push(book.authorId);
        });
      });
      join.send();
    });
    check('the joined doc is published', t.isPublished('authors', 'A1'), true);

    t.db.update('posts', 'post1', { title: 'b' }); // does not touch bookId
    t.flush();
    check('an unrelated parent edit keeps it', t.isPublished('authors', 'A1'), true);
    check('and the $in still holds it', joinedIds(t, 'authors'), ['A1']);

    t.db.remove('posts', 'post1'); // the parent really leaves
    t.flush();
    check('the parent leaving does release it', t.isPublished('authors', 'A1'), false);
    check('and an empty membership keeps no observer', t.db.live('authors').length, 0);
  }

  // === a rebuilt cursor that CAN re-declare gives the carry back ===========
  // The other half of the shape above. When the rebuilt cursor does match its
  // documents again they speak for themselves, through their own controllers,
  // and what was carried across the swap has to go: it sits on the cursor's
  // slot, which only dies with the parent document, so keeping it would outlive
  // every child it came from - one nested document leaving would then release
  // its own contribution while the carried copy held the id anyway.
  {
    const t = build();
    const posts = t.db.coll('posts');
    const books = t.db.coll('books');
    const authors = t.db.coll('authors');
    t.db.insert('authors', { _id: 'A1' });
    t.db.insert('authors', { _id: 'A2' });
    t.db.insert('books', { _id: 'B1', postId: 'post1', authorId: 'A1' });
    t.db.insert('books', { _id: 'B2', postId: 'post1', authorId: 'A2' });
    t.db.insert('posts', { _id: 'post1', title: 'a' });

    t.publish(function () {
      const join = this.relations.join(authors);
      this.relations.cursor(posts.find({}), function (id, doc) {
        // built from the parent _id, so the rebuild matches its books again
        this.relations.cursor(books.find({ postId: id }), function (bid, book) {
          if (book.authorId) join.push(book.authorId);
        });
      });
      join.send();
    });
    check('both authors joined', joinedIds(t, 'authors'), ['A1', 'A2']);

    t.db.update('posts', 'post1', { title: 'b' }); // rebuilds the nested cursor
    t.flush();
    check('the rebuild holds them both', joinedIds(t, 'authors'), ['A1', 'A2']);

    t.db.remove('books', 'B2'); // one nested doc leaves, the parent stays
    t.flush();
    check('and one child leaving releases exactly its own', joinedIds(t, 'authors'), ['A1']);
    check('the orphan is retracted', t.isPublished('authors', 'A2'), false);
  }

  // === a re-pointed key, the three cases the README names =================
  // Same edit each time - book.authorId going from one author to another - and
  // only the position of the push differs. The join is on a third collection so
  // that what is released is unambiguous: it is what the OLD nested document
  // declared, not the key that was re-pointed.
  {
    const t = build();
    const books = t.db.coll('books');
    const authors = t.db.coll('authors');
    const countries = t.db.coll('countries');
    t.db.insert('countries', { _id: 'C1' });
    t.db.insert('countries', { _id: 'C2' });
    t.db.insert('authors', { _id: 'A1', countryId: 'C1' });
    t.db.insert('authors', { _id: 'A2', countryId: 'C2' });
    t.db.insert('books', { _id: 'B1', authorId: 'A1' });

    t.publish(function () {
      const join = this.relations.join(countries);
      this.relations.cursor(books.find({}), function (id, doc) {
        if (doc.authorId) {
          this.relations.cursor(authors.find({ _id: doc.authorId }), function (aid, author) {
            if (author.countryId) join.push(author.countryId);
          });
        }
      });
      join.send();
    });
    const joined = () => {
      const observer = t.db.live('countries')[0];
      return observer ? observer.selector._id.$in : null;
    };
    check('the first author country is joined', joined(), ['C1']);

    // (2) the rebuilt cursor matches A2, so it re-declares for itself and what
    // A1 declared goes - off the client too, not only out of the {$in}.
    t.db.update('books', 'B1', { authorId: 'A2' });
    t.flush();
    check('re-pointing releases what the old author declared', joined(), ['C2']);
    check('and retracts it from the client', t.isPublished('countries', 'C1'), false);
    check('while the new one is published', t.isPublished('countries', 'C2'), true);

    // (3) the key is present and the selector is valid, but nothing matches it.
    // Indistinguishable from "the update did not carry the key", so it is kept.
    t.db.update('books', 'B1', { authorId: 'A404' });
    t.flush();
    check('re-pointing at nothing keeps the old declaration', joined(), ['C2']);
    check('and leaves it on the client', t.isPublished('countries', 'C2'), true);
  }

  // === the carry is no substitute for guarding a nested cursor =============
  // Where the two blocks above end. Carrying contributions across a rebuild
  // keeps the JOIN whole; it does nothing for the rebuilt cursor itself, which
  // is left observing a selector built from a key the update did not carry. Its
  // documents stay on the client - stopping an observer sends no removed - and
  // stop being reactive. Guarding the nested cursor is still the only fix.
  {
    const t = build();
    const posts = t.db.coll('posts');
    const books = t.db.coll('books');
    const authors = t.db.coll('authors');
    t.db.insert('authors', { _id: 'A1' });
    t.db.insert('books', { _id: 'B1', authorId: 'A1', title: 'orig' });
    t.db.insert('posts', { _id: 'post1', bookId: 'B1', note: 'a' });

    t.publish(function () {
      const join = this.relations.join(authors);
      this.relations.cursor(posts.find({}), function (id, doc) {
        // deliberately unguarded - the shape the README warns about
        this.relations.cursor(books.find({ _id: doc.bookId }), function (bid, book) {
          if (book.authorId) join.push(book.authorId);
        });
      });
      join.send();
    });

    t.db.update('posts', 'post1', { note: 'b' }); // rebuilds on {_id: undefined}
    t.flush();
    check('the join keeps its member', joinedIds(t, 'authors'), ['A1']);
    check('and the book is still on the client', t.isPublished('books', 'B1'), true);

    const before = t.events.length;
    t.db.update('books', 'B1', { title: 'edited' });
    check('but nothing observes it any more', t.events.length > before, false);
  }

  // === two guarded cursors on ONE collection share a counter ===============
  // Known limitation, pinned so a change to it is deliberate. Registry keys are
  // positional per collection, so when a changed callback skips the first guard
  // the second call is handed the first's slot: it stops that cursor's observer
  // and puts its own there, while its own previous observer keeps running in the
  // slot it no longer claims. The document behind the stopped observer stays on
  // the client with nothing watching it.
  {
    const t = build();
    const posts = t.db.coll('posts');
    const books = t.db.coll('books');
    t.db.insert('books', { _id: 'B1' });
    t.db.insert('books', { _id: 'B2' });
    t.db.insert('books', { _id: 'B3' });
    t.db.insert('posts', { _id: 'post1', bookId: 'B1', otherBookId: 'B2' });

    t.publish(function () {
      this.relations.cursor(posts.find({}), function (id, doc) {
        if (doc.bookId) this.relations.cursor(books.find({ _id: doc.bookId }));
        if (doc.otherBookId) this.relations.cursor(books.find({ _id: doc.otherBookId }));
      });
    });
    check('one observer per guarded cursor', t.db.live('books').map(o => o.selector._id), ['B1', 'B2']);

    t.db.update('posts', 'post1', { otherBookId: 'B3' }); // the delta skips the first guard
    t.flush();
    check('the second cursor took the first ones slot', t.db.live('books').map(o => o.selector._id), ['B2', 'B3']);
    check('leaving B1 published with nothing observing it', t.isPublished('books', 'B1'), true);
  }

  // === a skipped nested cursor must not shift the next one's key ===========
  // The guarded form the README recommends - `if (doc.someId) this.relations.cursor(...)`
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
      this.relations.cursor(posts.find({}), function (id, doc) {
        if (doc.bookId) this.relations.cursor(books.find({ _id: doc.bookId }));
        if (doc.authorId) this.relations.cursor(authors.find({ _id: doc.authorId }));
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
      const join = this.relations.join(authors);
      this.relations.cursor(posts.find({}), function (id) {
        this.relations.observeChanges(comments.find({ postId: id }), {
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
    check('both pushes are released with it', [t.isPublished('authors', 'A1'), t.isPublished('authors', 'A2')], [false, false]);
    check('and nothing is left to observe', t.db.live('authors').length, 0);
  }

  // === a push from a removed callback is scoped, not permanent =============
  {
    const t = build();
    const posts = t.db.coll('posts');
    const authors = t.db.coll('authors');
    t.db.insert('authors', { _id: 'A1' });
    t.db.insert('authors', { _id: 'A2' });
    t.db.insert('posts', { _id: 'post1', authorId: 'A1' });

    t.publish(function () {
      const join = this.relations.join(authors);
      this.relations.cursor(posts.find({}), {
        added(id, doc) {
          join.push(doc.authorId);
        },
        changed() {},
        removed() {
          join.push('A2'); // the one callback that used to run with no contributor
        },
      });
      join.send();
    });
    check('the added push is published', t.isPublished('authors', 'A1'), true);

    t.db.remove('posts', 'post1');
    t.flush();
    check('the removed push belongs to the document too', [t.isPublished('authors', 'A1'), t.isPublished('authors', 'A2')], [false, false]);
    check('leaving nothing to observe', t.db.live('authors').length, 0);
  }

  // === every callback is framed the same way ===============================
  // README promises the package's methods are available on `this` in a callback,
  // and removed used to be the one invoked without them. A throwing callback
  // must also not keep the document's controller alive: stopping it is what
  // releases the contributions.
  {
    const t = build();
    const posts = t.db.coll('posts');
    const authors = t.db.coll('authors');
    t.db.insert('authors', { _id: 'A1' });
    t.db.insert('posts', { _id: 'post1', authorId: 'A1' });

    const sawMethods = {};
    t.publish(function () {
      const join = this.relations.join(authors);
      this.relations.cursor(posts.find({}), {
        added(id, doc) {
          sawMethods.added = typeof this.relations.cursor === 'function';
          join.push(doc.authorId);
        },
        changed() {},
        removed() {
          sawMethods.removed = typeof this.relations.cursor === 'function';
          throw new Error('a callback may throw');
        },
      });
      join.send();
    });
    check('added gets the methods', sawMethods.added, true);

    let threw = false;
    try {
      t.db.remove('posts', 'post1');
    } catch (error) {
      threw = true;
    }
    t.flush();

    check('so does removed', sawMethods.removed, true);
    check('the throw was not swallowed', threw, true);
    check('but the contribution was released anyway', [t.isPublished('authors', 'A1'), t.db.live('authors').length], [false, 0]);
  }

  // === the API namespace resolves at every depth ===========================
  // publish_relations.js hands the body the subscription with the package API
  // on this.relations. A nested callback is handed the per-document
  // CursorMethods instead, where the same expression has to keep working (the
  // getter in nonreactive/cursor.js) - and the subscription stays reachable
  // there as this.sub.
  {
    const t = build();
    const posts = t.db.coll('posts');
    const comments = t.db.coll('comments');
    t.db.insert('posts', { _id: 'p1' });
    t.db.insert('comments', { _id: 'c1', postId: 'p1' });

    const seen = {};
    t.publish(function () {
      seen.topIsSub = this === t.sub;
      seen.topApi = typeof this.relations.cursor === 'function';
      this.relations.cursor(posts.find({}), function (id) {
        seen.nestedIsApi = this.relations === this;
        seen.nestedSub = this.sub === t.sub;
        this.relations.cursor(comments.find({ postId: id }));
      });
    });

    check('the body runs with the subscription as this', [seen.topIsSub, seen.topApi], [true, true]);
    check('a nested callback resolves this.relations to itself', [seen.nestedIsApi, seen.nestedSub], [true, true]);
    check('and a cursor opened through it publishes', t.isPublished('comments', 'c1'), true);
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
      join = this.relations.joinNonreactive(authors);
      join.selector = _id => {
        handed.push(_id.$in);
        return { _id: _id };
      };
      this.relations.cursor(posts.find({}), function (id, doc) {
        join.push(doc.authorId);
      });
      join.send();
    });
    check('the joined doc is published', t.isPublished('authors', 'A1'), true);
    check('and no observer was created for it', t.db.live('authors').length, 0);

    join.push('A2'); // a late push has to publish on the spot
    check('a late push is published too', t.isPublished('authors', 'A2'), true);

    // The same {$in} shape both times, so a selector written for this.relations.join fits.
    check('the selector is always handed an $in array', handed, [['A1'], ['A2']]);

    // ...and a copy of it: this.data keeps growing, the handed array must not.
    join.push('A3');
    check('the handed array is a copy', handed[0], ['A1']);
  }

  // === _id belongs to the message, not to the fields =======================
  // observeChanges strips _id from the fields it hands over and a plain find
  // does not, so the reactive callback never sees one and the nonreactive one
  // always does. That asymmetry is fine - one is a document, the other is an
  // update - but it must not reach the wire: DDP carries the id separately, and
  // the merge box drops an _id found in fields ("Publish API ignores _id if
  // present in fields", SessionDocumentView.changeField), so sending it is
  // wasted at best and a silent difference between the two paths at worst.
  {
    const t = build();
    const books = t.db.coll('books');
    t.db.insert('books', { _id: 'B1', title: 'orig' });

    const seen = {};
    t.publish(function () {
      this.relations.cursor(books.find({}), 'reactive', function (id, doc) {
        seen.reactive = doc;
      });
      this.relations.cursorNonreactive(books.find({}), 'nonreactive', function (id, doc) {
        seen.nonreactive = doc;
      });
    });

    check('the reactive callback is handed an update, without _id', seen.reactive, { title: 'orig' });
    check('the nonreactive one is handed the whole document', seen.nonreactive, { _id: 'B1', title: 'orig' });

    const fieldsFor = name => (t.events.find(e => e[0] === 'added' && e[1] === name) || [])[3];
    check('neither puts _id in the fields it sends', ['_id' in fieldsFor('reactive'), '_id' in fieldsFor('nonreactive')], [false, false]);
    check('and both send the fields themselves', [fieldsFor('reactive'), fieldsFor('nonreactive')], [{ title: 'orig' }, { title: 'orig' }]);
  }

  return report();
};

publish-relations
=============================

> **Maintained fork of [cottz:publish-relations](https://github.com/lfades/cottz-publish-relations) by [lfades](https://github.com/lfades) (MIT).**
> Includes bug fixes and improvements over the original — see the [releases](https://github.com/danmacko/meteor-publish-relations/releases).

Edit your documents before sending without too much stress.
provides a number of methods to easily manipulate data using internally observe and observeChanges in the server

## Installation

```sh
$ meteor add danmacko:publish-relations
```

## Compatibility

| | |
|---|---|
| **Minimum** | Meteor 2.3 — enforced by `api.versionsFrom` in `package.js` |
| **Tested on** | Meteor 2.15 (both test layers, see [Testing](#testing)) |
| **Meteor 3** | Not yet supported |

## Quick Start
Assuming we have the following collections
```js
// Authors
{
  _id: 'someAuthorId',
  name: 'Luis',
  profile: 'someProfileId',
  bio: 'I am a very good and happy author',
  interests: ['writing', 'reading', 'others']
}

// Books
{
  _id: 'someBookId',
  authorId: 'someAuthorId',
  name: 'meteor for dummies'
}

// Comments
{
  _id: 'someCommentId',
  bookId: 'someBookId',
  text: 'This book is better than meteor for pros :O'
}
```
I want publish the autor with his books and comments of the books
```js
import PublishRelations from 'meteor/danmacko:publish-relations';

PublishRelations('author', function (authorId) {
  this.relations.cursor(Authors.find(authorId), function (id, doc) {
    this.relations.cursor(Books.find({authorId: id}), function (id, doc) {
      this.relations.cursor(Comments.find({bookId: id}));
    });
  });

  return this.ready();
});
```
Note: The above code is very nice and works correctly, but I recommend that you read the Performance Notes
## Main API
to use the following methods you should use `PublishRelations` instead of `Meteor.publish`

The package's methods live in their own namespace on `this.relations`, so they
can never collide with something Meteor's `Subscription` gains later. `this` in
a publication body is the plain subscription, which means `this.userId`,
`this.added()`, `this.ready()` and `this.onStop()` behave exactly as they do in
`Meteor.publish`:

```js
Meteor.publishRelations('books', function () {
  this.relations.cursor(Books.find({ownerId: this.userId}));

  return this.ready();
});
```

Inside a callback the two swap places: `this` is that document's methods object,
where `this.relations` is a self-reference — so the same expression keeps
working at any depth — and the subscription is reachable as `this.sub`.

| where | `this` | the package API | the subscription |
|---|---|---|---|
| publication body | the `Subscription` | `this.relations` | `this` |
| inside a callback | that document's methods | `this.relations` (which is `this`) | `this.sub` |

> **Upgrading from 3.x:**
> earlier versions merged the methods onto the subscription, so `this.cursor()`
> and `this.ready()` sat on one object. Prefix the package's calls in the
> publication body with `relations.`; calls inside callbacks need no change,
> since `this` is the methods object there either way.

### this.relations.cursor (cursor, collection, callbacks(id, doc, changed))
publishes a cursor, `collection` is not required
* **collection** is the collection where the cursor will be sent. if not sent, is the default cursor collection name
* **callbacks** is an object with 3 functions (added, changed, removed) or a function that is called when it is added and changed and receive in third parameter a Boolean value that indicates if is changed
* If you send `callbacks` you can use all the methods again and you can edit the document directly (doc.property = 'some') or send it in the return.

> **Note:**
> when a document changes (update) **doc** contains only the changes, not the whole document.

That note cuts both ways, and the writing side is the easy one to miss. Editing
a field that is *not* in the update adds it to the update, so the client is told
that field changed — and whatever it already held is overwritten:

```js
this.relations.cursor(Meteor.users.find(), function (id, doc, changed) {
  // BUG: on any update that does not touch roomId, doc.roomId is undefined, so
  // this puts 'lobby' into the update and the client loses the real roomId
  doc.roomId = doc.roomId || 'lobby';
});
```

Guard it on the add, or on the field really being part of the update:

```js
this.relations.cursor(Meteor.users.find(), function (id, doc, changed) {
  if (!changed || 'roomId' in doc)
    doc.roomId = doc.roomId || 'lobby';
});
```

`'roomId' in doc` also holds when the field was cleared — a removed field arrives
in the update with the value `undefined` — so the default still applies then.

Defaults like this are usually better applied where the data is read, since the
publication has to describe a change and not a state.

### this.relations.join (Collection, options, name)
It allows you to collect a lot of _ids and then make a single query, only Collection is required.
* **Collection** is the Mongo Collection to be used
* **options** the options parameter in a Collection.find
* **name** the name of a different collection to receive documents there

After creating an instance of `this.relations.join` you can do the following
```js
const comments = this.relations.join(Comments, {});
// default query is {_id: {$in: _ids}}
// if you need to use another field use selector
comments.selector = function (_ids) {
  // _ids is always {$in: [...]}, one element or many
  return {bookId: _ids};
};
// Adds a new id to the query
comments.push(id);
comments.push(id2, id3, id4);
// Sends the query to the client. From then on a push that changes what the
// join holds restarts its query - coalesced, so a burst of them costs one
// restart, and a push of something already held costs nothing at all. You do
// not have to worry about reactivity or performance with this method
comments.send();
```
Why use this and not `this.relations.cursor`? because they are just 2 queries
```js
const comments = this.relations.join(Comments, {});
comments.selector = _ids => ({bookId: _ids});

this.relations.cursor(Books.find(), function (id, doc) {
  comments.push(id);
});

comments.send();
```

> **Note:**
> declare a join in the publication body, as above — never inside a callback.
> `push()` belongs in callbacks, the join itself does not.

A callback runs again every time its document changes, so a join created inside
one is a new instance each time. Each of them registers on that document's
handler and none of them is released until the document leaves the result set —
at which point every stale instance restarts its own observe and retracts
through it. Nothing leaks, but a document that has changed fifty times pays for
fifty of them at once, and there is no way for the package to tell that the
older ones are finished with.

### this.relations.observe / this.relations.observeChanges (cursor, callbacks)
observe or observe changes in a cursor without sending anything to the client, callbacks are the same as those used by meteor

## Nonreactive API
The following methods work much like their peers but they are not reactive

### this.relations.cursorNonreactive (cursor, collection, callback)
It has 2 differences with `this.relations.cursor`
- `callback` is only a function that executes when a document is added
- you can only use non-reactive methods within the callback

### this.relations.joinNonreactive (Collection, options, name)
Is exactly the same as `this.relations.join` but non reactive

## Performance Notes
* every method hands back something with a `stop()` on it, with one exception:
  `join.send()` returns nothing when the join has collected no ids, because
  there is no observer to stop until it has some
* all cursors are stopped when the publication stop
* when the parent cursor is stopped or a document with cursors is removed all related cursors are stopped
* all cursors use basic observeChanges as meteor does by default, performance does not come down
* if when the callback is re-executes not called again some method (within an If for example), the method continues to run normally, if you re-call method (because the selector is now different) the previous method is replaced with the new
```js
// For example we have a collection users and each user has a roomId
// we want to publish the users and their rooms
this.relations.cursor(Meteor.users.find(), function (id, doc) {
  // this function is executed on added/changed
  this.relations.cursor(Rooms.find({_id: doc.roomId}));
});
// the previous cursor is good but has a bug, when an user is changed we can't make sure
// that the roomId is changed and 'doc' only comes with the changes, so roomId is undefined
// and our Rooms cursor no longer work anymore

// to fix the above problem we need to check the roomId
this.relations.cursor(Meteor.users.find(), function (id, doc) {
  if (doc.roomId)
    this.relations.cursor(Rooms.find({_id: doc.roomId}));
});
// or we can use an object with 'added' instead of a function
// this way is better than the above if we are sure that roomId is not going to change
this.relations.cursor(Meteor.users.find(), {
  added: function (id, doc) {
    this.relations.cursor(Rooms.find({_id: doc.roomId}));
  }
});
```
* As I said in Quick Start you can do this
```js
this.relations.cursor(Authors.find(authorId), function (id, doc) {
  this.relations.cursor(Books.find({authorId: id}), function (id, doc) {
    this.relations.cursor(Comments.find({bookId: id}));
  });
});
```
but you will find that the publication is becoming increasingly slow, suppose you have 10 books for a given author and every book has 100 reviews, with this method would make the following queries:
1 author + 1 books + 10 comments = 12 queries, for each book found a query is made to find comments which creates a performance issue and publication could take seconds

The solution is to use `this.relations.join` to join all the comments and send them in a single query, passing from 12 queries to 3 queries for mongo
```js
const comments = this.relations.join(Comments);
comments.selector = _ids => ({bookId: _ids});

this.relations.cursor(Authors.find(authorId), function (id, doc) {
  // We not have to worry about the books cursor because we only have one author
  this.relations.cursor(Books.find({authorId: id}), function (id, doc) {
    comments.push(id);
  });
});

comments.send();
```
* publications are completed as usual
```js
// you can do this to finish writing your publication
this.ready();
return this.ready();
return [];
return [cursor1, cursor2, cursor3];
```

## Limitations

### A re-pointed foreign key can keep the old joined document

What a callback pushes is added to what that document already contributes; a
re-run never replaces it. Whether a re-pointed foreign key therefore leaves the
old joined document behind depends on where the `push` is:

Both of these react to the same edit — `book.authorId` going from `A1` to `A2` —
and only the position of the `push` differs:

```js
// (1) the callback that reads the key pushes
this.relations.cursor(Books.find(), function (id, doc) {
  if (doc.authorId) authors.push(doc.authorId);
});
// -> the join holds A1 AND A2

// (2) a cursor that the key rebuilds pushes
this.relations.cursor(Books.find(), function (id, doc) {
  if (doc.authorId) this.relations.cursor(Authors.find({_id: doc.authorId}), function (aid, author) {
    countries.push(author.countryId);
  });
});
// -> the join holds A2's country, and A1's is retracted
```

| where the `push` is | what happens on a re-pointed key |
|---|---|
| the callback that reads the key | the old value stays declared alongside the new one |
| a nested cursor's callback, when the rebuilt cursor matches something | what the old nested document declared is released and retracted from the client |
| a nested cursor's callback, when the rebuilt cursor matches nothing | kept, exactly as in the first row |

(1) is a deliberate trade, not an oversight. Replacing on every re-run needs the
callback to state everything the document contributes, and it cannot: a `changed`
callback is handed the update, not the document, so on any update that does not
touch `authorId` it would declare nothing and drop a valid link. Ending up with
one document too many is the better failure, so that is the one the package
takes. It costs an extra document on the client and one extra id in the `{$in}`,
both bounded by how many times the contributing documents re-point a key while
they are in the result set.

(2) works because the rebuilt cursor re-declares for itself what it still holds,
which is a statement (1) has no way to make. The joined documents it no longer
declares are retracted, so this is the shape to reach for when a key really does
churn — but only where a nested cursor makes sense in the first place. Replacing
the join with a nested cursor on the joined collection is not the same move and
does not help: stopping an observer sends no `removed`, so the old document stays
on the client from there too.

(3) is where the rebuilt cursor delivered nothing at all, and the package cannot
tell "the selector was built from a key this update does not carry" from "the
selector is right and nothing matches it". The first means it could not ask and
the ids must be kept; the second means they should go. It keeps them, because a
document too many beats a document missing — so a key re-pointed at something
that does not exist behaves like (1).

Client code normally reads joined documents by the foreign key it finds on the
parent, so a superseded one is simply never looked up; where the collection
itself is what gets rendered, filter it by the keys the parent documents hold.

### Two guarded `this.relations.cursor` calls on the same collection, in one callback

Needs all four of these together, so most publications can stop reading here:

* two or more `this.relations.cursor` calls **on the same collection**
* both inside the **same callback**
* at least one of them **guarded** by an `if`
* an update that reaches a later call while skipping an earlier one

`this.relations.join` is not affected at all, however many joins there are on a
collection: a join takes its slot key once, when it is constructed in the
publication body, and keeps it for the life of the subscription.

A nested cursor, by contrast, is identified by its collection and by the order of
the `this.relations.cursor` calls in the callback - which is what lets a re-run replace its
own observer. A guard breaks that ordering when two of them are on the same
collection:

```js
this.relations.cursor(Books.find(), function (id, doc) {
  if (doc.mainAuthorId) this.relations.cursor(Authors.find({_id: doc.mainAuthorId}));
  if (doc.editorId) this.relations.cursor(Authors.find({_id: doc.editorId}));   // same collection
});
```

On an update carrying `editorId` but not `mainAuthorId` the first call is
skipped, so the second is now the first `Authors` cursor of that run and is
handed the first one's slot: it stops the main author's observer and leaves its
own previous observer running in a slot it no longer claims.

Nothing leaks on the server - the number of observers stays put - but the client
does not recover on its own. Each such update leaves one more document sitting in
the client's collection with no observer behind it: it will never change again and
never be removed, and nothing about it says so. Meanwhile the editor the second
cursor stopped claiming keeps its observer, so the client also keeps receiving
updates for a document the publication no longer wants.

Two cursors on one collection are fine unguarded, and any number of guarded
cursors are fine on *different* collections. Only the combination bites. Send
one of the two under its own name - `this.relations.cursor(cursor, 'editors')` is a
separate slot, and a separate collection on the client - or use the `added`
form from Performance Notes, which does not re-run at all. Removing the guards
is not a fix: the callback is handed the update, so the selector would be built
from a key that is not in it, and the cursor would be replaced by one matching
nothing.

### One publication sending the same collection twice

Everything a publication sends goes out under a single subscription handle, and
Meteor tracks a published document per *handle*, not per publisher. So when a
join and a cursor of the same publication both send documents of one collection,
the first `removed` from either takes the document away from the client even
though the other still matches it — and it comes back only when that other
cursor next restarts.

Keep the join's membership a superset of what the cursors publish and it cannot
happen, because a retraction then only fires once nothing wants the document any
more:

```js
// Rooms are sent twice here: by the join, and by the nested cursor.
this.relations.cursor(Meteor.users.find(), function (id, doc) {
  if (doc.roomId) {
    rooms.push(doc.roomId);                       // <- keeps the two in step
    this.relations.cursor(Rooms.find({_id: doc.roomId}), function (id, room) {
      owners.push(room.ownerId);
    });
  }
});
```

That is worth checking whenever a publication has both a join and a cursor on
one collection: for every such cursor there should be a push into the matching
join. Where that is not possible, either give one of them its own name
(`this.relations.cursor(cursor, 'roomsLookup', callbacks)`, which sends it to a separate
client collection) or use `this.relations.observe`, which runs callbacks without sending
anything at all.

In development the package warns when this bites — once per collection, when a
cursor tries to update a document a join has already retracted. Two caveats:
the warning reports the *consequence*, so a document that is retracted and then
never changes again produces no warning at all; and it is gated on
`Meteor.isDevelopment`, so a staging build running in production mode stays
silent about it.

## Testing

The suite has two layers. Run both with one command:

```bash
./test.sh          # unit layer only — fast, no side effects
./test.sh --full   # both layers (boots a Meteor test server on $PORT, default 3199)
```

**Unit layer** (`tests/unit/`) loads the server modules into a `vm` with Meteor
stubbed out, so it runs in plain node in milliseconds — no Meteor, no MongoDB.
That gives deterministic control over things a DDP client cannot see: what sits
in the deferred restart queue, what happens when a release lands in the same
tick as a push, how a subscription that is already deactivated behaves, and the
overlap guards for two cursors publishing the same collection.

```bash
node tests/unit/run.js
```

The test scripts require Node 14.18+ (they import builtins with the `node:`
prefix). `./test.sh` checks for that and falls back to the Node bundled with
the Meteor tool when the system one is older or missing.

**Tinytest layer** (`tests/*.js`) runs against a real Meteor server, a real
MongoDB and a real DDP client. It owns what only the real stack can show: DDP
message order, and the observer lifecycle — leaks, replacement on restart, and
teardown. Observer counts are read from `MongoInternals`, because a leaked
observer is invisible from the client side.

Every test in this package is server-side, so no browser is needed. Instead of
the browser reporter (or `test-in-console`, which drives headless Chrome via
puppeteer), `tests/headless-driver.js` speaks raw DDP to the test server and
calls the `tinytest/run` method directly:

```bash
meteor test-packages --release METEOR@2.15 --port 3199 ./   # one shell
node tests/headless-driver.js                               # another
```

It exits non-zero when a test fails, so it can gate CI.

Tinytest has no built-in timeouts, so the tests use a `deadline()` helper — a
regression must report as a failure, not hang the suite.

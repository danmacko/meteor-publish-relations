import { isThenable } from '../../thenable';

export default class CursorMethodsNR {
  constructor (sub) {
    this.sub = sub;

    // Registrations started in THIS frame and not finished yet. A frame is one
    // callback invocation - or the publication body, which is the outermost one.
    //
    // On Meteor 3 a cursor is only registered asynchronously, so without this
    // the package would have to be driven with an await on every call. Instead
    // each frame remembers what it started and waits for it at the point where
    // the answer matters: cursor.js waits before it writes the parent document,
    // publish_relations.js before it lets ready() reach the client.
    this._pending = [];
    // The first failure among them, kept until _settle() reports it.
    this._failed = null;
  }

  // Nested callbacks are invoked with a CursorMethods(NR) as `this`, not with
  // the subscription - so `this.relations` has to resolve here too, otherwise
  // the API would be this.relations.cursor() at the top level and this.cursor()
  // one line deeper. Inherited by CursorMethods, so both nested call sites
  // (cursor.js and cursorNonreactive) are covered by this one getter.
  // The subscription API stays reachable inside a callback as `this.sub`.
  get relations () {
    return this;
  }

  // Registers a promise with this frame and hands it straight back, so every
  // method still returns what it always returned (awaiting it is the caller's
  // choice, never a requirement).
  _track (promise) {
    if (!isThenable(promise)) return promise;

    const pending = this._pending;
    pending.push(promise);

    // Forgotten as soon as it settles: the publication body's frame lives for
    // the whole subscription, and it has no business holding on to every
    // registration a live update ever made. Attaching the handler here also
    // means a rejection is never unhandled - _settle() is what reports it.
    const forget = () => {
      const at = pending.indexOf(promise);
      if (at !== -1) pending.splice(at, 1);
    };

    promise.then(forget, error => {
      forget();
      // Remembered, because dropping it is not the same as having reported it.
      // _settle() may not run until well after this - an async body, or a
      // callback that awaits something else after opening a cursor - and by
      // then there would be nothing left in the list for Promise.all to reject
      // with. The publication would go quietly ready on a registration that
      // never happened, with the failure swallowed by this very handler.
      if (!this._failed) this._failed = error;
    });

    return promise;
  }

  // Everything this frame has started, settled. Loops rather than awaiting one
  // snapshot: a registration can start another (a cursor whose callback opens a
  // nested one), and the frame is only done when nothing is left running.
  async _settle () {
    while (this._pending.length) {
      await Promise.all(this._pending.slice());
    }

    // Cleared as it is reported: the caller turns this into the publication's
    // error (or the parent document's), and a frame that settles again later
    // has no business raising the same thing twice.
    const failure = this._failed;
    if (failure) {
      this._failed = null;
      throw failure;
    }
  }

  // What the frame has started SO FAR. No loop, deliberately: a caller that is
  // about to join the frame itself - a join's send() - can wait for what came
  // before it without ending up waiting for itself.
  _settleStarted () {
    return Promise.all(this._pending.slice());
  }

  cursorNonreactive (cursor, collection, onAdded) {
    return this._track(this._readCursorNonreactive(cursor, collection, onAdded));
  }

  async _readCursorNonreactive (cursor, collection, onAdded) {
    const sub = this.sub;

    if (typeof collection !== 'string') {
      onAdded = collection;
      collection = cursor._getCollectionName();
    }
    if (typeof onAdded !== 'function')
      onAdded = function () {};

    // forEachAsync awaits each callback in turn (AsynchronousCursor.forEach), so
    // an async callback finishes before the next document is read - the same
    // one-document-at-a-time order the synchronous cursor had.
    await cursor.forEachAsync(async (doc) => {
      const id = doc._id;
      const methods = new CursorMethodsNR(sub);
      // The callback is handed the whole document, _id included - it comes from
      // a plain find, not from observeChanges, and there is no reason to hide it.
      // What goes out is another matter: DDP carries the id separately and the
      // merge box drops an _id found in fields (SessionDocumentView.changeField,
      // "Publish API ignores _id if present in fields"), so sending it is at best
      // wasted bytes and at worst a difference from the reactive path that has to
      // be remembered. Stripped after the callback, so a callback that edited or
      // replaced the document still decides what the fields are.
      const { _id, ...fields } = (await onAdded.call(methods, id, doc)) || doc;
      // Whatever the callback opened is part of this document, so it goes out
      // with it rather than after the publication has said it is ready.
      await methods._settle();
      sub.added(collection, id, fields);
    });
  }
};

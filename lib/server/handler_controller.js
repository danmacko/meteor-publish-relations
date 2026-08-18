import { withObserveLock } from './observe-lock';

// Two things in one tree. Every observer a publication creates hangs somewhere
// in it - `handler` is this node's own, `handlers` its children - so stopping a
// node stops everything below it, which is how a document leaving a result set
// takes its nested cursors with it.
//
// The nodes are also identities: the per-document controller under a cursor is
// what a join keys its contributions by, so the same teardown that stops the
// observers is what releases the joined ids (see trackFor).
export default class HandlerController {
  constructor () {
    this.handlers = {};
    this._stopped = false;
  }
  set (handler) {
    // observeChanges() only returns once it has delivered its initial adds, so
    // by the time the handle gets here the replacement has re-declared, through
    // its own per-document controllers, whatever it still holds - and what was
    // carried over the swap (see transferContributionsTo) has done its job.
    //
    // Unless it delivered nothing at all, which is not the same statement. A
    // cursor rebuilt from a changed callback is handed the update, not the
    // document, so its selector is routinely built from a foreign key the update
    // does not carry; it then matches nothing, and dropping the carry would take
    // the joined documents off the client on any unrelated edit of the parent.
    // One document is enough to tell a cursor describing the world from one with
    // nothing to describe it with.
    //
    // _holders is the whole record of the carry, with nothing else mixed in:
    // add() always builds a FRESH controller for the replacement, and the only
    // other way to get registered here is push(), whose owner is always a
    // per-document controller (cursor.js frames every callback with addBasic).
    // So a cursor slot's holders can only have come from a transfer.
    if (this._delivered() && this._holders) {
      const holders = this._holders;
      this._holders = null;
      holders.forEach(holder => holder.dropContribution(this));
    }

    // add() registers a slot EMPTY and it is only filled here, because
    // cursor.observeChanges() yields while building the observer. A restart or
    // teardown landing in that window would otherwise find nothing to stop,
    // and this observer would arrive orphaned - live against Mongo with no
    // reference left to stop it. Stop the late arrival instead of storing it.
    if (this._stopped) {
      if (handler && typeof handler.stop === 'function') handler.stop();
      return handler;
    }
    return this.handler = handler;
  }
  // A controller that is born stopped, handed to registrations that land after
  // stop(). set() on it disposes of whatever the caller is still building, and
  // add()/addBasic() on it keep returning stopped controllers, so a whole
  // subtree created after teardown stays inert.
  _stoppedChild () {
    const child = new HandlerController();
    child._stopped = true;
    return child;
  }
  addBasic (collection) {
    // Nothing may go live after stop(): this controller is never stopped again,
    // and stop() has already emptied this.handlers, so a child registered here
    // would keep its observers running until the server restarts. set()'s latch
    // does not cover it - add()/addBasic() hand out FRESH controllers whose own
    // _stopped is false, and set() is called on those, not on this one.
    if (this._stopped) return this._stoppedChild();

    const oldHandler = this.handlers[collection];
    return oldHandler || (this.handlers[collection] = new HandlerController());
  }
  async add (cursor, options) {
    if (!cursor)
      throw new Error("you're not sending the cursor");

    // Same as addBasic. Returning before the cursor[options.handler]() call
    // below also avoids building an observer only to tear it down again.
    if (this._stopped) return this._stoppedChild();

    const description = cursor._cursorDescription;
    const collection = options.collection || description.collectionName;
    // One slot per LOGICAL cursor, not per collection: the key is unique per
    // call site but stable across re-runs of the same cursor, so replacing a
    // slot always means "this cursor superseding itself" - never two different
    // cursors on one collection killing each other's observers.
    const key = options.key || collection;

    // The previous handler must be stopped before the reference is overwritten,
    // or its observeChanges keeps running with the older $in and produces "The
    // Mongo server and the Meteor query disagree on how many documents match
    // your query" during oplog reconciliation.
    const oldHandler = this.handlers[key];

    // An observe/observeChanges handle has to be stopped before its replacement
    // is built, so the two never run at once. A plain controller carries
    // contributions instead, and those must be handed over BEFORE stop() - it
    // empties the subtree the handover has to walk - so its replacement is built
    // first. Nothing observable happens either way: a fresh controller is inert
    // until set() fills it.
    const newHandler = options.handler ? null : new HandlerController();

    if (newHandler && oldHandler && typeof oldHandler.transferContributionsTo === 'function') {
      oldHandler.transferContributionsTo(newHandler);
    }

    if (oldHandler && typeof oldHandler.stop === 'function') {
      oldHandler.stop();
    }

    // Awaited: on Meteor 3 observe() and observeChanges() both hand back a
    // promise (MongoConnection._observeChanges is async), so without this the
    // slot would hold a promise - nothing to stop, and nothing that answers
    // _delivered() either.
    const handler = newHandler ||
      await withObserveLock(cursor, () => cursor[options.handler](options.callbacks));

    // Building an observe handle suspends this call, so a teardown can land in
    // the middle of it. The this.cursor path is covered by set()'s latch, this
    // one has no such step - and stop() has already emptied this.handlers, so
    // storing it here would leave it running against Mongo with nothing left to
    // stop it.
    if (this._stopped) {
      if (handler && typeof handler.stop === 'function') handler.stop();
      return handler;
    }

    return this.handlers[key] = handler;
  }
  // A per-document controller is the identity of "this document's callback, in
  // this cursor", so it is what a join keys its contributions by. Holders
  // register here; stop() then releases them, and since stop() already recurses
  // into child controllers, a parent document's teardown releases its nested
  // contributions too - which is why the join needs no parentage bookkeeping.
  // A holder must expose dropContribution(controller).
  trackFor (holder) {
    (this._holders || (this._holders = new Set())).add(holder);
  }
  // Carry everything this subtree contributed over to `target`. Called when a
  // cursor supersedes itself: the old controllers leave the tree, so without
  // this their contributions would have no owner left, and a join would retract
  // them in the window between the old observer stopping and the new one
  // delivering its adds.
  //
  // It is a bridge across that window and nothing more. The per-document
  // granularity does not survive it - every id lands on one controller - so
  // holding on to it would pin ids to a controller that only dies with the
  // parent document: a later `removed` of one nested document would find its own
  // contribution released and the carried copy still standing. set() drops the
  // lot as soon as the replacement has spoken for itself.
  transferContributionsTo (target) {
    let handlers = this.handlers;
    for (let key in handlers) {
      const child = handlers[key];
      if (child && typeof child.transferContributionsTo === 'function') child.transferContributionsTo(target);
    }

    const holders = this._holders;
    if (!holders) return;
    this._holders = null;
    holders.forEach(holder => holder.transferContribution(this, target));
  }
  // Did the observer just built deliver any document? Its callbacks register a
  // controller per document (addBasic), so one entry is the whole answer.
  _delivered () {
    for (const key in this.handlers) return true;
    return false;
  }
  // Stopping releases what this subtree contributed, because it means the
  // documents behind it left the result set. A cursor rebuilding ITSELF is the
  // one case where that is wrong - the callback merely re-ran - and add() covers
  // it by handing the contributions over first, so nothing is left here to
  // release. Getting that backwards would be a data-loss bug rather than a leak:
  // a changed callback is handed the update, not the document, so a rebuilt
  // cursor may be unable to re-declare what the old one held, and the joined
  // documents would vanish on any unrelated edit of the parent.
  //
  // Whether it CAN re-declare is not decided here - set() decides it, once the
  // replacement has had its say. This method only has to leave nothing behind.
  stop () {
    let handlers = this.handlers;

    // Latch first: an observer still being created for this slot must be
    // stopped by set() when it arrives (see the comment there).
    this._stopped = true;
    this.handler && this.handler.stop();

    for (let key in handlers) {
      handlers[key].stop();
    };

    this.handlers = {};

    // After the children, so a parent's release sees a fully torn-down subtree.
    const holders = this._holders;
    if (holders) {
      this._holders = null;
      holders.forEach(holder => holder.dropContribution(this));
    }
  }
  remove (_id) {
    let handler = this.handlers[_id];
    if (handler) {
      handler.stop();
      delete this.handlers[_id];
    }
  }
}

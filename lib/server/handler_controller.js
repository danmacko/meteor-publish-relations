// The aim of handler Controller is to keep all observers that can be created within methods
// its structure is very simple, has a 'handlers' object containing all observers children and
// the observer father is stored within 'handler'
export default class HandlerController {
  constructor () {
    this.handlers = {};
    this._stopped = false;
  }
  set (handler) {
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
  addBasic (collection, handler) {
    // Nothing may go live after stop(): this controller is never stopped again,
    // and stop() has already emptied this.handlers, so a child registered here
    // would keep its observers running until the server restarts. set()'s latch
    // does not cover it - add()/addBasic() hand out FRESH controllers whose own
    // _stopped is false, and set() is called on those, not on this one.
    if (this._stopped) {
      if (handler && typeof handler.stop === 'function') handler.stop();
      return this._stoppedChild();
    }

    const oldHandler = this.handlers[collection];
    return oldHandler || (this.handlers[collection] = handler || new HandlerController());
  }
  add (cursor, options) {
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
    const selector = description.selector;

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

    const handler = newHandler || cursor[options.handler](options.callbacks);
    handler.selector = selector;

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
  // Move everything this subtree contributed onto `target`. Called when a cursor
  // supersedes itself: the old controllers leave the tree, so without this their
  // contributions would have no owner left and could never be released. The
  // per-document granularity is lost in the process, which costs nothing - that
  // cursor is gone, and only the replacement can be released from now on.
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
  // Stopping releases what this subtree contributed, because it means the
  // documents behind it left the result set. A cursor rebuilding ITSELF is the
  // one case where that is wrong - the callback merely re-ran - and add() covers
  // it by handing the contributions over first, so nothing is left here to
  // release. Getting that backwards would be a data-loss bug rather than a leak:
  // a changed callback is handed the update, not the document, so a rebuilt
  // cursor usually cannot re-declare what the old one held, and the joined
  // documents would vanish on any unrelated edit of the parent.
  stop () {
    let handlers = this.handlers;

    // Latch first: an observer still being created for this slot must be
    // stopped by set() when it arrives (see the comment there).
    this._stopped = true;
    this.handler && this.handler.stop();

    for (let key in handlers) {
      handlers[key].stop();
    };

    this.handlers = [];

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

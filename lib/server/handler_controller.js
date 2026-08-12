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
  addBasic (collection, handler) {
    const oldHandler = this.handlers[collection];
    return oldHandler || (this.handlers[collection] = handler || new HandlerController());
  }
  add (cursor, options) {
    if (!cursor)
      throw new Error("you're not sending the cursor");

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
    if (oldHandler && typeof oldHandler.stop === 'function') {
      oldHandler.stop();
    }

    const newHandler = options.handler
    ? cursor[options.handler](options.callbacks)
    : new HandlerController();

    newHandler.selector = selector;

    return this.handlers[key] = newHandler;
  }
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
  }
  remove (_id) {
    let handler = this.handlers[_id];
    if (handler) {
      handler.stop();
      delete this.handlers[_id];
    }
  }
}

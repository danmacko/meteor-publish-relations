import CursorMethods from './cursor';

// Per-call registry keys for the same reason as in cursor(): two observes on
// the same collection must not stop each other (see HandlerController.add).
CursorMethods.prototype.observe = function (cursor, callbacks) {
  this.handler.add(cursor, {
    handler: 'observe',
    callbacks: callbacks,
    key: cursor._cursorDescription.collectionName + '#o' + this._registrySeq++
  });
};

CursorMethods.prototype.observeChanges = function (cursor, callbacks) {
  this.handler.add(cursor, {
    handler: 'observeChanges',
    callbacks: callbacks,
    key: cursor._cursorDescription.collectionName + '#o' + this._registrySeq++
  });
};

import CursorMethods from './cursor';
import { currentContributor, runInContributor } from './contributor-context';

// Binds every callback to the contributor that created the observe.
//
// Without this, ownership would depend on WHEN a document arrives. Meteor
// dispatches an observe's initial adds through _SynchronousQueue.runTask, which
// wraps the task in bindEnvironment and so restores the creating fiber's
// environment - those callbacks do see the contributor. Later events go through
// queueTask, which wraps nothing, so they run in the drain fiber with no
// contributor at all and anything they push into a join becomes a static id
// that is never released. The same callback would be tracked or pinned for good
// depending on which documents happened to be there at the time.
//
// One rule instead: what an observe pushes belongs to whoever created it. Inside
// a cursor callback that is the parent document, so those pushes are released
// with it; in the publication body there is no contributor, and they are static
// exactly like a push written in the body itself.
function bindToContributor (callbacks) {
  if (!callbacks) return callbacks;

  const owner = currentContributor();
  const bound = {};

  for (const name in callbacks) {
    const callback = callbacks[name];
    // `this` is the observe handle Meteor supplies - keep it.
    bound[name] = typeof callback !== 'function' ? callback : function (...args) {
      return runInContributor(owner, () => callback.apply(this, args));
    };
  }

  return bound;
}

// Per-call registry keys for the same reason as in cursor(): two observes on
// the same collection must not stop each other (see HandlerController.add).
// Both hand back the observe handle, so the documented "every method returns
// something with a stop() on it" holds here too - add() already has it, these
// were simply dropping it on the floor.
//
// A promise of the handle, since Meteor 3: add() awaits the observe, and these
// forward what it hands back. Awaiting it is optional for the caller - the
// publication does not become ready until the registration has settled.
CursorMethods.prototype.observe = function (cursor, callbacks) {
  return this._track(this.handler.add(cursor, {
    handler: 'observeAsync',
    callbacks: bindToContributor(callbacks),
    key: this._nextRegistryKey(cursor._cursorDescription.collectionName, 'o')
  }));
};

CursorMethods.prototype.observeChanges = function (cursor, callbacks) {
  return this._track(this.handler.add(cursor, {
    handler: 'observeChangesAsync',
    callbacks: bindToContributor(callbacks),
    key: this._nextRegistryKey(cursor._cursorDescription.collectionName, 'o')
  }));
};

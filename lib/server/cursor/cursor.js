import { Meteor } from 'meteor/meteor';
import CursorMethodsNR from './nonreactive';
import { isPublishedInSub } from './published';
import { runInContributor } from './contributor-context';

export default class CursorMethods extends CursorMethodsNR {
  constructor (sub, handler, _id, collection) {
    super(sub);

    this.handler = handler;
    this._id = _id;
    this.collection = collection;

    // Feeds the per-cursor registry keys (see HandlerController.add). A nested
    // callback re-run gets a FRESH CursorMethods, so the counter restarts and
    // the same call order yields the same keys - which is what lets a
    // re-created cursor supersede its own previous observer.
    this._registrySeq = Object.create(null);
  }
  // Per-call registry key (see HandlerController.add), counted PER COLLECTION so
  // that a conditionally created cursor cannot shift the key of the next one.
  // Guarding a nested cursor - `if (doc.someId) this.cursor(...)`, the form the
  // README recommends - means the callback sometimes creates it and sometimes
  // does not. With one shared counter the following cursor would then be handed
  // the skipped one's key, fail to supersede its OWN previous observer, and
  // leave it running for the life of the document.
  _nextRegistryKey (name, kind) {
    const bucket = name + '#' + kind;
    const seq = this._registrySeq[bucket] || 0;
    this._registrySeq[bucket] = seq + 1;
    return bucket + seq;
  }
  cursor (cursor, collection, callbacks, registryKey) {
    const sub = this.sub;

    if (typeof collection !== 'string') {
      callbacks = collection;
      collection = cursor._getCollectionName();
    }

    // registryKey is internal (a join passes its stable key so a restart
    // replaces its own observer); plain this.cursor() calls get a fresh slot.
    const cursorKey = registryKey || this._nextRegistryKey(collection, 'c');
    const handler = this.handler.add(cursor, {
      collection: collection,
      key: cursorKey
    });
    // if (handler.equalSelector)
    //   return handler;

    // add() hands back a pre-stopped controller when the subscription is already
    // torn down. set() would dispose of the observer below anyway, but bailing
    // out here skips building it - and the Mongo round-trip - in the first place.
    if (handler._stopped) return handler;

    if (callbacks)
      callbacks = this._getCallbacks(callbacks);

    function applyCallback (id, doc, method) {
      const cb = callbacks && callbacks[method];

      if (cb) {
        // The per-document controller IS this contributor's identity: stable
        // across added/changed, distinct for the same _id reached through
        // another cursor, and distinct for the same nested doc reached under
        // another parent. join.push() keys its contributions by it.
        const owner = handler.addBasic(id);
        let methods = new CursorMethods(sub, owner, id, collection),
        isChanged = method === 'changed';

        // Bound to the Fiber rather than kept on a stack, so a callback that
        // yields keeps its own owner even while a sibling cursor's callback runs
        // (see contributor-context.js).
        return runInContributor(owner, () => cb.call(methods, id, doc, isChanged) || doc);
      } else
        return doc;
    };

    let observeChanges = cursor.observeChanges({
      added (id, doc) {
        sub.added(collection, id, applyCallback(id, doc, 'added'));
      },
      changed (id, doc) {
        // Run the callback unconditionally - it keeps join contributions and
        // nested cursors up to date regardless of what the client sees.
        const fields = applyCallback(id, doc, 'changed');

        // Two observers of this publication may share a DDP collection name: if
        // the other one already retracted this doc, forwarding a changed throws
        // "Could not find element with id ... to change". Only a provable "not
        // published" suppresses the call.
        if (isPublishedInSub(sub, collection, id) === false) {
          // The doc is in the "transient hide" state and stays invisible until
          // this cursor re-adds it. Correct overlap would need per-cursor
          // refcounting (the merge box "XXX" in Meteor core); warn once per
          // subscription+collection in development instead.
          if (Meteor.isDevelopment) {
            const warned = sub._prHiddenWarned || (sub._prHiddenWarned = new Set());
            if (!warned.has(collection)) {
              warned.add(collection);
              console.warn(`publish-relations: '${collection}' doc '${id}' is retracted but still matched by another ` +
                'cursor of the same publication - overlapping same-name publishes hide such docs until the next restart');
            }
          }
          return;
        }

        sub.changed(collection, id, fields);
      },
      removed (id) {
        if (callbacks) {
          callbacks.removed(id);
          // Stopping this document's controller releases whatever its callback
          // contributed to any join, and recurses into the controllers of any
          // cursor it nested - so the {$in} shrinks instead of growing forever,
          // with no parentage bookkeeping anywhere.
          handler.remove(id);
        }

        // Same two-observer situation as in changed: when the doc is already
        // retracted (the other observer's removed fired first, or a join
        // retraction took it), a second sub.removed throws "Removed
        // nonexistent document". Only a provable "not published" suppresses
        // the call - on unknown (null) we forward, as skipping would strand
        // the doc on the client forever.
        if (isPublishedInSub(sub, collection, id) === false) return;

        sub.removed(collection, id);
      }
    });

    return handler.set(observeChanges);
  }
};
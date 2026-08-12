import { Meteor } from 'meteor/meteor';
import CursorMethodsNR from './nonreactive';
import { isPublishedInSub } from './published';

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
    this._registrySeq = 0;
  }
  cursor (cursor, collection, callbacks, registryKey) {
    const sub = this.sub;
    // The doc whose callback created THIS cursor (null at the top level). It is
    // the parent of every contributor this cursor produces, captured here so it
    // is correct even for asynchronous nested adds (where the parent is no
    // longer on the call stack). Lets a parent removal cascade-release its
    // nested contributors (see join.js release()).
    const parentContributorId = this._id != null ? this._id : null;

    if (typeof collection !== 'string') {
      callbacks = collection;
      collection = cursor._getCollectionName();
    }

    // registryKey is internal (a join passes its stable key so a restart
    // replaces its own observer); plain this.cursor() calls get a fresh slot.
    const handler = this.handler.add(cursor, {
      collection: collection,
      key: registryKey || collection + '#c' + this._registrySeq++
    });
    // if (handler.equalSelector)
    //   return handler;

    if (callbacks)
      callbacks = this._getCallbacks(callbacks);

    function applyCallback (id, doc, method) {
      const cb = callbacks && callbacks[method];

      if (cb) {
        let methods = new CursorMethods(sub, handler.addBasic(id), id, collection),
        isChanged = method === 'changed';

        // Expose the current contributor id AND its parent so join.push() can
        // refcount by contributor, record parentage, and later release it (and
        // cascade to nested contributors) on removal (see cursor/join.js).
        const stack = (sub._prContributorStack = sub._prContributorStack || []);
        stack.push({ id, parent: parentContributorId });
        try {
          return cb.call(methods, id, doc, isChanged) || doc;
        } finally {
          stack.pop();
        }
      } else
        return doc;
    };

    let observeChanges = cursor.observeChanges({
      added (id, doc) {
        sub.added(collection, id, applyCallback(id, doc, 'added'));
      },
      changed (id, doc) {
        // Run the callback unconditionally - it keeps join refcounts and
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
          handler.remove(id);
        }

        // This parent doc is gone: let every join drop the joined ids that only
        // it was keeping alive, so the {$in} shrinks instead of growing forever.
        const joins = sub._prJoins;
        if (joins) {
          for (let i = 0; i < joins.length; i++) {
            joins[i].release(id);
          }
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
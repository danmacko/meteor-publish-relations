import CursorMethodsNR from './nonreactive';

export default class CursorMethods extends CursorMethodsNR {
  constructor (sub, handler, _id, collection) {
    super(sub);

    this.handler = handler;
    this._id = _id;
    this.collection = collection;
  }
  cursor (cursor, collection, callbacks) {
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

    const handler = this.handler.add(cursor, {collection: collection});
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
        sub.changed(collection, id, applyCallback(id, doc, 'changed'));
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

        sub.removed(collection, id);
      }
    });

    return handler.set(observeChanges);
  }
};
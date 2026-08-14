import { Meteor } from 'meteor/meteor';
import CursorMethodsNR from './nonreactive';
import { isPublishedInSub } from './published';
import { runInContributor } from './contributor-context';

export default class CursorMethods extends CursorMethodsNR {
  constructor (sub, handler, _id, collection, contributorId) {
    super(sub);

    this.handler = handler;
    this._id = _id;
    this.collection = collection;

    // Qualified identity of the doc whose callback created this object, null at
    // the top level. A bare _id is NOT unique within a publication - two cursors
    // can both see the same _id (overlapping selectors on one collection, or ids
    // that collide across collections) - and the join refcounts by this key, so
    // a bare _id lets one cursor's removed() release contributions owned by
    // another, shrinking the {$in} while it is still referenced.
    this._contributorId = contributorId != null ? contributorId : null;

    // Feeds the per-cursor registry keys (see HandlerController.add). A nested
    // callback re-run gets a FRESH CursorMethods, so the counter restarts and
    // the same call order yields the same keys - which is what lets a
    // re-created cursor supersede its own previous observer.
    this._registrySeq = 0;
  }
  cursor (cursor, collection, callbacks, registryKey) {
    const sub = this.sub;
    // The contributor whose callback created THIS cursor (null at the top
    // level). It is the parent of every contributor this cursor produces,
    // captured here so it is correct even for asynchronous nested adds (where
    // the parent is no longer on the call stack). Lets a parent removal
    // cascade-release its nested contributors (see join.js release()).
    const parentContributorId = this._contributorId;

    if (typeof collection !== 'string') {
      callbacks = collection;
      collection = cursor._getCollectionName();
    }

    // registryKey is internal (a join passes its stable key so a restart
    // replaces its own observer); plain this.cursor() calls get a fresh slot.
    const cursorKey = registryKey || collection + '#c' + this._registrySeq++;
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

    // Contributor ids are paths, not doc ids. The registry key alone repeats
    // under every parent - each nested callback gets a fresh CursorMethods, so
    // the sequence restarts from 0 - so only the full path back to the root
    // identifies a contributor uniquely across the whole subscription.
    const contributorIdFor = _id => (parentContributorId ? parentContributorId + '>' : '') + cursorKey + '|' + _id;

    if (callbacks)
      callbacks = this._getCallbacks(callbacks);

    function applyCallback (id, doc, method) {
      const cb = callbacks && callbacks[method];

      if (cb) {
        const contributorId = contributorIdFor(id);
        let methods = new CursorMethods(sub, handler.addBasic(id), id, collection, contributorId),
        isChanged = method === 'changed';

        // Expose the current contributor id AND its parent so join.push() can
        // refcount by contributor, record parentage, and later release it (and
        // cascade to nested contributors) on removal (see cursor/join.js).
        // Bound to the Fiber rather than kept on a stack, so a callback that
        // yields keeps its own frame (see contributor-context.js).
        return runInContributor({ id: contributorId, parent: parentContributorId },
          () => cb.call(methods, id, doc, isChanged) || doc);
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
        // Release by the qualified contributor id, never the bare _id - a join
        // fed by another cursor may hold the same _id as its own contributor.
        const joins = sub._prJoins;
        if (joins) {
          const contributorId = contributorIdFor(id);
          for (let i = 0; i < joins.length; i++) {
            joins[i].release(contributorId);
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
import { Meteor } from 'meteor/meteor';
import CursorMethodsNR from './nonreactive';
import { isPublishedInSub } from './published';
import { runInContributor } from './contributor-context';
import { isThenable } from '../thenable';
import { enqueue } from './write-queue';
import { withObserveLock } from '../observe-lock';

export default class CursorMethods extends CursorMethodsNR {
  constructor (sub, handler) {
    super(sub);

    this.handler = handler;

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
  //
  // Taken before the first await, so that cursors registered in one body still
  // get their keys in call order even though they now build in parallel.
  _nextRegistryKey (name, kind) {
    const bucket = name + '#' + kind;
    const seq = this._registrySeq[bucket] || 0;
    this._registrySeq[bucket] = seq + 1;
    return bucket + seq;
  }
  // Split in two so that the promise can be registered with this frame before it
  // is handed to the caller (see CursorMethodsNR._track): the body then reaches
  // its first await having already told the frame it is running, which is what
  // lets a publication - or a callback opening a nested cursor - be written
  // without a single await and still not go out half-published.
  cursor (cursor, collection, callbacks, registryKey) {
    return this._track(this._openCursor(cursor, collection, callbacks, registryKey));
  }
  async _openCursor (cursor, collection, callbacks, registryKey) {
    const sub = this.sub;

    if (typeof collection !== 'string') {
      callbacks = collection;
      collection = cursor._getCollectionName();
    }

    // registryKey is internal (a join passes its stable key so a restart
    // replaces its own observer); plain this.cursor() calls get a fresh slot.
    const cursorKey = registryKey || this._nextRegistryKey(collection, 'c');
    const handler = await this.handler.add(cursor, {
      collection: collection,
      key: cursorKey
    });

    // add() hands back a pre-stopped controller when the subscription is already
    // torn down. set() would dispose of the observer below anyway, but bailing
    // out here skips building it - and the Mongo round-trip - in the first place.
    if (handler._stopped) return handler;

    if (callbacks)
      callbacks = this._getCallbacks(callbacks);

    // The one place a user callback is invoked from, so that every one of them
    // is framed identically: the same `this`, and the same contributor.
    //
    // The per-document controller IS that contributor's identity - stable across
    // added/changed, distinct for the same _id reached through another cursor,
    // and distinct for the same nested doc reached under another parent.
    // join.push() keys its contributions by it. Binding it to the async context
    // rather than to a stack is what lets a callback that suspends keep its own
    // owner while a sibling cursor's callback runs (see contributor-context.js).
    function runCallback (id, invoke) {
      const owner = handler.addBasic(id);
      const methods = new CursorMethods(sub, owner);

      return runInContributor(owner, () => invoke(methods));
    }

    // Hands back the fields to send, or a promise of them when the callback is
    // asynchronous - deliberately not an `await`, so that a synchronous callback
    // still reaches its write inside the multiplexer's own task (thenable.js).
    function applyCallback (id, doc, method) {
      const cb = callbacks && callbacks[method];
      if (!cb) return doc;

      const isChanged = method === 'changed';
      return runCallback(id, (methods) => {
        const fields = cb.call(methods, id, doc, isChanged);

        // What the callback opened belongs to this document, so it has to be out
        // before the document itself is written - that is the order a nested
        // cursor had when everything was synchronous, and what the client needs
        // to be able to read a parent and find its children already there.
        //
        // The `|| doc` is applied to the RESOLVED value: an async callback hands
        // back a promise, which is truthy, so testing it before it settles would
        // send the promise itself as the fields. Editing `doc` in place and
        // returning nothing therefore works at any point of an async callback,
        // exactly as it does in a synchronous one.
        if (isThenable(fields))
          return fields.then(edited => methods._settle().then(() => edited || doc));

        // A synchronous callback that opened nothing stays synchronous all the
        // way to the write below - the common leaf case, and the one that keeps
        // the multiplexer's own ordering doing the work for us.
        if (methods._pending.length)
          return methods._settle().then(() => fields || doc);

        return fields || doc;
      });
    }

    function writeChanged (id, fields) {
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
    }

    function writeRemoved (id) {
      // Same two-observer situation as in changed: when the doc is already
      // retracted (the other observer's removed fired first, or a join
      // retraction took it), a second sub.removed throws "Removed
      // nonexistent document". Only a provable "not published" suppresses
      // the call - on unknown (null) we forward, as skipping would strand
      // the doc on the client forever.
      if (isPublishedInSub(sub, collection, id) === false) return;

      sub.removed(collection, id);
    }

    // Every event for one document goes through that document's chain, so an
    // asynchronous callback cannot let a later event overtake it (write-queue.js).
    let observeChanges = await withObserveLock(cursor, () => cursor.observeChangesAsync({
      added (id, doc) {
        // Returned rather than awaited: ObserveMultiplexer._sendAdds collects
        // what the added callbacks hand back and settles all of them before
        // observeChangesAsync resolves. That is what keeps "this cursor has
        // delivered its initial adds" true by the time this method returns -
        // the statement HandlerController.set() and the join both rest on.
        return enqueue(sub, collection, id, () => {
          const fields = applyCallback(id, doc, 'added');

          if (isThenable(fields)) return fields.then(edited => sub.added(collection, id, edited));

          sub.added(collection, id, fields);
        });
      },
      changed (id, doc) {
        return enqueue(sub, collection, id, () => {
          // Run the callback unconditionally - it keeps join contributions and
          // nested cursors up to date regardless of what the client sees.
          const fields = applyCallback(id, doc, 'changed');

          if (isThenable(fields)) return fields.then(edited => writeChanged(id, edited));

          writeChanged(id, fields);
        });
      },
      removed (id) {
        return enqueue(sub, collection, id, () => removeDoc(id));
      }
    }));

    function removeDoc (id) {
      if (!callbacks) return writeRemoved(id);

      // Stopping the per-document controller is what releases whatever this
      // document contributed to any join, recursing into the controllers of
      // any cursor it nested - so the {$in} shrinks instead of growing
      // forever, with no parentage bookkeeping anywhere. It has to happen
      // whether the callback returns or throws, and for an async callback
      // only once it has finished: until then it may still be pushing.
      const release = () => handler.remove(id);

      let running;
      try {
        running = runCallback(id, (methods) => callbacks.removed.call(methods, id));
      } catch (error) {
        release();
        throw error;
      }

      if (!isThenable(running)) {
        release();
        return writeRemoved(id);
      }

      return running.then(
        () => {
          release();
          return writeRemoved(id);
        },
        (error) => {
          release();
          throw error;
        }
      );
    }

    return handler.set(observeChanges);
  }
};

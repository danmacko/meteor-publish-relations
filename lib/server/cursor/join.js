import { Meteor } from 'meteor/meteor';
import CursorMethods from './cursor';
import { isPublishedInSub } from './published';

CursorMethods.prototype.join = function (...params) {
  return new CursorJoin(this, ...params);
};

class CursorJoin {
  constructor (methods, collection, options, name) {
    this.methods = methods;
    this.collection = collection;
    this.options = options;
    this.name = name;

    this.data = [];
    this.sent = false;
    this._cursorScheduled = false;
    this._pendingRetractions = null;

    // Stable per-join registry key (see HandlerController.add): a restart must
    // replace THIS join's observer and nothing else. Keyed by collection name
    // alone, two joins on one collection stop each other on every restart.
    this._registryKey = (name || collection._name) + '#j' + methods._registrySeq++;

    // Refcounted {$in} so the join can SHRINK, not only grow.
    //   _idRefCounts:      joinedId -> how many contributors still reference it
    //   _idsByContributor: contributorId -> Set(joinedIds it pushed)
    // Without this, a removed parent (e.g. a join-table row) never decrements
    // the joined id, so the $in grows unbounded on long-lived/busy subscriptions
    // and eventually triggers the oplog "Mongo server and Meteor query disagree
    // on how many documents match" reconciliation error.
    this._idRefCounts = Object.create(null);
    this._idsByContributor = Object.create(null);

    // Parentage, so a parent removal cascades to its nested contributors. The
    // join refcounts by the INNERMOST contributor, so when an OUTER parent
    // merely leaves the result set its nested observer is only stopped and
    // never fires removed - release(parent) alone would leak those ids.
    this._contributorParent = Object.create(null);  // contributorId -> parentContributorId
    this._childrenByParent = Object.create(null);    // parentContributorId -> Set(childContributorIds)

    // Register on the subscription so a contributor removal (seen in cursor.js)
    // can reach every join and release the ids it was responsible for.
    const sub = methods.sub;
    (sub._prJoins = sub._prJoins || []).push(this);
  }
  _currentContributorId () {
    // Set by cursor.js around each parent-callback invocation. Innermost wins,
    // which is exactly the doc whose removal should release this push.
    const stack = this.methods.sub._prContributorStack;
    return stack && stack.length ? stack[stack.length - 1].id : null;
  }
  _currentParentId () {
    // The contributor one level up (the doc whose callback created the cursor
    // that is push()ing now), captured by cursor.js at cursor-creation time so
    // it is correct for asynchronous nested adds too, not only the initial ones.
    const stack = this.methods.sub._prContributorStack;
    return stack && stack.length ? stack[stack.length - 1].parent : null;
  }
  push (..._ids) {
    const contributorId = this._currentContributorId();
    const parentId = this._currentParentId();
    let changed = false;

    _ids.forEach(_id => {
      if (!_id) return;

      if (contributorId != null) {
        const contributedIds = this._idsByContributor[contributorId] || (this._idsByContributor[contributorId] = new Set());
        if (parentId != null && this._contributorParent[contributorId] === undefined) {
          // remember who owns this contributor, so releasing the parent releases it too
          this._contributorParent[contributorId] = parentId;
          (this._childrenByParent[parentId] || (this._childrenByParent[parentId] = new Set())).add(contributorId);
        }
        if (contributedIds.has(_id)) return; // this contributor already counts this id
        contributedIds.add(_id);
      } else if (this.data.includes(_id)) {
        return; // legacy path (push outside a cursor callback): old dedup, no refcount
      }

      const prevCount = this._idRefCounts[_id] || 0;
      this._idRefCounts[_id] = prevCount + 1;
      if (prevCount === 0) {
        this.data.push(_id);
        changed = true;
      }
    });

    if (this.sent && changed) {
      this._scheduleCursor();
    }
  }
  // A contributor doc was removed -> drop the ids only it was keeping alive,
  // and cascade to any nested contributors it owned.
  release (contributorId) {
    // Cascade first: a parent leaving the result set must also release the ids
    // its nested contributors kept alive - those nested observers are only
    // stopped, they never fire removed.
    const children = this._childrenByParent[contributorId];
    if (children) {
      delete this._childrenByParent[contributorId];
      children.forEach(childId => this.release(childId));
    }

    // Detach from our own parent so a later parent release won't recurse into us.
    const ownParentId = this._contributorParent[contributorId];
    if (ownParentId !== undefined) {
      delete this._contributorParent[contributorId];
      const siblings = this._childrenByParent[ownParentId];
      if (siblings) {
        siblings.delete(contributorId);
        if (!siblings.size) delete this._childrenByParent[ownParentId];
      }
    }

    const contributedIds = this._idsByContributor[contributorId];
    if (!contributedIds) return; // a pure parent with no ids of its own (children handled above)
    delete this._idsByContributor[contributorId];

    const droppedIds = [];
    contributedIds.forEach(_id => {
      const remaining = (this._idRefCounts[_id] || 1) - 1;
      if (remaining > 0) {
        this._idRefCounts[_id] = remaining;
        return;
      }
      // last contributor gone: this id leaves the $in
      delete this._idRefCounts[_id];
      droppedIds.push(_id);
    });

    if (droppedIds.length) {
      // Drop them all in a single O(n) pass instead of indexOf+splice per id
      // (O(k*n)). Reassigning this.data is safe: _selector() hands each observe
      // its own .slice(), so nothing holds a long-lived reference to it.
      const dropSet = new Set(droppedIds);
      this.data = this.data.filter(_id => !dropSet.has(_id));
    }

    if (!droppedIds.length || !this.sent) return;

    // Retracting here would be too early: the live observer still holds the OLD
    // {$in}, so a change to one of these docs before the swap would make it fire
    // against a doc already gone from the session view, and throw.
    // _scheduleCursor retracts and swaps in one yield-free block instead.
    this._scheduleCursor(droppedIds);
  }
  // A stopped+restarted observer does NOT retract docs that fell out of the
  // {$in}, so tell the client explicitly. Only call this from the deferred
  // restart, never inline - see release().
  _retract (droppedIds) {
    const sub = this.methods.sub;
    const name = this._name();

    // Releases coalesce into one restart, so the queue can hold an id twice, and
    // a push() in the same tick can put one back into the {$in}. A second
    // sub.removed for the same id throws, and a re-joined id must stay.
    const seen = new Set();
    const retractableIds = droppedIds.filter(_id => {
      if (seen.has(_id) || this._idRefCounts[_id] !== undefined) return false;
      seen.add(_id);
      return true;
    });
    if (!retractableIds.length) return;

    // Only ids provably published (=== true): a contributor may reference a
    // joined doc that never matched, and a removed for a doc never sent throws
    // on both ends ("Removed nonexistent document" on the server, "Expected to
    // find a document already present for removed" on the client). This also
    // means the retraction only applies to default-selector joins - with a
    // custom selector the pushed values are foreign keys, never found in
    // _documents, and the check correctly no-ops.
    retractableIds.forEach(_id => {
      if (isPublishedInSub(sub, name, _id) === true) sub.removed(name, _id);
    });
  }
  _name () {
    return this.name || this._collName || (this._collName = this.collection._name);
  }
  send () {
    this.sent = true;
    if (!this.data.length) return;

    return this._cursor();
  }
  _scheduleCursor (droppedIds) {
    // Ids that left the {$in} and must be retracted from the client when the
    // observer is swapped - queued here so several release() calls coalescing
    // into one restart all get retracted (see _retract).
    if (droppedIds && droppedIds.length) {
      (this._pendingRetractions || (this._pendingRetractions = [])).push(...droppedIds);
    }

    // Coalesce a burst of push()/release() calls (e.g. multiple parent docs
    // arriving from the same oplog batch) into a single observeChanges restart.
    if (this._cursorScheduled) return;
    this._cursorScheduled = true;

    Meteor.defer(() => {
      this._cursorScheduled = false;
      const pendingRetractions = this._pendingRetractions;
      this._pendingRetractions = null;

      // The subscription may have stopped while this was queued - handler.stop()
      // already ran, so a new observe here would leak until server restart.
      // Subscription exposes no public "stopped" flag; _isDeactivated() is the
      // accessor Meteor core itself uses after teardown.
      if (this.methods?.sub?._isDeactivated?.()) return;

      // Retract, then restart: _cursor() stops the old observer before starting
      // the new one and nothing in between yields, so the observer holding the
      // stale {$in} can never fire against a doc just retracted.
      if (pendingRetractions) this._retract(pendingRetractions);
      this._cursor();
    });
  }
  _selector () {
    // Each observe needs its OWN frozen copy of the $in. this.data is mutated in
    // place and the restart is deferred, so a live observe would otherwise have
    // its compiled minimongo matcher (frozen when built) drift from the Mongo
    // query (which re-reads the live array on every poll) - which is what makes
    // the oplog driver log "The Mongo server and the Meteor query disagree on
    // how many documents match your query".
    let _id = {$in: this.data.slice()};
    return typeof this.selector === 'function' ? this.selector(_id): {_id: _id};
  }
  _cursor () {
    const cursor = this.collection.find(this._selector(), this.options);
    // Cache the exact DDP collection name docs are published under (cursor.js
    // resolves it the same way), so release() removes from the right place.
    if (!this._collName) {
      this._collName = this.name || cursor._getCollectionName();
    }

    return this.methods.cursor(cursor, this.name, undefined, this._registryKey);
  }
};

import { Meteor } from 'meteor/meteor';
import { _ } from 'meteor/underscore';
import CursorMethods from './cursor';

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

    // Refcounted {$in} so the join can SHRINK, not only grow.
    //   _idRefCounts:      joinedId -> how many contributors still reference it
    //   _idsByContributor: contributorId -> Set(joinedIds it pushed)
    // Without this, a removed parent (e.g. a projectPeople row) never decrements
    // the joined id, so the $in grows unbounded on long-lived/busy subscriptions
    // and eventually triggers the oplog "Mongo server and Meteor query disagree
    // on how many documents match" reconciliation error.
    this._idRefCounts = Object.create(null);
    this._idsByContributor = Object.create(null);

    // Register on the subscription so a contributor removal (seen in cursor.js)
    // can reach every join and release the ids it was responsible for.
    const sub = methods.sub;
    (sub._prJoins = sub._prJoins || []).push(this);
  }
  _currentContributorId () {
    // Set by cursor.js around each parent-callback invocation. Innermost wins,
    // which is exactly the doc whose removal should release this push.
    const stack = this.methods.sub._prContributorStack;
    return stack && stack.length ? stack[stack.length - 1] : null;
  }
  push (..._ids) {
    const contributorId = this._currentContributorId();
    let changed = false;

    _.each(_ids, _id => {
      if (!_id) return;

      if (contributorId != null) {
        const contributedIds = this._idsByContributor[contributorId] || (this._idsByContributor[contributorId] = new Set());
        if (contributedIds.has(_id)) return; // this contributor already counts this id
        contributedIds.add(_id);
      } else if (_.contains(this.data, _id)) {
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
  // A contributor doc was removed -> drop the ids only it was keeping alive.
  release (contributorId) {
    const contributedIds = this._idsByContributor[contributorId];
    if (!contributedIds) return;
    delete this._idsByContributor[contributorId];

    const droppedIds = [];
    contributedIds.forEach(_id => {
      const remaining = (this._idRefCounts[_id] || 1) - 1;
      if (remaining > 0) {
        this._idRefCounts[_id] = remaining;
        return;
      }
      delete this._idRefCounts[_id];
      const idx = this.data.indexOf(_id);
      if (idx !== -1) {
        this.data.splice(idx, 1);
        droppedIds.push(_id);
      }
    });

    if (!droppedIds.length || !this.sent) return;

    // A stopped+restarted observer does NOT retract docs that fell out of the
    // {$in}, so explicitly tell the client those joined docs are gone. Only
    // remove ids that were actually published (a contributor may reference a
    // joined doc that never matched, e.g. an orphaned/deleted project) -
    // calling sub.removed for an unpublished doc throws inside Meteor.
    const sub = this.methods.sub;
    const name = this._name();
    const publishedDocs = sub._documents && sub._documents[name];
    const removedFromClient = publishedDocs ? droppedIds.filter(_id => publishedDocs.has(_id)) : [];

    removedFromClient.forEach(_id => sub.removed(name, _id));

    this._scheduleCursor();
  }
  _name () {
    return this.name || this._collName || (this._collName = this.collection._name);
  }
  send () {
    this.sent = true;
    if (!this.data.length) return;

    return this._cursor();
  }
  _scheduleCursor () {
    // Coalesce a burst of push()/release() calls (e.g. multiple parent docs
    // arriving from the same oplog batch) into a single observeChanges restart.
    if (this._cursorScheduled) return;
    this._cursorScheduled = true;

    Meteor.defer(() => {
      this._cursorScheduled = false;
      if (this.methods?.sub?._stopped) return;
      this._cursor();
    });
  }
  _selector () {
    let _id = {$in: this.data};
    return _.isFunction(this.selector) ? this.selector(_id): {_id: _id};
  }
  _cursor () {
    const cursor = this.collection.find(this._selector(), this.options);
    // Cache the exact DDP collection name docs are published under (cursor.js
    // resolves it the same way), so release() removes from the right place.
    if (!this._collName) {
      this._collName = this.name || cursor._getCollectionName();
    }

    return this.methods.cursor(cursor, this.name);
  }
};

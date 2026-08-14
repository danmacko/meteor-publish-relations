import { Meteor } from 'meteor/meteor';
import CursorMethods from './cursor';
import { isPublishedInSub } from './published';
import { currentContributor, runInContributor } from './contributor-context';

// Backoff for a failed observe restart. _cursor() stops the old observer before
// building the new one, so a throw leaves the join with none at all: a transient
// Mongo error must not freeze it for the lifetime of the subscription, but a
// persistent one must not spin either.
const RESTART_RETRY_DELAYS = [50, 250, 1000, 5000];

CursorMethods.prototype.join = function (...params) {
  return new CursorJoin(this, ...params);
};

class CursorJoin {
  constructor (methods, collection, options, name) {
    this.methods = methods;
    this.collection = collection;
    this.options = options;
    this.name = name;

    this.sent = false;
    this._cursorScheduled = false;

    // Stable per-join registry key (see HandlerController.add): a restart must
    // replace THIS join's observer and nothing else. Keyed by collection name
    // alone, two joins on one collection stop each other on every restart.
    this._registryKey = methods._nextRegistryKey(name || collection._name, 'j');

    // The ONLY bookkeeping: what each contributor currently declares.
    //
    //   contributions: HandlerController -> Set(joinedIds)
    //
    // Membership is union(contributions) - see _membership() - so there is no
    // refcount that can drift out of step with it, and no parentage to maintain
    // either: the key is the per-document controller cursor.js already builds,
    // and its stop() recurses into children, so a parent document leaving the
    // result set releases its nested contributions on the way down.
    this.contributions = new Map();

    // Ids pushed outside any callback (push() straight from the publish body).
    // Nothing owns them, so nothing can ever release them - they are permanent
    // members for the life of the subscription.
    this._staticIds = new Set();

    // The membership the live observer was built with, and what the next
    // reconcile diffs against to find what must be retracted.
    this.data = [];
  }
  push (..._ids) {
    const owner = currentContributor();
    const target = owner ? this._contributionsOf(owner) : this._staticIds;
    let changed = false;

    _ids.forEach(_id => {
      if (!_id || target.has(_id)) return;
      target.add(_id);
      changed = true;
    });

    if (changed) this._invalidate();
  }
  _contributionsOf (owner) {
    // A callback can outlive its own controller: it yields while a nested
    // observeChanges is built, its document leaves the result set in the
    // meantime, and it resumes after stop() has already released it. Storing a
    // contribution now would attach it to a controller whose stop() has run for
    // the last time, pinning that id for the life of the subscription - the
    // unbounded growth this whole model exists to prevent. The document is gone,
    // so the push has nothing to keep alive: take it and drop it.
    if (owner._stopped) return new Set();

    let ids = this.contributions.get(owner);
    if (!ids) {
      ids = new Set();
      this.contributions.set(owner, ids);
      // Release is driven by the controller tree rather than by parentage we
      // track ourselves (see HandlerController.trackFor).
      owner.trackFor(this);
    }
    return ids;
  }
  // The contributor's cursor rebuilt itself, so `from` is leaving the controller
  // tree. Membership is unchanged - the ids simply change owner, to the one that
  // can still be released (see HandlerController.transferContributionsTo).
  transferContribution (from, to) {
    const ids = this.contributions.get(from);
    if (!ids) return;
    this.contributions.delete(from);
    if (!ids.size) return;

    const target = this._contributionsOf(to);
    ids.forEach(_id => target.add(_id));
  }
  // The contributor is gone: its HandlerController was stopped, either because
  // its document left the result set or because an ancestor was torn down.
  dropContribution (owner) {
    if (this.contributions.delete(owner)) this._invalidate();
  }
  // Every joined id at least one contributor still declares.
  _membership () {
    const ids = new Set(this._staticIds);
    this.contributions.forEach(contributed => contributed.forEach(_id => ids.add(_id)));
    return Array.from(ids);
  }
  _invalidate () {
    // Before send() there is no observer yet and nothing is published, so the
    // first materialisation is send()'s job.
    if (this.sent) this._scheduleCursor();
  }
  // A stopped+restarted observer does NOT retract docs that fell out of the
  // {$in}, so tell the client explicitly. Only call this from the reconcile,
  // never inline: until the observer is swapped it still holds the OLD {$in},
  // and a change arriving for a doc already retracted would throw.
  _retract (droppedIds) {
    const sub = this.methods.sub;
    const name = this._name();

    // Only ids provably published (=== true): a contributor may reference a
    // joined doc that never matched, and a removed for a doc never sent throws
    // on both ends ("Removed nonexistent document" on the server, "Expected to
    // find a document already present for removed" on the client). This also
    // means the retraction only applies to default-selector joins - with a
    // custom selector the pushed values are foreign keys, never found in
    // _documents, and the check correctly no-ops.
    droppedIds.forEach(_id => {
      if (isPublishedInSub(sub, name, _id) === true) sub.removed(name, _id);
    });
  }
  _name () {
    return this.name || this._collName || (this._collName = this.collection._name);
  }
  send () {
    this.sent = true;
    this.data = this._membership();
    if (!this.data.length) return;

    return this._cursor();
  }
  _scheduleCursor () {
    // Coalesce a burst of push()/reset/drop calls (e.g. several parent docs
    // arriving from the same oplog batch) into a single observeChanges restart.
    if (this._cursorScheduled) return;
    this._cursorScheduled = true;

    this._deferRestart(Meteor.defer);
  }
  // Meteor.defer/setTimeout capture the environment of whoever scheduled them,
  // so clear the contributor: the restart is the join's own work, not a push by
  // the contributor that happened to trigger it (contributor-context.js).
  _deferRestart (schedule, delay) {
    schedule(() => runInContributor(null, () => this._reconcile()), delay);
  }
  // The single place that turns declared contributions into published state.
  _reconcile () {
    this._cursorScheduled = false;

    // The subscription may have stopped while this was queued - handler.stop()
    // already ran, so a new observe here would leak until server restart.
    // Subscription exposes no public "stopped" flag; _isDeactivated() is the
    // accessor Meteor core itself uses after teardown.
    if (this.methods?.sub?._isDeactivated?.()) return;

    const next = this._membership();
    const nextIds = new Set(next);
    const dropped = this.data.filter(_id => !nextIds.has(_id));

    // Membership is per-contributor, so a new contributor declaring an id the
    // join already holds changes contributions without changing the union. An
    // empty `dropped` means data is a subset of next, so equal lengths make the
    // two sets equal - and restarting on that would stop a healthy observer and
    // re-run its initial query for an identical {$in}. Common on live lists,
    // where several parent rows point at the same joined document.
    //
    // Only while the observer actually matches this.data, though: a pending
    // retry is here precisely because it does NOT, and this.data advanced before
    // the throw, so the shortcut would look like "nothing to do" and leave the
    // join without an observer for good.
    if (!this._restartAttempt && !dropped.length && next.length === this.data.length) {
      this.data = next;
      return;
    }

    try {
      // Retract, then swap: _cursor() stops the old observer before starting the
      // new one and nothing in between yields, so the observer holding the stale
      // {$in} can never fire against a doc just retracted. this.data advances
      // only once the retraction is out, so a throw here leaves the diff intact
      // for the retry (and _retract skips what it can see is already gone).
      if (dropped.length) this._retract(dropped);
      this.data = next;
      this._cursor();
      this._restartAttempt = 0;
    } catch (error) {
      // By the time _cursor() can throw it has already stopped the old observer,
      // so the join is left with NO observer at all. Meteor.defer would just
      // _debug-log this and the join would stay frozen until the client
      // resubscribes.
      this._retryRestart(error);
    }
  }
  _retryRestart (error) {
    const attempt = this._restartAttempt = (this._restartAttempt || 0) + 1;
    const delay = RESTART_RETRY_DELAYS[attempt - 1];

    if (delay === undefined) {
      this._restartAttempt = 0;
      Meteor._debug('publish-relations: join on ' + this._name() + ' gave up restarting its observe after ' +
        RESTART_RETRY_DELAYS.length + ' attempts; it has no live observer and will not update until the ' +
        'client resubscribes', error);
      return;
    }

    // Stay "scheduled" across the wait so a contribution change landing
    // meanwhile folds into this retry instead of starting a second restart.
    this._cursorScheduled = true;
    this._deferRestart(Meteor.setTimeout, delay);
  }
  _selector () {
    // Each observe needs its OWN frozen copy of the $in. this.data is replaced
    // on every reconcile and the restart is deferred, so a live observe would
    // otherwise have its compiled minimongo matcher (frozen when built) drift
    // from the Mongo query (which re-reads the live array on every poll) - which
    // is what makes the oplog driver log "The Mongo server and the Meteor query
    // disagree on how many documents match your query".
    //
    // Sorted, because Mongo caches one ObserveMultiplexer per
    // EJSON.stringify({ordered, ...cursorDescription}) - the ORDER of the $in
    // array is part of that key. Membership order follows the union, which
    // follows insertion into the contribution sets, so two subscriptions holding
    // the same members would otherwise drift apart as soon as either sees any
    // churn and could then never share an observer again. Sorting makes the key
    // a function of membership alone.
    let _id = {$in: this.data.slice().sort()};
    return typeof this.selector === 'function' ? this.selector(_id): {_id: _id};
  }
  _cursor () {
    const cursor = this.collection.find(this._selector(), this.options);
    // Cache the exact DDP collection name docs are published under (cursor.js
    // resolves it the same way), so _retract removes from the right place.
    if (!this._collName) {
      this._collName = this.name || cursor._getCollectionName();
    }

    return this.methods.cursor(cursor, this.name, undefined, this._registryKey);
  }
};

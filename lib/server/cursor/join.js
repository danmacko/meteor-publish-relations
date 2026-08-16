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
    // Whether a live observer currently matches this.data. Not derivable from
    // the retry counter: _retryRestart zeroes that when it gives up.
    this._observerLive = false;
    // Bumped by every _reconcile so a run that got superseded while it was
    // yielding can tell, and keep its hands off the shared state.
    this._reconcileGeneration = 0;

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
    let target = null;
    let changed = false;

    _ids.forEach(_id => {
      // == null, not falsy: 0 is a legitimate _id.
      if (_id == null) return;

      // Resolved on the first real id, never before. A callback pushing nothing
      // but undefined foreign keys - the everyday case on a changed callback,
      // which is handed the update rather than the document - then leaves no
      // contribution entry behind, and so nothing whose later removal would
      // schedule a reconcile with nothing in it.
      if (!target) {
        const owner = currentContributor();
        target = owner ? this._contributionsOf(owner) : this._staticIds;
      }

      if (target.has(_id)) return;
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
  // The first materialisation is the same operation as every later one: work out
  // the membership, apply the difference, put an observer on it. Kept as one
  // implementation deliberately - the two used to be written out separately and
  // had already drifted apart over which of them resets the retry counter.
  send () {
    this.sent = true;

    return this._reconcile();
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
    // _cursor() yields, and the slot is released here so that a push arriving
    // during that yield still schedules a reconcile rather than being lost. The
    // cost is that the later run can finish first, in its own fiber, leaving
    // this one holding a handle that set()'s latch has already thrown away.
    // Everything it writes afterwards would be about work nobody is using, so
    // it claims a generation on the way in and checks it on the way out.
    const generation = ++this._reconcileGeneration;
    this._cursorScheduled = false;

    // The subscription may have stopped while this was queued - handler.stop()
    // already ran, so a new observe here would leak until server restart.
    // Subscription exposes no public "stopped" flag; _isDeactivated() is the
    // accessor Meteor core itself uses after teardown.
    if (this.methods?.sub?._isDeactivated?.()) return;

    const next = this._membership();
    const nextIds = new Set(next);
    const dropped = this.data.filter(_id => !nextIds.has(_id));

    // Nothing live, nothing wanted, nothing to retract - which is what send()
    // looks like on a join that collected no ids. An observer over an empty
    // {$in} would be watching for nothing. (Once one IS up, an emptied
    // membership still goes through _cursor() below: that is what stops it.)
    if (!this._observerLive && !next.length && !dropped.length) {
      this.data = next;
      // Not a failed state any more, just an empty one - so a retry budget spent
      // getting here must not count against the next real membership.
      this._restartAttempt = 0;
      return;
    }

    // Membership is per-contributor, so a new contributor declaring an id the
    // join already holds changes contributions without changing the union. An
    // empty `dropped` means data is a subset of next, so equal lengths make the
    // two sets equal - and restarting on that would stop a healthy observer and
    // re-run its initial query for an identical {$in}. Common on live lists,
    // where several parent rows point at the same joined document.
    //
    // Only while an observer is actually up and matches this.data. Anything else
    // - a restart still being retried, or one that gave up - is here precisely
    // because it does not, and this.data may already have advanced past the
    // failure, so the shortcut would read as "nothing to do" and leave the join
    // without an observer for good.
    if (this._observerLive && !dropped.length && next.length === this.data.length) {
      this.data = next;
      return;
    }

    this._observerLive = false;

    try {
      // Retract, then swap: _cursor() stops the old observer before starting the
      // new one and nothing in between yields, so the observer holding the stale
      // {$in} can never fire against a doc just retracted.
      //
      // A throw from _retract leaves this.data untouched, so the retry recomputes
      // the same diff and _retract skips whatever already went out. A throw from
      // _cursor() lands after this.data has advanced, so the retry sees an empty
      // diff - correct, the retraction is already on the wire - and only needs to
      // rebuild the observer, which _observerLive keeps it from skipping.
      if (dropped.length) this._retract(dropped);
      this.data = next;
      const handle = this._cursor();

      // Superseded while _cursor() was yielding: a later reconcile already owns
      // this.data and the live observer. Claiming one here would tell a pending
      // retry that everything is fine and leave the join without an observer for
      // good - the very state _observerLive exists to prevent.
      if (generation !== this._reconcileGeneration) return handle;

      this._observerLive = true;
      this._restartAttempt = 0;

      // send() hands this back: every method of this package returns something
      // with a stop() on it.
      return handle;
    } catch (error) {
      // Same again: a superseded run's failure is a failure of work that has
      // been discarded, and retrying it would race whoever replaced it.
      if (generation !== this._reconcileGeneration) return;

      // By the time _cursor() can throw it has already stopped the old observer,
      // so the join is left with NO observer at all. Letting it out would take
      // the whole subscription down with a nosub when it happens on the first
      // materialisation, and Meteor.defer would merely _debug-log it when it
      // happens on a restart - so neither, and retry instead.
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

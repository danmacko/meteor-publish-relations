import CursorMethodsNR from './cursor';

CursorMethodsNR.prototype.joinNonreactive = function (...params) {
  return new CursorJoinNonreactive(this, ...params);
};

class CursorJoinNonreactive {
  // Takes the frame rather than the subscription, so that what it publishes is
  // registered with whoever created it (CursorMethodsNR._track) - the same
  // reason a nested cursor is: the publication must not report ready while a
  // join is still reading.
  constructor (methods, collection, options, name) {
    this.methods = methods;
    this.sub = methods.sub;
    this.collection = collection;
    this.options = options;
    this.name = name || collection._name;

    // A Set, because that is what it is: membership, no order to preserve and
    // nothing to look up by position. Dedup comes out O(1), and Array.from()
    // below can only ever hand out a fresh array - so the {$in} cannot alias
    // this collection and drift as push() grows it, which is the mistake the
    // reactive join makes visible as "The Mongo server and the Meteor query
    // disagree on how many documents match your query".
    this.data = new Set();
    this.sent = false;
  }
  // Always the same shape, whether this is publishing everything or a single
  // late arrival, so a custom selector written for this.join works here
  // unchanged instead of being handed a bare id half the time.
  _selector (ids = this.data) {
    const _id = {$in: Array.from(ids)};
    return typeof this.selector === 'function' ? this.selector(_id): {_id: _id};
  }
  push (..._ids) {
    let newIds = [];

    _ids.forEach(_id => {
      // == null, not falsy: 0 is a legitimate _id.
      if (_id == null || this.data.has(_id))
        return;

      this.data.add(_id);
      newIds.push(_id);
    });

    // Nothing is observed here, so a push after send() reaches the client only
    // by being published on the spot.
    if (this.sent && newIds.length)
      return this.methods._track(this.added(newIds));
  }
  send () {
    this.sent = true;
    if (!this.data.size) return;

    return this.methods._track(this.added());
  }
  async added (ids) {
    await this.collection.find(this._selector(ids), this.options).forEachAsync(doc => {
      const { _id: id, ...fields } = doc;
      this.sub.added(this.name, id, fields);
    });
  }
};
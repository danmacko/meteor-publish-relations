import CursorMethodsNR from './cursor';

CursorMethodsNR.prototype.joinNonreactive = function (...params) {
  return new CursorJoinNonreactive(this.sub, ...params);
};

class CursorJoinNonreactive {
  constructor (sub, collection, options, name) {
    this.sub = sub;
    this.collection = collection;
    this.options = options;
    this.name = name || collection._name;

    this.data = [];
    this.sent = false;
  }
  _selector (_id = {$in: this.data}) {
    return typeof this.selector === 'function' ? this.selector(_id): {_id: _id};
  }
  push (..._ids) {
    let newIds = [];

    _ids.forEach(_id => {
      if (!_id || this.data.includes(_id))
        return;

      this.data.push(_id);
      newIds.push(_id);
    });

    if (this.sent && newIds.length)
      return this.added(newIds.length > 1 ? {$in: newIds}: newIds[0]);
  }
  send () {
    this.sent = true;
    if (!this.data.length) return;

    return this.added();
  }
  added (_id) {
    this.collection.find(this._selector(_id), this.options).forEach(doc => {
      const { _id: id, ...fields } = doc;
      this.sub.added(this.name, id, fields);
    });
  }
};
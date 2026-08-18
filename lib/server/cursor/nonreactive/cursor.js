export default class CursorMethodsNR {
  constructor (sub) {
    this.sub = sub;
  }

  // Nested callbacks are invoked with a CursorMethods(NR) as `this`, not with
  // the subscription - so `this.relations` has to resolve here too, otherwise
  // the API would be this.relations.cursor() at the top level and this.cursor()
  // one line deeper. Inherited by CursorMethods, so both nested call sites
  // (cursor.js and cursorNonreactive) are covered by this one getter.
  // The subscription API stays reachable inside a callback as `this.sub`.
  get relations () {
    return this;
  }

  cursorNonreactive (cursor, collection, onAdded) {
    const sub = this.sub;

    if (typeof collection !== 'string') {
      onAdded = collection;
      collection = cursor._getCollectionName();
    }
    if (typeof onAdded !== 'function')
      onAdded = function () {};

    cursor.forEach((doc) => {
      const id = doc._id;
      // The callback is handed the whole document, _id included - it comes from
      // a plain find, not from observeChanges, and there is no reason to hide it.
      // What goes out is another matter: DDP carries the id separately and the
      // merge box drops an _id found in fields (SessionDocumentView.changeField,
      // "Publish API ignores _id if present in fields"), so sending it is at best
      // wasted bytes and at worst a difference from the reactive path that has to
      // be remembered. Stripped after the callback, so a callback that edited or
      // replaced the document still decides what the fields are.
      const { _id, ...fields } = onAdded.call(new CursorMethodsNR(sub), id, doc) || doc;
      sub.added(collection, id, fields);
    });
  }
};

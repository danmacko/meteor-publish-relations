import { runSerial } from '../serial';

// One document's writes, in the order the events for it arrived.
//
// The observe multiplexer serialises its own dispatch, but only the dispatch:
// _applyCallback invokes the callback and moves on to the next event without
// waiting for what it hands back (it only .catch()es it, to log). That is fine
// for Meteor's own publish path, whose callbacks are synchronous and therefore
// finished by the time the next event is dispatched. It is not fine here: on
// Meteor 3 a callback that reads another collection HAS to be asynchronous
// (findOne and friends throw on the server now), so two events for the same
// document can be in flight at once and the writes can come out reversed -
// sub.changed before sub.added, which ddp-server answers with "Could not find
// element with id ... to change".
//
// Per document rather than per publication on purpose: the merge box tracks
// documents, ObserveMultiplexer._sendAdds fires the initial adds in parallel,
// and one queue for the whole publication would turn a thousand-document
// subscription into a thousand sequential callbacks.
//
// It is also what serialises the CALLBACKS, not only the writes - which is what
// keeps a parent that re-runs from registering two replacements for the same
// nested cursor at once.
export function enqueue (sub, collection, id, write) {
  const chains = sub._prWrites || (sub._prWrites = new Map());

  return runSerial(chains, 'doc ' + collection + ' ' + id, write);
}

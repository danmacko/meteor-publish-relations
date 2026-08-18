import { EJSON } from 'meteor/ejson';
import { runSerial } from './serial';

// One registration at a time per distinct cursor description.
//
// MongoConnection._observeChanges keeps an ObserveMultiplexer per cursor
// description, so that identical queries from different subscriptions share a
// single Mongo observe. The block that looks that cache up and fills it says of
// itself that it "is guaranteed to not yield" - which was true under Fibers, and
// is not on Meteor 3: it awaits the driver's _init() on the way. Two identical
// registrations that overlap therefore both miss the cache, both build a driver,
// and the second overwrites the first's entry. What is left is one multiplexer
// with nothing pointing at it, and an onStop on the other one that deletes an
// entry it no longer owns - so the next handle to stop takes the map entry away
// from a live observer.
//
// Queueing identical registrations behind each other restores what Meteor 2 got
// from the fiber: the second one finds the cache filled and shares the handle.
// Where it does nothing - two cursors with different descriptions never meet
// here - it costs a map lookup, and where it does, the second registration was
// going to be a cache hit anyway.
const registrations = new Map();

export function withObserveLock (cursor, register) {
  const description = cursor && cursor._cursorDescription;
  if (!description) return register();

  let key;
  try {
    key = EJSON.stringify(description);
  } catch (error) {
    // Nothing to key on is no reason to fail the registration - it just means
    // this one runs the way it would have without the lock.
    return register();
  }

  return runSerial(registrations, 'query ' + key, register);
}

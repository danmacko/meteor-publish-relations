import { Meteor } from 'meteor/meteor';
import { isThenable } from './thenable';

// Run jobs one after another per key, and forget the key once nothing is using
// it. Two things in the package need exactly this and nothing more:
//
//   cursor/write-queue.js  one chain per published document, so that two events
//                          for it cannot have their writes come out reversed
//   observe-lock.js        one chain per cursor description, so that two
//                          identical registrations cannot both miss Mongo's
//                          multiplexer cache
//
// Held keys travel with the async context rather than on a stack, because a job
// suspends: an await inside one must not make a sibling look like it holds the
// key, and must not lose the key it holds itself.
const held = new Meteor.EnvironmentVariable();

export function runSerial (chains, key, job) {
  // Already inside a job for this key, so the caller IS the chain: queueing
  // behind it would be waiting for ourselves. Reachable both ways - a callback
  // that republishes its own document, and a callback that opens a cursor with
  // the description it is running under - and inline is right in both.
  const holding = held.getOrNullIfOutsideFiber();
  if (holding && holding.has(key)) return job();

  const owned = new Set(holding);
  owned.add(key);

  const run = () => held.withValue(owned, job);
  const previous = chains.get(key);

  // Both arms, so one failed job does not block the ones queued behind it.
  const running = previous ? previous.then(run, run) : run();

  // Nothing suspended: the job is done and there is no chain worth keeping.
  // This is the whole synchronous world - every callback that does not touch
  // the database - and it leaves no trace behind at all.
  if (!isThenable(running)) return running;

  const settled = running.then(() => {}, () => {});
  chains.set(key, settled);
  settled.then(() => {
    // Only if nobody queued behind us in the meantime, or the map would keep an
    // entry per key for the life of the process.
    if (chains.get(key) === settled) chains.delete(key);
  });

  return running;
}

// A callback that is not asynchronous has to stay that way all the way to the
// sub.added/changed/removed it feeds, because that write then happens inside the
// observe multiplexer's own task - the one place Meteor orders two events
// against each other. Writing those paths as `async` instead would finish every
// synchronous callback a microtask late and hand the ordering problem to the
// package even where the user never asked for anything asynchronous.
//
// Hence a check rather than an await: the value is passed through untouched when
// there is nothing to wait for.
export function isThenable (value) {
  return !!value && typeof value.then === 'function';
}

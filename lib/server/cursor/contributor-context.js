import { Meteor } from 'meteor/meteor';

// Which document's callback is running right now, as the per-document
// HandlerController that owns it. The join keys its contributions by that
// object (cursor/join.js), so push() has to be able to find it without being
// told - push() takes no owner argument and the public API is not changing.
//
// A plain stack on the subscription is not enough. Callbacks are serialised
// within ONE observe multiplexer's queue, but a publication has one queue per
// cursor, and a nested this.cursor() yields while observeChanges() builds its
// observer. So a callback can sit suspended mid-frame while a sibling cursor's
// callback pushes and pops its own frame - and whoever resumes then reads a
// foreign owner, or none at all.
//
// Meteor.EnvironmentVariable binds the owner to the Fiber instead, which is
// exactly the unit of execution a yielding callback keeps to itself, and
// withValue() nests correctly for nested cursors.
const contributor = new Meteor.EnvironmentVariable();

// withValue() asserts it is inside a Fiber. Every path that reaches a callback
// here is: the publish function, the observe multiplexer's task queue, and
// bindEnvironment'd timers all run in one. A local collection is the exception -
// it fires callbacks straight from insert/update/remove - but publishing one
// through this package is not supported.
export function runInContributor (owner, fn) {
  return contributor.withValue(owner, fn);
}

// getOrNullIfOutsideFiber, not get: push() is public API and a caller may reach
// it from outside a Fiber, where get() would throw instead of reporting "no
// contributor" (which is a state push() already handles).
export function currentContributor () {
  return contributor.getOrNullIfOutsideFiber() || null;
}

import { Meteor } from 'meteor/meteor';

// Which document's callback is running right now. The join refcounts its {$in}
// by contributor, so every push() has to know whose push it is (cursor/join.js).
//
// A plain stack on the subscription is not enough. Callbacks are serialised
// within ONE observe multiplexer's queue, but a publication has one queue per
// cursor, and a nested this.cursor() yields while observeChanges() builds its
// observer. So a callback can sit suspended mid-frame while a sibling cursor's
// callback pushes and pops its own frame - and whoever resumes then reads a
// foreign frame, or an empty stack. Either way push() refcounts under the wrong
// contributor (released when a foreign parent leaves the result set) or under
// no contributor at all, which takes the legacy no-refcount path and pins that
// id in the {$in} forever - the unbounded growth that ends in "The Mongo server
// and the Meteor query disagree on how many documents match your query".
//
// Meteor.EnvironmentVariable binds the frame to the Fiber instead, which is
// exactly the unit of execution a yielding callback keeps to itself, and
// withValue() nests correctly for nested cursors.
const contributor = new Meteor.EnvironmentVariable();

// frame: { id, parent } - the contributor whose callback is running, and the
// contributor one level up (captured by cursor.js at cursor-creation time).
export function runInContributor (frame, fn) {
  return contributor.withValue(frame, fn);
}

// getOrNullIfOutsideFiber, not get: push() is public API and a caller may reach
// it from outside a Fiber, where get() would throw instead of reporting "no
// contributor" (which is a state push() already handles).
export function currentContributor () {
  return contributor.getOrNullIfOutsideFiber() || null;
}

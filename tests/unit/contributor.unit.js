// The contributor frame under concurrency.
//
// A publication has one observe multiplexer queue per cursor, and a nested
// this.cursor() yields while observeChanges() builds its observer - so two of a
// publication's callbacks can be suspended at the same time. That is the case a
// shared stack on the subscription cannot survive: whoever resumes first reads
// whatever frame was pushed last, and the pops then unwind in the wrong order.
//
// The harness models Meteor.EnvironmentVariable with AsyncLocalStorage, and an
// await here plays the part a Fiber yield plays on the server: the callback is
// suspended INSIDE its frame rather than having returned from it.

const { loadModules, makeMeteorStub, createReporter } = require('./harness');

function deferred() {
  let resolve;
  const promise = new Promise(r => (resolve = r));
  return { promise, resolve };
}

module.exports = async function run() {
  const { check, report } = createReporter('unit: contributor context');

  const Meteor = makeMeteorStub();
  const { runInContributor, currentContributor } = loadModules(['cursor/contributor-context.js'], { Meteor });

  // --- two callbacks suspended at once keep their own frames ---------------
  {
    const gateA = deferred();
    const gateB = deferred();
    const seen = {};

    const a = runInContributor({ id: 'A', parent: null }, async () => {
      await gateA.promise;
      seen.a = currentContributor().id;
    });
    // B's callback starts while A is still suspended inside its frame.
    const b = runInContributor({ id: 'B', parent: null }, async () => {
      await gateB.promise;
      seen.b = currentContributor().id;
    });

    gateA.resolve();
    gateB.resolve();
    await Promise.all([a, b]);

    check('each suspended callback resumes as itself', [seen.a, seen.b], ['A', 'B']);
  }

  // --- a nested cursor's frame does not escape to a suspended sibling ------
  {
    const gate = deferred();
    const seen = {};

    const outer = runInContributor({ id: 'outer', parent: null }, async () => {
      await gate.promise;
      seen.outer = currentContributor().id;
      seen.outerParent = currentContributor().parent;
    });
    // A nested callback runs to completion while `outer` is suspended.
    runInContributor({ id: 'nested', parent: 'other' }, () => {
      seen.nested = currentContributor().id;
    });

    gate.resolve();
    await outer;

    check('nested frame seen correctly', seen.nested, 'nested');
    check('the suspended outer frame is untouched', [seen.outer, seen.outerParent], ['outer', null]);
  }

  // --- outside any frame there is no contributor, and no throw -------------
  check('no frame -> null, not a throw', currentContributor(), null);

  return report();
};

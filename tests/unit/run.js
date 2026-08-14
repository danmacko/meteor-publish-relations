#!/usr/bin/env node
//
// Runs the fast unit layer. No Meteor, no MongoDB - just node:
//
//   node tests/unit/run.js      (or: npm test)
//
// The Tinytest suite in tests/ covers everything that needs a real server:
//   meteor test-packages --release METEOR@2.15 ./

const suites = [require('./join.unit'), require('./publication.unit'), require('./contributor.unit')];

// A suite may be async (the contributor one suspends callbacks on purpose), so
// this is a promise chain - and it needs its own .catch: an unhandled rejection
// exits 0 on node 14, which would report a crashed run as a passing one.
(async () => {
  let failures = 0;
  let checks = 0;

  for (const suite of suites) {
    const result = await suite();
    failures += result.failures;
    checks += result.checks;
  }

  console.log(
    '\n' +
      checks +
      ' checks, ' +
      failures +
      ' failed' +
      (failures ? '' : '  -  run the Tinytest suite too: meteor test-packages ./')
  );

  process.exit(failures ? 1 : 0);
})().catch(error => {
  console.error('\nthe unit run itself failed:\n', error);
  process.exit(1);
});

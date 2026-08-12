#!/usr/bin/env node
//
// Runs the fast unit layer. No Meteor, no MongoDB - just node:
//
//   node tests/unit/run.js      (or: npm test)
//
// The Tinytest suite in tests/ covers everything that needs a real server:
//   meteor test-packages --release METEOR@2.15 ./

const suites = [require('./join.unit'), require('./publication.unit')];

let failures = 0;
let checks = 0;

for (const suite of suites) {
  const result = suite();
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

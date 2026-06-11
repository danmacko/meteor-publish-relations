Package.describe({
  name: 'danmacko:publish-relations',
  summary: 'Reactive joins for Meteor publications (fork of cottz:publish-relations)',
  version: '3.0.2',
  git: 'https://github.com/danmacko/meteor-publish-relations',
  documentation: 'README.md'
});

Package.onUse(function (api) {
  api.versionsFrom('1.5.1');

  api.use([
    'ecmascript'
  ]);

  api.mainModule('lib/server/index.js', 'server');

  api.export('PublishRelations', 'server');
});

Package.onTest(function (api) {
  api.use([
    'ecmascript',
    'tinytest',
    'random',
    'mongo',
    'ddp',
    'danmacko:publish-relations'
  ]);

  api.mainModule('tests/index.js');
});

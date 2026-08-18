import { Meteor } from 'meteor/meteor';
import { Tinytest } from 'meteor/tinytest';
import { Mongo } from 'meteor/mongo';
import { Random } from 'meteor/random';
import PublishRelations from 'meteor/danmacko:publish-relations';
import { data, Client } from './data';

Tinytest.add('Package - exported API surface', function (test) {
  // package.js promises three entry points and nothing else in the suite
  // touches two of them: the api.export global, and Meteor.publishRelations.
  // They would break silently if publish_relations.js ever declared its
  // function with const/let instead of the bare assignment api.export needs.
  test.equal(typeof PublishRelations, 'function', 'default import is callable');
  test.equal(typeof Meteor.publishRelations, 'function', 'Meteor.publishRelations is installed');
  test.isTrue(Meteor.publishRelations === PublishRelations, 'Meteor.publishRelations is the same function');

  const pkg = Package['danmacko:publish-relations'];
  test.isTrue(!!pkg, 'package is registered under its Atmosphere name');
  test.equal(typeof pkg.PublishRelations, 'function', "api.export('PublishRelations') resolves");
  test.isTrue(pkg.PublishRelations === PublishRelations, 'the exported global is the same function');
});

Tinytest.addAsync('Cursor', async function (test) {
  var quotes = new Mongo.Collection(Random.id()),
    publish = Random.id(),
    docs = data.quotes;

  for (var doc in docs) {
    await quotes.insertAsync(docs[doc]);
  };

  // Read once, up front: a DDP message handler is not async, and findOne no
  // longer exists on the server (findOneAsync does).
  const fieldsById = new Map((await quotes.find().fetchAsync()).map(({_id, ...fields}) => [_id, fields]));

  PublishRelations(publish, function () {
    this.relations.cursor(quotes.find());

    return this.ready();
  });

  await new Promise(done => {
    var client = Client();
    client._livedata_data = function (msg) {
      if (msg.msg == 'added') {
        test.equal(msg.fields, fieldsById.get(msg.id));
      } else if (msg.msg == 'ready') {
        client.disconnect();
        done();
      }
    };

    client.subscribe(publish);
  });
});

Tinytest.addAsync('Observes', async function (test) {
  var quotes = new Mongo.Collection(Random.id()),
    publish = Random.id(),
    publish2 = Random.id(),
    docs = data.quotes;

  for (var doc in docs) {
    await quotes.insertAsync(docs[doc]);
  }

  const byId = new Map((await quotes.find().fetchAsync()).map(doc => [doc._id, doc]));

  PublishRelations(publish, function () {
    this.relations.observe(quotes.find(), {
      added: function (doc) {
        test.equal(doc, byId.get(doc._id));
      }
    });
  });

  PublishRelations(publish2, function () {
    this.relations.observeChanges(quotes.find(), {
      added: function (id, doc) {
        const { _id, ...fields } = byId.get(id);
        test.equal(doc, fields);
      }
    });
  
    return this.ready();
  });

  await new Promise(done => {
    var client = Client();
    client._livedata_data = function (msg) {
      test.equal(msg.msg, 'ready');
      client.disconnect();
    };

    client.subscribe(publish);

    var client2 = Client();
    client2._livedata_data = function (msg) {
      test.equal(msg.msg, 'ready');
      client2.disconnect();
      done();
    };

    client2.subscribe(publish2);
  });
});
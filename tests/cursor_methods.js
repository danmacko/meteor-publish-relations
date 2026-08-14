import { Tinytest } from 'meteor/tinytest';
import { Mongo } from 'meteor/mongo';
import { Random } from 'meteor/random';
import PublishRelations from 'meteor/danmacko:publish-relations';
import { data, Client } from './data';

Tinytest.addAsync('Relations - observe', function (test, done) {
  var quotesName = Random.id(),
    quotes = new Mongo.Collection(quotesName),
    products = new Mongo.Collection(Random.id()),
    publish = Random.id(),
    names = data.names,
    docs = data.quotes;

  for (var doc in docs) {
    var quote = docs[doc];

    quotes.insert(quote);
    for (var i = 0; i < 3; i ++) {
      products.insert({
        quoteId: quote._id,
        name: names[i],
        price: 1000 * i
      });
    }
  };

  PublishRelations(publish, function () {
    this.cursor(quotes.find(), function (id, doc) {
      var productsCursor = products.find({quoteId: id});

      this.observe(productsCursor, {
        added: function (doc) {
          test.equal(doc.name, names[doc.price / 1000]);
        }
      });

      this.observeChanges(productsCursor, {
        added: function (prodId, doc) {
          test.equal(doc.name, names[doc.price / 1000]);
        }
      });
    });
  
    return this.ready();
  });

  var client = Client();

  client._livedata_data = function (msg) {
    if (msg.msg == 'added') {
      test.equal(msg.collection, quotesName);
    } else if (msg.msg == 'ready') {
      client.disconnect();
      done();
    }
  };

  client.subscribe(publish);
});

Tinytest.addAsync('Relations - cursor basic', function (test, done) {
  var quotesName = Random.id(),
    productsName = Random.id(),
    quotes = new Mongo.Collection(quotesName),
    products = new Mongo.Collection(productsName),
    names = data.names,
    publish = Random.id(),
    docs = data.quotes;

  for (var doc in docs) {
    var quote = docs[doc];

    quotes.insert(quote);
    for (var i = 0; i < 3; i ++) {
      products.insert({
        quoteId: quote._id,
        name: names[i],
        price: 1000 * i
      });
    }
  };

  PublishRelations(publish, function () {
    this.cursor(quotes.find(), function (id, doc) {
      this.cursor(products.find({quoteId: id}));
    });
  
    return this.ready();
  });

  var client = Client();
  client._livedata_data = function (msg) {
    if (msg.collection == productsName) {
      var fields = msg.fields;

      test.isTrue(quotes.findOne({_id: fields.quoteId}));
      test.equal(fields.name, names[fields.price / 1000]);

    } else if (msg.collection == quotesName) {
      test.equal(msg.fields, quotes.findOne({_id: msg.id}, {fields: {_id: 0}}));

    } else if (msg.msg == 'ready') {
      client.disconnect();
      done();
    }
  };

  client.subscribe(publish);
});

Tinytest.addAsync('Relations - cursor', function (test, done) {
  var quotesName = Random.id(),
    productsName = Random.id(),
    quotes = new Mongo.Collection(quotesName),
    products = new Mongo.Collection(productsName),
    names = data.names,
    colors = ['blue', 'black', 'red'],
    publish = Random.id(),
    docs = data.quotes;

  for (var doc in docs) {
    var quote = docs[doc];

    quotes.insert(quote);
    for (var i = 0; i < 3; i ++) {
      products.insert({
        quoteId: quote._id,
        name: names[i],
        price: 1000 * i
      });
    }
  };

  PublishRelations(publish, function () {
    this.cursor(quotes.find(), function (id, doc) {
      this.cursor(products.find({quoteId: id}), function (prodId, prod) {
        // the callback may edit the document it is publishing
        prod.color = colors[prod.price / 1000];
      });
    });

    return this.ready();
  });

  var client = Client();
  client._livedata_data = function (msg) {
    if(msg.collection == productsName) {
      var fields = msg.fields;

      test.isTrue(quotes.findOne({_id: fields.quoteId}));
      test.equal(fields.name, names[fields.price / 1000]);
      test.equal(fields.color, colors[fields.price / 1000]);
    } else if (msg.collection == quotesName) {
      test.equal(msg.fields, quotes.findOne({_id: msg.id}, {fields: {_id: 0}}));

    } else if (msg.msg == 'ready') {
      client.disconnect();
      done();
    }
  };

  client.subscribe(publish);
});

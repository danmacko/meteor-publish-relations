import { Tinytest } from 'meteor/tinytest';
import { Mongo } from 'meteor/mongo';
import { Random } from 'meteor/random';
import PublishRelations from 'meteor/danmacko:publish-relations';
import { data, Client } from './data';

Tinytest.addAsync('Relations - observe', async function (test) {
  var quotesName = Random.id(),
    quotes = new Mongo.Collection(quotesName),
    products = new Mongo.Collection(Random.id()),
    publish = Random.id(),
    names = data.names,
    docs = data.quotes;

  for (var doc in docs) {
    var quote = docs[doc];

    await quotes.insertAsync(quote);
    for (var i = 0; i < 3; i ++) {
      await products.insertAsync({
        quoteId: quote._id,
        name: names[i],
        price: 1000 * i
      });
    }
  };

  // Read once, up front: a DDP message handler cannot await, and on the server
  // findOne is gone in favour of findOneAsync.
  const quoteFields = new Map((await quotes.find().fetchAsync()).map(({_id, ...fields}) => [_id, fields]));

  PublishRelations(publish, function () {
    this.relations.cursor(quotes.find(), function (id, doc) {
      var productsCursor = products.find({quoteId: id});

      this.relations.observe(productsCursor, {
        added: function (doc) {
          test.equal(doc.name, names[doc.price / 1000]);
        }
      });

      this.relations.observeChanges(productsCursor, {
        added: function (prodId, doc) {
          test.equal(doc.name, names[doc.price / 1000]);
        }
      });
    });
  
    return this.ready();
  });

  await new Promise(done => {
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
});

Tinytest.addAsync('Relations - cursor basic', async function (test) {
  var quotesName = Random.id(),
    productsName = Random.id(),
    quotes = new Mongo.Collection(quotesName),
    products = new Mongo.Collection(productsName),
    names = data.names,
    publish = Random.id(),
    docs = data.quotes;

  for (var doc in docs) {
    var quote = docs[doc];

    await quotes.insertAsync(quote);
    for (var i = 0; i < 3; i ++) {
      await products.insertAsync({
        quoteId: quote._id,
        name: names[i],
        price: 1000 * i
      });
    }
  };

  // Read once, up front: a DDP message handler cannot await, and on the server
  // findOne is gone in favour of findOneAsync.
  const quoteFields = new Map((await quotes.find().fetchAsync()).map(({_id, ...fields}) => [_id, fields]));

  PublishRelations(publish, function () {
    this.relations.cursor(quotes.find(), function (id, doc) {
      this.relations.cursor(products.find({quoteId: id}));
    });
  
    return this.ready();
  });

  await new Promise(done => {
    var client = Client();
    client._livedata_data = function (msg) {
      if (msg.collection == productsName) {
        var fields = msg.fields;

        test.isTrue(quoteFields.has(fields.quoteId));
        test.equal(fields.name, names[fields.price / 1000]);

      } else if (msg.collection == quotesName) {
        test.equal(msg.fields, quoteFields.get(msg.id));

      } else if (msg.msg == 'ready') {
        client.disconnect();
        done();
      }
    };

    client.subscribe(publish);
  });
});

Tinytest.addAsync('Relations - cursor', async function (test) {
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

    await quotes.insertAsync(quote);
    for (var i = 0; i < 3; i ++) {
      await products.insertAsync({
        quoteId: quote._id,
        name: names[i],
        price: 1000 * i
      });
    }
  };

  // Read once, up front: a DDP message handler cannot await, and on the server
  // findOne is gone in favour of findOneAsync.
  const quoteFields = new Map((await quotes.find().fetchAsync()).map(({_id, ...fields}) => [_id, fields]));

  PublishRelations(publish, function () {
    this.relations.cursor(quotes.find(), function (id, doc) {
      this.relations.cursor(products.find({quoteId: id}), function (prodId, prod) {
        // the callback may edit the document it is publishing
        prod.color = colors[prod.price / 1000];
      });
    });

    return this.ready();
  });

  await new Promise(done => {
    var client = Client();
    client._livedata_data = function (msg) {
      if(msg.collection == productsName) {
        var fields = msg.fields;

        test.isTrue(quoteFields.has(fields.quoteId));
        test.equal(fields.name, names[fields.price / 1000]);
        test.equal(fields.color, colors[fields.price / 1000]);
      } else if (msg.collection == quotesName) {
        test.equal(msg.fields, quoteFields.get(msg.id));

      } else if (msg.msg == 'ready') {
        client.disconnect();
        done();
      }
    };

    client.subscribe(publish);
  });
});

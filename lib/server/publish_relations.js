import { Meteor } from 'meteor/meteor';
import HandlerController from './handler_controller';
import CursorMethods from './cursor';

PublishRelations = function (name, callback) {
  return Meteor.publish(name, function (...params) {
    let handler = new HandlerController();

    // The package API lives in its own namespace instead of being merged onto
    // the subscription, so it can never collide with a method Meteor's
    // Subscription may gain later. `this` stays the raw Subscription, which
    // keeps ready()/added()/userId/onStop() working - and keeps them operating
    // on the real subscription state (a merged copy would strand _ready on it).
    this.relations = new CursorMethods(this, handler);

    this.onStop(() => handler.stop());

    return callback.apply(this, params);
  });
};

Meteor.publishRelations = PublishRelations;

export default PublishRelations;
export { PublishRelations };

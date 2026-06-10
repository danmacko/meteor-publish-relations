import { Meteor } from 'meteor/meteor';
import HandlerController from './handler_controller';
import CursorMethods from './cursor';

PublishRelations = function (name, callback) {
  return Meteor.publish(name, function (...params) {
    let handler = new HandlerController(),
    cursors = new CursorMethods(this, handler);

    this._publicationName = name;
    this.onStop(() => handler.stop());

    // Copy the subscription's own AND inherited (prototype) members onto the
    // cursor methods object, so the callback's `this` exposes both APIs:
    // this.cursor/join/... and this.ready/added/userId/... . Object.assign would
    // only copy own properties and drop prototype methods like ready().
    for (const key in this) {
      cursors[key] = this[key];
    }
    let cb = callback.apply(cursors, params);
    // kadira show me alerts when I use this return (but works well)
    // return cb || (!this._ready && this.ready());
    return cb;
  });
};

Meteor.publishRelations = PublishRelations;

export default PublishRelations;
export { PublishRelations };
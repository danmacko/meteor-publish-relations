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
    const relations = this.relations = new CursorMethods(this, handler);

    this.onStop(() => handler.stop());

    // ready() is recorded here rather than sent. On Meteor 3 a cursor is only
    // registered asynchronously, so a body written the way every body is -
    // register, then `return this.ready()` - would otherwise tell the client
    // "that is everything" while the observers are still being built, and the
    // documents would land after the client had already rendered an empty list.
    //
    // Held until the registrations have settled, which is what makes awaiting
    // them optional for the caller. readyNow() is the escape hatch for a
    // publication that means the literal Meteor behaviour: ready first, data
    // when it comes.
    const sendReady = this.ready.bind(this);
    let readyRequested = false;

    this.ready = () => { readyRequested = true; };
    relations.readyNow = sendReady;

    const run = async () => {
      try {
        // Awaited, not just forwarded: an async body has to finish before its
        // registrations can be settled, and its return value is what Meteor
        // then applies the "a cursor, or an array of cursors" convention to.
        const result = await callback.apply(this, params);

        await relations._settle();

        // The body may have stopped or errored the subscription while we were
        // waiting; ready() would ignore it anyway, but there is no reason to
        // announce anything about a subscription that is already gone.
        if (readyRequested && !this._isDeactivated()) sendReady();

        return result;
      } finally {
        // Callbacks of live updates run long after this, and they must reach
        // the real one.
        this.ready = sendReady;
      }
    };

    return run();
  });
};

Meteor.publishRelations = PublishRelations;

export default PublishRelations;
export { PublishRelations };

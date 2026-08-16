import CursorMethods from './cursor';

function getCB (cb, method) {
  var callback = cb[method];
  if (callback && typeof callback !== 'function')
    throw new Error(method + ' should be a function or undefined');

  return callback || function () {};
};

CursorMethods.prototype._getCallbacks = function (cb) {
  // A bare function stands for added AND changed, and says nothing about
  // removals - the third argument tells the two apart.
  if (typeof cb === 'function') {
    return {
      added: cb,
      changed: cb,
      removed: function () {}
    };
  }

  return {
    added: getCB(cb, 'added'),
    changed: getCB(cb, 'changed'),
    removed: getCB(cb, 'removed')
  };
};
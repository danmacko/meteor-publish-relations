// Is this subscription currently publishing `id` under `collection`?
//   true  - provably published
//   false - provably not
//   null  - unknown; the caller picks its own fail-safe side. Forwarding
//           someone else's removed: treat null as "go ahead" (skipping
//           strands the doc). Initiating one (join retraction): treat null
//           as "don't" (a removed for a doc never sent throws on both ends).
//
// Reads only sub._documents and sub._idFilter, both guarded: a change of
// shape degrades to null / identity. Deliberately does NOT consult
// getPublicationStrategy() - a rename there could otherwise flip an answer.
export function isPublishedInSub (sub, collection, id) {
  if (typeof sub._documents?.get !== 'function') return null; // ancient/unknown Meteor

  // A missing Set proves nothing: nothing published under this name yet, or a
  // strategy that keeps no accounting. An existing Set does prove it -
  // ddp-server deletes ids from a Set, never the Set itself - and every caller
  // that acts on `false` does so for an already-published doc, so the Set
  // exists by then and this null costs nothing.
  const publishedDocs = sub._documents.get(collection);
  if (!publishedDocs) return null;

  // The Set holds stringified ids (sub.added/removed apply _idFilter first).
  const idStringify = sub._idFilter?.idStringify;
  return publishedDocs.has(idStringify ? idStringify(id) : id);
}

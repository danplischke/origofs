// Generator for the byte fixtures in `origofs-core/tests/coedit_tree_slate.rs`
// (issue #152). Run it to regenerate them against a newer @slate-yjs/core:
//
//     npm install yjs@13 @slate-yjs/core@1.0.2
//     node slate_yjs_client.mjs
//
// It prints the two updates as hex, plus what the Slate side reconstructs, so a
// version bump that changes the encoding shows up as a diff rather than as a
// silently weaker test.
import * as Y from 'yjs';
import { slateNodesToInsertDelta, yTextToSlateElement } from '@slate-yjs/core';

// Exactly what `@platejs/yjs` does: bind the Slate root through
// `@slate-yjs/core`, which roots the document at a `Y.XmlText`.
const doc = new Y.Doc();
const sharedRoot = doc.get('content', Y.XmlText);
console.log('root type:', sharedRoot.constructor.name);   // YXmlText

sharedRoot.applyDelta(
  slateNodesToInsertDelta([
    { type: 'paragraph', children: [{ text: 'hello ' }, { text: 'world', bold: true }] },
    { type: 'paragraph', children: [{ text: 'second para' }] },
  ]),
  { sanitize: false },
);
const first = Y.encodeStateAsUpdate(doc);
console.log('SLATE_INITIAL =', Buffer.from(first).toString('hex'));

// A second edit, from a client that has already synced: typing into the first
// paragraph. This is what makes the attribution assertion meaningful — origofs
// stamps it with whichever actor's connection delivered it.
const sv = Y.encodeStateVector(doc);
const para = sharedRoot.toDelta()[0].insert;
para.insert(para.length, ' TYPED-BY-CLIENT');
console.log('SLATE_SECOND_EDIT =', Buffer.from(Y.encodeStateAsUpdate(doc, sv)).toString('hex'));

console.log('slate value:', JSON.stringify(yTextToSlateElement(sharedRoot)));

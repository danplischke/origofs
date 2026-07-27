// The Plate plugin set: basic blocks (headings, blockquote, lists, code) + basic
// marks (bold/italic/underline/strike/code), and the Markdown plugin so the
// document round-trips to the Markdown origofs stores and blames by line.

import { BasicBlocksPlugin, BasicMarksPlugin } from "@platejs/basic-nodes/react";
import { MarkdownPlugin } from "@platejs/markdown";

export const editorPlugins = [BasicBlocksPlugin, BasicMarksPlugin, MarkdownPlugin];

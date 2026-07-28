// Bridge origofs's per-line blame to the editor.
//
// origofs attributes documents by **source line** (1-based, inclusive). PlateJS
// edits **blocks**. These helpers map between the two:
//   * `blameLines`       — the exact, line-by-line view (for the Blame tab).
//   * `blockLineSpans`   — which source lines each top-level block round-trips to
//                          (by serializing the block alone via `serializeMd`).
//   * `blockAuthorship`  — the dominant author of each block (for the gutter).

import { serializeMd } from "@platejs/markdown";
import type { Actor, BlameRange } from "./types";

/** The actor who authored a given 1-based line, or null if unattributed. */
export function authorForLine(blame: BlameRange[], line: number): Actor | null {
  for (const r of blame) {
    if (line >= r.line_start && line <= r.line_end) return r.actor;
  }
  return null;
}

export interface BlameLine {
  line: number;
  text: string;
  actor: Actor | null;
}

/**
 * Split a document into lines paired with their author. Matches origofs's line
 * counting (it splits on '\n' keeping the trailing newline), so line N here is
 * the same line N that blame refers to.
 */
export function blameLines(text: string, blame: BlameRange[]): BlameLine[] {
  const parts = text.split("\n");
  // origofs's split_inclusive('\n') on "a\nb\n" yields 2 lines; JS split yields a
  // trailing "" — drop it so our line count matches blame's.
  if (parts.length > 0 && parts[parts.length - 1] === "") parts.pop();
  return parts.map((t, i) => ({ line: i + 1, text: t, actor: authorForLine(blame, i + 1) }));
}

/**
 * The 1-based, inclusive source-line span each top-level block occupies.
 *
 * We serialize each block on its own (`serializeMd(editor, { value: [node] })`)
 * to learn how many lines it contributes, then advance past the blank line
 * remark puts between blocks. This mirrors how the whole document serializes, so
 * the spans line up with what origofs stored and blamed.
 */
export function blockLineSpans(editor: unknown, nodes: unknown[]): Array<[number, number]> {
  const spans: Array<[number, number]> = [];
  let line = 1;
  for (const node of nodes) {
    let k = 1;
    try {
      const md = serializeMd(editor as never, { value: [node as never] }).replace(/\n+$/, "");
      k = md.length ? md.split("\n").length : 1;
    } catch {
      k = 1;
    }
    spans.push([line, line + k - 1]);
    line += k + 1; // + the blank separator line between blocks
  }
  return spans;
}

export interface BlockAuthorship {
  /** The block's dominant author (most lines), or null if unattributed. */
  primary: Actor | null;
  /** Every distinct author with a line in this block. */
  authors: Actor[];
  /** True when more than one actor authored lines in this block. */
  mixed: boolean;
  lineStart: number;
  lineEnd: number;
}

/** Resolve each block span to its dominant author + whether it's mixed. */
export function blockAuthorship(
  spans: Array<[number, number]>,
  blame: BlameRange[],
): BlockAuthorship[] {
  return spans.map(([start, end]) => {
    const counts = new Map<number, { actor: Actor; lines: number }>();
    for (let ln = start; ln <= end; ln++) {
      const a = authorForLine(blame, ln);
      if (!a) continue;
      const c = counts.get(a.id) ?? { actor: a, lines: 0 };
      c.lines += 1;
      counts.set(a.id, c);
    }
    let primary: Actor | null = null;
    let best = -1;
    for (const { actor, lines } of counts.values()) {
      if (lines > best) {
        best = lines;
        primary = actor;
      }
    }
    return {
      primary,
      authors: [...counts.values()].map((c) => c.actor),
      mixed: counts.size > 1,
      lineStart: start,
      lineEnd: end,
    };
  });
}

// The exact, line-precise attribution view — origofs blames by source line, and this
// renders exactly that: every line with its author, like `git blame`. Unlike the
// editor gutter (which maps blame onto rendered blocks), this is 1:1 with what
// origofs stored, so it's the ground truth.

import { useMemo } from "react";
import { blameLines } from "../lib/blame";
import { actorColor, kindGlyph } from "../lib/colors";
import type { BlameRange } from "../lib/types";

export function BlameView({ text, blame }: { text: string; blame: BlameRange[] }) {
  const lines = useMemo(() => blameLines(text, blame), [text, blame]);

  if (lines.length === 0) {
    return <div className="empty">This document is empty.</div>;
  }

  return (
    <div className="blame-view">
      {blame.length === 0 && (
        <div className="notice">
          No attribution recorded — this content was written without an actor (a plain{" "}
          <code>write</code>, not <code>write_as</code>). Save an edit while signed in and every
          line gets a byte-accurate author.
        </div>
      )}
      <table className="blame-table">
        <tbody>
          {lines.map(({ line, text: lineText, actor }) => {
            const c = actor ? actorColor(actor.id, actor.kind) : null;
            return (
              <tr key={line}>
                <td className="blame-actor" style={{ background: c?.bg }}>
                  {actor ? (
                    <span
                      style={{ color: c!.fg }}
                      title={
                        actor.kind === "agent" && actor.agent_model
                          ? `${actor.display_name} · ${actor.agent_model}`
                          : actor.display_name
                      }
                    >
                      {kindGlyph(actor.kind)} {actor.display_name}
                    </span>
                  ) : (
                    <span className="muted">—</span>
                  )}
                </td>
                <td className="blame-lineno">{line}</td>
                <td className="blame-code" style={{ borderLeftColor: c?.fg ?? "transparent" }}>
                  <pre>{lineText || " "}</pre>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// Native inline attribution — a Plate plugin that *decorates* authored text with
// its author's color, rendered through Plate's own leaf pipeline (no DOM
// overlays). Per-block author info is pushed in as a plugin option and the
// decorations refresh via editor.api.redecorate().

import { createPlatePlugin, PlateLeaf, type PlateLeafProps } from "platejs/react";
import type { BlockAuthorship } from "../lib/blame";
import { actorColor } from "../lib/colors";

export interface AttributionOptions {
  /** Per top-level block (index-aligned) dominant author, from origofs blame. */
  spans: BlockAuthorship[];
  enabled: boolean;
}

interface AttributionDeco {
  color: string;
  bg: string;
  name: string;
}

// Leaf renderer: underline the run in the author's color, tint faintly, and
// name the author on hover. Falls through to a plain leaf when undecorated.
function AttributionLeaf(props: PlateLeafProps) {
  const deco = (props.leaf as { attribution?: AttributionDeco }).attribution;
  if (!deco) return <PlateLeaf {...props} />;
  return (
    <PlateLeaf
      {...props}
      className="attributed"
      style={{ boxShadow: `inset 0 -0.14em 0 ${deco.color}`, background: deco.bg }}
    />
  );
}

export const AttributionPlugin = createPlatePlugin({
  key: "attribution",
  node: { isLeaf: true },
  options: { spans: [], enabled: true } as AttributionOptions,
  render: { node: AttributionLeaf },
}).extend(({ getOptions }) => ({
  decorate: ({ entry }) => {
    const [node, path] = entry as [{ text?: string }, number[]];
    const { spans, enabled } = getOptions();
    if (!enabled || path.length === 0 || typeof node.text !== "string" || node.text.length === 0) {
      return [];
    }
    const info = spans[path[0]]; // path[0] = top-level block index
    if (!info?.primary) return [];
    const c = actorColor(info.primary.id, info.primary.kind);
    const deco: AttributionDeco = {
      color: c.fg,
      bg: c.bg,
      name: info.mixed ? `${info.primary.display_name} (+ others)` : info.primary.display_name,
    };
    return [
      {
        anchor: { path, offset: 0 },
        focus: { path, offset: node.text.length },
        attribution: deco,
      },
    ] as never;
  },
}));

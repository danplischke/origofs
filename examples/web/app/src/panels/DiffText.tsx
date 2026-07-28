// Render a unified diff (origofs returns plain `diff -u` text) with +/- coloring.

export function DiffText({ text }: { text: string }) {
  if (!text) return <div className="empty">No diff.</div>;
  const lines = text.split("\n");
  return (
    <pre className="diff-text">
      {lines.map((line, i) => {
        let cls = "ctx";
        if (line.startsWith("+++") || line.startsWith("---")) cls = "meta";
        else if (line.startsWith("@@")) cls = "hunk";
        else if (line.startsWith("+")) cls = "add";
        else if (line.startsWith("-")) cls = "del";
        return (
          <span key={i} className={`diff-line ${cls}`}>
            {line || " "}
            {"\n"}
          </span>
        );
      })}
    </pre>
  );
}

// The Edit tab: the Plate editor, plus the inline agent-suggestion review.
//
// When a suggestion is pending for the open document, a banner offers to review
// it; "Review" swaps the editor for the inline diff (ReviewOverlay). This is the
// VSCode "agent proposed edits — keep/discard" flow, backed by origofs's suggestion
// queue.

import { useCallback, useEffect, useState } from "react";
import { EditorTab } from "./Editor";
import { ReviewOverlay } from "../review/ReviewOverlay";
import { useSession } from "../session";
import { ActorChip } from "../components/ActorBadge";
import type { BlameRange, Suggestion } from "../lib/types";

export function EditPane({
  path,
  initialText,
  blame,
  onChanged,
}: {
  path: string;
  initialText: string;
  blame: BlameRange[];
  onChanged: () => void;
}) {
  const { client, resolveActor, actorName } = useSession();
  const [pending, setPending] = useState<Suggestion[]>([]);
  const [reviewId, setReviewId] = useState<number | null>(null);
  const [toast, setToast] = useState<string | null>(null);

  const refreshPending = useCallback(() => {
    client
      .listSuggestions("pending", path)
      .then(setPending)
      .catch(() => setPending([]));
  }, [client, path]);

  // Poll for pending suggestions on this document (an agent may propose at any
  // time). Also refresh right after the doc reloads.
  useEffect(() => {
    refreshPending();
    const h = setInterval(refreshPending, 4000);
    return () => clearInterval(h);
  }, [refreshPending, blame]);

  if (reviewId != null) {
    return (
      <ReviewOverlay
        key={reviewId}
        id={reviewId}
        onCancel={() => setReviewId(null)}
        onDone={(msg) => {
          setReviewId(null);
          setToast(msg);
          onChanged();
          refreshPending();
        }}
      />
    );
  }

  return (
    <div className="edit-pane">
      {pending.length > 0 && (
        <div className="review-banner">
          <span className="review-banner-icon" aria-hidden>
            🤖
          </span>
          <span>
            {pending.length} proposed {pending.length === 1 ? "change" : "changes"} for this
            document
            {pending[0] && (
              <>
                {" "}
                — newest by{" "}
                <ActorChip
                  name={resolveActor(pending[0].actor_id)?.display_name ?? actorName(pending[0].actor_id)}
                  kind={resolveActor(pending[0].actor_id)?.kind ?? "agent"}
                />
              </>
            )}
          </span>
          <span className="spacer" />
          <button className="primary" onClick={() => setReviewId(pending[0].id)}>
            Review inline
          </button>
        </div>
      )}
      {toast && (
        <div className="notice ok-notice" onAnimationEnd={() => setToast(null)}>
          {toast}
        </div>
      )}
      <EditorTab
        key={path}
        path={path}
        initialText={initialText}
        blame={blame}
        onSaved={() => {
          onChanged();
          refreshPending();
        }}
      />
    </div>
  );
}

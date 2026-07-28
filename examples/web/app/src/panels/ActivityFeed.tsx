// Live collaboration: presence (who's here now) + an SSE stream of every
// attributed change, straight off origofs's change feed. The signed-in principal
// heartbeats presence so they show up for everyone else.

import { useEffect, useMemo, useRef, useState } from "react";
import { useSession } from "../session";
import { relativeTime } from "../lib/time";
import { ActorChip } from "../components/ActorBadge";
import type { ChangeEvent, Presence } from "../lib/types";

const MAX_EVENTS = 60;

export function ActivityFeed({ currentPath }: { currentPath: string }) {
  const { client, token, me, actorName, colorFor } = useSession();
  const [events, setEvents] = useState<ChangeEvent[]>([]);
  const [presence, setPresence] = useState<Presence[]>([]);
  const seen = useRef<Set<number>>(new Set());

  // Live change feed over SSE.
  useEffect(() => {
    const es = new EventSource(client.feedUrl(0));
    es.onmessage = (e) => {
      try {
        const ev = JSON.parse(e.data) as ChangeEvent;
        if (seen.current.has(ev.seq)) return;
        seen.current.add(ev.seq);
        setEvents((prev) => [ev, ...prev].slice(0, MAX_EVENTS));
      } catch {
        /* ignore keep-alive comments / malformed frames */
      }
    };
    return () => es.close();
  }, [client]);

  // Poll presence.
  useEffect(() => {
    let live = true;
    const tick = () =>
      client
        .presence(60)
        .then((p) => live && setPresence(p))
        .catch(() => undefined);
    tick();
    const h = setInterval(tick, 5000);
    return () => {
      live = false;
      clearInterval(h);
    };
  }, [client]);

  // Heartbeat our own presence (needs a session, i.e. signed in).
  useEffect(() => {
    if (!token || me?.session_id == null) return;
    const beat = () => client.touch(currentPath).catch(() => undefined);
    beat();
    const h = setInterval(beat, 20000);
    return () => clearInterval(h);
  }, [client, token, me?.session_id, currentPath]);

  const others = useMemo(
    () => presence.filter((p) => p.actor_id !== me?.actor_id),
    [presence, me?.actor_id],
  );

  return (
    <div className="activity">
      <div className="presence">
        <h4>Present</h4>
        <div className="presence-chips">
          {me && <ActorChip name={`${me.display_name ?? "you"} (you)`} kind={me.kind ?? "human"} />}
          {others.map((p) => (
            <ActorChip
              key={p.session_id}
              name={p.display_name}
              kind={p.kind}
              color={colorFor(p.actor_id, p.kind)}
              title={p.path ? `on ${p.path}` : undefined}
            />
          ))}
          {!me && others.length === 0 && <span className="muted">no one signed in</span>}
        </div>
      </div>
      <div className="feed">
        <h4>Activity</h4>
        {events.length === 0 ? (
          <div className="muted">waiting for changes…</div>
        ) : (
          <ul className="feed-list">
            {events.map((ev) => {
              const c = colorFor(ev.actor_id);
              return (
                <li key={ev.seq}>
                  <span className="feed-verb" style={{ color: c.fg, background: c.bg }}>
                    {ev.kind}
                  </span>
                  <span className="feed-who">{actorName(ev.actor_id)}</span>
                  <span className="feed-path" title={ev.detail ?? undefined}>
                    {ev.path}
                  </span>
                  <span className="feed-time">{relativeTime(ev.ts)}</span>
                </li>
              );
            })}
          </ul>
        )}
      </div>
    </div>
  );
}

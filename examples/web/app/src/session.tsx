// Session context: the origofs client, the signed-in principal, and the actor
// directory used to resolve the `actor_id` in events/suggestions to a name.
//
// This is a *demo* sign-in — it picks one of the server's hardcoded tokens. A
// real app would obtain the token from its own login and never expose a picker.

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { OrigoFSClient } from "./lib/origofsClient";
import { actorColor, type ActorColor } from "./lib/colors";
import type { ActorKind, AppConfig, DirectoryActor, Me } from "./lib/types";

const TOKEN_KEY = "origofs-web.token";

interface SessionValue {
  client: OrigoFSClient;
  config: AppConfig | null;
  token: string | null;
  me: Me | null;
  actors: DirectoryActor[];
  signIn: (token: string) => void;
  signOut: () => void;
  /** Resolve an actor id (from events/suggestions) to a directory entry. */
  resolveActor: (id: number | null | undefined) => DirectoryActor | null;
  /** A display name for an actor id, falling back to `actor #id`. */
  actorName: (id: number | null | undefined) => string;
  /** A stable color for an actor id (kind taken from the directory). */
  colorFor: (id: number | null | undefined, kindHint?: ActorKind) => ActorColor;
}

const Ctx = createContext<SessionValue | null>(null);

export function SessionProvider({ children }: { children: ReactNode }) {
  const client = useMemo(() => new OrigoFSClient(), []);
  const [config, setConfig] = useState<AppConfig | null>(null);
  const [actors, setActors] = useState<DirectoryActor[]>([]);
  const [token, setToken] = useState<string | null>(() => localStorage.getItem(TOKEN_KEY));
  const [me, setMe] = useState<Me | null>(null);

  // Keep the client's token in sync so writes are attributed.
  useEffect(() => {
    client.setToken(token);
  }, [client, token]);

  // Static-ish data: the demo config + the actor directory.
  useEffect(() => {
    client.config().then(setConfig).catch(() => setConfig(null));
    client.actors().then(setActors).catch(() => setActors([]));
  }, [client]);

  // Resolve "who am I" whenever the token changes.
  useEffect(() => {
    if (!token) {
      setMe(null);
      return;
    }
    let live = true;
    client
      .me()
      .then((m) => live && setMe(m))
      .catch(() => live && setMe(null));
    return () => {
      live = false;
    };
  }, [client, token]);

  const signIn = useCallback((t: string) => {
    localStorage.setItem(TOKEN_KEY, t);
    setToken(t);
  }, []);

  const signOut = useCallback(() => {
    localStorage.removeItem(TOKEN_KEY);
    setToken(null);
  }, []);

  const byId = useMemo(() => {
    const m = new Map<number, DirectoryActor>();
    for (const a of actors) m.set(a.id, a);
    return m;
  }, [actors]);

  const resolveActor = useCallback(
    (id: number | null | undefined) => (id == null ? null : byId.get(id) ?? null),
    [byId],
  );

  const actorName = useCallback(
    (id: number | null | undefined) => {
      if (id == null) return "git-level";
      return byId.get(id)?.display_name ?? `actor #${id}`;
    },
    [byId],
  );

  const colorFor = useCallback(
    (id: number | null | undefined, kindHint?: ActorKind) => {
      if (id == null) return actorColor(-1, "system");
      const kind = byId.get(id)?.kind ?? kindHint ?? "human";
      return actorColor(id, kind);
    },
    [byId],
  );

  const value: SessionValue = {
    client,
    config,
    token,
    me,
    actors,
    signIn,
    signOut,
    resolveActor,
    actorName,
    colorFor,
  };
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useSession(): SessionValue {
  const v = useContext(Ctx);
  if (!v) throw new Error("useSession must be used within <SessionProvider>");
  return v;
}

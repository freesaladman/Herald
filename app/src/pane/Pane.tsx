import { useCallback, useEffect, useState, type CSSProperties } from "react";
import {
  ApiError,
  getUiState,
  hidePane,
  listUnviewed,
  markAllViewed,
  onUiState,
  onUnviewedChanged,
  setPinned,
  type Event,
  type UiState,
} from "../api";
import { Connections } from "./Connections";
import { RecordRow } from "./RecordRow";

type View = "records" | "connections";

const banner: CSSProperties = {
  padding: "4px 12px",
  fontSize: 12,
  borderBottom: "1px solid var(--border)",
  background: "var(--hover)",
};

function errorMessage(e: unknown): string {
  return e instanceof ApiError ? e.message : String(e);
}

function newestFirst(events: Event[]): Event[] {
  return [...events].sort(
    (a, b) => Date.parse(b.occurredUtc) - Date.parse(a.occurredUtc),
  );
}

function Banners({
  ui,
  now,
  onConnections,
  onAdd,
  hideEmptyNote,
}: {
  ui: UiState;
  now: number;
  onConnections: () => void;
  onAdd: () => void;
  hideEmptyNote: boolean;
}) {
  const snoozed =
    ui.snoozedUntilUtc && Date.parse(ui.snoozedUntilUtc) > now
      ? new Date(ui.snoozedUntilUtc)
      : null;
  const [firstError] = ui.configErrors;

  return (
    <>
      {snoozed && (
        <div style={banner}>Snoozed until {snoozed.toLocaleTimeString()}</div>
      )}
      {ui.connections.length === 0 && !hideEmptyNote && (
        <div
          style={{ ...banner, display: "flex", alignItems: "center", gap: 8 }}
        >
          <span>No connections yet.</span>
          <button type="button" className="btn" onClick={onAdd}>
            Add connection
          </button>
        </div>
      )}
      {ui.connections.map((c) => (
        <div key={c.id}>
          {c.authState === "missing" && (
            <button
              type="button"
              style={{
                ...banner,
                display: "block",
                width: "100%",
                color: "inherit",
                font: "inherit",
                textAlign: "left",
                border: 0,
                borderBottom: "1px solid var(--border)",
                cursor: "pointer",
              }}
              onClick={onConnections}
            >
              {c.authMethod === "azcli"
                ? `Sign in to the Azure CLI (az login) for ${c.id}, then choose Poll now`
                : `Add a PAT for ${c.id}`}
            </button>
          )}
          {c.authState === "invalid" && (
            <div style={{ ...banner, color: "var(--sev-critical)" }}>
              {c.authMethod === "azcli"
                ? `Sign-in for ${c.id} was rejected`
                : `PAT for ${c.id} was rejected`}
            </div>
          )}
          {c.disabledSources.length > 0 && (
            <div style={{ ...banner, color: "var(--sev-warn)" }}>
              {c.id}: disabled sources: {c.disabledSources.join(", ")}
            </div>
          )}
        </div>
      ))}
      {firstError && (
        <div style={{ ...banner, color: "var(--sev-critical)" }}>
          {ui.configErrors.length} config error
          {ui.configErrors.length === 1 ? "" : "s"}: {firstError.path}{" "}
          {firstError.message}
        </div>
      )}
      {!ui.secretsNative && (
        <div style={{ ...banner, color: "var(--sev-warn)" }}>
          OS keychain unavailable
        </div>
      )}
    </>
  );
}

export function Pane() {
  const [events, setEvents] = useState<Event[]>([]);
  const [ui, setUi] = useState<UiState | null>(null);
  const [now, setNow] = useState(() => Date.now());
  const [view, setView] = useState<View>("records");
  const [startWizard, setStartWizard] = useState(false);
  const openConnections = (wizard: boolean) => {
    setStartWizard(wizard);
    setView("connections");
  };
  const [error, setError] = useState<string | null>(null);

  const applyUi = useCallback((state: UiState) => {
    setUi(state);
    setNow(Date.now());
  }, []);
  const fail = useCallback((e: unknown) => setError(errorMessage(e)), []);
  const refresh = useCallback(
    () =>
      listUnviewed()
        .then((list) => setEvents(newestFirst(list)))
        .catch(fail),
    [fail],
  );

  useEffect(() => {
    void refresh();
    getUiState().then(applyUi).catch(fail);

    let cancelled = false;
    const unlisteners: Array<() => void> = [];
    const track = (p: Promise<() => void>) =>
      p
        .then((un) => {
          if (cancelled) un();
          else unlisteners.push(un);
        })
        .catch(fail);
    track(onUnviewedChanged(() => void refresh()));
    track(onUiState(applyUi));
    return () => {
      cancelled = true;
      unlisteners.forEach((un) => un());
    };
  }, [refresh, applyUi, fail]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") hidePane().catch(fail);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [fail]);

  return (
    <div className="pane-root">
      <header className="row" style={{ flexShrink: 0 }}>
        <strong>Herald</strong>
        <span className="badge" aria-label="Unviewed count">
          {events.length}
        </span>
        <span style={{ flex: 1 }} />
        <button
          type="button"
          className="btn btn-ghost"
          onClick={() => markAllViewed().catch(fail)}
        >
          Mark all viewed
        </button>
        <button
          type="button"
          className="btn btn-ghost"
          aria-pressed={ui?.pinned ?? false}
          onClick={() => setPinned(!ui?.pinned).catch(fail)}
        >
          {ui?.pinned ? "Unpin" : "Pin"}
        </button>
        <button
          type="button"
          className="btn btn-ghost"
          aria-label="Settings"
          onClick={() =>
            view === "records" ? openConnections(false) : setView("records")
          }
        >
          ⚙
        </button>
      </header>
      {ui && (
        <Banners
          ui={ui}
          now={now}
          onConnections={() => openConnections(false)}
          onAdd={() => openConnections(true)}
          hideEmptyNote={view === "connections"}
        />
      )}
      {error && (
        <div
          role="alert"
          style={{ ...banner, color: "var(--sev-critical)", display: "flex" }}
        >
          <span style={{ flex: 1 }}>{error}</span>
          <button
            type="button"
            aria-label="Dismiss error"
            onClick={() => setError(null)}
            style={{ background: "none", border: 0, color: "inherit" }}
          >
            ×
          </button>
        </div>
      )}
      {view === "connections" ? (
        <Connections
          connections={ui?.connections ?? []}
          secretsNative={ui?.secretsNative ?? true}
          startWizard={startWizard}
          onBack={() => setView("records")}
        />
      ) : (
        <div style={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
          {events.length === 0 ? (
            <div
              style={{
                padding: 24,
                textAlign: "center",
                color: "var(--muted)",
              }}
            >
              All caught up
            </div>
          ) : (
            events.map((e) => <RecordRow key={e.id} event={e} onError={fail} />)
          )}
        </div>
      )}
    </div>
  );
}

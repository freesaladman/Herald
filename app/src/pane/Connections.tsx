import { useEffect, useRef, useState, type CSSProperties } from "react";
import {
  ApiError,
  clearSecret,
  setSecret,
  testConnection,
  type AuthState,
  type ConnectionStatus,
} from "../api";

const CONFIRM_MS = 3000;

const stateColor: Record<AuthState, string> = {
  ok: "var(--sev-success)",
  missing: "var(--sev-warn)",
  invalid: "var(--sev-critical)",
  unknown: "var(--muted)",
};

const section: CSSProperties = {
  padding: "8px 12px",
  borderBottom: "1px solid var(--border)",
  display: "flex",
  flexDirection: "column",
  gap: 6,
};

const note: CSSProperties = { fontSize: 12, color: "var(--muted)" };

function errorMessage(e: unknown): string {
  return e instanceof ApiError ? e.message : String(e);
}

function ConnectionCard({
  connection: c,
  secretsNative,
}: {
  connection: ConnectionStatus;
  secretsNative: boolean;
}) {
  const [secret, setSecretValue] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [confirmClear, setConfirmClear] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  useEffect(() => () => clearTimeout(timer.current), []);

  const run = async (action: () => Promise<string>): Promise<boolean> => {
    try {
      setMessage(await action());
      return true;
    } catch (e) {
      setMessage(errorMessage(e));
      return false;
    }
  };

  const save = (e: React.FormEvent) => {
    e.preventDefault();
    const value = secret;
    if (value === "") return;
    void run(async () => {
      await setSecret(c.id, value);
      return "Saved";
    }).then((ok) => {
      if (ok) setSecretValue("");
    });
  };

  const test = () =>
    run(async () => {
      const who = await testConnection(c.id);
      return `Signed in as ${who.displayName} (${who.uniqueName})`;
    });

  const clear = () => {
    if (!confirmClear) {
      setConfirmClear(true);
      timer.current = setTimeout(() => setConfirmClear(false), CONFIRM_MS);
      return;
    }
    clearTimeout(timer.current);
    setConfirmClear(false);
    void run(async () => {
      await clearSecret(c.id);
      return "Cleared";
    });
  };

  const isAzcli = c.authMethod === "azcli";
  const inputId = `secret-${c.id}`;

  return (
    <section style={section} aria-label={`Connection ${c.id}`}>
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <strong>{c.id}</strong>
        <span style={note}>
          {c.provider} · {c.authMethod}
        </span>
        <span
          data-testid={`auth-state-${c.id}`}
          style={{
            fontSize: 11,
            padding: "0 6px",
            borderRadius: 8,
            color: "#fff",
            background: stateColor[c.authState],
          }}
        >
          {c.authState}
        </span>
      </div>
      {c.identity && (
        <div style={note}>
          {c.identity.displayName} ({c.identity.uniqueName})
        </div>
      )}
      {c.disabledSources.length > 0 && (
        <div style={{ ...note, color: "var(--sev-warn)" }}>
          Disabled sources: {c.disabledSources.join(", ")}
        </div>
      )}
      {isAzcli ? (
        <div style={note}>Uses your az login; no PAT needed</div>
      ) : (
        <form onSubmit={save} style={{ display: "flex", gap: 6 }}>
          <label htmlFor={inputId} style={{ ...note, alignSelf: "center" }}>
            {c.secretLabel}
          </label>
          <input
            id={inputId}
            type="password"
            autoComplete="off"
            spellCheck={false}
            value={secret}
            onChange={(e) => setSecretValue(e.target.value)}
            style={{ flex: 1, minWidth: 0 }}
          />
          <button type="submit" className="btn" disabled={!secretsNative}>
            Save
          </button>
        </form>
      )}
      <div style={{ display: "flex", gap: 6 }}>
        <button type="button" className="btn btn-ghost" onClick={test}>
          Test connection
        </button>
        {!isAzcli && (
          <button type="button" className="btn btn-ghost" onClick={clear}>
            {confirmClear ? "Confirm clear" : "Clear"}
          </button>
        )}
      </div>
      {message && (
        <div role="status" style={note}>
          {message}
        </div>
      )}
    </section>
  );
}

interface Props {
  connections: ConnectionStatus[];
  secretsNative: boolean;
}

export function Connections({ connections, secretsNative }: Props) {
  return (
    <div style={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
      {!secretsNative && (
        <div
          style={{ ...section, color: "var(--sev-warn)", fontSize: 12 }}
          role="alert"
        >
          OS keychain unavailable: tokens cannot be saved
        </div>
      )}
      {connections.length === 0 && (
        <div style={{ ...section, ...note }}>No connections configured</div>
      )}
      {connections.map((c) => (
        <ConnectionCard
          key={c.id}
          connection={c}
          secretsNative={secretsNative}
        />
      ))}
    </div>
  );
}

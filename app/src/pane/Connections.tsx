import { useEffect, useRef, useState, type CSSProperties } from "react";
import {
  clearSecret,
  listConnections,
  removeConnection,
  setSecret,
  testConnection,
  type AuthState,
  type ConnectionStatus,
} from "../api";
import { ConnectionWizard, errorMessage, useEscape } from "./ConnectionWizard";

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

function ConfirmRemove({
  id,
  onCancel,
  onConfirm,
}: {
  id: string;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  useEscape(onCancel);
  const cancelRef = useRef<HTMLButtonElement>(null);
  useEffect(() => cancelRef.current?.focus(), []);
  return (
    <div className="dialog" role="alertdialog" aria-label="Confirm removal">
      <div>
        Remove connection <strong>{id}</strong>? Its stored secret is deleted
        too.
      </div>
      <div className="wizard-actions">
        <button
          ref={cancelRef}
          type="button"
          className="btn btn-ghost"
          onClick={onCancel}
        >
          Cancel
        </button>
        <button type="button" className="btn btn-danger" onClick={onConfirm}>
          Remove
        </button>
      </div>
    </div>
  );
}

function ConnectionCard({
  connection: c,
  secretsNative,
  onEdit,
  onRemove,
}: {
  connection: ConnectionStatus;
  secretsNative: boolean;
  onEdit: () => void;
  onRemove: () => void;
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
      if (ok) setSecretValue((cur) => (cur === value ? "" : cur));
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
        <span style={{ flex: 1 }} />
        <button type="button" className="btn btn-ghost" onClick={onEdit}>
          Edit
        </button>
        <button type="button" className="btn btn-ghost" onClick={onRemove}>
          Remove
        </button>
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
  onBack?: () => void;
  startWizard?: boolean;
}

export function Connections({
  connections,
  secretsNative,
  onBack,
  startWizard = false,
}: Props) {
  const [wizard, setWizard] = useState<{ editId: string | null } | null>(
    startWizard ? { editId: null } : null,
  );
  const [removing, setRemoving] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  // Fresher list fetched after a change; dropped once the parent's list updates.
  const [fresh, setFresh] = useState<{
    base: ConnectionStatus[];
    list: ConnectionStatus[];
  } | null>(null);
  const shown = fresh && fresh.base === connections ? fresh.list : connections;

  const refresh = () =>
    listConnections()
      .then((list) => setFresh({ base: connections, list }))
      .catch((e) => setError(errorMessage(e)));

  const remove = async (id: string) => {
    setRemoving(null);
    try {
      await removeConnection(id);
      setError(null);
      void refresh();
    } catch (e) {
      setError(errorMessage(e));
    }
  };

  if (wizard) {
    return (
      <div style={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
        <ConnectionWizard
          editId={wizard.editId}
          onClose={() => setWizard(null)}
          onSaved={() => {
            setWizard(null);
            void refresh();
          }}
        />
      </div>
    );
  }

  return (
    <div style={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
      {onBack && !removing && (
        <div style={{ padding: "4px 12px" }}>
          <button type="button" className="btn btn-ghost" onClick={onBack}>
            Back
          </button>
        </div>
      )}
      <div className="row">
        <strong>Connections</strong>
        <span style={{ flex: 1 }} />
        <button
          type="button"
          className="btn"
          onClick={() => setWizard({ editId: null })}
        >
          Add connection
        </button>
      </div>
      {error && (
        <div role="alert" style={{ ...section, color: "var(--sev-critical)" }}>
          {error}
        </div>
      )}
      {removing && (
        <ConfirmRemove
          id={removing}
          onCancel={() => setRemoving(null)}
          onConfirm={() => void remove(removing)}
        />
      )}
      {!secretsNative && (
        <div
          style={{ ...section, color: "var(--sev-warn)", fontSize: 12 }}
          role="alert"
        >
          OS keychain unavailable: tokens cannot be saved
        </div>
      )}
      {shown.length === 0 && (
        <div style={{ ...section, ...note }}>
          No connections yet. Add one to start watching.
        </div>
      )}
      {shown.map((c) => (
        <ConnectionCard
          key={c.id}
          connection={c}
          secretsNative={secretsNative}
          onEdit={() => setWizard({ editId: c.id })}
          onRemove={() => setRemoving(c.id)}
        />
      ))}
    </div>
  );
}

import {
  useEffect,
  useRef,
  useState,
  type FormEvent,
  type ReactNode,
} from "react";
import {
  ApiError,
  getConnectionDraft,
  listDraftScopes,
  listProviders,
  saveConnection,
  testDraftConnection,
  type AuthMethodSpec,
  type ConnectionDraft,
  type FieldSpec,
  type ProviderDescriptor,
  type ScopeOption,
} from "../api";

const NAME_PATTERN = /^[a-z0-9-]+$/;
const NAME_MESSAGE = "Use lowercase letters, digits and - only";
const STEPS = ["Provider", "Sign in", "Details"];
const SEARCH_THRESHOLD = 10;

export function errorMessage(e: unknown): string {
  return e instanceof ApiError ? e.message : String(e);
}

export function parseList(text: string): string[] {
  return text
    .split(/[\n,]/)
    .map((s) => s.trim())
    .filter((s) => s !== "");
}

function suggestName(text: string): string {
  return text
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

// Capture phase so the pane's own Esc handler (which hides the window) never sees it.
export function useEscape(onEscape: () => void) {
  const handler = useRef(onEscape);
  useEffect(() => {
    handler.current = onEscape;
  });
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      e.stopPropagation();
      handler.current();
    };
    window.addEventListener("keydown", onKey, true);
    return () => window.removeEventListener("keydown", onKey, true);
  }, []);
}

type ScopeLoad =
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "ready"; options: ScopeOption[] };

const ids = (...parts: (string | false | null | undefined)[]) =>
  parts.filter(Boolean).join(" ") || undefined;

interface ScopePickerProps {
  field: FieldSpec;
  load: ScopeLoad | undefined;
  selected: string[];
  onChange: (next: string[]) => void;
  onRetry: () => void;
}

function ScopePicker({
  field,
  load,
  selected,
  onChange,
  onRetry,
}: ScopePickerProps) {
  const [query, setQuery] = useState("");
  const base = `wiz-f-${field.key}`;
  const fetched = load?.status === "ready" ? load.options : [];
  const known = new Set(fetched.map((o) => o.id));
  // Saved values the server no longer returns stay visible so they are not dropped silently.
  const options: ScopeOption[] = [
    ...fetched,
    ...selected
      .filter((v) => !known.has(v))
      .map((v) => ({ id: v, name: `${v} (not found)` })),
  ];
  const q = query.trim().toLowerCase();
  const shown = q
    ? options.filter((o) => o.name.toLowerCase().includes(q))
    : options;
  const noun = field.label.toLowerCase();

  const toggle = (value: string, on: boolean) =>
    onChange(on ? [...selected, value] : selected.filter((v) => v !== value));

  return (
    <fieldset
      className="field scope"
      aria-describedby={ids(field.help && `${base}-help`, `${base}-summary`)}
    >
      <legend>{field.label}</legend>
      {field.help && (
        <span className="field-help" id={`${base}-help`}>
          {field.help}
        </span>
      )}
      {(!load || load.status === "loading") && (
        <span className="field-help" role="status">
          Loading {noun}...
        </span>
      )}
      {load?.status === "error" && (
        <div className="field-error" role="alert">
          {load.message}{" "}
          <button type="button" className="link" onClick={onRetry}>
            Retry
          </button>
        </div>
      )}
      {options.length > SEARCH_THRESHOLD && (
        <input
          type="text"
          autoComplete="off"
          spellCheck={false}
          aria-label={`Search ${noun}`}
          placeholder={`Search ${noun}`}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
      )}
      {options.length > 0 && (
        <div className="scope-list">
          {shown.map((o) => (
            <label key={o.id}>
              <input
                type="checkbox"
                checked={selected.includes(o.id)}
                onChange={(e) => toggle(o.id, e.target.checked)}
              />
              {o.name}
            </label>
          ))}
        </div>
      )}
      <div className="field-help" id={`${base}-summary`}>
        {selected.length === 0 ? `All ${noun}` : `${selected.length} selected`}
        {selected.length > 0 && (
          <>
            {" "}
            <button type="button" className="link" onClick={() => onChange([])}>
              Select none
            </button>
          </>
        )}
      </div>
    </fieldset>
  );
}

function Labeled({
  field,
  children,
  error,
}: {
  field: FieldSpec;
  children: ReactNode;
  error: string | null;
}) {
  return (
    <div className="field">
      <label htmlFor={`wiz-f-${field.key}`}>
        {field.label}
        {field.required ? " *" : ""}
      </label>
      {children}
      {field.help && (
        <span className="field-help" id={`wiz-f-${field.key}-help`}>
          {field.help}
        </span>
      )}
      {error && (
        <span className="field-error" id={`wiz-f-${field.key}-err`}>
          {error}
        </span>
      )}
    </div>
  );
}

interface Props {
  editId: string | null;
  onClose: () => void;
  onSaved: () => void;
}

export function ConnectionWizard({ editId, onClose, onSaved }: Props) {
  const editing = editId !== null;
  const [providers, setProviders] = useState<ProviderDescriptor[] | null>(null);
  const [provider, setProvider] = useState<ProviderDescriptor | null>(null);
  const [step, setStep] = useState(1);
  const [name, setName] = useState("");
  const [nameTouched, setNameTouched] = useState(editing);
  const [values, setValues] = useState<Record<string, string>>({});
  const [scopes, setScopes] = useState<Record<string, string[]>>({});
  const [loads, setLoads] = useState<Record<string, ScopeLoad>>({});
  // Settings the descriptor does not describe (hand-edited); saved back untouched.
  const [extra, setExtra] = useState<ConnectionDraft["settings"]>({});
  const [bots, setBots] = useState("");
  const [enabled, setEnabled] = useState(true);
  const [authMethod, setAuthMethod] = useState("");
  const [secret, setSecret] = useState("");
  const [signInError, setSignInError] = useState<string | null>(null);
  const [signedInAs, setSignedInAs] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showErrors, setShowErrors] = useState(false);
  const [busy, setBusy] = useState(false);
  const stepRef = useRef<HTMLDivElement>(null);
  const loadSeq = useRef(0);

  useEscape(onClose);

  useEffect(() => {
    stepRef.current
      ?.querySelector<HTMLElement>("input, textarea, button")
      ?.focus();
  }, [step, provider]);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      const list = await listProviders();
      const draft = editId === null ? null : await getConnectionDraft(editId);
      if (cancelled) return;
      setProviders(list);
      if (!draft) return;
      const p = list.find((x) => x.kind === draft.provider) ?? null;
      if (!p) {
        setError(`Unknown provider: ${draft.provider}`);
        return;
      }
      setProvider(p);
      setName(draft.id);
      setEnabled(draft.enabled);
      setBots(draft.bots.join("\n"));
      setAuthMethod(draft.authMethod);
      setExtra(draft.settings);
      const vals: Record<string, string> = {};
      const sel: Record<string, string[]> = {};
      for (const f of p.fields) {
        const v = draft.settings[f.key];
        if (f.kind === "scope") sel[f.key] = Array.isArray(v) ? v : [];
        else vals[f.key] = Array.isArray(v) ? v.join("\n") : (v ?? "");
      }
      setValues(vals);
      setScopes(sel);
      setStep(2);
    };
    load().catch((e) => {
      if (!cancelled) setError(errorMessage(e));
    });
    return () => {
      cancelled = true;
    };
  }, [editId]);

  const pickProvider = (p: ProviderDescriptor) => {
    setProvider(p);
    setAuthMethod(p.authMethods[0]?.id ?? "");
    setBots(p.defaultBots.join("\n"));
    setError(null);
    setStep(2);
  };

  const fields = provider?.fields ?? [];
  const signInFields = fields.filter((f) => f.kind !== "scope" && !f.advanced);
  const scopeFields = fields.filter((f) => f.kind === "scope");
  const advancedFields = fields.filter((f) => f.kind !== "scope" && f.advanced);

  const method: AuthMethodSpec | undefined = provider?.authMethods.find(
    (m) => m.id === authMethod,
  );
  const wantsSecret = Boolean(method?.secret);

  const fieldError = (f: FieldSpec): string | null => {
    const v = values[f.key] ?? "";
    const empty =
      f.kind === "list" ? parseList(v).length === 0 : v.trim() === "";
    return f.required && empty ? `${f.label} is required` : null;
  };
  const signInValid = signInFields.every((f) => !fieldError(f));
  let nameError: string | null = null;
  if (name === "") nameError = "Name is required";
  else if (!NAME_PATTERN.test(name)) nameError = NAME_MESSAGE;

  const buildDraft = (): ConnectionDraft => {
    const settings: ConnectionDraft["settings"] = { ...extra };
    for (const f of fields) {
      if (f.kind === "scope") {
        // Empty list means "all"; sent explicitly so a cleared selection overwrites the saved one.
        settings[f.key] = scopes[f.key] ?? [];
        continue;
      }
      const next =
        f.kind === "list"
          ? parseList(values[f.key] ?? "")
          : (values[f.key] ?? "").trim();
      if (next.length === 0) delete settings[f.key];
      else settings[f.key] = next;
    }
    return {
      id: name,
      provider: provider?.kind ?? "",
      enabled,
      authMethod,
      bots: parseList(bots),
      settings,
    };
  };

  const secretArg = (): string | null =>
    wantsSecret && secret !== "" ? secret : null;

  const loadScope = async (key: string) => {
    const seq = ++loadSeq.current;
    setLoads((l) => ({ ...l, [key]: { status: "loading" } }));
    let next: ScopeLoad;
    try {
      const options = await listDraftScopes(
        buildDraft(),
        secretArg(),
        editId,
        key,
      );
      next = { status: "ready", options };
    } catch (e) {
      next = { status: "error", message: errorMessage(e) };
    }
    if (seq === loadSeq.current) setLoads((l) => ({ ...l, [key]: next }));
  };

  const enterDetails = () => {
    if (!nameTouched) {
      const first = signInFields.find((f) => f.required && f.kind === "text");
      setName(first ? suggestName(values[first.key] ?? "") : "");
    }
    setShowErrors(false);
    setStep(3);
    for (const f of scopeFields) void loadScope(f.key);
  };

  const submitSignIn = async (e: FormEvent) => {
    e.preventDefault();
    setShowErrors(true);
    if (!signInValid) return;
    setBusy(true);
    setSignInError(null);
    try {
      const who = await testDraftConnection(buildDraft(), secretArg(), editId);
      setSignedInAs(`Signed in as ${who.displayName} (${who.uniqueName})`);
      enterDetails();
    } catch (err) {
      setSignInError(errorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  const continueUntested = () => {
    setSignedInAs(null);
    setSignInError(null);
    enterDetails();
  };

  const goBack = () => {
    setError(null);
    setSignInError(null);
    setSignedInAs(null);
    setStep((s) => s - 1);
  };

  const save = async (e: FormEvent) => {
    e.preventDefault();
    setShowErrors(true);
    if (nameError) return;
    setBusy(true);
    setError(null);
    try {
      await saveConnection(buildDraft(), secretArg(), editId);
      setSecret("");
      onSaved();
    } catch (err) {
      setError(errorMessage(err));
      setBusy(false);
    }
  };

  const cancel = () => {
    setSecret("");
    onClose();
  };

  const fieldInput = (f: FieldSpec) => {
    const err = showErrors ? fieldError(f) : null;
    const common = {
      id: `wiz-f-${f.key}`,
      spellCheck: false,
      placeholder: f.placeholder ?? undefined,
      value: values[f.key] ?? "",
      "aria-invalid": err ? true : undefined,
      "aria-describedby": ids(
        f.help && `wiz-f-${f.key}-help`,
        err && `wiz-f-${f.key}-err`,
      ),
    };
    const set = (text: string) => setValues((v) => ({ ...v, [f.key]: text }));
    return (
      <Labeled field={f} error={err} key={f.key}>
        {f.kind === "list" ? (
          <textarea
            {...common}
            rows={3}
            onChange={(e) => set(e.target.value)}
          />
        ) : (
          <input
            {...common}
            type="text"
            autoComplete="off"
            onChange={(e) => set(e.target.value)}
          />
        )}
      </Labeled>
    );
  };

  return (
    <div className="wizard" role="dialog" aria-label="Connection wizard">
      <ol className="wizard-steps">
        {STEPS.map((label, i) => (
          <li
            key={label}
            className={i + 1 === step ? "wizard-step-current" : undefined}
            aria-current={i + 1 === step ? "step" : undefined}
          >
            {i + 1}. {label}
          </li>
        ))}
      </ol>
      {error && (
        <div role="alert" className="field-error">
          {error}
        </div>
      )}

      <div ref={stepRef}>
        {step === 1 && (
          <div className="wizard-cards">
            {providers === null && !error && (
              <span className="field-help">Loading providers</span>
            )}
            {providers?.map((p) => (
              <button
                key={p.kind}
                type="button"
                className="wizard-card"
                onClick={() => pickProvider(p)}
              >
                {p.displayName}
              </button>
            ))}
            <div className="wizard-actions" style={{ flexBasis: "100%" }}>
              <button type="button" className="btn btn-ghost" onClick={cancel}>
                Cancel
              </button>
            </div>
          </div>
        )}

        {step === 2 && provider && (
          <form
            onSubmit={(e) => void submitSignIn(e)}
            className="wizard"
            noValidate
          >
            <div className="field-help">{provider.displayName}</div>
            {signInFields.map(fieldInput)}
            {provider.authMethods.map((m) => (
              <div className="field" key={m.id}>
                <label
                  style={{ display: "flex", gap: 6, alignItems: "center" }}
                >
                  <input
                    type="radio"
                    name="auth-method"
                    value={m.id}
                    checked={m.id === authMethod}
                    aria-describedby={
                      m.help ? `wiz-auth-${m.id}-help` : undefined
                    }
                    onChange={() => {
                      setAuthMethod(m.id);
                      if (!m.secret) setSecret("");
                      setSignInError(null);
                    }}
                  />
                  {m.label}
                </label>
                {m.help && (
                  <span className="field-help" id={`wiz-auth-${m.id}-help`}>
                    {m.help}
                  </span>
                )}
              </div>
            ))}
            {method?.secret && (
              <div className="field">
                <label htmlFor="wiz-secret">{method.secret.label}</label>
                <input
                  id="wiz-secret"
                  type="password"
                  autoComplete="off"
                  spellCheck={false}
                  placeholder={
                    editing
                      ? "Leave empty to keep the stored secret"
                      : undefined
                  }
                  aria-describedby={
                    method.secret.help ? "wiz-secret-help" : undefined
                  }
                  value={secret}
                  onChange={(e) => {
                    setSecret(e.target.value);
                    setSignInError(null);
                  }}
                />
                {method.secret.help && (
                  <span className="field-help" id="wiz-secret-help">
                    {method.secret.help}
                  </span>
                )}
              </div>
            )}
            {signInError && (
              <div role="alert" className="field-error">
                {signInError}
              </div>
            )}
            <div className="wizard-actions">
              <button type="button" className="btn btn-ghost" onClick={cancel}>
                Cancel
              </button>
              {!editing && (
                <button
                  type="button"
                  className="btn btn-ghost"
                  onClick={goBack}
                >
                  Back
                </button>
              )}
              {signInError && (
                <button
                  type="button"
                  className="btn btn-ghost"
                  onClick={continueUntested}
                >
                  Continue without testing
                </button>
              )}
              <button type="submit" className="btn" disabled={busy}>
                {busy ? "Testing..." : "Test and continue"}
              </button>
            </div>
          </form>
        )}

        {step === 3 && provider && (
          <form onSubmit={(e) => void save(e)} className="wizard" noValidate>
            {signedInAs && (
              <div role="status" className="field-help">
                {signedInAs}
              </div>
            )}
            <div className="field">
              <label htmlFor="wiz-name">Name</label>
              <input
                id="wiz-name"
                type="text"
                autoComplete="off"
                spellCheck={false}
                value={name}
                aria-invalid={showErrors && nameError ? true : undefined}
                aria-describedby={ids(
                  "wiz-name-help",
                  showErrors && nameError && "wiz-name-err",
                )}
                onChange={(e) => {
                  setNameTouched(true);
                  setName(e.target.value);
                }}
              />
              <span className="field-help" id="wiz-name-help">
                Used in the config file and hooks
              </span>
              {showErrors && nameError && (
                <span className="field-error" id="wiz-name-err">
                  {nameError}
                </span>
              )}
            </div>
            {scopeFields.map((f) => (
              <ScopePicker
                key={f.key}
                field={f}
                load={loads[f.key]}
                selected={scopes[f.key] ?? []}
                onChange={(next) => setScopes((s) => ({ ...s, [f.key]: next }))}
                onRetry={() => void loadScope(f.key)}
              />
            ))}
            <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <input
                type="checkbox"
                checked={enabled}
                onChange={(e) => setEnabled(e.target.checked)}
              />
              Enabled
            </label>
            <details className="advanced">
              <summary>Advanced</summary>
              <div className="wizard">
                <div className="field">
                  <label htmlFor="wiz-bots">Bots</label>
                  <textarea
                    id="wiz-bots"
                    rows={3}
                    spellCheck={false}
                    aria-describedby="wiz-bots-help"
                    value={bots}
                    onChange={(e) => setBots(e.target.value)}
                  />
                  <span className="field-help" id="wiz-bots-help">
                    {provider.botsHelp}
                  </span>
                </div>
                {advancedFields.map(fieldInput)}
              </div>
            </details>
            <div className="wizard-actions">
              <button type="button" className="btn btn-ghost" onClick={cancel}>
                Cancel
              </button>
              <button type="button" className="btn btn-ghost" onClick={goBack}>
                Back
              </button>
              <button type="submit" className="btn" disabled={busy}>
                Save
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}

import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";

export type Severity = "critical" | "warn" | "success" | "info";
export type ActorKind = "me" | "human" | "bot" | "system";
export type PrRole =
  | "author"
  | "reviewer"
  | "groupReviewer"
  | "commenter"
  | "mentioned";
export type AuthState = "unknown" | "ok" | "missing" | "invalid";

export interface ConnectionRef {
  id: string;
  provider: string;
}

export interface Actor {
  kind: ActorKind;
  id: string;
  displayName: string;
  uniqueName?: string | null;
}

export interface RepoRef {
  id: string;
  name: string;
  namespace?: string | null;
}

export interface PrRef {
  id: number;
  title: string;
  sourceBranch: string;
  targetBranch: string;
  isDraft: boolean;
  role: PrRole;
}

export interface BranchRef {
  name: string;
  shortName: string;
}

export interface RunRef {
  id: string;
  definition: string;
  sourceBranch: string;
  reason: string;
  status: string;
  result: string;
  stage?: string | null;
}

export interface DeploymentRef {
  releaseId: string;
  releaseName: string;
  definition: string;
  environment?: string | null;
  deploymentId?: string | null;
  status?: string | null;
}

export interface CommentRef {
  threadId: string;
  commentId: string;
  excerpt: string;
  threadStatus: string;
}

export interface ApprovalRef {
  id: string;
  kind: string;
  stage: string;
  status: string;
}

export interface LocalPaths {
  repoPath?: string | null;
  worktreePath?: string | null;
}

export interface Event {
  id: string;
  type: string;
  connection: ConnectionRef;
  occurredUtc: string;
  detectedUtc: string;
  inferred: boolean;
  catchUp: boolean;
  test: boolean;
  severity: Severity;
  actor: Actor;
  alert: boolean;
  title: string;
  subtitle?: string | null;
  url?: string | null;
  subjectKey: string;
  repo?: RepoRef | null;
  pr?: PrRef | null;
  branch?: BranchRef | null;
  run?: RunRef | null;
  deployment?: DeploymentRef | null;
  comment?: CommentRef | null;
  approval?: ApprovalRef | null;
  local?: LocalPaths | null;
  data?: unknown;
}

export interface Identity {
  id: string;
  displayName: string;
  uniqueName: string;
  groupIds: string[];
}

export interface ConnectionStatus {
  id: string;
  provider: string;
  authMethod: string;
  authState: AuthState;
  identity: Identity | null;
  secretLabel: string;
  disabledSources: string[];
}

export interface ConfigError {
  path: string;
  message: string;
}

export interface UiState {
  pinned: boolean;
  muted: boolean;
  snoozedUntilUtc: string | null;
  connections: ConnectionStatus[];
  configErrors: ConfigError[];
  secretsNative: boolean;
}

export interface ToastCard {
  cardId: string;
  severity: Severity;
  title: string;
  subtitle: string | null;
  count: number;
  durationMs: number;
  test: boolean;
}

export class ApiError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ApiError";
  }
}

async function call<T>(
  command: string,
  args?: Record<string, unknown>,
): Promise<T> {
  try {
    return await invoke<T>(command, args);
  } catch (e) {
    throw new ApiError(typeof e === "string" ? e : String(e));
  }
}

export const listUnviewed = () => call<Event[]>("list_unviewed");
export const unviewedCount = () => call<number>("unviewed_count");
export const markViewed = (ids: string[]) => call<void>("mark_viewed", { ids });
export const markAllViewed = () => call<void>("mark_all_viewed");
export const activateRecord = (id: string) =>
  call<void>("activate_record", { id });
export const getUiState = () => call<UiState>("get_ui_state");
export const setPinned = (pinned: boolean) =>
  call<void>("set_pinned", { pinned });
export const listConnections = () =>
  call<ConnectionStatus[]>("list_connections");
export const setSecret = (connectionId: string, secret: string) =>
  call<void>("set_secret", { connectionId, secret });
export const clearSecret = (connectionId: string) =>
  call<void>("clear_secret", { connectionId });
export const testConnection = (connectionId: string) =>
  call<Identity>("test_connection", { connectionId });
export const toastClicked = () => call<void>("toast_clicked");
export const toastHover = (hovering: boolean) =>
  call<void>("toast_hover", { hovering });
export const toastResize = (heightLogical: number) =>
  call<void>("toast_resize", { heightLogical });
export const hidePane = () => call<void>("hide_pane");

export const EVENT_UNVIEWED_CHANGED = "herald://unviewed-changed";
export const EVENT_UI_STATE = "herald://ui-state";
export const EVENT_TOASTS = "herald://toasts";

export const onUnviewedChanged = (
  cb: (n: number) => void,
): Promise<UnlistenFn> =>
  listen<number>(EVENT_UNVIEWED_CHANGED, (e) => cb(e.payload));
export const onUiState = (cb: (state: UiState) => void): Promise<UnlistenFn> =>
  listen<UiState>(EVENT_UI_STATE, (e) => cb(e.payload));
export const onToasts = (
  cb: (cards: ToastCard[]) => void,
): Promise<UnlistenFn> =>
  listen<ToastCard[]>(EVENT_TOASTS, (e) => cb(e.payload));

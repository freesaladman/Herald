import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { emit } from "@tauri-apps/api/event";
import { clearMocks, mockIPC } from "@tauri-apps/api/mocks";
import {
  EVENT_UNVIEWED_CHANGED,
  type ConnectionStatus,
  type Event,
  type UiState,
} from "../api";
import { Pane } from "./Pane";
import { relativeTime } from "./RecordRow";

function makeEvent(
  id: string,
  title: string,
  occurredUtc: string,
  level: Event["level"] = "alert",
): Event {
  return {
    id,
    type: "pr.created",
    connection: { id: "c", provider: "ado" },
    occurredUtc,
    detectedUtc: occurredUtc,
    inferred: false,
    catchUp: false,
    test: false,
    severity: "info",
    actor: { kind: "human", id: "a", displayName: "Ann" },
    alert: level === "alert",
    level,
    viaGroup: false,
    title,
    subjectKey: "k",
  };
}

function makeConnection(over: Partial<ConnectionStatus>): ConnectionStatus {
  return {
    id: "conn-a",
    provider: "ado",
    authMethod: "pat",
    authState: "ok",
    identity: null,
    secretLabel: "Personal access token",
    disabledSources: [],
    ...over,
  };
}

function makeUi(over: Partial<UiState> = {}): UiState {
  return {
    pinned: false,
    muted: false,
    snoozedUntilUtc: null,
    connections: [makeConnection({})],
    configErrors: [],
    secretsNative: true,
    ...over,
  };
}

function setup(
  events: Event[],
  ui: UiState = makeUi(),
  count = events.length,
) {
  const calls: { cmd: string; args: unknown }[] = [];
  mockIPC(
    (cmd, args) => {
      calls.push({ cmd, args });
      if (cmd === "list_unviewed") return events;
      if (cmd === "get_ui_state") return ui;
      if (cmd === "unviewed_count") return count;
      return null;
    },
    { shouldMockEvents: true },
  );
  return calls;
}

afterEach(() => {
  cleanup();
  clearMocks();
});

describe("Pane", () => {
  it("renders rows newest first", async () => {
    setup([
      makeEvent("old", "Older", "2026-01-01T00:00:00Z"),
      makeEvent("new", "Newer", "2026-01-02T00:00:00Z"),
    ]);
    render(<Pane />);
    const rows = await screen.findAllByTestId("record-row");
    expect(rows[0]).toHaveTextContent("Newer");
    expect(rows[1]).toHaveTextContent("Older");
  });

  it("badge shows the unviewed count, not the list length", async () => {
    setup(
      [
        makeEvent("a", "Alert row", "2026-01-02T00:00:00Z"),
        makeEvent("p", "Pane row", "2026-01-01T00:00:00Z", "pane"),
      ],
      makeUi(),
      1,
    );
    render(<Pane />);
    expect(await screen.findAllByTestId("record-row")).toHaveLength(2);
    await waitFor(() =>
      expect(screen.getByLabelText("Unviewed count")).toHaveTextContent("1"),
    );
  });

  it("badge follows the unviewed-changed payload", async () => {
    setup([
      makeEvent("a", "Alert row", "2026-01-02T00:00:00Z"),
      makeEvent("p", "Pane row", "2026-01-01T00:00:00Z", "pane"),
    ]);
    render(<Pane />);
    expect(await screen.findAllByTestId("record-row")).toHaveLength(2);
    await waitFor(async () => {
      await emit(EVENT_UNVIEWED_CHANGED, 7);
      expect(screen.getByLabelText("Unviewed count")).toHaveTextContent("7");
    });
    expect(await screen.findAllByTestId("record-row")).toHaveLength(2);
  });

  it("shows the empty state", async () => {
    setup([]);
    render(<Pane />);
    expect(await screen.findByText("All caught up")).toBeInTheDocument();
  });

  it("activates on row click and only dismisses on x", async () => {
    const calls = setup([makeEvent("e1", "Hello", "2026-01-01T00:00:00Z")]);
    render(<Pane />);
    const user = userEvent.setup();

    await user.click(
      await screen.findByRole("button", { name: "Dismiss Hello" }),
    );
    await waitFor(() =>
      expect(calls).toContainEqual({
        cmd: "mark_viewed",
        args: { ids: ["e1"] },
      }),
    );
    expect(calls.some((c) => c.cmd === "activate_record")).toBe(false);

    await user.click(screen.getByTestId("record-row"));
    await waitFor(() =>
      expect(calls).toContainEqual({
        cmd: "activate_record",
        args: { id: "e1" },
      }),
    );
  });

  it("marks all viewed", async () => {
    const calls = setup([]);
    render(<Pane />);
    await userEvent.click(
      await screen.findByRole("button", { name: "Mark all viewed" }),
    );
    await waitFor(() =>
      expect(calls.some((c) => c.cmd === "mark_all_viewed")).toBe(true),
    );
  });

  it("words auth banners for Azure CLI sign-in", async () => {
    setup(
      [],
      makeUi({
        connections: [
          makeConnection({ id: "a", authMethod: "azcli", authState: "missing" }),
          makeConnection({ id: "b", authMethod: "azcli", authState: "invalid" }),
        ],
      }),
    );
    render(<Pane />);
    expect(
      await screen.findByText("Sign in to the Azure CLI (az login) for a, then choose Poll now"),
    ).toBeInTheDocument();
    expect(screen.getByText("Sign-in for b was rejected")).toBeInTheDocument();
  });

  it("renders auth, config error and no-connection banners", async () => {
    setup(
      [],
      makeUi({
        connections: [
          makeConnection({ id: "a", authState: "missing" }),
          makeConnection({ id: "b", authState: "invalid" }),
        ],
        configErrors: [{ path: "hooks[0]", message: "bad glob" }],
      }),
    );
    const { unmount } = render(<Pane />);
    expect(await screen.findByText("Add a PAT for a")).toBeInTheDocument();
    expect(screen.getByText("PAT for b was rejected")).toBeInTheDocument();
    expect(screen.getByText(/1 config error.*bad glob/)).toBeInTheDocument();
    unmount();

    setup([], makeUi({ connections: [] }));
    render(<Pane />);
    expect(await screen.findByText("No connections yet.")).toBeInTheDocument();
  });

  it("opens the connection wizard from the no-connections banner", async () => {
    setup([], makeUi({ connections: [] }));
    render(<Pane />);
    await screen.findByText("No connections yet.");
    await userEvent.click(
      screen.getByRole("button", { name: "Add connection" }),
    );
    expect(
      await screen.findByRole("button", { name: "Cancel" }),
    ).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Back" })).toBeNull();
    expect(screen.queryByText("No connections yet.")).toBeNull();
  });

  it("renders ADO-controlled titles as literal text", async () => {
    const title = "<img src=x onerror=alert(1)>";
    setup([makeEvent("x", title, "2026-01-01T00:00:00Z")]);
    const { container } = render(<Pane />);
    expect(await screen.findByText(title)).toBeInTheDocument();
    expect(container.querySelector("img")).toBeNull();
  });
});

describe("relativeTime", () => {
  const now = new Date(2026, 5, 15, 12, 0, 0).getTime();
  const ago = (ms: number) => new Date(now - ms).toISOString();

  it.each([
    [10_000, "now"],
    [2 * 60_000, "2m"],
    [3 * 3_600_000, "3h"],
  ])("formats %i ms ago as %s", (ms, expected) => {
    expect(relativeTime(ago(ms), now)).toBe(expected);
  });

  it("uses yesterday and day counts by calendar date", () => {
    expect(relativeTime(new Date(2026, 5, 14, 9).toISOString(), now)).toBe(
      "yesterday",
    );
    expect(relativeTime(new Date(2026, 5, 10, 9).toISOString(), now)).toBe(
      "5d",
    );
  });
});

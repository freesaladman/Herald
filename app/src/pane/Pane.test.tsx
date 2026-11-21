import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { clearMocks, mockIPC } from "@tauri-apps/api/mocks";
import type { ConnectionStatus, Event, UiState } from "../api";
import { Pane } from "./Pane";
import { relativeTime } from "./RecordRow";

function makeEvent(id: string, title: string, occurredUtc: string): Event {
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
    alert: true,
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

function setup(events: Event[], ui: UiState = makeUi()) {
  const calls: { cmd: string; args: unknown }[] = [];
  mockIPC(
    (cmd, args) => {
      calls.push({ cmd, args });
      if (cmd === "list_unviewed") return events;
      if (cmd === "get_ui_state") return ui;
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
    expect(
      await screen.findByText("No connections configured: edit config.toml"),
    ).toBeInTheDocument();
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

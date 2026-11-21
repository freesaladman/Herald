import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { clearMocks, mockIPC } from "@tauri-apps/api/mocks";
import { emit } from "@tauri-apps/api/event";
import { EVENT_TOASTS, type ToastCard } from "../api";
import { Toast } from "./Toast";

function card(over: Partial<ToastCard>): ToastCard {
  return {
    cardId: "1",
    severity: "info",
    title: "Title",
    subtitle: null,
    count: 1,
    durationMs: 5000,
    test: false,
    ...over,
  };
}

let calls: { cmd: string; args: unknown }[];

beforeEach(() => {
  calls = [];
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      disconnect() {}
    },
  );
  mockIPC(
    (cmd, args) => {
      calls.push({ cmd, args });
    },
    { shouldMockEvents: true },
  );
});

afterEach(async () => {
  cleanup();
  await Promise.resolve();
  clearMocks();
  vi.unstubAllGlobals();
});

async function show(cards: ToastCard[]) {
  render(<Toast />);
  await act(async () => {});
  await act(async () => {
    await emit(EVENT_TOASTS, cards);
  });
}

describe("Toast", () => {
  it("renders titles as literal text with count pill and TEST tag", async () => {
    await show([
      card({ title: "<b>bold</b>", subtitle: "repo", count: 3, test: true }),
    ]);
    expect(await screen.findByText("<b>bold</b>")).toBeInTheDocument();
    expect(screen.getByText("repo")).toBeInTheDocument();
    expect(screen.getByText("3")).toBeInTheDocument();
    expect(screen.getByText("TEST")).toBeInTheDocument();
  });

  it("invokes toast_clicked when a card is clicked", async () => {
    await show([card({})]);
    await userEvent.click(await screen.findByTestId("toast-card"));
    expect(calls.some((c) => c.cmd === "toast_clicked")).toBe(true);
  });

  it("invokes toast_hover on enter and leave", async () => {
    await show([card({})]);
    const el = await screen.findByTestId("toast-card");
    await userEvent.hover(el);
    await userEvent.unhover(el);
    const hovers = calls.filter((c) => c.cmd === "toast_hover");
    expect(hovers.map((c) => c.args)).toEqual([
      { hovering: true },
      { hovering: false },
    ]);
  });

  it("reports height 0 when there are no cards", async () => {
    await show([]);
    expect(calls).toContainEqual({
      cmd: "toast_resize",
      args: { heightLogical: 0 },
    });
  });
});

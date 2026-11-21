import { afterEach, describe, expect, it } from "vitest";
import { clearMocks, mockIPC } from "@tauri-apps/api/mocks";
import * as api from "./api";

interface Call {
  cmd: string;
  args: unknown;
}

function record(result: unknown = null): Call[] {
  const calls: Call[] = [];
  mockIPC((cmd, args) => {
    calls.push({ cmd, args });
    return result;
  });
  return calls;
}

afterEach(() => clearMocks());

describe("command wrappers", () => {
  it.each<[string, () => Promise<unknown>, string, unknown]>([
    ["listUnviewed", () => api.listUnviewed(), "list_unviewed", undefined],
    ["unviewedCount", () => api.unviewedCount(), "unviewed_count", undefined],
    ["markViewed", () => api.markViewed(["a"]), "mark_viewed", { ids: ["a"] }],
    ["markAllViewed", () => api.markAllViewed(), "mark_all_viewed", undefined],
    [
      "activateRecord",
      () => api.activateRecord("a"),
      "activate_record",
      { id: "a" },
    ],
    ["getUiState", () => api.getUiState(), "get_ui_state", undefined],
    ["setPinned", () => api.setPinned(true), "set_pinned", { pinned: true }],
    [
      "listConnections",
      () => api.listConnections(),
      "list_connections",
      undefined,
    ],
    [
      "setSecret",
      () => api.setSecret("c", "s"),
      "set_secret",
      { connectionId: "c", secret: "s" },
    ],
    [
      "clearSecret",
      () => api.clearSecret("c"),
      "clear_secret",
      { connectionId: "c" },
    ],
    [
      "testConnection",
      () => api.testConnection("c"),
      "test_connection",
      { connectionId: "c" },
    ],
    ["toastClicked", () => api.toastClicked(), "toast_clicked", undefined],
    [
      "toastHover",
      () => api.toastHover(true),
      "toast_hover",
      { hovering: true },
    ],
    [
      "toastResize",
      () => api.toastResize(80),
      "toast_resize",
      { heightLogical: 80 },
    ],
    ["hidePane", () => api.hidePane(), "hide_pane", undefined],
  ])("%s invokes the right command and args", async (_n, run, cmd, args) => {
    const calls = record();
    await run();
    expect(calls).toHaveLength(1);
    expect(calls[0].cmd).toBe(cmd);
    expect(calls[0].args ?? undefined).toEqual(args ?? {});
  });

  it("returns the command result", async () => {
    record(3);
    expect(await api.unviewedCount()).toBe(3);
  });

  it("turns a string rejection into an ApiError", async () => {
    mockIPC(() => {
      throw "boom";
    });
    const err = await api.hidePane().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(api.ApiError);
    expect((err as api.ApiError).message).toBe("boom");
  });
});

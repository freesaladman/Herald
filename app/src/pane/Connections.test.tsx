import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { clearMocks, mockIPC } from "@tauri-apps/api/mocks";
import type { ConnectionStatus } from "../api";
import { Connections } from "./Connections";

function conn(over: Partial<ConnectionStatus> = {}): ConnectionStatus {
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

function setup(
  handler: (cmd: string, args: unknown) => unknown,
  c = conn(),
  secretsNative = true,
) {
  const calls: { cmd: string; args: unknown }[] = [];
  mockIPC((cmd, args) => {
    calls.push({ cmd, args });
    return handler(cmd, args);
  });
  render(<Connections connections={[c]} secretsNative={secretsNative} />);
  return calls;
}

afterEach(() => {
  cleanup();
  clearMocks();
  vi.useRealTimers();
  localStorage.clear();
  sessionStorage.clear();
});

describe("Connections", () => {
  it("saves the secret, clears the input and never touches web storage", async () => {
    const calls = setup(() => null);
    const input = screen.getByLabelText("Personal access token");
    await userEvent.type(input, "s3cret");
    await userEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(await screen.findByText("Saved")).toBeTruthy();
    expect(calls.find((c) => c.cmd === "set_secret")?.args).toEqual({
      connectionId: "conn-a",
      secret: "s3cret",
    });
    expect((input as HTMLInputElement).value).toBe("");
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
  });

  it("keeps the typed secret and shows the error when saving fails", async () => {
    setup((cmd) => {
      if (cmd === "set_secret") throw "keychain locked";
      return null;
    });
    const input = screen.getByLabelText("Personal access token");
    await userEvent.type(input, "s3cret");
    await userEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(await screen.findByText("keychain locked")).toBeTruthy();
    expect((input as HTMLInputElement).value).toBe("s3cret");
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
  });

  it("keeps text typed while the save is in flight", async () => {
    let release: () => void = () => {};
    const gate = new Promise<void>((r) => {
      release = r;
    });
    setup(async (cmd) => {
      if (cmd === "set_secret") await gate;
      return null;
    });
    const input = screen.getByLabelText("Personal access token");
    await userEvent.type(input, "old");
    await userEvent.click(screen.getByRole("button", { name: "Save" }));
    await userEvent.type(input, "new");
    release();

    expect(await screen.findByText("Saved")).toBeTruthy();
    expect((input as HTMLInputElement).value).toBe("oldnew");
  });

  it("shows the identity on a successful test", async () => {
    setup((cmd) =>
      cmd === "test_connection"
        ? { id: "1", displayName: "Ann", uniqueName: "ann@x.com", groupIds: [] }
        : null,
    );
    await userEvent.click(
      screen.getByRole("button", { name: "Test connection" }),
    );
    expect(
      await screen.findByText("Signed in as Ann (ann@x.com)"),
    ).toBeTruthy();
  });

  it("shows the ApiError message", async () => {
    setup((cmd) => {
      if (cmd === "test_connection") throw "unauthorized";
      return null;
    });
    await userEvent.click(
      screen.getByRole("button", { name: "Test connection" }),
    );
    expect(await screen.findByText("unauthorized")).toBeTruthy();
  });

  it("hides the PAT form for azcli", () => {
    setup(() => null, conn({ authMethod: "azcli" }));
    expect(screen.getByText("Uses your az login; no PAT needed")).toBeTruthy();
    expect(screen.queryByLabelText("Personal access token")).toBeNull();
    expect(screen.queryByRole("button", { name: "Save" })).toBeNull();
  });

  it("disables Save when the keychain is unavailable", () => {
    setup(() => null, conn(), false);
    expect(
      (screen.getByRole("button", { name: "Save" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
  });

  it("requires a second click to clear", async () => {
    const calls = setup(() => null);
    await userEvent.click(screen.getByRole("button", { name: "Clear" }));
    expect(calls.some((c) => c.cmd === "clear_secret")).toBe(false);
    await userEvent.click(
      screen.getByRole("button", { name: "Confirm clear" }),
    );
    expect(await screen.findByText("Cleared")).toBeTruthy();
    expect(calls.find((c) => c.cmd === "clear_secret")?.args).toEqual({
      connectionId: "conn-a",
    });
  });
});

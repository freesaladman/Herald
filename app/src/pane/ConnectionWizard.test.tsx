import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { clearMocks, mockIPC } from "@tauri-apps/api/mocks";
import type {
  ConnectionDraft,
  ConnectionStatus,
  ProviderDescriptor,
  ScopeOption,
} from "../api";
import { Connections } from "./Connections";

const ado: ProviderDescriptor = {
  kind: "ado",
  displayName: "Azure DevOps",
  fields: [
    {
      key: "organization",
      label: "Organization",
      kind: "text",
      required: true,
      placeholder: "my-org",
      help: "The name after dev.azure.com/",
    },
    {
      key: "projects",
      label: "Projects",
      kind: "scope",
      required: false,
      help: "Projects to watch.",
    },
  ],
  authMethods: [
    {
      id: "pat",
      label: "Personal access token",
      secret: { label: "Token", help: "Scopes: Code (Read)" },
    },
    { id: "azcli", label: "Azure CLI login", help: "Nothing is stored" },
  ],
  defaultBots: ["*Build Service*"],
  botsHelp: "Activity from these accounts won't alert you.",
};

const other: ProviderDescriptor = {
  kind: "other",
  displayName: "Other Host",
  fields: [],
  authMethods: [],
  defaultBots: [],
  botsHelp: "",
};

const who = {
  id: "1",
  displayName: "Ann",
  uniqueName: "ann@x.com",
  groupIds: [],
};

const existing: ConnectionStatus = {
  id: "work",
  provider: "ado",
  authMethod: "pat",
  authState: "ok",
  identity: null,
  secretLabel: "Token",
  disabledSources: [],
};

const existingDraft: ConnectionDraft = {
  id: "work",
  provider: "ado",
  enabled: false,
  authMethod: "pat",
  bots: ["bot*"],
  settings: {
    organization: "acme",
    projects: ["alpha", "gone"],
    custom_flag: "keep-me",
  },
};

const scopeOptions: ScopeOption[] = [
  { id: "alpha", name: "alpha" },
  { id: "beta", name: "beta" },
];

type Call = { cmd: string; args: Record<string, unknown> };

function setup(
  over: (cmd: string, args: Record<string, unknown>) => unknown = () =>
    undefined,
  connections: ConnectionStatus[] = [],
) {
  const calls: Call[] = [];
  mockIPC((cmd, payload) => {
    const args = (payload ?? {}) as Record<string, unknown>;
    calls.push({ cmd, args });
    const custom = over(cmd, args);
    if (custom !== undefined) return custom;
    if (cmd === "list_providers") return [ado, other];
    if (cmd === "get_connection_draft") return existingDraft;
    if (cmd === "list_connections") return connections;
    if (cmd === "test_draft_connection") return who;
    if (cmd === "list_draft_scopes") return scopeOptions;
    return null;
  });
  render(<Connections connections={connections} secretsNative={true} />);
  return calls;
}

const click = (name: string) =>
  userEvent.click(screen.getByRole("button", { name }));
const find = (cmd: string, calls: Call[]) => calls.find((c) => c.cmd === cmd);

async function toSignIn() {
  await click("Add connection");
  await userEvent.click(
    await screen.findByRole("button", { name: "Azure DevOps" }),
  );
}

async function toDetails(org = "My Org") {
  await toSignIn();
  await userEvent.type(screen.getByLabelText(/Organization/), org);
  await userEvent.type(screen.getByLabelText("Token"), "s3cret");
  await click("Test and continue");
  await screen.findByLabelText("Name");
}

afterEach(() => {
  cleanup();
  clearMocks();
  localStorage.clear();
  sessionStorage.clear();
  vi.restoreAllMocks();
});

describe("ConnectionWizard sign-in step", () => {
  it("lists every provider on step 1, even a single one", async () => {
    setup();
    await click("Add connection");
    expect(
      await screen.findByRole("button", { name: "Azure DevOps" }),
    ).toBeTruthy();
    expect(screen.getByRole("button", { name: "Other Host" })).toBeTruthy();
  });

  it("tests the draft with originalId and moves on, hiding scope and advanced fields", async () => {
    const calls = setup();
    await toSignIn();
    expect(screen.queryByLabelText("Projects")).toBeNull();
    expect(screen.queryByLabelText("Bots")).toBeNull();
    await userEvent.type(screen.getByLabelText(/Organization/), "My Org");
    await userEvent.type(screen.getByLabelText("Token"), "s3cret");
    await click("Test and continue");

    expect(await screen.findByText(/Signed in as Ann/)).toBeTruthy();
    expect(find("test_draft_connection", calls)?.args).toEqual({
      draft: {
        id: "",
        provider: "ado",
        enabled: true,
        authMethod: "pat",
        bots: ["*Build Service*"],
        settings: { organization: "My Org", projects: [] },
      },
      secret: "s3cret",
      originalId: null,
    });
  });

  it("offers Continue without testing after a failed test", async () => {
    setup((cmd) => {
      if (cmd === "test_draft_connection") throw "401 unauthorized";
      return undefined;
    });
    await toSignIn();
    await userEvent.type(screen.getByLabelText(/Organization/), "acme");
    await click("Test and continue");
    expect((await screen.findByRole("alert")).textContent).toBe(
      "401 unauthorized",
    );
    await click("Continue without testing");
    expect(await screen.findByLabelText("Name")).toBeTruthy();
    expect(screen.queryByText(/Signed in as/)).toBeNull();
  });

  it("sends no secret for a method without one, even after typing one", async () => {
    const calls = setup();
    await toSignIn();
    await userEvent.type(screen.getByLabelText(/Organization/), "acme");
    await userEvent.type(screen.getByLabelText("Token"), "s3cret");
    await userEvent.click(screen.getByLabelText("Azure CLI login"));
    expect(screen.queryByLabelText("Token")).toBeNull();
    await click("Test and continue");
    await screen.findByLabelText("Name");
    expect(find("test_draft_connection", calls)?.args.secret).toBeNull();
  });

  it("exposes step, error and help semantics for assistive tech", async () => {
    setup();
    await toSignIn();
    expect(screen.getByText(/1\. Provider/).closest("ol")).not.toBeNull();
    expect(screen.getByText(/2\. Sign in/).getAttribute("aria-current")).toBe(
      "step",
    );
    const org = screen.getByLabelText(/Organization/);
    expect(document.activeElement).toBe(org);
    expect(screen.getByLabelText("Token").getAttribute("type")).toBe(
      "password",
    );

    await click("Test and continue");
    expect(org.getAttribute("aria-invalid")).toBe("true");
    const described = (org.getAttribute("aria-describedby") ?? "").split(" ");
    expect(described).toHaveLength(2);
    for (const id of described) {
      expect(document.getElementById(id)).not.toBeNull();
    }
    expect(screen.getByText("Organization is required")).toBeTruthy();
  });
});

describe("ConnectionWizard details step", () => {
  it("suggests the name from the organization and validates the pattern", async () => {
    setup();
    await toDetails("My_Org");
    const name = screen.getByLabelText("Name") as HTMLInputElement;
    expect(name.value).toBe("my-org");
    expect(screen.getByText("Used in the config file and hooks")).toBeTruthy();

    for (const bad of ["Upper", "under_score"]) {
      await userEvent.clear(name);
      await userEvent.type(name, bad);
      await click("Save");
      expect(
        screen.getByText("Use lowercase letters, digits and - only"),
      ).toBeTruthy();
      expect(name.getAttribute("aria-invalid")).toBe("true");
    }
  });

  it("loads scopes, filters by search, and saves ticked values", async () => {
    const many = Array.from({ length: 12 }, (_, i) => ({
      id: `proj${i}`,
      name: `Proj ${i}`,
    }));
    const calls = setup((cmd) =>
      cmd === "list_draft_scopes" ? many : undefined,
    );
    await toDetails();
    expect(find("list_draft_scopes", calls)?.args).toMatchObject({
      fieldKey: "projects",
      originalId: null,
      secret: "s3cret",
    });
    await screen.findByLabelText("Proj 0");
    expect(screen.getByText("All projects")).toBeTruthy();

    await userEvent.type(screen.getByLabelText("Search projects"), "proj 1");
    expect(screen.queryByLabelText("Proj 2")).toBeNull();
    expect(screen.getByLabelText("Proj 10")).toBeTruthy();

    await userEvent.click(screen.getByLabelText("Proj 10"));
    await userEvent.click(screen.getByLabelText("Proj 1"));
    expect(screen.getByText("2 selected")).toBeTruthy();
    await click("Save");
    await vi.waitFor(() => expect(find("save_connection", calls)).toBeTruthy());
    const draft = find("save_connection", calls)?.args.draft as ConnectionDraft;
    expect(draft.settings.projects).toEqual(["proj10", "proj1"]);
    expect(draft.id).toBe("my-org");
  });

  it("saves an empty scope list when nothing is ticked, and has no search for short lists", async () => {
    const calls = setup();
    await toDetails();
    await userEvent.click(await screen.findByLabelText("alpha"));
    await click("Select none");
    expect(screen.queryByLabelText("Search projects")).toBeNull();
    await click("Save");
    await vi.waitFor(() => expect(find("save_connection", calls)).toBeTruthy());
    const draft = find("save_connection", calls)?.args.draft as ConnectionDraft;
    expect(draft.settings.projects).toEqual([]);
  });

  it("shows a scope load error and retries", async () => {
    let fail = true;
    setup((cmd) => {
      if (cmd !== "list_draft_scopes") return undefined;
      if (fail) throw "boom";
      return scopeOptions;
    });
    await toDetails();
    expect((await screen.findByRole("alert")).textContent).toContain("boom");
    fail = false;
    await click("Retry");
    expect(await screen.findByLabelText("beta")).toBeTruthy();
  });

  it("prefills default bots for a new connection inside a collapsed Advanced section", async () => {
    setup();
    await toDetails();
    const details = screen
      .getByText("Advanced")
      .closest("details") as HTMLDetailsElement;
    expect(details.open).toBe(false);
    const bots = screen.getByLabelText("Bots") as HTMLTextAreaElement;
    expect(bots.value).toBe("*Build Service*");
    expect(
      screen.getByText("Activity from these accounts won't alert you."),
    ).toBeTruthy();
  });

  it("saves a new connection, clears the secret and closes the wizard", async () => {
    const calls = setup();
    await toDetails();
    await click("Save");
    expect(
      await screen.findByRole("button", { name: "Add connection" }),
    ).toBeTruthy();
    const save = find("save_connection", calls);
    expect(save?.args.secret).toBe("s3cret");
    expect(save?.args.originalId).toBeNull();
    expect(screen.queryByDisplayValue("s3cret")).toBeNull();
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
  });

  it("shows a server-side save error and stays on the step", async () => {
    setup((cmd) => {
      if (cmd === "save_connection") throw "duplicate id";
      return undefined;
    });
    await toDetails();
    await click("Save");
    expect(await screen.findByText("duplicate id")).toBeTruthy();
    expect(screen.getByRole("button", { name: "Save" })).toBeTruthy();
  });
});

describe("Connections navigation", () => {
  it("shows the view Back button only while no wizard or confirm is open", async () => {
    setup(() => undefined, [existing]);
    cleanup();
    const onBack = vi.fn();
    render(
      <Connections
        connections={[existing]}
        secretsNative={true}
        onBack={onBack}
      />,
    );
    const back = () => screen.queryAllByRole("button", { name: "Back" });
    expect(back()).toHaveLength(1);

    await click("Remove");
    expect(back()).toHaveLength(0);
    await userEvent.click(
      within(screen.getByRole("alertdialog")).getByRole("button", {
        name: "Cancel",
      }),
    );
    expect(back()).toHaveLength(1);

    await click("Add connection");
    await userEvent.click(
      await screen.findByRole("button", { name: "Azure DevOps" }),
    );
    expect(back()).toHaveLength(1);
    await click("Back");
    expect(onBack).not.toHaveBeenCalled();
    expect(
      await screen.findByRole("button", { name: "Azure DevOps" }),
    ).toBeTruthy();
    expect(back()).toHaveLength(0);
  });

  it("opens straight at step 1 when startWizard is set", async () => {
    setup(() => undefined);
    cleanup();
    render(<Connections connections={[]} secretsNative={true} startWizard />);
    expect(
      await screen.findByRole("button", { name: "Azure DevOps" }),
    ).toBeTruthy();
  });

  it("shows the friendly empty note", () => {
    setup(() => undefined);
    expect(
      screen.getByText("No connections yet. Add one to start watching."),
    ).toBeTruthy();
  });
});

describe("ConnectionWizard edit", () => {
  it("opens at sign-in prefilled and tests with originalId", async () => {
    const calls = setup(() => undefined, [existing]);
    await click("Edit");
    expect(
      ((await screen.findByLabelText(/Organization/)) as HTMLInputElement)
        .value,
    ).toBe("acme");
    expect(
      (screen.getByLabelText("Token") as HTMLInputElement).placeholder,
    ).toBe("Leave empty to keep the stored secret");
    expect(screen.queryByRole("button", { name: "Back" })).toBeNull();

    await click("Test and continue");
    await screen.findByLabelText("Name");
    expect(find("test_draft_connection", calls)?.args).toMatchObject({
      secret: null,
      originalId: "work",
    });
  });

  it("pre-ticks saved scopes, marks missing ones, keeps bots and hand-added settings", async () => {
    const calls = setup(() => undefined, [existing]);
    await click("Edit");
    await screen.findByLabelText(/Organization/);
    await click("Test and continue");

    expect(
      ((await screen.findByLabelText("alpha")) as HTMLInputElement).checked,
    ).toBe(true);
    expect((screen.getByLabelText("beta") as HTMLInputElement).checked).toBe(
      false,
    );
    expect(
      (screen.getByLabelText("gone (not found)") as HTMLInputElement).checked,
    ).toBe(true);
    expect((screen.getByLabelText("Name") as HTMLInputElement).value).toBe(
      "work",
    );
    expect((screen.getByLabelText("Bots") as HTMLTextAreaElement).value).toBe(
      "bot*",
    );
    expect((screen.getByLabelText("Enabled") as HTMLInputElement).checked).toBe(
      false,
    );

    await click("Save");
    await vi.waitFor(() => expect(find("save_connection", calls)).toBeTruthy());
    const save = find("save_connection", calls);
    expect(save?.args.originalId).toBe("work");
    expect(save?.args.secret).toBeNull();
    expect(save?.args.draft).toEqual(existingDraft);
  });
});

describe("ConnectionWizard dialogs", () => {
  it("asks for confirmation before removing and focuses Cancel", async () => {
    const calls = setup(() => undefined, [existing]);
    await userEvent.click(screen.getByRole("button", { name: "Remove" }));
    const dialog = screen.getByRole("alertdialog");
    expect(dialog.textContent).toContain("work");
    expect(document.activeElement).toBe(
      within(dialog).getByRole("button", { name: "Cancel" }),
    );
    expect(find("remove_connection", calls)).toBeUndefined();

    await userEvent.click(
      within(dialog).getByRole("button", { name: "Remove" }),
    );
    await vi.waitFor(() =>
      expect(find("remove_connection", calls)?.args).toEqual({ id: "work" }),
    );
  });

  it("Esc closes the wizard without reaching the pane handler", async () => {
    setup();
    const paneHandler = vi.fn();
    window.addEventListener("keydown", paneHandler);
    await click("Add connection");
    await screen.findByRole("button", { name: "Azure DevOps" });
    await userEvent.keyboard("{Escape}");
    window.removeEventListener("keydown", paneHandler);

    expect(screen.queryByRole("dialog")).toBeNull();
    expect(paneHandler).not.toHaveBeenCalled();
  });
});

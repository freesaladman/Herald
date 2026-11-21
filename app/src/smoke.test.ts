import { describe, expect, it } from "vitest";

describe("test tooling", () => {
  it("runs under jsdom with jest-dom matchers", () => {
    const el = document.createElement("div");
    document.body.appendChild(el);
    expect(el).toBeInTheDocument();
  });
});

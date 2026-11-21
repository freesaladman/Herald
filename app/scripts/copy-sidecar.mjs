// Copies the release herald CLI to the name Tauri expects for an externalBin sidecar.
// Runs with cwd app/ (see build.beforeBuildCommand in src-tauri/tauri.bundle.conf.json).
import { execFileSync } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync } from "node:fs";
import { resolve } from "node:path";

const ext = process.platform === "win32" ? ".exe" : "";

const hostMatch = /^host:\s*(\S+)\s*$/m.exec(
  execFileSync("rustc", ["-vV"], { encoding: "utf8" }),
);
if (!hostMatch) {
  console.error("copy-sidecar: could not find the host triple in `rustc -vV` output");
  process.exit(1);
}
const triple = hostMatch[1];

const source = resolve("..", "target", "release", `herald${ext}`);
if (!existsSync(source)) {
  console.error(
    `copy-sidecar: ${source} not found; run \`cargo build -p herald-cli --release\` first`,
  );
  process.exit(1);
}

const destDir = resolve("src-tauri", "binaries");
mkdirSync(destDir, { recursive: true });
const dest = resolve(destDir, `herald-${triple}${ext}`);
copyFileSync(source, dest);
console.log(`copy-sidecar: ${source} -> ${dest}`);

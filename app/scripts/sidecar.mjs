// Builds the release herald CLI and copies it to the name Tauri expects for an externalBin sidecar.
// Runs with cwd app/ from build.beforeBuildCommand in the src-tauri/tauri.bundle*.conf.json files.
// Tauri sets TAURI_ENV_TARGET_TRIPLE for `tauri build --target`; without it the host triple is used.
import { execFileSync } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync } from "node:fs";
import { resolve } from "node:path";

const hostMatch = /^host:\s*(\S+)\s*$/m.exec(
  execFileSync("rustc", ["-vV"], { encoding: "utf8" }),
);
if (!hostMatch) {
  console.error("sidecar: could not find the host triple in `rustc -vV` output");
  process.exit(1);
}
const host = hostMatch[1];
const triple = process.env.TAURI_ENV_TARGET_TRIPLE || host;
const ext = triple.includes("windows") ? ".exe" : "";

// Building for the host keeps the default target dir, so a local build reuses its cache.
const cross = triple !== host;
const args = ["build", "-p", "herald-cli", "--release"];
if (cross) {
  args.push("--target", triple);
}
execFileSync("cargo", args, { stdio: "inherit", cwd: resolve("..") });

const outDir = cross
  ? resolve("..", "target", triple, "release")
  : resolve("..", "target", "release");
const source = resolve(outDir, `herald${ext}`);
if (!existsSync(source)) {
  console.error(`sidecar: ${source} not found after the build`);
  process.exit(1);
}

const destDir = resolve("src-tauri", "binaries");
mkdirSync(destDir, { recursive: true });
const dest = resolve(destDir, `herald-${triple}${ext}`);
copyFileSync(source, dest);
console.log(`sidecar: ${source} -> ${dest}`);

"use strict";
const fs = require("node:fs");
const path = require("node:path");
const assert = require("node:assert/strict");
const Ajv = require("ajv");
const root = path.resolve(__dirname, "..");
const read = (name) =>
  JSON.parse(fs.readFileSync(path.join(root, name), "utf8"));
const config = read("src-tauri/tauri.conf.json");
const schema = require("@tauri-apps/cli/config.schema.json");
const ajv = new Ajv({
  allErrors: true,
  strict: false,
  validateFormats: false,
  unicodeRegExp: false,
});
assert.ok(ajv.validate(schema, config), JSON.stringify(ajv.errors, null, 2));
const capability = read("src-tauri/capabilities/setup.json");
assert.deepEqual(config.app.security.capabilities, ["setup"]);
assert.deepEqual(capability.windows, ["setup"]);
assert.equal(capability.local, true);
assert.equal(capability.remote, undefined);
assert.deepEqual(capability.permissions, [
  "allow-get-configuration",
  "allow-connect-server",
  "allow-reveal-setup",
]);
assert.equal(config.app.windows.find((window) => window.label === "setup").visible, false);
assert.equal(config.bundle.windows.webviewInstallMode.type, "offlineInstaller");
assert.equal(config.bundle.createUpdaterArtifacts, true);
assert.deepEqual(config.plugins.updater.endpoints, [
  "https://github.com/dev-merit01/radio-zimbabwe-chatbot/releases/latest/download/latest.json",
]);
assert.equal(config.plugins.updater.windows.installMode, "passive");
// Never grant the remote workspace JavaScript installer/update privileges.
assert.equal(JSON.stringify(capability).includes("updater:"), false);
assert.equal(config.bundle.windows.nsis.installMode, "currentUser");
for (const icon of config.bundle.icon)
  assert.ok(fs.existsSync(path.join(root, "src-tauri", icon)));
const vectorIcon = fs.readFileSync(path.join(root, "src-tauri/icons/airvote-icon.svg"), "utf8");
assert.equal(vectorIcon, fs.readFileSync(path.join(root, "desktop/ui/assets/airvote-icon.svg"), "utf8"));
assert.ok(vectorIcon.includes('viewBox="0 0 512 512"'));
assert.ok(fs.readFileSync(path.join(root, "desktop/ui/index.html"), "utf8").includes('src="assets/airvote-icon.svg"'));
assert.ok(
  fs.readFileSync(path.join(root, "src-tauri/src/windows.rs"), "utf8")
    .includes('unwrap_or("https://airvote.up.railway.app/")'),
  "A clean install must have the production Railway origin preconfigured",
);
const server = process.env.VOTING_STUDIO_SERVER_URL;
if (server) {
  assert.ok(
    !/[\x00-\x1f\x7f\\]/.test(server),
    "Invalid preconfigured server address",
  );
  const url = new URL(server);
  assert.ok(
    (url.protocol === "https:" ||
      (url.protocol === "http:" && url.hostname === "127.0.0.1")) &&
      url.pathname === "/" &&
      !url.username &&
      !url.password &&
      !url.search &&
      !url.hash &&
      url.port !== "0",
    "Use an HTTPS origin, or http://127.0.0.1:8000 for local testing",
  );
  assert.ok(
    !["tauri.localhost", "ipc.localhost"].includes(url.hostname),
    "Reserved server address",
  );
}
console.log(
  "Tauri configuration, local permissions, installer and assets validated.",
);

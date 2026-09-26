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
]);
assert.equal(config.bundle.windows.webviewInstallMode.type, "offlineInstaller");
assert.equal(config.bundle.windows.nsis.installMode, "currentUser");
for (const icon of config.bundle.icon)
  assert.ok(fs.existsSync(path.join(root, "src-tauri", icon)));
const server = process.env.VOTING_STUDIO_SERVER_URL;
if (server) {
  assert.ok(
    !/[\x00-\x1f\x7f\\]/.test(server),
    "Invalid preconfigured server address",
  );
  const url = new URL(server);
  assert.ok(
    url.protocol === "https:" &&
      url.pathname === "/" &&
      !url.username &&
      !url.password &&
      !url.search &&
      !url.hash &&
      url.port !== "0",
    "Use an HTTPS origin for VOTING_STUDIO_SERVER_URL",
  );
  assert.ok(
    !["tauri.localhost", "ipc.localhost"].includes(url.hostname),
    "Reserved server address",
  );
}
console.log(
  "Tauri configuration, local permissions, installer and assets validated.",
);

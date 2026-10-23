const test = require("node:test");
const assert = require("node:assert");
const { assertRequiredSecrets, REQUIRED_SECRETS } = require("../build/config/required-secrets");

const full = {
  FIREBASE_SERVICE_ACCOUNT: { privateKey: "x" },
  SOL_PUBLIC_RPC_NODES: ["https://node-a", "https://node-b"],
  ALCHEMY_SOLANA_KEY: "k",
};

test("passes when every key is present", () => {
  assert.doesNotThrow(() => assertRequiredSecrets(full, REQUIRED_SECRETS));
});

test("lists every missing key in one error", () => {
  assert.throws(() => assertRequiredSecrets({}, ["FIREBASE_SERVICE_ACCOUNT", "SOL_PUBLIC_RPC_NODES"]), {
    message: "Missing ZooKeeper config keys: FIREBASE_SERVICE_ACCOUNT, SOL_PUBLIC_RPC_NODES",
  });
});

test("treats an empty value as missing", () => {
  assert.throws(() => assertRequiredSecrets({ SSL: { KEY: "" } }, ["SSL.KEY"]), /SSL\.KEY/);
});

test("requires the Firebase account, the public node list and the Alchemy key", () => {
  for (const k of ["FIREBASE_SERVICE_ACCOUNT", "SOL_PUBLIC_RPC_NODES", "ALCHEMY_SOLANA_KEY"]) {
    assert.ok(REQUIRED_SECRETS.includes(k), k);
    const partial = { ...full };
    delete partial[k];
    assert.throws(() => assertRequiredSecrets(partial, REQUIRED_SECRETS), new RegExp(k));
  }
});

test("no source file still carries a redacted placeholder", () => {
  const { execSync } = require("child_process");
  const hits = execSync("git grep -n REDACTED -- src || true", { encoding: "utf8" }).trim();
  assert.strictEqual(hits, "");
});

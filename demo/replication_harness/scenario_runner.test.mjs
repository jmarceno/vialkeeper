import { test } from "node:test";
import assert from "node:assert/strict";
import { scenarioExists, scenarioManifest } from "./scenario_runner.mjs";

test("scenario manifest is unique, bounded, and executable", () => {
  const ids = scenarioManifest.map((scenario) => scenario.id);

  assert.equal(new Set(ids).size, ids.length);
  assert.ok(ids.length >= 5);
  for (const scenario of scenarioManifest) {
    assert.match(scenario.id, /^[a-z0-9-]+$/);
    assert.ok(scenario.description.length <= 300);
    assert.equal(scenarioExists(scenario.id), true);
  }
});

test("unknown scenario IDs are not advertised as executable", () => {
  assert.equal(scenarioExists("not-a-real-scenario"), false);
});

// Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const {test} = require('node:test');
const yaml = require('js-yaml');
const guard = require('./release-notes-guard.cjs');

const repo = {owner: 'scylladb', repo: 'scylla-cdc-go'};
const label = 'omit-from-release-notes';
const file = (filename, extra = {}) => ({filename, status: 'modified', ...extra});
const workflowText = readFileSync(join(__dirname, '../workflows/label-release-notes.yml'), 'utf8');
const workflow = yaml.load(workflowText);
const steps = workflow.jobs.classify.steps;
const scripts = steps.filter(step => step.with?.script).map(step => step.with.script);
const targetScript = steps.find(step => step.id === 'target').with.script;
const dispatchStep = steps.find(step => step.name === 'Require default-branch manual dispatch');
const collectScript = steps.find(step => step.id === 'guard').with.script;
const verifyScript = steps.at(-1).with.script;
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

function fixture(options = {}) {
  const files = options.files ?? [file('README.md')];
  const labels = new Set(options.labels ?? ['unrelated']);
  const calls = [];
  let gets = 0;
  let listings = 0;
  const initial = {number: 20, head: {sha: 'head'}, base: {sha: 'base'}, changed_files: files.length};
  const github = {rest: {pulls: {
    get: async params => {
      assert.deepEqual(params, {...repo, pull_number: 20});
      calls.push('get');
      gets++;
      if (options.failGetAt === gets) throw options.lookupError ?? new Error('PR lookup unavailable');
      return {data: {...initial, ...options.revisions?.[gets - 1]}};
    },
    listFiles: () => {}
  }, issues: {
    listLabelsOnIssue: () => {},
    removeLabel: async params => {
      assert.deepEqual(params, {...repo, issue_number: 20, name: label});
      calls.push('remove');
      if (options.cleanupFailure) throw new Error('Removal unavailable');
      // GitHub looks up the logical label without case sensitivity while
      // preserving its stored spelling in returned metadata.
      const actual = [...labels].find(name => name.toLowerCase() === label);
      if (actual === undefined) throw {status: 404};
      labels.delete(actual);
    }
  }}, paginate: async (method, params) => {
    assert.equal(params.per_page, 100);
    if (method === github.rest.pulls.listFiles) {
      assert.deepEqual(params, {...repo, pull_number: 20, per_page: 100});
      calls.push('files');
      if (options.fileFailure) throw new Error('File listing unavailable');
      return options.listings?.[listings++] ?? files;
    }
    assert.equal(method, github.rest.issues.listLabelsOnIssue);
    assert.deepEqual(params, {...repo, issue_number: 20, per_page: 100});
    calls.push('labels');
    if (options.labelFailure) throw new Error('Labels unavailable');
    return options.labelMetadata ?? [...labels].map(name => ({name}));
  }};
  return {github, labels, calls};
}

async function adapter(f, hash, overrides = {}, load = () => guard) {
  // Existing finalizer cases start with a previously confirmed PR.
  const env = {PR_NUMBER: '20', CONFIRMED_PR_NUMBER: '20', TARGET_OUTCOME: 'success',
    CHECKOUT_OUTCOME: 'success', INSTALL_OUTCOME: 'success',
    GUARD_OUTCOME: 'success', LABELER_OUTCOME: 'success', UNSAFE_RENAME: 'false',
    SNAPSHOT_HASH: hash, ...overrides};
  const logs = [];
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process', verifyScript);
  let error;
  try {
    await run(f.github, {repo}, {info: text => logs.push(text), error: text => logs.push(text)},
      load, {env});
  } catch (caught) { error = caught; }
  return {error, logs};
}

test('canonical policy matches mixed support files and keeps runtime or unknown paths visible', async () => {
  const files = ['.github/workflows/release.yml', '.github/.hidden', 'README.md', 'RELEASING.md',
    'reader_test.go', 'examples/replicator/main_test.go', 'testutils/utils.go', 'ci/docker-compose.yml',
    '.golangci.yml', '.gitignore', 'renovate.json', 'examples/replicator/.gitignore'].map(name => file(name));
  assert.equal((await guard.snapshot(fixture({files}).github, repo, 20)).omit, true);
  for (const name of ['doc.go', 'reader.go', 'go.mod', 'go.sum', 'Makefile', 'LICENSE',
    'examples/main.go', 'testutils/new.go', 'ci/config.yml', 'unknown.txt', 'docs/code.go', 'docs/guide.md']) {
    const current = await guard.snapshot(fixture({files: [...files, file(name)]}).github, repo, 20);
    assert.equal(current.omit, false, name);
  }
});

test('renames use the actual canonical policy for both prior and current paths', async () => {
  for (const [previous, omit] of [['README.md', true], ['reader.go', false], ['unknown', false]]) {
    const current = await guard.snapshot(fixture({files: [file('RELEASING.md', {
      status: 'renamed', previous_filename: previous})]}).github, repo, 20);
    assert.equal(current.omit, omit);
    assert.equal(current.unsafeRename, !omit);
  }
});

test('support-to-runtime renames remain visible without skipping the stock action', async () => {
  const current = await guard.snapshot(fixture({files: [file('reader.go', {
    status: 'renamed', previous_filename: 'reader_test.go'})]}).github, repo, 20);
  assert.equal(current.omit, false);
  assert.equal(current.unsafeRename, false);
});

test('unsupported config shapes fail instead of approximating a second policy', () => {
  for (const source of ['{}', 'omit-from-release-notes: []',
    'omit-from-release-notes: [{head-branch: [main]}]',
    'omit-from-release-notes: [{changed-files: [{any-glob-to-any-file: ["**"]}]}]',
    'omit-from-release-notes: [{changed-files: [{all-globs-to-all-files: []}]}]',
    'other-label: [{changed-files: [{all-globs-to-all-files: ["**"]}]}]']) {
    assert.throws(() => guard.policy(source), /Unsupported labeler configuration/);
  }
});

test('PR input rejects malformed, ambiguous and unsafe numbers', () => {
  assert.equal(guard.parseNumber('20'), 20);
  for (const value of ['', '0', '-1', '01', '1.0', '1e2', ' 20', '20\n',
    '20,21', '${{ arbitrary }}', '9007199254740992', undefined, 20]) {
    assert.throws(() => guard.parseNumber(value), /canonical positive safe PR number/);
  }
});

test('pagination requests full metadata and accepts a complete listing beyond one page', async () => {
  const f = fixture({files: Array.from({length: 105}, (_, i) => file(`fixtures/${i}_test.go`))});
  const snapshot = await guard.snapshot(f.github, repo, 20);
  assert.equal(snapshot.count, 105);
  assert.equal(snapshot.omit, true);
  assert.match(snapshot.hash, /^[0-9a-f]{64}$/);
  assert.deepEqual(f.calls, ['get', 'files', 'get']);
});

test('zero, incomplete, truncated, duplicate and invalid file evidence fail visibly', async () => {
  const cases = [
    {files: []},
    {revisions: [{changed_files: 2}, {changed_files: 2}]},
    {files: [file('README.md'), file('README.md')]},
    {files: [file('README.md', {status: 'renamed'})]},
    {files: [file('README.md', {status: undefined})]},
    {files: [file('README.md', {previous_filename: ''})]},
    {files: [file('README.md')], listings: [[]]}
  ];
  for (const options of cases) await assert.rejects(guard.snapshot(fixture(options).github, repo, 20));
});

test('head, base and count changes during collection are rejected', async () => {
  for (const changed of [{head: {sha: 'new'}}, {base: {sha: 'new'}}, {changed_files: 2}]) {
    const f = fixture({revisions: [{}, changed]});
    await assert.rejects(guard.snapshot(f.github, repo, 20), /revisions changed/);
  }
});

test('verification detects before/after revisions and filename, status or prior-path changes', async () => {
  const original = await guard.snapshot(fixture().github, repo, 20);
  for (const options of [
    {revisions: [{head: {sha: 'new'}}, {head: {sha: 'new'}}]},
    {revisions: [{base: {sha: 'new'}}, {base: {sha: 'new'}}]},
    {files: [file('docs/new.md')]},
    {files: [file('README.md', {status: 'added'})]},
    {files: [file('README.md', {status: 'renamed', previous_filename: 'docs/old.md'})]}
  ]) {
    await assert.rejects(guard.verify(fixture(options).github, repo, 20, original.hash), /snapshot changed/);
  }
  const f = fixture({labels: [label], revisions: [{}, {}, {head: {sha: 'new'}}]});
  await assert.rejects(guard.verify(f.github, repo, 20, original.hash), /revisions changed during label/);
});

test('lookup, listing and actual-label lookup failures propagate', async () => {
  await assert.rejects(guard.snapshot(fixture({failGetAt: 1}).github, repo, 20), /lookup unavailable/);
  await assert.rejects(guard.snapshot(fixture({failGetAt: 2}).github, repo, 20), /lookup unavailable/);
  await assert.rejects(guard.snapshot(fixture({fileFailure: true}).github, repo, 20), /listing unavailable/);
  const original = await guard.snapshot(fixture().github, repo, 20);
  await assert.rejects(guard.verify(fixture({labelFailure: true}).github, repo, 20, original.hash), /Labels unavailable/);
});

test('actual-label verification catches stale state after internal action skip', async () => {
  for (const [files, labels] of [[[file('README.md')], []], [[file('doc.go')], [label]]]) {
    const f = fixture({files, labels: [...labels, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 20);
    const {error} = await adapter(f, original.hash);
    assert.match(error.message, /Actual omission label/);
    assert.equal(f.labels.has(label), false);
    assert.equal(f.labels.has('unrelated'), true);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('failed final snapshot, lookup or label retrieval attempts cleanup', async () => {
  const original = await guard.snapshot(fixture().github, repo, 20);
  for (const options of [{files: [file('docs/changed.md')]}, {failGetAt: 1},
    {fileFailure: true}, {labelFailure: true}]) {
    const f = fixture({...options, labels: [label, 'unrelated']});
    const {error} = await adapter(f, original.hash);
    assert.ok(error);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('successful verification checks actual labels and preserves unrelated labels', async () => {
  for (const [files, labels] of [[[file('README.md')], [label]], [[file('doc.go')], []]]) {
    const f = fixture({files, labels: [...labels, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 20);
    const {error, logs} = await adapter(f, original.hash);
    assert.ifError(error);
    assert.match(logs[0], /Verified head=head base=base count=1 snapshot=.*actual label checked/);
    assert.equal(f.labels.has('unrelated'), true);
    assert.ok(!f.calls.includes('remove'));
  }
});

test('actual logical marker identity preserves support omissions and rejects runtime omissions regardless of casing', async () => {
  const marker = 'Omit-from-release-notes';
  const other = 'OMIT-FROM-RELEASE-NOTES-extra';
  for (const [name, omit] of [['README.md', true], ['reader.go', false]]) {
    const f = fixture({files: [file(name)], labels: [marker, other, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 20);
    const {error} = await adapter(f, original.hash);
    if (omit) {
      assert.ifError(error);
      assert.equal(f.labels.has(marker), true);
      assert.ok(!f.calls.includes('remove'));
    } else {
      assert.match(error.message, /Actual omission label/);
      assert.equal(f.labels.has(marker), false);
      assert.equal(f.calls.filter(call => call === 'remove').length, 1);
    }
    assert.equal(f.labels.has(other), true);
    assert.equal(f.labels.has('unrelated'), true);
  }
});

test('invalid actual-label metadata fails verification and attempts canonical cleanup', async () => {
  const original = await guard.snapshot(fixture().github, repo, 20);
  for (const item of [null, {}, {name: 42}, {name: ''}]) {
    const f = fixture({labels: [label, 'unrelated'], labelMetadata: [item]});
    const {error} = await adapter(f, original.hash);
    assert.match(error.message, /Invalid label metadata/);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('intentional unsafe-rename skip removes omission label and verifies visible state', async () => {
  const f = fixture({files: [file('README.md', {status: 'renamed', previous_filename: 'reader.go'})],
    labels: [label, 'unrelated']});
  const original = await guard.snapshot(f.github, repo, 20);
  const {error, logs} = await adapter(f, original.hash, {UNSAFE_RENAME: 'true', LABELER_OUTCOME: 'skipped'});
  assert.ifError(error);
  assert.match(logs[0], /omit=false; actual label checked/);
  assert.deepEqual([...f.labels], ['unrelated']);
  assert.equal(f.calls.filter(call => call === 'remove').length, 1);
});

test('failed or skipped predecessors clean up confirmed PRs without requiring the helper', async () => {
  for (const key of ['TARGET_OUTCOME', 'CHECKOUT_OUTCOME', 'INSTALL_OUTCOME', 'GUARD_OUTCOME', 'LABELER_OUTCOME']) {
    for (const outcome of ['failure', 'skipped', 'cancelled', '']) {
      const f = fixture({labels: [label, 'unrelated']});
      const {error} = await adapter(f, '', {[key]: outcome}, () => { throw new Error('Must not require helper'); });
      assert.match(error.message, /predecessor failed or unexpectedly skipped/);
      assert.deepEqual([...f.labels], ['unrelated']);
      assert.deepEqual(f.calls, ['remove']);
    }
  }
  const f = fixture({labels: [label]});
  const {error} = await adapter(f, '', {}, () => { throw new Error('Helper missing'); });
  assert.match(error.message, /Helper missing/);
  assert.deepEqual(f.calls, ['remove']);
});

test('cleanup API failures remain visible without losing the original failure or retrying', async () => {
  const f = fixture({cleanupFailure: true});
  const result = await adapter(f, '', {INSTALL_OUTCOME: 'failure'});
  assert.match(result.error.message, /predecessor failed/);
  assert.match(result.logs[0], /cleanup failed: Removal unavailable/);
  assert.deepEqual(f.calls, ['remove']);
  const unsafe = fixture({cleanupFailure: true});
  const result2 = await adapter(unsafe, '', {UNSAFE_RENAME: 'true', LABELER_OUTCOME: 'skipped'});
  assert.match(result2.error.message, /Removal unavailable/);
  assert.deepEqual(unsafe.calls, ['remove']);
});

test('workflow adapters use environment input and emit bounded verified snapshot outputs', async () => {
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process', collectScript);
  const outputs = {};
  const f = fixture();
  await run(f.github, {repo}, {setOutput: (key, value) => { outputs[key] = value; }, info: () => {}},
    () => guard, {env: {PR_NUMBER: '20'}});
  assert.deepEqual(Object.keys(outputs), ['hash', 'unsafe-rename']);
  assert.equal(outputs['unsafe-rename'], 'false');
  const invalid = await adapter(f, '', {PR_NUMBER: '01'});
  assert.match(invalid.error.message, /canonical positive safe PR number/);
  assert.equal(steps.at(-1).if, 'always()');
  assert.equal(steps.find(step => step.id === 'checkout').with.ref, '${{ github.sha }}');
  assert.equal(steps.find(step => step.id === 'checkout').with['persist-credentials'], false);
  assert.equal(steps.find(step => step.id === 'install').run, 'npm ci --ignore-scripts --prefix .github');
  const action = steps.find(step => step.id === 'labeler');
  assert.equal(action.if, "success() && steps.guard.outputs.unsafe-rename == 'false'");
  assert.equal(action.with['sync-labels'], true);
  assert.equal(action.with['pr-number'], '${{ env.PR_NUMBER }}');
  for (const script of scripts) assert.ok(!script.includes('${{'));
});

test('merged-PR backfills load event tooling rather than the old PR base or head', () => {
  const expression = steps.find(step => step.id === 'checkout').with.ref.replace(/^\$\{\{\s*|\s*\}\}$/g, '');
  const checkoutRef = new Function('github', `return ${expression}`);
  for (const event of [{pull_request: {head: {sha: 'untrusted'}, base: {sha: 'old-base'}}}, {}]) {
    assert.equal(checkoutRef({sha: 'trusted-default-event', event}), 'trusted-default-event');
  }
});

test('dispatch precondition rejects non-default refs before checkout and cleanup needs no helper', async () => {
  const run = new AsyncFunction('context', dispatchStep.with.script);
  const payload = {repository: {default_branch: 'master'}};
  await run({eventName: 'workflow_dispatch', ref: 'refs/heads/master', payload});
  await run({eventName: 'pull_request_target', ref: 'refs/heads/release-branch', payload});
  for (const ref of ['refs/heads/feature', 'refs/tags/v1.2.3']) {
    await assert.rejects(run({eventName: 'workflow_dispatch', ref, payload}), /repository default branch/);
  }
  const f = fixture({labels: [label, 'unrelated']});
  const {error} = await adapter(f, '', {CHECKOUT_OUTCOME: 'skipped', INSTALL_OUTCOME: 'skipped',
    GUARD_OUTCOME: 'skipped', LABELER_OUTCOME: 'skipped'}, () => {throw new Error('Must not load helper');});
  assert.match(error.message, /predecessor failed/);
  assert.deepEqual([...f.labels], ['unrelated']);
  assert.deepEqual(f.calls, ['remove']);
});

async function confirmTarget(f, context, raw = '20') {
  const outputs = {};
  let error;
  try {
    await new AsyncFunction('github', 'context', 'core', 'process', targetScript)(f.github, context,
      {setOutput: (key, value) => { outputs[key] = value; }}, {env: {PR_NUMBER: raw}});
  } catch (caught) { error = caught; }
  return {outputs, error, outcome: error ? 'failure' : 'success'};
}

async function failAfterConfirmation(f, confirmation, overrides = {}) {
  return adapter(f, '', {CONFIRMED_PR_NUMBER: confirmation.outputs['confirmed-pr-number'] ?? '',
    TARGET_OUTCOME: confirmation.outcome, CHECKOUT_OUTCOME: 'skipped', INSTALL_OUTCOME: 'skipped',
    GUARD_OUTCOME: 'skipped', LABELER_OUTCOME: 'skipped', ...overrides},
  () => { throw new Error('Cleanup must not require a helper'); });
}

test('unconfirmed manual issues, unavailable or mismatched PRs never write labels', async () => {
  for (const options of [{failGetAt: 1, lookupError: {status: 404}}, {failGetAt: 1},
    {revisions: [{number: 21}]}, {revisions: [{number: '20'}]}]) {
    const f = fixture({...options, labels: [label, 'unrelated']});
    const confirmation = await confirmTarget(f, {repo, eventName: 'workflow_dispatch'});
    assert.ok(confirmation.error);
    assert.deepEqual(confirmation.outputs, {});
    const {error} = await failAfterConfirmation(f, confirmation);
    assert.deepEqual(f.calls, ['get']);
    assert.deepEqual([...f.labels], [label, 'unrelated']);
    assert.match(error.message, /not confirmed/);
  }
});

test('trusted PR payload must match the requested identity and avoids a lookup', async () => {
  const f = fixture({failGetAt: 1, labels: [label, 'unrelated']});
  const confirmation = await confirmTarget(f, {repo, eventName: 'pull_request_target',
    payload: {pull_request: {number: 20}}});
  assert.ifError(confirmation.error);
  assert.deepEqual(confirmation.outputs, {'confirmed-pr-number': '20'});
  assert.deepEqual(f.calls, []);
  const {error} = await failAfterConfirmation(f, confirmation);
  assert.deepEqual(f.calls, ['remove']);
  assert.deepEqual([...f.labels], ['unrelated']);
  assert.match(error.message, /predecessor failed/);
  for (const payload of [{}, {pull_request: {number: 21}}]) {
    const other = fixture({labels: [label]});
    const rejected = await confirmTarget(other, {repo, eventName: 'pull_request_target', payload});
    assert.match(rejected.error.message, /does not match/);
    await failAfterConfirmation(other, rejected);
    assert.deepEqual(other.calls, []);
    assert.equal(other.labels.has(label), true);
  }
});

test('confirmed manual PR identity survives later lookup, setup and helper failure', async () => {
  for (const overrides of [{}, {CHECKOUT_OUTCOME: 'failure'},
    {CHECKOUT_OUTCOME: 'success', INSTALL_OUTCOME: 'failure'},
    {CHECKOUT_OUTCOME: 'success', INSTALL_OUTCOME: 'success', GUARD_OUTCOME: 'failure'}]) {
    const f = fixture({labels: [label, 'unrelated'], failGetAt: 2});
    const confirmation = await confirmTarget(f, {repo, eventName: 'workflow_dispatch'});
    assert.ifError(confirmation.error);
    const {error} = await failAfterConfirmation(f, confirmation, overrides);
    assert.match(error.message, /predecessor failed/);
    assert.deepEqual(f.calls, ['get', 'remove']);
    assert.deepEqual([...f.labels], ['unrelated']);
  }
  const f = fixture({labels: [label, 'unrelated'], failGetAt: 2});
  const confirmation = await confirmTarget(f, {repo, eventName: 'workflow_dispatch'});
  const {error} = await adapter(f, '', {CONFIRMED_PR_NUMBER: confirmation.outputs['confirmed-pr-number']});
  assert.match(error.message, /lookup unavailable/);
  assert.deepEqual(f.calls, ['get', 'get', 'remove']);
  assert.deepEqual([...f.labels], ['unrelated']);
});

test('identity output wiring rejects a different number before all label writes', async () => {
  const f = fixture({labels: [label]});
  const {error} = await adapter(f, '', {CONFIRMED_PR_NUMBER: '21'});
  assert.match(error.message, /not confirmed/);
  assert.deepEqual(f.calls, []);
  const target = steps.findIndex(step => step.id === 'target');
  assert.ok(target < steps.indexOf(dispatchStep));
  assert.ok(target < steps.findIndex(step => step.id === 'checkout'));
  assert.ok(target < steps.findIndex(step => step.id === 'install'));
  assert.equal(steps.at(-1).env.CONFIRMED_PR_NUMBER, '${{ steps.target.outputs.confirmed-pr-number }}');
  assert.equal(steps.at(-1).env.TARGET_OUTCOME, '${{ steps.target.outcome }}');
});

test('confirmed identity and collected outputs feed successful verification for both entrypoints', async () => {
  for (const eventName of ['workflow_dispatch', 'pull_request_target']) {
    const f = fixture({labels: [label, 'unrelated']});
    const context = {repo, eventName, payload: {pull_request: {number: 20}}};
    const confirmation = await confirmTarget(f, context);
    assert.ifError(confirmation.error);
    const collected = {};
    await new AsyncFunction('github', 'context', 'core', 'require', 'process', collectScript)(
      f.github, context, {setOutput: (key, value) => { collected[key] = value; }, info: () => {}},
      () => guard, {env: {PR_NUMBER: '20'}});
    const {error, logs} = await adapter(f, collected.hash, {
      CONFIRMED_PR_NUMBER: confirmation.outputs['confirmed-pr-number'],
      TARGET_OUTCOME: confirmation.outcome, UNSAFE_RENAME: collected['unsafe-rename']
    });
    assert.ifError(error);
    assert.match(logs[0], /omit=true; actual label checked/);
    assert.ok(!f.calls.includes('remove'));
    assert.deepEqual([...f.labels], [label, 'unrelated']);
  }
});

test('confirmed manual targets retain cleanup after a rejected dispatch branch', async () => {
  const f = fixture({labels: [label, 'unrelated'], failGetAt: 2});
  const context = {repo, eventName: 'workflow_dispatch', ref: 'refs/heads/other',
    payload: {repository: {default_branch: 'master'}}};
  const confirmation = await confirmTarget(f, context);
  assert.ifError(confirmation.error);
  await assert.rejects(new AsyncFunction('context', dispatchStep.with.script)(context), /default branch/);
  await failAfterConfirmation(f, confirmation);
  assert.deepEqual(f.calls, ['get', 'remove']);
  assert.deepEqual([...f.labels], ['unrelated']);
});

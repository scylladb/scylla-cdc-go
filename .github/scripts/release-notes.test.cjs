// Copyright (C) 2026 ScyllaDB
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {supportOnly, classify} = require('./release-notes.cjs');
const file = filename => ({filename, status: 'modified'});

test('support-only mixtures qualify; production, manifests and unknown paths remain visible', () => {
  const files = ['README.md', 'RELEASING.md', 'consumer_test.go',
    'examples/replicator/replicator_test.go', 'testutils/utils.go',
    '.github/workflows/ci.yml', 'ci/docker-compose.yml',
    '.gitignore', '.golangci.yml', 'renovate.json'].map(file);
  assert.equal(supportOnly(files, files.length), true);
  for (const path of ['reader.go', 'doc.go', 'go.mod', 'go.sum',
    'examples/replicator-gcp-pub/go.mod', 'examples/replicator-gcp-pub/go.sum',
    'Makefile', 'examples/printer/main.go', 'unknown.md',
    'tools/build.sh', 'build/package.sh', 'testutils/new.go',
    '.github/../reader.go', '/README.md']) {
    assert.equal(supportOnly([...files, file(path)], files.length + 1), false, path);
  }
  assert.equal(supportOnly([], 0), false);
  assert.equal(supportOnly(files, files.length + 1), false);
  assert.equal(supportOnly([file('README.md'), file('README.md')], 2), false);
});

test('renames require both old and new support paths', () => {
  for (const [filename, previous_filename, expected] of [
    ['README.md', 'RELEASING.md', true],
    ['README.md', 'reader.go', false],
    ['reader.go', 'README.md', false],
    ['README.md', undefined, false],
  ]) {
    assert.equal(supportOnly([{filename, previous_filename, status: 'renamed'}], 1), expected);
  }
});

function fixture(files, count = files.length) {
  const calls = [];
  const pull = {head: {sha: 'a'.repeat(40)}, base: {sha: 'b'.repeat(40)}, changed_files: count};
  const github = {
    rest: {
      pulls: {get: async () => ({data: pull}), listFiles: () => {}},
      issues: {
        createLabel: async args => calls.push(['create', args]),
        addLabels: async args => calls.push(['add', args]),
        removeLabel: async args => calls.push(['remove', args]),
      },
    },
    paginate: async (endpoint, args) => {
      assert.equal(endpoint, github.rest.pulls.listFiles);
      assert.equal(args.per_page, 100);
      calls.push(['paginate']);
      return files;
    },
  };
  return {github, context: {repo: {owner: 'scylladb', repo: 'scylla-cdc-go'}},
    core: {info: () => {}, warning: () => {}}, calls, pull};
}

test('pagination supports more than one page and creates a missing label', async () => {
  const f = fixture(Array.from({length: 201}, (_, i) => file(`test${i}_test.go`)));
  await classify(f, 23);
  assert.deepEqual(f.calls.map(call => call[0]), ['paginate', 'create', 'add']);
});

test('existing label is reused', async () => {
  const f = fixture([file('README.md')]);
  f.github.rest.issues.createLabel = async () => {throw {status: 422};};
  await classify(f, 23);
  assert.equal(f.calls.at(-1)[0], 'add');
});

test('mixed PR removes stale omission label', async () => {
  const f = fixture([file('README.md'), file('go.mod')]);
  await classify(f, 23);
  assert.equal(f.calls.at(-1)[0], 'remove');
  assert.equal(f.calls.at(-1)[1].name, 'omit-from-release-notes');
});

test('incomplete/duplicated listing and API/write failures clean stale labels and fail visibly', async () => {
  for (const failure of ['truncated', 'duplicate', 'get', 'paginate', 'create', 'add']) {
    const f = fixture([file('README.md')]);
    if (failure === 'truncated') f.pull.changed_files = 2;
    if (failure === 'duplicate') {
      f.pull.changed_files = 2;
      f.github.paginate = async () => [file('README.md'), file('README.md')];
    }
    if (failure === 'get') f.github.rest.pulls.get = async () => {throw new Error('get');};
    if (failure === 'paginate') f.github.paginate = async () => {throw new Error('paginate');};
    if (failure === 'create') f.github.rest.issues.createLabel = async () => {throw new Error('create');};
    if (failure === 'add') f.github.rest.issues.addLabels = async () => {throw new Error('add');};
    await assert.rejects(classify(f, 23));
    assert.equal(f.calls.at(-1)[0], 'remove', failure);
  }
});

test('head, base and file-count changes invalidate a snapshot', async () => {
  for (const part of ['head', 'base', 'count']) {
    const f = fixture([file('README.md')]);
    let requests = 0;
    f.github.rest.pulls.get = async () => {
      const data = {...f.pull};
      if (++requests === 2) {
        if (part === 'count') data.changed_files++;
        else data[part] = {sha: 'changed'};
      }
      return {data};
    };
    await assert.rejects(classify(f, 23), /PR changed/);
    assert.equal(f.calls.at(-1)[0], 'remove');
  }
});

test('missing stale label is harmless; cleanup failures preserve the original failure', async () => {
  const f = fixture([file('go.mod')]);
  f.github.rest.issues.removeLabel = async () => {throw {status: 404};};
  await classify(f, 23);
  f.github.paginate = async () => {throw new Error('original');};
  f.github.rest.issues.removeLabel = async () => {throw new Error('cleanup');};
  await assert.rejects(classify(f, 23), /original/);
});


test('malformed PR snapshots fail visibly and remove stale labels', async () => {
  for (const mutate of [
    pr => {delete pr.head;},
    pr => {pr.base.sha = '';},
    pr => {pr.head.sha = 'not-a-sha';},
    pr => {pr.changed_files = 0;},
  ]) {
    const f = fixture([file('README.md')]);
    mutate(f.pull);
    await assert.rejects(classify(f, 23), /PR changed/);
    assert.equal(f.calls.at(-1)[0], 'remove');
  }
});

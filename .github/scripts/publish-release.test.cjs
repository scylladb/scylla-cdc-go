// Copyright (C) 2026 ScyllaDB
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {VERSION, publish} = require('./publish-release.cjs');
const commit = 'a'.repeat(40);
const published = {tag_name: 'v1.2.2', draft: false, prerelease: false, published_at: '2026-10-25', html_url: 'release'};
function fixture() {
  const calls = [];
  const github = {rest: {
    git: {
      getRef: async args => {calls.push(['ref', args]); return {data: {ref: 'refs/tags/v1.2.2', object: {type: 'commit', sha: commit}}};},
      getTag: async () => ({data: {object: {type: 'commit', sha: commit}}}),
    },
    repos: {
      getCommit: async args => {calls.push(['commit', args]); return {data: {sha: commit}};},
      listReleases: () => {},
      getContent: async args => {assert.equal(args.ref, commit); return {data: {encoding: 'base64', content: Buffer.from('changelog:\n  exclude:\n    labels:\n      - omit-from-release-notes\n').toString('base64')}};},
      createRelease: async args => {calls.push(['create', args]); return {data: published};},
    },
  }, paginate: async (method, args) => {
    assert.equal(method, github.rest.repos.listReleases);
    assert.deepEqual(args, {owner: 'scylladb', repo: 'scylla-cdc-go', per_page: 100});
    return [];
  }};
  return {github, context: {repo: {owner: 'scylladb', repo: 'scylla-cdc-go'}, ref: 'refs/tags/v1.2.2', sha: commit,
    payload: {repository: {default_branch: 'master'}}}, core: {info: () => {}}, calls};
}

test('only canonical stable version tags qualify', async () => {
  assert.equal(VERSION.test('v1.2.1'), true);
  for (const tag of ['1.2.1', 'v01.2.1', 'v1.2.1-rc.1', 'v1.2.1/other', 'v1.2.1.4']) {
    const f = fixture(); f.context.ref = `refs/tags/${tag}`;
    await assert.rejects(publish(f), /Expected/);
    assert.equal(f.calls.length, 0);
  }
});

test('verified existing tag publishes generated notes without a redundant target', async () => {
  const f = fixture();
  await publish(f);
  const request = f.calls.find(([type]) => type === 'create')[1];
  assert.equal(request.tag_name, 'v1.2.2');
  assert.equal(Object.hasOwn(request, 'target_commitish'), false);
  assert.equal(request.generate_release_notes, true);
  assert.equal(request.draft, false);
  assert.equal(f.calls.filter(([type]) => type === 'ref').length, 3);
});

test('an older existing tag uses its verified revision without requesting workflow-write permission', async () => {
  const f = fixture();
  f.github.rest.repos.createRelease = async args => {
    // GitHub validates an explicit old target's workflow differences even
    // though existing-tag resolution does not use target_commitish.
    if (Object.hasOwn(args, 'target_commitish')) throw {status: 403};
    f.calls.push(['create', args]);
    return {data: published};
  };
  f.context.payload.repository.default_branch = 'newer-default-branch';
  await publish(f);
  assert.equal(f.calls.find(([kind]) => kind === 'commit')[1].ref, commit);
  assert.equal(f.calls.find(([kind]) => kind === 'create')[1].tag_name, 'v1.2.2');
});

test('a post-publication tag change fails visibly without rewriting or rolling back the release', async () => {
  const f = fixture(); let checks = 0;
  f.github.rest.git.getRef = async () => ({data: {ref: f.context.ref,
    object: {type: 'commit', sha: ++checks > 2 ? 'b'.repeat(40) : commit}}});
  await assert.rejects(publish(f), /no longer points/);
  assert.equal(f.calls.filter(([kind]) => kind === 'create').length, 1);
});

test('annotated tags peel to the event commit', async () => {
  const f = fixture();
  f.github.rest.git.getRef = async () => ({data: {ref: f.context.ref, object: {type: 'tag', sha: 'b'.repeat(40)}}});
  await publish(f);
  assert.equal(f.calls.at(-1)[0], 'create');
});

test('published stable releases are preserved; drafts, unpublished releases and prereleases fail', async () => {
  for (const existing of [published, {...published, draft: true}, {...published, published_at: null}, {...published, prerelease: true}]) {
    const f = fixture();
    f.github.paginate = async () => [existing];
    if (existing === published) await publish(f);
    else await assert.rejects(publish(f), /Existing release/);
    assert.equal(f.calls.some(([type]) => type === 'create'), false);
  }
});

test('draft-aware lookup selects the exact tag beyond the first page', async () => {
  const f = fixture();
  f.github.paginate = async (method, args) => {
    assert.equal(method, f.github.rest.repos.listReleases);
    assert.equal(args.per_page, 100);
    return [...Array.from({length: 100}, (_, i) => ({...published, tag_name: `v0.0.${i}`})),
      {...published, draft: true, published_at: null}];
  };
  await assert.rejects(publish(f), /Existing release/);
  assert.equal(f.calls.some(([type]) => type === 'create'), false);
});

test('unrelated releases do not block creation and duplicate matches fail visibly', async () => {
  const f = fixture();
  f.github.paginate = async () => [{...published, tag_name: 'v9.9.9', draft: true}];
  await publish(f);
  assert.equal(f.calls.some(([type]) => type === 'create'), true);
  const duplicate = fixture();
  duplicate.github.paginate = async () => [published, {...published, draft: true}];
  await assert.rejects(publish(duplicate), /Multiple releases/);
  assert.equal(duplicate.calls.some(([type]) => type === 'create'), false);
});

test('moved, deleted, mismatched, or malformed tags never publish', async () => {
  for (const failure of ['moved', 'deleted', 'deleted-during-notes', 'wrong-ref', 'tag-cycle', 'moved-during-notes']) {
    const f = fixture(); let checks = 0;
    f.github.rest.git.getRef = async () => {
      checks++;
      if (failure === 'deleted' || (failure === 'deleted-during-notes' && checks > 1)) throw {status: 404};
      return {data: {ref: failure === 'wrong-ref' ? 'refs/tags/v9.9.9' : f.context.ref,
        object: {type: failure === 'tag-cycle' ? 'tag' : 'commit',
          sha: failure === 'moved' || (failure === 'moved-during-notes' && checks > 1) ? 'b'.repeat(40) : commit}}};
    };
    if (failure === 'tag-cycle') f.github.rest.git.getTag = async () => ({data: {object: {type: 'tag', sha: commit}}});
    await assert.rejects(publish(f));
    assert.equal(f.calls.some(([type]) => type === 'create'), false, failure);
  }
});

test('API failures and missing configuration fail visibly before creating release', async () => {
  for (const endpoint of ['getCommit', 'getContent']) {
    const f = fixture(); f.github.rest.repos[endpoint] = async () => {throw new Error(endpoint);};
    await assert.rejects(publish(f), new RegExp(endpoint));
    assert.equal(f.calls.some(([type]) => type === 'create'), false);
  }
  const listing = fixture();
  listing.github.paginate = async () => {throw new Error('Release listing unavailable');};
  await assert.rejects(publish(listing), /listing unavailable/);
  assert.equal(listing.calls.some(([type]) => type === 'create'), false);
  const f = fixture();
  f.github.rest.repos.getContent = async () => ({data: {encoding: 'base64', content: Buffer.from('changelog: {}').toString('base64')}});
  await assert.rejects(publish(f), /configuration/);
});

test('unexpected draft or prerelease response cannot report publication success', async () => {
  for (const invalid of [{draft: true}, {prerelease: true}]) {
    const f = fixture();
    f.github.rest.repos.createRelease = async () => ({data: {...published, ...invalid}});
    await assert.rejects(publish(f), /did not publish/);
  }
});

test('release configuration requires the canonical exclusion at its actual YAML path', async () => {
  const cases = [
    ['changelog: {exclude: {labels: [omit-from-release-notes]}}', true],
    ['changelog:\n  categories:\n    - title: Support\n      labels:\n        - omit-from-release-notes\n', false],
    ['changelog: {exclude: {labels: omit-from-release-notes}}', false],
    ['other: {labels: [omit-from-release-notes]}', false],
    ['# labels:\n# - omit-from-release-notes\nchangelog: {}', false],
    ['changelog: {exclude: {labels: [different-label]}}', false],
    ['changelog: [invalid', false],
  ];
  for (const [source, allowed] of cases) {
    const f = fixture();
    f.github.rest.repos.getContent = async () => ({data: {
      encoding: 'base64', content: Buffer.from(source).toString('base64')
    }});
    if (allowed) await publish(f);
    else await assert.rejects(publish(f));
    assert.equal(f.calls.some(([type]) => type === 'create'), allowed, source);
  }
});

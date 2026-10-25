// Copyright (C) 2026 ScyllaDB
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {VERSION, publish} = require('./publish-release.cjs');
const commit = 'a'.repeat(40);
const published = {tag_name: 'v1.2.2', draft: false, published_at: '2026-10-25', html_url: 'release'};
function fixture() {
  const calls = [];
  const github = {rest: {
    git: {
      getRef: async args => {calls.push(['ref', args]); return {data: {ref: 'refs/tags/v1.2.2', object: {type: 'commit', sha: commit}}};},
      getTag: async () => ({data: {object: {type: 'commit', sha: commit}}}),
    },
    repos: {
      getCommit: async args => {calls.push(['commit', args]); return {data: {sha: commit}};},
      getReleaseByTag: async () => {throw {status: 404};},
      getContent: async args => {assert.equal(args.ref, commit); return {data: {encoding: 'base64', content: Buffer.from('changelog:\n  exclude:\n    labels:\n      - omit-from-release-notes\n').toString('base64')}};},
      createRelease: async args => {calls.push(['create', args]); return {data: published};},
    },
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

test('verified tag publishes with generated notes and exact immutable target', async () => {
  const f = fixture();
  await publish(f);
  const request = f.calls.find(([type]) => type === 'create')[1];
  assert.equal(request.tag_name, 'v1.2.2');
  assert.equal(request.target_commitish, commit);
  assert.equal(request.generate_release_notes, true);
  assert.equal(request.draft, false);
  assert.equal(f.calls.filter(([type]) => type === 'ref').length, 3);
});

test('annotated tags peel to the event commit', async () => {
  const f = fixture();
  f.github.rest.git.getRef = async () => ({data: {ref: f.context.ref, object: {type: 'tag', sha: 'b'.repeat(40)}}});
  await publish(f);
  assert.equal(f.calls.at(-1)[0], 'create');
});

test('published releases are preserved, drafts and unpublished releases fail', async () => {
  for (const existing of [published, {...published, draft: true}, {...published, published_at: null}, {...published, tag_name: 'v9.9.9'}]) {
    const f = fixture();
    f.github.rest.repos.getReleaseByTag = async () => ({data: existing});
    if (existing === published) await publish(f);
    else await assert.rejects(publish(f), /Existing release/);
    assert.equal(f.calls.some(([type]) => type === 'create'), false);
  }
});

test('moved, deleted, mismatched, or malformed tags never publish', async () => {
  for (const failure of ['moved', 'deleted', 'wrong-ref', 'tag-cycle', 'moved-during-notes']) {
    const f = fixture(); let checks = 0;
    f.github.rest.git.getRef = async () => {
      checks++;
      if (failure === 'deleted') throw {status: 404};
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
  for (const endpoint of ['getCommit', 'getReleaseByTag', 'getContent']) {
    const f = fixture(); f.github.rest.repos[endpoint] = async () => {throw new Error(endpoint);};
    await assert.rejects(publish(f), new RegExp(endpoint));
    assert.equal(f.calls.some(([type]) => type === 'create'), false);
  }
  const f = fixture();
  f.github.rest.repos.getContent = async () => ({data: {encoding: 'base64', content: Buffer.from('changelog: {}').toString('base64')}});
  await assert.rejects(publish(f), /configuration/);
});

test('unexpected draft response cannot report publication success', async () => {
  const f = fixture();
  f.github.rest.repos.createRelease = async () => ({data: {...published, draft: true}});
  await assert.rejects(publish(f), /did not publish/);
});

// Copyright (C) 2026 ScyllaDB
const VERSION = /^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;

async function publish({github, context, core}) {
  const {owner, repo} = context.repo;
  const tag = context.ref.replace(/^refs\/tags\//, '');
  if (!context.ref.startsWith('refs/tags/') || !VERSION.test(tag) ||
      !/^[0-9a-f]{40}$/i.test(context.sha)) {
    throw new Error('Expected a stable vMAJOR.MINOR.PATCH tag and immutable event SHA');
  }
  // Resolve lightweight and annotated tags to commits. The event SHA remains
  // immutable even when the remote tag is subsequently moved or deleted.
  const eventCommit = (await github.rest.repos.getCommit({owner, repo, ref: context.sha})).data.sha;
  if (!/^[0-9a-f]{40}$/i.test(eventCommit || '')) throw new Error('Invalid event commit');
  async function verifyTag() {
    const ref = (await github.rest.git.getRef({owner, repo, ref: `tags/${tag}`})).data;
    if (ref.ref !== `refs/tags/${tag}`) throw new Error('Tag ref mismatch');
    let object = ref.object;
    const seen = new Set();
    while (object?.type === 'tag') {
      if (seen.has(object.sha) || seen.size >= 10) throw new Error('Invalid tag chain');
      seen.add(object.sha);
      object = (await github.rest.git.getTag({owner, repo, tag_sha: object.sha})).data.object;
    }
    if (object?.type !== 'commit' || object.sha !== eventCommit) {
      throw new Error('Tag no longer points to the triggering commit');
    }
  }
  await verifyTag();
  // Tag lookup returns published releases only. With this job's write access,
  // paginated listings also expose drafts that require manual resolution.
  const releases = await github.paginate(github.rest.repos.listReleases,
    {owner, repo, per_page: 100});
  const matches = releases.filter(release => release.tag_name === tag);
  if (matches.length > 1) throw new Error('Multiple releases match the tag; resolve manually');
  const existing = matches[0];
  if (existing) {
    if (existing.draft || !existing.published_at || existing.prerelease !== false) {
      throw new Error('Existing release is a draft, unpublished, or prerelease; resolve manually');
    }
    core.info(`Preserving published release ${tag}`);
    return;
  }
  // Generated notes use the release revision's configuration. Require the
  // canonical exclusion there, including when tagging an older commit.
  const config = (await github.rest.repos.getContent({owner, repo,
    path: '.github/release.yml', ref: eventCommit})).data;
  const text = Buffer.from(config.content || '', 'base64').toString('utf8');
  if (config.encoding !== 'base64' || !/labels:\s*\n\s*- omit-from-release-notes\s*(?:\n|$)/.test(text)) {
    throw new Error('Release-revision configuration lacks the canonical exclusion');
  }
  await verifyTag();
  const release = (await github.rest.repos.createRelease({owner, repo,
    tag_name: tag, target_commitish: eventCommit, name: tag,
    draft: false, prerelease: false, generate_release_notes: true})).data;
  if (release.tag_name !== tag || release.draft || !release.published_at || release.prerelease !== false) {
    throw new Error('GitHub did not publish the requested release');
  }
  await verifyTag();
  core.info(`Published ${release.html_url}`);
}

module.exports = {VERSION, publish};

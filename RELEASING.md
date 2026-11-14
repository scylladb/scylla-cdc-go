# GitHub releases and release notes

Push an existing release candidate as a stable version tag such as `v1.2.2`
(the current format is `vMAJOR.MINOR.PATCH`). The release workflow verifies the
exact tag still resolves to the triggering commit and creates a GitHub release
with generated notes. Prepare the version and tag before publication; this workflow
does not choose versions or publish packages. Tags outside the workflow's
`v[0-9]*.[0-9]*.[0-9]*` push filter are
ignored. Matching tags that fail the canonical stable-version check fail visibly.
Release jobs serialize per tag;
rerunning a successful job preserves the existing published release and curated
text. A draft, unpublished release, or prerelease under a stable tag fails visibly
and must be resolved manually before rerunning. The workflow must be on the default branch and the tagged commit must contain
`.github/release.yml` with the canonical exclusion before tagging. Existing tags do not trigger a run retroactively.

The verified existing tag determines the release revision. The request omits
`target_commitish` so older tags do not require workflow-write permission merely
because their workflow files differ from the default branch. Tag reads and release
creation are separate API operations. If the tag disappears between the last read
and creation, GitHub can recreate it at the default branch before the final check
fails. A tag move during that interval can also leave a created release requiring
manual correction. The workflow reports detected changes without rollback; it
does not provide an atomic remote-ref guarantee. Avoid moving or deleting a tag
while publication runs.

Generated notes exclude pull requests carrying `omit-from-release-notes`.
`actions/labeler` uses `.github/labeler.yml` to add it only when every reported
changed filename matches a known support path: `README.md`, this guide, Go
`_test.go` files, the test-only `testutils/utils.go`, the CI Docker Compose file,
GitHub automation, and the inspected ignore, lint, and Renovate configuration.
Mixtures of these paths also qualify. Other source files (including `doc.go`),
examples, module manifests, the Makefile, and unknown paths stay visible. Keep
the filename rules in sync with actual imports if `testutils` gains production
consumers. Documentation contents qualify even though Go may include those
documents in a module archive.

The workflow rechecks PR updates and closure, and `sync-labels: true` removes
the label when the filename rules stop matching. Jobs serialize by PR and read
trusted repository configuration without checking out PR code. The guard and
stock action share the canonical filename policy. Before checkout or dependency
installation, the workflow confirms the exact PR number using its trusted PR
event or a manual PR API lookup. Ordinary issues, unavailable lookups, and
mismatched or unconfirmed targets leave all labels unchanged. Before classification,
the guard reads paginated file metadata and checks the count and head/base revisions.
Both the source and destination of a rename must qualify. After the action, it
checks the same file snapshot and the actual label. If setup, classification, or
verification fails or is unexpectedly skipped for a confirmed PR, the final step
attempts to remove the unverified omission label using the saved confirmation, without needing a new
identity lookup or the installed helper. API outages can prevent cleanup;
failures remain visible. These before/after reads are not atomic and do not rule
out later changes or races. Do not use this label for runtime dependencies,
packaging, compatibility, API, performance, or security changes; author, title,
`chore`, and `dependencies` labels never hide a change.

Before a release, backfill previously merged support-only PRs from Actions →
**Label support-only release changes** → **Run workflow**, supplying each PR
number on the default branch. For example:

```sh
gh workflow run label-release-notes.yml --ref master -f pr_number=123
```

Manual dispatch from another branch or a tag fails before helper checkout;
the finalizer still attempts omission-label cleanup for a confirmed PR. The check
validates the dispatch entrypoint; trusted workflow code remains a prerequisite.

For every PR merged since the previous release and included in the intended tag,
run or rerun classification on the default branch. Check the job's final
`Verified head=... base=... count=... snapshot=... omit=...; actual label checked`
line against the current PR's head SHA, base SHA, and changed-file count:

```sh
gh api repos/scylladb/scylla-cdc-go/pulls/123 --jq '{head: .head.sha, base: .base.sha, count: .changed_files}'
gh pr view 123 --repo scylladb/scylla-cdc-go --json labels
```

Confirm the current omission label matches the verified result, and inspect the
release preview before tagging. A green job alone does not prove that an older
classification still matches the PR. New merges, PR changes, and manual label
changes require another check. Release publication remains asynchronous and does
not wait for classification. Maintainers must perform these checks manually;
the release workflow does not synchronize publication with classification.
Missed checks can leave omissions or unwanted support entries
in published notes; rerunning the publisher preserves those notes.

A manually applied omission label can also be removed by the next labeler run
if the PR contains visible changes. Maintainers can correct the filename rules when a
new support path is introduced; unknown paths are deliberately conservative.

The omission label controls GitHub release-note presentation. Local Jira label
callers ignore add/remove events for exactly `omit-from-release-notes`, including
manual changes, while preserving other events. The whole-name condition follows
Actions' case-insensitive string comparisons; suffixes and other names retain
normal routing. Default-`GITHUB_TOKEN` label events do not trigger those workflows.
Existing Jira copies are not cleaned, and other
synchronization paths can restore copied metadata; shared automation is unchanged.

Run the release-automation tests without starting Scylla:

```sh
npm ci --ignore-scripts --prefix .github
node --test .github/scripts/*.test.cjs
```

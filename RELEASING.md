# GitHub releases and release notes

Push an existing release candidate as a stable version tag such as `v1.2.2`
(the current format is `vMAJOR.MINOR.PATCH`). The release workflow verifies the
exact tag still resolves to the triggering commit and creates a GitHub release
with generated notes. It does not choose versions, create tags, or publish
packages. Unsupported tag formats fail visibly. Release jobs serialize per tag;
rerunning a successful job preserves the existing published release and curated
text. A draft or unpublished release fails visibly and must be resolved manually
before rerunning. The workflow must be on the default branch and the tagged commit must contain
`.github/release.yml` with the canonical exclusion before tagging. Existing tags do not trigger a run retroactively.

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
trusted repository configuration without checking out PR code. Classification
uses the filenames reported by the stock action, including its handling of
renames and API results. Do not use this label for runtime dependencies,
packaging, compatibility, API, performance, or security changes; author, title,
`chore`, and `dependencies` labels never hide a change.

Before a release, backfill previously merged support-only PRs from Actions →
**Label support-only release changes** → **Run workflow**, supplying each PR
number on the default branch. For example:

```sh
gh workflow run label-release-notes.yml --ref master -f pr_number=123
```

Wait for those runs to succeed before tagging, and inspect the release preview.
A manually applied omission label can also be removed by the next labeler run
if the PR contains visible changes. Maintainers can correct the filename rules when a
new support path is introduced; unknown paths are deliberately conservative.

Run the release publisher tests without starting Scylla:

```sh
node --test .github/scripts/*.test.cjs
```

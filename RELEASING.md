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
The label workflow adds it when every changed path is a known support file:
`README.md`, this guide, Go `_test.go` files, the test-only `testutils/utils.go`,
the CI Docker Compose file, GitHub automation, and the inspected ignore, lint,
and Renovate configuration. Mixtures of these paths also qualify. Other source
files (including `doc.go`), examples, module manifests, the Makefile, and unknown
paths stay visible. Keep the helper allowlist in sync with actual imports if
`testutils` gains production consumers. Documentation contents qualify even
though Go may include those documents in a module archive.

Classification checks both sides of renames and a complete, unique paginated
file listing with stable head/base commits and file count. PR updates and closure
recheck the label; a mixed PR removes it. API errors or incomplete/changing
snapshots fail the job and best-effort remove stale omission labels. Jobs
serialize by PR, and execute trusted default-branch code rather than PR code.
Do not use this label for runtime dependencies, packaging, compatibility, API,
performance, or security changes; author, title, `chore`, and `dependencies`
labels never hide a change.

Before a release, backfill previously merged support-only PRs from Actions →
**Label support-only release changes** → **Run workflow**, supplying each PR
number on the default branch. For example:

```sh
gh workflow run label-release-notes.yml --ref master -f pr_number=123
```

Wait for those runs to succeed before tagging, and inspect the release preview.
A manually applied omission label can also be removed by the next classifier run
if the PR contains visible changes. Maintainers can correct the allowlist when a
new support path is introduced; unknown paths are deliberately conservative.

Run the automation tests without starting Scylla:

```sh
node --test .github/scripts/*.test.cjs
```

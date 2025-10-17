# Contributing Guidelines

## Task Source of Truth

- All work items live in the GitHub Project board. Markdown files in this repository must not contain live task checkboxes.
- Any remaining checklist in historical docs should be replaced with a note that links to the corresponding GitHub tasks.
- Code comments that describe future work must reference an existing GitHub task using the format `# TODO(<owner>, GH-123): message`.

## Prohibited Patterns

- Unchecked Markdown checkboxes (`- `) outside the designated template directories (`docs/templates/**`, `prds/_template/**`).
- `# TODO` comments that do not include both an owner and a GitHub task identifier.

## Pull Request Expectations

Every pull request must:

1. Reference the relevant GitHub task identifiers in the PR body.
2. Include a concise changelog entry that can be copied into `CHANGELOG.md`.
3. Confirm that no disallowed checkboxes or TODO comments were introduced.

## Recommended Local Hooks

To run the repository hygiene checks before each commit:

```bash
git config core.hooksPath .githooks
chmod +x .githooks/pre-commit
```

The provided pre-commit hook executes the Markdown checkbox scanner and TODO linter described in `tools/`.

## Release Checklist Additions

1. Ensure the changelog entry references the relevant GitHub issues using `[#123]` or `GH-123`.
2. Run the changelog synchroniser to move completed items into the “Done” column (optionally include recent commits with `--commits-since <ref>`):
   ```bash
   python3 tools/mark_tasks_done.py --project-number 5 --owner @me --section Unreleased --commits-since v1.2.0 --close-issues --comment
   ```
3. (Optional) Trigger the `Changelog Sync` GitHub Action (`workflow_dispatch`) to perform the same reconciliation using the `PROJECT_SYNC_TOKEN` secret (a PAT with the `project` scope).

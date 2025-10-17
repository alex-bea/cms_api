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

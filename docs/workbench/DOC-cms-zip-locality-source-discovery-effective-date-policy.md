# CMS ZIP-Locality Source Discovery and Effective-Date Policy

**Status:** Active policy for production-readiness work
**Updated:** 2026-06-11
**Applies to:** CMS geography production ingestion and CMS pricing preflight

## Purpose

This policy defines how the harness selects the public CMS ZIP-locality source
package and how it decides whether a valuation date is covered by that source.
It exists so the production preflight runbook can use explicit gates instead of
rediscovering source and dating rules during a future cutover.

## Current Verified Source

The currently accepted CMS ZIP-locality source is:

- Source URL:
  `https://www.cms.gov/files/zip/zip-code-carrier-locality-file-revised-08/14/2025.zip`
- Local source file:
  `data/cms_raw/zip-code-carrier-locality-file-revised-08-14-2025.zip`
- Dataset ID: `ZIP_LOCALITY`
- Release ID: `zip_locality_2025_Q4`
- Source files: `ZIP5_OCT2025.txt`, `ZIP9_OCT2025.txt`
- Source quarter: `2025Q4`
- Strict source window: `2025-10-01` through `2025-12-31`
- Verified digest:
  `b14c414de73256ac9594d7cb0a58a75214ba04f4fe043468ffda507c3dd75c2e`

Verified readiness metrics:

- Total rows: `1,118,970`
- ZIP5 rows: `42,956`
- ZIP9 rows: `1,076,014`
- Rejected rows: `0`
- Duplicate source keys: `0`
- Locality `00` rows: `39,476`
- Probe ZIP: `94110 -> CA/05/01112`
- Special source-state probe: `66012 -> EK/00/05202`

## Source Discovery Policy

The accepted source is pinned to the verified CMS package above until a newer
CMS ZIP-locality package is explicitly reviewed. A new package is not accepted
just because it downloads successfully.

To accept a newer package, the operator or harness must:

1. Record the CMS URL, local source file path, source files, digest, release ID,
   source quarter, and effective window.
2. Prove the archive contains exactly one ZIP5 text source and one ZIP9 text
   source accepted by `cms_pricing.ingestion.parsers.cms_geography`.
3. Run the source scan and readiness gates against the full package.
4. Update this policy, the epic brief, and the production preflight runbook with
   the new digest, metrics, and expected probe rows.
5. Treat row-count floors, reject counts, duplicate-key counts, locality `00`
   preservation, and probe mismatches as blocking stop conditions.

The parser is the source contract. Manual file-name guessing, ad hoc fixed-width
parsing, or using only layout files is not sufficient evidence.

## Effective-Date Policy

Strict source-window mode is the default policy. In strict mode, a valuation
date must be inside the source quarter window derived from the CMS row
`year_quarter` field. For the current verified source, that means
`2025-10-01 <= valuation_date <= 2025-12-31`.

Latest-active/open-ended mode is an explicit exception. It may be used only when
all of these are true:

1. The current source is the newest verified public CMS ZIP-locality package.
2. The target valuation date is later than the strict source window.
3. The command explicitly passes `--open-ended-latest`.
4. The evidence report records `open_ended_latest=true` and
   `effective_to=null`.
5. The production preflight approval explicitly accepts latest-active geography
   behavior for the target valuation date.

Local/dev smoke may use latest-active mode for the `2026-07-01` RVU smoke date
because no newer package has been verified in this repo yet. A future production
mutation must default to strict mode unless the reviewed preflight approval
allows latest-active behavior for the specific release.

## Stop Conditions

Stop the harness or preflight if any of these occur:

- CMS URL, downloaded package digest, or source file names differ from this
  policy and the new source has not been reviewed.
- The package does not contain one accepted ZIP5 text source and one accepted
  ZIP9 text source.
- Source scan reports rejected rows or duplicate source keys.
- Total, ZIP5, ZIP9, or locality `00` row counts fall below the verified floors
  without a reviewed source-policy update.
- `94110` no longer resolves to `CA/05/01112`.
- Special source-state ZIP `66012` no longer resolves to `EK/00/05202`.
- CMS locality `00` is normalized to `01` or otherwise lost.
- Strict mode does not cover the target valuation date.
- Latest-active mode is needed but `--open-ended-latest` is absent, the evidence
  report does not record it, or approval has not accepted it.
- Post-RVU smoke evidence depends on `scripts/seed_post_rvu_load_local.py`.

## Evidence Commands

Strict source-window dry run, expected to block for `2026-07-01` until a newer
source is verified:

```bash
.venv/bin/python scripts/load_cms_geography_local.py \
  --dry-run \
  --require-valuation-date-coverage \
  --production-readiness-gates \
  --report-json /tmp/cms-geography-strict-readiness.json
```

Current local/dev latest-active dry run:

```bash
.venv/bin/python scripts/load_cms_geography_local.py \
  --dry-run \
  --open-ended-latest \
  --require-valuation-date-coverage \
  --production-readiness-gates \
  --report-json /tmp/cms-geography-open-ended-readiness.json
```

Current production-style local smoke plan:

```bash
.venv/bin/python scripts/run_cms_pricing_local_smoke.py \
  --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing \
  --dry-run-plan \
  --report-json /tmp/cms-pricing-local-smoke-plan.json
```

These commands are evidence only. Production mutation remains blocked until the
production preflight runbook names the approved source, mode, scoped reload
strategy, rollback plan, and final smoke checklist.

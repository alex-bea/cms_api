#!/usr/bin/env bash
set -euo pipefail

missing=()

for file in prds/*.md; do
  header=$(head -n 20 "$file")
  if ! grep -q "Status:" <<<"$header" \
     || ! grep -q "Owners:" <<<"$header" \
     || ! grep -q "Consumers:" <<<"$header" \
     || ! grep -q "Change control:" <<<"$header"; then
    missing+=("$file")
  fi
done

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "PRDs missing required governance header fields:" >&2
  printf '  %s\n' "${missing[@]}" >&2
  exit 1
fi

#!/usr/bin/env bash
set -euo pipefail

# Usage: ./tools/update-prd-refs.sh old_filename new_filename
# Example: ./tools/update-prd-refs.sh api_standards_architecture_prd_v_1.md API-STD-Architecture_prd_v1.0.md

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <old_name> <new_name>" >&2
  exit 1
fi

old=$1
new=$2

find prds -type f -name '*.md' -print0 \
  | xargs -0 perl -pi -e "s/\Q${old}\E/${new}/g"


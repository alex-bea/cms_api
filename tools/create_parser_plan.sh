#!/bin/bash
# Create standardized parser planning structure
# Usage: ./tools/create_parser_plan.sh {parser_name} {schema_id} {prd_name}

set -e

# Color codes for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 <parser_name> <schema_id> [prd_name]"
    echo ""
    echo "Example:"
    echo "  $0 mac_locality cms_mac_locality_v1.0 PRD-mac-locality-prd-v1.0.md"
    echo ""
    echo "Arguments:"
    echo "  parser_name: Snake_case name (e.g., mac_locality, oppscap)"
    echo "  schema_id:   Schema contract ID (e.g., cms_mac_locality_v1.0)"
    echo "  prd_name:    (Optional) Related PRD filename"
    exit 1
fi

PARSER_NAME="$1"
SCHEMA_ID="$2"
PRD_NAME="${3:-PRD-rvu-gpci-prd-v0.1.md}"  # Default PRD
TODAY=$(date +%Y-%m-%d)

# Paths
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PARSER_DIR="${BASE_DIR}/planning/parsers/${PARSER_NAME}"
TEMPLATE_FILE="${BASE_DIR}/planning/parsers/_template/README.md"
README_FILE="${PARSER_DIR}/README.md"

echo -e "${BLUE}=== Creating Parser Planning Structure ===${NC}"
echo ""

# Step 1: Create directory structure
echo -e "${YELLOW}Step 1: Creating directories...${NC}"
if [ -d "$PARSER_DIR" ]; then
    echo "  ⚠️  Directory already exists: $PARSER_DIR"
    read -p "  Continue and overwrite README? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "  ❌ Aborted"
        exit 1
    fi
else
    mkdir -p "${PARSER_DIR}/archive"
    echo -e "  ${GREEN}✓${NC} Created: ${PARSER_DIR}"
    echo -e "  ${GREEN}✓${NC} Created: ${PARSER_DIR}/archive"
fi

# Step 2: Copy and customize template
echo ""
echo -e "${YELLOW}Step 2: Creating README from template...${NC}"
if [ ! -f "$TEMPLATE_FILE" ]; then
    echo "  ❌ Template not found: $TEMPLATE_FILE"
    exit 1
fi

# Convert parser_name to display name (e.g., mac_locality → MAC Locality)
DISPLAY_NAME=$(echo "$PARSER_NAME" | sed 's/_/ /g' | awk '{for(i=1;i<=NF;i++) $i=toupper(substr($i,1,1)) tolower(substr($i,2)); }1')

# Create README with substitutions
sed -e "s/{PARSER_NAME}/${DISPLAY_NAME}/g" \
    -e "s/{schema_id}/${SCHEMA_ID}/g" \
    -e "s/YYYY-MM-DD/${TODAY}/g" \
    -e "s/{relevant-prd}/${PRD_NAME}/g" \
    -e "s/{parser_name}/${PARSER_NAME}/g" \
    "$TEMPLATE_FILE" > "$README_FILE"

echo -e "  ${GREEN}✓${NC} Created: ${README_FILE}"
echo -e "  ${GREEN}✓${NC} Substituted: {PARSER_NAME} → ${DISPLAY_NAME}"
echo -e "  ${GREEN}✓${NC} Substituted: {schema_id} → ${SCHEMA_ID}"
echo -e "  ${GREEN}✓${NC} Substituted: YYYY-MM-DD → ${TODAY}"

# Step 3: Update master index
echo ""
echo -e "${YELLOW}Step 3: Instructions for manual updates...${NC}"
echo ""
echo "  📝 TODO: Add entry to planning/parsers/README.md"
echo "     Section: §3 CMS Parsers (RVU Bundle)"
echo "     Template:"
echo ""
echo "     ### X. ${DISPLAY_NAME} Parser"
echo "     **Status:** ⏳ Planned"
echo "     **Location:** \`${PARSER_NAME}/\`"
echo "     **File:** (planned)"
echo "     **Schema:** \`${SCHEMA_ID}\`"
echo "     **Docs:** README only (stub)"
echo "     **Priority:** PX (TBD)"
echo ""
echo "  📝 TODO: Add backwards reference to PRD"
echo "     File: prds/${PRD_NAME}"
echo "     Section: Implementation Resources:"
echo "     Add:"
echo ""
echo "     - **${DISPLAY_NAME} Planning:** \`planning/parsers/${PARSER_NAME}/README.md\`"
echo "     - **Implementation Plan:** \`planning/parsers/${PARSER_NAME}/IMPLEMENTATION.md\` (when created)"
echo ""

# Step 4: Completion summary
echo ""
echo -e "${GREEN}=== Parser Planning Structure Created! ===${NC}"
echo ""
echo "  📁 Directory: ${PARSER_DIR}"
echo "  📄 README: ${README_FILE}"
echo "  📂 Archive: ${PARSER_DIR}/archive/"
echo ""
echo "Next steps:"
echo "  1. Review and customize ${README_FILE}"
echo "  2. Fill in remaining placeholders (search for 'TBD')"
echo "  3. Update planning/parsers/README.md (add entry)"
echo "  4. Add backwards reference to prds/${PRD_NAME}"
echo "  5. Create IMPLEMENTATION.md (copy from gpci/ as template)"
echo ""
echo "Reference:"
echo "  • Complete example: planning/parsers/gpci/"
echo "  • Simple example: planning/parsers/conversion_factor/"
echo "  • Standards: prds/STD-parser-contracts-prd-v1.0.md"
echo ""
echo -e "${GREEN}✓ Done!${NC}"


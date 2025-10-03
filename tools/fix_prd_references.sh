#!/bin/bash

# Script to fix old PRD filename references with underscores to new format with hyphens

echo "Fixing PRD filename references..."

# Define the mapping of old -> new filenames
declare -A replacements=(
    ["PRD-mpfs_prd_v1.0"]="PRD-mpfs-prd-v1.0"
    ["PRD-opps_prd_v1.0"]="PRD-opps-prd-v1.0"
    ["REF-geography-mapping-cursor_prd_v1.0"]="REF-geography-mapping-cursor-prd-v1.0"
    ["REF-nearest-zip-resolver_prd_v1.0"]="REF-nearest-zip-resolver-prd-v1.0"
    ["RUN-global-operations_prd_v1.0"]="RUN-global-operations-prd-v1.0"
    ["STD-api-architecture_prd_v1.0"]="STD-api-architecture-prd-v1.0"
    ["STD-api-contract-management_prd_v1.0"]="STD-api-contract-management-prd-v1.0"
    ["STD-api-security-and-auth_prd_v1.0"]="STD-api-security-and-auth-prd-v1.0"
    ["STD-data-architecture_prd_v1.0"]="STD-data-architecture-prd-v1.0"
    ["STD-doc-governance_prd_v1.0"]="STD-doc-governance-prd-v1.0"
    ["STD-observability-monitoring_prd_v1.0"]="STD-observability-monitoring-prd-v1.0"
    ["STD-qa-testing_prd_v1.0"]="STD-qa-testing-prd-v1.0"
    ["STD-scraper_prd_v1.0"]="STD-scraper-prd-v1.0"
)

# Also handle the .md versions
declare -A md_replacements=(
    ["PRD-mpfs_prd_v1.0.md"]="PRD-mpfs-prd-v1.0.md"
    ["PRD-opps_prd_v1.0.md"]="PRD-opps-prd-v1.0.md"
    ["REF-geography-mapping-cursor_prd_v1.0.md"]="REF-geography-mapping-cursor-prd-v1.0.md"
    ["REF-nearest-zip-resolver_prd_v1.0.md"]="REF-nearest-zip-resolver-prd-v1.0.md"
    ["RUN-global-operations_prd_v1.0.md"]="RUN-global-operations-prd-v1.0.md"
    ["STD-api-architecture_prd_v1.0.md"]="STD-api-architecture-prd-v1.0.md"
    ["STD-doc-governance_prd_v1.0.md"]="STD-doc-governance-prd-v1.0.md"
    ["STD-scraper_prd_v1.0.md"]="STD-scraper-prd-v1.0.md"
)

# Process each PRD file
for file in prds/*.md; do
    if [[ -f "$file" ]]; then
        echo "Processing $file..."
        
        # Create temporary file
        temp_file=$(mktemp)
        
        # Copy original content to temp file
        cp "$file" "$temp_file"
        
        # Apply replacements for non-.md references
        for old_ref in "${!replacements[@]}"; do
            new_ref="${replacements[$old_ref]}"
            sed -i.bak "s|${old_ref}|${new_ref}|g" "$temp_file"
        done
        
        # Apply replacements for .md references
        for old_ref in "${!md_replacements[@]}"; do
            new_ref="${md_replacements[$old_ref]}"
            sed -i.bak "s|${old_ref}|${new_ref}|g" "$temp_file"
        done
        
        # Replace original file with updated content
        mv "$temp_file" "$file"
        
        # Clean up backup files
        rm -f "$file.bak"
    fi
done

echo "PRD filename references fixed!"

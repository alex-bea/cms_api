#!/usr/bin/env bash
# Comprehensive audit suite runner
# Runs all documentation audits and optionally tests

set -Eeuo pipefail

echo "=========================================="
echo "CMS API Documentation Audit Suite"
echo "=========================================="
echo ""

# Parse arguments
WITH_TESTS=false
QUICK=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --with-tests)
            WITH_TESTS=true
            shift
            ;;
        --quick)
            QUICK=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --with-tests    Run documentation tests in addition to audits"
            echo "  --quick         Skip slow tests (only with --with-tests)"
            echo "  --help          Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                      # Run audits only"
            echo "  $0 --with-tests         # Run audits + all tests"
            echo "  $0 --with-tests --quick # Run audits + quick tests"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Track failures
FAILED=0
TOTAL=0

# Function to run audit
run_audit() {
    local name="$1"
    local command="$2"
    
    TOTAL=$((TOTAL + 1))
    
    echo "Running: $name"
    echo "------------------------------------------"
    
    if eval "$command"; then
        echo "✅ $name PASSED"
    else
        echo "❌ $name FAILED"
        FAILED=$((FAILED + 1))
    fi
    echo ""
}

# Core documentation audits
echo "Documentation Audits:"
echo ""

run_audit "Documentation Catalog" "python tools/audit_doc_catalog.py"
run_audit "Documentation Links" "python tools/audit_doc_links.py"
run_audit "Cross-References" "python tools/audit_cross_references.py"
run_audit "Documentation Metadata" "python tools/audit_doc_metadata.py"
run_audit "Documentation Dependencies" "python tools/audit_doc_dependencies.py"
run_audit "Companion Documents" "python tools/audit_companion_docs.py"
run_audit "Source Map Verification" "python tools/verify_source_map.py"
run_audit "Makefile .PHONY" "python tools/audit_makefile_phony.py"

# Run tests if requested
if [ "$WITH_TESTS" = true ]; then
    echo "=========================================="
    echo "Running Documentation Tests"
    echo "=========================================="
    echo ""
    
    run_audit "PRD Documentation Tests" "pytest tests/prd_docs -v --tb=short"
    
    if [ "$QUICK" = false ]; then
        run_audit "Scraper Tests" "pytest tests/scrapers -v --tb=short -k 'not performance'"
        run_audit "Ingestor Tests (Quick)" "pytest tests/ingestors -k 'not e2e' -v --tb=short"
    fi
fi

# Summary
echo "=========================================="
echo "AUDIT SUMMARY"
echo "=========================================="
echo ""

if [ $FAILED -eq 0 ]; then
    echo "✅ All $TOTAL checks passed!"
    echo ""
    exit 0
else
    PASSED=$((TOTAL - FAILED))
    echo "❌ $FAILED of $TOTAL checks failed"
    echo "✅ $PASSED of $TOTAL checks passed"
    echo ""
    exit 1
fi

    python tools/audit_changelog.py || ((fails+=1))

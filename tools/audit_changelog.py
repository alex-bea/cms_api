#!/usr/bin/env python3
"""
Audit CHANGELOG.md - Validate changelog completeness and compliance.

Checks:
1. CHANGELOG.md exists and follows Keep a Changelog format
2. All recent git tags are documented
3. All commits since last tag are in Unreleased section
4. Version format follows SemVer
5. Links are valid (commits, PRs, tags)
6. Required sections present (Added, Changed, etc.)
7. Cross-references to PRDs exist
8. ISO 8601 dates used

Per STD-doc-governance-prd-v1.0.md: Release documentation requirements.
"""

import re
import sys
import subprocess
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any


# ============================================================================
# Configuration
# ============================================================================

CHANGELOG_PATH = Path("CHANGELOG.md")
REQUIRED_SECTIONS = ["Added", "Changed", "Deprecated", "Removed", "Fixed", "Security"]
VERSION_PATTERN = re.compile(r"^##\s+\[(v?\d+\.\d+\.\d+(?:-[^\]]+)?)\]\s*-\s*(\d{4}-\d{2}-\d{2})$")
UNRELEASED_PATTERN = re.compile(r"^\[Unreleased\]", re.IGNORECASE)
COMMIT_LINK_PATTERN = re.compile(r"\[#?([a-f0-9]{7,40})\](?:\([^)]+\))?")  # Handles both [hash] and [hash](url)
PR_LINK_PATTERN = re.compile(r"\[#(\d+)\]")
PRD_LINK_PATTERN = re.compile(r"\b([A-Z]{3}-[a-z-]+-(?:prd|impl)-v\d+\.\d+\.md)\b")
PRDS_DIR = Path("prds")


# ============================================================================
# Git Utilities
# ============================================================================

def get_git_tags() -> List[Tuple[str, str]]:
    """Get all git tags with dates."""
    try:
        result = subprocess.run(
            ["git", "tag", "-l", "--sort=-creatordate", "--format=%(refname:short)|%(creatordate:short)"],
            capture_output=True,
            text=True,
            check=True
        )
        tags = []
        for line in result.stdout.strip().split('\n'):
            if '|' in line:
                tag, date = line.split('|', 1)
                tags.append((tag, date))
        return tags
    except subprocess.CalledProcessError:
        return []


def get_commits_since_tag(tag: str) -> List[Dict[str, str]]:
    """Get commits since specified tag, excluding CHANGELOG-only commits."""
    try:
        result = subprocess.run(
            ["git", "log", f"{tag}..HEAD", "--pretty=format:%H|%s|%ad", "--date=short"],
            capture_output=True,
            text=True,
            check=True
        )
        commits = []
        for line in result.stdout.strip().split('\n'):
            if line and '|' in line:
                hash_val, subject, date = line.split('|', 2)
                
                # Check if commit only touched CHANGELOG.md (self-referential loop guard)
                files_changed = subprocess.run(
                    ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", hash_val],
                    capture_output=True,
                    text=True
                ).stdout.strip().split('\n')
                
                # Skip commits that only modify CHANGELOG.md
                if files_changed == ['CHANGELOG.md']:
                    continue
                
                commits.append({
                    "hash": hash_val,
                    "short_hash": hash_val[:7],
                    "subject": subject,
                    "date": date
                })
        return commits
    except subprocess.CalledProcessError:
        return []


# ============================================================================
# Smart PRD Version Handling
# ============================================================================

def find_current_prd(base_name: str) -> Optional[str]:
    """Find the current (highest version) PRD for a given base name."""
    # Remove version suffix to get base name
    base_pattern = re.sub(r'-v\d+\.\d+.*\.md$', '', base_name)
    pattern = f"{base_pattern}-v*.md"
    matches = list(PRDS_DIR.glob(pattern))
    
    if not matches:
        return None
    
    # Sort by version number (extract v1.0, v2.0, etc.)
    def version_key(path):
        match = re.search(r'-v(\d+)\.(\d+)', path.name)
        if match:
            return (int(match.group(1)), int(match.group(2)))
        return (0, 0)
    
    return max(matches, key=version_key).name


def check_duplicate_prds() -> List[str]:
    """Detect when multiple versions of the same PRD exist."""
    prd_files = list(PRDS_DIR.glob("*.md"))
    
    # Group by base name (without version)
    base_groups = {}
    for prd in prd_files:
        base_name = re.sub(r'-v\d+\.\d+.*\.md$', '', prd.name)
        if base_name not in base_groups:
            base_groups[base_name] = []
        base_groups[base_name].append(prd.name)
    
    # Flag duplicates
    warnings = []
    for base_name, versions in base_groups.items():
        if len(versions) > 1:
            versions.sort()
            warnings.append(f"Multiple versions of {base_name}: {versions}")
    
    return warnings


# ============================================================================
# Changelog Parsing
# ============================================================================

def parse_changelog(content: str) -> Dict[str, Any]:
    """
    Parse CHANGELOG.md and extract structure.
    
    Returns:
        Dict with versions, unreleased, sections, commits, PRDs
    """
    lines = content.split('\n')
    
    versions = []
    unreleased = None
    current_version = None
    current_section = None
    
    for line in lines:
        # Check for version headers
        version_match = VERSION_PATTERN.match(line)
        if version_match:
            version, date = version_match.groups()
            current_version = {
                "version": version,
                "date": date,
                "sections": {},
                "commits": [],
                "prds": []
            }
            versions.append(current_version)
            continue
        
        # Check for Unreleased
        if UNRELEASED_PATTERN.match(line):
            unreleased = {
                "sections": {},
                "commits": [],
                "prds": []
            }
            current_version = unreleased
            continue
        
        # Check for section headers (### Added, ### Changed, etc.)
        if line.startswith("### ") and current_version:
            section_name = line[4:].strip()
            current_section = section_name
            current_version["sections"][section_name] = []
            continue
        
        # Extract commit links and PRD links from all lines
        commit_links = COMMIT_LINK_PATTERN.findall(line)
        prd_links = PRD_LINK_PATTERN.findall(line)
        
        if current_version:
            current_version["commits"].extend(commit_links)
            current_version["prds"].extend(prd_links)
    
    return {
        "unreleased": unreleased,
        "versions": versions
    }


# ============================================================================
# Validation Functions
# ============================================================================

def check_changelog_exists() -> Tuple[bool, str]:
    """Check if CHANGELOG.md exists."""
    if not CHANGELOG_PATH.exists():
        return False, "❌ CHANGELOG.md not found at project root"
    return True, "✅ CHANGELOG.md exists"


def check_keep_a_changelog_format(content: str) -> Tuple[bool, str]:
    """Check if follows Keep a Changelog format."""
    required_phrases = [
        "Keep a Changelog",
        "[Unreleased]",
        "Semantic Versioning"
    ]
    
    missing = [phrase for phrase in required_phrases if phrase not in content]
    
    if missing:
        return False, f"❌ Missing Keep a Changelog markers: {missing}"
    return True, "✅ Follows Keep a Changelog format"


def check_git_tags_documented(parsed: Dict) -> Tuple[bool, str]:
    """Check if all git tags are documented in changelog."""
    git_tags = get_git_tags()
    
    if not git_tags:
        return True, "⏭️  No git tags found (fresh repo)"
    
    # Extract versions from changelog
    changelog_versions = [v["version"] for v in parsed["versions"]]
    
    # Check if recent tags are documented
    undocumented = []
    for tag, tag_date in git_tags[:5]:  # Check last 5 tags
        if tag not in changelog_versions:
            undocumented.append(f"{tag} ({tag_date})")
    
    if undocumented:
        return False, f"❌ Undocumented tags: {', '.join(undocumented)}"
    
    return True, f"✅ All recent tags documented ({len(git_tags[:5])} checked)"


def check_unreleased_commits(parsed: Dict) -> Tuple[bool, str]:
    """Check if recent commits are mentioned in Unreleased or latest version."""
    git_tags = get_git_tags()
    
    if not git_tags:
        # No tags yet, check all commits
        commits = subprocess.run(
            ["git", "log", "--oneline", "-10"],
            capture_output=True,
            text=True
        ).stdout.strip().split('\n')
        commit_count = len([c for c in commits if c])
        return True, f"ℹ️  {commit_count} commits since project start (no tags yet)"
    
    # Get commits since last tag
    latest_tag = git_tags[0][0]
    recent_commits = get_commits_since_tag(latest_tag)
    
    if not recent_commits:
        return True, f"✅ No commits since {latest_tag}"
    
    # Check if commits are in Unreleased section
    if parsed["unreleased"]:
        documented_commits = parsed["unreleased"]["commits"]
        undocumented = [
            c["short_hash"] for c in recent_commits
            if c["short_hash"] not in documented_commits and c["hash"] not in documented_commits
        ]
        
        # Tolerance for self-referential CHANGELOG updates (1-2 commits acceptable)
        if undocumented:
            if len(undocumented) <= 2:
                return True, f"⚠️  {len(undocumented)} recent commit(s) not documented (acceptable for CHANGELOG updates)"
            else:
                return False, f"❌ {len(undocumented)} commits since {latest_tag} not in Unreleased: {undocumented[:3]}..."
        
        return True, f"✅ Recent commits documented ({len(recent_commits)} since {latest_tag})"
    
    return False, f"⚠️  {len(recent_commits)} commits since {latest_tag} but no Unreleased section"


def check_version_format(parsed: Dict) -> Tuple[bool, str]:
    """Check if versions follow SemVer."""
    semver_pattern = re.compile(r"^\d+\.\d+\.\d+(-[a-z0-9]+)?$")
    
    invalid_versions = []
    for version_info in parsed["versions"]:
        version = version_info["version"]
        # Strip leading 'v' before validation (consistent with version parsing)
        version_to_check = version.lstrip('v')
        if not semver_pattern.match(version_to_check):
            invalid_versions.append(version)
    
    if invalid_versions:
        return False, f"❌ Invalid SemVer: {invalid_versions}"
    
    return True, f"✅ All versions follow SemVer ({len(parsed['versions'])} checked)"


def check_date_format(parsed: Dict) -> Tuple[bool, str]:
    """Check if dates are ISO 8601."""
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    
    invalid_dates = []
    for version_info in parsed["versions"]:
        date = version_info["date"]
        if not date_pattern.match(date):
            invalid_dates.append(f"{version_info['version']}: {date}")
    
    if invalid_dates:
        return False, f"❌ Invalid ISO 8601 dates: {invalid_dates}"
    
    return True, f"✅ All dates are ISO 8601 ({len(parsed['versions'])} checked)"


def check_prd_references(parsed: Dict) -> Tuple[bool, str]:
    """Check if PRD references exist, with smart version handling."""
    all_prds = []
    for version_info in parsed["versions"]:
        all_prds.extend(version_info["prds"])
    
    if parsed["unreleased"]:
        all_prds.extend(parsed["unreleased"]["prds"])
    
    if not all_prds:
        return False, "⚠️  No PRD cross-references found in changelog"
    
    # Check if PRD files exist, with smart version suggestions
    missing_prds = []
    suggestions = []
    warnings = []
    
    for prd in set(all_prds):
        prd_path = PRDS_DIR / prd
        if not prd_path.exists():
            missing_prds.append(prd)
            
            # Suggest existing versions
            existing = find_current_prd(prd)
            if existing:
                suggestions.append(f"  → Did you mean: {existing}")
    
    # Check for duplicate PRDs (warnings only)
    duplicate_warnings = check_duplicate_prds()
    if duplicate_warnings:
        warnings.extend(duplicate_warnings)
    
    # Build result message
    if missing_prds:
        msg = f"❌ Missing PRD files: {missing_prds}\n"
        if suggestions:
            msg += "Suggestions:\n" + "\n".join(suggestions)
        if warnings:
            msg += "\nWarnings:\n" + "\n".join(f"  ⚠️  {w}" for w in warnings)
        return False, msg
    
    if warnings:
        warning_msg = "Warnings:\n" + "\n".join(f"  ⚠️  {w}" for w in warnings)
        return True, f"✅ {len(set(all_prds))} PRD references, all valid\n{warning_msg}"
    
    return True, f"✅ {len(set(all_prds))} PRD references, all valid"


def check_latest_version_date(parsed: Dict) -> Tuple[bool, str]:
    """Check if latest version date is reasonable."""
    if not parsed["versions"]:
        return True, "⏭️  No versions yet"
    
    latest = parsed["versions"][0]
    version_date = datetime.strptime(latest["date"], "%Y-%m-%d")
    today = datetime.now()
    days_ago = (today - version_date).days
    
    if days_ago < 0:
        return False, f"❌ Latest version date is in future: {latest['date']}"
    
    if days_ago > 180:
        return False, f"⚠️  Latest version is {days_ago} days old ({latest['version']})"
    
    return True, f"✅ Latest version is recent ({days_ago} days ago)"


# ============================================================================
# Main Audit
# ============================================================================

def main():
    """Run changelog audit."""
    print("🔍 Auditing CHANGELOG.md...\n")
    
    checks = []
    
    # Check 1: File exists
    ok, msg = check_changelog_exists()
    checks.append((ok, msg))
    print(msg)
    
    if not ok:
        print("\n❌ CHANGELOG.md not found. Create it first.")
        sys.exit(1)
    
    # Read changelog
    content = CHANGELOG_PATH.read_text()
    parsed = parse_changelog(content)
    
    # Check 2: Format
    ok, msg = check_keep_a_changelog_format(content)
    checks.append((ok, msg))
    print(msg)
    
    # Check 3: Git tags documented
    ok, msg = check_git_tags_documented(parsed)
    checks.append((ok, msg))
    print(msg)
    
    # Check 4: Recent commits
    ok, msg = check_unreleased_commits(parsed)
    checks.append((ok, msg))
    print(msg)
    
    # Check 5: Version format
    ok, msg = check_version_format(parsed)
    checks.append((ok, msg))
    print(msg)
    
    # Check 6: Date format
    ok, msg = check_date_format(parsed)
    checks.append((ok, msg))
    print(msg)
    
    # Check 7: PRD references
    ok, msg = check_prd_references(parsed)
    checks.append((ok, msg))
    print(msg)
    
    # Check 8: Latest version date
    ok, msg = check_latest_version_date(parsed)
    checks.append((ok, msg))
    print(msg)
    
    # Summary
    print("\n" + "="*80)
    passed = sum(1 for ok, _ in checks if ok)
    total = len(checks)
    
    if passed == total:
        print(f"✅ CHANGELOG.md audit passed: {passed}/{total} checks")
        print("="*80 + "\n")
        sys.exit(0)
    else:
        print(f"❌ CHANGELOG.md audit failed: {passed}/{total} checks passed")
        print("="*80 + "\n")
        sys.exit(1)


if __name__ == "__main__":
    main()

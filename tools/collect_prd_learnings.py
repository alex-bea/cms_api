#!/usr/bin/env python3
"""
Collect potential documentation updates (“PRD learnings”) based on recent code changes.

Usage (typical inside GitHub Actions):
    python tools/collect_prd_learnings.py \
        --base-sha "${{ github.event.pull_request.base.sha }}" \
        --event-path "${{ github.event_path }}" \
        --json-output learning_results.json \
        --markdown-output learning_results.md

The script inspects the git diff, PR metadata, and a declarative rules file
(.github/prd_learning_rules.yml) to infer which standards/PRDs likely need
attention. When suppression tokens are present (e.g., “[skip-prd-reminder]”),
no output is produced.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from fnmatch import fnmatch
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

import yaml

RULES_PATH = Path(".github/prd_learning_rules.yml")


@dataclass
class Learning:
    prd: str
    reasons: Set[str] = field(default_factory=set)
    files: Set[str] = field(default_factory=set)
    triggers: Set[str] = field(default_factory=set)

    def to_dict(self) -> Dict[str, object]:
        return {
            "prd": self.prd,
            "reasons": sorted(self.reasons),
            "files": sorted(self.files),
            "triggers": sorted(self.triggers),
        }


def run_git_cmd(args: List[str]) -> str:
    result = subprocess.run(
        ["git"] + args, check=False, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed: {result.stderr.strip() or result.stdout.strip()}"
        )
    return result.stdout


def load_rules() -> Dict[str, object]:
    if not RULES_PATH.exists():
        raise FileNotFoundError(
            f"Rules file not found: {RULES_PATH}. "
            "Create it before running the PRD learning collector."
        )
    with RULES_PATH.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def gather_changed_files(base_sha: Optional[str]) -> List[str]:
    """
    Return a list of changed files relative to base_sha (or the previous commit).
    """
    if base_sha:
        diff_range = f"{base_sha}..HEAD"
    else:
        diff_range = "HEAD~1..HEAD"

    output = run_git_cmd(["diff", "--name-only", diff_range])
    files = [
        line.strip()
        for line in output.splitlines()
        if line.strip()
    ]
    return files


def gather_commit_messages(base_sha: Optional[str]) -> str:
    """
    Concatenate commit messages in range base_sha..HEAD (exclusive of base).
    """
    if base_sha:
        range_arg = f"{base_sha}..HEAD"
    else:
        range_arg = "HEAD"
    output = run_git_cmd(["log", "--format=%B", range_arg])
    return output


def parse_event_metadata(event_path: Optional[str]) -> Dict[str, object]:
    if not event_path:
        return {}
    event_file = Path(event_path)
    if not event_file.exists():
        return {}
    try:
        return json.loads(event_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def suppression_present(
    tokens: Iterable[str],
    commit_messages: str,
    pr_title: str,
    pr_body: str,
    labels: Iterable[str],
) -> bool:
    haystack = "\n".join(
        [commit_messages or "", pr_title or "", pr_body or "", "\n".join(labels or [])]
    ).lower()
    return any(token.lower() in haystack for token in tokens)


def evaluate_rules(
    rules: Dict[str, object],
    changed_files: Iterable[str],
    commit_messages: str,
    event_metadata: Dict[str, object],
) -> List[Learning]:
    learnings: Dict[str, Learning] = {}
    pr_title = ""
    pr_body = ""
    pr_labels: List[str] = []

    if "pull_request" in event_metadata:
        pr = event_metadata["pull_request"]
        pr_title = pr.get("title", "") or ""
        pr_body = pr.get("body", "") or ""
        pr_labels = [lbl.get("name", "") for lbl in pr.get("labels", [])]

    suppression_tokens = rules.get("suppression_tokens", []) or []
    if suppression_present(
        suppression_tokens,
        commit_messages,
        pr_title,
        pr_body,
        pr_labels,
    ):
        return []

    def record(prd: str, reason: str, trigger: str, filepath: Optional[str] = None):
        entry = learnings.setdefault(prd, Learning(prd=prd))
        if reason:
            entry.reasons.add(reason)
        if trigger:
            entry.triggers.add(trigger)
        if filepath:
            entry.files.add(filepath)

    # Path-based rules
    for rule in rules.get("paths", []) or []:
        pattern = rule.get("pattern")
        if not pattern:
            continue
        reason = rule.get("reason", "")
        prds = rule.get("prds", [])
        for file_path in changed_files:
            normalized = file_path.replace("\\", "/")
            if fnmatch(normalized, pattern):
                for prd in prds:
                    record(prd, reason, f"path:{pattern}", normalized)

    # Keyword rules
    combined_text = "\n".join(
        [
            commit_messages or "",
            pr_title or "",
            pr_body or "",
        ]
    ).lower()
    for rule in rules.get("keywords", []) or []:
        trigger = rule.get("trigger")
        if not trigger:
            continue
        if trigger.lower() not in combined_text:
            continue
        reason = rule.get("reason", "")
        prds = rule.get("prds", [])
        for prd in prds:
            record(prd, reason, f"keyword:{trigger}")

    return sorted(learnings.values(), key=lambda item: item.prd)


def render_markdown(learnings: List[Learning]) -> str:
    if not learnings:
        return ""

    lines: List[str] = ["## PRD Learning Suggestions"]
    for entry in learnings:
        reasons = ", ".join(sorted(entry.reasons)) or "Triggered by rules"
        files = ", ".join(sorted(entry.files))
        triggers = ", ".join(sorted(entry.triggers))
        lines.append(
            f"- `{entry.prd}` — {reasons}"
            + (f". Files: {files}" if files else "")
            + (f". Triggers: {triggers}" if triggers else "")
        )
    lines.append("")
    lines.append("_Add `[skip-prd-reminder]` to suppress reminders on this branch._")
    return "\n".join(lines)


def write_output(path: Optional[str], content: str):
    if not path:
        return
    output_path = Path(path)
    output_path.write_text(content, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect PRD learning suggestions.")
    parser.add_argument("--base-sha", help="Base commit SHA to diff against.")
    parser.add_argument("--event-path", help="Path to GitHub event JSON payload.")
    parser.add_argument("--json-output", help="File to write JSON summary.")
    parser.add_argument("--markdown-output", help="File to write markdown summary.")
    args = parser.parse_args()

    try:
        rules = load_rules()
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    base_sha = args.base_sha or os.environ.get("PRD_LEARNING_BASE_SHA")
    changed_files = gather_changed_files(base_sha)
    commit_messages = gather_commit_messages(base_sha)
    event_metadata = parse_event_metadata(args.event_path or os.environ.get("GITHUB_EVENT_PATH"))

    learnings = evaluate_rules(rules, changed_files, commit_messages, event_metadata)

    summary_json = json.dumps({"items": [item.to_dict() for item in learnings]}, indent=2)
    write_output(args.json_output, summary_json + "\n")

    summary_md = render_markdown(learnings)
    write_output(args.markdown_output, summary_md + ("\n" if summary_md else ""))

    print(summary_md or "No PRD learnings detected.")

    # Exit code signals whether follow-up is needed (0 == no findings).
    return 0


if __name__ == "__main__":
    sys.exit(main())


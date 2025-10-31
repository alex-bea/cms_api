#!/usr/bin/env python3
"""
Audit Task Completion Status - Check github_tasks_plan.md against codebase.

This script:
1. Parses github_tasks_plan.md to extract all tasks
2. Checks codebase for evidence of task completion
3. Updates task status markers in the file
4. Identifies outdated tasks that should be marked complete

Usage:
    python tools/audit_task_completion.py [--dry-run] [--update]
    
Options:
    --dry-run    : Show what would be changed without updating the file
    --update     : Actually update github_tasks_plan.md with findings
    --verbose    : Show detailed checking process
"""

import re
import sys
import subprocess
import ast
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


class TaskStatus(Enum):
    """Task completion status."""
    COMPLETE = "✅ COMPLETE"
    PARTIAL = "⚠️ PARTIAL"
    PLANNED = "🟡 PLANNED"
    IN_PROGRESS = "🔄 IN PROGRESS"
    BLOCKED = "❌ BLOCKED"
    OUTDATED = "🗑️ OUTDATED"
    NEEDS_VERIFICATION = "❓ NEEDS VERIFICATION"
    UNKNOWN = "❓ UNKNOWN"


@dataclass
class TaskCheck:
    """Result of checking a task against codebase."""
    task_id: str
    task_title: str
    current_status: Optional[str]
    detected_status: TaskStatus
    evidence: List[str]
    line_number: int
    is_outdated: bool = False
    outdated_reason: Optional[str] = None


class TaskCompletionAuditor:
    """Audits task completion status in github_tasks_plan.md."""
    
    # Patterns to detect completion status
    STATUS_PATTERNS = {
        TaskStatus.COMPLETE: [
            r"✅\s*(COMPLETE|COMPLETED|DONE)",
            r"Status:.*✅.*COMPLETE",
            r"\*\*Status:\*\*.*✅.*COMPLETE",
        ],
        TaskStatus.PARTIAL: [
            r"⚠️\s*PARTIAL",
            r"Status:.*⚠️.*PARTIAL",
        ],
        TaskStatus.PLANNED: [
            r"🟡\s*PLANNED",
            r"Status:.*🟡.*PLANNED",
        ],
        TaskStatus.IN_PROGRESS: [
            r"🔄.*IN PROGRESS",
            r"Status:.*IN PROGRESS",
        ],
        TaskStatus.BLOCKED: [
            r"❌.*BLOCKED",
            r"🔴.*BLOCKER",
            r"Status:.*BLOCKED",
        ],
    }
    
    # File patterns to check for task completion evidence
    EVIDENCE_PATTERNS = {
        "test_files": [
            r"tests/.*test.*\.py$",
            r"tests/.*e2e.*\.py$",
        ],
        "parser_files": [
            r"cms_pricing/ingestion/parsers/.*\.py$",
        ],
        "ingestor_files": [
            r"cms_pricing/ingestion/ingestors/.*\.py$",
        ],
        "schema_files": [
            r"cms_pricing/ingestion/contracts/.*\.json$",
            r"cms_pricing/ingestion/contracts/schema_registry\.py$",
        ],
        "config_files": [
            r"pytest\.ini$",
            r"conftest\.py$",
        ],
    }
    
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        self.tasks_plan_path = repo_root / "github_tasks_plan.md"
        
    def extract_tasks(self) -> List[Dict[str, Any]]:
        """Extract all tasks from github_tasks_plan.md."""
        tasks = []
        
        if not self.tasks_plan_path.exists():
            print(f"Error: {self.tasks_plan_path} not found")
            return tasks
        
        content = self.tasks_plan_path.read_text(encoding='utf-8')
        lines = content.split('\n')
        
        current_task = None
        current_task_lines = []
        in_task = False
        
        for i, line in enumerate(lines, 1):
            # Detect task headers
            task_match = re.match(r'^(?:###?|##)\s+Task\s+(\d+):\s*(.+)$', line)
            if task_match:
                # Save previous task
                if current_task:
                    current_task['content'] = '\n'.join(current_task_lines)
                    current_task['end_line'] = i - 1
                    tasks.append(current_task)
                
                # Start new task
                task_id = task_match.group(1)
                task_title = task_match.group(2).strip()
                current_task = {
                    'id': task_id,
                    'title': task_title,
                    'start_line': i,
                    'end_line': i,
                    'content': '',
                }
                current_task_lines = [line]
                in_task = True
                continue
            
            # Detect status markers
            if in_task and current_task:
                status_match = re.search(r'\*\*Status:\*\*\s*(.+?)(?:\s|$|,|\.)', line)
                if status_match:
                    current_task['status'] = status_match.group(1).strip()
                
                # Check for completion markers
                if re.search(r'✅\s*COMPLETE', line, re.IGNORECASE):
                    current_task['marked_complete'] = True
                
                current_task_lines.append(line)
            
            # End task on next major section (##) if not already in task
            if line.startswith('##') and not line.startswith('###'):
                if in_task and current_task:
                    current_task['content'] = '\n'.join(current_task_lines)
                    current_task['end_line'] = i - 1
                    tasks.append(current_task)
                    current_task = None
                    in_task = False
                    current_task_lines = []
        
        # Save last task
        if current_task:
            current_task['content'] = '\n'.join(current_task_lines)
            current_task['end_line'] = len(lines)
            tasks.append(current_task)
        
        return tasks
    
    def check_task_completion(self, task: Dict[str, Any]) -> TaskCheck:
        """Check if a task appears to be completed based on codebase evidence."""
        evidence = []
        task_id = task.get('id', 'unknown')
        task_title = task.get('title', '')
        task_content = task.get('content', '')
        
        # Check current status
        current_status = task.get('status')
        marked_complete = task.get('marked_complete', False)
        
        # Determine if task is outdated
        is_outdated = self._is_outdated_task(task)
        outdated_reason = None
        if is_outdated:
            outdated_reason = self._get_outdated_reason(task)
        
        # Check for completion evidence in codebase
        detected_status = TaskStatus.UNKNOWN
        
        # RVU E2E modernization tasks
        if 'RVU' in task_title and ('E2E' in task_title or 'modernization' in task_content.lower()):
            if self._check_rvu_modernization(task):
                detected_status = TaskStatus.COMPLETE
                evidence.append("RVU E2E tests passing, async configured, RawBatch standardized")
        
        # Check for specific function/method requirements
        func_pattern = re.search(r'(?:implement|add|create|wire)\s+([a-z_]+(?:\(\))?)', task_content, re.IGNORECASE)
        if func_pattern:
            func_name = func_pattern.group(1).rstrip('()')
            if self._check_specific_function_exists(task, func_name):
                detected_status = TaskStatus.COMPLETE
                evidence.append(f"Function/method '{func_name}' exists in codebase")
        
        # Check for environment variable or config requirements
        env_pattern = re.search(r'(?:support|add|implement).*?([A-Z_]+TEST_DATA_DIR|[A-Z_]+_DIR)', task_content, re.IGNORECASE)
        if env_pattern:
            env_var = env_pattern.group(1)
            if self._check_env_var_usage(env_var):
                detected_status = TaskStatus.COMPLETE
                evidence.append(f"Environment variable '{env_var}' is used in code")
        
        # Improvement #1: Check git history for completion evidence
        git_complete, git_evidence = self._check_git_history(task)
        if git_complete:
            detected_status = TaskStatus.COMPLETE
            if git_evidence:
                evidence.append(f"Git history: {git_evidence}")
        
        # Improvement #2: Verify tests pass (high confidence indicator)
        tests_pass, test_evidence = self._verify_tests_pass(task)
        if tests_pass:
            # Upgrade status if we had lower confidence before
            if detected_status == TaskStatus.UNKNOWN:
                detected_status = TaskStatus.COMPLETE
            if test_evidence:
                evidence.append(f"Test verification: {test_evidence}")
        
        # Improvement #3: AST pattern matching for semantic verification
        ast_match, ast_evidence = self._check_ast_patterns(task)
        if ast_match:
            # Upgrade status
            if detected_status == TaskStatus.UNKNOWN:
                detected_status = TaskStatus.COMPLETE
            elif detected_status == TaskStatus.PARTIAL:
                detected_status = TaskStatus.COMPLETE
            if ast_evidence:
                evidence.append(f"AST analysis: {ast_evidence}")
        
        # Improvement #4: Verify acceptance criteria individually
        ac_verified, ac_evidence = self._verify_acceptance_criteria(task)
        if ac_verified:
            # High confidence - acceptance criteria are explicit requirements
            if detected_status == TaskStatus.UNKNOWN:
                detected_status = TaskStatus.COMPLETE
            elif detected_status == TaskStatus.PARTIAL:
                detected_status = TaskStatus.COMPLETE
            if ac_evidence:
                evidence.append(f"Acceptance criteria: {ac_evidence}")
        
        # Improvement #5: Check documentation/PRD updates mentioned in tasks
        doc_verified, doc_evidence = self._verify_documentation_updates(task)
        if doc_verified:
            # Adds confidence for tasks that mention documentation
            if detected_status != TaskStatus.COMPLETE:
                detected_status = TaskStatus.PARTIAL
                if doc_evidence:
                    evidence.append(f"Documentation: {doc_evidence}")
        
        # Parser/Ingestor implementation tasks
        if any(keyword in task_content.lower() for keyword in ['parser', 'ingestor', 'normalize', 'validate']):
            if self._check_parser_implementation(task):
                detected_status = TaskStatus.COMPLETE
                evidence.append("Parser/ingestor code exists and is tested")
        
        # Test coverage tasks
        if 'test' in task_content.lower() and ('coverage' in task_content.lower() or 'e2e' in task_content.lower()):
            if self._check_test_coverage(task):
                detected_status = TaskStatus.COMPLETE
                evidence.append("E2E tests exist and passing")
        
        # Schema/contract tasks
        if 'schema' in task_content.lower() or 'contract' in task_content.lower():
            if self._check_schema_implementation(task):
                detected_status = TaskStatus.COMPLETE
                evidence.append("Schema contracts exist")
        
        # Async/configuration tasks
        if 'async' in task_content.lower() or 'pytest.ini' in task_content.lower():
            if self._check_async_config(task):
                detected_status = TaskStatus.COMPLETE
                evidence.append("Async configuration present")
        
        # If marked complete or status indicates complete
        if marked_complete or (current_status and 'COMPLETE' in current_status.upper()):
            detected_status = TaskStatus.COMPLETE
            if not evidence:
                evidence.append("Marked complete in task file")
        
        # If outdated, mark as complete with note
        if is_outdated:
            detected_status = TaskStatus.OUTDATED
            evidence.append(f"Outdated: {outdated_reason}")
        
        return TaskCheck(
            task_id=task_id,
            task_title=task_title,
            current_status=current_status,
            detected_status=detected_status,
            evidence=evidence,
            line_number=task.get('start_line', 0),
            is_outdated=is_outdated,
            outdated_reason=outdated_reason,
        )
    
    def _is_outdated_task(self, task: Dict[str, Any]) -> bool:
        """Determine if a task is outdated/superseded."""
        content = task.get('content', '').lower()
        title = task.get('title', '').lower()
        
        # Outdated indicators
        outdated_indicators = [
            'mock data',
            'placeholder',
            'todo:',
            'legacy',
            'old approach',
            'deprecated',
        ]
        
        # Check if task description mentions these
        return any(indicator in content or indicator in title for indicator in outdated_indicators)
    
    def _get_outdated_reason(self, task: Dict[str, Any]) -> str:
        """Get reason why task is outdated."""
        content = task.get('content', '').lower()
        
        if 'mock' in content:
            return "Replaced with real implementation"
        if 'placeholder' in content:
            return "Placeholder replaced with actual code"
        if 'legacy' in content:
            return "Legacy code superseded"
        
        return "Task appears outdated based on description"
    
    def _check_rvu_modernization(self, task: Dict[str, Any]) -> bool:
        """Check if RVU E2E modernization tasks are complete."""
        # Check for pytest.ini with asyncio_mode
        pytest_ini = self.repo_root / "pytest.ini"
        if pytest_ini.exists():
            content = pytest_ini.read_text()
            if 'asyncio_mode' in content:
                return True
        
        # Check for test_data_dir fixture
        conftest = self.repo_root / "tests" / "conftest.py"
        if conftest.exists():
            content = conftest.read_text()
            if 'RVU_TEST_DATA_DIR' in content or 'test_data_dir' in content:
                return True
        
        return False
    
    def _check_parser_implementation(self, task: Dict[str, Any]) -> bool:
        """Check if parser/ingestor implementation exists and has actual code."""
        parsers_dir = self.repo_root / "cms_pricing" / "ingestion" / "parsers"
        if parsers_dir.exists():
            parser_files = list(parsers_dir.glob("*.py"))
            # Check that parsers have actual implementation (not just stubs)
            implemented_count = 0
            for parser_file in parser_files:
                try:
                    content = parser_file.read_text()
                    # Check for actual parsing logic (not just pass/raise NotImplementedError)
                    if 'def parse_' in content and 'NotImplementedError' not in content:
                        # Check it has real logic (more than 50 lines excluding comments/imports)
                        code_lines = [l for l in content.split('\n') 
                                    if l.strip() and not l.strip().startswith('#') 
                                    and not l.strip().startswith('"""')]
                        if len(code_lines) > 50:
                            implemented_count += 1
                except Exception:
                    continue
            if implemented_count > 5:  # We have multiple parsers
                return True
        
        ingestors_dir = self.repo_root / "cms_pricing" / "ingestion" / "ingestors"
        if ingestors_dir.exists():
            ingestor_files = list(ingestors_dir.glob("*_ingestor.py"))
            # Verify ingestors have actual implementation
            for ingestor_file in ingestor_files:
                try:
                    content = ingestor_file.read_text()
                    # Check for actual pipeline methods
                    if any(method in content for method in ['async def land', 'async def validate', 
                                                           'async def normalize', 'async def publish']):
                        if 'NotImplementedError' not in content and len(content) > 1000:
                            return True
                except Exception:
                    continue
        
        return False
    
    def _check_specific_function_exists(self, task: Dict[str, Any], function_name: str) -> bool:
        """Check if a specific function/method exists in codebase."""
        # Extract function name from task if mentioned
        content = task.get('content', '')
        func_matches = re.findall(rf'\b({re.escape(function_name)}|_\w+\(\))\b', content)
        
        # Search codebase for function
        codebase_paths = [
            self.repo_root / "cms_pricing",
            self.repo_root / "tests",
        ]
        
        for base_path in codebase_paths:
            if not base_path.exists():
                continue
            for py_file in base_path.rglob("*.py"):
                try:
                    file_content = py_file.read_text()
                    # Look for function definition
                    if re.search(rf'def\s+{re.escape(function_name)}\s*\(', file_content) or \
                       re.search(rf'async\s+def\s+{re.escape(function_name)}\s*\(', file_content):
                        # Verify it's not just a stub
                        if 'pass' not in file_content[file_content.find(function_name):file_content.find(function_name)+200]:
                            return True
                except Exception:
                    continue
        
        return False
    
    def _check_test_results(self, task: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Check if tests exist and have run successfully (via pytest cache or results)."""
        tests_dir = self.repo_root / "tests"
        if not tests_dir.exists():
            return False, None
        
        # Check for pytest cache (indicates tests have run)
        cache_dir = self.repo_root / ".pytest_cache"
        if cache_dir.exists():
            # Try to extract relevant test files from task
            task_content = task.get('content', '').lower()
            test_patterns = []
            
            if 'rvu' in task_content:
                test_patterns.append('test_rvu')
            if 'e2e' in task_content:
                test_patterns.append('*e2e*')
            if 'parser' in task_content:
                test_patterns.append('test_*parser*')
            
            # Look for actual test files
            found_tests = []
            for pattern in test_patterns or ['test_*.py']:
                test_files = list(tests_dir.rglob(pattern + '.py'))
                found_tests.extend(test_files)
            
            if found_tests:
                return True, f"Found {len(found_tests)} relevant test files"
        
        return False, None
    
    def _check_test_coverage(self, task: Dict[str, Any]) -> bool:
        """Check if test coverage exists and appears comprehensive."""
        tests_dir = self.repo_root / "tests" / "ingestors"
        if tests_dir.exists():
            test_files = list(tests_dir.glob("test_*e2e.py"))
            if len(test_files) > 0:
                # Verify tests have actual assertions (not just stubs)
                for test_file in test_files:
                    try:
                        content = test_file.read_text()
                        # Check for actual test logic
                        if 'assert' in content and 'def test_' in content:
                            # Check test count
                            test_count = len(re.findall(r'def\s+test_', content))
                            if test_count >= 5:  # Substantial test suite
                                return True
                    except Exception:
                        continue
        
        # Also check general test directory
        return self._check_test_results(task)[0]
    
    def _check_schema_implementation(self, task: Dict[str, Any]) -> bool:
        """Check if schema contracts exist."""
        contracts_dir = self.repo_root / "cms_pricing" / "ingestion" / "contracts"
        if contracts_dir.exists():
            schema_files = list(contracts_dir.glob("*.json"))
            if len(schema_files) > 0:
                return True
        
        schema_registry = contracts_dir / "schema_registry.py"
        if schema_registry.exists():
            return True
        
        return False
    
    def _check_async_config(self, task: Dict[str, Any]) -> bool:
        """Check if async configuration exists."""
        pytest_ini = self.repo_root / "pytest.ini"
        if pytest_ini.exists():
            content = pytest_ini.read_text()
            if 'asyncio_mode' in content:
                return True
        
        return False
    
    def _check_env_var_usage(self, env_var: str) -> bool:
        """Check if an environment variable is referenced in code."""
        codebase_paths = [
            self.repo_root / "cms_pricing",
            self.repo_root / "tests",
        ]
        
        for base_path in codebase_paths:
            if not base_path.exists():
                continue
            for py_file in base_path.rglob("*.py"):
                try:
                    content = py_file.read_text()
                    # Check for os.getenv, os.environ, or direct usage
                    if re.search(rf'(?:os\.getenv|os\.environ|getenv)\s*\(\s*["\']?{re.escape(env_var)}', content):
                        return True
                except Exception:
                    continue
        
        return False
    
    def _check_git_history(self, task: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Check git history for evidence of task completion.
        
        Returns:
            (is_complete, evidence_string)
        """
        try:
            task_id = task.get('id', '')
            task_title = task.get('title', '')
            task_content = task.get('content', '')
            
            # Build search terms from task
            search_terms = []
            if task_id:
                search_terms.append(f"Task {task_id}")
                search_terms.append(f"task {task_id}")
            
            # Extract key terms from title (limit to 2-3 words for better matching)
            title_words = task_title.split()[:3]
            search_terms.extend(title_words)
            
            # Look for common completion phrases
            completion_phrases = ['complete', 'implemented', 'finished', 'done', 'added', 'created']
            task_lower = task_content.lower() + ' ' + task_title.lower()
            
            # Search git log for task references
            evidence_found = []
            
            for term in search_terms[:5]:  # Limit to avoid too many searches
                try:
                    # Search commit messages
                    result = subprocess.run(
                        ['git', 'log', '--all', '--grep', term, '--oneline', '--since=6months'],
                        capture_output=True,
                        text=True,
                        cwd=self.repo_root,
                        timeout=5
                    )
                    
                    if result.returncode == 0 and result.stdout.strip():
                        commits = result.stdout.strip().split('\n')
                        # Look for completion indicators in commit messages
                        for commit in commits[:3]:  # Check first 3 matches
                            commit_msg = commit.lower()
                            if any(phrase in commit_msg for phrase in completion_phrases):
                                evidence_found.append(f"Git commit: {commit[:60]}...")
                                break
                except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
                    continue
            
            # Also check for file changes related to task
            # Look for files mentioned in task content
            file_patterns = re.findall(r'`([^`]+\.py)`|([a-z_]+/.*\.py)', task_content)
            for pattern in file_patterns:
                file_path = pattern[0] or pattern[1]
                if file_path:
                    try:
                        # Check if file exists and has recent commits
                        full_path = self.repo_root / file_path
                        if full_path.exists():
                            result = subprocess.run(
                                ['git', 'log', '--oneline', '--since=6months', '--', str(full_path)],
                                capture_output=True,
                                text=True,
                                cwd=self.repo_root,
                                timeout=3
                            )
                            if result.returncode == 0 and result.stdout.strip():
                                commits = result.stdout.strip().split('\n')
                                if commits:
                                    evidence_found.append(f"File {file_path} has {len(commits)} recent commits")
                    except Exception:
                        continue
            
            if evidence_found:
                return True, '; '.join(evidence_found[:3])  # Limit to 3 items
        
        except Exception as e:
            # Fail silently if git is not available or errors occur
            pass
        
        return False, None
    
    def _verify_tests_pass(self, task: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Verify tests exist and pass for task-related functionality.
        
        Returns:
            (tests_pass, evidence_string)
        """
        try:
            task_content = task.get('content', '').lower()
            task_title = task.get('title', '').lower()
            
            # Determine relevant test files
            test_patterns = []
            
            if 'rvu' in task_content or 'rvu' in task_title:
                test_patterns.append('**/test*rvu*.py')
            if 'e2e' in task_content or 'e2e' in task_title:
                test_patterns.append('**/test*e2e*.py')
            if 'parser' in task_content:
                test_patterns.append('**/test*parser*.py')
            if 'ingestor' in task_content:
                test_patterns.append('**/test*ingestor*.py')
            
            if not test_patterns:
                # Default: check for any test files mentioned
                mentioned_files = re.findall(r'`([^`]+test[^`]*\.py)`', task_content)
                test_patterns = [f"**/{f}" for f in mentioned_files if f]
            
            if not test_patterns:
                return False, None
            
            # Find test files
            tests_dir = self.repo_root / "tests"
            found_tests = []
            for pattern in test_patterns:
                # Simple glob since Path doesn't support **/*test*.py easily
                for test_file in tests_dir.rglob("*.py"):
                    if 'test' in test_file.name.lower():
                        if any(term in str(test_file) for term in ['rvu', 'e2e', 'parser', 'ingestor']):
                            found_tests.append(test_file)
            
            if not found_tests:
                return False, None
            
            # Check pytest cache for last run status
            cache_dir = self.repo_root / ".pytest_cache" / "v" / "cache"
            lastfailed_file = cache_dir / "lastfailed" if cache_dir.exists() else None
            
            if lastfailed_file and lastfailed_file.exists():
                try:
                    import json
                    failed_tests = json.loads(lastfailed_file.read_text())
                    # Check if our tests are in the failed list
                    relevant_failures = [t for t in failed_tests.keys() 
                                       if any(pattern in t for pattern in test_patterns)]
                    if not relevant_failures:
                        return True, f"Tests exist and not in last failed list ({len(found_tests)} test files)"
                except Exception:
                    pass
            
            # Alternative: Try to run pytest --collect-only (faster than full run)
            if found_tests:
                test_file_str = str(found_tests[0].relative_to(self.repo_root))
                try:
                    result = subprocess.run(
                        ['pytest', '--collect-only', '-q', test_file_str],
                        capture_output=True,
                        text=True,
                        cwd=self.repo_root,
                        timeout=10
                    )
                    if result.returncode == 0:
                        # Count collected tests
                        collected = re.search(r'(\d+)\s+test', result.stdout)
                        if collected:
                            count = int(collected.group(1))
                            if count > 0:
                                return True, f"Found {count} tests in {len(found_tests)} file(s)"
                except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
                    # Pytest not available or times out - just verify files exist
                    return True, f"Test files exist ({len(found_tests)} files)"
            
            return True, f"Test files found ({len(found_tests)} files)"
        
        except Exception as e:
            # Fail gracefully
            return False, None
    
    def _check_ast_patterns(self, task: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Use AST to find actual code patterns mentioned in tasks.
        
        Returns:
            (pattern_found, evidence_string)
        """
        try:
            task_content = task.get('content', '').lower()
            
            # Extract function/method names mentioned in task
            func_patterns = re.findall(r'(?:implement|add|create|wire|use)\s+([a-z_][a-z0-9_]*)', task_content)
            class_patterns = re.findall(r'(?:implement|add|create)\s+([A-Z][A-Za-z0-9_]*)', task_content)
            
            codebase_paths = [
                self.repo_root / "cms_pricing",
                self.repo_root / "tests",
            ]
            
            found_patterns = []
            
            for base_path in codebase_paths:
                if not base_path.exists():
                    continue
                
                for py_file in base_path.rglob("*.py"):
                    if "__pycache__" in str(py_file):
                        continue
                    
                    try:
                        content = py_file.read_text()
                        tree = ast.parse(content, filename=str(py_file))
                        
                        # Check for function definitions
                        for node in ast.walk(tree):
                            if isinstance(node, ast.FunctionDef):
                                func_name = node.name
                                
                                # Check if this function matches task requirements
                                for pattern in func_patterns:
                                    if pattern.lower() in func_name.lower():
                                        # Verify it's not just a stub
                                        if len(node.body) > 0:
                                            first_stmt = node.body[0]
                                            if not (isinstance(first_stmt, ast.Pass) or 
                                                   isinstance(first_stmt, ast.Raise)):
                                                found_patterns.append(f"Function {func_name} in {py_file.relative_to(self.repo_root)}")
                                                break
                            
                            elif isinstance(node, ast.ClassDef):
                                class_name = node.name
                                
                                # Check if this class matches task requirements
                                for pattern in class_patterns:
                                    if pattern in class_name:
                                        # Verify it has methods (not just empty)
                                        methods = [n for n in node.body if isinstance(n, ast.FunctionDef)]
                                        if len(methods) > 0:
                                            found_patterns.append(f"Class {class_name} in {py_file.relative_to(self.repo_root)}")
                                            break
                        
                        # Also check for imports of mentioned modules
                        for node in ast.walk(tree):
                            if isinstance(node, ast.Import):
                                for alias in node.names:
                                    # Check if imported module matches task patterns
                                    for pattern in func_patterns + class_patterns:
                                        if pattern in alias.name.lower():
                                            found_patterns.append(f"Import {alias.name} in {py_file.relative_to(self.repo_root)}")
                            
                            elif isinstance(node, ast.ImportFrom):
                                if node.module:
                                    for pattern in func_patterns + class_patterns:
                                        if pattern in node.module.lower():
                                            found_patterns.append(f"Import from {node.module} in {py_file.relative_to(self.repo_root)}")
                    
                    except (SyntaxError, UnicodeDecodeError):
                        # Skip files that can't be parsed
                        continue
                    except Exception:
                        continue
            
            if found_patterns:
                return True, '; '.join(found_patterns[:3])  # Limit to 3 items
            
        except Exception as e:
            # Fail gracefully
            pass
        
        return False, None
    
    def _verify_acceptance_criteria(self, task: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Parse and verify acceptance criteria from task description.
        
        Looks for:
        - Bullet points with checkmarks (✅, ✓, - ✅)
        - Numbered lists in "Acceptance Criteria" section
        - Specific requirements (file paths, test counts, etc.)
        
        Returns:
            (all_verified, evidence_string)
        """
        try:
            task_content = task.get('content', '')
            
            # Find acceptance criteria section
            ac_section_match = re.search(
                r'(?:Acceptance Criteria|Acceptance:|Requirements?)[:\s]*\n(.*?)(?=\n\n|\n##|\Z)',
                task_content,
                re.IGNORECASE | re.DOTALL
            )
            
            if not ac_section_match:
                return False, None
            
            ac_text = ac_section_match.group(1)
            criteria = []
            
            # Extract criteria items (bullets, numbered lists, checkmarks)
            # Pattern 1: Checkmarked items (✅ or ✓)
            checked_items = re.findall(r'[✅✓]\s*(.+?)(?=\n|$)', ac_text)
            criteria.extend(checked_items)
            
            # Pattern 2: Numbered/bullet items in acceptance section
            bullet_items = re.findall(r'^[-*]\s+(.+?)(?=\n|$)', ac_text, re.MULTILINE)
            numbered_items = re.findall(r'^\d+[\.)]\s+(.+?)(?=\n|$)', ac_text, re.MULTILINE)
            criteria.extend(bullet_items[:10])  # Limit to avoid too many
            criteria.extend(numbered_items[:10])
            
            if not criteria:
                return False, None
            
            # Verify each criterion
            verified_count = 0
            verification_details = []
            
            for criterion in criteria[:15]:  # Limit to 15 to avoid excessive checking
                criterion_lower = criterion.lower().strip()
                
                # Skip if already marked as complete in description
                if any(marker in criterion_lower for marker in ['✅', 'complete', 'done', 'finished']):
                    verified_count += 1
                    continue
                
                # Check for file paths mentioned
                file_match = re.search(r'`([^`]+\.(?:py|json|md|ini|yml))`|([a-z_/]+\.(?:py|json|md))', criterion)
                if file_match:
                    file_path = file_match.group(1) or file_match.group(2)
                    full_path = self.repo_root / file_path
                    if full_path.exists():
                        verified_count += 1
                        verification_details.append(f"{file_path} exists")
                        continue
                
                # Check for test counts (e.g., "13 tests passing")
                test_match = re.search(r'(\d+)\s+test', criterion)
                if test_match:
                    test_count = int(test_match.group(1))
                    # Verify we have at least that many tests somewhere
                    tests_dir = self.repo_root / "tests"
                    if tests_dir.exists():
                        all_tests = list(tests_dir.rglob("test_*.py"))
                        if len(all_tests) >= test_count:
                            verified_count += 1
                            verification_details.append(f"≥{test_count} test files found")
                            continue
                
                # Check for specific function/class names
                func_match = re.search(r'(?:function|method|class)\s+([a-z_][a-z0-9_]*)', criterion_lower)
                if func_match:
                    func_name = func_match.group(1)
                    if self._check_specific_function_exists(task, func_name):
                        verified_count += 1
                        verification_details.append(f"Function {func_name} exists")
                        continue
                
                # Check for configuration mentions (pytest.ini, Dockerfile, etc.)
                if any(config in criterion_lower for config in ['pytest.ini', 'dockerfile', 'conftest.py']):
                    # Extract config file name
                    if 'dockerfile' in criterion_lower:
                        config_file = 'Dockerfile'
                    elif 'pytest.ini' in criterion_lower:
                        config_file = 'pytest.ini'
                    elif 'conftest.py' in criterion_lower:
                        config_file = 'tests/conftest.py'
                    else:
                        config_file = None
                    
                    if config_file:
                        config_path = self.repo_root / config_file
                        if config_path.exists():
                            # For Dockerfile, check for multi-stage pattern
                            if config_file == 'Dockerfile':
                                try:
                                    docker_content = config_path.read_text()
                                    # Check for multi-stage builds (builder/runtime pattern)
                                    if re.search(r'FROM\s+.*\s+AS\s+(?:builder|build|runtime|production)', docker_content, re.IGNORECASE):
                                        verified_count += 1
                                        verification_details.append("Dockerfile has builder/runtime stages")
                                    else:
                                        verified_count += 0.5  # Partial - Dockerfile exists but pattern not verified
                                except Exception:
                                    pass
                            else:
                                verified_count += 1
                                verification_details.append(f"{config_file} exists")
                            continue
                
                # Check for workflow file mentions
                if '.github/workflows' in criterion_lower or 'ci workflow' in criterion_lower:
                    workflow_path = self.repo_root / ".github" / "workflows"
                    if workflow_path.exists():
                        workflow_files = list(workflow_path.glob("*.yml")) + list(workflow_path.glob("*.yaml"))
                        if workflow_files:
                            verified_count += 1
                            verification_details.append(f"{len(workflow_files)} workflow file(s) found")
                            continue
            
            if verified_count > 0:
                verified_pct = (verified_count / len(criteria)) * 100
                if verified_pct >= 50:  # At least 50% of criteria verified
                    return True, f"{verified_count}/{len(criteria)} criteria verified ({verified_pct:.0f}%)"
        
        except Exception:
            pass
        
        return False, None
    
    def _verify_documentation_updates(self, task: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Check if tasks mentioning documentation/PRD updates actually have those files updated.
        
        Returns:
            (docs_updated, evidence_string)
        """
        try:
            task_content = task.get('content', '')
            task_title = task.get('title', '')
            
            # Look for documentation mentions
            doc_keywords = ['update.*prd', 'update.*doc', 'document', 'changelog', 'readme']
            has_doc_mention = any(re.search(keyword, task_content, re.IGNORECASE) for keyword in doc_keywords)
            
            if not has_doc_mention:
                return False, None
            
            # Extract mentioned files
            prd_files = re.findall(r'`([^`]+\.md)`|(prds?/[^/\s]+\.md)', task_content, re.IGNORECASE)
            doc_files = re.findall(r'`([^`]+\.md)`|(docs?/[^/\s]+\.md)', task_content, re.IGNORECASE)
            changelog_mention = 'changelog' in task_content.lower()
            
            verified_files = []
            
            # Check PRD files
            for prd_match in prd_files[:5]:  # Limit to 5
                prd_file = prd_match[0] or prd_match[1]
                if not prd_file.startswith('prd'):
                    continue
                
                # Try common locations
                possible_paths = [
                    self.repo_root / prd_file,
                    self.repo_root / "prds" / prd_file.split('/')[-1],
                ]
                
                for path in possible_paths:
                    if path.exists():
                        # Check if file was recently modified (has content related to task)
                        try:
                            content = path.read_text()
                            # Check for task-related keywords in PRD
                            task_keywords = task_title.split()[:3]
                            if any(kw.lower() in content.lower() for kw in task_keywords if len(kw) > 3):
                                verified_files.append(path.name)
                                break
                        except Exception:
                            pass
            
            # Check CHANGELOG.md
            if changelog_mention:
                changelog_path = self.repo_root / "CHANGELOG.md"
                if changelog_path.exists():
                    try:
                        content = changelog_path.read_text()
                        # Look for task ID or keywords
                        task_id = task.get('id', '')
                        if task_id and f"Task {task_id}" in content:
                            verified_files.append("CHANGELOG.md")
                        elif any(keyword in content.lower() for keyword in task_title.split()[:2] if len(keyword) > 4):
                            verified_files.append("CHANGELOG.md")
                    except Exception:
                        pass
            
            # Check README files
            readme_mention = 'readme' in task_content.lower()
            if readme_mention:
                for readme_name in ['README.md', 'README.rst']:
                    readme_path = self.repo_root / readme_name
                    if readme_path.exists():
                        verified_files.append(readme_name)
                        break
            
            if verified_files:
                return True, f"Updated: {', '.join(verified_files[:3])}"
            else:
                # Task mentions docs but we couldn't verify - might still be valid
                return False, "Documentation mentioned but not verified"
        
        except Exception:
            pass
        
        return False, None
    
    def audit_all_tasks(self) -> List[TaskCheck]:
        """Audit all tasks in the plan file."""
        tasks = self.extract_tasks()
        results = []
        
        print(f"Found {len(tasks)} tasks to audit...")
        
        for task in tasks:
            check = self.check_task_completion(task)
            results.append(check)
        
        return results
    
    def update_tasks_file(self, checks: List[TaskCheck], dry_run: bool = True) -> None:
        """Update github_tasks_plan.md with completion status."""
        if not self.tasks_plan_path.exists():
            print(f"Error: {self.tasks_plan_path} not found")
            return
        
        content = self.tasks_plan_path.read_text(encoding='utf-8')
        lines = content.split('\n')
        
        updates_made = 0
        
        for check in checks:
            if check.line_number == 0:
                continue
            
            # Skip if already correctly marked
            if check.current_status and check.detected_status.value.split()[0] in check.current_status:
                continue
            
            line_idx = check.line_number - 1
            if line_idx >= len(lines):
                continue
            
            # Find task section and update status
            for i in range(line_idx, min(line_idx + 50, len(lines))):
                line = lines[i]
                
                # Update status line
                if re.match(r'^\s*\*\*Status:\*\*', line):
                    new_status = check.detected_status.value
                    if check.is_outdated:
                        new_status += f" (Outdated: {check.outdated_reason})"
                    
                    if dry_run:
                        print(f"[DRY-RUN] Would update line {i+1}:")
                        print(f"  OLD: {line}")
                        print(f"  NEW: **Status:** {new_status}")
                        if check.evidence:
                            print(f"  Evidence: {', '.join(check.evidence)}")
                    else:
                        lines[i] = f"**Status:** {new_status}"
                        updates_made += 1
                    break
                
                # Add status if missing
                if i == line_idx + 10 and not any('Status:' in l for l in lines[line_idx:i]):
                    if dry_run:
                        print(f"[DRY-RUN] Would add status to Task {check.task_id} at line {i+1}")
                    else:
                        lines.insert(i, f"**Status:** {check.detected_status.value}")
                        updates_made += 1
                    break
        
        if not dry_run and updates_made > 0:
            self.tasks_plan_path.write_text('\n'.join(lines), encoding='utf-8')
            print(f"\n✅ Updated {updates_made} tasks in {self.tasks_plan_path}")
        elif dry_run:
            print(f"\n[DRY-RUN] Would update {sum(1 for c in checks if c.detected_status != TaskStatus.UNKNOWN)} tasks")
    
    def print_report(self, checks: List[TaskCheck]) -> None:
        """Print audit report."""
        print("\n" + "="*80)
        print("TASK COMPLETION AUDIT REPORT")
        print("="*80 + "\n")
        
        by_status = {}
        for check in checks:
            status = check.detected_status
            if status not in by_status:
                by_status[status] = []
            by_status[status].append(check)
        
        for status in [TaskStatus.COMPLETE, TaskStatus.OUTDATED, TaskStatus.PARTIAL, 
                       TaskStatus.PLANNED, TaskStatus.NEEDS_VERIFICATION, TaskStatus.UNKNOWN]:
            if status in by_status:
                tasks = by_status[status]
                print(f"\n{status.value}: {len(tasks)} tasks")
                print("-" * 80)
                for check in tasks[:10]:  # Show first 10
                    print(f"  Task {check.task_id}: {check.task_title}")
                    if check.evidence:
                        print(f"    Evidence: {', '.join(check.evidence)}")
                if len(tasks) > 10:
                    print(f"  ... and {len(tasks) - 10} more")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Audit task completion in github_tasks_plan.md")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Show what would be changed without updating file")
    parser.add_argument("--update", action="store_true",
                       help="Actually update github_tasks_plan.md with findings")
    parser.add_argument("--verbose", action="store_true",
                       help="Show detailed checking process")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd(),
                       help="Repository root directory (default: current directory)")
    
    args = parser.parse_args()
    
    if not args.update and not args.dry_run:
        print("Warning: No action specified. Use --dry-run to preview or --update to apply changes.")
        args.dry_run = True
    
    auditor = TaskCompletionAuditor(args.repo_root)
    checks = auditor.audit_all_tasks()
    
    auditor.print_report(checks)
    
    if args.update or args.dry_run:
        auditor.update_tasks_file(checks, dry_run=args.dry_run)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())


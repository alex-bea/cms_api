#!/usr/bin/env python3
"""Task Manager CLI Tool for GitHub Projects.

This tool provides a command-line interface for managing GitHub tasks
extracted from code comments and TODO files.

Usage
-----
    python tools/task_manager.py --help
    python tools/task_manager.py scan
    python tools/task_manager.py create-project
    python tools/task_manager.py add-tasks
    python tools/task_manager.py sync

Features
--------
- Scan codebase for TODO comments
- Create GitHub Projects
- Add tasks to GitHub Projects
- Sync local tasks with GitHub
- Generate reports and summaries
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

from github_tasks_setup import TodoScanner, GitHubProjectPlanner, TodoFileConverter


class GitHubTaskManager:
    """Main task manager for GitHub Projects."""
    
    def __init__(self, repo: str = "alex-bea/cms_api"):
        self.repo = repo
        self.project_number: Optional[str] = None
        self.scanner = TodoScanner()
        self.planner = GitHubProjectPlanner()
        self.converter = TodoFileConverter()
    
    def check_gh_cli(self) -> bool:
        """Check if GitHub CLI is installed and authenticated."""
        try:
            result = subprocess.run(
                ["gh", "auth", "status"], 
                capture_output=True, 
                text=True, 
                check=True
            )
            return "✓ Logged in" in result.stdout
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False
    
    def scan_todos(self) -> Dict:
        """Scan codebase for TODO comments and return categorized results."""
        todos = self.scanner.scan_codebase()
        
        # Categorize by priority and type
        categories = {}
        priorities = {}
        
        for todo in todos:
            # Count by category
            if todo.category not in categories:
                categories[todo.category] = []
            categories[todo.category].append(todo)
            
            # Count by priority
            if todo.priority not in priorities:
                priorities[todo.priority] = []
            priorities[todo.priority].append(todo)
        
        return {
            "total": len(todos),
            "categories": {k: len(v) for k, v in categories.items()},
            "priorities": {k: len(v) for k, v in priorities.items()},
            "todos": todos
        }
    
    def create_project(self, name: str = None, description: str = None) -> str:
        """Create a new GitHub Project."""
        if not self.check_gh_cli():
            raise RuntimeError("GitHub CLI not installed or not authenticated. Run 'gh auth login' first.")
        
        name = name or self.planner.project_name
        description = description or self.planner.project_description
        
        try:
            result = subprocess.run(
                ["gh", "project", "create", "--title", name, "--owner", "alex-bea"],
                capture_output=True,
                text=True,
                check=True
            )
            
            # Extract project number from output
            # Output format: "Created project 1: CMS API Development Tasks"
            output_lines = result.stdout.strip().split('\n')
            for line in output_lines:
                if "Created project" in line:
                    project_number = line.split()[2].rstrip(':')
                    self.project_number = project_number
                    return project_number
            
            raise RuntimeError("Could not extract project number from GitHub CLI output")
            
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to create GitHub project: {e.stderr}")
    
    def setup_project_fields(self, project_number: str = None) -> bool:
        """Set up custom fields for the GitHub Project."""
        if not self.check_gh_cli():
            raise RuntimeError("GitHub CLI not installed or not authenticated")
        
        project_number = project_number or self.project_number
        if not project_number:
            raise ValueError("Project number is required")
        
        fields = [
            {
                "name": "Priority",
                "options": ["Critical", "High", "Medium", "Low"]
            },
            {
                "name": "Category", 
                "options": ["Data Ingestion", "API Development", "Testing", "Security", 
                           "Performance", "Monitoring", "Documentation", "Database", "DevOps", "General"]
            },
            {
                "name": "Estimated Time"
            },
            {
                "name": "Phase",
                "options": ["Phase 1: Core", "Phase 2: Enhancement", "Phase 3: Optimization"]
            }
        ]
        
        for field in fields:
            try:
                if "options" in field:
                    # Create single-select field
                    cmd = ["gh", "project", "field-create", project_number, 
                           "--name", field["name"]]
                    for option in field["options"]:
                        cmd.extend(["--single-select-option", option])
                    
                    subprocess.run(cmd, check=True, capture_output=True)
                else:
                    # Create text field
                    subprocess.run([
                        "gh", "project", "field-create", project_number,
                        "--name", field["name"], "--text"
                    ], check=True, capture_output=True)
                
                print(f"✓ Created field: {field['name']}")
                
            except subprocess.CalledProcessError as e:
                print(f"✗ Failed to create field {field['name']}: {e.stderr}")
                return False
        
        return True
    
    def add_task(self, project_number: str, title: str, body: str, 
                 category: str, priority: str, estimated_time: str = "TBD") -> bool:
        """Add a single task to GitHub Project."""
        if not self.check_gh_cli():
            raise RuntimeError("GitHub CLI not installed or not authenticated")
        
        try:
            # Enhanced body with metadata since custom fields aren't supported
            enhanced_body = f"""**Category:** {category}
**Priority:** {priority}
**Estimated Time:** {estimated_time}

{body}"""
            
            cmd = [
                "gh", "project", "item-create", project_number,
                "--title", title,
                "--body", enhanced_body,
                "--owner", "alex-bea"
            ]
            
            subprocess.run(cmd, check=True, capture_output=True)
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"✗ Failed to add task '{title}': {e.stderr}")
            return False
    
    def add_tasks_from_todos(self, project_number: str = None, max_tasks: int = None) -> int:
        """Add tasks to GitHub Project from scanned TODOs."""
        project_number = project_number or self.project_number
        if not project_number:
            raise ValueError("Project number is required")
        
        # Scan for TODOs
        scan_results = self.scan_todos()
        todos = scan_results["todos"]
        
        if max_tasks:
            todos = todos[:max_tasks]
        
        added_count = 0
        
        print(f"Adding {len(todos)} tasks to GitHub Project {project_number}...")
        
        for i, todo in enumerate(todos, 1):
            title = f"{todo.category}: {todo.content[:50]}{'...' if len(todo.content) > 50 else ''}"
            body = f"**File:** `{todo.file_path}:{todo.line_number}`\n\n**Details:** {todo.content}"
            
            if self.add_task(project_number, title, body, todo.category, todo.priority):
                added_count += 1
                print(f"✓ Added task {i}/{len(todos)}: {title}")
            else:
                print(f"✗ Failed to add task {i}/{len(todos)}: {title}")
        
        print(f"\nSuccessfully added {added_count}/{len(todos)} tasks")
        return added_count
    
    def add_tasks_from_files(self, project_number: str = None) -> int:
        """Add tasks from existing TODO files."""
        project_number = project_number or self.project_number
        if not project_number:
            raise ValueError("Project number is required")
        
        # Convert TODO files to tasks
        next_todos_tasks = self.converter.convert_next_todos(Path("NEXT_TODOS.md"))
        ingestor_tasks = self.converter.convert_ingestor_tasks(Path("INGESTOR_DEVELOPMENT_TASKS.md"))
        
        all_tasks = next_todos_tasks + ingestor_tasks
        added_count = 0
        
        print(f"Adding {len(all_tasks)} tasks from TODO files to GitHub Project {project_number}...")
        
        for i, task in enumerate(all_tasks, 1):
            if self.add_task(project_number, task.title, task.description, 
                           task.category, task.priority, task.estimated_time):
                added_count += 1
                print(f"✓ Added task {i}/{len(all_tasks)}: {task.title}")
            else:
                print(f"✗ Failed to add task {i}/{len(all_tasks)}: {task.title}")
        
        print(f"\nSuccessfully added {added_count}/{len(all_tasks)} tasks")
        return added_count
    
    def generate_report(self) -> str:
        """Generate a comprehensive task management report."""
        scan_results = self.scan_todos()
        
        report = f"""# Task Management Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Summary
- **Total TODO items:** {scan_results['total']}

## By Category
"""
        
        for category, count in sorted(scan_results['categories'].items()):
            report += f"- **{category}:** {count} tasks\n"
        
        report += "\n## By Priority\n"
        
        for priority in ["Critical", "High", "Medium", "Low"]:
            if priority in scan_results['priorities']:
                count = scan_results['priorities'][priority]
                report += f"- **{priority}:** {count} tasks\n"
        
        report += "\n## High Priority Items\n"
        
        high_priority_todos = [t for t in scan_results['todos'] 
                              if t.priority in ["Critical", "High"]]
        
        if high_priority_todos:
            for todo in high_priority_todos:
                report += f"- **{todo.category}:** {todo.content}\n"
                report += f"  - File: `{todo.file_path}:{todo.line_number}`\n"
                report += f"  - Priority: {todo.priority}\n\n"
        else:
            report += "No high priority items found.\n"
        
        return report
    
    def sync(self, project_number: str = None, dry_run: bool = False) -> Dict:
        """Sync local tasks with GitHub Project."""
        project_number = project_number or self.project_number
        if not project_number:
            raise ValueError("Project number is required")
        
        if dry_run:
            print("DRY RUN: Would sync the following tasks...")
        
        # Get current state
        scan_results = self.scan_todos()
        
        # Get tasks from files
        next_todos_tasks = self.converter.convert_next_todos(Path("NEXT_TODOS.md"))
        ingestor_tasks = self.converter.convert_ingestor_tasks(Path("INGESTOR_DEVELOPMENT_TASKS.md"))
        
        all_tasks = next_todos_tasks + ingestor_tasks
        
        sync_results = {
            "scanned_todos": len(scan_results['todos']),
            "file_tasks": len(all_tasks),
            "total_tasks": len(scan_results['todos']) + len(all_tasks),
            "added": 0,
            "failed": 0
        }
        
        if not dry_run:
            # Add tasks from code TODOs
            sync_results["added"] += self.add_tasks_from_todos(project_number)
            
            # Add tasks from files
            sync_results["added"] += self.add_tasks_from_files(project_number)
        
        return sync_results


def main():
    parser = argparse.ArgumentParser(description="GitHub Task Manager CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Scan command
    scan_parser = subparsers.add_parser("scan", help="Scan codebase for TODO comments")
    scan_parser.add_argument("--json", action="store_true", help="Output as JSON")
    scan_parser.add_argument("--report", action="store_true", help="Generate detailed report")
    
    # Create project command
    create_parser = subparsers.add_parser("create-project", help="Create GitHub Project")
    create_parser.add_argument("--name", help="Project name")
    create_parser.add_argument("--description", help="Project description")
    create_parser.add_argument("--setup-fields", action="store_true", help="Set up custom fields")
    
    # Add tasks command
    add_parser = subparsers.add_parser("add-tasks", help="Add tasks to GitHub Project")
    add_parser.add_argument("project_number", help="GitHub Project number")
    add_parser.add_argument("--from-todos", action="store_true", help="Add tasks from code TODOs")
    add_parser.add_argument("--from-files", action="store_true", help="Add tasks from TODO files")
    add_parser.add_argument("--max-tasks", type=int, help="Maximum number of tasks to add")
    
    # Sync command
    sync_parser = subparsers.add_parser("sync", help="Sync local tasks with GitHub Project")
    sync_parser.add_argument("project_number", help="GitHub Project number")
    sync_parser.add_argument("--dry-run", action="store_true", help="Show what would be synced")
    
    # Report command
    report_parser = subparsers.add_parser("report", help="Generate task management report")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    manager = GitHubTaskManager()
    
    try:
        if args.command == "scan":
            results = manager.scan_todos()
            
            if args.json:
                print(json.dumps(results, indent=2, default=str))
            elif args.report:
                print(manager.generate_report())
            else:
                print(f"Found {results['total']} TODO items:")
                for todo in results['todos']:
                    print(f"  - {todo.file_path}:{todo.line_number} [{todo.category}] [{todo.priority}]")
                    print(f"    {todo.content}")
                    print()
        
        elif args.command == "create-project":
            if not manager.check_gh_cli():
                print("Error: GitHub CLI not installed or not authenticated")
                print("Please run: gh auth login")
                return 1
            
            project_number = manager.create_project(args.name, args.description)
            print(f"Created GitHub Project: {project_number}")
            
            if args.setup_fields:
                if manager.setup_project_fields(project_number):
                    print("✓ Set up custom fields")
                else:
                    print("✗ Failed to set up some custom fields")
        
        elif args.command == "add-tasks":
            if not manager.check_gh_cli():
                print("Error: GitHub CLI not installed or not authenticated")
                return 1
            
            added_count = 0
            
            if args.from_todos:
                added_count += manager.add_tasks_from_todos(args.project_number, args.max_tasks)
            
            if args.from_files:
                added_count += manager.add_tasks_from_files(args.project_number)
            
            if not args.from_todos and not args.from_files:
                # Default: add both
                added_count += manager.add_tasks_from_todos(args.project_number, args.max_tasks)
                added_count += manager.add_tasks_from_files(args.project_number)
            
            print(f"Total tasks added: {added_count}")
        
        elif args.command == "sync":
            if not manager.check_gh_cli():
                print("Error: GitHub CLI not installed or not authenticated")
                return 1
            
            results = manager.sync(args.project_number, args.dry_run)
            
            if args.dry_run:
                print(f"Would sync {results['total_tasks']} tasks:")
                print(f"  - {results['scanned_todos']} from code TODOs")
                print(f"  - {results['file_tasks']} from TODO files")
            else:
                print(f"Sync completed:")
                print(f"  - Added: {results['added']}")
                print(f"  - Failed: {results['failed']}")
        
        elif args.command == "report":
            print(manager.generate_report())
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

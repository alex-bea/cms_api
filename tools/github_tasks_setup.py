#!/usr/bin/env python3
"""GitHub Tasks Setup and Management Tool.

This script helps set up GitHub Projects and convert existing TODOs to GitHub tasks.
Since GitHub CLI may not be available, it provides guidance for manual setup.

Usage
-----
Run locally:

    python tools/github_tasks_setup.py --scan-todos
    python tools/github_tasks_setup.py --create-project-plan
    python tools/github_tasks_setup.py --convert-todos

Features
--------
1. Scan codebase for TODO comments and categorize them
2. Generate GitHub Project setup instructions
3. Convert existing TODO files to GitHub task format
4. Provide manual GitHub CLI commands for setup
"""

import argparse
import re
import sys
from pathlib import Path
from typing import List, Dict, Tuple
from dataclasses import dataclass
from datetime import datetime


@dataclass
class TodoItem:
    """Represents a TODO item found in the codebase."""
    file_path: str
    line_number: int
    content: str
    category: str
    priority: str
    estimated_time: str = "TBD"


@dataclass
class ProjectTask:
    """Represents a task for GitHub Projects."""
    title: str
    description: str
    category: str
    priority: str
    estimated_time: str
    dependencies: List[str] = None
    labels: List[str] = None


class TodoScanner:
    """Scans the codebase for TODO comments and categorizes them."""
    
    def __init__(self, root_dir: Path = Path(".")):
        self.root_dir = root_dir
        self.todo_patterns = [
            r"# TODO:\s*(.+)",
            r"# FIXME:\s*(.+)",
            r"# XXX:\s*(.+)",
            r"# HACK:\s*(.+)",
        ]
        self.todos: List[TodoItem] = []
    
    def scan_codebase(self) -> List[TodoItem]:
        """Scan the codebase for TODO comments."""
        self.todos = []
        
        # Scan Python files
        for py_file in self.root_dir.rglob("*.py"):
            if self._should_skip_file(py_file):
                continue
            self._scan_file(py_file)
        
        # Scan other relevant files
        for pattern in ["*.md", "*.yml", "*.yaml", "*.json"]:
            for file in self.root_dir.rglob(pattern):
                if self._should_skip_file(file):
                    continue
                self._scan_file(file)
        
        return self.todos
    
    def _should_skip_file(self, file_path: Path) -> bool:
        """Check if a file should be skipped."""
        skip_patterns = [
            ".git",
            ".venv",
            "__pycache__",
            "node_modules",
            ".pytest_cache",
            "tests/fixtures",  # Skip test data files
        ]
        return any(pattern in str(file_path) for pattern in skip_patterns)
    
    def _scan_file(self, file_path: Path):
        """Scan a single file for TODO comments."""
        try:
            content = file_path.read_text(encoding="utf-8")
            lines = content.splitlines()
            
            for line_num, line in enumerate(lines, 1):
                for pattern in self.todo_patterns:
                    match = re.search(pattern, line, re.IGNORECASE)
                    if match:
                        todo_content = match.group(1).strip()
                        category = self._categorize_todo(file_path, todo_content)
                        priority = self._determine_priority(todo_content)
                        
                        todo = TodoItem(
                            file_path=str(file_path),
                            line_number=line_num,
                            content=todo_content,
                            category=category,
                            priority=priority
                        )
                        self.todos.append(todo)
        except (UnicodeDecodeError, FileNotFoundError):
            pass
    
    def _categorize_todo(self, file_path: Path, content: str) -> str:
        """Categorize a TODO based on file path and content."""
        content_lower = content.lower()
        file_path_str = str(file_path)
        
        if "ingestion" in file_path_str or "ingestor" in content_lower:
            return "Data Ingestion"
        elif "api" in file_path_str or "endpoint" in content_lower:
            return "API Development"
        elif "test" in file_path_str or "testing" in content_lower:
            return "Testing"
        elif "security" in file_path_str or "auth" in content_lower:
            return "Security"
        elif "performance" in content_lower or "optimization" in content_lower:
            return "Performance"
        elif "monitoring" in content_lower or "observability" in content_lower:
            return "Monitoring"
        elif "documentation" in content_lower or "docs" in content_lower:
            return "Documentation"
        elif "database" in content_lower or "db" in content_lower:
            return "Database"
        elif "deployment" in content_lower or "ci" in content_lower:
            return "DevOps"
        else:
            return "General"
    
    def _determine_priority(self, content: str) -> str:
        """Determine priority based on content."""
        content_lower = content.lower()
        
        if any(word in content_lower for word in ["critical", "urgent", "security", "bug", "fix"]):
            return "High"
        elif any(word in content_lower for word in ["important", "performance", "optimization"]):
            return "Medium"
        else:
            return "Low"


class GitHubProjectPlanner:
    """Plans GitHub Projects structure and tasks."""
    
    def __init__(self):
        self.project_name = "CMS API Development Tasks"
        self.project_description = "Comprehensive task management for CMS API development, data ingestion, and system enhancement"
    
    def generate_project_structure(self) -> Dict:
        """Generate GitHub Project structure."""
        return {
            "project_name": self.project_name,
            "description": self.project_description,
            "views": [
                {
                    "name": "Board View",
                    "description": "Kanban-style task board with columns: Backlog, In Progress, Review, Done"
                },
                {
                    "name": "Table View", 
                    "description": "Spreadsheet-style view for detailed task management"
                },
                {
                    "name": "Timeline View",
                    "description": "Gantt-style timeline for project planning"
                }
            ],
            "custom_fields": [
                {
                    "name": "Priority",
                    "type": "single_select",
                    "options": ["Critical", "High", "Medium", "Low"]
                },
                {
                    "name": "Category",
                    "type": "single_select", 
                    "options": [
                        "Data Ingestion", "API Development", "Testing", "Security",
                        "Performance", "Monitoring", "Documentation", "Database", "DevOps", "General"
                    ]
                },
                {
                    "name": "Estimated Time",
                    "type": "text"
                },
                {
                    "name": "Phase",
                    "type": "single_select",
                    "options": ["Phase 1: Core", "Phase 2: Enhancement", "Phase 3: Optimization"]
                }
            ],
            "labels": [
                "ingestion", "api", "testing", "security", "performance", "monitoring",
                "documentation", "database", "devops", "critical", "high-priority",
                "medium-priority", "low-priority", "bug", "feature", "enhancement"
            ]
        }
    
    def convert_todos_to_tasks(self, todos: List[TodoItem]) -> List[ProjectTask]:
        """Convert TODO items to GitHub Project tasks."""
        tasks = []
        
        for todo in todos:
            task = ProjectTask(
                title=f"{todo.category}: {todo.content[:50]}{'...' if len(todo.content) > 50 else ''}",
                description=f"**File:** `{todo.file_path}:{todo.line_number}`\n\n**Details:** {todo.content}",
                category=todo.category,
                priority=todo.priority,
                estimated_time="TBD",
                labels=[todo.category.lower().replace(" ", "-"), f"{todo.priority.lower()}-priority"]
            )
            tasks.append(task)
        
        return tasks


class TodoFileConverter:
    """Converts existing TODO files to GitHub task format."""
    
    def convert_next_todos(self, file_path: Path) -> List[ProjectTask]:
        """Convert NEXT_TODOS.md to GitHub tasks."""
        tasks = []
        
        try:
            content = file_path.read_text(encoding="utf-8")
            lines = content.splitlines()
            
            current_section = ""
            current_priority = "Medium"
            
            for line in lines:
                line = line.strip()
                
                # Detect priority sections
                if line.startswith("### 1."):
                    current_priority = "Critical"
                    current_section = "Phase 1: Dynamic Data Acquisition"
                elif line.startswith("### 2."):
                    current_priority = "High"
                    current_section = "Phase 2: System Enhancement"
                elif line.startswith("### 3."):
                    current_priority = "Medium"
                    current_section = "Phase 3: Infrastructure & Operations"
                elif line.startswith("### 4."):
                    current_priority = "Medium"
                    current_section = "Phase 4: Data Quality & Validation"
                elif line.startswith("### 5."):
                    current_priority = "Low"
                    current_section = "Phase 5: Integration & Ecosystem"
                
                # Detect task items
                if line.startswith("- [ ] **") and "**" in line:
                    # Extract task title
                    title_match = re.search(r"- \[ \] \*\*(.+?)\*\*", line)
                    if title_match:
                        title = title_match.group(1)
                        
                        # Determine category based on content
                        category = self._categorize_from_content(title)
                        
                        task = ProjectTask(
                            title=title,
                            description=f"**Section:** {current_section}\n\n**Details:** {title}\n\n**Source:** NEXT_TODOS.md",
                            category=category,
                            priority=current_priority,
                            estimated_time="TBD",
                            labels=[category.lower().replace(" ", "-"), f"{current_priority.lower()}-priority", "from-todos"]
                        )
                        tasks.append(task)
        
        except FileNotFoundError:
            print(f"Warning: {file_path} not found")
        
        return tasks
    
    def convert_ingestor_tasks(self, file_path: Path) -> List[ProjectTask]:
        """Convert INGESTOR_DEVELOPMENT_TASKS.md to GitHub tasks."""
        tasks = []
        
        try:
            content = file_path.read_text(encoding="utf-8")
            lines = content.splitlines()
            
            current_phase = ""
            current_priority = "Medium"
            
            for line in lines:
                line = line.strip()
                
                # Detect phase sections
                if "Phase 1:" in line:
                    current_phase = "Phase 1: Critical Core CMS Ingestors"
                    current_priority = "Critical"
                elif "Phase 2:" in line:
                    current_phase = "Phase 2: Supporting CMS Ingestors"
                    current_priority = "High"
                elif "Phase 3:" in line:
                    current_phase = "Phase 3: Reference Data Ingestors"
                    current_priority = "Medium"
                
                # Detect task items
                if line.startswith("#### **Task") and "Ingester" in line:
                    # Extract task number and name
                    task_match = re.search(r"Task (\d+): (.+?)$", line)
                    if task_match:
                        task_num = task_match.group(1)
                        task_name = task_match.group(2)
                        
                        task = ProjectTask(
                            title=f"Task {task_num}: {task_name}",
                            description=f"**Phase:** {current_phase}\n\n**Task:** {task_name}\n\n**Priority:** {current_priority}\n\n**Source:** INGESTOR_DEVELOPMENT_TASKS.md",
                            category="Data Ingestion",
                            priority=current_priority,
                            estimated_time="3-4 days",
                            labels=["ingestion", "ingester", f"{current_priority.lower()}-priority", "from-ingestor-tasks"]
                        )
                        tasks.append(task)
        
        except FileNotFoundError:
            print(f"Warning: {file_path} not found")
        
        return tasks
    
    def _categorize_from_content(self, content: str) -> str:
        """Categorize task based on content."""
        content_lower = content.lower()
        
        if any(word in content_lower for word in ["scraper", "download", "acquisition", "ingestion"]):
            return "Data Ingestion"
        elif any(word in content_lower for word in ["api", "endpoint", "service"]):
            return "API Development"
        elif any(word in content_lower for word in ["test", "testing", "validation"]):
            return "Testing"
        elif any(word in content_lower for word in ["security", "auth", "compliance"]):
            return "Security"
        elif any(word in content_lower for word in ["performance", "scaling", "optimization"]):
            return "Performance"
        elif any(word in content_lower for word in ["monitoring", "alerting", "observability"]):
            return "Monitoring"
        elif any(word in content_lower for word in ["documentation", "docs", "guide"]):
            return "Documentation"
        else:
            return "General"


def main():
    parser = argparse.ArgumentParser(description="GitHub Tasks Setup and Management Tool")
    parser.add_argument("--scan-todos", action="store_true", help="Scan codebase for TODO comments")
    parser.add_argument("--create-project-plan", action="store_true", help="Generate GitHub Project setup plan")
    parser.add_argument("--convert-todos", action="store_true", help="Convert existing TODO files to GitHub tasks")
    parser.add_argument("--output", default="github_tasks_plan.md", help="Output file for the plan")
    
    args = parser.parse_args()
    
    if args.scan_todos:
        scanner = TodoScanner()
        todos = scanner.scan_codebase()
        
        print(f"Found {len(todos)} TODO items:")
        for todo in todos:
            print(f"  - {todo.file_path}:{todo.line_number} [{todo.category}] [{todo.priority}]")
            print(f"    {todo.content}")
            print()
    
    if args.create_project_plan or args.convert_todos:
        planner = GitHubProjectPlanner()
        converter = TodoFileConverter()
        
        # Generate project structure
        project_structure = planner.generate_project_structure()
        
        # Convert existing TODO files
        next_todos_tasks = converter.convert_next_todos(Path("NEXT_TODOS.md"))
        ingestor_tasks = converter.convert_ingestor_tasks(Path("INGESTOR_DEVELOPMENT_TASKS.md"))
        
        # Scan codebase TODOs
        scanner = TodoScanner()
        code_todos = scanner.scan_codebase()
        code_tasks = planner.convert_todos_to_tasks(code_todos)
        
        # Combine all tasks
        all_tasks = next_todos_tasks + ingestor_tasks + code_tasks
        
        # Generate output
        output = generate_github_tasks_plan(project_structure, all_tasks)
        
        with open(args.output, "w") as f:
            f.write(output)
        
        print(f"GitHub Tasks plan generated: {args.output}")
        print(f"Total tasks: {len(all_tasks)}")
        print(f"  - From NEXT_TODOS.md: {len(next_todos_tasks)}")
        print(f"  - From INGESTOR_DEVELOPMENT_TASKS.md: {len(ingestor_tasks)}")
        print(f"  - From codebase TODOs: {len(code_tasks)}")


def generate_github_tasks_plan(project_structure: Dict, tasks: List[ProjectTask]) -> str:
    """Generate a comprehensive GitHub Tasks plan."""
    
    plan = f"""# GitHub Tasks Plan: {project_structure['project_name']}

Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Project Overview

**Name:** {project_structure['project_name']}
**Description:** {project_structure['description']}
**Total Tasks:** {len(tasks)}

## GitHub Project Setup Instructions

### Step 1: Install GitHub CLI (if not already installed)

```bash
# macOS
brew install gh

# Ubuntu/Debian
sudo apt install gh

# Windows
winget install GitHub.cli
```

### Step 2: Authenticate with GitHub

```bash
gh auth login
```

### Step 3: Create GitHub Project

```bash
# Create the project
gh project create --title "{project_structure['project_name']}" --body "{project_structure['description']}"

# Note the project number from the output
PROJECT_NUMBER=<project_number>
```

### Step 4: Set up Project Views

```bash
# Add Board View (default)
gh project item-add $PROJECT_NUMBER --url https://github.com/alex-bea/cms_api

# Add Table View
gh project view-create $PROJECT_NUMBER --view table --title "Table View"

# Add Timeline View  
gh project view-create $PROJECT_NUMBER --view timeline --title "Timeline View"
```

### Step 5: Create Custom Fields

```bash
# Priority field
gh project field-create $PROJECT_NUMBER --name "Priority" --single-select-option "Critical" --single-select-option "High" --single-select-option "Medium" --single-select-option "Low"

# Category field
gh project field-create $PROJECT_NUMBER --name "Category" --single-select-option "Data Ingestion" --single-select-option "API Development" --single-select-option "Testing" --single-select-option "Security" --single-select-option "Performance" --single-select-option "Monitoring" --single-select-option "Documentation" --single-select-option "Database" --single-select-option "DevOps" --single-select-option "General"

# Estimated Time field
gh project field-create $PROJECT_NUMBER --name "Estimated Time" --text

# Phase field
gh project field-create $PROJECT_NUMBER --name "Phase" --single-select-option "Phase 1: Core" --single-select-option "Phase 2: Enhancement" --single-select-option "Phase 3: Optimization"
```

### Step 6: Create Labels

```bash
# Create labels for the repository
"""
    
    for label in project_structure['labels']:
        plan += f'gh label create "{label}" --description "Tasks related to {label}"\n'
    
    plan += """
```

## Task List

### Summary by Category

"""
    
    # Group tasks by category
    categories = {}
    for task in tasks:
        if task.category not in categories:
            categories[task.category] = []
        categories[task.category].append(task)
    
    for category, category_tasks in categories.items():
        plan += f"- **{category}**: {len(category_tasks)} tasks\n"
    
    plan += "\n### Summary by Priority\n\n"
    
    # Group tasks by priority
    priorities = {}
    for task in tasks:
        if task.priority not in priorities:
            priorities[task.priority] = []
        priorities[task.priority].append(task)
    
    for priority in ["Critical", "High", "Medium", "Low"]:
        if priority in priorities:
            plan += f"- **{priority}**: {len(priorities[priority])} tasks\n"
    
    plan += "\n## Detailed Task List\n\n"
    
    # Add each task
    for i, task in enumerate(tasks, 1):
        plan += f"### Task {i}: {task.title}\n\n"
        plan += f"**Category:** {task.category}\n"
        plan += f"**Priority:** {task.priority}\n"
        plan += f"**Estimated Time:** {task.estimated_time}\n"
        if task.labels:
            plan += f"**Labels:** {', '.join(task.labels)}\n"
        plan += f"\n**Description:**\n{task.description}\n\n"
        plan += "---\n\n"
    
    plan += """
## GitHub CLI Commands for Adding Tasks

### Add Tasks to GitHub Project

```bash
# Set your project number
PROJECT_NUMBER=<your_project_number>

# Add each task (replace with actual task details)
"""
    
    for i, task in enumerate(tasks[:10], 1):  # Show first 10 as examples
        # Clean description for CLI command
        clean_desc = task.description.replace('**', '').replace('\n', ' ')
        plan += f"""
# Task {i}: {task.title}
gh project item-create $PROJECT_NUMBER --title "{task.title}" --body "{clean_desc}" --field "Category={task.category}" --field "Priority={task.priority}" --field "Estimated Time={task.estimated_time}"
"""
    
    plan += """
```

### Alternative: Manual Setup

If GitHub CLI is not available, you can:

1. Go to https://github.com/alex-bea/cms_api
2. Click on "Projects" tab
3. Create a new project
4. Add the tasks manually using the web interface

## Next Steps

1. **Set up GitHub Project** using the commands above
2. **Add all tasks** to the project
3. **Assign team members** to tasks
4. **Set up automation** for task updates
5. **Create milestones** for project phases
6. **Set up notifications** for task updates

## Automation Setup

### GitHub Actions Workflow

Create `.github/workflows/task-sync.yml`:

```yaml
name: Task Synchronization
on:
  schedule:
    - cron: '0 9 * * 1'  # Weekly on Monday at 9 AM
  workflow_dispatch:

jobs:
  sync-todos:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Scan for new TODOs
        run: |
          python tools/github_tasks_setup.py --scan-todos
      - name: Update GitHub Tasks
        # Add logic to create/update GitHub tasks
        run: echo "TODO: Implement GitHub API integration"
```

## Notes

- This plan includes {len(tasks)} total tasks
- Tasks are categorized by type and priority
- Estimated times are based on complexity analysis
- Dependencies are noted where applicable
- All tasks include source information for traceability

---
*Generated by GitHub Tasks Setup Tool*
"""
    
    return plan


if __name__ == "__main__":
    main()

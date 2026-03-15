# LLMOps Course on Databricks — Project Guidelines

## Course description and goals

This project contains the code for a course on LLMOps on Databricks. Where we will build an AI project and especially work on the OPs part: gaining control on the agent(s), imporve them, deploy them...

This project is an adapted copy of a similar one owned by the course teacher. This project will differ with the teacher's because of the topic and data chosen: the Global Findex survey and reports, from the World Bank.

Context on the project, the data, the Global findex ... will be found in the /notes/ folder.

## System settings

Many prompts for Claude Code will have educational purposes. When asked about certain topics, Claude will act as a teacher and give educated answers. Not too long but precise enough for the student to gain knowledge on new concepts.

Some prompts will also concern the comparisons with the teacher's repository, which lives here on this machine: /Users/perso/Documents/Trainings/llmops_with_databricks/course-code-hub/ . Claude should look into the teacher's repository when the student asks for comparisons between certain files.

Example:
- Student asks "What are the latest changes on the file databricks.yml in the teacher's repo? Explain them, show me which ones I should consider bringing in my project"
- Expected behaviour for Claude:
    - Inspect the contents of student file: /Users/perso/Documents/Trainings/llmops_with_databricks/llmops-databricks-course-bencorret/databricks.yml
    - Inspect the contents of the teachers equivalent of this file: /Users/perso/Documents/Trainings/llmops_with_databricks/course-code-hub/databricks.yml
    - Inform the student about the recent evolutions brought by the teachers

## Development Environment

This project uses `uv` for dependency management and running tools.
Python **3.12** is required (matches Databricks Serverless Environment 4).

### Running Commands

**ALWAYS use `uv run` prefix for all Python tools:**

```bash
# Linting, formatting
uv run pre-commit run --all-files

# Running tests
uv run pytest
```

## Project Structure

```
llmops-databricks-course-bencorret/
├── .claude/
│   └── commands/           # Claude Code slash commands (fix-deps, run-notebook, ship)
├── .github/
│   └── workflows/ci.yml
├── notebooks/              # Databricks-format notebooks
├── notes/                  # Notes on project study subject
├── project_ddl_infra/      # Saved files to rebuild unity catalog and workspace objects
├── resources/              # Databricks Asset Bundle job definitions (*.yml)
├── tests/                  # PyTest tests
├── databricks.yml          # Databricks Asset Bundle configuration
├── project_config.yml
├── pyproject.toml
└── version.txt
```

## Dependency Management

### Pinning Rules

**Regular dependencies** (`[project] dependencies`): pin to exact version.
```toml
"pydantic==2.11.7"
"databricks-sdk==0.85.0"
```

**Optional / dev dependencies**: use `>=X.Y.Z,<NEXT_MAJOR`.
```toml
"pytest>=8.3.4,<9"
"pre-commit>=4.1.0,<5"
```

### Packages That Must Always Be Optional

Never put these in `[project] dependencies`:
- `databricks-connect` → `dev` extra
- `ipykernel` → `dev` extra
- `pytest`, `pre-commit` → `ci` extra

### Updating Dependencies

Use the `/fix-deps` skill to look up the latest PyPI versions and update `pyproject.toml` automatically.

After any dependency changes, validate the environment resolves:
```bash
uv sync --extra dev
```

## Skills

Custom slash commands are defined in `.claude/commands/`. Use them to automate common workflows:

| Skill | Command | Description |
|-------|---------|-------------|
| Fix dependencies | `/fix-deps` | Look up latest PyPI versions and update `pyproject.toml` |
| Run notebook | `/run-notebook <path>` | Deploy and run a notebook on Databricks via Asset Bundles |
| Ship | `/ship` | Commit all changes with a structured message and push (blocks on `main`) |

### `/run-notebook`

Deploys the project wheel and runs a notebook as a Databricks job.

```bash
/run-notebook notebooks/hello_world.py
```

What it does:
1. Derives a job resource key from the notebook filename (e.g. `hello_world_job`)
2. Ensures `resources/` exists and is included in `databricks.yml`
3. Creates `resources/<key>.yml` if it doesn't exist, with `env`, `git_sha`, and `run_id` base parameters
4. Runs `databricks bundle deploy` then `databricks bundle run <key>`

## Notebook File Format

All Python files in `notebooks/` must be formatted as Databricks notebooks:

- **First line**: `# Databricks notebook source`
- **Cell separator**: `# COMMAND ----------` between logical sections

This enables running them interactively in both VS Code (via the Jupyter extension) and Databricks.

```python
# Databricks notebook source
"""
Example description.
"""

import os

# COMMAND ----------

print("Hello, world!")
```

**NEVER** use `#!/usr/bin/env python` shebangs in notebook files.

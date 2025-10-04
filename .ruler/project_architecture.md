# Project Architecture

* Using uv to manage packages
* Monorepo Structure
* Data Platform using Databricks
* Domain Driven Design
* Using Una

  * Una is a tool to build and productionise Python monorepos with uv.
  * uv has Workspaces, but no ability to build them. This means if you have dependencies between packages in your workspace, there's no good way to distribute or productionise the end result.
  * `uvx --from build pyproject-build --installer=uv --outdir=dist --wheel projects/marketing_kpi`

UNA CLI:

```bash
uv run una --help

 Usage: una [OPTIONS] COMMAND [ARGS]...

╭─ Options ─────────────────────────────────────────────╮
│ --help          Show this message and exit.           │
╰───────────────────────────────────────────────────────╯
╭─ Commands ────────────────────────────────────────────╮
│ create   Commands for creating workspace and packages.│
│ sync     Update packages with missing dependencies.   │
╰───────────────────────────────────────────────────────╯
```

## We are using the namespace dataplatform so una the hatch tool we use to build is able to tell the difference between local packages and Pypi

## LAYOUT

``` bash
➜ tree .
.
├── LICENSE
├── README.md
├── dist
├── packages
│   ├── client
│   │   ├── README.md
│   │   ├── dataplatform 
│   │   │   └── infrastructure
│   │   │       ├── __init__.py
│   │   └── pyproject.toml
│   ├── core 
│   │   ├── README.md
│   │   ├── src
│   │   │   └── infrastructure
│   │   │       ├── __init__.py
│   │   └── pyproject.toml
│   ├── transformation 
│   │   ├── README.md
│   │   ├── dataplatform 
│   │   │   └── infrastructure
│   │   │       ├── __init__.py
│   │   └── pyproject.toml
├── projects
│   └── marketing
│       └── marketing_kpi
│           ├── dataplatform 
│           │   ├── marketing_kpi
│           │   │   ├── __init__.py
│           │   │   └── py.typed
│           │   └── pyproject.toml
├── pyproject.toml
└── uv.lock
```
## Understanding Layout

### Packages

* Packages are reusable pieces of code that can be used in projects

### Projects

* Projects are deployable pieces of code that can have dependencies on packages

* We are using uv una to handle the wheel building using external libs since uv worskpaces doesn't support for the moment
https://una.rdrn.me/quickstart/

* `run uvx una tree` to see dependencies and where your packages are installed

<img src=".images/una_tree_output.png" width="600">

## Notebooks Versus python files (.ipynb vs .py)

Because of compatibility and operational issues and the difficulty of PRs with .ipynb files, we are using .py files with - # Databricks notebook source - at the top.

This line must be omitted if you want jobs to run via a python wheel and if you want to use your file in databricks UI you must add that line to the top. It has no impact inside of an IDE.

```python
# Databricks notebook source
```

 This is the cell delimiter command in a .py file:

```python
# COMMAND ----------
```


## Trunk-based Development

This outlines the process for creating a feature branch, developing your feature, submitting your changes for peer review, and merging back to the `main`.

## Workflow Steps

### 1. Update Your Local `main` Branch

Before starting any new work, update your local `main` branch with the latest changes from the remote repository.

```bash
# Switch to the main branch
git checkout main

# Pull the latest changes
git pull origin main
```

### 2. Create and switch to a new branch for your feature:
```bash 
git checkout -b feature/DS-ticket-number-feature-name
```

Example:
```bash
git checkout -b feature/DS-123-feature-name
```

### 3. Develop your feature and add changes (make sure you stage what you want to commit)

```bash
git add --all # stage all changes
git commit -am "[description of changes]"
```

### 4. Push your changes to the branch

```bash
git push
```

### 5. Create a Pull Request (PR)

## Workflow Steps

### 1. Deploy to Dev Workspace
After a pull request has been merged into the `main` branch, the deployment process will automatically trigger to the dev workspace. 
Because this is a monorepo, only projects that have been changed will be deployed.


> **Important:** Deployments to the dev workspace will automatically trigger and asynchronously run the Databricks job configured in the GitHub workflow yml file. This allows for immediate testing of the deployed changes.

<img src=".images/deploy_to_dev.png">

### 2. Test Your Work
Either through manual testing or automatically via Databricks notebooks/tasks. 
> **Important:** Make sure the job completes and the data is as expected.
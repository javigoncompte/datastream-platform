# DataStream Platform
## *Databricks Bundles Deploy Monorepo*

## Prerequisites
1. Install Databricks CLI 0.238 or later.
   See [Install or update the Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html).

2. Install uv. See [Installing uv](https://docs.astral.sh/uv/getting-started/installation/).
   We use uv to create a virtual environment and install the required dependencies.

3. Authenticate to your Databricks workspace if you have not done so already:
    ```
    $ databricks configure
    ```
libraries and tools/packages to use in data platforms

4. Optionally, install developer tools such as the Databricks extension for Visual Studio Code from
   https://docs.databricks.com/dev-tools/vscode-ext.html. Or read the "getting started" documentation for
   **Databricks Connect** for instructions on running the included Python code from a different IDE.

5. For documentation on the Databricks Asset Bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.

## Deploy and run jobs

1. Create a new virtual environment and install the required dependencies:
    ```
    $ uv sync
    ```

2. To deploy the bundle to the development target:
    ```
    $ databricks bundle deploy --target dev
    ```

   *(Note that "dev" is the default target, so the `--target` parameter is optional here.)*

   This deploys everything that's defined for this project.
   For example, the default template would deploy a job called
   `[dev yourname] datastream_job` to your workspace.
   You can find that job by opening your workspace and clicking on **Workflows**.

3. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

   Note that the default job from the template has a schedule that runs every day
   (defined in resources/datastream_job.py). The schedule
   is paused when deploying in development mode (see [Databricks Asset Bundle deployment modes](
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html)).

4. To run a job:
   ```
   $ databricks bundle run
   ```

## Development
Using [uv](https://docs.astral.sh/uv/) for development:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Install Python and dependencies:
```bash
uv sync --all-packages
```

Read on below...

## Structure
- **libs**: importable packages, never run independently, do not have entry points
- **apps**: have entry points, never imported


## Docker

## Syncing
To make life easier while you're working across the workspace, you should run:
```bash
uv sync --all-packages
```

uv's sync behaviour is as follows:
- If you're in the workspace root and you run `uv sync`, it will sync only the
dependencies of the root workspace, which for this kind of monorepo should be bare.
This is not very useful.
- If you're in eg `apps/myserver` and run `uv sync`, it will sync only that package.
- You can run `uv sync --package=postmodern-server` to sync only that package.
- You can run `uv sync --all-packages` to sync all packages.

You can add an alias to your `.bashrc`/`.zshrc` if you like:
```bash
alias uvs="uv sync --all-packages
```

## Dependencies
You'll notice that `apps/mycli` has `urllib3` as a dependency.
Because of this, _every_ package in the workspace is able to import `urllib3` **in local development**,
even though they don't include it as a direct or transitive dependency.

This can make it possible to import stuff and write passing tests, only to have stuff fail
in production, where presumably you've only got the necessary dependencies installed.

There are two ways to guard against this:

1. If you're working _only_ on eg `libs/server`, you can sync only that package (see above).
This will make your LSP shout and your tests fail, if you try to import something that isn't
available to that package.

2. Your CI (see example in [.github/workflows](.github/workflows)) should do the same.

## Dev dependencies
This repo has all the global dev dependencies (`basedpyright`, `pytest`, `ruff` etc) in the root
pyproject.toml, and then repeated again in each package _without_ their version specifiers.

It's annoying to have to duplicate them, but at least excluding the versions makes it easier
to keep things in sync.

The reason they have to be repeated, is that if you follow the example above and install only
a specific package, it won't include anything specified in the root package.

## Tasks/scripts
[Poe the Poet](https://poethepoet.natn.io/index.html) is used until uv includes its own task runner.

Tasks are defined in the root pyproject.toml, mostly running again `${PWD}` so that if
you run a task from within a package, it'll only run for that package.

You can run the tasks as follows:
```bash
uv run poe fmt
           lint
           check
           test

# or to run them all
uv run poe all
```

If you run any of these from the workspace root, it will run for all packages,
whereas if you run them inside a package, they'll run for only that package.

## Testing
This repo includes a simple pytest test for each package.

To test all packages:
```bash
uv sync --all-packages
uv run poe test
```

To test a single package:
```bash
cd apps/server
uv sync
uv run poe test
```

## Pyright
(This repo actually uses [basedpyright](https://docs.basedpyright.com/latest/).

The following needs to be included with every package `pyproject.toml`:
```toml
[tool.pyright]
venvPath = "../.."       # point to the workspace root where the venv is
venv = ".venv"
strict = ["**/*.py"]
pythonVersion = "3.13"
```

Then you can run `uv run poe check` as for tests.

```
uvx --from build pyproject-build --installer=uv \
    --outdir=dist --wheel apps/printer
```


## Building EvalGen
`uvx --from build pyproject-build --installer=uv --outdir=dist  --wheel libs/evalgen`




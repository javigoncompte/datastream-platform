# DataStream Platform
libraries and tools/packages to use in data platforms

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
- **libs**: importable libraries, never run independently, do not have entry points
- **packages**: packages that can run independently. Might have entry points
- **platform**: platform related importable libraries. Manging a Data Platform


## Docker
The Dockerfile is at [apps/server/Dockerfile](apps/server/Dockerfile).

Build the Docker image from the workspace root, so that it has access to all libraries:
```bash
docker build --tag=postmodern-server -f apps/server/Dockerfile .
```

And run it:
```bash
docker run --rm -it postmodern-server
```

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
You'll notice that `packages/` has `x` as a dependency.
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
This repo has all the global dev dependencies (`pyright`, `pytest`, `ruff` etc) in the root
pyproject.toml, and then repeated again in each package _without_ their version specifiers.

It's annoying to have to duplicate them, but at least excluding the versions makes it easier
to keep things in sync.

The reason they have to be repeated, is that if you follow the example above and install only
a specific package, it won't include anything specified in the root package.



## Pyright
(This repo actually uses [basedpyright](https://docs.basedpyright.com/latest/).


## Una for building wheels on Packages

`uvx --from build pyproject-build --installer uv`
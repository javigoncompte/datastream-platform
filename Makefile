all: fmt lint check test

fmt:
	@uv run ruff format

fmt-check:
	uv run ruff format --check

lint:
	uv run ruff check --fix

lint-check:
	uv run ruff check

check:
	uv run basedpyright

test:
	uv run pytest

build:
	uvx --from build pyproject-build --installer=uv --wheel --outdir=dist libs/evalgen

# build-docker: build
# 	uv export --format=requirements-txt  --locked --no-dev --package=evalgen > requirements.txt
# 	docker build --tag example .

# this Dockerfile should be built from the repo root
FROM python:3.13.1-slim-bookworm

ENV PYTHONUNBUFFERED=True
WORKDIR /workspace

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

COPY pyproject.toml uv.lock /workspace/
RUN uv sync --frozen --no-install-workspace --package=postmodern-server

COPY . /workspace
RUN uv sync --frozen --package=postmodern-server

CMD ["/workspace/.venv/bin/python", "/workspace/apps/server/postmodern/server/run.py"]

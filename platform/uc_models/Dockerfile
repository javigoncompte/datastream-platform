# this Dockerfile should be built from the repo root
FROM ghcr.io/astral-sh/uv:python3.11-bookworm

ENV PYTHONUNBUFFERED=True
WORKDIR /workspace

COPY pyproject.toml uv.lock /workspace/
RUN uv sync --frozen --no-install-workspace --package=uc_models

COPY . /workspace
RUN uv sync --frozen --package=uc_models

CMD ["/workspace/.venv/bin/python", "/workspace/platform/uc_models/src/uc_models/run.py"]
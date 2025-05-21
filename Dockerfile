#TODO improve dockerfile multi stage
FROM python:3.13-alpine3.21

# Install uv.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never 

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --no-dev --locked --no-install-project 
    
WORKDIR /app

# Copy the application into the container.
COPY main.py pyproject.toml uv.lock ./


RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev 

# Run the application.
CMD ["uv", "run", "/app/main.py"]
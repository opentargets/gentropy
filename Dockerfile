# Stage 1: Build stage - Used to prepare dependencies and package the application
FROM python:3.11-slim-bookworm AS base

# Install Java runtime (required for PySpark/Hail) and procps for process management
RUN echo "deb http://deb.debian.org/debian oldstable main" >> /etc/apt/sources.list
RUN apt-get update && \
    apt-get install --no-install-recommends -y openjdk-11-jdk-headless procps && \
    # Clean up apt cache to reduce image size
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

FROM base AS builder

# Add UV binaries
COPY --from=ghcr.io/astral-sh/uv:python3.11-bookworm-slim /usr/local/bin/uv /usr/local/bin/

# Set working directory for application files
WORKDIR /app

# Copy project configuration and source code
ADD ./pyproject.toml  ./uv.lock /app/
ADD ./src/ /app/src/
# Create an empty README file (required by some package managers)
RUN touch README.md

# Install dependencies and project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-editable

# Stage 2: Runtime stage - Creates the final minimal image
FROM base AS production

# Set working directory in the runtime container
WORKDIR /app

# Copy the virtual environment with all dependencies from the builder stage
COPY --from=builder /app/.venv .venv

# Configure PATH to use the virtual environment's binaries
ENV PATH="/app/.venv/bin:$PATH"

# Set environment variables for PySpark and Hail locations
ENV SPARK_HOME=/app/.venv/lib/python3.11/site-packages/pyspark
ENV HAIL_DIR=/app/.venv/lib/python3.11/site-packages/hail

ENTRYPOINT [ "gentropy" ]

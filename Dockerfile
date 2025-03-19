# Stage 1: Build stage - Used to prepare dependencies and package the application
FROM python:3.11-bookworm AS builder

# Update package lists and install build essentials (compilers and development tools)
RUN apt-get update && \
    apt-get install --no-install-recommends -y build-essential && \
    # Clean up apt cache to reduce image size
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and install UV (a Python package manager) from the provided URL
ADD https://astral.sh/uv/install.sh /install.sh
RUN chmod -R 655 /install.sh && /install.sh && rm /install.sh

# Add UV's binary directory to PATH for subsequent commands
ENV PATH="/root/.local/bin:$PATH"

# Set working directory for application files
WORKDIR /app

# Copy project configuration and source code
COPY ./pyproject.toml .
COPY ./src src

# Create an empty README file (required by some package managers)
RUN touch README.md

# Synchronize dependencies using UV (installs requirements from pyproject.toml)
RUN uv sync

# Install the package (and its dependencies) into a virtual environment
RUN uv pip install .

# Stage 2: Runtime stage - Creates the final minimal image
FROM python:3.11-slim-bookworm

# Install Java runtime (required for PySpark/Hail) and procps for process management
RUN echo "deb http://deb.debian.org/debian oldstable main" >> /etc/apt/sources.list 
RUN apt-get update && \
    apt-get install --no-install-recommends -y openjdk-11-jdk-headless procps && \
    # Clean up apt cache to reduce image size
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory in the runtime container
WORKDIR /app

# Copy the virtual environment with all dependencies from the builder stage
COPY --from=builder /app/.venv .venv

# Configure PATH to use the virtual environment's binaries
ENV PATH="/app/.venv/bin:$PATH"

# Set environment variables for PySpark and Hail locations
ENV SPARK_HOME=/app/.venv/lib/python3.11/site-packages/pyspark
ENV HAIL_DIR=/app/.venv/lib/python3.11/site-packages/hail

# Add the CLI script to the container
ADD src/gentropy/cli.py cli.py

# Define the default command to run when container starts
ENTRYPOINT [ "python", "cli.py" ]

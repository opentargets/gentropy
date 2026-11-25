FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS uv_builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
# Disable python downloads to use the one from the base image
ENV UV_PYTHON_DOWNLOADS=0

# Set working directory for application files
WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev
COPY src /app/src
COPY README.md /app/README.md
COPY LICENSE.md /app/LICENSE.md
COPY pyproject.toml /app/pyproject.toml
COPY uv.lock /app/uv.lock
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev


# Stage 2: Runtime stage - Creates the final minimal image
FROM python:3.12.11-slim-trixie AS production

# Install ps (required for pyspark) and the Google Cloud CLI.
# Installed via apt (rather than the google/cloud-sdk image, which only
# publishes amd64) so this stage builds natively for both amd64 and arm64.
RUN apt-get update && apt-get install -y --no-install-recommends \
    procps \
    apt-transport-https \
    ca-certificates \
    gnupg \
    curl \
    && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -y --no-install-recommends google-cloud-cli=581.0.0-0 \
    && rm -rf /var/lib/apt/lists/*

ENV CLOUDSDK_PYTHON=/usr/local/bin/python3

# Google Batch injects CLOUDSDK_PYTHON=/usr/bin/python3 into container runnables,
# overriding image-level ENV. This base image ships python at /usr/local/bin only,
# so provide the conventional system path for gcloud (and anything else hardcoding it).
RUN ln -sf /usr/local/bin/python3 /usr/bin/python3

# Create app user and group
RUN groupadd --gid 1000 app && \
    useradd --uid 1000 --gid app --shell /bin/bash --create-home app

# # Set working directory in the runtime container
COPY --from=uv_builder --chown=app:app /app /app
# # Copy the virtual environment with all dependencies from the builder stage
COPY --from=amazoncorretto:21.0.13-al2023-headless /usr/lib/jvm/java-21-amazon-corretto /usr/lib/jvm/java-21-amazon-corretto
# Copy certificates from the Corretto image
COPY --from=amazoncorretto:21.0.13-al2023-headless /etc/pki/ca-trust/extracted/java/cacerts /usr/lib/jvm/java-21-amazon-corretto/lib/security/cacerts
# Copy harmonisation script
COPY --chmod=0755 utils/harmonise_sumstats.sh harmonise-sumstats.sh

# GCS connector jar, so sessions with add_gcs_connector=true need no internet access
ARG GCS_CONNECTOR_VERSION=3.1.17
ARG GCS_CONNECTOR_SHA256=423b95c346bcd09b257cb18106448d02b8911901f384724985e6be042e03022d
ENV GCS_CONNECTOR_JAR=/opt/gcs-connector/gcs-connector-${GCS_CONNECTOR_VERSION}-shaded.jar
RUN mkdir -p /opt/gcs-connector \
    && curl -fsSL -o "${GCS_CONNECTOR_JAR}" "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-${GCS_CONNECTOR_VERSION}.jar" \
    && echo "${GCS_CONNECTOR_SHA256}  ${GCS_CONNECTOR_JAR}" | sha256sum -c -

# # Configure PATH to use the virtual environment's binaries
ENV PATH="/app/.venv/bin:$PATH"

# # Set environment variables for PySpark and Hail locations
ENV JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto
ENV SPARK_HOME=/app/.venv/lib/python3.12/site-packages/pyspark
ENV HAIL_HOME=/app/.venv/lib/python3.12/site-packages/hail

ENTRYPOINT [ "gentropy" ]

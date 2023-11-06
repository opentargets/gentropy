FROM apache/airflow:slim-2.7.3-python3.10

# Install additional Python requirements.
# --no-cache-dir is a good practice when installing packages using pip, because it helps to keep the image lightweight.
COPY requirements.txt /requirements.txt
RUN pip install --quiet --user --no-cache-dir --upgrade pip setuptools && \
    pip install --quiet --user --no-cache-dir -r /requirements.txt

# Source: https://airflow.apache.org/docs/docker-stack/recipes.html
# Installing the GCP CLI in the container
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

USER 0
ARG CLOUD_SDK_VERSION=452.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
    --bash-completion=false \
    --path-update=false \
    --usage-reporting=false \
    --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

# Switch back to a non-root user for security purposes
USER airflow

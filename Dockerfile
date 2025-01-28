FROM python:3.10-bullseye

RUN apt-get update && \
    apt-get clean && \
    apt-get install -y openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr

RUN pip install uv
WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN touch README.md

RUN uv sync

COPY src ./src

ENTRYPOINT ["uv", "run", "gentropy"]

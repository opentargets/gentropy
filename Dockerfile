FROM python:3.10-bullseye


RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN java -version

# Set environment variables for Java
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

RUN pip install poetry==1.7.1

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN touch README.md

RUN poetry config installer.max-workers 10
RUN poetry install --without dev,docs,tests --no-root --no-interaction --no-ansi -vvv && rm -rf $POETRY_CACHE_DIR

COPY src ./src

RUN poetry install --without dev,docs,tests

ENTRYPOINT ["poetry", "run", "gentropy"]

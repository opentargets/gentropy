FROM python:3.10

RUN pip install poetry==1.7.1

COPY . .
RUN poetry install --without dev,docs,tests

ENTRYPOINT ["poetry", "run", "gentropy"]

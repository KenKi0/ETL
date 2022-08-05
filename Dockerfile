FROM python:3.10

ARG PROJECT_ENV

WORKDIR /service

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update && \
    apt-get install -y libpq-dev wait-for-it && \
    apt-get clean

RUN pip install --no-cache-dir poetry==1.1.13

COPY poetry.lock poetry.lock
COPY pyproject.toml pyproject.toml

RUN poetry config virtualenvs.create false && \
    poetry install $(test "$PROJECT_ENV" = production && echo "--no-dev") --no-interaction --no-ansi

COPY src/ ./src
COPY .flake8 .flake8

CMD python3 src/main.py
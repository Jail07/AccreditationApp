FROM postgres:14

# Устанавливаем параметры БД
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=1234
ENV POSTGRES_DB=accr_db

# Прокидываем порт PostgreSQL
EXPOSE 5432

# Копируем скрипт инициализации
COPY init-db.sql /docker-entrypoint-initdb.d/

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt requirements.txt

RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
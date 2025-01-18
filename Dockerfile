FROM postgres:14

# Устанавливаем параметры БД
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=1234
ENV POSTGRES_DB=accr_db

# Прокидываем порт PostgreSQL
EXPOSE 5432

# Копируем скрипт инициализации
COPY init-db.sql /docker-entrypoint-initdb.d/

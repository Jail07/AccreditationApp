# Используем базовый образ с Python
FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файлы проекта
COPY . .

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    libgl1-mesa-dev \
    libxkbcommon-x11-0 \
    libglib2.0-0 \
    libxcb-xinerama0 \
    libxcb-randr0 \
    libx11-xcb1 \
    libxcb-util1 \
    libxcb-render0 \
    libxcb-shape0 \
    libxcb-shm0 \
    libfreetype6 \
    libxcb1 \
    libqt5gui5 \
    libqt5widgets5 \
    libqt5core5a \
    libqt5dbus5 \
    libxcb-glx0 \
    && rm -rf /var/lib/apt/lists/*

# Создаем пользователя с UID 1000
RUN useradd -m -u 1000 appuser

# Создаем runtime-директорию и назначаем правильные права
RUN mkdir -p /tmp/runtime-root && chown appuser:appuser /tmp/runtime-root && chmod 700 /tmp/runtime-root

# Переключаемся на пользователя appuser
USER appuser

# Устанавливаем зависимости Python
RUN ./install_dependencies.sh

# Устанавливаем переменные среды
ENV QT_QPA_PLATFORM=offscreen
ENV XDG_RUNTIME_DIR=/tmp/runtime-root
ENV TZ=Europe/Moscow

# Открываем порт для приложения
EXPOSE 8000

# Запускаем приложение
CMD ["python", "main.py"]

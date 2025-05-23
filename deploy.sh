#!/bin/bash

# Убедимся, что все действия остановятся при ошибке
set -e

# Переменные
REPO_URL="https://github.com/Jail07/AccreditationApp.git" # Укажите URL репозитория
APP_DIR="AccreditationApp"  # Название папки приложения
DOCKER_COMPOSE_FILE="docker-compose.yml"
SCHEDULER_SCRIPT="scheduler_runner.py"
SYSTEMD_SERVICE="/etc/systemd/system/scheduler.service"
CRON_JOB="@reboot /usr/bin/python3 /root/$APP_DIR/$SCHEDULER_SCRIPT"

# Функция: Установить зависимости (Docker и Docker Compose)
install_dependencies() {
    echo "Устанавливаем Docker и Docker Compose..."
    if ! command -v docker &> /dev/null; then
        curl -fsSL https://get.docker.com -o get-docker.sh
        sh get-docker.sh
        rm get-docker.sh
    fi

    if ! command -v docker-compose &> /dev/null; then
        sudo curl -L "https://github.com/docker/compose/releases/download/2.31.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
        sudo chmod +x /usr/local/bin/docker-compose
    fi
    echo "Docker и Docker Compose установлены."
}

# Функция: Загрузить проект из Git
clone_repository() {
    if [ -d "$APP_DIR" ]; then
        echo "Обновляем существующий репозиторий..."
        cd "$APP_DIR"
        git pull
        cd ..
    else
        echo "Клонируем репозиторий..."
        git clone "$REPO_URL" "$APP_DIR"
    fi
}

# Функция: Запустить Docker Compose
run_docker_compose() {
    echo "Запускаем Docker Compose для проекта AccreditationApp..."
    cd "$APP_DIR"
    # Убедимся, что переменные из .env файла доступны для docker-compose
    if [ ! -f .env ]; then
        echo "Файл .env не найден! Пожалуйста, создайте его с конфигурацией БД."
        # exit 1 # Или скопируйте шаблон .env.example, если он есть
    fi
    docker-compose down --remove-orphans # Останавливаем и удаляем старые контейнеры
    docker-compose build # Пересобираем образы, если Dockerfile изменился
    docker-compose up -d # Запускаем в фоновом режиме
    cd ..
    echo "Docker Compose запущен."
}

echo "Начинаем процесс деплоя..."
install_dependencies
clone_repository
run_docker_compose # Запускаем docker-compose

echo "Деплой завершён успешно!"
echo "Проверяем состояние контейнеров..."
docker ps -a
echo "Логи планировщика (scheduler):"
docker logs -f $(docker-compose ps -q scheduler)

# Инструкция для использования скрипта
cat << EOF
На сервере Ubuntu 22.04 загружаете файл deploy.sh, после вводите команды:
chmod +x deploy.sh
./deploy.sh
EOF

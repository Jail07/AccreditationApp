#!/bin/bash

# Убедимся, что все действия остановятся при ошибке
set -e

# Переменные
REPO_URL="https://github.com/Jail07/AccreditationApp.git" # Укажите URL репозитория
APP_DIR="AccreditationApp"  # Название папки приложения
DOCKER_COMPOSE_FILE="docker-compose.yml"

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
run_docker() {
    echo "Запускаем Docker Compose..."
    cd "$APP_DIR"
    docker-compose down
    docker-compose build
    docker-compose up
    echo "Docker Compose запущен."
}

# Основной процесс
echo "Начинаем процесс деплоя..."
install_dependencies
clone_repository
run_docker
echo "Деплой завершён успешно!"
# Проверка контейнеров после запуска
echo "Проверяем состояние контейнеров..."
docker ps -a

if docker logs accr_app 2>&1 | grep -q "could not connect to display"; then
    echo "Ошибка: Проблемы с Xvfb или xcb."
    exit 1
fi

# На сервере Ubuntu 22.04 загружаете файл deploy.sh, после вводите команду:
#chmod +x deploy.sh
# И запускаете программу, остальное она сама сделает
# ./deploy.sh
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
run_docker() {
    echo "Запускаем Docker Compose..."
    cd "$APP_DIR"
    docker-compose down
    docker-compose build
    docker-compose up -d
    echo "Docker Compose запущен."
}

# Функция: Настроить и запустить systemd службу
setup_systemd() {
    echo "Создаём systemd службу для планировщика..."
    cat << EOF | sudo tee $SYSTEMD_SERVICE > /dev/null
[Unit]
Description=Запуск планировщика задач AccreditationApp
After=network.target

[Service]
ExecStart=/usr/bin/python3 /root/$APP_DIR/$SCHEDULER_SCRIPT
Restart=always
User=root
WorkingDirectory=/root/$APP_DIR

[Install]
WantedBy=multi-user.target
EOF

    sudo systemctl daemon-reload
    sudo systemctl enable scheduler.service
    sudo systemctl start scheduler.service
    echo "systemd служба настроена и запущена."
}

# Функция: Настроить cron задачу
setup_cron() {
    echo "Настраиваем cron задачу для планировщика..."
    (crontab -l 2>/dev/null | grep -v "$SCHEDULER_SCRIPT"; echo "$CRON_JOB") | crontab -
    echo "Cron задача настроена: $CRON_JOB"
}

# Основной процесс
echo "Начинаем процесс деплоя..."
install_dependencies
clone_repository
run_docker

# Запрос на выбор метода автоматического запуска
read -p "Выберите метод автоматического запуска планировщика (1 - systemd, 2 - cron): " choice
if [ "$choice" -eq 1 ]; then
    setup_systemd
elif [ "$choice" -eq 2 ]; then
    setup_cron
else
    echo "Неверный выбор. Пропуск настройки автоматического запуска."
fi

echo "Деплой завершён успешно!"
# Проверка контейнеров после запуска
echo "Проверяем состояние контейнеров..."
docker ps -a

if docker logs accr_app 2>&1 | grep -q "could not connect to display"; then
    echo "Ошибка: Проблемы с Xvfb или xcb."
    exit 1
fi

# Инструкция для использования скрипта
cat << EOF
На сервере Ubuntu 22.04 загружаете файл deploy.sh, после вводите команды:
chmod +x deploy.sh
./deploy.sh
EOF

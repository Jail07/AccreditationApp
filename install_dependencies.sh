#!/bin/bash
set -e  # Прерывать выполнение при ошибках

echo "Создание виртуального окружения..."
python3 -m venv .venv

echo "Активация виртуального окружения..."
source .venv/bin/activate

echo "Обновление системы..."
apt update && apt upgrade -y

echo "Установка зависимостей для Python и PostgreSQL..."
apt install -y python3 python3-pip libpq-dev

echo "Установка Python-зависимостей из requirements.txt..."
pip install --no-cache-dir -r requirements.txt

echo "Установка завершена!"

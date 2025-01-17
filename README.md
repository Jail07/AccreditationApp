README: Accreditation Application

Описание

AccreditationApp — это клиент-серверное приложение, предназначенное для управления процессами аккредитации сотрудников. Серверная часть развернута на Linux с базой данных PostgreSQL, а клиентская часть предоставляет графический интерфейс на Windows. Клиент подключается к серверу через IP-адрес для взаимодействия с базой данных.

Архитектура

• Сервер:

• Linux (Ubuntu 22.04).

• PostgreSQL — основная база данных.

• REST API для связи с клиентом (Flask/Django REST Framework).

• Клиент:

• Python + PyQt5 для графического интерфейса.

• Работает на Windows.

• Отправляет запросы на сервер через IP.

Установка и запуск

Серверная часть

1. Установите зависимости:
```
sudo apt update && sudo apt install -y \
    python3 python3-pip python3.11 python3.11-venv \
    postgresql postgresql-contrib \
    libpq-dev
```


2. Создайте базу данных PostgreSQL:
```
sudo -i -u postgres
psql
CREATE DATABASE accr_db;
CREATE USER accr_user WITH PASSWORD 'yourpassword';
GRANT ALL PRIVILEGES ON DATABASE accr_db TO accr_user;
\q
exit
```
3. Склонируйте репозиторий на сервер:
```
git clone https://github.com/yourusername/AccreditationApp.git
cd AccreditationApp
```
4. Настройте виртуальное окружение Python:
```
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
5. Настройте .env файл: Создайте файл .env в корне проекта:
```
DB_NAME=accr_db
DB_USER=accr_user
DB_PASSWORD=yourpassword
DB_HOST=localhost
DB_PORT=5432
```
6. Запустите сервер:
```
python manage.py runserver 0.0.0.0:8000
```
Сервер будет доступен по адресу http://<IP_сервера>:8000.

Клиентская часть

Установите Python на Windows:
Скачайте Python 3.11 и установите его.

Установите зависимости:
Откройте PowerShell или командную строку и выполните:
```
pip install -r requirements.txt
```
Настройте подключение:
В файле config.py укажите IP-адрес сервера:
```
SERVER_URL = "http://<IP_сервера>:8000"
```
Запустите клиент:
```
python main.py
```
Docker (Опционально)

Для упрощения развертывания можно использовать Docker.

Развертывание сервера с Docker

Убедитесь, что Docker и Docker Compose установлены:
```
sudo apt install -y docker.io docker-compose
```
Склонируйте репозиторий и выполните деплой:
```
git clone https://github.com/yourusername/AccreditationApp.git
cd AccreditationApp
chmod +x deploy.sh
./deploy.sh
```
Проверьте запущенные контейнеры:

docker ps

Сервер будет доступен по указанному в docker-compose.yml порту.

Основные зависимости

Сервер:

Django или Flask.

PostgreSQL.

Клиент:

PyQt5.

Requests.

Возможные проблемы

Проблемы с подключением:

Убедитесь, что сервер и клиент находятся в одной сети.

Проверьте настройки брандмауэра.

VNC не используется:

Клиент работает локально на Windows, VNC не требуется.

Версии библиотек:

Убедитесь, что используются совместимые версии PyQt и Python.

Лицензия

Данный проект лицензирован под MIT License.

Контакты

Если у вас есть вопросы, свяжитесь с разработчиком:

Email: jail.alimbekov@alatoo.edu.kg

Telegram: @AlimbekovJail


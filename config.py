# config.py
import os
import logging
from dotenv import load_dotenv

# Загрузка переменных из файла .env (если он существует)
# Создайте файл .env в корне проекта со следующим содержимым:
# DB_NAME=accr_db
# DB_USER=postgres
# DB_PASSWORD=your_secure_password
# DB_HOST=localhost
# DB_PORT=5432
load_dotenv()

# Настройка базового логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("application.log", encoding='utf-8'),
        logging.StreamHandler() # Вывод также в консоль
    ]
)

def get_logger(name):
    """Возвращает настроенный логгер."""
    return logging.getLogger(name)

def get_db_config():
    """
    Загружает конфигурацию БД из переменных окружения.
    Предоставляет значения по умолчанию, если переменные не установлены.
    """
    logger = get_logger(__name__)
    config = {
        'database': os.getenv('DB_NAME', 'accr_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '1234'), # Оставьте значение по умолчанию или удалите его для большей безопасности
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432')
    }
    # Проверка наличия пароля (если нет значения по умолчанию)
    if not config['password'] and os.getenv('DB_PASSWORD') is None:
         logger.warning("Пароль БД не найден в переменных окружения (DB_PASSWORD). Используется пустое значение или стандартное.")
         # Можно добавить логику запроса пароля или выбросить исключение
         # raise ValueError("DB_PASSWORD environment variable not set.")

    # Попытка преобразовать порт в int
    try:
        config['port'] = int(config['port'])
    except (ValueError, TypeError):
        logger.warning(f"Неверное значение для DB_PORT: {config['port']}. Используется порт по умолчанию 5432.")
        config['port'] = 5432

    logger.info(f"Конфигурация БД загружена: host={config['host']}, port={config['port']}, dbname={config['database']}, user={config['user']}")
    return config

# --- Необязательно: Функции для старого config.txt ---
# Оставлено для совместимости, если переход будет постепенным,
# но рекомендуется полностью перейти на .env
CONFIG_PATH = "./config.txt"
legacy_logger = get_logger('legacy_config')

def load_legacy_config():
    """Загружает host:port из config.txt (устаревший метод)."""
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, 'r') as file:
                content = file.read().strip()
                if ':' in content:
                    host, port_str = content.split(':', 1)
                    try:
                        port = int(port_str)
                        return host, port
                    except ValueError:
                        legacy_logger.error(f"Неверный формат порта в {CONFIG_PATH}: {port_str}")
                else:
                    legacy_logger.error(f"Неверный формат в {CONFIG_PATH}. Ожидалось 'host:port'.")
        except Exception as e:
            legacy_logger.error(f"Ошибка чтения {CONFIG_PATH}: {e}")
    return None, None # Возвращаем None, если файла нет или ошибка

def save_legacy_config(ip, port):
     """Сохраняет host:port в config.txt (устаревший метод)."""
     try:
        with open(CONFIG_PATH, 'w') as file:
            file.write(f"{ip}:{port}")
     except Exception as e:
         legacy_logger.error(f"Ошибка записи в {CONFIG_PATH}: {e}")
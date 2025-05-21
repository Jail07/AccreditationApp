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

def get_scheduler_output_dir():
    """Возвращает путь к папке для вывода файлов планировщика."""
    default_dir = os.path.join(os.getcwd(), "scheduler_output") # По умолчанию папка в корне проекта
    output_dir = os.getenv('SCHEDULER_OUTPUT_DIR', default_dir)
    # Можно добавить проверку существования и создание папки здесь или в планировщике
    return output_dir

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

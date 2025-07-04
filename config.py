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
    logger = get_logger(__name__)
    config = {
        'database': os.getenv('DB_NAME', 'accr_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '1234'),
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432')
    }
    if not config['password'] and os.getenv('DB_PASSWORD') is None:
         logger.warning("Пароль БД не найден в переменных окружения (DB_PASSWORD). Используется пустое значение или стандартное.")


    # Попытка преобразовать порт в int
    try:
        config['port'] = int(config['port'])
    except (ValueError, TypeError):
        logger.warning(f"Неверное значение для DB_PORT: {config['port']}. Используется порт по умолчанию 5432.")
        config['port'] = 5432

    logger.info(f"Конфигурация БД загружена: host={config['host']}, port={config['port']}, dbname={config['database']}, user={config['user']}")
    return config

def get_schedule_config(job_name_prefix, default_hour, default_minute, default_day_of_week=None):
    """
    Загружает настройки cron (час, минута, день недели) для задачи из .env.
    job_name_prefix: например, 'EXPIRY', 'RECHECK', 'WEEKLY_TD'
    """
    logger = get_logger(__name__)
    try:
        hour = int(os.getenv(f'SCHED_{job_name_prefix}_HOUR', default_hour))
        minute = int(os.getenv(f'SCHED_{job_name_prefix}_MINUTE', default_minute))
        day_of_week_env = os.getenv(f'SCHED_{job_name_prefix}_DAY_OF_WEEK')

        # Если default_day_of_week не None, значит это еженедельная задача
        if default_day_of_week is not None:
            day_of_week = day_of_week_env if day_of_week_env else default_day_of_week
            logger.info(f"Расписание для {job_name_prefix}: day_of_week={day_of_week}, hour={hour}, minute={minute}")
            return {'day_of_week': day_of_week, 'hour': hour, 'minute': minute}
        else:  # Ежедневная задача
            logger.info(f"Расписание для {job_name_prefix}: hour={hour}, minute={minute}")
            return {'hour': hour, 'minute': minute}

    except ValueError:
        logger.error(
            f"Ошибка чтения числовых параметров расписания для {job_name_prefix}. Использованы значения по умолчанию.")
        if default_day_of_week is not None:
            return {'day_of_week': default_day_of_week, 'hour': default_hour, 'minute': default_minute}
        else:
            return {'hour': default_hour, 'minute': default_minute}
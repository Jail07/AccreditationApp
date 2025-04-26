import os
import logging
from scheduler import Scheduler

CONFIG_PATH = "./config.txt"

def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r') as file:
            return file.read().strip()
    return None

def save_config(ip, port):
    with open(CONFIG_PATH, 'w') as file:
        file.write(f"{ip}:{port}")

def get_db_config():
    config = load_config()
    if config:
        host, port = config.split(":")
    else:
        host, port = "localhost", "5432"

    return {
        'dbname': 'accr_db',
        'user': 'postgres',
        'password': '1234',
        'host': host,
        'port': int(port),
    }

logging.basicConfig(
    filename='scheduler.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

if __name__ == "__main__":
    try:
        logging.info("Запуск планировщика задач...")

        db_config = get_db_config()

        scheduler = Scheduler(db_config)
        scheduler.start()

    except Exception as e:
        logging.error(f"Ошибка при запуске планировщика: {e}")

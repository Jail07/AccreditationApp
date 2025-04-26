# scheduler_runner.py
import os
import time
import signal # Для корректной обработки сигналов завершения
import sys

# Настройка конфига и логирования (делаем это до импорта scheduler)
from config import get_db_config, get_logger
logger = get_logger('scheduler_runner') # Логгер для этого скрипта

from scheduler import Scheduler

# Глобальная ссылка на планировщик для остановки
scheduler_instance = None
keep_running = True # Флаг для основного цикла

def handle_signal(signum, frame):
    """Обработчик сигналов SIGINT (Ctrl+C) и SIGTERM."""
    global keep_running
    logger.warning(f"Получен сигнал {signal.Signals(signum).name}. Завершение работы...")
    keep_running = False # Устанавливаем флаг для выхода из цикла
    # Пытаемся штатно остановить планировщик
    if scheduler_instance:
         logger.info("Попытка штатной остановки планировщика...")
         scheduler_instance.stop()

if __name__ == "__main__":
    logger.info("================ Запуск standalone планировщика ================")

    # Регистрация обработчиков сигналов
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        logger.info("Загрузка конфигурации БД...")
        db_config = get_db_config()

        logger.info("Инициализация планировщика...")
        scheduler_instance = Scheduler(db_config)

        if scheduler_instance.scheduler: # Проверяем, что инициализация успешна
             scheduler_instance.start()

             logger.info("Планировщик запущен. Нажмите Ctrl+C для остановки.")
             # Основной цикл для поддержания работы скрипта
             while keep_running:
                 time.sleep(1) # Проверяем флаг каждую секунду
        else:
            logger.error("Не удалось инициализировать планировщик. Завершение работы.")
            sys.exit(1)

    except ConnectionError as ce:
         logger.exception("Критическая ошибка: Не удалось подключиться к базе данных!")
         sys.exit(1)
    except Exception as e:
        logger.exception(f"Критическая ошибка при запуске standalone планировщика: {e}")
        sys.exit(1)
    finally:
        # Дополнительная проверка и остановка планировщика, если он еще работает
        if scheduler_instance and scheduler_instance.scheduler and scheduler_instance.scheduler.running:
             logger.warning("Планировщик все еще работает после основного цикла. Принудительная остановка.")
             scheduler_instance.stop() # Вызовет закрытие пула БД

        logger.info("Standalone планировщик завершил работу.")
        logger.info("============================================================")
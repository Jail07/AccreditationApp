# main.py
import sys
import os
import threading
import time
from PyQt5.QtWidgets import QApplication, QMessageBox
from PyQt5.QtGui import QIcon

# Первым делом настраиваем конфиг и логирование
from config import get_db_config, get_logger, load_legacy_config, save_legacy_config
logger = get_logger('main') # Получаем главный логгер

from ui import AccreditationApp
from database_manager import DatabaseManager
from scheduler import Scheduler

# Глобальная ссылка на планировщик для возможности остановки
scheduler_instance = None
stop_scheduler_flag = threading.Event()

def start_scheduler_thread(db_conf):
    """Функция для запуска планировщика в отдельном потоке."""
    global scheduler_instance
    logger.info("Поток планировщика запущен.")
    try:
        scheduler_instance = Scheduler(db_conf)
        if scheduler_instance.scheduler: # Проверяем, что планировщик инициализирован
            scheduler_instance.start()
            # Держим поток активным, пока флаг не установлен
            while not stop_scheduler_flag.is_set():
                 time.sleep(1) # Пауза, чтобы не грузить процессор
        else:
             logger.error("Не удалось запустить планировщик (ошибка инициализации).")
    except Exception as e:
         logger.exception("Критическая ошибка в потоке планировщика.")
    finally:
        if scheduler_instance:
             scheduler_instance.stop()
        logger.info("Поток планировщика завершен.")


if __name__ == "__main__":
    logger.info("================ Запуск приложения ================")

    # --- Получение конфигурации БД ---
    try:
        db_config = get_db_config()
        # Проверка соединения с БД перед запуском UI
        logger.info("Проверка соединения с БД...")
        temp_db_manager = DatabaseManager(db_config)
        # Закрываем тестовое соединение/пул (если создался)
        # close_pool() безопасен, даже если пул не был создан полностью
        temp_db_manager.close_pool()
        logger.info("Соединение с БД успешно установлено.")
    except ConnectionError as ce:
         logger.exception("Критическая ошибка: Не удалось подключиться к базе данных при запуске!")
         QMessageBox.critical(None, "Ошибка подключения к БД",
                              f"Не удалось установить соединение с базой данных.\n"
                              f"Проверьте настройки в файле .env или переменные окружения.\n"
                              f"Ошибка: {ce}\n\nПриложение будет закрыто.")
         sys.exit(1) # Выход из приложения
    except Exception as e:
        logger.exception("Критическая ошибка при инициализации конфигурации или БД!")
        QMessageBox.critical(None, "Критическая ошибка",
                              f"Произошла непредвиденная ошибка при запуске:\n{e}\n\nПриложение будет закрыто.")
        sys.exit(1)

    # --- Инициализация QApplication ---
    app = QApplication(sys.argv)

    # --- Запуск планировщика в фоновом потоке ---
    scheduler_db_config = db_config.copy() # Отдельная копия конфига для потока
    scheduler_thread = threading.Thread(target=start_scheduler_thread, args=(scheduler_db_config,), daemon=True)
    scheduler_thread.start()

    # --- Создание и запуск основного окна приложения ---
    try:
        # Создаем основной экземпляр DatabaseManager для GUI
        main_db_manager = DatabaseManager(db_config)
        # Передаем менеджер БД и логгер в UI
        main_window = AccreditationApp(db_manager=main_db_manager, logger=logger)
        main_window.show()
        logger.info("Главное окно приложения отображено.")
    except Exception as e:
         logger.exception("Ошибка при создании главного окна!")
         QMessageBox.critical(None, "Ошибка UI", f"Не удалось создать интерфейс пользователя:\n{e}")
         stop_scheduler_flag.set() # Сигнал потоку планировщика на остановку
         scheduler_thread.join(timeout=5) # Ждем завершения потока
         if hasattr(main_window, 'db_manager') and main_window.db_manager:
              main_window.db_manager.close_pool()
         sys.exit(1)


    # --- Основной цикл приложения ---
    exit_code = app.exec_()

    # --- Завершение работы ---
    logger.info("Начало процедуры завершения работы...")
    # 1. Сигнал потоку планировщика на остановку
    stop_scheduler_flag.set()
    logger.info("Отправлен сигнал на остановку планировщика.")

    # 2. Закрытие пула соединений основного менеджера БД
    if main_db_manager:
        main_db_manager.close_pool()
        logger.info("Пул соединений основного DBManager закрыт.")

    # 3. Ожидание завершения потока планировщика
    logger.info("Ожидание завершения потока планировщика...")
    scheduler_thread.join(timeout=10) # Даем потоку время на завершение
    if scheduler_thread.is_alive():
        logger.warning("Поток планировщика не завершился в течение 10 секунд.")
    else:
        logger.info("Поток планировщика успешно завершен.")

    logger.info(f"Приложение завершено с кодом выхода: {exit_code}")
    logger.info("=====================================================")
    sys.exit(exit_code)
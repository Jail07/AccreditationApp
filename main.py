# main.py
import sys
import os
import threading
import time

from PyQt5.QtWidgets import QApplication, QMessageBox
from PyQt5.QtGui import QIcon
# import qdarktheme # Закомментировано, если вызывает проблемы, для отладки

# Первым делом настраиваем конфиг и логирование
from config import get_db_config, get_logger
logger = get_logger('main') # Получаем главный логгер

from ui import AccreditationApp
from database_manager import DatabaseManager
from scheduler import Scheduler

# Глобальные ссылки
scheduler_instance_global = None # Изменено имя для ясности
stop_scheduler_flag = threading.Event()
main_db_manager_global = None # Глобальная ссылка для доступа в finally
main_window_global = None     # Глобальная ссылка для доступа в finally

def start_scheduler_thread(db_conf):
    """Функция для запуска планировщика в отдельном потоке."""
    global scheduler_instance_global
    logger.info("Поток планировщика: запущен.")
    try:
        # Каждый поток должен иметь свой экземпляр DatabaseManager, если пул не потокобезопасен
        # или если DatabaseManager не спроектирован для общего использования между потоками.
        # Ваш DatabaseManager с классовым атрибутом _pool должен быть потокобезопасен,
        # но создание отдельного экземпляра для планировщика (как в scheduler.py) - хорошая практика.
        # Здесь db_conf уже передается.
        scheduler_instance_global = Scheduler(db_conf)
        if scheduler_instance_global.scheduler: # Проверяем, что планировщик инициализирован
            scheduler_instance_global.start()
            logger.info("Поток планировщика: внутренний планировщик APScheduler запущен.")
            # Держим поток активным, пока флаг не установлен
            while not stop_scheduler_flag.is_set():
                 time.sleep(1) # Пауза, чтобы не грузить процессор
            logger.info("Поток планировщика: получен флаг на остановку.")
        else:
             logger.error("Поток планировщика: не удалось запустить (ошибка инициализации экземпляра Scheduler).")
    except Exception as e:
         logger.exception("Поток планировщика: критическая ошибка.")
    finally:
        if scheduler_instance_global:
             logger.info("Поток планировщика: попытка остановки внутреннего планировщика APScheduler...")
             scheduler_instance_global.stop() # Этот метод должен закрыть и пул БД планировщика
        logger.info("Поток планировщика: завершен.")


if __name__ == "__main__":
    logger.info("================ Запуск приложения AccreditationApp ================")
    exit_code = 1 # Код выхода по умолчанию - ошибка

    # --- Получение конфигурации БД и предварительная проверка соединения ---
    db_config = None
    try:
        db_config = get_db_config()
        logger.info("Предварительная проверка соединения с БД...")
        # Создаем временный менеджер только для проверки соединения и создания таблиц
        # Это гарантирует, что пул инициализируется до UI
        temp_db_manager_for_check = DatabaseManager(db_config)
        temp_db_manager_for_check.create_tables() # Создаем/проверяем таблицы при запуске
        # Не закрываем пул здесь, так как он является классовым атрибутом (_pool)
        # и будет использоваться основным main_db_manager.
        # Если бы каждый DatabaseManager создавал свой собственный пул, то нужно было бы закрыть.
        # temp_db_manager_for_check.close_pool() # Закрывать, если пул - атрибут экземпляра
        logger.info("Предварительная проверка соединения с БД и структуры таблиц прошла успешно.")
    except ConnectionError as ce:
         logger.exception(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось подключиться к базе данных при запуске! {ce}")
         QMessageBox.critical(None, "Ошибка подключения к БД",
                              f"Не удалось установить соединение с базой данных.\n"
                              f"Проверьте настройки (например, в .env) и доступность сервера БД.\n"
                              f"Ошибка: {ce}\n\nПриложение будет закрыто.")
         sys.exit(exit_code)
    except psycopg2.pool.PoolError as pe:
         logger.exception(f"КРИТИЧЕСКАЯ ОШИБКА: Ошибка пула соединений psycopg2 при запуске! {pe}")
         QMessageBox.critical(None, "Ошибка пула БД",
                              f"Произошла ошибка при инициализации пула соединений с БД.\n"
                              f"Ошибка: {pe}\n\nПриложение будет закрыто.")
         sys.exit(exit_code)
    except Exception as e:
        logger.exception("КРИТИЧЕСКАЯ ОШИБКА: Непредвиденная ошибка при инициализации конфигурации или БД!")
        QMessageBox.critical(None, "Критическая ошибка",
                              f"Произошла непредвиденная ошибка при запуске:\n{e}\n\nПриложение будет закрыто.")
        sys.exit(exit_code)

    # --- Инициализация QApplication ---
    app = QApplication(sys.argv)

    # --- Применение иконки приложения ---
    try:
        icon_path = 'icon.png' # Предполагаем, что иконка в той же папке
        if os.path.exists(icon_path):
            app.setWindowIcon(QIcon(icon_path))
            logger.info(f"Иконка приложения '{icon_path}' установлена.")
        else:
            logger.warning(f"Файл иконки '{icon_path}' не найден.")
    except Exception as e:
        logger.error(f"Ошибка при установке иконки приложения: {e}")


    # --- Применение темной темы (если используется) ---
    logger.info("Попытка применения темной темы через pyqtdarktheme...")
    try:
        import qdarktheme # Импорт здесь, чтобы не падать, если его нет
        qdarktheme.setup_theme("dark") # или "auto"
        logger.info("Темная тема pyqtdarktheme применена.")
    except ImportError:
        logger.warning("Библиотека pyqtdarktheme не найдена. Будет использована стандартная тема.")
    except AttributeError: # Ловим ошибку, если setup_theme отсутствует
        logger.error("Ошибка атрибута в pyqtdarktheme. Вероятно, установлена старая версия без setup_theme. Обновите: pip install --upgrade pyqtdarktheme")
    except Exception as e: # Другие возможные ошибки с qdarktheme
        logger.warning(f"Не удалось применить тему qdarktheme: {e}. Будет использована стандартная тема.")

    # --- Запуск планировщика в фоновом потоке ---
    # Передаем копию конфига, чтобы избежать потенциальных проблем с изменяемыми объектами
    scheduler_db_config_copy = db_config.copy()
    scheduler_thread = threading.Thread(target=start_scheduler_thread, args=(scheduler_db_config_copy,), daemon=True)
    scheduler_thread.start()

    try:
        # Создаем основной экземпляр DatabaseManager для GUI
        # Пул уже должен быть инициализирован предыдущей проверкой
        main_db_manager_global = DatabaseManager(db_config)
        main_window_global = AccreditationApp(db_manager=main_db_manager_global, logger=logger)
        main_window_global.show()
        logger.info("Главное окно приложения отображено.")

        exit_code = app.exec_() # Основной цикл приложения

    except psycopg2.pool.PoolError as pe: # Ловим ошибку пула и здесь
         logger.exception(f"КРИТИЧЕСКАЯ ОШИБКА: Ошибка пула соединений psycopg2 во время работы UI! {pe}")
         QMessageBox.critical(None, "Ошибка пула БД",
                              f"Произошла ошибка пула соединений с БД во время работы приложения.\n"
                              f"Ошибка: {pe}\n\nПриложение будет закрыто.")
         # Попытка корректно завершить то, что можно
    except Exception as e:
        logger.exception("КРИТИЧЕСКАЯ ОШИБКА: Непредвиденная ошибка при создании или работе главного окна!")
        QMessageBox.critical(None, "Критическая ошибка UI",
                              f"Произошла непредвиденная ошибка в работе интерфейса:\n{e}\n\nПриложение будет закрыто.")
        # Попытка корректно завершить то, что можно
    finally:
        logger.info("Начало процедуры ЗАВЕРШЕНИЯ РАБОТЫ ПРИЛОЖЕНИЯ (блок finally в main)...")

        # 1. Сигнал потоку планировщика на остановку
        if scheduler_thread and scheduler_thread.is_alive():
            logger.info("Отправка сигнала на остановку потока планировщика...")
            stop_scheduler_flag.set()
            logger.info("Ожидание завершения потока планировщика (до 10 секунд)...")
            scheduler_thread.join(timeout=10) # Даем потоку время на штатное завершение
            if scheduler_thread.is_alive():
                logger.warning("Поток планировщика не завершился штатно в течение 10 секунд.")
            else:
                logger.info("Поток планировщика успешно завершен.")
        elif scheduler_thread:
             logger.info("Поток планировщика уже был завершен или не запущен.")
        else:
             logger.info("Поток планировщика не был создан.")


        # 2. Закрытие пула соединений основного менеджера БД
        # Пул соединений (_pool) является атрибутом класса DatabaseManager.
        # Достаточно одного вызова close_pool() для этого класса.
        # Но для чистоты, если main_db_manager_global был создан, можно вызвать через него.
        # Однако, scheduler_instance_global.stop() уже должен был закрыть свой db_manager.close_pool(),
        # который обращается к тому же классовому пулу.
        # Чтобы избежать двойного закрытия или закрытия несуществующего пула,
        # лучше всего, чтобы метод close_pool был идемпотентным.
        logger.info("Попытка закрытия пула соединений PostgreSQL...")
        if DatabaseManager._pool is not None: # Проверяем, что пул вообще был создан
            try:
                DatabaseManager.close_pool() # Статический вызов или через экземпляр
                logger.info("Пул соединений PostgreSQL успешно закрыт.")
            except Exception as e_pool_close:
                logger.error(f"Ошибка при закрытии пула соединений: {e_pool_close}")
        else:
            logger.info("Пул соединений не был инициализирован, закрытие не требуется.")

        logger.info(f"Приложение AccreditationApp завершено с кодом выхода: {exit_code}")
        logger.info("========================= КОНЕЦ РАБОТЫ =========================")
        sys.exit(exit_code)
import sys
from PyQt5.QtWidgets import QApplication
from ui import AccreditationApp
from database_manager import DatabaseManager
from scheduler import Scheduler



db_config = {
    "db_name": "bap",
    "user": "postgres",
    "password": 1234,
    "host": "localhost",
    "port": 5432
}

db_manager = DatabaseManager(**db_config)
db_manager.create_tables()

# scheduler = Scheduler(db_config)
# scheduler.start()

if __name__ == "__main__":
    app = QApplication(sys.argv)


    ex = AccreditationApp(db_manager)  # Создаем главное окно
    ex.show()  # Показываем окно

    # scheduler = Scheduler(db_config)
    #
    # # Добавляем задачу, которая выполняется каждые 2 минуты
    # scheduler.scheduler.add_job(scheduler.generate_recheck_file, "interval", minutes=1)
    #
    # # Запуск планировщика
    # scheduler.start()
    #
    # print("Планировщик запущен. Ожидайте выполнения задачи.")
    #
    # try:
    #     # Оставляем планировщик работать
    #     while True:
    #         pass
    # except (KeyboardInterrupt, SystemExit):
    #     # Остановка планировщика при завершении работы
    #     scheduler.stop()

    sys.exit(app.exec_())
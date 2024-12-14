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

scheduler = Scheduler(db_config)
scheduler.start()

if __name__ == "__main__":
    app = QApplication(sys.argv)


    ex = AccreditationApp(db_manager)  # Создаем главное окно
    ex.show()  # Показываем окно

    sys.exit(app.exec_())
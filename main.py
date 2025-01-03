import os
import sys
from datetime import datetime
from pytz import timezone

from PyQt5.QtWidgets import QApplication, QLabel
from ui import AccreditationApp
from database_manager import DatabaseManager
from scheduler import Scheduler
import threading


db_config = {
    "db_name": "bap",
    "user": "postgres",
    "password": "1234",
    "host": "db",  # Имя сервиса в Docker Compose
    "port": 5432
}




def start_scheduler():
    """
    Запускает планировщик в отдельном потоке.
    """
    scheduler = Scheduler(db_config)
    scheduler.start()



if __name__ == "__main__":
    # Создаем поток для работы планировщика
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()

    # Основное приложение
    app = QApplication(sys.argv)
    db_manager = DatabaseManager(**db_config)
    db_manager.create_tables()

    ex = AccreditationApp(db_manager)
    ex.show()

    sys.exit(app.exec_())


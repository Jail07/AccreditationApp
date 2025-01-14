import os
import sys


from PyQt5.QtWidgets import QApplication, QLabel
from ui import AccreditationApp
from database_manager import DatabaseManager
from scheduler import Scheduler
import threading


db_config = {
    'db_name': os.getenv('DB_NAME', 'accr_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '1234'),
    'host': os.getenv('DB_HOST', 'db'),
    'port': os.getenv('DB_PORT', 5432),
}


def start_scheduler():
    scheduler = Scheduler(db_config)
    scheduler.start()


if __name__ == "__main__":
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()

    app = QApplication(sys.argv)
    db_manager = DatabaseManager(**db_config)
    db_manager.create_tables()

    ex = AccreditationApp(db_manager)
    ex.show()

    sys.exit(app.exec_())


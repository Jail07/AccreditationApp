import os
import sys
import threading
import psycopg2
from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox
from ui import AccreditationApp
from database_manager import DatabaseManager
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

def connect_to_db(ip, port):
    try:
        conn = psycopg2.connect(
            dbname="accr_db",
            user="postgres",
            password="1234",
            host=ip,
            port=port
        )
        return conn
    except Exception as e:
        print("Connection error:", e)
        return None

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Клиент PostgreSQL")
        self.resize(400, 300)

        config = load_config()
        if not config:
            self.ask_for_connection()
        else:
            ip, port = config.split(":")
            self.test_connection(ip, port)

    def ask_for_connection(self):
        ip, port = "localhost", "5432"
        save_config(ip, port)
        self.test_connection(ip, port)

    def test_connection(self, ip, port):
        conn = connect_to_db(ip, port)
        if conn:
            QMessageBox.information(self, "Успешно", "Подключение к БД установлено!")
            conn.close()
        else:
            QMessageBox.warning(self, "Ошибка", "Не удалось подключиться к серверу.")

if __name__ == "__main__":
    config = load_config()
    if config:
        host, port = config.split(":")
    else:
        host, port = "localhost", "5432"

    db_config = {
        'db_name': os.getenv('DB_NAME', 'accr_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '1234'),
        'host': host,
        'port': int(port),
    }

    def start_scheduler():
        scheduler = Scheduler(db_config)
        scheduler.start()

    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()

    app = QApplication(sys.argv)

    db_manager = DatabaseManager(**db_config)
    try:
        db_manager.create_tables()
    except Exception as e:
        print("Ошибка при создании таблиц:", e)

    main_window = AccreditationApp(db_manager)
    main_window.show()

    sys.exit(app.exec_())

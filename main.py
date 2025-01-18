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
    """Загрузка конфигурации из файла."""
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r') as file:
            return file.read().strip()
    return None

def save_config(ip, port):
    """Сохранение конфигурации в файл."""
    with open(CONFIG_PATH, 'w') as file:
        file.write(f"{ip}:{port}")

def connect_to_db(ip, port):
    """Подключение к базе данных с использованием указанных параметров."""
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

        # Загружаем конфиг
        config = load_config()
        if not config:
            self.ask_for_connection()
        else:
            ip, port = config.split(":")
            self.test_connection(ip, port)

    def ask_for_connection(self):
        """Спрашивает у пользователя IP и порт сервера."""
        ip, port = "localhost", "5432"  # Значения по умолчанию
        # Здесь добавить форму для ввода IP/порта через GUI
        save_config(ip, port)
        self.test_connection(ip, port)

    def test_connection(self, ip, port):
        """Проверяет подключение к базе данных."""
        conn = connect_to_db(ip, port)
        if conn:
            QMessageBox.information(self, "Успешно", "Подключение к БД установлено!")
            conn.close()
        else:
            QMessageBox.warning(self, "Ошибка", "Не удалось подключиться к серверу.")

if __name__ == "__main__":
    # Основной процесс
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

    # Запускаем планировщик в отдельном потоке
    def start_scheduler():
        scheduler = Scheduler(db_config)
        scheduler.start()

    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()

    # Инициализируем приложение
    app = QApplication(sys.argv)

    # Создаём менеджер базы данных и выполняем инициализацию
    db_manager = DatabaseManager(**db_config)
    try:
        db_manager.create_tables()
    except Exception as e:
        print("Ошибка при создании таблиц:", e)

    # Запускаем графический интерфейс
    main_window = AccreditationApp(db_manager)
    main_window.show()

    sys.exit(app.exec_())

import os
import psycopg2
from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox

CONFIG_PATH = "config.txt"

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

        # Загружаем конфиг
        config = load_config()
        if not config:
            self.ask_for_connection()
        else:
            ip, port = config.split(":")
            self.test_connection(ip, port)

    def ask_for_connection(self):
        ip, port = "localhost", "5432"  # Значения по умолчанию
        # Здесь добавить форму для ввода IP/порта через GUI
        save_config(ip, port)
        self.test_connection(ip, port)

    def test_connection(self, ip, port):
        conn = connect_to_db(ip, port)
        if conn:
            QMessageBox.information(self, "Успешно", "Подключение к БД установлено!")
        else:
            QMessageBox.warning(self, "Ошибка", "Не удалось подключиться к серверу.")

if __name__ == "__main__":
    app = QApplication([])
    window = MainWindow()
    window.show()
    app.exec()

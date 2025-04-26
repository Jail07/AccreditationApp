# file_manager.py
import os
import pandas as pd
from PyQt5.QtWidgets import QFileDialog, QMessageBox
from datetime import datetime
import logging
from config import get_logger # Используем настроенный логгер

class FileManager:
    def __init__(self, parent_widget=None):
        # parent_widget нужен только для QFileDialog
        self.parent_widget = parent_widget
        self.logger = get_logger(__name__)

    def open_file_dialog(self):
        """Открывает диалог выбора Excel файла."""
        file_name, _ = QFileDialog.getOpenFileName(
            self.parent_widget,
            "Выберите файл Excel",
            "",
            "Excel Files (*.xlsx *.xls)"
        )
        if file_name:
            self.logger.info(f"Выбран файл для загрузки: {file_name}")
        return file_name

    def save_file_dialog(self, default_filename="report"):
        """Открывает диалог сохранения Excel файла."""
        # Добавляем дату и время к имени файла по умолчанию
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        suggested_filename = f"{default_filename}_{timestamp}.xlsx"

        save_path, _ = QFileDialog.getSaveFileName(
            self.parent_widget,
            "Сохранить файл Excel",
            suggested_filename,
            "Excel Files (*.xlsx);;All Files (*)"
        )
        if save_path:
             # Убедимся, что расширение .xlsx
             if not save_path.lower().endswith('.xlsx'):
                 save_path += '.xlsx'
             self.logger.info(f"Выбран путь для сохранения: {save_path}")
        return save_path

    def save_dataframe(self, df, default_filename="report"):
        """Сохраняет DataFrame в Excel, запрашивая путь у пользователя."""
        if df is None or df.empty:
            self.logger.warning(f"Попытка сохранить пустой DataFrame ({default_filename}).")
            QMessageBox.warning(self.parent_widget, "Нет данных", f"Нет данных для сохранения в файл '{default_filename}'.")
            return False # Возвращаем False при неудаче

        save_path = self.save_file_dialog(default_filename)
        if not save_path:
            self.logger.info("Сохранение файла отменено пользователем.")
            return False # Возвращаем False при отмене

        try:
            df.to_excel(save_path, index=False, engine='openpyxl') # Указываем движок
            self.logger.info(f"DataFrame успешно сохранен в: {save_path}")
            QMessageBox.information(self.parent_widget, "Успешно", f"Файл '{os.path.basename(save_path)}' успешно сохранен.")
            return True # Возвращаем True при успехе
        except ImportError:
             self.logger.exception("Ошибка сохранения: движок 'openpyxl' не установлен. Установите его: pip install openpyxl")
             QMessageBox.critical(self.parent_widget, "Ошибка", "Необходима библиотека 'openpyxl'. Установите ее и перезапустите приложение.\n(pip install openpyxl)")
             return False
        except Exception as e:
            self.logger.exception(f"Ошибка при сохранении DataFrame в {save_path}: {e}")
            QMessageBox.critical(self.parent_widget, "Ошибка сохранения", f"Не удалось сохранить файл '{os.path.basename(save_path)}':\n{e}")
            return False # Возвращаем False при ошибке

    def save_reports(self, reports_dict):
        """
        Сохраняет несколько DataFrame из словаря, запрашивая путь для каждого.
        reports_dict: {'Название_отчета': DataFrame}
        """
        self.logger.info(f"Запрос на сохранение {len(reports_dict)} отчетов.")
        saved_count = 0
        total_count = len(reports_dict)

        for report_name, df in reports_dict.items():
            if df is not None and not df.empty:
                if self.save_dataframe(df, default_filename=report_name):
                    saved_count += 1
            else:
                self.logger.info(f"Отчет '{report_name}' пуст, сохранение пропущено.")
                total_count -= 1 # Уменьшаем общее число, если отчет пуст

        self.logger.info(f"Сохранено {saved_count} из {total_count} непустых отчетов.")
        return saved_count

    def generate_file_scheduler(self, df, filename_prefix):
        """
        Генерирует файл в предопределенной папке (для планировщика).
        Возвращает путь к файлу или None.
        """
        if df is None or df.empty:
            self.logger.warning(f"Планировщик: Нет данных для генерации файла {filename_prefix}.")
            return None

        output_dir = "scheduler_output" # Папка для файлов планировщика
        os.makedirs(output_dir, exist_ok=True) # Создаем папку, если её нет

        timestamp = datetime.now().strftime('%Y%m%d')
        filename = f"{filename_prefix}_{timestamp}.xlsx"
        save_path = os.path.join(output_dir, filename)

        try:
            df.to_excel(save_path, index=False, engine='openpyxl')
            self.logger.info(f"Планировщик: Файл успешно сгенерирован: {save_path}")
            return save_path
        except ImportError:
             self.logger.exception("Ошибка генерации файла: движок 'openpyxl' не установлен.")
             # В планировщике нет GUI, просто логируем
             return None
        except Exception as e:
            self.logger.exception(f"Планировщик: Ошибка при генерации файла {save_path}: {e}")
            return None
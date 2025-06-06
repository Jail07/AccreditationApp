# file_manager.py
import os
import pandas as pd

from datetime import datetime
import logging
from config import get_logger, get_scheduler_output_dir  # Используем настроенный логгер

# Только если доступен GUI
try:
    from PyQt5.QtWidgets import QFileDialog, QMessageBox
    GUI_AVAILABLE = True
except ImportError:
    GUI_AVAILABLE = False
    logging.warning('PyQt5 not available')

class FileManager:
    def __init__(self, parent_widget=None):
        # parent_widget нужен только для QFileDialog
        self.parent_widget = parent_widget
        self.logger = get_logger(__name__)

    def _apply_excel_formatting(self, df, writer, sheet_name):
        """
        Применяет форматирование к листу Excel: автоширина столбцов и границы.
        df: DataFrame, который был записан.
        writer: объект pandas ExcelWriter.
        sheet_name: имя листа для форматирования.
        """
        if df.empty:
            return

        workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        # 1. Формат для границ
        border_format = workbook.add_format({'border': 1})

        # Применяем границы ко всем ячейкам с данными и заголовкам
        # Формат: (первая_строка, первый_столбец, последняя_строка, последний_столбец, формат)
        worksheet.conditional_format(0, 0, len(df), len(df.columns) - 1, {
            'type': 'no_blanks',  # Применяем ко всем непустым ячейкам
            'format': border_format
        })
        # Принудительно применяем к заголовкам тоже
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, border_format)

        # 2. Автоматическая ширина столбцов
        for idx, col in enumerate(df.columns):
            # Находим максимальную ширину в столбце (учитывая заголовок)
            max_len = max(
                len(str(col)),  # Длина заголовка
                df[col].astype(str).str.len().max()  # Максимальная длина значения в столбце
            )
            # Устанавливаем ширину с небольшим запасом
            worksheet.set_column(idx, idx, max_len + 2)

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
        Сохраняет несколько отчетов (DataFrame'ов) в один ФОРМАТИРОВАННЫЙ Excel файл,
        каждый на своем листе.
        reports_dict: словарь, где ключ - имя листа, значение - DataFrame.
        """
        column_rename_map = {"Validation_Errors": "Ошибка данных"}
        columns_to_remove = ["Статус БД", "ID", "Статус Проверки"]

        # Удаляем их из всех DataFrame'ов в отчете
        for sheet_name, df in reports_dict.items():
            reports_dict[sheet_name] = df.drop(columns=[col for col in columns_to_remove if col in df.columns])

        # Подготовка отчетов с фильтрацией и переименованием
        filtered_reports = {}
        # Фильтрация и переименование
        for df in reports_dict.values():
            df.rename(columns=column_rename_map, inplace=True)

        total_files_to_save = sum(1 for df in reports_dict.values() if not df.empty)
        if total_files_to_save == 0:
            QMessageBox.information(self.parent_widget, "Нет данных", "Нет данных для сохранения в отчеты.")
            return 0

        save_path, _ = QFileDialog.getSaveFileName(
            self.parent_widget, "Сохранить отчеты как...", f"Отчет_{datetime.now().strftime('%Y%m%d')}.xlsx",
            "Excel Files (*.xlsx)"
        )

        if not save_path:
            return 0

        saved_count = 0
        print("Список листов в отчёте:", reports_dict.keys())

        try:
            with pd.ExcelWriter(save_path, engine='xlsxwriter') as writer:
                for sheet_name, df_report in reports_dict.items():
                    if not df_report.empty:
                        df_report.to_excel(writer, sheet_name=sheet_name, index=False)
                        # Применяем форматирование для каждого листа
                        self._apply_excel_formatting(df_report, writer, sheet_name)
                        saved_count += 1
            QMessageBox.information(self.parent_widget, "Успешно", f"Отчеты успешно сохранены в файл:\n{save_path}")
        except Exception as e:
            error_details = str(e)
            self.logger.exception(f"Ошибка при сохранении отчетов в Excel: {e}")
            QMessageBox.critical(self.parent_widget, "Ошибка сохранения", f"Не удалось сохранить отчеты.\n\nОшибка: {error_details}")

        self.logger.info(f"Сохранено {saved_count} из {total_files_to_save} непустых отчетов.")
        return saved_count

    def generate_file_scheduler(self, df, filename_prefix):
        """
        Генерирует ФОРМАТИРОВАННЫЙ файл в предопределенной папке (для планировщика).
        Возвращает путь к файлу или None.
        """
        if df is None or df.empty:
            self.logger.warning(f"Планировщик: Нет данных для генерации файла {filename_prefix}.")
            return None

        output_dir = get_scheduler_output_dir() # Используем функцию из config.py
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        filename = f"{filename_prefix}_{timestamp}.xlsx"
        save_path = os.path.join(output_dir, filename)
        sheet_name = 'Данные' # Название листа внутри Excel файла

        try:
            # Используем ExcelWriter с движком xlsxwriter
            with pd.ExcelWriter(save_path, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                # Применяем форматирование
                self._apply_excel_formatting(df, writer, sheet_name)

            self.logger.info(f"Планировщик: Файл успешно сгенерирован и отформатирован: {save_path}")
            return save_path
        except Exception as e:
            self.logger.exception(f"Ошибка при генерации отформатированного файла: {e}")
            return None
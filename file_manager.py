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
        Сохраняет листы из reports_dict в два отдельных Excel-файла:
        - Все листы с "ГПХ" в названии -> в файл *_ГПХ.xlsx
        - Все листы с "Подрядчики" и "Ошибки_Подрядчики" -> в файл *_Подрядчики.xlsx
        """
        if not any(not df.empty for df in reports_dict.values()):
            QMessageBox.information(self.parent_widget, "Нет данных", "Нет данных для сохранения в отчеты.")
            return 0

        # Спросим у пользователя базовое имя и путь для отчетов
        save_path_base, _ = QFileDialog.getSaveFileName(
            self.parent_widget,
            "Укажите базовое имя для отчетов (будет создано 2 файла)",
            f"Отчет_{datetime.now().strftime('%Y%m%d')}",
            "Excel Files (*.xlsx)"
        )

        if not save_path_base:
            return 0

        # Убираем расширение .xlsx, если пользователь его добавил
        if save_path_base.endswith('.xlsx'):
            save_path_base = save_path_base[:-5]

        # Определяем полные имена для двух файлов
        file_gph_path = f"{save_path_base}_ГПХ.xlsx"
        file_others_path = f"{save_path_base}_Подрядчики.xlsx"

        # Колонки, которые не нужны в отчетах
        columns_to_remove = ["Статус БД", "ID", "Статус Проверки", "Name_Check_Required"]
        # Как переименовать колонку с ошибками
        column_rename_map = {"Validation_Errors": "Причина отклонения"}

        saved_sheets_count = 0
        try:
            # --- Файл 1: Только ГПХ ---
            with pd.ExcelWriter(file_gph_path, engine='xlsxwriter') as writer_gph:
                self.logger.info(f"Создание файла для ГПХ: {file_gph_path}")
                for sheet_name, df in reports_dict.items():
                    if "ГПХ" in sheet_name and not df.empty:
                        df_to_write = df.drop(columns=[col for col in columns_to_remove if col in df.columns],
                                              errors='ignore')
                        df_to_write.rename(columns=column_rename_map, inplace=True)

                        # Создаем более читаемое имя листа
                        clean_sheet_name = sheet_name.replace('ГПХ_', '').replace('_', ' ')
                        df_to_write.to_excel(writer_gph, sheet_name=clean_sheet_name, index=False)
                        self._apply_excel_formatting(df_to_write, writer_gph, clean_sheet_name)
                        saved_sheets_count += 1

            # --- Файл 2: Подрядчики и их ошибки ---
            with pd.ExcelWriter(file_others_path, engine='xlsxwriter') as writer_others:
                self.logger.info(f"Создание файла для Подрядчиков: {file_others_path}")
                for sheet_name, df in reports_dict.items():
                    if "Подрядчики" in sheet_name and not df.empty:
                        df_to_write = df.drop(columns=[col for col in columns_to_remove if col in df.columns],
                                              errors='ignore')
                        df_to_write.rename(columns=column_rename_map, inplace=True)

                        clean_sheet_name = sheet_name.replace('Подрядчики_', '').replace('_', ' ')
                        df_to_write.to_excel(writer_others, sheet_name=clean_sheet_name, index=False)
                        self._apply_excel_formatting(df_to_write, writer_others, clean_sheet_name)
                        saved_sheets_count += 1

            if saved_sheets_count > 0:
                QMessageBox.information(
                    self.parent_widget,
                    "Успешно",
                    f"Отчеты успешно сохранены в два файла:\n\n1. {os.path.basename(file_gph_path)}\n2. {os.path.basename(file_others_path)}"
                )
            else:
                QMessageBox.information(self.parent_widget, "Нет данных",
                                        "Данных для сохранения в отчеты не найдено после фильтрации.")

        except Exception as e:
            error_details = str(e)
            self.logger.exception(f"Ошибка при сохранении отчетов: {e}")
            QMessageBox.critical(
                self.parent_widget,
                "Ошибка сохранения",
                f"Не удалось сохранить файлы отчетов.\n\nОшибка: {error_details}"
            )

        self.logger.info(f"Успешно сохранено {saved_sheets_count} листов в два Excel файла.")
        return saved_sheets_count

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
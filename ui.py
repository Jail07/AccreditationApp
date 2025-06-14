# ui.py
import inspect
import os
import sys

import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
from multiprocessing import Pool, cpu_count
import pytz
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor # Для фоновых задач

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton, QTableWidget,
    QTableWidgetItem, QTextEdit, QLabel, QMessageBox, QApplication, QSplitter,
    QHeaderView, QAbstractItemView, QDialog,
    QCheckBox, QDialogButtonBox, QTableView, QInputDialog
)
from PyQt5.QtGui import QIcon, QColor, QBrush, QStandardItem, QStandardItemModel
from PyQt5.QtCore import Qt, QRunnable, QThreadPool, pyqtSignal, QObject, pyqtSlot, QThread

from config import get_logger
from data_processing import DataProcessor, process_data_chunk
from file_manager import FileManager
from database_manager import DatabaseManager

# --- Worker для фоновых задач ---
class WorkerSignals(QObject):
    """Сигналы для Worker'а"""
    finished = pyqtSignal(object) # Сигнал завершения с результатом
    error = pyqtSignal(str)       # Сигнал ошибки
    progress = pyqtSignal(int)    # Сигнал прогресса (0-100)
    log = pyqtSignal(str, str)    # Сигнал для логирования (message, level)
    request_confirmation = pyqtSignal(str, int) # Запрос подтверждения у пользователя (message, row_index)
    request_new_employee_action = pyqtSignal(dict, int)

class Worker(QRunnable):
    """Исполнитель задач в отдельном потоке"""
    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()
        # Добавляем сигналы в kwargs, чтобы функция могла их использовать
        # self.kwargs['signals'] = self.signals

    @pyqtSlot()
    def run(self):
        try:
            self.signals.log.emit(f"Запуск задачи {self.fn.__name__} в фоновом потоке...", "DEBUG")

            # --- ИЗМЕНЕНИЕ ЗДЕСЬ: Проверяем, принимает ли функция 'signals' ---
            func_signature = inspect.signature(self.fn)
            pass_signals = False
            if 'signals' in func_signature.parameters:
                pass_signals = True
            else:
                # Проверяем наличие **kwargs
                for param in func_signature.parameters.values():
                    if param.kind == param.VAR_KEYWORD:  # VAR_KEYWORD соответствует **kwargs
                        pass_signals = True
                        break

            final_kwargs = self.kwargs.copy()
            if pass_signals:
                final_kwargs['signals'] = self.signals

            result = self.fn(*self.args, **final_kwargs)  # Используем final_kwargs
            self.signals.log.emit(f"Задача {self.fn.__name__} завершена.", "DEBUG")
            self.signals.finished.emit(result)
        except Exception as e:
            error_msg = f"Ошибка в фоновой задаче {self.fn.__name__}: {e}\n{traceback.format_exc()}"
            self.signals.log.emit(error_msg, "ERROR")
            self.signals.error.emit(str(e))

# --- Диалог подтверждения необычных имен ---
class ConfirmationDialog(QDialog):
    def __init__(self, message, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Подтверждение данных")
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(message))

        self.confirm_checkbox = QCheckBox("Подтверждаю, данные корректны")
        layout.addWidget(self.confirm_checkbox)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, Qt.Horizontal, self)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        # Кнопка OK неактивна, пока не отмечен чекбокс
        ok_button = buttons.button(QDialogButtonBox.Ok)
        ok_button.setEnabled(False)
        self.confirm_checkbox.stateChanged.connect(lambda state: ok_button.setEnabled(state == Qt.Checked))

    def is_confirmed(self):
        return self.result() == QDialog.Accepted and self.confirm_checkbox.isChecked()

class HistoryDialog(QDialog):
    def __init__(self, history_records, parent=None):
        super().__init__(parent)
        self.setWindowTitle("История операций")
        self.setMinimumSize(600, 400)

        layout = QVBoxLayout(self)
        self.historyTable = QTableView() # Используем QTableView для лучшей производительности
        self.historyTable.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.historyTable.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.historyTable.setSelectionMode(QAbstractItemView.SingleSelection)
        self.historyTable.setAlternatingRowColors(True)
        self.historyTable.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.historyTable.setSortingEnabled(True) # Включаем сортировку

        layout.addWidget(self.historyTable)

        # Кнопка Закрыть
        buttons = QDialogButtonBox(QDialogButtonBox.Close, Qt.Horizontal, self)
        buttons.rejected.connect(self.reject) # Close привязан к reject
        layout.addWidget(buttons)

        self.populate_history(history_records)

    def populate_history(self, records):
        model = QStandardItemModel(len(records), 3, self) # Строки, 3 колонки
        model.setHorizontalHeaderLabels(['Дата операции', 'Тип операции', 'Детали'])

        if not records: # Проверка на пустой список
             return model

        # Устанавливаем таймзону для отображения
        local_tz = pytz.timezone("Europe/Moscow") # Или используйте self.timezone из основного окна

        for i, record in enumerate(records):
            # Преобразование времени в локальное
            dt_aware = record['operation_date'].astimezone(local_tz)
            dt_str = dt_aware.strftime('%Y-%m-%d %H:%M:%S')

            model.setItem(i, 0, QStandardItem(dt_str))
            model.setItem(i, 1, QStandardItem(record.get('operation_type', '')))
            model.setItem(i, 2, QStandardItem(record.get('details', '')))

        self.historyTable.setModel(model)
        self.historyTable.resizeColumnsToContents()
        self.historyTable.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch) # Растягиваем Детали
        self.historyTable.sortByColumn(0, Qt.DescendingOrder) # Сортируем по дате (сначала новые)

# --- Основное окно приложения ---
class AccreditationApp(QWidget):
    # Сигнал для обновления UI из другого потока
    update_table_signal = pyqtSignal(pd.DataFrame)
    update_log_signal = pyqtSignal(str, str)
    update_notes_signal = pyqtSignal(str)
    task_finished_signal = pyqtSignal(str) # Сигнал завершения долгой задачи
    ask_confirmation_signal = pyqtSignal(str, int) # Сигнал для запроса подтверждения
    ask_new_employee_action_signal = pyqtSignal(dict, int)

    COL_STATUS_DB = 7
    COL_STATUS_PROV = 8
    COL_ERR_VALID = 9
    COL_PRIM = 10
    COL_START_AKKR = 11
    COL_END_AKKR = 12

    # Словарь для хранения подтверждений пользователя по индексам строк
    user_confirmations = {}
    new_employee_actions = {}  # Для новых сотрудников в файле активации
    def __init__(self, db_manager: DatabaseManager, logger: logging.Logger):
        super().__init__()
        self.db_manager = db_manager
        self.logger = logger
        self.file_manager = FileManager(parent_widget=self)
        self.processor = DataProcessor()
        self.timezone = pytz.timezone("Europe/Moscow")
        self.df_loaded = None # Исходный DataFrame после загрузки
        self.df_processed = None # DataFrame после очистки и валидации
        self.df_to_add_td = pd.DataFrame() # Данные для добавления в TD
        self.current_reports = {} # Словарь для сгенерированных отчетов
        self.thread_pool = QThreadPool() # Пул потоков для задач
        self.logger.info(f"Максимальное количество потоков: {self.thread_pool.maxThreadCount()}")
        self.initUI()
        self.connect_signals()

    def connect_signals(self):
        """Подключение сигналов к слотам."""
        self.update_table_signal.connect(self.displayTable)
        self.update_log_signal.connect(self.logMessage)
        self.update_notes_signal.connect(self.displayNotes)
        self.task_finished_signal.connect(self.on_task_finished)
        self.ask_confirmation_signal.connect(self.handle_confirmation_request)
        self.ask_new_employee_action_signal.connect(self.handle_new_employee_action_request)

    def logMessage(self, message, level="INFO"):
        """Логирует сообщение в QTextEdit и стандартный логгер."""
        # Проверка, вызывается ли из основного потока
        if QApplication.instance().thread() != QThread.currentThread():
            # Если из другого потока, используем сигнал
            self.update_log_signal.emit(message, level)
            return

        # Логирование через стандартный логгер
        log_level = getattr(logging, level.upper(), logging.INFO)
        self.logger.log(log_level, message)

        # Отображение в QTextEdit
        timestamp = datetime.now(self.timezone).strftime("%Y-%m-%d %H:%M:%S")
        color_map = {
            "DEBUG": "grey",
            "INFO": "green",
            "WARNING": "orange",
            "ERROR": "darkred",
            "CRITICAL": "red"
        }
        color = color_map.get(level.upper(), "black")
        formatted_message = f'<font color="{color}">[{timestamp}] [{level.upper()}] {message}</font>'
        self.logText.append(formatted_message)

    def initUI(self):
        """Инициализация интерфейса."""
        self.setWindowTitle('Система Учета Аккредитации v2.0') # Новое название
        # Установка иконки (замените 'icon.png' на путь к вашей иконке)
        try:
            self.setWindowIcon(QIcon('icon.png'))
        except Exception as e:
             self.logMessage(f"Не удалось загрузить иконку 'icon.png': {e}", "WARNING")

        # --- Основной Layout ---
        main_layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Horizontal)  # Используем горизонтальный разделитель

        # --- Левая панель (Таблица, Поиск, Примечания) ---
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)

        # --- Правая панель (Управление файлами, Лог) ---
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)

        # Поиск, ЧС, История, Активация файлом
        action_layout = QHBoxLayout()
        action_layout.addWidget(QLabel("Поиск:"))
        self.searchEdit = QLineEdit()
        self.searchEdit.setPlaceholderText("Введите ФИО или организацию...")
        self.searchEdit.returnPressed.connect(self.run_search_people)
        self.btnSearch = QPushButton("Найти")
        self.btnSearch.clicked.connect(self.run_search_people)
        self.btnBlacklist = QPushButton("В ЧС / Из ЧС")
        self.btnBlacklist.clicked.connect(self.run_manage_blacklist)
        self.btnHistory = QPushButton("История")
        self.btnHistory.clicked.connect(self.run_show_history)
        self.btnLoadActivation = QPushButton("Загрузить файл активации")  # <-- Новая кнопка
        self.btnLoadActivation.clicked.connect(self.run_process_activation_file)  # <-- Новый слот

        action_layout.addWidget(self.searchEdit)
        action_layout.addWidget(self.btnSearch)
        action_layout.addWidget(self.btnBlacklist)
        # УБРАЛИ self.btnSetActive
        action_layout.addWidget(self.btnHistory)
        action_layout.addWidget(self.btnLoadActivation)  # <-- Добавили кнопку активации
        right_layout.addLayout(action_layout)


        # Кнопки управления файлами и добавления в БД
        control_layout = QHBoxLayout()
        self.btnLoad = QPushButton("Загрузить файл для проверки")
        self.btnLoad.clicked.connect(self.run_load_and_process_file)
        self.btnSaveReports = QPushButton("Сохранить отчеты")
        self.btnSaveReports.clicked.connect(self.save_generated_reports)
        self.btnSaveReports.setEnabled(False)
        self.btnAddTD = QPushButton("Добавить 'На проверку' в БД")
        self.btnAddTD.clicked.connect(self.run_add_to_temporary_db)
        self.btnAddTD.setEnabled(False)
        control_layout.addWidget(self.btnLoad)
        control_layout.addWidget(self.btnSaveReports)
        control_layout.addWidget(self.btnAddTD)
        left_layout.addLayout(control_layout)

        # Разделитель или отступ
        right_layout.addSpacing(10)

        # Логгер
        right_layout.addWidget(QLabel("Лог операций:"))
        self.logText = QTextEdit()
        self.logText.setReadOnly(True)
        left_layout.addWidget(self.logText)

        splitter.addWidget(right_panel)

        # Таблица данных
        self.dataTable = QTableWidget()
        self.dataTable.setColumnCount(13) # Увеличено для статуса и ID
        self.dataTable.setHorizontalHeaderLabels([
            'ID', 'Фамилия', 'Имя', 'Отчество', 'Дата рождения', 
            'Организация', 'Должность', 'Статус БД', 'Статус Проверки',
            'Ошибки Валидации', 'Прим.',
            'Начало аккр.', 'Конец аккр.'
        ])
        self.dataTable.setEditTriggers(QAbstractItemView.NoEditTriggers) # Запрет редактирования
        self.dataTable.setSelectionBehavior(QAbstractItemView.SelectRows) # Выделение строк
        self.dataTable.setSelectionMode(QAbstractItemView.SingleSelection) # Только одна строка
        self.dataTable.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch) # Растягивание колонок
        self.dataTable.itemSelectionChanged.connect(self.on_table_selection_changed) # Загрузка примечаний при выборе
        self.dataTable.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents) # ID
        self.dataTable.horizontalHeader().setSectionResizeMode(10, QHeaderView.ResizeToContents)  # Прим.
        self.dataTable.horizontalHeader().setSectionResizeMode(11, QHeaderView.ResizeToContents)  # Начало аккр.
        self.dataTable.horizontalHeader().setSectionResizeMode(12, QHeaderView.ResizeToContents)  # Конец аккр.
        right_layout.addWidget(self.dataTable)

        # Примечания (ЕДИНОЕ ПОЛЕ)
        notes_layout = QVBoxLayout()
        notes_layout.addWidget(QLabel("Примечания:"))
        self.notesEdit = QTextEdit()
        self.notesEdit.setPlaceholderText(
            "Примечания к выбранной записи ИЛИ введите сюда примечания ПЕРЕД добавлением в БД...")

        # --- Кнопки для примечаний ---
        notes_buttons_layout = QHBoxLayout()
        self.btnSaveNotes = QPushButton("Сохранить (выбранному)")  # Уточнили название
        self.btnSaveNotes.setEnabled(False)
        self.btnSaveNotes.clicked.connect(self.run_save_notes_for_selected)  # Новый отдельный слот

        self.btnSaveNotesForAll = QPushButton("Сохранить для всех (в таблице)")  # <-- Новая кнопка
        self.btnSaveNotesForAll.clicked.connect(self.run_save_notes_for_all_visible)  # <-- Новый слот

        notes_buttons_layout.addWidget(self.btnSaveNotes)
        notes_buttons_layout.addWidget(self.btnSaveNotesForAll)
        # --- Конец кнопок ---
        notes_layout.addWidget(self.notesEdit)
        notes_layout.addLayout(notes_buttons_layout)  # Добавляем кнопки под полем
        left_layout.addLayout(notes_layout)

        splitter.addWidget(left_panel)  # Добавляем левую панель (таблица)
        # Установка начальных размеров панелей (например, 70% таблица, 30% лог)
        total_width = self.width() if self.width() > 0 else 1200  # Базовая ширина
        splitter.setSizes([int(total_width * 0.7), int(total_width * 0.3)])

        main_layout.addWidget(splitter)
        self.setLayout(main_layout)

        self.resize(1200, 800) # Установка размера окна

    # --- Слоты для обработки результатов фоновых задач ---

    def update_column_visibility(self, df_for_check):
        """Скрывает или показывает столбцы в зависимости от их содержимого."""
        if df_for_check is None or df_for_check.empty:
            # Если данных нет, можно скрыть все опциональные или показать все
            self.dataTable.setColumnHidden(self.COL_STATUS_PROV, True)
            self.dataTable.setColumnHidden(self.COL_ERR_VALID, True)
            self.dataTable.setColumnHidden(self.COL_PRIM, True)
            self.dataTable.setColumnHidden(self.COL_START_AKKR, True)
            self.dataTable.setColumnHidden(self.COL_END_AKKR, True)
            return

        # Проверяем, есть ли непустые значения (кроме пустых строк и None/NaN)
        # Преобразуем в строки и удаляем пробелы для проверки на "пустоту"
        has_status_prov = not df_for_check['Статус Проверки'].astype(str).str.strip().replace('', pd.NA).isna().all()
        has_err_valid = not df_for_check['Ошибки Валидации'].astype(str).str.strip().replace('', pd.NA).isna().all()
        has_prim = not df_for_check['Прим.'].astype(str).str.strip().replace('', pd.NA).isna().all()
        has_start_akkr = not df_for_check['Начало аккр.'].astype(str).str.strip().replace('', pd.NA).isna().all()
        has_end_akkr = not df_for_check['Конец аккр.'].astype(str).str.strip().replace('', pd.NA).isna().all()

        self.dataTable.setColumnHidden(self.COL_STATUS_PROV, not has_status_prov)
        self.dataTable.setColumnHidden(self.COL_ERR_VALID, not has_err_valid)
        self.dataTable.setColumnHidden(self.COL_PRIM, not has_prim)
        self.dataTable.setColumnHidden(self.COL_START_AKKR, not has_start_akkr)
        self.dataTable.setColumnHidden(self.COL_END_AKKR, not has_end_akkr)

        # Статус БД всегда показываем, он ключевой
        self.dataTable.setColumnHidden(self.COL_STATUS_DB, False)
    def on_task_finished(self, task_name):
        """Обработчик завершения фоновой задачи."""
        self.logMessage(f"Задача '{task_name}' завершена.", "INFO")
        # Здесь можно разблокировать кнопки или обновить статус

    def handle_load_and_process_result(self, result):
        """Обработка результата загрузки и проверки файла."""
        if isinstance(result, dict) and 'processed_df' in result:
            self.df_processed = result['processed_df']
            self.current_reports = result.get('reports', {})
            self.df_to_add_td = result.get('to_td', pd.DataFrame())

            self.logMessage(f"Файл обработан. Найдено {len(self.df_processed)} записей.", "INFO")
            self.update_table_signal.emit(self.df_processed) # Обновляем таблицу в основном потоке

            # Активируем кнопки сохранения отчетов и добавления в TD, если есть данные
            self.btnSaveReports.setEnabled(any(not df.empty for df in self.current_reports.values()))
            self.btnAddTD.setEnabled(not self.df_to_add_td.empty)
            self.task_finished_signal.emit("Загрузка и обработка файла")
        elif isinstance(result, str) and "Отменено" in result:
             self.logMessage(result, "INFO") # Логируем отмену
        else:
             self.logMessage("Обработка файла завершилась с неопределенным результатом или ошибкой.", "ERROR")

    def handle_search_result(self, result_df):
        """Обработка результата поиска."""
        if result_df is not None:
            self.logMessage(f"Поиск завершен. Найдено {len(result_df)} записей.", "INFO")
            # Добавляем пустые колонки для совместимости с displayTable
            result_df['Статус Проверки'] = ''
            result_df['Ошибки Валидации'] = ''
            self.update_table_signal.emit(result_df)
        else:
            self.logMessage("Ошибка во время поиска или ничего не найдено.", "WARNING")
            self.dataTable.setRowCount(0) # Очищаем таблицу
        self.task_finished_signal.emit("Поиск сотрудников")

    def handle_add_td_result(self, success_count):
        """Обработка результата добавления в TD."""
        if success_count is not None:
            self.logMessage(f"Успешно добавлено {success_count} записей в временную таблицу.", "INFO")
            # Очищаем список для добавления и деактивируем кнопку
            self.df_to_add_td = pd.DataFrame()
            self.btnAddTD.setEnabled(False)
            # НЕ ОЧИЩАЕМ поле notesEdit, пользователь сам решит
            # self.notesEdit.clear()
        else:
            self.logMessage("Ошибка при добавлении записей в временную таблицу.", "ERROR")
        self.task_finished_signal.emit("Добавление в БД")

    def handle_manage_blacklist_result(self, result):
        """Обработка результата изменения статуса ЧС."""
        if isinstance(result, dict):
             message = result.get('message')
             row_index = result.get('row_index')
             new_status = result.get('new_status')
             self.logMessage(message, "INFO")
             # Обновляем статус в таблице UI
             if row_index is not None and new_status is not None:
                 status_item = QTableWidgetItem(new_status)
                 status_item.setForeground(QBrush(QColor("orange" if new_status == "В черном списке" else "lightblue")))
                 self.dataTable.setItem(row_index, 7, status_item) # Колонка 'Статус БД'
                 # Обновляем статус проверки тоже
                 check_status_item = QTableWidgetItem(new_status)
                 self.dataTable.setItem(row_index, 8, check_status_item) # Колонка 'Статус Проверки'
        elif isinstance(result, str): # Сообщение об ошибке или отмене
            self.logMessage(result, "WARNING")
        self.task_finished_signal.emit("Управление черным списком")

    def handle_save_notes_result(self, result):
        """Обработка результата сохранения ОДНОГО примечания."""
        success = result.get('success')
        message = result.get('message', 'Неизвестный результат.')
        person_id = result.get('person_id')

        if success:
             self.logMessage(f"Примечания для ID {person_id} сохранены.", "INFO")
             # Обновляем индикатор в таблице для этой строки
             self.update_note_indicator_in_table(person_id, True)
        else:
             self.logMessage(f"Ошибка сохранения примечаний для ID {person_id}: {message}", "ERROR")
             QMessageBox.warning(self, "Ошибка", f"Не удалось сохранить примечания:\n{message}")
        self.task_finished_signal.emit("Сохранение примечаний (1)")

    def update_note_indicator_in_table(self, person_id, has_notes):
        """Обновляет индикатор 'Прим.' для строки с заданным ID."""
        note_indicator = '✓' if has_notes else ''
        for row in range(self.dataTable.rowCount()):
            id_item = self.dataTable.item(row, 0)  # ID column
            note_item = self.dataTable.item(row, 10)  # 'Прим.' column
            if id_item and id_item.text().isdigit() and int(id_item.text()) == person_id:
                if note_item:
                    note_item.setText(note_indicator)
                else:
                    # Если ячейки нет, создаем
                    new_item = QTableWidgetItem(note_indicator)
                    new_item.setTextAlignment(Qt.AlignCenter)
                    self.dataTable.setItem(row, 10, new_item)
                break  # Нашли строку, выходим

    def refresh_current_view(self):
        """Обновляет текущее представление таблицы (например, повтор поиска)."""
        # Простейший вариант - повторить последний поиск
        search_text = self.searchEdit.text()
        if search_text:
            self.logMessage("Обновление результатов поиска...", "DEBUG")
            self.run_search_people()
        else:
            # Если поиска не было, можно очистить таблицу или загрузить всё (не рекомендуется)
            self.dataTable.setRowCount(0)

    @pyqtSlot(dict, int)
    def handle_new_employee_action_request(self, data_dict, index):
        """Запрашивает у пользователя действие для нового сотрудника."""
        fio = f"{data_dict.get('Фамилия', '')} {data_dict.get('Имя', '')} {data_dict.get('Отчество', '')}".strip()
        items = ("Добавить как 'Активный'", "Добавить в TD 'На проверку'", "Пропустить")
        item, ok = QInputDialog.getItem(self, "Новый сотрудник",
                                        f"Сотрудник {fio} не найден в базе.\nВыберите действие:",
                                        items, 0, False)
        action = None
        if ok and item:
            if item == items[0]:  # Добавить как 'Активный'
                action = 'activate'
            elif item == items[1]:  # Добавить в TD 'На проверку'
                action = 'add_to_td'
            else:  # Пропустить
                action = 'skip'
        else:
            action = 'skip'  # Считаем пропуском, если диалог отменен

        self.new_employee_actions[index] = action  # Сохраняем выбор пользователя
        self.logMessage(f"Действие для нового сотрудника {fio} (индекс {index}): {action}", "INFO")

        # --- Слот для сохранения примечания ВЫБРАННОМУ ---

    def run_save_notes_for_selected(self):
        selected_row_index = self.get_selected_row_index()
        notes_text = self.notesEdit.toPlainText().strip()  # Получаем актуальный текст

        if selected_row_index is not None:
            person_id = None
            person_id_item = self.dataTable.item(selected_row_index, 0)
            if person_id_item and person_id_item.text().isdigit():
                person_id = int(person_id_item.text())

            if person_id is None:
                QMessageBox.warning(self, "Ошибка",
                                    "Не найден ID сотрудника в выбранной строке для сохранения примечания.")
                return
            if not notes_text:  # Проверяем, есть ли что сохранять
                # Можно спросить "Сохранить пустое примечание?" или просто не делать ничего
                # self.logMessage(f"Поле примечаний пусто для ID {person_id}. Сохранение отменено.", "INFO")
                # return
                pass  # Разрешаем сохранить пустое примечание

            self.logMessage(f"Запуск сохранения примечания для ID: {person_id}", "DEBUG")
            self.run_task_in_background(self._task_save_notes_for_one, self.handle_save_notes_result, person_id,
                                        notes_text)
        else:
            QMessageBox.information(self, "Информация",
                                    "Сначала выберите сотрудника в таблице, чтобы сохранить для него примечание.")

        # --- Слот для кнопки "Сохранить для всех" ---

    def run_save_notes_for_all_visible(self):
        notes_text = self.notesEdit.toPlainText().strip()
        if not notes_text:
            QMessageBox.information(self, "Информация", "Поле примечаний пусто. Нечего сохранять для всех.")
            return

        visible_ids = []
        for row in range(self.dataTable.rowCount()):
            id_item = self.dataTable.item(row, 0)
            if id_item and id_item.text().isdigit():
                visible_ids.append(int(id_item.text()))

        if not visible_ids:
            QMessageBox.warning(self, "Нет данных",
                                "В таблице нет сотрудников с ID из основной базы, для которых можно было бы сохранить примечание.")
            return

        reply = QMessageBox.question(self, 'Подтверждение',
                                     f"Сохранить введенное примечание для ВСЕХ {len(visible_ids)} видимых сотрудников в таблице (у кого есть ID)?",
                                     QMessageBox.Yes | QMessageBox.Cancel, QMessageBox.Cancel)
        if reply == QMessageBox.Yes:
            self.logMessage(f"Запуск массового сохранения примечания для {len(visible_ids)} ID...", "DEBUG")
            self.run_task_in_background(self._task_save_notes_for_many, self.handle_save_notes_mass_result, visible_ids,
                                        notes_text)
        else:
            self.logMessage("Массовое сохранение примечаний отменено.", "INFO")

    # --- Методы, выполняемые в фоновых потоках ---

    def _task_load_and_process_file(self, signals):
        """Worker: Загружает, параллельно обрабатывает и проверяет файл."""
        signals.log.emit("Запрос выбора файла...", "INFO")
        file_name = self.file_manager.open_file_dialog()
        if not file_name:
            return "Загрузка файла отменена пользователем."

        try:
            # --- Шаг 1: Загрузка и предварительная очистка пустых строк ---
            signals.log.emit(f"Загрузка данных из {os.path.basename(file_name)}...", "INFO")
            self.df_loaded = pd.read_excel(file_name, engine='openpyxl')

            initial_rows = len(self.df_loaded)
            cols_to_check = self.df_loaded.columns.tolist()
            if cols_to_check and ('№ п/п' in cols_to_check[0] or 'пп' in cols_to_check[0].lower()):
                cols_to_check = cols_to_check[1:]
            if cols_to_check:
                self.df_loaded.dropna(subset=cols_to_check, how='all', inplace=True)  # <--- Эта строка удаляет пустые
                removed_count = initial_rows - len(self.df_loaded)
                if removed_count > 0:
                    signals.log.emit(f"Удалено {removed_count} пустых строк.", "INFO")
            # --- КОНЕЦ УДАЛЕНИЯ ПУСТЫХ СТРОК ---

            if self.df_loaded.empty:
                signals.log.emit("Файл пуст после удаления пустых строк.", "WARNING")
                return {'status': 'error', 'message': "Файл не содержит данных."}

        except FileNotFoundError:
            signals.log.emit(f"Файл не найден: {file_name}", "ERROR")
            return None
        except ImportError:
             signals.log.emit("Движок 'openpyxl' не установлен. Установите: pip install openpyxl", "ERROR")
             return None
        except Exception as e:
            signals.log.emit(f"Ошибка чтения Excel файла: {e}", "ERROR")
            return None

        signals.progress.emit(10)

        try:
            # --- Шаг 2: Многопроцессорная обработка (очистка и валидация) ---
            signals.log.emit("Начало многопроцессорной обработки данных...", "INFO")
            num_processes = max(1, cpu_count() - 1)
            df_chunks = np.array_split(self.df_loaded, num_processes)

            with Pool(processes=num_processes) as pool:
                processed_chunks = pool.map(process_data_chunk, df_chunks)

            df_validated = pd.concat(processed_chunks, ignore_index=True)
            signals.log.emit("Многопроцессорная обработка завершена.", "INFO")
            signals.progress.emit(60)
        except Exception as e:
            self.logger.exception("Ошибка во время многопроцессорной обработки.")
            return f"Ошибка обработки: {e}"

        suspicious_indices = df_validated[df_validated['Name_Check_Required'] == True].index.tolist()
        confirmed_indices = set(df_validated.index)
        self.user_confirmations.clear()

        if suspicious_indices:
            signals.log.emit(f"Обнаружено {len(suspicious_indices)} строк с подозрительными данными. Требуется подтверждение.", "WARNING")
            for idx in suspicious_indices:
                 row_data = df_validated.loc[idx, ['Фамилия', 'Имя', 'Отчество']].to_string(header=False)
                 message = f"Обнаружены потенциально некорректные данные в строке {idx+1}:\n{row_data}\n\nПроверьте данные и подтвердите их корректность."
                 # Используем сигнал для запроса подтверждения в основном потоке
                 signals.request_confirmation.emit(message, idx)
                 # Здесь поток будет ждать ответа (это не идеально, но проще чем сложная машина состояний)
                 # Правильнее было бы разбить задачу, но для простоты пока так.
                 # Ждем, пока основной поток обработает сигнал и запишет результат в self.user_confirmations
                 while idx not in self.user_confirmations:
                      QThread.msleep(100) # Небольшая пауза

                 if not self.user_confirmations.get(idx, False):
                     signals.log.emit(f"Строка {idx+1} не подтверждена пользователем и будет пропущена.", "WARNING")
                     confirmed_indices.discard(idx) # Удаляем неподтвержденные
                     # Добавляем ошибку валидации
                     df_validated.loc[idx, 'Validation_Errors'] = (df_validated.loc[idx, 'Validation_Errors'] or "") + "ФИО не подтвержден"

        validation_errors_mask = df_validated['Validation_Errors'].notna()
        df_to_process = df_validated[~validation_errors_mask].copy()
        df_invalid = df_validated[validation_errors_mask].copy()

        signals.log.emit(f"Проверку прошли {len(df_to_process)} строк. Отклонено/не подтверждено: {len(df_invalid)}.",
                         "INFO")

        signals.progress.emit(60)

        # --- Шаг 5: Проверка статуса в БД (только для прошедших валидацию) ---
        if not df_to_process.empty:
            signals.log.emit("Проверка статусов сотрудников в БД...", "INFO")
            statuses_db, person_ids = [], []
            for _, row in df_to_process.iterrows():
                # Данные передаются в get_person_status в том же виде, в каком они были обработаны.
                # Это должно решить проблему с неправильным определением статуса.
                status_info = self.db_manager.get_person_status(
                    row.get('Фамилия'), row.get('Имя'), row.get('Отчество'), row.get('Дата рождения')
                )
                statuses_db.append(status_info['status'])
                person_ids.append(status_info['person_id'])
            df_to_process['Статус БД'] = statuses_db
            df_to_process['ID'] = person_ids

        signals.progress.emit(90)

        # --- Шаг 6: Финальное распределение по статусам и отчетам ---
        df_to_process['Статус Проверки'] = ''
        df_for_td_list = []

        # Инициализируем словарь reports со ВСЕМИ возможными ключами
        reports = {
            'ГПХ_На_проверку': pd.DataFrame(),
            'Подрядчики_На_проверку': pd.DataFrame(),
            'ГПХ_Ранее_проверенные': pd.DataFrame(),
            'Подрядчики_Ранее_проверенные': pd.DataFrame(),
            'ГПХ_Ранее_отведенные': pd.DataFrame(),
            'Подрядчики_Ранее_отведенные': pd.DataFrame(),
            'Ошибки_ГПХ': df_invalid[df_invalid['Организация'].str.contains('ГПХ', case=False, na=False)],
            'Ошибки_Подрядчики': df_invalid[~df_invalid['Организация'].str.contains('ГПХ', case=False, na=False)]
        }

        for index, row in df_to_process.iterrows():
            org = str(row.get('Организация', '')).upper()
            status_db = row['Статус БД']
            is_gph = 'ГПХ' in org

            if status_db == 'BLACKLISTED':
                status_check = "Ранее отведен"
                report_key = 'ГПХ_Ранее_отведенные' if is_gph else 'Подрядчики_Ранее_отведенные'
            elif status_db == 'ACTIVE':
                status_check = "Активен"
                report_key = 'ГПХ_Ранее_проверенные' if is_gph else 'Подрядчики_Ранее_проверенные'
            else:  # NOT_FOUND, EXPIRED, и т.д.
                status_check = "На проверку"
                report_key = 'ГПХ_На_проверку' if is_gph else 'Подрядчики_На_проверку'
                df_for_td_list.append(row.to_dict())

            df_to_process.loc[index, 'Статус Проверки'] = status_check
            reports[report_key] = pd.concat([reports[report_key], row.to_frame().T], ignore_index=True)

        df_for_td = pd.DataFrame(df_for_td_list)

        # --- Шаг 7: Подготовка к возврату результата ---
        df_display = pd.concat([df_to_process, df_invalid], ignore_index=True).fillna('')
        if 'Validation_Errors' in df_display.columns:
            df_display.rename(columns={'Validation_Errors': 'Ошибки Валидации'}, inplace=True)
        # Убедимся, что все нужные колонки есть
        all_cols = [
            'ID', 'Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Место рождения',
            'Организация', 'Должность', 'Статус БД', 'Статус Проверки',
            'Ошибки Валидации', 'Прим.',
            'Начало аккр.', 'Конец аккр.'
        ]
        for col in all_cols:
             if col not in df_display.columns:
                 df_display[col] = ''
        df_display = df_display[all_cols]
        result_dict = {
            'processed_df': df_display,
            'to_td': df_for_td,
            'reports': reports,
            'stats': {
                'passed': len(df_for_td),
                'rejected': len(df_invalid),
                'suspicious': len(suspicious_indices)
            }
        }

        signals.progress.emit(100)
        signals.log.emit("Обработка файла завершена.", "INFO")
        return result_dict

    # ui.py (полная функция _task_search_people)

    def _task_search_people(self, signals):
        """Worker: Выполняет поиск в БД."""
        search_term = self.searchEdit.text().strip()
        if not search_term:
            signals.log.emit("Поисковый запрос пуст.", "WARNING")
            # Возвращаем пустой DataFrame, чтобы очистить таблицу
            return pd.DataFrame(
                columns=[
            'ID', 'Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Место рождения',
            'Организация', 'Должность', 'Статус БД', 'Статус Проверки',
            'Ошибки Валидации', 'Прим.',
            'Начало аккр.', 'Конец аккр.'
        ])

        signals.log.emit(f"Выполнение поиска по запросу: '{search_term}'...", "INFO")
        try:
            results = self.db_manager.search_people(search_term)  # Вызываем обновленный метод

            if results is None:
                signals.log.emit(f"Ошибка при поиске (БД вернула None) по запросу: '{search_term}'.", "ERROR")
                return None
            if not results:
                signals.log.emit(f"По запросу '{search_term}' ничего не найдено.", "INFO")
                # Возвращаем пустой DataFrame с НОВЫМ набором колонок
                return pd.DataFrame(
                    columns=[
                            'ID', 'Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Место рождения',
                            'Организация', 'Должность', 'Статус БД', 'Статус Проверки',
                            'Ошибки Валидации', 'Прим.',
                            'Начало аккр.', 'Конец аккр.'
                        ])

            df_results = pd.DataFrame(results)
            if df_results.empty:
                signals.log.emit(f"По запросу '{search_term}' ничего не найдено (после DataFrame).", "INFO")
                return pd.DataFrame(
                    columns=[
                            'ID', 'Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Место рождения',
                            'Организация', 'Должность', 'Статус БД', 'Статус Проверки',
                            'Ошибки Валидации', 'Прим.',
                            'Начало аккр.', 'Конец аккр.'
                        ])

            # --- Определение Статус БД ---
            now_tz = datetime.now(self.timezone)

            def map_status(row):
                source = row.get('source')
                if source == 'AccrTable':
                    is_blacklisted = row.get('black_list', False)
                    end_accr_val = row.get('end_accr')
                    status_val = row.get('accr_status')  # Используем accr_status

                    if status_val == 'в ожидании': return "В ожидании (Accr)"

                    if is_blacklisted: return "В черном списке"
                    end_accr_aware = pd.to_datetime(end_accr_val).tz_convert(self.timezone) if pd.notna(
                        end_accr_val) else None
                    if end_accr_aware and end_accr_aware > now_tz: return "Аккредитован"
                    if pd.notna(status_val) and status_val:

                        if status_val == 'отведен': return "Отведен (статус)"
                        if status_val == 'истек срок': return "Истек срок"
                        return str(status_val)
                    if end_accr_aware and end_accr_aware <= now_tz: return "Истек срок (расчет)"
                    return 'Неизвестно (Accr)'
                elif source == 'TD':
                    # Для записей из TD просто возвращаем их статус
                    td_status = row.get('td_status', 'статус неизв.')
                    return f"В TD ({td_status})"  # Показываем, что запись временная
                else:
                    return 'Источник неизв.'

            df_results['Статус БД'] = df_results.apply(map_status, axis=1)

            # --- Выбор и переименование колонок ---
            display_cols_map = {
                'id': 'ID',  # Для AccrTable 'id', для TD будет None или temp_id
                'surname': 'Фамилия',
                'name': 'Имя',
                'middle_name': 'Отчество',
                'birth_date': 'Дата рождения',
                'organization': 'Организация',
                'position': 'Должность',
                'Статус БД': 'Статус БД',
                'has_notes': 'Прим.',     # <-- Карта для новой колонки
                'start_accr': 'Начало аккр.',  # <-- Новая карта
                'end_accr': 'Конец аккр.'
            }
            # Используем id из AccrTable, если источник TD, ID будет None
            df_results['id'] = df_results.apply(lambda row: row['id'] if row['source'] == 'AccrTable' else None, axis=1)

            cols_to_select = [db_col for db_col in display_cols_map.keys() if db_col in df_results.columns]
            if not cols_to_select:
                signals.log.emit("В результатах поиска нет ни одной из ожидаемых колонок.", "ERROR")
                return pd.DataFrame(columns=display_cols_map.values())

            df_display = df_results[cols_to_select].copy()
            df_display.rename(columns=display_cols_map, inplace=True)

            # --- Форматирование дат ---
            date_format = '%d.%m.%Y'
            datetime_format = '%d.%m.%Y %H:%M'
            local_tz = self.timezone # Используем таймзону из __init__

            def format_birth_date(dt):
                if pd.notna(dt):
                    try:
                        return pd.to_datetime(dt).strftime(date_format)
                    except:
                        return str(dt)
                return ''

            def format_timestamp_date(dt_val, signals_obj, col_name_log): # Общая функция форматирования
                if pd.notna(dt_val):
                    try:
                        dt_aware = pd.to_datetime(dt_val)
                        if dt_aware.tzinfo is None or dt_aware.tzinfo.utcoffset(dt_aware) is None:
                            dt_aware = dt_aware.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
                            if pd.isna(dt_aware): return str(dt_val)
                        # Для 'Начало аккр.' и 'Конец аккр.' нужен только формат даты
                        return dt_aware.tz_convert(local_tz).strftime(date_format)
                    except Exception as e:
                        signals_obj.log.emit(f"Ошибка форматирования '{col_name_log}': {e} для {dt_val}", "ERROR")
                        return str(dt_val)
                return ''

            if 'Дата рождения' in df_display.columns:
                 df_display['Дата рождения'] = df_display['Дата рождения'].apply(format_birth_date)
            if 'Начало аккр.' in df_display.columns:
                 df_display['Начало аккр.'] = df_display['Начало аккр.'].apply(lambda x: format_timestamp_date(x, signals, 'Начало аккр.'))
                 # print(df_display['Начало аккр.'])
            if 'Конец аккр.' in df_display.columns:
                 df_display['Конец аккр.'] = df_display['Конец аккр.'].apply(lambda x: format_timestamp_date(x, signals, 'Конец аккр.'))


            # Добавляем недостающие колонки (Статус Проверки, Ошибки Валидации - они не приходят из поиска)
            if 'Статус Проверки' not in df_display.columns: df_display['Статус Проверки'] = ''
            if 'Ошибки Валидации' not in df_display.columns: df_display['Ошибки Валидации'] = ''
            if 'Прим.' not in df_display.columns: df_display['Прим.'] = False  # По умолчанию False, если не пришло

            # Преобразуем bool в строку для колонки "Прим."
            df_display['Прим.'] = df_display['Прим.'].apply(lambda x: '✓' if x else '')

            # Упорядочиваем колонки
            final_order = ['ID', 'Фамилия', 'Имя', 'Отчество', 'Дата рождения',
                           'Организация', 'Должность', 'Статус БД', 'Статус Проверки',
                           'Ошибки Валидации', 'Прим.', 'Начало аккр.', 'Конец аккр.']
            # Убедимся, что все колонки из final_order есть в df_display
            for col in final_order:
                if col not in df_display.columns:
                    df_display[col] = ''  # Добавляем пустую, если вдруг отсутствует

            df_display = df_display[final_order]  # Применяем порядок

            signals.log.emit(f"Поиск завершен. Найдено {len(df_display)} записей.", "INFO")
            return df_display

        except Exception as e:
            # Логируем любую другую ошибку во время поиска/обработки
            signals.log.emit(f"Ошибка при запуске поиска: '{e}'.", "ERROR")
            self.logger.exception("Полная трассировка ошибки поиска:")  # Логируем traceback
            return None  # Возвращаем None при ошибке


    def _task_add_to_temporary_db(self, notes_to_add, signals):
        """Worker: Добавляет отфильтрованные данные в временную таблицу TD."""
        # ... (код метода как в прошлой версии, использует notes_to_add) ...
        # Проверим, что ключ "Примечания" используется правильно
        if self.df_to_add_td.empty:
            signals.log.emit("Нет данных для добавления в временную таблицу.", "WARNING")
            return 0

        signals.log.emit(f"Добавление {len(self.df_to_add_td)} записей в TD (с примечанием: '{notes_to_add[:50]}...')...", "INFO")
        added_count = 0
        total_count = len(self.df_to_add_td)
        for index, row_data in self.df_to_add_td.iterrows():
             data_dict = row_data.to_dict()
             data_dict['Примечания'] = notes_to_add # Добавляем примечание

             # Передаем data_dict в add_to_td, он сам извлечет нужные поля
             result = self.db_manager.add_to_td(data_dict)
             if result:
                 added_count += 1

        if added_count == total_count:
            signals.log.emit(f"Успешно добавлено {added_count} записей в TD.", "INFO")
        else:
             signals.log.emit(f"Добавлено {added_count} из {total_count} записей в TD. Проверьте лог на наличие ошибок.", "WARNING")
        return added_count


    def run_add_to_temporary_db(self):
        # Читаем примечания из ЕДИНОГО поля notesEdit
        notes = self.notesEdit.toPlainText().strip()
        self.logMessage(f"Запуск добавления в БД с примечаниями: '{notes[:50]}...'", "DEBUG")
        # Передаем примечания как аргумент в задачу
        self.run_task_in_background(self._task_add_to_temporary_db, self.handle_add_td_result, notes)


    def _task_manage_blacklist(self, signals):
        selected_row_index = self.get_selected_row_index()
        if selected_row_index is None:
            return "Не выбрана строка для управления черным списком."

        # Собираем все доступные данные из строки таблицы
        person_data = {}
        headers = [self.dataTable.horizontalHeaderItem(i).text() for i in range(self.dataTable.columnCount())]
        for col_idx, header in enumerate(headers):
            item = self.dataTable.item(selected_row_index, col_idx)
            if item:
                person_data[header] = item.text() # Сохраняем как текст
            else:
                person_data[header] = None

        # Преобразуем дату рождения в объект date, если она есть
        if 'Дата рождения' in person_data and person_data['Дата рождения']:
             try:
                 # data_processing.normalizeDate может быть полезнее, если формат сложный
                 # но для простоты здесь предполагается формат, который pd.to_datetime поймет
                 date_obj = pd.to_datetime(person_data['Дата рождения'], dayfirst=True, errors='coerce').date()
                 if pd.notna(date_obj):
                      person_data['Дата рождения'] = date_obj
                 else:
                      signals.log.emit(f"Некорректная дата рождения в строке: {person_data['Дата рождения']}", "ERROR")
                      return f"Некорректная дата рождения в строке: {person_data['Дата рождения']}"
             except Exception:
                  signals.log.emit(f"Ошибка парсинга даты рождения: {person_data['Дата рождения']}", "ERROR")
                  return f"Ошибка парсинга даты рождения: {person_data['Дата рождения']}"
        elif not person_data.get('Дата рождения'): # Если даты рождения нет, это проблема
            signals.log.emit("Отсутствует дата рождения в выбранной строке.", "ERROR")
            return "Отсутствует дата рождения в выбранной строке."


        fio = f"{person_data.get('Фамилия', '')} {person_data.get('Имя', '')}".strip()
        signals.log.emit(f"Запрос на изменение статуса ЧС для: {fio} (данные строки: {person_data})...", "INFO")

        action_taken, message = self.db_manager.toggle_blacklist(person_data)

        if action_taken:
            signals.log.emit(f"Для {fio}: {message}", "INFO")
            # Определяем новый отображаемый статус в UI
            new_status_display = "В черном списке"
            if "убран" in action_taken:
                 new_status_display = "На проверку (TD)" if "TD" in action_taken else "Снят с ЧС"
            return {'message': message, 'row_index': selected_row_index, 'new_status': new_status_display, 'action': action_taken}
        else:
             signals.log.emit(f"Ошибка при изменении статуса ЧС для {fio}: {message}", "ERROR")
             return message # Возвращаем строку с ошибкой

    def _task_save_notes(self, signals):
        """Worker: Сохраняет примечания для выбранного сотрудника."""
        # Получаем ID НЕ из сигнала, а из ТЕКУЩЕГО ВЫБОРА В ТАБЛИЦЕ
        # Это безопаснее, если пользователь кликнул куда-то еще во время сохранения
        selected_row_index = self.get_selected_row_index() # Используем метод для получения индекса
        person_id = None
        if selected_row_index is not None:
             person_id_item = self.dataTable.item(selected_row_index, 0)
             if person_id_item and person_id_item.text().isdigit():
                  person_id = int(person_id_item.text())

        if person_id is None:
            # Логируем ошибку и возвращаем None или сообщение
            error_msg = "Не удалось определить ID сотрудника для сохранения примечаний (строка не выбрана или нет ID)."
            signals.log.emit(error_msg, "WARNING")
            return error_msg # Возвращаем строку с ошибкой

        # Получаем текст из notesEdit (это безопасно делать в worker'е, т.к. он читает свойство)
        notes = self.notesEdit.toPlainText()

        signals.log.emit(f"Сохранение примечаний для ID: {person_id}...", "INFO")
        success = self.db_manager.update_notes(person_id, notes)

        if success:
             return {'success': success, 'person_id': person_id}
        else:
             return f"Ошибка при сохранении примечаний для ID {person_id}."

    # --- НОВАЯ ФОНОВАЯ ЗАДАЧА для файла активации ---
    def _task_process_activation_file(self, signals):
        """Worker: Обрабатывает файл активации."""
        signals.log.emit("Запрос выбора файла активации...", "INFO")
        file_name = self.file_manager.open_file_dialog()
        if not file_name:
            return {'status': 'cancelled', 'message': "Выбор файла активации отменен."}

        signals.log.emit(f"Загрузка данных из файла активации: {os.path.basename(file_name)}...", "INFO")
        try:
            df_activation = pd.read_excel(file_name, engine='openpyxl')
            signals.log.emit(f"Загружено {len(df_activation)} строк из файла активации.", "INFO")
        except Exception as e:
            error_msg = f"Ошибка чтения Excel файла активации: {e}"
            signals.log.emit(error_msg, "ERROR")
            return {'status': 'error', 'message': error_msg}

        # 1. Очистка данных из файла активации
        signals.log.emit("Очистка данных из файла активации...", "INFO")
        df_cleaned = self.processor.clean_dataframe(df_activation)
        # Здесь можно добавить валидацию дат и обязательных полей для файла активации, если нужно

        activated_count = 0
        added_td_count = 0
        skipped_count = 0
        error_count = 0
        processed_count = 0
        total_rows = len(df_cleaned)

        # Сбрасываем предыдущие решения по новым сотрудникам
        self.new_employee_actions.clear()

        signals.log.emit("Начало обработки файла активации...", "INFO")
        for index, row in df_cleaned.iterrows():
            processed_count += 1
            signals.progress.emit(int(100 * processed_count / total_rows))

            # Извлекаем данные для поиска и возможного добавления
            data_dict = row.to_dict()  # Сохраняем всю строку на всякий случай
            surname = row.get('Фамилия')
            name = row.get('Имя')
            middle_name = row.get('Отчество')
            birth_date = row.get('Дата рождения')
            custom_date = row.get('Дата проверки')

            # Проверяем минимально необходимые данные
            if not surname or not name or not birth_date:
                signals.log.emit(f"Строка {index + 1}: Пропущена из-за отсутствия ФИО или Даты рождения.",
                                 "WARNING")
                skipped_count += 1
                continue

            # Пытаемся активировать существующего сотрудника 'в ожидании'
            success, message, person_id = self.db_manager.activate_person_by_details(
                surname, name, middle_name, birth_date, custom_date
            )
            signals.log.emit(f"Строка {index + 1}: {success} {message} ID: {person_id}",
                             "WARNING")


            if success and "успешно активирован" in message:

                activated_count += 1
                signals.log.emit(f"Строка {index + 1} ({surname} {name}): {message}", "INFO")
            elif success and "Активация не требуется" in message:
                # Статус уже не 'в ожидании', просто пропускаем
                signals.log.emit(f"Строка {index + 1} ({surname} {name}): {message}", "DEBUG")
                # skipped_count += 1 # Не считаем это пропуском в смысле ошибки
            elif not success and person_id is None:  # Сотрудник не найден
                signals.log.emit(f"Строка {index + 1}: {message} Запрос действия у пользователя...", "WARNING")
                # Запрашиваем действие у пользователя через сигнал
                # Передаем data_dict, чтобы можно было добавить сотрудника
                request_data = {
                    'Фамилия': surname, 'Имя': name, 'Отчество': middle_name,
                    'Дата рождения': birth_date,
                    'Место рождения': row.get('Место рождения'),  # Передаем доп. поля
                    'Регистрация': row.get('Адрес регистрации'),
                    'Организация': row.get('Организация', 'Не указана'),  # Нужна организация
                    'Должность': row.get('Должность', 'Не указана'),
                    'Примечания': row.get('Примечания', '')  # И примечания, если есть в файле
                }
                signals.request_new_employee_action.emit(request_data, index)
                # Ждем ответа пользователя
                while index not in self.new_employee_actions:
                    QThread.msleep(100)
                action = self.new_employee_actions.get(index, 'skip')

                if action == 'activate':
                    # Добавляем как активный
                    signals.log.emit(f"Строка {index + 1}: Пользователь выбрал 'Добавить как Активный'.", "INFO")
                    new_person_id = self.db_manager.add_to_accrtable(request_data, status='аккредитован')
                    if new_person_id:
                        # Сразу обновляем mainTable для нового активного

                        now_tz = datetime.now(self.timezone)
                        if custom_date is not None and not pd.isna(custom_date):
                            start_date = custom_date + timedelta(days=1)
                        else:
                            start_date = now_tz
                        end_accr = start_date + timedelta(days=180)
                        query_main = """
                            INSERT INTO mainTable (person_id, start_accr, end_accr, black_list, last_checked)
                            VALUES (%s, %s, %s, FALSE, %s);
                         """
                        self.db_manager.execute_query(query_main, (new_person_id, start_date, end_accr, now_tz),
                                                      commit=True)
                        self.db_manager.log_transaction(new_person_id, 'Добавлен и Активирован (файл)')
                        activated_count += 1
                    else:
                        signals.log.emit(f"Строка {index + 1}: Ошибка добавления нового активного сотрудника.",
                                         "ERROR")
                        error_count += 1
                elif action == 'add_to_td':
                    # Добавляем в TD
                    signals.log.emit(f"Строка {index + 1}: Пользователь выбрал 'Добавить в TD'.", "INFO")
                    td_id = self.db_manager.add_to_td(request_data)
                    if td_id:
                        added_td_count += 1
                    else:
                        signals.log.emit(f"Строка {index + 1}: Ошибка добавления нового сотрудника в TD.", "ERROR")
                        error_count += 1
                else:  # action == 'skip'
                    signals.log.emit(f"Строка {index + 1}: Сотрудник пропущен по выбору пользователя.", "INFO")
                    skipped_count += 1

            else:  # Ошибка активации для существующего сотрудника
                signals.log.emit(f"Строка {index + 1} ({surname} {name}): Ошибка активации - {message}", "ERROR")
                error_count += 1

        signals.progress.emit(100)
        summary_message = f"Обработка файла активации завершена. Успешно активировано: {activated_count}, Добавлено в TD: {added_td_count}, Пропущено: {skipped_count}, Ошибок: {error_count}."
        signals.log.emit(summary_message, "INFO")
        return {'status': 'completed', 'message': summary_message, 'activated': activated_count,
                'added_td': added_td_count, 'skipped': skipped_count, 'errors': error_count}

    # --- Методы для запуска фоновых задач ---

    def run_task_in_background(self, task_function, on_finished_slot, *args, **kwargs):
        """Универсальный метод для запуска задачи в фоновом потоке."""
        self.logMessage(f"Запуск задачи '{task_function.__name__}'...", "INFO")
        # Блокируем кнопки на время выполнения? (Опционально)
        # self.set_controls_enabled(False)

        worker = Worker(task_function, *args, **kwargs)
        worker.signals.finished.connect(on_finished_slot)
        worker.signals.error.connect(lambda e: self.logMessage(f"Критическая ошибка в задаче: {e}", "CRITICAL"))
        worker.signals.progress.connect(self.update_progress) # Подключаем прогресс
        worker.signals.log.connect(self.logMessage) # Подключаем логирование из потока
        worker.signals.request_confirmation.connect(self.handle_confirmation_request) # Подключаем запрос подтверждения
        worker.signals.request_new_employee_action.connect(self.handle_new_employee_action_request)

        # Добавляем задачу в пул потоков
        self.thread_pool.start(worker)

    def run_load_and_process_file(self):
        self.run_task_in_background(self._task_load_and_process_file, self.handle_load_and_process_result)

    def run_search_people(self):
         self.run_task_in_background(self._task_search_people, self.handle_search_result)

    def run_manage_blacklist(self):
         self.run_task_in_background(self._task_manage_blacklist, self.handle_manage_blacklist_result)

    def run_save_notes(self):
        selected_row_index = self.get_selected_row_index()
        notes_text = self.notesEdit.toPlainText().strip()

        if selected_row_index is not None:
            # --- Случай 1: Строка выбрана ---
            person_id = None
            person_id_item = self.dataTable.item(selected_row_index, 0)
            if person_id_item and person_id_item.text().isdigit():
                person_id = int(person_id_item.text())

            if person_id is None:
                QMessageBox.warning(self, "Ошибка", "Не найден ID сотрудника в выбранной строке для сохранения примечания.")
                return

            self.logMessage(f"Запуск сохранения примечания для ID: {person_id}", "DEBUG")
            # Запускаем задачу для одного ID
            self.run_task_in_background(self._task_save_notes_for_one, self.handle_save_notes_result, person_id, notes_text)

        else:
            # --- Случай 2: Строка НЕ выбрана ---
            if not notes_text:
                 QMessageBox.information(self, "Информация", "Поле примечаний пусто. Нечего сохранять.")
                 return

            # Получаем ID всех видимых строк с валидным ID AccrTable
            visible_ids = []
            for row in range(self.dataTable.rowCount()):
                 # Проверяем, видима ли строка (если используется фильтрация/скрытие - пока нет)
                 # if not self.dataTable.isRowHidden(row): # Раскомментировать, если будет фильтрация
                 id_item = self.dataTable.item(row, 0) # Колонка ID
                 if id_item and id_item.text().isdigit():
                     visible_ids.append(int(id_item.text()))

            if not visible_ids:
                 QMessageBox.warning(self, "Нет данных", "В таблице нет сотрудников с ID из основной базы данных, для которых можно было бы сохранить примечание.")
                 return

            # Запрашиваем подтверждение
            reply = QMessageBox.question(self, 'Подтверждение',
                                         f"Нет выбранной строки.\nСохранить введенное примечание для ВСЕХ {len(visible_ids)} видимых сотрудников в таблице (у кого есть ID)?",
                                         QMessageBox.Yes | QMessageBox.Cancel, QMessageBox.Cancel)

            if reply == QMessageBox.Yes:
                 self.logMessage(f"Запуск массового сохранения примечания для {len(visible_ids)} ID...", "DEBUG")
                 # Запускаем задачу для списка ID
                 self.run_task_in_background(self._task_save_notes_for_many, self.handle_save_notes_mass_result, visible_ids, notes_text)
            else:
                 self.logMessage("Массовое сохранение примечаний отменено.", "INFO")

    # --- Старая фоновая задача (теперь для одного) ---
    def _task_save_notes_for_one(self, person_id, notes, signals):
        """Worker: Сохраняет примечания для ОДНОГО выбранного сотрудника."""
        signals.log.emit(f"Сохранение примечаний для ID: {person_id}...", "INFO")
        success = self.db_manager.update_notes(person_id, notes)
        message = "Примечания успешно сохранены." if success else "Ошибка при сохранении примечаний."
        return {'success': success, 'person_id': person_id, 'message': message}

        # --- НОВАЯ фоновая задача (для многих) ---

    def _task_save_notes_for_many(self, person_ids, notes, signals):
        """Worker: Сохраняет ОДНО примечание для СПИСКА сотрудников."""
        if not person_ids:
            return {'success': False, 'count': 0, 'message': "Список ID пуст."}

        signals.log.emit(f"Массовое сохранение примечания '{notes[:30]}...' для {len(person_ids)} ID...", "INFO")
        success_count = 0
        total_count = len(person_ids)
        errors = []

        for i, person_id in enumerate(person_ids):
            try:
                success = self.db_manager.update_notes(person_id, notes)
                if success:
                    success_count += 1
                else:
                    errors.append(f"ID {person_id}: Не удалось сохранить")
            except Exception as e:
                errors.append(f"ID {person_id}: Ошибка - {e}")
            signals.progress.emit(int(100 * (i + 1) / total_count))

        signals.progress.emit(100)
        message = f"Массовое сохранение завершено. Успешно: {success_count}/{total_count}."
        if errors:
            message += f"\nОшибки:\n" + "\n".join(errors[:5])  # Показываем первые 5 ошибок
        return {'success': success_count == total_count, 'count': success_count, 'total': total_count,
                'message': message}

    def handle_save_notes_mass_result(self, result):
        """Обработка результата МАССОВОГО сохранения примечаний."""
        success = result.get('success')
        count = result.get('count', 0)
        total = result.get('total', 0)
        message = result.get('message', 'Неизвестный результат.')

        self.logMessage(message, "INFO" if success else "WARNING")
        if success:
            QMessageBox.information(self, "Успешно", message)
            # Обновляем индикаторы для всех обработанных строк?
            # Это может быть медленно, возможно, проще перезагрузить поиск
            self.refresh_current_view()  # Метод для обновления таблицы
        else:
            QMessageBox.warning(self, "Завершено с ошибками", message)
        self.task_finished_signal.emit("Сохранение примечаний (массовое)")

    # --- Вспомогательные методы GUI ---

    def get_selected_row_index(self):
        """Возвращает индекс выделенной строки или None."""
        selected_items = self.dataTable.selectedItems()
        if selected_items:
            return selected_items[0].row()
        return None

    def displayTable(self, df_display):
        """Отображает DataFrame в QTableWidget."""
         # Проверка, что вызывается из основного потока
        if QApplication.instance().thread() != QThread.currentThread():
            self.update_table_signal.emit(df_display) # Перенаправляем в основной поток
            return

        self.dataTable.setRowCount(0) # Очищаем таблицу перед заполнением
        if df_display is None or df_display.empty:
             self.logMessage("Нет данных для отображения в таблице.", "INFO")
             return

        self.logMessage(f"Отображение {len(df_display)} строк в таблице...", "DEBUG")
        self.dataTable.setRowCount(len(df_display))

        # Определение порядка колонок для отображения
        display_columns = ['ID', 'Фамилия', 'Имя', 'Отчество', 'Дата рождения',
                           'Организация', 'Должность', 'Статус БД', 'Статус Проверки',
                           'Ошибки Валидации', 'Прим.', 'Начало аккр.', 'Конец аккр.']

        status_colors = {
            "Ранее отведен": QColor("salmon"),
            "В черном списке": QColor("salmon"),
            "Активен": QColor("lightgreen"),
            "Аккредитован": QColor("lightgreen"),
            "На проверку": QColor("lightblue"),
            "В ожидании": QColor("lightblue"),
            "Истек срок": QColor("lightcoral"),
            "НЕ ПОДТВЕРЖДЕНО": QColor("yellow"),
            "Ошибка": QColor("orangered"),
        }

        for row_idx, (index, row_data) in enumerate(df_display.iterrows()):
            for col_idx, col_name in enumerate(display_columns):
                value = row_data.get(col_name)  # Используем get для безопасности
                if col_name in row_data:
                     value = row_data[col_name]
                     # --- Преобразование перед отображением ---
                     if pd.isna(value):
                         display_value = ""
                     elif col_name == 'ID' and not isinstance(value,
                                                              (int, float)):  # Если ID не число (напр. "N/A (TD)")
                         display_value = str(value)
                     elif col_name == 'ID':  # ID числовой
                         display_value = str(int(value))  # Преобразуем float ID (если вдруг) в int строку
                     elif col_name in ['Дата рождения', 'Начало аккр.', 'Конец аккр.']:
                         # Даты уже должны быть строками после _task_search_people
                         # Но на всякий случай проверим
                         if isinstance(value, (date, datetime)):
                             try:
                                 if col_name == 'Дата рождения' or col_name == 'Конец аккр.':
                                     display_value = value.strftime('%d.%m.%Y')
                                 else:  # Начало аккр.
                                     display_value = value.strftime('%d.%m.%Y %H:%M')
                             except:
                                 display_value = str(value)  # Если ошибка форматирования
                         else:
                             display_value = str(value)  # Если пришло не датой
                     else:
                         display_value = str(value)

                     item = QTableWidgetItem(display_value)

                     # Раскраска статусов и ошибок
                     cell_color = None
                     if col_name == 'Статус Проверки':
                         for status, color in status_colors.items():
                             if status.lower() in display_value.lower():
                                 cell_color = color
                                 break
                     elif col_name == 'Статус БД':
                          for status, color in status_colors.items():
                             if status.lower() in display_value.lower():
                                 cell_color = color
                                 break
                     elif col_name == 'Validation_Errors' and display_value:
                          cell_color = status_colors.get("Ошибка", QColor("orangered"))
                          if "НЕ ПОДТВЕРЖДЕНО" in display_value:
                               cell_color = status_colors.get("НЕ ПОДТВЕРЖДЕНО", QColor("yellow"))


                     if cell_color:
                          item.setBackground(QBrush(cell_color))
                          # Делаем текст черным для светлых фонов для лучшей читаемости
                          if cell_color.lightness() > 180:
                               item.setForeground(QBrush(QColor("black")))

                        # Выравнивание по центру для колонки "Прим."
                     if col_name == 'Прим.':
                        item.setTextAlignment(Qt.AlignCenter)


                     self.dataTable.setItem(row_idx, col_idx, item)
                else:
                    self.dataTable.setItem(row_idx, col_idx, QTableWidgetItem("")) # Пустая ячейка, если колонки нет

        self.update_column_visibility(df_display) # <--- Вызов метода
        self.dataTable.resizeColumnsToContents() # Подгонка ширины колонок - может быть медленно

        self.logMessage("Таблица обновлена.", "DEBUG")
        # Очищаем поле примечаний при обновлении таблицы
        self.notesEdit.clear()
        self.notesEdit.setReadOnly(True)
        self.btnSaveNotes.setEnabled(False)


    def save_generated_reports(self):
        """Сохраняет отчеты, сгенерированные после проверки файла."""
        if not self.current_reports:
            self.logMessage("Нет отчетов для сохранения. Сначала загрузите и обработайте файл.", "WARNING")
            return
        self.logMessage("Запрос на сохранение сгенерированных отчетов...", "INFO")
        self.file_manager.save_reports(self.current_reports)

        # --- Метод on_table_selection_changed теперь только загружает и управляет кнопкой "Сохранить (выбранному)" ---
    def on_table_selection_changed(self):
        selected_rows = self.dataTable.selectionModel().selectedRows()
        person_id = None
        can_save_to_selected_accr = False

        if selected_rows:
            selected_row_index = selected_rows[0].row()
            person_id_item = self.dataTable.item(selected_row_index, 0)

            if person_id_item and person_id_item.text().isdigit():
                person_id = int(person_id_item.text())
                can_save_to_selected_accr = True
                self.logMessage(f"Выбрана строка с ID: {person_id}. Загрузка примечаний...", "DEBUG")
                self.notesEdit.setPlaceholderText("Загрузка примечаний...")
                self.notesEdit.clear()
                self.notesEdit.setReadOnly(True)
                worker = Worker(self.db_manager.get_notes, person_id)
                worker.signals.finished.connect(self.update_notes_signal.emit)  # Используем общий сигнал
                worker.signals.error.connect(lambda e: self.logMessage(f"Ошибка загрузки примечаний: {e}", "ERROR"))
                self.thread_pool.start(worker)
            else:
                self.notesEdit.setPlaceholderText(
                    "Нет ID в БД. Примечания не загружены. Можно ввести общие примечания.")
                self.notesEdit.clear()
                self.notesEdit.setReadOnly(False)
        else:
            self.notesEdit.setPlaceholderText("Примечания к выбранной записи ИЛИ введите сюда общие примечания...")
            self.notesEdit.clear()
            self.notesEdit.setReadOnly(False)

        self.btnSaveNotes.setEnabled(can_save_to_selected_accr)  # Управляем только кнопкой для выбранного

        # --- Метод displayNotes теперь только отображает текст ---

    def displayNotes(self, notes_text):
        """Отображает текст примечаний И делает поле доступным для редактирования."""
        self.notesEdit.setPlainText(notes_text if notes_text is not None else "")  # Обработка None
        self.notesEdit.setReadOnly(False)
        self.notesEdit.setPlaceholderText("Редактируйте примечания здесь...")
        # Кнопка "Сохранить (выбранному)" уже управляется из on_table_selection_changed

    @pyqtSlot(str, int)
    def handle_confirmation_request(self, message, row_index):
        """Обрабатывает запрос на подтверждение данных от пользователя (в основном потоке)."""
        dialog = ConfirmationDialog(message, self)
        confirmed = dialog.exec_() == QDialog.Accepted and dialog.is_confirmed()
        self.user_confirmations[row_index] = confirmed # Сохраняем результат
        self.logMessage(f"Пользователь {'подтвердил' if confirmed else 'не подтвердил'} данные для строки {row_index+1}", "INFO")


    def update_progress(self, value):
        """Обновляет индикатор прогресса (можно добавить QProgressBar)."""
        # Пока просто логируем
        self.logMessage(f"Прогресс задачи: {value}%", "DEBUG")
        # Пример с QProgressDialog (требует доработки управления им):
        # if not hasattr(self, 'progress_dialog'):
        #     self.progress_dialog = QProgressDialog("Выполнение операции...", "Отмена", 0, 100, self)
        #     self.progress_dialog.setWindowModality(Qt.WindowModal)
        #     self.progress_dialog.setAutoClose(True)
        #     self.progress_dialog.setAutoReset(True)
        # self.progress_dialog.setValue(value)
        # if value == 100:
        #      self.progress_dialog.reset()

        # --- Слоты для обработки активации ---
    def handle_set_active_result(self, result):
        success = result.get('success', False)
        message = result.get('message', 'Неизвестная ошибка.')
        row_index = result.get('row_index')
        person_id = result.get('person_id')

        if success:
            self.logMessage(message, "INFO")
            # Обновляем статус в таблице UI
            if row_index is not None:
                status_item = QTableWidgetItem("Аккредитован")
                status_item.setBackground(QBrush(QColor("lightgreen")))
                status_item.setForeground(QBrush(QColor("black")))
                self.dataTable.setItem(row_index, 7, status_item)  # Колонка 'Статус БД'
                self.dataTable.setItem(row_index, 8, QTableWidgetItem("Аккредитован"))  # Колонка 'Статус Проверки'
        else:
            self.logMessage(f"Ошибка активации для ID {person_id}: {message}", "ERROR")
            QMessageBox.warning(self, "Ошибка активации", message)

        self.task_finished_signal.emit("Установка статуса 'Активен'")

    # --- Метод, выполняемый в фоне для активации ---
    def _task_set_active_status(self, person_id, row_index, signals):
        """Worker: Устанавливает статус 'аккредитован' для сотрудника."""
        if not person_id:
            return {'success': False, 'message': "Не удалось получить ID сотрудника.", 'row_index': row_index,
                    'person_id': person_id}

        signals.log.emit(f"Запрос на установку статуса 'Активен' для ID: {person_id}...", "INFO")
        success, message = self.db_manager.set_status_active(person_id)
        return {'success': success, 'message': message, 'row_index': row_index, 'person_id': person_id}

    # --- НОВЫЙ МЕТОД для запуска обработки файла активации ---
    def run_process_activation_file(self):
        self.logMessage("Запуск обработки файла активации...", "INFO")
        # Обработчик результата может быть более детальным
        self.run_task_in_background(self._task_process_activation_file, self.handle_activation_result)

        # --- НОВЫЙ ОБРАБОТЧИК для результатов активации ---

    def handle_activation_result(self, result):
        status = result.get('status', 'error')
        message = result.get('message', 'Неизвестный результат обработки файла активации.')

        if status == 'completed':
            self.logMessage(message, "INFO")
            QMessageBox.information(self, "Обработка завершена", message)
            # Опционально: Обновить таблицу после активации?
            # self.run_search_people() # Можно выполнить поиск по какому-то критерию или общий
        elif status == 'cancelled':
            self.logMessage(message, "INFO")
        else:  # status == 'error'
            self.logMessage(message, "ERROR")
            QMessageBox.critical(self, "Ошибка обработки файла", message)

        self.task_finished_signal.emit("Обработка файла активации")

    # --- Слоты для обработки истории ---
    def handle_show_history_result(self, result):
        if isinstance(result, list):
            if result:
                # Создаем и показываем диалог
                history_dialog = HistoryDialog(result, self)
                history_dialog.exec_()  # Показываем модально
            else:
                self.logMessage("История операций для выбранного сотрудника пуста.", "INFO")
                QMessageBox.information(self, "История операций",
                                        "История операций для выбранного сотрудника пуста.")
        elif isinstance(result, str):  # Сообщение об ошибке
            self.logMessage(result, "WARNING")
            QMessageBox.warning(self, "Ошибка", result)
        self.task_finished_signal.emit("Просмотр истории")

    # --- Метод, выполняемый в фоне для истории ---
    def _task_show_history(self, person_id, signals):
        """Worker: Получает историю операций для сотрудника."""
        if not person_id:
            return "Не удалось получить ID сотрудника."  # Возвращаем строку с ошибкой
        signals.log.emit(f"Запрос истории для ID: {person_id}...", "INFO")
        history_records = self.db_manager.get_employee_records(person_id)
        # history_records будет списком словарей или None в случае ошибки
        if history_records is None:
            return f"Ошибка при получении истории для ID: {person_id}."
        return history_records  # Возвращаем список

    # --- Метод для запуска просмотра истории ---
    def run_show_history(self):
        selected_row_index = self.get_selected_row_index()
        person_id = None
        if selected_row_index is not None:
            person_id_item = self.dataTable.item(selected_row_index, 0)  # Колонка ID
            if person_id_item and person_id_item.text().isdigit():
                person_id = int(person_id_item.text())

        if person_id is None:
            QMessageBox.warning(self, "Ошибка", "Выберите строку с сотрудником (с ID), чтобы посмотреть историю.")
            return

        self.logMessage(f"Запуск получения истории для ID: {person_id}", "DEBUG")
        self.run_task_in_background(self._task_show_history, self.handle_show_history_result, person_id)

    # ui.py (в closeEvent)
    def closeEvent(self, event):
        self.logMessage("Запрос на закрытие приложения...", "INFO")
        reply = QMessageBox.question(self, 'Подтверждение выхода',
                                     "Вы уверены, что хотите выйти?",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if reply == QMessageBox.Yes:
            self.logMessage("Начало закрытия фоновых задач UI...", "INFO")
            # Даем задачам ограниченное время на завершение
            if not self.thread_pool.waitForDone(5000):  # Ждем 5 секунд
                self.logMessage("Не все фоновые задачи UI завершились за 5 секунд. Возможны активные задачи.",
                                "WARNING")
            else:
                self.logMessage("Все фоновые задачи UI завершены.", "INFO")

            # Сигнал для основного потока main.py, что можно начинать закрытие
            # (если закрытие пула БД и планировщика происходит в main.py)
            event.accept()
            self.logMessage("Окно UI готово к закрытию.", "INFO")
        else:
            self.logMessage("Выход из приложения отменен пользователем.", "INFO")
            event.ignore()
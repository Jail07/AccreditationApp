# ui.py
import os
import sys
import pandas as pd
from datetime import datetime, date
import pytz
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor # Для фоновых задач

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton, QTableWidget,
    QTableWidgetItem, QTextEdit, QLabel, QMessageBox, QApplication, QSplitter,
    QHeaderView, QAbstractItemView, QStyleFactory, QProgressDialog, QDialog,
    QFormLayout, QCheckBox, QDialogButtonBox
)
from PyQt5.QtGui import QIcon, QColor, QBrush, QPalette, QFont
from PyQt5.QtCore import Qt, QRunnable, QThreadPool, pyqtSignal, QObject, pyqtSlot, QThread

from config import get_logger
from data_processing import DataProcessor
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

class Worker(QRunnable):
    """Исполнитель задач в отдельном потоке"""
    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()
        # Добавляем сигналы в kwargs, чтобы функция могла их использовать
        self.kwargs['signals'] = self.signals

    @pyqtSlot()
    def run(self):
        try:
            self.signals.log.emit(f"Запуск задачи {self.fn.__name__} в фоновом потоке...", "DEBUG")
            result = self.fn(*self.args, **self.kwargs)
            self.signals.log.emit(f"Задача {self.fn.__name__} завершена.", "DEBUG")
            self.signals.finished.emit(result)
        except Exception as e:
            error_msg = f"Ошибка в фоновой задаче {self.fn.__name__}: {e}\n{traceback.format_exc()}"
            self.signals.log.emit(error_msg, "ERROR")
            self.signals.error.emit(str(e)) # Отправляем краткое сообщение об ошибке

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

# --- Основное окно приложения ---
class AccreditationApp(QWidget):
    # Сигнал для обновления UI из другого потока
    update_table_signal = pyqtSignal(pd.DataFrame)
    update_log_signal = pyqtSignal(str, str)
    update_notes_signal = pyqtSignal(str)
    task_finished_signal = pyqtSignal(str) # Сигнал завершения долгой задачи
    ask_confirmation_signal = pyqtSignal(str, int) # Сигнал для запроса подтверждения

    # Словарь для хранения подтверждений пользователя по индексам строк
    user_confirmations = {}

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
            "INFO": "black",
            "WARNING": "orange",
            "ERROR": "red",
            "CRITICAL": "darkred"
        }
        color = color_map.get(level.upper(), "black")
        formatted_message = f'<font color="{color}">[{timestamp}] [{level.upper()}] {message}</font>'
        self.logText.append(formatted_message)

    def initUI(self):
        """Инициализация интерфейса."""
        self.setWindowTitle('Система Учета Аккредитации v1.0') # Новое название
        # Установка иконки (замените 'icon.png' на путь к вашей иконке)
        try:
            self.setWindowIcon(QIcon('icon.png'))
        except Exception as e:
             self.logMessage(f"Не удалось загрузить иконку 'icon.png': {e}", "WARNING")

        # --- Основной Layout ---
        main_layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Horizontal)

        # --- Левая панель (Управление и Лог) ---
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)

        # Кнопки управления
        control_layout = QHBoxLayout()
        self.btnLoad = QPushButton("Загрузить файл")
        self.btnLoad.clicked.connect(self.run_load_and_process_file) # Запуск через Worker
        self.btnSaveReports = QPushButton("Сохранить отчеты")
        self.btnSaveReports.clicked.connect(self.save_generated_reports)
        self.btnSaveReports.setEnabled(False) # Изначально неактивна
        self.btnAddTD = QPushButton("Добавить 'На проверку' в БД")
        self.btnAddTD.clicked.connect(self.run_add_to_temporary_db) # Запуск через Worker
        self.btnAddTD.setEnabled(False) # Изначально неактивна

        control_layout.addWidget(self.btnLoad)
        control_layout.addWidget(self.btnSaveReports)
        control_layout.addWidget(self.btnAddTD)
        left_layout.addLayout(control_layout)

        # Логгер
        left_layout.addWidget(QLabel("Лог операций:"))
        self.logText = QTextEdit()
        self.logText.setReadOnly(True)
        left_layout.addWidget(self.logText)

        splitter.addWidget(left_panel)

        # --- Правая панель (Таблица, Поиск, Примечания) ---
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)

        # Поиск и управление ЧС
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("Поиск:"))
        self.searchEdit = QLineEdit()
        self.searchEdit.setPlaceholderText("Введите ФИО или организацию...")
        self.searchEdit.returnPressed.connect(self.run_search_people) # Запуск через Worker
        self.btnSearch = QPushButton("Найти")
        self.btnSearch.clicked.connect(self.run_search_people) # Запуск через Worker
        self.btnBlacklist = QPushButton("В ЧС / Из ЧС")
        self.btnBlacklist.clicked.connect(self.run_manage_blacklist) # Запуск через Worker
        search_layout.addWidget(self.searchEdit)
        search_layout.addWidget(self.btnSearch)
        search_layout.addWidget(self.btnBlacklist)
        right_layout.addLayout(search_layout)

        # Таблица данных
        self.dataTable = QTableWidget()
        self.dataTable.setColumnCount(10) # Увеличено для статуса и ID
        self.dataTable.setHorizontalHeaderLabels([
            'ID', 'Фамилия', 'Имя', 'Отчество', 'Дата рождения',
            'Организация', 'Должность', 'Статус БД', 'Статус Проверки', 'Ошибки Валидации'
        ])
        self.dataTable.setEditTriggers(QAbstractItemView.NoEditTriggers) # Запрет редактирования
        self.dataTable.setSelectionBehavior(QAbstractItemView.SelectRows) # Выделение строк
        self.dataTable.setSelectionMode(QAbstractItemView.SingleSelection) # Только одна строка
        self.dataTable.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch) # Растягивание колонок
        self.dataTable.itemSelectionChanged.connect(self.on_table_selection_changed) # Загрузка примечаний при выборе
        right_layout.addWidget(self.dataTable)

        # Примечания
        notes_layout = QVBoxLayout()
        notes_layout.addWidget(QLabel("Примечания к выбранной записи:"))
        self.notesEdit = QTextEdit()
        self.notesEdit.setPlaceholderText("Выберите запись в таблице для просмотра или редактирования примечаний...")
        self.notesEdit.setReadOnly(True) # Изначально только чтение
        self.btnSaveNotes = QPushButton("Сохранить примечания")
        self.btnSaveNotes.setEnabled(False) # Неактивна, пока не выбрана строка
        self.btnSaveNotes.clicked.connect(self.run_save_notes) # Запуск через Worker
        notes_layout.addWidget(self.notesEdit)
        notes_layout.addWidget(self.btnSaveNotes, alignment=Qt.AlignRight)
        right_layout.addLayout(notes_layout)

        splitter.addWidget(right_panel)
        splitter.setSizes([int(self.width() * 0.4), int(self.width() * 0.6)]) # Размеры панелей

        main_layout.addWidget(splitter)
        self.setLayout(main_layout)

        # Применение темной темы
        self.apply_dark_theme()

        self.resize(1200, 800) # Установка размера окна

    def apply_dark_theme(self):
        """Применяет темную тему Fusion."""
        QApplication.setStyle(QStyleFactory.create('Fusion'))
        dark_palette = QPalette()
        dark_color = QColor(45, 45, 45)
        disabled_color = QColor(127, 127, 127)
        text_color = Qt.white
        highlight_color = QColor(42, 130, 218)

        dark_palette.setColor(QPalette.Window, dark_color)
        dark_palette.setColor(QPalette.WindowText, text_color)
        dark_palette.setColor(QPalette.Base, QColor(35, 35, 35))
        dark_palette.setColor(QPalette.AlternateBase, dark_color)
        dark_palette.setColor(QPalette.ToolTipBase, text_color)
        dark_palette.setColor(QPalette.ToolTipText, text_color)
        dark_palette.setColor(QPalette.Text, text_color)
        dark_palette.setColor(QPalette.Disabled, QPalette.Text, disabled_color)
        dark_palette.setColor(QPalette.Button, dark_color)
        dark_palette.setColor(QPalette.ButtonText, text_color)
        dark_palette.setColor(QPalette.Disabled, QPalette.ButtonText, disabled_color)
        dark_palette.setColor(QPalette.BrightText, Qt.red)
        dark_palette.setColor(QPalette.Link, highlight_color)
        dark_palette.setColor(QPalette.Highlight, highlight_color)
        dark_palette.setColor(QPalette.HighlightedText, Qt.black)
        dark_palette.setColor(QPalette.Disabled, QPalette.HighlightedText, disabled_color)

        QApplication.setPalette(dark_palette)
        self.setStyleSheet("QWidget { color: white; } QToolTip { color: #ffffff; background-color: #2a82da; border: 1px solid white; }")

    # --- Слоты для обработки результатов фоновых задач ---

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
        """Обработка результата сохранения примечаний."""
        if isinstance(result, dict) and result.get('success'):
             self.logMessage(f"Примечания для ID {result.get('person_id')} успешно сохранены.", "INFO")
        elif isinstance(result, str):
            self.logMessage(result, "WARNING") # Ошибка или не выбрана строка
        else:
             self.logMessage("Ошибка при сохранении примечаний.", "ERROR")
        self.task_finished_signal.emit("Сохранение примечаний")

    # --- Методы, выполняемые в фоновых потоках ---

    def _task_load_and_process_file(self, signals):
        """Worker: Загружает, очищает, валидирует и проверяет файл."""
        signals.log.emit("Запрос выбора файла...", "INFO")
        file_name = self.file_manager.open_file_dialog()
        if not file_name:
            return "Загрузка файла отменена пользователем."

        signals.log.emit(f"Загрузка данных из {os.path.basename(file_name)}...", "INFO")
        try:
            self.df_loaded = pd.read_excel(file_name, engine='openpyxl')
            signals.log.emit(f"Загружено {len(self.df_loaded)} строк.", "INFO")
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
        # 1. Очистка данных
        signals.log.emit("Очистка данных...", "INFO")
        df_cleaned = self.processor.clean_dataframe(self.df_loaded)
        signals.progress.emit(30)

        # 2. Валидация обязательных полей
        signals.log.emit("Валидация обязательных полей...", "INFO")
        df_validated = self.processor.validate_data(df_cleaned)
        signals.progress.emit(40)

        # 3. Проверка на необычные имена/фамилии
        signals.log.emit("Поиск необычных имен/фамилий...", "INFO")
        suspicious_indices = self.processor.detect_unusual_names(df_validated)
        confirmed_indices = set(df_validated.index) # Изначально все подтверждены

        # Сбрасываем предыдущие подтверждения
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
                     df_validated.loc[idx, 'Validation_Errors'] = (df_validated.loc[idx, 'Validation_Errors'] or "") + "; НЕ ПОДТВЕРЖДЕНО ПОЛЬЗОВАТЕЛЕМ"


        df_to_process = df_validated[df_validated.index.isin(confirmed_indices) & df_validated['Validation_Errors'].isna()].copy()
        df_invalid = df_validated[~df_validated.index.isin(confirmed_indices) | df_validated['Validation_Errors'].notna()].copy()

        signals.log.emit(f"Проверку прошли {len(df_to_process)} строк. Отклонено/не подтверждено: {len(df_invalid)}.", "INFO")
        signals.progress.emit(60)

        # 4. Проверка статуса в БД
        signals.log.emit("Проверка статусов сотрудников в БД...", "INFO")
        statuses_db = []
        person_ids = []
        processed_count = 0
        total_to_process = len(df_to_process)

        for index, row in df_to_process.iterrows():
            status_info = self.db_manager.get_person_status(
                row.get('Фамилия'), row.get('Имя'), row.get('Отчество'), row.get('Дата рождения')
            )
            statuses_db.append(status_info['status'])
            person_ids.append(status_info['person_id'])
            processed_count += 1
            if total_to_process > 0:
                 signals.progress.emit(60 + int(40 * processed_count / total_to_process))


        df_to_process['Статус БД'] = statuses_db
        df_to_process['ID'] = person_ids # Добавляем ID из БД

        # 5. Формирование отчетов и списка для TD
        signals.log.emit("Формирование отчетов...", "INFO")
        reports = {
            'ГПХ_Ранее_отведенные': pd.DataFrame(),
            'Подрядчики_Ранее_отведенные': pd.DataFrame(),
            'ГПХ_Ранее_проверенные_активные': pd.DataFrame(),
            'Подрядчики_Ранее_проверенные_активные': pd.DataFrame(),
            'ГПХ_На_проверку': pd.DataFrame(),
            'Подрядчики_На_проверку': pd.DataFrame(),
            'Ошибки_и_Отклоненные': df_invalid # Добавляем отчет об ошибках
        }
        df_to_process['Статус Проверки'] = '' # Новая колонка для итогового статуса

        df_for_td_list = []

        for index, row in df_to_process.iterrows():
            org = str(row.get('Организация', '')).upper()
            status_db = row['Статус БД']
            is_gph = 'ГПХ' in org

            report_key = None
            status_check = ""

            if status_db == 'BLACKLISTED':
                report_key = 'ГПХ_Ранее_отведенные' if is_gph else 'Подрядчики_Ранее_отведенные'
                status_check = "Ранее отведен"
            elif status_db == 'ACTIVE':
                report_key = 'ГПХ_Ранее_проверенные_активные' if is_gph else 'Подрядчики_Ранее_проверенные_активные'
                status_check = "Активен"
            elif status_db in ['EXPIRED', 'NOT_FOUND']:
                 report_key = 'ГПХ_На_проверку' if is_gph else 'Подрядчики_На_проверку'
                 status_check = "На проверку"
                 # Собираем данные для добавления в TD
                 row_for_td = row.to_dict()
                 row_for_td['status'] = status_check # Добавляем статус проверки
                 df_for_td_list.append(row_for_td)

            df_to_process.loc[index, 'Статус Проверки'] = status_check
            if report_key:
                 # Добавляем строку в соответствующий отчет
                 # Используем .loc[index:index] для сохранения структуры DataFrame
                 reports[report_key] = pd.concat([reports[report_key], df_to_process.loc[index:index]], ignore_index=True)


        df_for_td = pd.DataFrame(df_for_td_list)

        # Объединяем результаты обработки с невалидными строками для отображения в таблице
        df_display = pd.concat([df_to_process, df_invalid], ignore_index=True).fillna('')
        # Убедимся, что все нужные колонки есть
        all_cols = ['ID', 'Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Организация', 'Должность', 'Статус БД', 'Статус Проверки', 'Validation_Errors']
        for col in all_cols:
             if col not in df_display.columns:
                 df_display[col] = ''
        df_display = df_display[all_cols] # Упорядочиваем колонки


        signals.progress.emit(100)
        signals.log.emit("Обработка файла завершена.", "INFO")
        return {'processed_df': df_display, 'reports': reports, 'to_td': df_for_td}

    def _task_search_people(self, signals):
        """Worker: Выполняет поиск в БД."""
        search_term = self.searchEdit.text().strip()
        if not search_term:
            signals.log.emit("Поисковый запрос пуст.", "WARNING")
            return pd.DataFrame() # Возвращаем пустой DataFrame

        signals.log.emit(f"Выполнение поиска по запросу: '{search_term}'...", "INFO")
        results = self.db_manager.search_people(search_term)
        if results is not None:
             # Преобразуем список словарей в DataFrame
             df_results = pd.DataFrame(results)
             # Преобразуем статусы БД для отображения
             now_tz = datetime.now(self.timezone)
             def map_status(row):
                 if row['black_list']: return "В черном списке"
                 if row['end_accr'] and pd.to_datetime(row['end_accr']).tz_convert(self.timezone) > now_tz: return "Аккредитован"
                 if row['status'] == 'в ожидании': return "В ожидании"
                 if row['status'] == 'отведен': return "Отведен (статус)"
                 if row['status'] == 'истек срок': return "Истек срок"
                 return row['status'] # Возвращаем исходный, если не подходит

             df_results['Статус БД'] = df_results.apply(map_status, axis=1)
             # Выбираем нужные колонки и переименовываем для совместимости
             df_results = df_results[['id', 'surname', 'name', 'middle_name', 'birth_date', 'organization', 'position', 'Статус БД']]
             df_results.columns = ['ID', 'Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Организация', 'Должность', 'Статус БД']
             return df_results
        else:
             signals.log.emit(f"Ошибка при поиске по запросу: '{search_term}'.", "ERROR")
             return None # Возвращаем None при ошибке БД

    def _task_add_to_temporary_db(self, signals):
        """Worker: Добавляет отфильтрованные данные в временную таблицу TD."""
        if self.df_to_add_td.empty:
            signals.log.emit("Нет данных для добавления в временную таблицу.", "WARNING")
            return 0

        signals.log.emit(f"Добавление {len(self.df_to_add_td)} записей в TD...", "INFO")
        added_count = 0
        total_count = len(self.df_to_add_td)
        for index, row_data in self.df_to_add_td.iterrows():
             # Преобразуем Series в dict
             data_dict = row_data.to_dict()
             # Добавляем статус проверки, если его нет
             if 'status' not in data_dict:
                  data_dict['status'] = 'На проверку'

             result = self.db_manager.add_to_td(data_dict)
             if result:
                 added_count += 1
             signals.progress.emit(int(100 * (index + 1) / total_count))

        signals.progress.emit(100)
        if added_count == total_count:
            signals.log.emit(f"Успешно добавлено {added_count} записей в TD.", "INFO")
        else:
             signals.log.emit(f"Добавлено {added_count} из {total_count} записей в TD. Проверьте лог на наличие ошибок.", "WARNING")
        return added_count

    def _task_manage_blacklist(self, signals):
        """Worker: Добавляет/удаляет выбранного сотрудника из черного списка."""
        selected_row_index = self.get_selected_row_index()
        if selected_row_index is None:
            return "Не выбрана строка для управления черным списком."

        # Получаем ID и ФИО из ВЫБРАННОЙ строки таблицы UI
        person_id_item = self.dataTable.item(selected_row_index, 0) # Колонка ID
        surname_item = self.dataTable.item(selected_row_index, 1)
        name_item = self.dataTable.item(selected_row_index, 2)
        middlename_item = self.dataTable.item(selected_row_index, 3)


        if not person_id_item or not person_id_item.text().isdigit():
            # Пытаемся найти ID по ФИО + ДР, если в таблице нет ID (например, после загрузки файла)
            dob_item = self.dataTable.item(selected_row_index, 4)
            if surname_item and name_item and dob_item:
                surname = surname_item.text()
                name = name_item.text()
                middle_name = middlename_item.text() if middlename_item else None
                birth_date = self.processor.normalize_date(dob_item.text())
                if birth_date:
                    person_id = self.db_manager.find_person_in_accrtable(surname, name, middle_name, birth_date)
                    if not person_id:
                        return f"Не удалось найти сотрудника {surname} {name} в основной таблице AccrTable для изменения статуса ЧС."
                else:
                     return "Некорректная дата рождения в выбранной строке."
            else:
                return "Недостаточно данных (ID или ФИО+ДР) в выбранной строке для управления черным списком."
        else:
             person_id = int(person_id_item.text())
             surname = surname_item.text() if surname_item else "N/A"
             name = name_item.text() if name_item else "N/A"


        signals.log.emit(f"Запрос на изменение статуса ЧС для ID: {person_id} ({surname} {name})...", "INFO")
        # Подтверждение от пользователя (в основном потоке) - пока убрано, можно добавить при необходимости
        # if not self.showConfirmationDialog(f"Вы уверены, что хотите изменить статус ЧС для {surname} {name}?"):
        #    return "Действие отменено пользователем."

        result_action = self.db_manager.toggle_blacklist(person_id)

        if result_action == "добавлен в черный список":
            message = f"Сотрудник ID {person_id} ({surname} {name}) добавлен в черный список."
            new_status_display = "В черном списке"
            return {'message': message, 'row_index': selected_row_index, 'new_status': new_status_display}
        elif result_action == "убран из черного списка":
            message = f"Сотрудник ID {person_id} ({surname} {name}) убран из черного списка."
            new_status_display = "Снят с ЧС" # Отображаем, что снят
            return {'message': message, 'row_index': selected_row_index, 'new_status': new_status_display}
        else:
             error_msg = f"Ошибка при изменении статуса ЧС для ID: {person_id} ({surname} {name})."
             signals.log.emit(error_msg, "ERROR")
             return error_msg # Возвращаем строку с ошибкой

    def _task_save_notes(self, signals):
        """Worker: Сохраняет примечания для выбранного сотрудника."""
        selected_row_index = self.get_selected_row_index()
        if selected_row_index is None:
            return "Не выбрана строка для сохранения примечаний."

        person_id_item = self.dataTable.item(selected_row_index, 0) # Колонка ID
        if not person_id_item or not person_id_item.text().isdigit():
             return "Не найден ID сотрудника в выбранной строке."

        person_id = int(person_id_item.text())
        notes = self.notesEdit.toPlainText() # Получаем текст из QTextEdit

        signals.log.emit(f"Сохранение примечаний для ID: {person_id}...", "INFO")
        success = self.db_manager.update_notes(person_id, notes)

        return {'success': success, 'person_id': person_id}

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

        # Добавляем задачу в пул потоков
        self.thread_pool.start(worker)

    def run_load_and_process_file(self):
        self.run_task_in_background(self._task_load_and_process_file, self.handle_load_and_process_result)

    def run_search_people(self):
         self.run_task_in_background(self._task_search_people, self.handle_search_result)

    def run_add_to_temporary_db(self):
        self.run_task_in_background(self._task_add_to_temporary_db, self.handle_add_td_result)

    def run_manage_blacklist(self):
         self.run_task_in_background(self._task_manage_blacklist, self.handle_manage_blacklist_result)

    def run_save_notes(self):
         self.run_task_in_background(self._task_save_notes, self.handle_save_notes_result)

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
                           'Организация', 'Должность', 'Статус БД', 'Статус Проверки', 'Validation_Errors']

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
                if col_name in row_data:
                     value = row_data[col_name]
                     # Форматирование даты
                     if col_name == 'Дата рождения' and isinstance(value, (date, datetime)):
                         display_value = value.strftime('%d.%m.%Y')
                     elif pd.isna(value):
                          display_value = ""
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


                     self.dataTable.setItem(row_idx, col_idx, item)
                else:
                    self.dataTable.setItem(row_idx, col_idx, QTableWidgetItem("")) # Пустая ячейка, если колонки нет

        # self.dataTable.resizeColumnsToContents() # Подгонка ширины колонок - может быть медленно
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

    def on_table_selection_changed(self):
        """Загружает примечания при выборе строки в таблице."""
        selected_row_index = self.get_selected_row_index()
        if selected_row_index is not None:
            person_id_item = self.dataTable.item(selected_row_index, 0) # Колонка ID
            if person_id_item and person_id_item.text().isdigit():
                person_id = int(person_id_item.text())
                self.logMessage(f"Загрузка примечаний для ID: {person_id}", "DEBUG")
                # Запускаем загрузку в фоне, чтобы не блокировать UI, если БД медленная
                worker = Worker(self.db_manager.get_notes, person_id)
                worker.signals.finished.connect(self.update_notes_signal.emit) # Обновляем UI по завершении
                worker.signals.error.connect(lambda e: self.logMessage(f"Ошибка загрузки примечаний: {e}", "ERROR"))
                self.thread_pool.start(worker)
                # Активируем поле и кнопку сохранения
                self.notesEdit.setReadOnly(False)
                self.btnSaveNotes.setEnabled(True)
            else:
                # Если нет ID, показываем сообщение
                self.notesEdit.setPlaceholderText("Не найден ID сотрудника в этой строке. Нельзя загрузить или сохранить примечания.")
                self.notesEdit.clear()
                self.notesEdit.setReadOnly(True)
                self.btnSaveNotes.setEnabled(False)
        else:
            # Если ничего не выбрано
            self.notesEdit.setPlaceholderText("Выберите запись в таблице для просмотра или редактирования примечаний...")
            self.notesEdit.clear()
            self.notesEdit.setReadOnly(True)
            self.btnSaveNotes.setEnabled(False)

    def displayNotes(self, notes_text):
         """Отображает текст примечаний в QTextEdit (вызывается из основного потока)."""
         self.notesEdit.setPlainText(notes_text if notes_text else "")


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


    def closeEvent(self, event):
        """Обработка закрытия окна."""
        self.logMessage("Запрос на закрытие приложения...", "INFO")
        reply = QMessageBox.question(self, 'Подтверждение выхода',
                                     "Вы уверены, что хотите выйти?",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if reply == QMessageBox.Yes:
            self.logMessage("Закрытие пула потоков...", "INFO")
            self.thread_pool.waitForDone() # Ждем завершения активных задач
            # Закрытие пула соединений БД (если нужно) - лучше делать в main
            # self.db_manager.close_pool()
            self.logMessage("Приложение закрывается.", "INFO")
            event.accept()
        else:
            self.logMessage("Выход отменен.", "INFO")
            event.ignore()
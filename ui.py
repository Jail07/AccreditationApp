from datetime import datetime

from PyQt5.QtWidgets import QWidget, QVBoxLayout, QPushButton, QFileDialog, QTableWidget, QTableWidgetItem, QTextEdit, QHBoxLayout
from apscheduler.schedulers.background import BackgroundScheduler
from PyQt5.QtGui import QColor
from data_processing import DataProcessor
from file_manager import FileManager
import pandas as pd


class AccreditationApp(QWidget):
    def __init__(self, db_manager):
        super().__init__()
        self.scheduler = BackgroundScheduler()
        self.db_manager = db_manager
        self.file_manager = FileManager()
        self.initUI()

    def logMessage(self, message):
        """Добавляет сообщение в текстовое поле логов."""
        if not self.logText.isVisible():
            self.logText.show()
        self.logText.append(message)

    def initUI(self):
        self.setWindowTitle('Обработка аккредитации')
        layout = QHBoxLayout()

        left_layout = QVBoxLayout()
        self.tableWidget = QTableWidget(self)
        left_layout.addWidget(self.tableWidget)

        self.loadButton = QPushButton('Загрузить таблицу', self)
        self.loadButton.clicked.connect(self.loadFile)
        left_layout.addWidget(self.loadButton)

        self.checkButton = QPushButton('Проверка данных', self)
        self.checkButton.clicked.connect(self.checkData)
        left_layout.addWidget(self.checkButton)

        # Кнопка для добавления во временную таблицу (TD)
        self.addToTempDBButton = QPushButton('Добавить в временную БД', self)
        self.addToTempDBButton.clicked.connect(self.addToTemporaryDB)
        left_layout.addWidget(self.addToTempDBButton)

        # Кнопка для добавления в постоянную таблицу (AccrTable)
        self.addToPermDBButton = QPushButton('Добавить в постоянную БД', self)
        self.addToPermDBButton.clicked.connect(self.addToPermanentDB)
        left_layout.addWidget(self.addToPermDBButton)

        self.generateFileButton = QPushButton('Генерация файла проверки', self)
        self.generateFileButton.clicked.connect(self.generateRecheckFile)
        left_layout.addWidget(self.generateFileButton)

        self.blacklistButton = QPushButton('Черный список', self)
        self.blacklistButton.clicked.connect(self.manageBlacklist)
        left_layout.addWidget(self.blacklistButton)

        layout.addLayout(left_layout)

        self.logText = QTextEdit(self)
        self.logText.setReadOnly(True)
        self.logText.hide()
        layout.addWidget(self.logText)

        self.setLayout(layout)
        self.df = None
        self.processor = DataProcessor()
        self.file_manager = FileManager()

    def loadFile(self):
        file_name = self.file_manager.openFile(self)
        if file_name:
            self.df = pd.read_excel(file_name)
            self.displayTable()
            self.logText.clear()

    def displayTable(self):
        """Отображает содержимое DataFrame в QTableWidget."""
        self.tableWidget.setRowCount(len(self.df))
        self.tableWidget.setColumnCount(len(self.df.columns))
        self.tableWidget.setHorizontalHeaderLabels(self.df.columns)

        for i in range(len(self.df)):
            for j, column in enumerate(self.df.columns):
                value = str(self.df.iloc[i, j]) if pd.notna(self.df.iloc[i, j]) else ''
                item = QTableWidgetItem(value)
                self.tableWidget.setItem(i, j, item)

    def checkData(self):
        if self.df is None:
            self.logMessage("Ошибка: Данные не загружены!")
            return

        self.logText.show()
        self.logText.clear()

        try:
            # Этап 1: Нормализация данных
            try:
                self.df = self.processor.cleanData(self.df)
                self.logMessage("Данные нормализованы.")
                self.displayTable()
            except Exception as e:
                self.logMessage(f"Ошибка нормализации данных: {e}")
                return

            # Этап 2: Проверка обязательных данных
            required_columns = ['Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Организация']
            for i in self.df.index:
                missing_columns = [col for col in required_columns if pd.isna(self.df.at[i, col])]
                if missing_columns:
                    self.df.at[i, 'Статус'] = f"Отсутствуют: {', '.join(missing_columns)}"
                    self.logMessage(f"Строка {i + 1}: отсутствуют обязательные поля: {', '.join(missing_columns)}")
                else:
                    self.df.at[i, 'Статус'] = "Корректные данные"

            self.displayTable()

            # Этап 3: Проверка на дубликаты
            duplicates = self.df.duplicated(subset=['Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Организация'], keep='first')
            for idx in self.df.index[duplicates]:
                self.logMessage(f"Дубликат удален: строка {idx + 1}.")
            self.df = self.df[~duplicates].reset_index(drop=True)
            self.displayTable()

            # Этап 4: Проверка в БД
            for i, row in self.df.iterrows():
                if self.db_manager.find_matches_TD(row['Фамилия'], row['Имя'], row['Отчество'], row['Дата рождения'], row['Организация']) or self.db_manager.find_matches_AccrTable(row['Фамилия'], row['Имя'], row['Отчество'], row['Дата рождения'], row['Организация']):
                    self.df.at[i, 'Статус'] = "уже есть в Временной БД"
                    self.logMessage(f"Дублирование с БД: строка {i + 1}. Статус изменен на 'уже есть в БД'.")

                if self.db_manager.find_matches_AccrTable(row['Фамилия'], row['Имя'], row['Отчество'], row['Дата рождения'], row['Организация']):
                    self.df.at[i, 'Статус'] = "уже есть в Постаянной БД"
                    self.logMessage(f"Дублирование с БД: строка {i + 1}. Статус изменен на 'уже есть в БД'.")

            self.displayTable()

            # Этап 5: Подозрительная последовательность дат
            suspicious_indices = self.processor.detectSequentialDates(self.df, 'Дата рождения')
            for idx in suspicious_indices:
                self.df.at[idx, 'Статус'] = "Подозрительная последовательность дат"
                self.logMessage(f"Подозрительная последовательность дат: строка {idx + 1}.")

            self.displayTable()

        except Exception as e:
            self.logMessage(f"Ошибка проверки данных: {e}")

    def addToTemporaryDB(self):
        """Добавляет строки во временную таблицу TD."""
        if self.df is None:
            self.logMessage("Ошибка: Данные не загружены!")
            return

        try:
            added_count = 0
            for _, row in self.df.iterrows():
                if row['Статус'] == "Корректные данные" or row['Статус'] == "Подозрительная последовательность дат":
                    self.db_manager.add_to_td(row.to_dict())
                    added_count += 1

            self.logMessage(f"Добавлено {added_count} строк в временную таблицу TD.")
        except Exception as e:
            self.logMessage(f"Ошибка добавления в временную БД: {e}")

    def addToPermanentDB(self):
        """Добавляет строки в постоянную таблицу AccrTable."""
        if self.df is None:
            self.logMessage("Ошибка: Данные не загружены!")
            return

        try:
            added_count = 0
            for _, row in self.df.iterrows():
                if row['Статус'] == "Корректные данные":
                    self.db_manager.add_to_accrtable(row.to_dict(), status="аккредитован")
                    added_count += 1

            self.logMessage(f"Добавлено {added_count} строк в постоянную таблицу AccrTable.")
        except Exception as e:
            self.logMessage(f"Ошибка добавления в постоянную БД: {e}")

    def generateRecheckFile(self):
        try:
            self.generate_check_file()
            # self.db_manager.transfer_to_accrtable()
            self.logMessage("Файл проверки сгенерирован и данные перемещены в AccrTable.")
        except Exception as e:
            print(f"Ошибка при генерации файла: {e}")
            self.logMessage(f"Ошибка при генерации файла: {e}")

    def generate_check_file(self):
        print("1"*100)
        """
        Генерация файла проверки во вторник.
        Сохраняет файл и логирует действия.
        """
        try:
            # Получаем данные для перепроверки
            people_for_recheck = self.db_manager.transfer_to_accrtable()
            print(people_for_recheck)

            if people_for_recheck:
                today_date = datetime.now().strftime('%d-%m-%Y')
                file_name = f"Запрос на проверку_{today_date}.xlsx"


                # Формируем данные для файла
                data = [
                    {
                        "Фамилия": p[1],
                        "Имя": p[2],
                        "Отчество": p[3] or '',
                        "Дата рождения": p[4].strftime('%d.%m.%Y'),
                        "Организация": p[5],
                    }
                    for p in people_for_recheck
                ]

                # Создаем DataFrame и сохраняем файл
                df = pd.DataFrame(data)
                self.file_manager.saveFile(df, file_name)

                # Логируем транзакции
                for person in people_for_recheck:
                    self.db_manager.log_transaction(person[0], "Generated Recheck File")

                print(f"Файл проверки успешно сгенерирован: {file_name}")
            else:
                print("Нет данных для генерации файла проверки.")
        except Exception as e:
            print(f"Ошибка при генерации файла проверки: {e}")


    def manageBlacklist(self):
        """Управление статусом 'в ЧС'."""
        selected_row = self.tableWidget.currentRow()
        if selected_row == -1:
            self.logMessage("Ошибка: Не выбрана строка для управления черным списком.")
            return
        surname = self.tableWidget.item(selected_row, 1).text()
        name = self.tableWidget.item(selected_row, 2).text()
        middle_name = self.tableWidget.item(selected_row, 3).text()
        birth_date = self.tableWidget.item(selected_row, 4).text()
        birth_place = self.tableWidget.item(selected_row, 5).text()
        registration = self.tableWidget.item(selected_row, 6).text()
        organization = self.tableWidget.item(selected_row, 7).text()
        position = self.tableWidget.item(selected_row, 8).text()

        try:
            status_changed = self.db_manager.toggle_blacklist(surname, name, middle_name, birth_date, birth_place, registration, organization, position)
            if status_changed == "добавлен в черный список":
                self.logMessage(f"{surname} {name} {middle_name} добавлен в черный список.")
            elif status_changed == "убран из черного списка":
                self.logMessage(f"{surname} {name} {middle_name} убран из черного списка.")
            elif not status_changed:
                self.logMessage(f"Ошибка управления черным списком: {surname} {name} {middle_name} не изменён.")

        except Exception as e:
            self.logMessage(f"Ошибка управления черным списком: {e}")

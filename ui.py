from PyQt5.QtWidgets import QWidget, QVBoxLayout, QPushButton, QFileDialog, QTableWidget, QTableWidgetItem
from PyQt5.QtGui import QColor
from data_processing import DataProcessor
from file_manager import FileManager
import pandas as pd


class AccreditationApp(QWidget):
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Обработка аккредитации')

        # Основной макет
        layout = QVBoxLayout()

        # Таблица
        self.tableWidget = QTableWidget(self)
        layout.addWidget(self.tableWidget)

        # Кнопки
        self.loadButton = QPushButton('Загрузить таблицу', self)
        self.loadButton.clicked.connect(self.loadFile)
        layout.addWidget(self.loadButton)

        self.processButton = QPushButton('Обработать данные', self)
        self.processButton.clicked.connect(self.processData)
        layout.addWidget(self.processButton)

        self.saveButton = QPushButton('Сохранить таблицу', self)
        self.saveButton.clicked.connect(self.saveFile)
        layout.addWidget(self.saveButton)

        self.blacklistButton = QPushButton('Добавить в черный список', self)
        self.blacklistButton.clicked.connect(self.addToBlacklist)
        layout.addWidget(self.blacklistButton)

        self.setLayout(layout)
        self.df = None  # Для хранения данных
        self.processor = DataProcessor()
        self.file_manager = FileManager()

        self.show()

    def loadFile(self):
        file_name = self.file_manager.openFile(self)
        if file_name:
            self.df = pd.read_excel(file_name)
            self.displayTable()

    def displayTable(self):
        self.tableWidget.setRowCount(len(self.df))
        self.tableWidget.setColumnCount(len(self.df.columns))
        self.tableWidget.setHorizontalHeaderLabels(self.df.columns)

        for i in range(len(self.df)):
            for j in range(len(self.df.columns)):
                item = QTableWidgetItem(str(self.df.iloc[i, j]))
                self.tableWidget.setItem(i, j, item)

    def processData(self):
        if self.df is None:
            print("Нет данных для обработки")
            return

        try:
            # 1. Нормализация данных
            self.df = self.processor.cleanData(self.df)

            # 2. Проверка на заполненность обязательных столбцов
            required_columns = ['Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Место рождения', 'Регистрация',
                                'Организация', 'Должность']
            valid_data = self.df.dropna(subset=required_columns)
            invalid_data = self.df[~self.df.index.isin(valid_data.index)]

            # 3. Проверка на дублирование сотрудников в файле
            self.df = self.processor.checkDuplicates(self.df)

            # 4. Проверка последовательных дат
            suspicious_indices = self.processor.detectSequentialDates(self.df, 'Дата рождения')

            # 5. Проверка сотрудников в базе данных
            added_indices = []  # Список строк, добавленных в БД
            for index, row in valid_data.iterrows():
                if index not in suspicious_indices:  # Исключаем строки с подозрительными датами
                    if self.db_manager.find_matches(
                            row['Фамилия'], row['Имя'], row['Отчество'], row['Дата рождения'], row['Организация']
                    ):
                        self.df.at[index, 'В_БД'] = True  # Строка уже существует в БД
                    else:
                        self.db_manager.save_valid_data([row.to_dict()])
                        self.df.at[index, 'Добавлено'] = True  # Помечаем строку как добавленную
                        added_indices.append(index)  # Добавляем индекс в список добавленных строк

            # 6. Обновление цветов в таблице
            self.updateTableColors(invalid_data.index, suspicious_indices)

        except Exception as e:
            print(f"Ошибка при обработке данных: {e}")

    def updateTableColors(self, invalid_indices, suspicious_indices):
        """
        Обновляет цвета строк в таблице по приоритетам:
        1. Пропущенные данные — оранжевый.
        2. Дублирование в файле — красный.
        3. Дублирование в БД — фиолетовый.
        4. Подозрительные даты — синий.
        5. Корректные данные, добавленные в БД — зеленый.
        """
        for i in range(self.tableWidget.rowCount()):
            # Проверяем приоритеты последовательно
            color = None

            if i in invalid_indices:
                color = QColor(255, 165, 0)  # Оранжевый для пропущенных данных
            elif self.df.iloc[i].get('Повтор', False):
                color = QColor(255, 0, 0)  # Красный для дубликатов в файле
            elif i in suspicious_indices:
                color = QColor(0, 0, 255)  # Синий для подозрительных дат
            elif self.df.iloc[i].get('В_БД', False):
                color = QColor(128, 0, 128)  # Фиолетовый для строк, уже в БД
            else:
                color = QColor(0, 255, 0)  # Зеленый для корректных данных, добавленных в БД

            # Применяем цвет, если он установлен
            for j in range(self.tableWidget.columnCount()):
                item = self.tableWidget.item(i, j) or QTableWidgetItem(str(self.df.iloc[i, j]))
                if color:
                    item.setBackground(color)
                self.tableWidget.setItem(i, j, item)

    def highlight_suspicious_rows(self, suspicious_indices):
        """
        Подсвечивает подозрительные строки (автозаполненные даты) зеленым цветом.
        """
        for i in suspicious_indices:
            for j in range(self.tableWidget.columnCount()):
                item = self.tableWidget.item(i, j) or QTableWidgetItem(str(self.df.iloc[i, j]))
                item.setBackground(QColor(0, 255, 0))  # Зеленый для подозрительных строк
                self.tableWidget.setItem(i, j, item)


    def mark_table_colors(self, valid_data, invalid_data):
        for i in range(len(self.df)):
            for j in range(len(self.df.columns)):
                item = QTableWidgetItem(str(self.df.iloc[i, j]))
                if i in invalid_data.index:
                    item.setBackground(QColor(255, 165, 0))  # Orange for incomplete
                elif self.df.iloc[i].get('Повтор', False):
                    item.setBackground(QColor(255, 0, 0))  # Red for duplicates
                self.tableWidget.setItem(i, j, item)

    def saveFile(self):
        if self.df is None:
            return

        # Фильтруем только валидные данные
        valid_data = self.df.dropna(subset=['Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Место рождения', 'Регистрация', 'Организация', 'Должность'])
        self.db_manager.save_valid_data(valid_data.to_dict('records'))

        # Сохраняем файл
        self.file_manager.saveFile(self.df, self)

    def addToBlacklist(self):
        selected_row = self.tableWidget.currentRow()
        if selected_row != -1:
            fio = self.tableWidget.item(selected_row, 0).text()
            birth_date = self.tableWidget.item(selected_row, 2).text()

            person_id = self.db_manager.get_person_id(fio, birth_date)
            if person_id:
                self.db_manager.move_to_blacklist(person_id)
                print(f"{fio} добавлен в черный список.")



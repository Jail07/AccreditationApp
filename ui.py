from PyQt5.QtWidgets import QWidget, QVBoxLayout, QPushButton, QFileDialog, QTableWidget, QTableWidgetItem
from PyQt5.QtGui import QColor
from data_processing import DataProcessor
from file_manager import FileManager
import pandas as pd


class AccreditationApp(QWidget):
    def __init__(self):
        super().__init__()
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
            return

        # Очистка и проверка данных
        self.df = self.processor.cleanData(self.df)
        self.df = self.processor.checkDuplicates(self.df)
        self.updateTableColors()

    def updateTableColors(self):
        for i in range(len(self.df)):
            for j in range(len(self.df.columns)):
                item = QTableWidgetItem(str(self.df.iloc[i, j]))
                if self.df.iloc[i].get('Повтор', False):
                    item.setBackground(QColor(255, 0, 0))  # Красный цвет для повторов
                elif pd.isna(self.df.iloc[i, j]) and j != 0:
                    item.setBackground(QColor(255, 165, 0))  # Оранжевый цвет для отсутствующих данных
                self.tableWidget.setItem(i, j, item)

    def saveFile(self):
        if self.df is None:
            return
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

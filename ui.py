import traceback
from datetime import datetime, date
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton, QFileDialog,
    QTableWidget, QTableWidgetItem, QTextEdit, QLabel, QMessageBox, QApplication
)
from apscheduler.schedulers.background import BackgroundScheduler
from data_processing import DataProcessor
from file_manager import FileManager
import pandas as pd


class AccreditationApp(QWidget):
    def __init__(self, db_manager):
        super().__init__()
        self.scheduler = BackgroundScheduler()
        self.db_manager = db_manager
        self.file_manager = FileManager()
        self.processor = DataProcessor()
        self.df = None
        self.initUI()

    def logMessage(self, message, level="INFO"):

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_message = f"[{timestamp}] [{level}] {message}"
        self.logText.append(full_message)
        print(full_message)  # Для отладки в консоли

    def initUI(self):
        self.setWindowTitle('Обработка аккредитации')

        screen_geometry = QApplication.primaryScreen().availableGeometry()
        self.setGeometry(screen_geometry)

        main_layout = QHBoxLayout()

        right_layout = QVBoxLayout()

        self.logText = QTextEdit(self)
        self.logText.setReadOnly(True)
        right_layout.addWidget(self.logText, stretch=2)

        search_layout = QVBoxLayout()

        search_section = QHBoxLayout()
        self.searchField = QLineEdit(self)
        self.searchField.setPlaceholderText("Введите данные для поиска (Фамилия, Имя и т.д.)")
        self.searchButton = QPushButton("Поиск", self)
        self.searchButton.clicked.connect(self.searchData)
        search_section.addWidget(self.searchField)
        search_section.addWidget(self.searchButton)
        search_layout.addLayout(search_section)

        self.resultTable = QTableWidget(self)
        self.resultTable.setColumnCount(3)
        self.resultTable.setHorizontalHeaderLabels(["ФИО", "Дата рождения", "Статус"])

        self.resultTable.setColumnWidth(0, 300)
        self.resultTable.setColumnWidth(1, 100)
        self.resultTable.setColumnWidth(2, 300)

        search_layout.addWidget(self.resultTable)

        right_layout.addLayout(search_layout, stretch=1)

        left_layout = QVBoxLayout()

        self.tableWidget = QTableWidget(self)
        left_layout.addWidget(self.tableWidget)

        self.loadButton = QPushButton('Загрузить таблицу', self)
        self.loadButton.clicked.connect(self.loadFile)
        left_layout.addWidget(self.loadButton)

        self.checkButton = QPushButton('Проверка данных', self)
        self.checkButton.clicked.connect(self.checkData)
        left_layout.addWidget(self.checkButton)

        self.addToTempDBButton = QPushButton('Добавить в временную БД', self)
        self.addToTempDBButton.clicked.connect(self.addToTemporaryDB)
        left_layout.addWidget(self.addToTempDBButton)

        self.addToPermDBButton = QPushButton('Добавить в постоянную БД', self)
        self.addToPermDBButton.clicked.connect(self.addToPermanentDB)
        left_layout.addWidget(self.addToPermDBButton)

        self.generateFileButton = QPushButton('Генерация файла проверки', self)
        self.generateFileButton.clicked.connect(self.generateRecheckFile)
        left_layout.addWidget(self.generateFileButton)
        
        self.recordButton = QPushButton("Генерация записи сотрудника", self)
        self.recordButton.clicked.connect(self.generateEmployeeRecord)
        left_layout.addWidget(self.recordButton)

        self.blacklistButton = QPushButton('Черный список', self)
        self.blacklistButton.clicked.connect(self.manageBlacklist)
        left_layout.addWidget(self.blacklistButton)


        main_layout.addLayout(left_layout, stretch=3)
        main_layout.addLayout(right_layout, stretch=2)


        self.setLayout(main_layout)


    def displayTable(self):
        self.tableWidget.setRowCount(len(self.df))
        self.tableWidget.setColumnCount(len(self.df.columns))
        self.tableWidget.setHorizontalHeaderLabels(self.df.columns)

        for i in range(len(self.df)):
            for j, column in enumerate(self.df.columns):
                value = str(self.df.iloc[i, j]) if pd.notna(self.df.iloc[i, j]) else ''
                item = QTableWidgetItem(value)
                self.tableWidget.setItem(i, j, item)

    def loadFile(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Выберите файл для проверки",
            "",
            "Excel Files (*.xlsx *.xls);;All Files (*)"
        )

        if file_path:
            try:
                self.df = pd.read_excel(file_path)
                self.displayTable()
                self.logMessage(f"Файл успешно загружен: {file_path}")
            except Exception as e:
                self.logMessage(f"Ошибка при загрузке файла: {e}")

    def searchData(self):
        search_term = self.searchField.text().strip()
        if not search_term:
            self.logMessage("Ошибка: Поле поиска пустое.")
            return

        results = self.db_manager.search_person(search_term)

        if not results:
            self.logMessage(f"По запросу '{search_term}' ничего не найдено.")
            self.resultTable.setRowCount(0)
            return

        self.resultTable.setRowCount(len(results))
        for i, (fio, birth_date, status) in enumerate(results):
            self.resultTable.setItem(i, 0, QTableWidgetItem(fio))
            self.resultTable.setItem(i, 1, QTableWidgetItem(birth_date.strftime("%d.%m.%Y")))
            self.resultTable.setItem(i, 2, QTableWidgetItem(status))
        self.logMessage(f"Найдено {len(results)} совпадений для '{search_term}'.")

    def showConfirmationDialog(self, message):
        confirmation_dialog = QMessageBox(self)
        confirmation_dialog.setIcon(QMessageBox.Question)
        confirmation_dialog.setWindowTitle("Подтверждение")
        confirmation_dialog.setText(message)
        confirmation_dialog.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
        confirmation_dialog.setDefaultButton(QMessageBox.No)

        response = confirmation_dialog.exec_()
        return response == QMessageBox.Yes

    def generateEmployeeRecord(self):
        selected_row = self.resultTable.currentRow()
        if selected_row == -1:
            self.logMessage("Ошибка: Не выбран сотрудник для генерации записи.")
            return

        fio = self.resultTable.item(selected_row, 0).text()
        birth_date = self.resultTable.item(selected_row, 1).text()
        print(fio)
        print(birth_date)

        records = self.db_manager.get_employee_records(fio, birth_date)

        if not records:
            self.logMessage("Нет записей для выбранного сотрудника.")
            return

        df = pd.DataFrame(records, columns=["Дата/Время", "Объект", "Организация", "Событие"])
        file_name, _ = QFileDialog.getSaveFileName(self, "Сохранить запись", "", "Excel Files (*.xlsx)")
        if file_name:
            df.to_excel(file_name, index=False)
            self.logMessage(f"Запись сотрудника сохранена: {file_name}")

    def checkData(self):
        if self.df is None:
            self.logMessage("Ошибка: Данные не загружены!")
            return

        self.logText.show()
        self.logText.clear()

        try:
            try:
                self.df = self.processor.cleanData(self.df)
                self.logMessage("Данные нормализованы.")
                self.displayTable()
            except Exception as e:
                self.logMessage(f"Ошибка нормализации данных: {e}")
                return

            required_columns = ['Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Организация']
            for i in self.df.index:
                missing_columns = [col for col in required_columns if pd.isna(self.df.at[i, col])]
                if missing_columns:
                    self.df.at[i, 'Статус'] = f"Отсутствуют: {', '.join(missing_columns)}"
                    self.logMessage(f"Строка {i + 1}: отсутствуют обязательные поля: {', '.join(missing_columns)}")
                else:
                    self.df.at[i, 'Статус'] = "Корректные данные"

            self.displayTable()

            duplicates = self.df.duplicated(subset=['Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Организация'], keep='first')
            for idx in self.df.index[duplicates]:
                self.logMessage(f"Дубликат удален: строка {idx + 1}")
            self.df = self.df[~duplicates].reset_index(drop=True)
            self.displayTable()

            suspicious_indices = self.processor.detectSequentialDates(self.df, 'Дата рождения')
            for idx in suspicious_indices:
                self.df.at[idx, 'Статус'] = "Подозрительная последовательность дат"
                self.logMessage(f"Подозрительная последовательность дат: строка {idx + 1}.")
            self.displayTable()

            for i, row in self.df.iterrows():
                if self.db_manager.find_matches_TD(row['Фамилия'], row['Имя'], row['Отчество'], row['Дата рождения']):
                    self.df.at[i, 'Статус'] = "уже есть в Временной БД"
                    self.logMessage(f"Дублирование с БД: строка {i + 1}. Статус изменен на 'уже есть в Временной БД'.")

                if self.db_manager.find_matches_AccrTable(row['Фамилия'], row['Имя'], row['Отчество'], row['Дата рождения']):
                    self.df.at[i, 'Статус'] = "уже есть в Постоянной БД"
                    self.logMessage(f"Дублирование с БД: строка {i + 1}. Статус изменен на 'уже есть в Постоянной БД'.")

            self.displayTable()

        except Exception as e:
            self.logMessage(f"Ошибка проверки данных: {e}")

    def addToTemporaryDB(self):
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


    def process_accreditation_file(self):

        if self.df is None:
            self.logMessage("Ошибка: Файл не загружен.")
            return

        try:
            valid_ids = self.db_manager.validate_accreditation_file(self.df)
            if not valid_ids:
                self.logMessage("Нет соответствий сотрудников из файла с данными в постоянной БД.")
                return

            self.db_manager.update_accreditation_status_from_file(valid_ids)
            self.logMessage("Данные сотрудников успешно обновлены. Статус изменён на 'аккредитован'.")
        except Exception as e:
            self.logMessage(f"Ошибка обработки файла: {e}")

    def addToPermanentDB(self):
        if self.df is None:
            self.logMessage("Ошибка: Данные не загружены!")
            return

        if not self.showConfirmationDialog("Вы уверены, что сотрудники аккредитованы и данные корректны?"):
            self.logMessage("Действие отменено пользователем.")
            return

        try:
            valid_ids = self.db_manager.validate_accreditation_file(self.df)
            if not valid_ids:
                self.logMessage("Нет соответствий сотрудников из файла с данными в постоянной БД.")
                return

            self.db_manager.update_accreditation_status_from_file(valid_ids)
            self.logMessage("Сотрудники успешно добавлены в постоянную БД и обновлены.")
        except Exception as e:
            self.logMessage(f"Ошибка добавления сотрудников: {e}")

    def generateRecheckFile(self):
        try:
            self.generate_check_file()
            # self.db_manager.transfer_to_accrtable()
            self.logMessage("Файл проверки сгенерирован и данные перемещены в AccrTable.")
        except Exception as e:
            print(f"Ошибка при генерации файла: {e}")
            self.logMessage(f"Ошибка при генерации файла: {e}")

    def generate_check_file(self):
        try:
            print("[INFO] Генерация файлов проверки началась.")

            people_for_recheck = self.db_manager.get_people_for_recheck_full()

            if people_for_recheck:
                today_date = datetime.now().strftime('%d-%m-%Y')

                gph_data = [p for p in people_for_recheck if "ГПХ" in (p['organization'] or "").upper()]
                other_data = [p for p in people_for_recheck if "ГПХ" not in (p['organization'] or "").upper()]

                if gph_data:
                    gph_file_name = f"Запрос на проверку_ГПХ_{today_date}.xlsx"
                    gph_df = pd.DataFrame([
                        {
                            "Фамилия": p['surname'],
                            "Имя": p['name'],
                            "Отчество": p['middle_name'] or '',
                            "Дата рождения": p['birth_date'].strftime('%d.%m.%Y') if isinstance(p['birth_date'],
                                                                                                date) else '',
                            "Место рождения": p['birth_place'],
                            "Регистрация": p['registration'],
                            "Организация": p['organization']
                        }
                        for p in gph_data
                    ])
                    self.file_manager.saveFile(gph_df, gph_file_name)
                    print(f"[INFO] Файл для ГПХ успешно сгенерирован: {gph_file_name}")
                else:
                    print("[INFO] Нет данных для ГПХ.")

                if other_data:
                    other_file_name = f"Запрос на проверку_Другие_{today_date}.xlsx"
                    other_df = pd.DataFrame([
                        {
                            "Фамилия": p['surname'],
                            "Имя": p['name'],
                            "Отчество": p['middle_name'] or '',
                            "Дата рождения": p['birth_date'].strftime('%d.%m.%Y') if isinstance(p['birth_date'],
                                                                                                date) else '',
                            "Место рождения": p['birth_place'],
                            "Регистрация": p['registration'],
                            "Организация": p['organization']
                        }
                        for p in other_data
                    ])
                    self.file_manager.saveFile(other_df, other_file_name)
                    print(f"[INFO] Файл для других организаций успешно сгенерирован: {other_file_name}")
                else:
                    print("[INFO] Нет данных для других организаций.")
            else:
                print("[INFO] Нет данных для генерации файлов проверки.")

        except Exception as e:
            print(f"[ERROR] Ошибка при генерации файлов проверки: {e}\n{traceback.format_exc()}")

    def manageBlacklist(self):
        if self.resultTable.rowCount() > 0 and self.resultTable.currentRow() != -1:
            selected_row = self.resultTable.currentRow()
            surname = self.resultTable.item(selected_row, 0).text().split()[0]
            name = self.resultTable.item(selected_row, 0).text().split()[1]
            middle_name = self.resultTable.item(selected_row, 0).text().split()[2] if len(
                self.resultTable.item(selected_row, 0).text().split()) > 2 else None
            birth_date = self.resultTable.item(selected_row, 1).text()

            if not self.showConfirmationDialog(
                    f"Вы уверены, что хотите изменить статус сотрудника {surname} {name} {middle_name}?"):
                self.logMessage("Действие отменено пользователем.")
                return

            try:
                status_changed = self.db_manager.toggle_blacklist(surname, name, middle_name, birth_date, None, None,
                                                                  None, None)
                if status_changed == "добавлен в черный список":
                    self.logMessage(f"{surname} {name} {middle_name} добавлен в черный список.")
                elif status_changed == "убран из черного списка":
                    self.logMessage(f"{surname} {name} {middle_name} убран из черного списка.")
                elif not status_changed:
                    self.logMessage(f"Ошибка управления черным списком: {surname} {name} {middle_name} не изменён.")
            except Exception as e:
                self.logMessage(f"Ошибка управления черным списком: {e}")


        elif self.tableWidget.rowCount() > 0 and self.tableWidget.currentRow() != -1:
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

            if not self.showConfirmationDialog(
                    f"Вы уверены, что хотите изменить статус сотрудника {surname} {name} {middle_name}?"):
                self.logMessage("Действие отменено пользователем.")
                return

            try:
                status_changed = self.db_manager.toggle_blacklist(surname, name, middle_name, birth_date, birth_place,
                                                                  registration, organization, position)
                print(status_changed, surname, name, middle_name, birth_date)
                if status_changed == "добавлен в черный список":
                    self.logMessage(f"{surname} {name} {middle_name} добавлен в черный список.")
                    self.df.at[selected_row, 'Статус'] = "В черном списке"
                    self.displayTable()
                elif status_changed == "убран из черного списка":
                    self.logMessage(f"{surname} {name} {middle_name} убран из черного списка.")
                    self.df.at[selected_row, 'Статус'] = "Нужно проверить"
                    self.displayTable()
                elif not status_changed:
                    self.logMessage(f"Ошибка управления черным списком: {surname} {name} {middle_name} не изменён.")
            except Exception as e:
                self.logMessage(f"Ошибка управления черным списком: {e}")
        else:
            self.logMessage("Ошибка: Не выбран сотрудник для управления черным списком.")


from PyQt5.QtWidgets import QFileDialog
from openpyxl import Workbook
from openpyxl.styles import PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows


class FileManager:
    def openFile(self, parent):
        options = QFileDialog.Options()
        file_name, _ = QFileDialog.getOpenFileName(parent, "Загрузить файл", "", "Excel Files (*.xls *.xlsx)",
                                                   options=options)
        return file_name

    def saveFile(self, df, parent):
        options = QFileDialog.Options()
        file_name, _ = QFileDialog.getSaveFileName(parent, "Сохранить файл", "", "Excel Files (*.xlsx)",
                                                   options=options)
        if file_name:
            df_to_save = df.drop(columns=['Повтор'], errors='ignore')
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Обработанные данные"
            for row in dataframe_to_rows(df_to_save, index=False, header=True):
                sheet.append(row)
            wb.save(file_name)

import pandas as pd
from PyQt5.QtWidgets import QFileDialog


class FileManager:
    def openFile(self, parent):
        file_name, _ = QFileDialog.getOpenFileName(parent, "Выберите файл", "", "Excel Files (*.xlsx)")
        return file_name

    def saveFile(self, df, file_name):
        df.to_excel(file_name, index=False)

import os

from PyQt5.QtWidgets import QFileDialog, QMessageBox


class FileManager:
    def __init__(self, parent=None):
        self.parent = parent
    def openFile(self):
        file_name, _ = QFileDialog.getOpenFileName(self.parent, "Выберите файл", "", "Excel Files (*.xlsx)")
        return file_name

    def saveFile(self, df, name):
        options = QFileDialog.Options()
        if df is None:
            self.parent.logMessage("Нет данных для сохранения.")
            return

        save_path, _ = QFileDialog.getSaveFileName(
            self.parent,
            "Сохранить файл как",
            f"{name}",
            "Excel Files (*.xlsx);;All Files (*)", options=options
        )

        if save_path:
            try:
                df.to_excel(save_path, index=False)
                self.parent.logMessage(f"Файл успешно сохранен: {save_path}")
            except Exception as e:
                self.parent.logMessage(f"Ошибка при сохранении файла: {e}")

    def genFile(self, df, name):
        if df is None:
            self.parent.logMessage("Нет данных для генерации.")
            return

        save_path = os.path.join(os.getcwd(), f"{name}.xlsx")

        try:
            df.to_excel(save_path, index=False)
            self.parent.logMessage(f"Файл успешно сгенерирован: {save_path}")
        except Exception as e:
            self.parent.logMessage(f"Ошибка при генерации файла: {e}")
            QMessageBox.critical(self.parent, "Ошибка", f"Не удалось сохранить файл:\n{e}")
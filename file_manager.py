from PyQt5.QtWidgets import QFileDialog


class FileManager:
    def openFile(self, parent):
        file_name, _ = QFileDialog.getOpenFileName(parent, "Выберите файл", "", "Excel Files (*.xlsx)")
        return file_name

    def saveFile(self):
        if self.df is None:
            self.logMessage("Нет данных для сохранения.")
            return

        save_path, _ = QFileDialog.getSaveFileName(
            self,
            "Сохранить файл как",
            "",
            "Excel Files (*.xlsx);;All Files (*)"
        )

        if save_path:
            try:
                self.df.to_excel(save_path, index=False)
                self.logMessage(f"Файл успешно сохранен: {save_path}")
            except Exception as e:
                self.logMessage(f"Ошибка при сохранении файла: {e}")

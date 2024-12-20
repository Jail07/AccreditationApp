import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, date
from pytz import timezone
import traceback

from database_manager import DatabaseManager
from file_manager import FileManager



class Scheduler:
    def __init__(self, db_config):
        self.scheduler = BackgroundScheduler(timezone="Asia/Bishkek")  # Установите вашу временную зону
        self.db_manager = DatabaseManager(**db_config)
        self.file_manager = FileManager()

    def generate_recheck_file(self):
        """
        Генерация файла проверки.
        Сохраняет файл и логирует действия.
        """
        try:
            print("Генерация файла проверки началась.")
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
                        "Дата рождения": p[4].strftime('%d.%m.%Y') if isinstance(p[4], date) else '',
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

    def check_schedule_and_generate(self):
        """
        Проверяет расписание и вызывает генерацию файла проверки.
        """
        try:
            now = datetime.now()
            print(f"[DEBUG] Проверка расписания в: {now}.")
            self.generate_recheck_file()
        except Exception as e:
            print(f"Ошибка при проверке расписания: {e}\n{traceback.format_exc()}")

    def start(self):
        """
        Запуск планировщика.
        """
        try:
            print("[DEBUG] Запуск планировщика.")
            self.scheduler.add_job(
                self.check_schedule_and_generate,
                "cron",
                day_of_week="fri",
                hour=22,
                minute=58,
            )
            self.scheduler.start()
            print(f"[DEBUG] Планировщик успешно запущен. Задача будет выполняться каждый вторник в 10:00.")
        except Exception as e:
            print(f"Ошибка при запуске планировщика: {e}\n{traceback.format_exc()}")

    def stop(self):
        """
        Останавливает планировщик.
        """
        try:
            self.scheduler.shutdown()
            print("[DEBUG] Планировщик успешно остановлен.")
        except Exception as e:
            print(f"Ошибка при остановке планировщика: {e}\n{traceback.format_exc()}")

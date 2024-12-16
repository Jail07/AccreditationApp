from apscheduler.schedulers.background import BackgroundScheduler
from file_manager import FileManager
from database_manager import DatabaseManager
from datetime import datetime
import pandas as pd
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


class Scheduler:
    def __init__(self, db_config):
        self.scheduler = BackgroundScheduler()
        self.db_manager = DatabaseManager(**db_config)
        self.file_manager = FileManager()

    def generate_recheck_file(self):
        logging.info("Начало выполнения задачи: generate_recheck_file")
        try:
            people_for_recheck = self.db_manager.get_people_for_recheck()
            if people_for_recheck:
                today_date = datetime.now().strftime('%d-%m-%Y')
                file_name = f"Запрос на проверку_{today_date}.xlsx"

                data = [
                    {
                        "ФИО": f"{p[1]} {p[2]} {p[3] if p[3] else ''}".strip(),
                        "Дата рождения": p[4].strftime('%d.%m.%Y'),
                        "Срок аккредитации истёк": p[5].strftime('%d.%m.%Y')
                    }
                    for p in people_for_recheck
                ]

                df = pd.DataFrame(data)
                self.file_manager.saveFile(df, file_name)

                for person in people_for_recheck:
                    self.db_manager.log_transaction(person[0], "Generated Recheck File")

                logging.info(f"Файл '{file_name}' успешно создан.")
            else:
                logging.info("Нет данных для проверки.")
        except Exception as e:
            logging.error(f"Ошибка во время выполнения задачи: {e}")

    def start(self):
        logging.info("Запуск планировщика.")
        self.scheduler.add_job(self.generate_recheck_file, "cron", day_of_week="tue", hour=10)
        self.scheduler.start()

    def stop(self):
        logging.info("Остановка планировщика.")
        self.scheduler.shutdown()

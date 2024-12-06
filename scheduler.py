from apscheduler.schedulers.background import BackgroundScheduler
from file_manager import FileManager
from database_manager import DatabaseManager
import pandas as pd


class Scheduler:
    def __init__(self, db_config):
        self.scheduler = BackgroundScheduler()
        self.db_manager = DatabaseManager(**db_config)
        self.file_manager = FileManager()

    def generate_recheck_file(self):
        people_for_recheck = self.db_manager.get_people_for_recheck()
        if people_for_recheck:
            data = [{"ФИО": p[1], "Дата рождения": p[2], "Срок аккредитации истёк": p[3].strftime('%d.%m.%Y')} for p in people_for_recheck]
            df = pd.DataFrame(data)

            # Генерация файла
            self.file_manager.save_generated_file(df, "Повторная проверка")
            for person in people_for_recheck:
                self.db_manager.log_transaction(person[0], "Generated Recheck File")

    def start(self):
        self.scheduler.add_job(self.generate_recheck_file, "cron", day_of_week="tue,thu", hour=10)
        self.scheduler.start()

    def stop(self):
        self.scheduler.shutdown()

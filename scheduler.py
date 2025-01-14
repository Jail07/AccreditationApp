import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, date, timedelta
import traceback

from database_manager import DatabaseManager
from file_manager import FileManager


class Scheduler:
    def __init__(self, db_config):
        self.scheduler = BackgroundScheduler(timezone="Asia/Bishkek")
        self.db_manager = DatabaseManager(**db_config)
        self.file_manager = FileManager()

    def generate_recheck_file(self):
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

    def check_accreditation_expiry(self):
        try:
            print(f"[INFO] Сотруднии проверены на срок аккредитации.")
            expired_employees = self.db_manager.get_expired_accreditations()
            print(expired_employees)

            for employee in expired_employees:
                self.db_manager.update_accreditation_status(employee[0], "не активен")
                self.db_manager.add_to_td(employee)
                self.db_manager.log_transaction(employee[0], "Перенесён в TD из-за истечения срока аккредитации")
                print(f"[INFO] Сотрудник {employee[1]} {employee[2]} {employee[3] if employee[3] else ''} перенесён в TD.")
            print("закончилась")
        except Exception as e:
            print(f"[ERROR] Ошибка проверки срока аккредитации: {e}\n{traceback.format_exc()}")

    def transfer_from_td_to_accrtable(self):
        try:
            employees = self.db_manager.get_all_from_td_full()
            print(employees)

            for employee in employees:
                self.db_manager.add_to_accrtable(employee, status="в ожидании")
                self.db_manager.clean_td()
                self.db_manager.log_transaction(employee['id'], "Добавлен в accrtable со статусом 'в ожидании'")
            print("[INFO] Все сотрудники из TD перенесены в accrtable.")
        except Exception as e:
            print(f"[ERROR] Ошибка при переносе сотрудников из TD в accrtable: {e}\n{traceback.format_exc()}")

    def start(self):
        try:
            print("[DEBUG] Запуск планировщика.")
            self.scheduler.add_job(self.check_accreditation_expiry, "cron", hour=0)
            self.scheduler.add_job(self.generate_recheck_file, "cron", day_of_week="thu", hour=10)
            self.scheduler.add_job(self.transfer_from_td_to_accrtable, "cron", day_of_week="thu", hour=13)
            self.scheduler.start()
            print("[DEBUG] Планировщик успешно запущен. Задачи добавлены.")
        except Exception as e:
            print(f"[ERROR] Ошибка при запуске планировщика: {e}\n{traceback.format_exc()}")

    def stop(self):
        try:
            self.scheduler.shutdown()
            print("[DEBUG] Планировщик успешно остановлен.")
        except Exception as e:
            print(f"[ERROR] Ошибка при остановке планировщика: {e}\n{traceback.format_exc()}")

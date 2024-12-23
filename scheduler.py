import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, date, timedelta
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
        Генерация двух файлов проверки: для ГПХ и для остальных организаций с полной информацией.
        """
        try:
            print("[INFO] Генерация файлов проверки началась.")

            # Получаем полную информацию о сотрудниках для проверки
            people_for_recheck = self.db_manager.get_people_for_recheck_full()

            if people_for_recheck:
                today_date = datetime.now().strftime('%d-%m-%Y')

                # Разделяем данные по организациям
                gph_data = [p for p in people_for_recheck if "ГПХ" in (p['organization'] or "").upper()]
                other_data = [p for p in people_for_recheck if "ГПХ" not in (p['organization'] or "").upper()]

                # Генерация файла для ГПХ
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

                # Генерация файла для остальных организаций
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
        """
        Проверяет срок аккредитации сотрудников каждый день.
        Если срок истёк, статус меняется на 'не активен', а запись переносится в TD.
        """
        try:
            print(f"[INFO] Сотруднии проверены на срок аккредитации.")
            expired_employees = self.db_manager.get_expired_accreditations()

            for employee in expired_employees:
                self.db_manager.update_accreditation_status(employee['id'], "не активен")
                self.db_manager.add_to_td(employee)
                self.db_manager.log_transaction(employee['id'], "Перенесён в TD из-за истечения срока аккредитации")
                print(f"[INFO] Сотрудник {employee['surname']} перенесён в TD.")
        except Exception as e:
            print(f"[ERROR] Ошибка проверки срока аккредитации: {e}\n{traceback.format_exc()}")

    def transfer_from_td_to_accrtable(self):
        """
        Переносит сотрудников из TD в accrtable со статусом 'в ожидании'.
        """
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
        """
        Запуск планировщика.
        """
        try:
            print("[DEBUG] Запуск планировщика.")
            self.scheduler.add_job(self.check_accreditation_expiry, "cron", hour=22, minute=23)
            self.scheduler.add_job(self.generate_recheck_file, "cron", day_of_week="mon", hour=23, minute=21)
            self.scheduler.add_job(self.transfer_from_td_to_accrtable, "cron", day_of_week="mon", hour=23, minute=22)
            self.scheduler.start()
            print("[DEBUG] Планировщик успешно запущен. Задачи добавлены.")
        except Exception as e:
            print(f"[ERROR] Ошибка при запуске планировщика: {e}\n{traceback.format_exc()}")

    def stop(self):
        """
        Останавливает планировщик.
        """
        try:
            self.scheduler.shutdown()
            print("[DEBUG] Планировщик успешно остановлен.")
        except Exception as e:
            print(f"[ERROR] Ошибка при остановке планировщика: {e}\n{traceback.format_exc()}")

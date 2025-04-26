# scheduler.py
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, date, timedelta
import traceback
import time

from config import get_logger # Используем настроенный логгер
from database_manager import DatabaseManager
from file_manager import FileManager


class Scheduler:
    def __init__(self, db_config):
        self.logger = get_logger(__name__)
        self.scheduler = BackgroundScheduler(timezone="Europe/Moscow")
        # Создаем свой экземпляр DB Manager для планировщика
        try:
             self.db_manager = DatabaseManager(db_config)
             # Используем отдельный FileManager, не связанный с GUI
             self.file_manager = FileManager()
             self.logger.info("Планировщик инициализирован с собственным DBManager.")
        except Exception as e:
             self.logger.exception("Критическая ошибка инициализации DBManager в планировщике!")
             # Не запускаем планировщик, если БД недоступна
             self.scheduler = None


    def _run_job(self, job_func, job_name):
        """Обертка для безопасного запуска задач планировщика."""
        self.logger.info(f"Запуск задачи планировщика: {job_name}")
        try:
            job_func()
            self.logger.info(f"Задача планировщика '{job_name}' успешно завершена.")
        except Exception as e:
            self.logger.exception(f"Ошибка выполнения задачи планировщика '{job_name}': {e}")

    def check_accreditation_expiry_job(self):
        """Задача: Проверяет истекшие аккредитации."""
        count = self.db_manager.check_accreditation_expiry()
        self.logger.info(f"Проверка истекших аккредитаций завершена. Обновлено статусов: {count}")

    def generate_recheck_files_job(self):
        """Задача: Генерирует файлы для повторной проверки (для статуса 'в ожидании')."""
        self.logger.info("Генерация файлов для повторной проверки началась.")

        # Получаем ID сотрудников со статусом 'в ожидании'
        person_ids_gph = self.db_manager.get_people_for_recheck(only_gph=True)
        person_ids_contractor = self.db_manager.get_people_for_recheck(only_gph=False)

        # Получаем детали для этих сотрудников
        people_details_gph = self.db_manager.get_people_details(person_ids_gph)
        people_details_contractor = self.db_manager.get_people_details(person_ids_contractor)

        # Формируем DataFrame и сохраняем файлы
        if people_details_gph:
            gph_df = pd.DataFrame(people_details_gph)
            # Выбираем нужные колонки для отчета
            gph_df_report = gph_df[['surname', 'name', 'middle_name', 'birth_date', 'organization', 'position']].fillna('')
            gph_df_report.columns = ['Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Организация', 'Должность']
            gph_df_report['Дата рождения'] = pd.to_datetime(gph_df_report['Дата рождения']).dt.strftime('%d.%m.%Y')
            self.file_manager.generate_file_scheduler(gph_df_report, "Запрос_на_проверку_ГПХ")
        else:
            self.logger.info("Нет сотрудников ГПХ со статусом 'в ожидании' для генерации файла.")

        if people_details_contractor:
            contractor_df = pd.DataFrame(people_details_contractor)
            contractor_df_report = contractor_df[['surname', 'name', 'middle_name', 'birth_date', 'organization', 'position']].fillna('')
            contractor_df_report.columns = ['Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Организация', 'Должность']
            contractor_df_report['Дата рождения'] = pd.to_datetime(contractor_df_report['Дата рождения']).dt.strftime('%d.%m.%Y')
            self.file_manager.generate_file_scheduler(contractor_df_report, "Запрос_на_проверку_Подрядчики")
        else:
             self.logger.info("Нет сотрудников Подрядчиков со статусом 'в ожидании' для генерации файла.")


    def transfer_from_td_to_accrtable_job(self):
        """Задача: Переносит данные из временной таблицы TD в основную AccrTable."""
        self.logger.info("Начало переноса сотрудников из TD в AccrTable.")
        employees_in_td = self.db_manager.get_all_from_td_full()

        if not employees_in_td:
            self.logger.info("Временная таблица TD пуста. Перенос не требуется.")
            return

        added_count = 0
        failed_count = 0
        for employee_data in employees_in_td:
            # Преобразуем RealDictRow в обычный dict для передачи в add_to_accrtable
            data_dict = dict(employee_data)
            # Статус берем из TD (установлен при проверке в UI) или 'в ожидании' по умолчанию
            status = data_dict.get('status', 'в ожидании')
            person_id = self.db_manager.add_to_accrtable(data_dict, status=status)
            if person_id:
                added_count += 1
                # Логируем перенос внутри add_to_accrtable или здесь
                # self.db_manager.log_transaction(person_id, 'Перенесен из TD', f'Статус при переносе: {status}')
            else:
                failed_count += 1
                self.logger.error(f"Не удалось перенести сотрудника из TD: {data_dict.get('surname')} {data_dict.get('name')}")

        self.logger.info(f"Перенос из TD завершен. Успешно: {added_count}, Ошибок: {failed_count}.")

        # Очищаем TD только если перенос был успешным (или по другой логике)
        if failed_count == 0 and added_count > 0:
            self.db_manager.clean_td()
        elif failed_count > 0:
            self.logger.warning("Временная таблица TD не была очищена из-за ошибок при переносе.")


    def start(self):
        """Добавляет задачи и запускает планировщик."""
        if not self.scheduler:
             self.logger.error("Планировщик не инициализирован (возможно, ошибка БД). Запуск отменен.")
             return

        try:
            self.logger.info("Добавление задач в планировщик...")
            # Проверка истекших аккредитаций - каждый день в полночь
            self.scheduler.add_job(
                lambda: self._run_job(self.check_accreditation_expiry_job, "Check Expiry"),
                "cron", hour=0, minute=5, id="check_expiry"
            )
            # Генерация файлов на проверку - каждый четверг в 10:00
            self.scheduler.add_job(
                lambda: self._run_job(self.generate_recheck_files_job, "Generate Recheck Files"),
                "cron", day_of_week="thu", hour=10, minute=0, id="generate_recheck"
            )
            # Перенос из TD в AccrTable - каждый четверг в 13:00
            self.scheduler.add_job(
                 lambda: self._run_job(self.transfer_from_td_to_accrtable_job, "Transfer TD to AccrTable"),
                 "cron", day_of_week="thu", hour=13, minute=0, id="transfer_td"
            )

            self.scheduler.start()
            self.logger.info("Планировщик успешно запущен с задачами.")

        except Exception as e:
            self.logger.exception(f"Критическая ошибка при запуске планировщика или добавлении задач: {e}")

    def stop(self):
        """Останавливает планировщик."""
        if self.scheduler and self.scheduler.running:
            self.logger.info("Остановка планировщика...")
            self.scheduler.shutdown()
            self.logger.info("Планировщик остановлен.")
        # Закрываем пул соединений БД планировщика
        if hasattr(self, 'db_manager') and self.db_manager:
             self.db_manager.close_pool()
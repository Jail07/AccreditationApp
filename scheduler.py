# scheduler.py
import os

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, date, timedelta
import traceback
import time

from config import get_logger, get_scheduler_output_dir, get_schedule_config
from database_manager import DatabaseManager
from file_manager import FileManager


class Scheduler:
    def __init__(self, db_config):
        self.logger = get_logger(__name__)
        self.scheduler = BackgroundScheduler(timezone="Europe/Moscow")
        self.output_dir = get_scheduler_output_dir()
        os.makedirs(self.output_dir, exist_ok=True)  # Создаем папку, если ее нет
        # Создаем свой экземпляр DB Manager для планировщика
        try:
             self.db_manager = DatabaseManager(db_config)
             self.file_manager = FileManager()
             self.logger.info("Планировщик инициализирован с собственным DBManager.")
        except Exception as e:
             self.logger.exception("Критическая ошибка инициализации DBManager в планировщике!")
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
            # --- ДОБАВЛЕНО: Столбец для проверки ---
            gph_df_report['Результат проверки'] = ''
            gph_df_report['Примечания проверяющего'] = ''
            # ------------------------------------
            self.file_manager.generate_file_scheduler(gph_df_report, "Запрос_на_проверку_ГПХ")
        else:
            self.logger.info("Нет сотрудников ГПХ со статусом 'в ожидании' для генерации файла.")

        if people_details_contractor:
            contractor_df = pd.DataFrame(people_details_contractor)
            contractor_df_report = contractor_df[['surname', 'name', 'middle_name', 'birth_date', 'organization', 'position']].fillna('')
            contractor_df_report.columns = ['Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Организация', 'Должность']
            contractor_df_report['Дата рождения'] = pd.to_datetime(contractor_df_report['Дата рождения']).dt.strftime('%d.%m.%Y')
            # --- ДОБАВЛЕНО: Столбец для проверки ---
            contractor_df_report['Результат проверки'] = ''
            contractor_df_report['Примечания проверяющего'] = ''
            # ------------------------------------
            self.file_manager.generate_file_scheduler(contractor_df_report, "Запрос_на_проверку_Подрядчики")
        else:
             self.logger.info("Нет сотрудников Подрядчиков со статусом 'в ожидании' для генерации файла.")


    def generate_weekly_check_file_job(self):
        """
        Задача: Собирает данные из TD, сохраняет в файл,
        ДОБАВЛЯЕТ их в AccrTable со статусом 'в ожидании' и очищает TD.
        """
        self.logger.info("Начало еженедельной выгрузки данных из TD и добавления в AccrTable.")
        employees_in_td = self.db_manager.get_all_from_td_full()

        if not employees_in_td:
            self.logger.info("Временная таблица TD пуста. Операции не требуются.")
            return

        df_to_check = pd.DataFrame(employees_in_td)
        self.logger.info(f"Собрано {len(df_to_check)} записей из TD.")

        # --- Шаг 1: Генерация файла ---
        # ... (код генерации df_report как в предыдущем исправлении) ...
        cols_to_keep = ['surname', 'name', 'middle_name', 'birth_date', 'birth_place',
                        'registration', 'organization', 'position', 'notes', 'status'] # Добавили notes
        df_report = df_to_check[[col for col in cols_to_keep if col in df_to_check.columns]].copy()
        rename_map = {
             'surname': 'Фамилия', 'name': 'Имя', 'middle_name': 'Отчество',
             'birth_date': 'Дата рождения', 'birth_place': 'Место рождения',
             'registration': 'Адрес регистрации', 'organization': 'Организация',
             'position': 'Должность', 'notes': 'Примечания', # Добавили notes
             'status': 'Статус проверки (из файла)'
        }
        df_report.rename(columns=rename_map, inplace=True)
        if 'Дата рождения' in df_report.columns:
             df_report['Дата рождения'] = pd.to_datetime(df_report['Дата рождения']).dt.strftime('%d.%m.%Y')

        # --- ДОБАВЛЕНО: Столбец для проверки ---
        df_report['Результат проверки'] = ''
        df_report['Примечания проверяющего'] = ''
        # ------------------------------------

        filename_prefix = "Еженедельный_список_на_проверку"
        saved_path = self.file_manager.generate_file_scheduler(df_report, filename_prefix)

        if not saved_path:
            self.logger.error("Не удалось сохранить еженедельный файл 'На проверку'. Перенос в AccrTable и очистка TD НЕ будут выполнены.")
            return # Прерываем операцию, если файл не сохранен

        self.logger.info(f"Еженедельный файл 'На проверку' сохранен: {saved_path}")

        # --- Шаг 2: Добавление в AccrTable ---
        added_to_accr_count = 0
        failed_on_accr_count = 0
        self.logger.info(f"Начало добавления {len(employees_in_td)} записей из TD в AccrTable со статусом 'в ожидании'...")

        for employee_data_row in employees_in_td:
             # Преобразуем RealDictRow в dict
             data_dict = dict(employee_data_row)
             # Используем ключи как в DataFrame для add_to_accrtable
             # Переименовываем ключи (если они не совпадают с ожидаемыми в add_to_accrtable)
             # Если add_to_accrtable ожидает 'Фамилия', 'Имя', то используем их
             data_for_accr = {
                 'Фамилия': data_dict.get('surname'),
                 'Имя': data_dict.get('name'),
                 'Отчество': data_dict.get('middle_name'),
                 'Дата рождения': data_dict.get('birth_date'),
                 'Место рождения': data_dict.get('birth_place'),
                 'Регистрация': data_dict.get('registration'),
                 'Организация': data_dict.get('organization'),
                 'Должность': data_dict.get('position'),
                 'Примечания': data_dict.get('notes') # Передаем примечания
             }
             # --- ПРОВЕРКА ПЕРЕД ДОБАВЛЕНИЕМ ---
             existing_person_id = self.db_manager.find_person_in_accrtable(
                 data_for_accr.get('Фамилия'),
                 data_for_accr.get('Имя'),
                 data_for_accr.get('Отчество'),
                 data_for_accr.get('Дата рождения')
             )
             if existing_person_id:
                 self.logger.info(
                     f"Сотрудник {data_for_accr.get('Фамилия')} {data_for_accr.get('Имя')} уже существует в AccrTable (ID: {existing_person_id}). Пропуск добавления.")
                 # Опционально: обновить существующую запись? Или добавить примечание?
                 # self.db_manager.update_notes(existing_person_id, data_for_accr.get('Примечания', ''))
                 continue  # Переходим к сле

             person_id = self.db_manager.add_to_accrtable(data_for_accr, status='в ожидании')
             if person_id:
                 added_to_accr_count += 1
             else:
                 failed_on_accr_count += 1
                 self.logger.error(f"Не удалось добавить в AccrTable: {data_for_accr.get('Фамилия')} {data_for_accr.get('Имя')}")

        self.logger.info(f"Добавление в AccrTable завершено. Успешно: {added_to_accr_count}, Ошибок: {failed_on_accr_count}.")

        # --- Шаг 3: Очистка TD ---
        # Очищаем TD, даже если были ошибки добавления в AccrTable, т.к. файл уже создан
        # (или изменить логику, если нужно гарантировать перенос)
        if failed_on_accr_count > 0:
             self.logger.warning("Были ошибки при добавлении записей в AccrTable.")

        cleaned = self.db_manager.clean_td()
        if cleaned:
            self.logger.info("Временная таблица TD успешно очищена после еженедельной обработки.")
        else:
            self.logger.error("Не удалось очистить временную таблицу TD после еженедельной обработки!")

    def start(self):
        """Добавляет задачи и запускает планировщик, используя настройки из .env."""
        if not self.scheduler:
            self.logger.error("Планировщик не инициализирован (возможно, ошибка БД). Запуск отменен.")
            return

        try:
            self.logger.info("Добавление задач в планировщик с настройками из .env...")

            # Задача проверки истекших аккредитаций (ежедневно)
            expiry_conf = get_schedule_config('EXPIRY', default_hour=0, default_minute=5)
            self.scheduler.add_job(
                lambda: self._run_job(self.check_accreditation_expiry_job, "Check Expiry"),
                "cron",
                hour=expiry_conf['hour'],
                minute=expiry_conf['minute'],
                id="check_expiry",
                replace_existing=True
            )

            # Задача генерации файлов для ПОВТОРНОЙ проверки
            recheck_conf = get_schedule_config('RECHECK', default_day_of_week="thu", default_hour=10, default_minute=0)
            self.scheduler.add_job(
                lambda: self._run_job(self.generate_recheck_files_job, "Generate Recheck Files"),
                "cron",
                day_of_week=recheck_conf['day_of_week'],
                hour=recheck_conf['hour'],
                minute=recheck_conf['minute'],
                id="generate_recheck",
                replace_existing=True
            )

            # Задача еженедельной выгрузки из TD
            weekly_td_conf = get_schedule_config('WEEKLY_TD', default_day_of_week="fri", default_hour=18, default_minute=0)
            self.scheduler.add_job(
                lambda: self._run_job(self.generate_weekly_check_file_job, "Generate Weekly Check File from TD"),
                "cron",
                day_of_week=weekly_td_conf['day_of_week'],
                hour=weekly_td_conf['hour'],
                minute=weekly_td_conf['minute'],
                id="generate_weekly_td",
                replace_existing=True
            )

            self.scheduler.start()
            self.logger.info("Планировщик успешно запущен с задачами, настроенными через .env.")

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
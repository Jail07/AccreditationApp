# database_manager.py
import pandas as pd
import psycopg2
import psycopg2.pool
import psycopg2.extras # Для RealDictCursor
from datetime import datetime, date, timedelta
import pytz
import logging # Используем стандартное логирование
from config import get_logger # Импортируем настроенный логгер

class DatabaseManager:
    _pool = None # Пул соединений будет инициализирован один раз

    def __init__(self, db_config, min_conn=1, max_conn=5):
        self.logger = get_logger(__name__)
        self.timezone = pytz.timezone("Europe/Moscow")

        # Инициализация пула соединений, если он еще не создан
        if DatabaseManager._pool is None:
            try:
                DatabaseManager._pool = psycopg2.pool.SimpleConnectionPool(
                    min_conn,
                    max_conn,
                    **db_config
                )
                self.logger.info(f"Пул соединений PostgreSQL инициализирован для {db_config.get('database')}@{db_config.get('host')}")
            except psycopg2.OperationalError as e:
                self.logger.exception("Ошибка инициализации пула соединений PostgreSQL!")
                DatabaseManager._pool = None # Сбрасываем пул в случае ошибки
                raise # Передаем исключение дальше

        # Проверка пула при создании экземпляра
        if DatabaseManager._pool is None:
             raise ConnectionError("Не удалось инициализировать пул соединений.")

        self.create_tables() # Проверяем/создаем таблицы при инициализации

    def _get_connection(self):
        """Получает соединение из пула."""
        if self._pool:
            return self._pool.getconn()
        else:
            self.logger.error("Пул соединений не инициализирован.")
            raise ConnectionError("Пул соединений недоступен.")

    def _release_connection(self, conn):
        """Возвращает соединение в пул."""
        if self._pool:
            self._pool.putconn(conn)

    def execute_query(self, query, params=None, fetch=None, commit=False):
        """
        Выполняет SQL-запрос с использованием соединения из пула.
        Теперь ожидает ЛИБО tuple (для %s), ЛИБО dict (для %(key)s).
        """
        conn = None
        result = None
        # Проверяем, что params - это словарь или кортеж/список (или None)
        if params is not None and not isinstance(params, (dict, tuple, list)):
            self.logger.error(
                f"Неверный тип параметров для execute_query: {type(params)}. Ожидался dict, tuple или list.")
            # Можно поднять исключение или вернуть None
            # raise TypeError("Параметры для execute_query должны быть dict, tuple или list")
            return None

        try:
            conn = self._get_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params)  # Передаем params напрямую
                if fetch == 'one':
                    result = cursor.fetchone()
                elif fetch == 'all':
                    result = cursor.fetchall()

                if commit:
                    conn.commit()
                log_query = query.strip().split('\n', 1)[0]
                self.logger.debug(f"Запрос выполнен успешно: {log_query[:150]}...")
            return result
        except psycopg2.Error as e:
            if conn and commit:
                try:
                    conn.rollback()
                    self.logger.warning(f"Транзакция отменена из-за ошибки: {e}")
                except psycopg2.Error as rb_e:
                    self.logger.error(f"Ошибка при откате транзакции: {rb_e}")
            # Логируем ошибку типа параметров отдельно, если это она
            if isinstance(e, psycopg2.ProgrammingError) and "not all arguments converted" in str(e):
                self.logger.error(
                    f"Ошибка несоответствия параметров psycopg2: {e}\nЗапрос: {query}\nПараметры ({type(params)}): {params}")
            else:
                self.logger.error(
                    f"Ошибка выполнения SQL запроса: {e}\nЗапрос: {query}\nПараметры ({type(params)}): {params}")
            return None
        except Exception as e:
            self.logger.exception(f"Неожиданная ошибка при выполнении запроса: {e}")
            raise
        finally:
            if conn:
                self._release_connection(conn)

    def create_tables(self):
        """Создает необходимые таблицы, если они не существуют."""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS AccrTable (
                id SERIAL PRIMARY KEY,
                surname TEXT NOT NULL,
                name TEXT NOT NULL,
                middle_name TEXT,
                birth_date DATE NOT NULL,
                birth_place TEXT,
                registration TEXT,
                organization TEXT NOT NULL, -- Используется для определения ГПХ/Подрядчик
                position TEXT,
                notes TEXT, -- Добавлено поле для примечаний
                added_date TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- Используем TIMESTAMPTZ
                status TEXT DEFAULT 'в ожидании'
                -- CONSTRAINT unique_person UNIQUE (surname, name, middle_name, birth_date) -- Убрано ограничение, т.к. могут быть ГПХ/Подрядчики
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS mainTable (
                id SERIAL PRIMARY KEY,
                person_id INT NOT NULL, -- REFERENCES AccrTable(id) ON DELETE CASCADE, -- Пока уберем CASCADE
                start_accr TIMESTAMPTZ, -- Используем TIMESTAMPTZ
                end_accr TIMESTAMPTZ,   -- Используем TIMESTAMPTZ
                black_list BOOLEAN DEFAULT FALSE,
                last_checked TIMESTAMPTZ DEFAULT NOW(),
                FOREIGN KEY (person_id) REFERENCES AccrTable(id) ON DELETE RESTRICT -- Запрещаем удаление AccrTable, если есть связи
            );
            """,
             """
            CREATE TABLE IF NOT EXISTS TD ( -- Временная таблица
                id SERIAL PRIMARY KEY,
                surname TEXT NOT NULL,
                name TEXT NOT NULL,
                middle_name TEXT,
                birth_date DATE NOT NULL,
                birth_place TEXT,
                registration TEXT,
                organization TEXT NOT NULL,
                position TEXT,
                notes TEXT, -- <--- ДОБАВЛЕНО ПОЛЕ ДЛЯ ПРИМЕЧАНИЙ
                status TEXT, -- Статус из UI после первичной проверки
                load_timestamp TIMESTAMPTZ DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS Records (
                id SERIAL PRIMARY KEY,
                person_id INT, -- REFERENCES AccrTable(id) ON DELETE SET NULL, -- Разрешаем удаление AccrTable
                operation_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                operation_type TEXT NOT NULL,
                details TEXT,
                FOREIGN KEY (person_id) REFERENCES AccrTable(id) ON DELETE SET NULL
            );
            """,
            # Индексы для ускорения поиска
            "CREATE INDEX IF NOT EXISTS idx_accrtable_names_dob ON AccrTable (surname, name, birth_date);",
            "CREATE INDEX IF NOT EXISTS idx_maintable_person_id ON mainTable (person_id);",
            "CREATE INDEX IF NOT EXISTS idx_maintable_end_accr ON mainTable (end_accr);",
            "CREATE INDEX IF NOT EXISTS idx_maintable_blacklist ON mainTable (black_list);",
            "CREATE INDEX IF NOT EXISTS idx_td_names_dob ON TD (surname, name, birth_date);"
        ]
        for query in queries:
             # Используем commit=True, так как CREATE TABLE требует этого вне транзакции
             result = self.execute_query(query, commit=True)
             # execute_query вернет None при успехе DDL или ошибке
             if result is not None: # Обычно None при успехе DDL
                 self.logger.info(f"Успешно выполнен DDL: {query.split()[0]} {query.split()[1]}...")
             # Об ошибках сообщит сам execute_query

        self.logger.info("Проверка и создание таблиц завершено.")

    def log_transaction(self, person_id, operation_type, details=""):
        """Логирует операцию в таблицу Records."""
        # Этот метод уже есть и должен использоваться всеми операциями изменения данных.
        # Убедитесь, что он вызывается в add_to_accrtable, activate_person_by_details, toggle_blacklist и т.д.
        query = """
           INSERT INTO Records (person_id, operation_type, details, operation_date)
           VALUES (%s, %s, %s, %s);
           """
        now_tz = datetime.now(self.timezone)
        params = (person_id, operation_type, details, now_tz)
        # Этот commit=True важен
        res = self.execute_query(query, params, commit=True)
        if res is None:  # Успешный INSERT/UPDATE/DELETE с commit=True вернет None
            self.logger.debug(f"Транзакция для person_id={person_id}, тип='{operation_type}' успешно залогирована.")
        # Об ошибке сообщит execute_query

    def add_to_td(self, data):
        """Добавляет запись во временную таблицу TD, включая примечания."""
        required_fields_from_data = ['Фамилия', 'Имя', 'Дата рождения', 'Организация'] # Ключи как в data
        if not all(field in data and pd.notna(data[field]) for field in required_fields_from_data):
            self.logger.warning(f"Пропущена запись в TD из-за отсутствия обязательных полей в data: {data}")
            return None

        # --- ИЗМЕНЕНИЕ: Переход на %s плейсхолдеры ---
        query = """
        INSERT INTO TD (surname, name, middle_name, birth_date, birth_place,
                       registration, organization, position, notes,
                       status, load_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        now_tz = datetime.now(self.timezone)
        notes_value = data.get('Примечания') # Ключ из data
        notes_str = str(notes_value) if notes_value is not None else ''
        # --------------------------------------------------
        # Формируем кортеж параметров в правильном порядке
        params_tuple = (
            data.get('Фамилия'),
            data.get('Имя'),
            data.get('Отчество'),
            data.get('Дата рождения'),
            data.get('Место рождения'),
            data.get('Регистрация'),
            data.get('Организация'),
            data.get('Должность'),
            notes_str,
            data.get('status', data.get('Статус Проверки', 'На проверку')),
            now_tz
        )


        result = self.execute_query(query, params_tuple, fetch='one', commit=True)

        if result:
            self.logger.info(f"Запись добавлена в TD: {data.get('Фамилия')} {data.get('Имя')}, ID: {result['id']}")
            return result['id']
        else:
            self.logger.error(f"Не удалось добавить запись в TD: {data.get('Фамилия')} {data.get('Имя')}")
            return None

    def find_person_in_accrtable(self, surname, name, middle_name, birth_date):
        """Ищет человека в AccrTable. Возвращает ID или None."""
        if not surname or not name or not birth_date:
             self.logger.warning("Попытка поиска в AccrTable без ФИО или даты рождения.")
             return None # Не можем искать без основных данных

        # Стандартизация middle_name: None и '' считаем эквивалентными
        middle_name_param = middle_name if middle_name else '' # Используем '' для запроса

        query = """
        SELECT id FROM AccrTable
        WHERE surname = %s AND name = %s AND birth_date = %s
        AND COALESCE(middle_name, '') = %s -- Используем COALESCE для сравнения None и ''
        ORDER BY id DESC LIMIT 1;
        """
        params = (surname, name, birth_date, middle_name_param)

        result = self.execute_query(query, params, fetch='one')
        found_id = result['id'] if result else None
        if found_id:
             self.logger.debug(f"Найден person_id={found_id} для {surname} {name} {middle_name_param} {birth_date}")
        else:
             self.logger.debug(f"Не найден person_id для {surname} {name} {middle_name_param} {birth_date}")
        return found_id

    def find_person_in_td(self, surname, name, middle_name, birth_date):
        """Ищет человека в TD. Возвращает ID или None."""
        if not surname or not name or not birth_date:
             self.logger.warning("Попытка поиска в AccrTable без ФИО или даты рождения.")
             return None # Не можем искать без основных данных

        # Стандартизация middle_name: None и '' считаем эквивалентными
        middle_name_param = middle_name if middle_name else '' # Используем '' для запроса

        query = """
        SELECT id FROM TD
        WHERE surname = %s AND name = %s AND birth_date = %s
        AND COALESCE(middle_name, '') = %s -- Используем COALESCE для сравнения None и ''
        ORDER BY id DESC LIMIT 1;
        """
        params = (surname, name, birth_date, middle_name_param)

        result = self.execute_query(query, params, fetch='one')
        found_id = result['id'] if result else None
        if found_id:
             self.logger.debug(f"Найден person_id={found_id} для {surname} {name} {middle_name_param} {birth_date}")
        else:
             self.logger.debug(f"Не найден person_id для {surname} {name} {middle_name_param} {birth_date}")
        return found_id

    def get_person_status(self, surname, name, middle_name, birth_date):
        """
        Проверяет статус человека в mainTable (активность, черный список).
        Возвращает словарь {'status': 'BLACKLISTED'|'ACTIVE'|'EXPIRED'|'NOT_FOUND', 'person_id': id | None}.
        """
        person_id = self.find_person_in_accrtable(surname, name, middle_name, birth_date)
        if not person_id:
            return {'status': 'NOT_FOUND', 'person_id': None}
        person_id = self.find_person_in_td(surname, name, middle_name, birth_date)
        if not person_id:
            return {'status': 'NOT_FOUND', 'person_id': None}

        now_tz = datetime.now(self.timezone)
        query = """
        SELECT black_list, end_accr FROM mainTable
        WHERE person_id = %s
        ORDER BY id DESC -- Берем самую последнюю запись статуса для этого человека
        LIMIT 1;
        """
        params = (person_id,)
        result = self.execute_query(query, params, fetch='one')

        if result:
            if result['black_list']:
                return {'status': 'BLACKLISTED', 'person_id': person_id}
            elif result['end_accr'] and result['end_accr'] > now_tz:
                 # Проверяем, что end_accr не NULL и больше текущего времени
                return {'status': 'ACTIVE', 'person_id': person_id}
            elif result['end_accr'] and result['end_accr'] < now_tz:
                # Статус есть, но он не черный список и аккредитация истекла или не была установлена
                 return {'status': 'EXPIRED', 'person_id': person_id}
            else:
                return {'status': 'CHECKING', 'person_id': person_id}
        else:
            # Человек есть в AccrTable, но нет записей в mainTable (например, только добавлен)
            return {'status': 'NOT_FOUND', 'person_id': person_id} # Считаем, что статус не найден

    def add_to_accrtable(self, data, status='в ожидании'):
        """
        Добавляет запись в AccrTable, если её там нет.
        Возвращает ID существующей или новой записи.
        Ожидает словарь `data` с ключами как в DataFrame (Фамилия, Имя и т.д.)
        """
        now_tz = datetime.now(self.timezone)
        try:
            params = {
                'surname': data.get('Фамилия'),
                'name': data.get('Имя'),
                'middle_name': data.get('Отчество'),
                'birth_date': data.get('Дата рождения'),
                'birth_place': data.get('Место рождения'),
                'registration': data.get('Регистрация'),
                'organization': data.get('Организация'),
                'position': data.get('Должность'),
                'notes': data.get('Примечания'),
                'status': status,  # Статус передается как аргумент функции
                'added_date': now_tz  # Добавляем дату добавления
            }
        except Exception as e:
            pass
        person_id = self.find_person_in_accrtable(
            data.get('surname'), data.get('name'), data.get('middle_name'), data.get('birth_date')
        )
        if person_id:
            self.logger.info(f"Человек {data.get('surname')} {data.get('name')} уже существует в AccrTable (ID: {person_id}).")
            # Опционально: Обновить данные существующей записи?
            return person_id

        # --- ИЗМЕНЕНИЕ ЗАПРОСА И ПАРАМЕТРОВ ---
        query = """
        INSERT INTO AccrTable (surname, name, middle_name, birth_date, birth_place, registration, organization, position, notes, status, added_date)
        VALUES (%(surname)s, %(name)s, %(middle_name)s, %(birth_date)s, %(birth_place)s, %(registration)s, %(organization)s, %(position)s, %(notes)s, %(status)s, %(added_date)s)
        RETURNING id;
        """


        # Передаем ТОЛЬКО СЛОВАРЬ params в execute_query
        result = self.execute_query(query, params, fetch='one', commit=True)
        # --- КОНЕЦ ИЗМЕНЕНИЯ ---

        if result:
            new_id = result['id']
            self.logger.info(f"Человек {params.get('surname')} {params.get('name')} добавлен в AccrTable (ID: {new_id}) со статусом '{status}'.")
            self.log_transaction(new_id, 'Добавлен в AccrTable', f'Статус: {status}')
            self.add_initial_main_record(new_id) # Добавляем запись в mainTable
            return new_id
        else:
            self.logger.error(f"Не удалось добавить человека {params.get('surname')} {params.get('name')} в AccrTable.")
            return None

    def add_initial_main_record(self, person_id):
        """Добавляет начальную запись в mainTable для нового сотрудника."""
        query_check = "SELECT 1 FROM mainTable WHERE person_id = %s"
        exists = self.execute_query(query_check, (person_id,), fetch='one')
        if not exists:
            query_insert = """
            INSERT INTO mainTable (person_id, black_list, last_checked)
            VALUES (%s, FALSE, %s)
            """
            now_tz = datetime.now(self.timezone)
            self.execute_query(query_insert, (person_id, now_tz), commit=True)


    def update_accreditation_status(self, person_id, new_status, start_date=None, days_valid=180):
        """Обновляет статус в AccrTable и добавляет/обновляет запись в mainTable."""
        if not person_id:
            self.logger.warning("Попытка обновить статус без person_id.")
            return False

        now_tz = datetime.now(self.timezone)
        start_accr = start_date.astimezone(self.timezone) if start_date else now_tz
        end_accr = start_accr + timedelta(days=days_valid)

        # 1. Обновляем статус в AccrTable
        query_accr = "UPDATE AccrTable SET status = %s WHERE id = %s"
        self.execute_query(query_accr, (new_status, person_id), commit=True)

        # 2. Добавляем или обновляем запись в mainTable
        # Ищем последнюю запись для person_id
        query_find_main = "SELECT id FROM mainTable WHERE person_id = %s ORDER BY id DESC LIMIT 1"
        main_record = self.execute_query(query_find_main, (person_id,), fetch='one')

        if main_record: # Если запись есть, обновляем её
             query_main = """
             UPDATE mainTable
             SET status = %s, start_accr = %s, end_accr = %s, black_list = FALSE, last_checked = %s
             WHERE id = %s
             """
             params_main = (new_status, start_accr, end_accr, now_tz, main_record['id'])
        else: # Если записи нет, вставляем новую
            query_main = """
            INSERT INTO mainTable (person_id, start_accr, end_accr, black_list, last_checked)
            VALUES (%s, %s, %s, FALSE, %s)
            """
            params_main = (person_id, start_accr, end_accr, now_tz)

        result_main = self.execute_query(query_main, params_main, commit=True)

        if result_main is not None: # commit=True вернет None при успехе
             self.logger.info(f"Статус для person_id={person_id} обновлен на '{new_status}'. Аккредитация до {end_accr.strftime('%Y-%m-%d')}.")
             self.log_transaction(person_id, 'Статус обновлен', f'Новый статус: {new_status}, аккр. до {end_accr.strftime("%Y-%m-%d")}')
             return True
        else:
             self.logger.error(f"Не удалось обновить статус в mainTable для person_id={person_id}.")
             return False

    def toggle_blacklist(self, person_data): # Теперь принимаем словарь с данными
        """
        Переключает статус black_list. Если сотрудник не найден, добавляет его
        в AccrTable и mainTable сразу с black_list=True.
        Если снимается с ЧС и это была единственная причина его нахождения в AccrTable,
        то удаляет из AccrTable/mainTable и добавляет в TD.
        """
        surname = person_data.get('Фамилия')
        name = person_data.get('Имя')
        middle_name = person_data.get('Отчество')
        birth_date = person_data.get('Дата рождения') # Должна быть дата

        if not surname or not name or not birth_date:
            self.logger.warning("toggle_blacklist: Недостаточно данных (ФИО, ДР) для поиска/создания сотрудника.")
            return None, "Недостаточно данных для операции."

        person_id = self.find_person_in_accrtable(surname, name, middle_name, birth_date)
        now_tz = datetime.now(self.timezone)
        action_taken = ""
        original_accr_status_if_exists = None

        if not person_id:
            # --- Сотрудника нет, добавляем сразу в ЧС ---
            self.logger.info(f"toggle_blacklist: Сотрудник {surname} {name} не найден. Добавление в ЧС...")
            # Собираем данные для AccrTable из person_data
            # Ключи в person_data должны соответствовать тем, что приходят из UI
            accr_data_for_new = {
                'Фамилия': surname, 'Имя': name, 'Отчество': middle_name,
                'Дата рождения': birth_date,
                'Место рождения': person_data.get('Место рождения'),
                'Регистрация': person_data.get('Адрес регистрации'), # Или 'Регистрация'
                'Организация': person_data.get('Организация', 'ЧС (не в штате)'), # Указываем источник
                'Должность': person_data.get('Должность'),
                'Примечания': person_data.get('Примечания', 'Добавлен сразу в ЧС')
            }
            # Добавляем в AccrTable со статусом 'отведен'
            person_id = self.add_to_accrtable(accr_data_for_new, status='отведен')
            if not person_id:
                self.logger.error(f"toggle_blacklist: Не удалось добавить нового сотрудника {surname} {name} в AccrTable.")
                return None, "Ошибка добавления нового сотрудника."

            # Запись в mainTable с black_list = TRUE
            query_main = """
            INSERT INTO mainTable (person_id, black_list, last_checked)
            VALUES (%s, TRUE, %s) RETURNING id;
            """
            main_record = self.execute_query(query_main, (person_id, now_tz), fetch='one', commit=True)
            if main_record:
                self.log_transaction(person_id, 'Добавлен в ЧС (новый)', f"Статус Accr: отведен")
                action_taken = "добавлен в черный список (новый)"
                return action_taken, f"Сотрудник {surname} {name} добавлен и помещен в ЧС."
            else:
                self.logger.error(f"toggle_blacklist: Не удалось создать запись в mainTable для нового ЧС сотрудника ID {person_id}.")
                # По-хорошему, откатить добавление в AccrTable, но это усложнит
                return None, "Ошибка создания записи о ЧС."
        else:
            # --- Сотрудник существует, переключаем статус ЧС ---
            # Сохраняем текущий статус AccrTable перед изменением
            current_accr_q = "SELECT status FROM AccrTable WHERE id = %s"
            current_accr_res = self.execute_query(current_accr_q, (person_id,), fetch='one')
            if current_accr_res:
                original_accr_status_if_exists = current_accr_res['status']


            query_get_main = "SELECT id, black_list FROM mainTable WHERE person_id = %s ORDER BY id DESC LIMIT 1"
            main_state = self.execute_query(query_get_main, (person_id,), fetch='one')

            if not main_state: # Если есть в AccrTable, но нет в mainTable (маловероятно после add_initial_main_record)
                 self.add_initial_main_record(person_id) # Создаем запись
                 main_state = {'id': self.execute_query("SELECT id FROM mainTable WHERE person_id = %s ORDER BY id DESC LIMIT 1", (person_id,), fetch='one')['id'], 'black_list': False}


            new_blacklist_status = not main_state['black_list']
            main_table_id = main_state['id']

            query_update_main = "UPDATE mainTable SET black_list = %s, last_checked = %s WHERE id = %s"
            res_main = self.execute_query(query_update_main, (new_blacklist_status, now_tz, main_table_id), commit=False) # commit=False
            conn = self._get_connection()

            if res_main is not None: # Успех без коммита (None)
                if new_blacklist_status:
                    # --- Помещаем в ЧС ---
                    accr_status_new = 'отведен'
                    action_taken = "добавлен в черный список"
                    query_update_accr = "UPDATE AccrTable SET status = %s WHERE id = %s"
                    self.execute_query(query_update_accr, (accr_status_new, person_id), commit=True) # Коммитим обе транзакции
                    self.log_transaction(person_id, 'Добавлен в ЧС', f"Старый статус Accr: {original_accr_status_if_exists}, Новый: {accr_status_new}")
                    return action_taken, f"Сотрудник {surname} {name} помещен в ЧС."
                else:
                    # --- Убираем из ЧС ---
                    self.logger.info(f"Сотрудник {surname} {name} (ID: {person_id}) убирается из ЧС.")
                    # Проверяем, был ли он только "отведен" из-за ЧС
                    # и нет ли у него активной аккредитации
                    has_active_accreditation = False
                    if main_state.get('end_accr') and pd.to_datetime(main_state.get('end_accr')).tz_convert(self.timezone) > now_tz:
                         has_active_accreditation = True

                    # Если был 'отведен' и нет активной аккредитации -> переносим в TD и удаляем
                    # ИЛИ если его добавили сразу в ЧС (original_accr_status_if_exists может быть 'отведен' или 'в ожидании', если создали и сразу в ЧС)
                    # Лучше ориентироваться на то, что если он был в ЧС и его оттуда убирают,
                    # и он НЕ аккредитован, то он должен пройти проверку (в TD)
                    if not has_active_accreditation:
                        self.logger.info(f"Сотрудник ID {person_id} не имеет активной аккредитации. Перенос в TD и удаление из Accr/mainTable...")
                        # 1. Получаем данные из AccrTable для TD
                        accr_details_q = "SELECT *, notes AS \"Примечания\" FROM AccrTable WHERE id = %s" # Добавляем алиас для Примечаний
                        accr_details = self.execute_query(accr_details_q, (person_id,), fetch='one')

                        if accr_details:
                            # Преобразуем ключи для add_to_td (Фамилия, Имя и т.д.)
                            data_for_td = {
                                'Фамилия': accr_details.get('surname'),
                                'Имя': accr_details.get('name'),
                                'Отчество': accr_details.get('middle_name'),
                                'Дата рождения': accr_details.get('birth_date'),
                                'Место рождения': accr_details.get('birth_place'),
                                'Регистрация': accr_details.get('registration'),
                                'Организация': accr_details.get('organization'),
                                'Должность': accr_details.get('position'),
                                'Примечания': accr_details.get('Примечания'), # Уже есть алиас
                                'status': 'На проверку (снят с ЧС)'
                            }
                            td_id = self.add_to_td(data_for_td) # add_to_td уже коммитит
                            if td_id:
                                self.log_transaction(person_id, 'Снят с ЧС и перенесен в TD')
                                # 2. Удаляем из mainTable и AccrTable (после успешного добавления в TD)
                                # Сначала mainTable из-за FOREIGN KEY
                                self.execute_query("DELETE FROM mainTable WHERE person_id = %s", (person_id,), commit=False)
                                self.execute_query("DELETE FROM AccrTable WHERE id = %s", (person_id,), commit=True) # Коммитим оба удаления
                                self.logger.info(f"Сотрудник ID {person_id} удален из AccrTable/mainTable.")
                                action_taken = "убран из черного списка и перенесен в TD"
                                return action_taken, f"Сотрудник {surname} {name} убран из ЧС и добавлен в TD для проверки."
                            else:
                                self.logger.error(f"Не удалось добавить сотрудника ID {person_id} в TD при снятии с ЧС. Откат.")
                                conn.rollback() # Откатываем обновление mainTable (снятие флага ЧС)
                                return None, "Ошибка переноса в TD при снятии с ЧС."
                        else:
                             self.logger.error(f"Не удалось получить детали сотрудника ID {person_id} из AccrTable для переноса в TD. Откат.")
                             conn.rollback()
                             return None, "Ошибка получения данных для переноса в TD."
                    else:
                        # Если был в ЧС, но аккредитация еще активна - просто снимаем флаг ЧС и ставим статус 'аккредитован'
                        accr_status_new = 'аккредитован'
                        query_update_accr = "UPDATE AccrTable SET status = %s WHERE id = %s"
                        self.execute_query(query_update_accr, (accr_status_new, person_id), commit=True) # Коммитим и обновление mainTable
                        self.log_transaction(person_id, 'Снят с ЧС (активен)', f"Старый статус Accr: {original_accr_status_if_exists}, Новый: {accr_status_new}")
                        action_taken = "убран из черного списка (аккредитация активна)"
                        return action_taken, f"Сотрудник {surname} {name} убран из ЧС, аккредитация активна."
            else: # Ошибка обновления mainTable
                self.logger.error(f"toggle_blacklist: Не удалось обновить mainTable для ID {person_id}.")
                conn.rollback()
                return None, "Ошибка обновления статуса ЧС."

        return None, "Непредвиденная ситуация в toggle_blacklist."

    def search_people(self, search_term):
        like_term = f"%{search_term}%"
        params = (like_term, like_term, like_term, like_term)
        query_accr = """
        SELECT
            a.id, a.surname, a.name, a.middle_name, a.birth_date,
            a.organization, a.position, a.status AS accr_status,
            (CASE WHEN a.notes IS NOT NULL AND a.notes != '' THEN TRUE ELSE FALSE END) AS has_notes,
            mt.black_list,
            mt.start_accr, -- <--- Начало аккредитации из mainTable
            mt.end_accr,
            a.added_date AS record_creation_date, -- Дата создания записи в AccrTable (если нужно отдельно)
            'AccrTable' AS source
        FROM AccrTable a
        LEFT JOIN mainTable mt ON a.id = mt.person_id AND mt.id = (
            SELECT MAX(sub.id) FROM mainTable sub WHERE sub.person_id = a.id
        )
        WHERE a.surname ILIKE %s OR a.name ILIKE %s OR a.middle_name ILIKE %s OR a.organization ILIKE %s
        """
        query_td = """
        SELECT
            NULL::INT AS id, t.surname, t.name, t.middle_name, t.birth_date,
            t.organization, t.position, t.status AS td_status,
            (CASE WHEN t.notes IS NOT NULL AND t.notes != '' THEN TRUE ELSE FALSE END) AS has_notes,
            FALSE AS black_list,
            NULL::TIMESTAMPTZ AS start_accr, -- <--- Для TD нет начала аккредитации
            NULL::TIMESTAMPTZ AS end_accr,
            t.load_timestamp AS record_creation_date, -- Дата загрузки в TD
            'TD' AS source
        FROM TD t
        WHERE t.surname ILIKE %s OR t.name ILIKE %s OR t.middle_name ILIKE %s OR t.organization ILIKE %s
        """
        full_query = f"({query_accr}) UNION ALL ({query_td}) ORDER BY surname, name;"
        full_params = params + params
        return self.execute_query(full_query, full_params, fetch='all')

    def get_employee_records(self, person_id):
         """Получает историю операций для сотрудника из таблицы Records."""
         query = """
         SELECT operation_date, operation_type, details
         FROM Records
         WHERE person_id = %s
         ORDER BY operation_date DESC;
         """
         return self.execute_query(query, (person_id,), fetch='all')

    def get_notes(self, person_id):
        """Получает примечания для сотрудника."""
        query = "SELECT notes FROM AccrTable WHERE id = %s;"
        result = self.execute_query(query, (person_id,), fetch='one')
        return result['notes'] if result else ""

    def update_notes(self, person_id, notes):
        """Обновляет примечания для сотрудника."""
        query = "UPDATE AccrTable SET notes = %s WHERE id = %s;"
        result = self.execute_query(query, (notes, person_id), commit=True)
        if result is None: # commit=True вернет None при успехе
             self.logger.info(f"Примечания для person_id={person_id} обновлены.")
             self.log_transaction(person_id, 'Примечания обновлены')
             return True
        else:
             self.logger.error(f"Не удалось обновить примечания для person_id={person_id}.")
             return False

    def get_people_for_recheck(self, only_gph=False):
         """Возвращает список ID людей для повторной проверки (статус 'в ожидании')."""
         # Эта логика может быть пересмотрена в зависимости от того, как статус 'в ожидании' используется
         # после внедрения немедленной проверки. Возможно, нужны люди, у кого скоро истекает срок?
         # Пока оставляем как есть - берем тех, кто 'в ожидании'.
         query = """
         SELECT id FROM AccrTable
         WHERE status = 'в ожидании'
         """
         org_filter = " AND organization ILIKE 'ГПХ'" if only_gph else " AND (organization IS NULL OR organization NOT ILIKE 'ГПХ')"
         query += org_filter
         query += ";"

         results = self.execute_query(query, fetch='all')
         return [row['id'] for row in results] if results else []

    def get_people_details(self, person_ids):
        """Получает полные данные людей по списку ID."""
        if not person_ids:
            return []
        query = """
        SELECT id, surname, name, middle_name, birth_date, organization, position
        FROM AccrTable
        WHERE id = ANY(%s);
        """
        # Преобразуем список ID в формат, понятный PostgreSQL (например, массив)
        params = (list(person_ids),)
        return self.execute_query(query, params, fetch='all')


    def get_all_from_td_full(self):
        """Получает все данные из временной таблицы TD."""
        query = "SELECT * FROM TD;"
        return self.execute_query(query, fetch='all')

    def clean_td(self):
        """Очищает временную таблицу TD."""
        query = "DELETE FROM TD;"
        result = self.execute_query(query, commit=True)
        if result is not None:
             self.logger.info("Временная таблица TD очищена.")
             return True
        else:
            self.logger.error("Не удалось очистить временную таблицу TD.")
            return False

    def check_accreditation_expiry(self):
        """ Обновляет статус на 'истек срок' для истёкших аккредитаций. """
        now_tz = datetime.now(self.timezone)
        query_find_expired = """
        SELECT mt.person_id
        FROM mainTable mt
        JOIN AccrTable a ON mt.person_id = a.id
        WHERE mt.end_accr <= %s
          AND mt.black_list = FALSE
          AND a.status = 'аккредитован'
          AND mt.id = (SELECT MAX(sub.id) FROM mainTable sub WHERE sub.person_id = mt.person_id);
        """
        expired_people = self.execute_query(query_find_expired, (now_tz,), fetch='all')

        if expired_people:
            expired_ids = [p['person_id'] for p in expired_people]
            self.logger.info(f"Найдены истекшие аккредитации для person_ids: {expired_ids}")

            query_update_accr = "UPDATE AccrTable SET status = 'истек срок' WHERE id = ANY(%s);"
            self.execute_query(query_update_accr, (expired_ids,), commit=True)

            # Не обновляем mainTable, т.к. end_accr уже показывает истечение
            for person_id in expired_ids:
                 self.log_transaction(person_id, 'Аккредитация истекла', 'Статус изменен на "истек срок"')
            return len(expired_ids)
        return 0

    def activate_person_by_details(self, surname, name, middle_name, birth_date):
        """
        Активирует сотрудника (статус 'аккредитован', даты в mainTable),
        если он найден в AccrTable и имеет статус 'в ожидании'.
        Возвращает кортеж (success: bool, message: str, person_id: int | None).
        """
        person_id = self.find_person_in_accrtable(surname, name, middle_name, birth_date)

        if not person_id:
            msg = f"Сотрудник {surname} {name} не найден в AccrTable."
            self.logger.info(msg)
            return False, msg, None # Возвращаем None как person_id

        # Проверяем текущий статус
        query_get_status = "SELECT status FROM AccrTable WHERE id = %s;"
        current_record = self.execute_query(query_get_status, (person_id,), fetch='one')

        if not current_record:
             msg = f"Не удалось получить текущий статус для ID {person_id} (хотя он был найден)."
             self.logger.error(msg)
             return False, msg, person_id
        current_status = current_record.get('status')

        if current_status != 'в ожидании':
            msg = f"Статус сотрудника ID {person_id} ({surname} {name}) не 'в ожидании' (текущий: '{current_status}'). Активация не требуется."
            self.logger.info(msg)
            # Считаем это "успехом" в смысле обработки строки, но без действия
            return True, f"Статус уже '{current_status}'. Активация не требуется.", person_id

        # --- Выполняем активацию ---
        self.logger.info(f"Активация сотрудника ID {person_id} ({surname} {name})...")
        new_status = 'аккредитован'
        now_tz = datetime.now(self.timezone)
        end_accr = now_tz + timedelta(days=180)

        # Используем одну транзакцию для обоих обновлений
        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                # 1. Обновляем AccrTable
                cursor.execute("UPDATE AccrTable SET status = %s WHERE id = %s", (new_status, person_id))

                # 2. Проверяем наличие записи в mainTable
                cursor.execute("SELECT id FROM mainTable WHERE person_id = %s", (person_id,))
                main_table_entry = cursor.fetchone()

                if main_table_entry:
                    # Запись существует, ОБНОВЛЯЕМ её
                    cursor.execute("""
                                       UPDATE mainTable
                                       SET start_accr = %s,
                                           end_accr = %s,
                                           black_list = FALSE,
                                           last_checked = %s
                                       WHERE person_id = %s;
                                       """, (now_tz, end_accr, now_tz, person_id))  # Условие по person_id
                else:
                    # Записи нет, ВСТАВЛЯЕМ новую
                    cursor.execute("""
                                       INSERT INTO mainTable (person_id, start_accr, end_accr, black_list, last_checked)
                                       VALUES (%s, %s, %s, FALSE, %s);
                                       """, (person_id, now_tz, end_accr, now_tz))
            conn.commit()
            self.logger.info(f"Сотрудник ID {person_id} успешно активирован. Аккредитация до {end_accr.strftime('%Y-%m-%d')}.")
            self.log_transaction(person_id, 'Статус Активен (файл)', f'Аккредитация до {end_accr.strftime("%Y-%m-%d")}')
            return True, "Сотрудник успешно активирован.", person_id

        except psycopg2.Error as e:
            if conn:
                try: conn.rollback()
                except psycopg2.Error: self.logger.error("Ошибка при откате транзакции активации.")
            self.logger.error(f"Ошибка БД при активации ID {person_id}: {e}")
            return False, f"Ошибка БД: {e}", person_id
        except Exception as e:
            if conn:
                 try: conn.rollback()
                 except psycopg2.Error: self.logger.error("Ошибка при откате транзакции активации.")
            self.logger.exception(f"Неожиданная ошибка при активации ID {person_id}: {e}")
            return False, f"Внутренняя ошибка: {e}", person_id
        finally:
             if conn:
                  self._release_connection(conn)


    def close_pool(self):
        """Закрывает пул соединений."""
        if self._pool:
            self._pool.closeall()
            self.logger.info("Пул соединений PostgreSQL закрыт.")
            DatabaseManager._pool = None
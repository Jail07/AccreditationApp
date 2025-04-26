# database_manager.py
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

        :param query: SQL-запрос (строка).
        :param params: Параметры запроса (tuple или dict).
        :param fetch: 'one', 'all' или None.
        :param commit: True для фиксации транзакции (INSERT, UPDATE, DELETE).
        :return: Результат fetch или None. Исключение при ошибке.
        """
        conn = None
        result = None
        try:
            conn = self._get_connection()
            # Используем RealDictCursor для получения результатов в виде словарей
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params)
                if fetch == 'one':
                    result = cursor.fetchone()
                elif fetch == 'all':
                    result = cursor.fetchall()

                if commit:
                    conn.commit()
                self.logger.debug(f"Запрос выполнен успешно: {query[:100]}...") # Логируем только начало запроса
            return result
        except psycopg2.Error as e:
            if conn and commit: # Откат только если была попытка commit
                try:
                    conn.rollback()
                    self.logger.warning(f"Транзакция отменена из-за ошибки: {e}")
                except psycopg2.Error as rb_e:
                     self.logger.error(f"Ошибка при откате транзакции: {rb_e}")
            self.logger.error(f"Ошибка выполнения SQL запроса: {e}\nЗапрос: {query}\nПараметры: {params}")
            # Не пробрасываем исключение дальше, чтобы приложение могло обработать None
            return None # Возвращаем None в случае ошибки psycopg2
        except Exception as e:
            self.logger.exception(f"Неожиданная ошибка при выполнении запроса: {e}")
            # Пробрасываем другие исключения
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
        query = """
        INSERT INTO Records (person_id, operation_type, details, operation_date)
        VALUES (%s, %s, %s, %s);
        """
        now_tz = datetime.now(self.timezone)
        params = (person_id, operation_type, details, now_tz)
        self.execute_query(query, params, commit=True)
        # Об ошибке сообщит execute_query

    def add_to_td(self, data):
        """Добавляет запись во временную таблицу TD."""
        required_fields = ['surname', 'name', 'birth_date', 'organization']
        if not all(data.get(field) for field in required_fields):
            self.logger.warning(f"Пропущена запись в TD из-за отсутствия обязательных полей: {data}")
            return None

        query = """
        INSERT INTO TD (surname, name, middle_name, birth_date, birth_place, registration, organization, position, status, load_timestamp)
        VALUES (%(surname)s, %(name)s, %(middle_name)s, %(birth_date)s, %(birth_place)s, %(registration)s, %(organization)s, %(position)s, %(status)s, %s)
        RETURNING id;
        """
        now_tz = datetime.now(self.timezone)
        params = {
            'surname': data.get('surname'),
            'name': data.get('name'),
            'middle_name': data.get('middle_name'),
            'birth_date': data.get('birth_date'),
            'birth_place': data.get('birth_place'),
            'registration': data.get('registration'),
            'organization': data.get('organization'),
            'position': data.get('position'),
            'status': data.get('status', 'На проверку') # Статус из UI
        }
        result = self.execute_query(query, (params, now_tz), fetch='one', commit=True)
        if result:
            self.logger.info(f"Запись добавлена в TD: {params.get('surname')} {params.get('name')}, ID: {result['id']}")
            return result['id']
        else:
            self.logger.error(f"Не удалось добавить запись в TD: {params.get('surname')} {params.get('name')}")
            return None

    def find_person_in_accrtable(self, surname, name, middle_name, birth_date):
        """Ищет человека в AccrTable. Возвращает ID или None."""
        query = """
        SELECT id FROM AccrTable
        WHERE surname = %s AND name = %s AND birth_date = %s
        """
        params_list = [surname, name, birth_date]
        if middle_name:
            query += " AND middle_name = %s"
            params_list.append(middle_name)
        else:
            query += " AND (middle_name IS NULL OR middle_name = '')"

        query += " ORDER BY id DESC LIMIT 1;" # Берем последнюю запись, если вдруг дубликаты

        result = self.execute_query(query, tuple(params_list), fetch='one')
        return result['id'] if result else None

    def get_person_status(self, surname, name, middle_name, birth_date):
        """
        Проверяет статус человека в mainTable (активность, черный список).
        Возвращает словарь {'status': 'BLACKLISTED'|'ACTIVE'|'EXPIRED'|'NOT_FOUND', 'person_id': id | None}.
        """
        person_id = self.find_person_in_accrtable(surname, name, middle_name, birth_date)
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
            else:
                # Статус есть, но он не черный список и аккредитация истекла или не была установлена
                 return {'status': 'EXPIRED', 'person_id': person_id}
        else:
            # Человек есть в AccrTable, но нет записей в mainTable (например, только добавлен)
            return {'status': 'NOT_FOUND', 'person_id': person_id} # Считаем, что статус не найден

    def add_to_accrtable(self, data, status='в ожидании'):
        """
        Добавляет запись в AccrTable, если её там нет.
        Возвращает ID существующей или новой записи.
        """
        person_id = self.find_person_in_accrtable(
            data.get('surname'), data.get('name'), data.get('middle_name'), data.get('birth_date')
        )
        if person_id:
            self.logger.info(f"Человек {data.get('surname')} {data.get('name')} уже существует в AccrTable (ID: {person_id}).")
            # Опционально: Обновить данные существующей записи?
            # query_update = "UPDATE AccrTable SET organization=%s, position=%s, ... WHERE id=%s"
            # self.execute_query(query_update, (..., person_id), commit=True)
            return person_id

        query = """
        INSERT INTO AccrTable (surname, name, middle_name, birth_date, birth_place, registration, organization, position, status, added_date)
        VALUES (%(surname)s, %(name)s, %(middle_name)s, %(birth_date)s, %(birth_place)s, %(registration)s, %(organization)s, %(position)s, %s, %s)
        RETURNING id;
        """
        now_tz = datetime.now(self.timezone)
        params = {
            'surname': data.get('surname'),
            'name': data.get('name'),
            'middle_name': data.get('middle_name'),
            'birth_date': data.get('birth_date'),
            'birth_place': data.get('birth_place'),
            'registration': data.get('registration'),
            'organization': data.get('organization'),
            'position': data.get('position')
        }
        result = self.execute_query(query, (params, status, now_tz), fetch='one', commit=True)
        if result:
            new_id = result['id']
            self.logger.info(f"Человек {params.get('surname')} {params.get('name')} добавлен в AccrTable (ID: {new_id}) со статусом '{status}'.")
            self.log_transaction(new_id, 'Добавлен в AccrTable', f'Статус: {status}')
            # Сразу добавляем запись в mainTable без дат аккредитации, но с black_list=False
            self.add_initial_main_record(new_id)
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
            self.logger.info(f"Добавлена начальная запись в mainTable для person_id={person_id}")

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

    def toggle_blacklist(self, person_id):
        """Переключает статус black_list для person_id в mainTable."""
        if not person_id:
            self.logger.warning("Попытка изменить черный список без person_id.")
            return None # Возвращаем None при ошибке

        now_tz = datetime.now(self.timezone)

        # Получаем текущий статус blacklist
        query_get = "SELECT id, black_list FROM mainTable WHERE person_id = %s ORDER BY id DESC LIMIT 1"
        current_state = self.execute_query(query_get, (person_id,), fetch='one')

        if not current_state:
             self.logger.warning(f"Не найдена запись в mainTable для person_id={person_id} при попытке изменить ЧС.")
              # Если человека нет в mainTable, но есть в AccrTable, добавляем запись с black_list=True
             self.add_initial_main_record(person_id)
             query_set_black = "UPDATE mainTable SET black_list = TRUE, last_checked = %s WHERE person_id = %s"
             self.execute_query(query_set_black, (now_tz, person_id), commit=True)
             self.log_transaction(person_id, 'Добавлен в черный список')
             return "добавлен в черный список"


        new_blacklist_status = not current_state['black_list']
        main_table_id = current_state['id']

        query_update = "UPDATE mainTable SET black_list = %s, last_checked = %s WHERE id = %s"
        result = self.execute_query(query_update, (new_blacklist_status, now_tz, main_table_id), commit=True)

        if result is not None: # commit=True вернет None при успехе
            action = "добавлен в черный список" if new_blacklist_status else "убран из черного списка"
            # Обновляем статус и в AccrTable
            accr_status = 'отведен' if new_blacklist_status else 'в ожидании'
            query_accr = "UPDATE AccrTable SET status = %s WHERE id = %s"
            self.execute_query(query_accr, (accr_status, person_id), commit=True)

            self.logger.info(f"Сотрудник person_id={person_id} {action}.")
            self.log_transaction(person_id, 'Изменен статус ЧС', f'Новый статус ЧС: {new_blacklist_status}, Статус Accr: {accr_status}')
            return action
        else:
            self.logger.error(f"Не удалось изменить статус черного списка для person_id={person_id}.")
            return None # Возвращаем None при ошибке

    def search_people(self, search_term):
        """Ищет людей по ФИО или организации."""
        query = """
        SELECT a.id, a.surname, a.name, a.middle_name, a.birth_date, a.organization, a.status,
               mt.black_list, mt.end_accr
        FROM AccrTable a
        LEFT JOIN mainTable mt ON a.id = mt.person_id AND mt.id = (
            SELECT MAX(id) FROM mainTable WHERE person_id = a.id
        )
        WHERE a.surname ILIKE %s OR a.name ILIKE %s OR a.middle_name ILIKE %s OR a.organization ILIKE %s
        ORDER BY a.surname, a.name;
        """
        like_term = f"%{search_term}%"
        params = (like_term, like_term, like_term, like_term)
        return self.execute_query(query, params, fetch='all')

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
        if result is not None: # commit=True вернет None при успехе
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

    def close_pool(self):
        """Закрывает пул соединений."""
        if self._pool:
            self._pool.closeall()
            self.logger.info("Пул соединений PostgreSQL закрыт.")
            DatabaseManager._pool = None
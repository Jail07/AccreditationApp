import psycopg2
from datetime import datetime, timedelta

class DatabaseManager:
    def __init__(self, db_name, user, password, host="localhost", port=5432):
        self.connection = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cursor = self.connection.cursor()

    def create_tables(self):
        try:
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS AccrTable (
                id SERIAL PRIMARY KEY,
                surname TEXT NOT NULL,
                name TEXT NOT NULL,
                middle_name TEXT,
                birth_date DATE NOT NULL,
                birth_place TEXT,
                registration TEXT,
                organization TEXT NOT NULL,
                position TEXT,
                added_date TIMESTAMP NOT NULL DEFAULT NOW(),
                status TEXT DEFAULT 'в ожидании',
                CONSTRAINT unique_person UNIQUE (surname, name, middle_name, birth_date)
            );
            """)

            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS mainTable (
                id SERIAL PRIMARY KEY,
                person_id INT NOT NULL REFERENCES AccrTable(id) ON DELETE CASCADE,
                start_accr TIMESTAMP,
                end_accr TIMESTAMP,
                black_list BOOLEAN DEFAULT FALSE
            );
            """)

            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS TD (
                id SERIAL PRIMARY KEY,
                surname TEXT NOT NULL,
                name TEXT NOT NULL,
                middle_name TEXT,
                birth_date DATE NOT NULL,
                birth_place TEXT,
                registration TEXT,
                organization TEXT,
                position TEXT,
                added_date TIMESTAMP NOT NULL DEFAULT NOW()
            );
            """)

            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Records (
                id SERIAL PRIMARY KEY,
                person_id INT NOT NULL,
                operation_date TIMESTAMP NOT NULL DEFAULT NOW(),
                operation_type TEXT NOT NULL
            );
            """)

            self.connection.commit()
            print("Таблицы успешно созданы или уже существуют.")
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка при создании таблиц: {e}")

    def add_to_td(self, data):
        try:
            required_fields = ['Фамилия', 'Имя', 'Дата рождения', 'Организация']
            missing_fields = [field for field in required_fields if not data.get(field)]

            if missing_fields:
                print(f"Ошибка: Отсутствуют обязательные поля для TD: {', '.join(missing_fields)}")
                return

            self.cursor.execute("""
                INSERT INTO TD (surname, name, middle_name, birth_date, birth_place, registration, organization, position)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                data.get('Фамилия'),
                data.get('Имя'),
                data.get('Отчество'),
                data.get('Дата рождения'),
                data.get('Место рождения'),
                data.get('Регистрация'),
                data.get('Организация'),
                data.get('Должность')
            ))
            self.connection.commit()
            print(f"Сотрудник {data.get('Фамилия')} {data.get('Имя')} добавлен в временную таблицу TD.")
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка добавления в временную БД: {e}")

    def clean_td(self):
        try:
            self.cursor.execute("DELETE FROM TD;")
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка удаление данных из TD: {e}")

    def add_to_main_table(self, person_id, start_accr, end_accr):
        try:
            self.cursor.execute("""
            INSERT INTO mainTable (person_id, start_accr, end_accr)
            VALUES (%s, %s, %s);
            """, (person_id, start_accr, end_accr))
            self.connection.commit()
            print(
                f"Запись добавлена в mainTable для person_id={person_id}, срок аккредитации: {start_accr} - {end_accr}")
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка добавления в mainTable: {e}")

    def update_accreditation_status(self, accr_table_id, new_status):
        try:
            self.cursor.execute("""
            UPDATE AccrTable SET status = %s WHERE id = %s;
            """, (new_status, accr_table_id))
            self.connection.commit()

            if new_status == "аккредитован":
                start_accr = datetime.now()
                end_accr = start_accr + timedelta(days=180)

                self.add_to_main_table(accr_table_id, start_accr, end_accr)

                self.log_transaction(accr_table_id,
                                     f"Статус изменен на 'аккредитован', срок аккредитации: {start_accr} - {end_accr}")
            else:
                self.log_transaction(accr_table_id, f"Статус изменен на '{new_status}'")

            print(f"Статус сотрудника ID {accr_table_id} изменен на '{new_status}'.")
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка обновления статуса: {e}")

    def check_accreditation_expiry(self):
        try:
            self.cursor.execute("""
            UPDATE AccrTable
            SET status = 'не активен'
            WHERE status = 'аккредитован' AND id IN (
                SELECT accr_table_id FROM mainTable WHERE end_date < NOW()
            );
            """)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка проверки истечения срока аккредитации: {e}")

    def log_transaction(self, person_id, action):
        try:
            self.cursor.execute("""
            INSERT INTO Records (person_id, operation_type)
            VALUES (%s, %s);
            """, (person_id, action))
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка записи транзакции: {e}")

    def find_matches_TD(self, surname, name, middle_name, birth_date):
        try:
            self.cursor.execute("""
                SELECT id FROM TD
                WHERE surname = %s AND name = %s AND middle_name = %s 
                AND birth_date = %s;
            """, (surname, name, middle_name, birth_date))
            return self.cursor.fetchone() is not None
        except Exception as e:
            print(f"Ошибка при поиске сотрудника: {e}")
            return False

    def find_matches_AccrTable(self, surname, name, middle_name, birth_date):
        try:
            self.cursor.execute("""
                SELECT id FROM AccrTable
                WHERE surname = %s AND name = %s AND middle_name = %s 
                AND birth_date = %s;
            """, (surname, name, middle_name, birth_date))
            return self.cursor.fetchone() is not None
        except Exception as e:
            print(f"Ошибка при поиске сотрудника: {e}")
            return False

    def toggle_blacklist(self, surname, name, middle_name, birth_date, birth_place, registration, organization,
                         position):
        try:
            self.cursor.execute("""
            SELECT id, status, birth_place, registration, organization, position
            FROM AccrTable
            WHERE surname = %s AND name = %s AND middle_name = %s AND birth_date = %s;
            """, (surname, name, middle_name, birth_date))
            result = self.cursor.fetchone()

            if result:
                person_id = result[0]
                current_status = result[1]
                birth_place = result[2]
                registration = result[3]
                organization = result[4]
                position = result[5]

                self.cursor.execute("""
                SELECT black_list FROM mainTable WHERE person_id = %s;
                """, (person_id,))
                blacklist_status = self.cursor.fetchone()

                if blacklist_status and blacklist_status[0]:
                    self.cursor.execute("""
                    UPDATE AccrTable SET status = 'не активен' WHERE id = %s;
                    """, (person_id,))

                    self.cursor.execute("""
                    UPDATE mainTable SET black_list = FALSE WHERE person_id = %s;
                    """, (person_id,))

                    self.cursor.execute("""
                    INSERT INTO TD (surname, name, middle_name, birth_date, birth_place, registration, organization, position)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """, (surname, name, middle_name, birth_date, birth_place, registration, organization, position))

                    self.log_transaction(person_id,
                                         "Убран из черного списка, статус изменен на 'не активен', добавлен в TD")

                    self.connection.commit()
                    print(
                        f"Сотрудник {surname} {name} {middle_name} убран из черного списка, статус изменен на 'не активен' и добавлен в TD.")
                    return "убран из черного списка"
                else:
                    self.cursor.execute("""
                    UPDATE mainTable SET black_list = TRUE WHERE person_id = %s;
                    """, (person_id,))

                    self.log_transaction(person_id, "Добавлен в черный список")
                    self.connection.commit()
                    print(f"Сотрудник {surname} {name} {middle_name} добавлен в черный список.")
                    return "добавлен в черный список"
            else:
                print(f"Сотрудник {surname} {name} {middle_name} отсутствует в AccrTable. Добавляем.")

                self.cursor.execute("""
                SELECT id FROM TD
                WHERE surname = %s AND name = %s AND middle_name = %s AND birth_date = %s;
                """, (surname, name, middle_name, birth_date))
                td_result = self.cursor.fetchone()

                if td_result:
                    self.cursor.execute("""
                    DELETE FROM TD WHERE id = %s;
                    """, (td_result[0],))
                    print(f"Сотрудник {surname} {name} {middle_name} удален из TD.")

                self.cursor.execute("""
                INSERT INTO AccrTable (
                    surname, name, middle_name, birth_date, birth_place, registration, organization, position, status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """, (
                    surname, name, middle_name, birth_date, birth_place, registration, organization, position, "в чс"))
                person_id = self.cursor.fetchone()[0]

                self.cursor.execute("""
                INSERT INTO mainTable (person_id, black_list)
                VALUES (%s, TRUE);
                """, (person_id,))

                self.log_transaction(person_id, "Добавлен в черный список")
                self.connection.commit()
                print(f"Сотрудник {surname} {name} {middle_name} добавлен в черный список.")
                return "добавлен в черный список"

        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка при управлении черным списком: {e}")
            return None

    def add_to_accrtable(self, data, status="в ожидании"):
        try:
            required_fields = ['surname', 'name', 'birth_date', 'organization']
            missing_fields = [field for field in required_fields if not data.get(field)]

            if missing_fields:
                raise ValueError(f"Ошибка: Отсутствуют обязательные поля: {', '.join(missing_fields)}")
            print(data)

            if status == "в чс":
                self.cursor.execute("""
                                INSERT INTO AccrTable (
                                    surname, name, middle_name, birth_date, birth_place, registration, organization, position, status, blacklist
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                    data['surname'], data['name'], data.get('middle_name'), data['birth_date'],
                    data.get('birth_place'), data.get('registration'), data['organization'], data.get('position'),
                    status, True
                ))
                self.connection.commit()
                print(f"Сотрудник {data['surname']} {data['name']} добавлен в AccrTable со статусом '{status}'.")

                return "добавлен в черный список"
            else:
                self.cursor.execute("""
                    INSERT INTO AccrTable (
                        surname, name, middle_name, birth_date, birth_place, registration, organization, position, status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (
                    data['surname'], data['name'], data.get('middle_name'), data['birth_date'],
                    data.get('birth_place'), data.get('registration'), data['organization'], data.get('position'), status
                ))
                self.connection.commit()
                print(f"Сотрудник {data['surname']} {data['name']} добавлен в AccrTable со статусом '{status}'.")



        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка добавления в AccrTable: {e}")

    def search_person(self, search_term):
        try:
            query = """
                SELECT 
                    CONCAT_WS(' ', a.surname, a.name, a.middle_name) AS fio,
                    a.birth_date,
                    CASE 
                        WHEN m.black_list = TRUE THEN CONCAT('в черном списке с ', TO_CHAR(m.start_accr, 'DD.MM.YYYY'))
                        WHEN m.end_accr < NOW() THEN CONCAT('срок аккредитации прошел с ', TO_CHAR(m.end_accr, 'DD.MM.YYYY'))
                        WHEN m.start_accr IS NOT NULL AND m.end_accr IS NOT NULL THEN CONCAT('аккредитован до ', TO_CHAR(m.end_accr, 'DD.MM.YYYY'))
                        ELSE 'в ожидании проверки'
                    END AS status
                FROM AccrTable a
                LEFT JOIN mainTable m ON a.id = m.person_id
                WHERE 
                    a.surname ILIKE %s OR 
                    a.name ILIKE %s OR 
                    a.middle_name ILIKE %s OR 
                    TO_CHAR(a.birth_date, 'DD.MM.YYYY') = %s;
            """
            search_pattern = f"%{search_term}%"
            self.cursor.execute(query, (search_pattern, search_pattern, search_pattern, search_term))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Ошибка при поиске сотрудника: {e}")
            return []

    def get_employee_records(self, fio, birth_date):
        try:
            self.cursor.execute("""
                    SELECT 
                        r.operation_date,
                        CONCAT_WS(' ', a.surname, a.name, a.middle_name) AS object,
                        a.organization, 
                        r.operation_type
                    FROM Records r
                    JOIN MainTable m ON r.person_id = m.id
                    JOIN AccrTable a ON m.person_id = a.id
                    WHERE CONCAT_WS(' ', a.surname, a.name, a.middle_name) = %s AND a.birth_date = %s;
                """, (fio, birth_date))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Ошибка при получении записей сотрудника: {e}")
            return []

    def get_people_for_recheck_full(self):
        self.cursor.execute("""
        SELECT id, surname, name, middle_name, birth_date, birth_place, registration,
               organization
        FROM TD;
        """)
        return [dict(zip([desc[0] for desc in self.cursor.description], row)) for row in self.cursor.fetchall()]

    def get_expired_accreditations(self):
        try:
            self.cursor.execute("""
            SELECT m.id, a.surname, a.name, a.middle_name, a.birth_date, a.organization, m.end_accr
            FROM mainTable m
            JOIN AccrTable a ON m.person_id = a.id
            WHERE m.end_accr < NOW() AND m.black_list = FALSE;
            """)
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Ошибка при получении сотрудников с истёкшей аккредитацией: {e}")
            return []

    def get_all_from_td_full(self):
        self.cursor.execute("""
        SELECT id, surname, name, middle_name, birth_date, birth_place, registration,
               organization, added_date, position
        FROM TD;
        """)
        return [dict(zip([desc[0] for desc in self.cursor.description], row)) for row in self.cursor.fetchall()]

    def validate_accreditation_file(self, file_data):
        try:
            valid_ids = []
            for _, row in file_data.iterrows():
                self.cursor.execute("""
                SELECT a.id
                FROM AccrTable a
                WHERE a.surname = %s AND a.name = %s AND a.middle_name = %s
                  AND a.birth_date = %s AND a.status = 'в ожидании' OR a.status = 'не активен';
                """, (row['Фамилия'], row['Имя'], row.get('Отчество'), row['Дата рождения']))
                result = self.cursor.fetchone()
                if result:
                    valid_ids.append(result[0])
            return valid_ids
        except Exception as e:
            print(f"Ошибка проверки файла аккредитации: {e}")
            return []

    def update_accreditation_status_from_file(self, valid_ids, start_date=None):
        try:
            end_date = start_date + timedelta(days=180) if start_date else datetime.now() + timedelta(days=180)
            for person_id in valid_ids:
                self.cursor.execute("""
                UPDATE AccrTable SET status = 'аккредитован' WHERE id = %s;
                """, (person_id,))

                self.cursor.execute("""
                INSERT INTO mainTable (person_id, start_accr, end_accr, black_list)
                VALUES (%s, %s, %s, FALSE);
                """, (person_id, start_date or datetime.now(), end_date))

                self.cursor.execute("""
                INSERT INTO Records (person_id, operation_date, operation_type)
                VALUES (%s, %s, %s);
                """, (person_id, start_date or datetime.now(), 'аккредитован'))

                self.log_transaction(person_id, "Изменён статус на 'аккредитован'")
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка обновления статуса аккредитации: {e}")

    def close(self):
        self.cursor.close()
        self.connection.close()

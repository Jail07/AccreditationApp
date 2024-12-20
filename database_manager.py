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
            # Создаем таблицу AccrTable, если ее нет
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
                status TEXT DEFAULT 'в ожидании',
                added_date TIMESTAMP NOT NULL DEFAULT NOW(),
                accreditation_end_date TIMESTAMP DEFAULT NOW() + INTERVAL '180 days',
                blacklist BOOLEAN DEFAULT FALSE
            );
            """)


            # Таблица для временных данных TD
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS TD (
                id SERIAL PRIMARY KEY,
                surname TEXT NOT NULL,
                name TEXT NOT NULL,
                middle_name TEXT,
                birth_date DATE NOT NULL,
                birth_place TEXT,
                registration TEXT,
                organization TEXT NOT NULL,
                position TEXT,
                added_date TIMESTAMP NOT NULL DEFAULT NOW()
            );
            """)

            # Таблица Records для действий над сотрудниками
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Records (
                id SERIAL PRIMARY KEY,
                person_id INT NOT NULL,
                operation_date TIMESTAMP NOT NULL DEFAULT NOW(),
                operation_type TEXT NOT NULL
            );
            """)

            self.connection.commit()
            print("Таблицы успешно созданы или уже существуют")
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка при создании таблиц: {e}")

    def add_to_td(self, data):
        """
        Добавляет строку во временную таблицу TD.
        """
        try:
            print(data)
            required_fields = ['Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Организация']
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

    def transfer_to_accrtable(self):
        """
        Перенос записей из TD в AccrTable.
        """
        try:
            self.cursor.execute("""
            INSERT INTO AccrTable (surname, name, middle_name, birth_date, organization, birth_place, registration, position)
            SELECT surname, name, middle_name, birth_date, organization, birth_place, registration, position
            FROM TD;
            """)
            self.cursor.execute("""
            SELECT id, surname, name, middle_name, birth_date, organization, birth_place, registration, position
            FROM TD;""")
            data = self.cursor.fetchall()
            self.cursor.execute("DELETE FROM TD;")
            self.connection.commit()
            return data
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка переноса данных из TD в AccrTable: {e}")

    def update_accreditation_status(self, accr_table_id, new_status):
        """
        Обновляет статус в AccrTable.
        """
        try:
            self.cursor.execute("""
            UPDATE AccrTable SET status = %s WHERE id = %s;
            """, (new_status, accr_table_id))
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка обновления статуса: {e}")

    def check_accreditation_expiry(self):
        """
        Проверяет и обновляет истекшие аккредитации.
        """
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

    def log_transaction(self, person_id, operation_type):
        """
        Логирует действия в Records.
        """
        try:
            self.cursor.execute("""
            INSERT INTO Records (person_id, operation_type)
            VALUES (%s, %s);
            """, (person_id, operation_type))
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка при записи транзакции: {e}")


    def find_matches_TD(self, surname, name, middle_name, birth_date, organization):
        """
        Проверяет, существует ли сотрудник с заданными данными в таблице AccrTable.
        """
        try:
            self.cursor.execute("""
                SELECT id FROM TD
                WHERE surname = %s AND name = %s AND middle_name = %s 
                AND birth_date = %s AND organization = %s;
            """, (surname, name, middle_name, birth_date, organization))
            return self.cursor.fetchone() is not None
        except Exception as e:
            print(f"Ошибка при поиске сотрудника: {e}")
            return False

    def find_matches_AccrTable(self, surname, name, middle_name, birth_date, organization):
        """
        Проверяет, существует ли сотрудник с заданными данными в таблице AccrTable.
        """
        try:
            self.cursor.execute("""
                SELECT id FROM AccrTable
                WHERE surname = %s AND name = %s AND middle_name = %s 
                AND birth_date = %s AND organization = %s;
            """, (surname, name, middle_name, birth_date, organization))
            return self.cursor.fetchone() is not None
        except Exception as e:
            print(f"Ошибка при поиске сотрудника: {e}")
            return False

    def toggle_blacklist(self, surname, name, middle_name, birth_date, birth_place, registration, organization, position):
        """Добавляет или убирает сотрудника из черного списка."""
        try:
            data = {
                "Фамилия": surname,
                "Имя": name,
                "Отчество": middle_name,
                "Дата рождения": birth_date,
                "Организация": organization,
            }
            print(surname, name, middle_name, birth_date, birth_place, registration, organization, position)
            self.cursor.execute("""
                    SELECT id, blacklist FROM AccrTable
                    WHERE surname = %s AND name = %s AND middle_name = %s 
                    AND birth_date = %s AND organization = %s;
                """, (data['Фамилия'], data['Имя'], data.get('Отчество'), data['Дата рождения'], data['Организация']))
            result = self.cursor.fetchone()
            print(result)

            if result:
                person_id, blacklist_status = result
                new_status = not blacklist_status
                self.cursor.execute("""
                        UPDATE AccrTable SET blacklist = %s, status = %s WHERE id = %s;
                    """, (new_status, 'в чс' if new_status else 'в ожидании', person_id))
                action = "добавлен в черный список" if new_status else "убран из черного списка"
                self.log_transaction(person_id, action)
                print(f"Сотрудник {surname} {name} {middle_name} {action}.")
                return action
            else:
                print(f"Сотрудник {surname} {name} {middle_name} не найден в AccrTable. Добавляем со статусом 'в чс'.")
                data = {
                    "Фамилия": surname,
                    "Имя": name,
                    "Отчество": middle_name,
                    "Дата рождения": birth_date,
                    "Место рождения": birth_place,
                    "Регистрация": registration,
                    "Организация": organization,
                    "Должность": position
                }

                return self.add_to_accrtable(data, status="в чс")

        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка управления черным списком: {e}")
            return False

    def add_to_accrtable(self, data, status="в ожидании"):
        try:
            required_fields = ['Фамилия', 'Имя', 'Дата рождения', 'Организация']
            missing_fields = [field for field in required_fields if not data.get(field)]

            if missing_fields:
                raise ValueError(f"Ошибка: Отсутствуют обязательные поля: {', '.join(missing_fields)}")
            print(data)
            self.cursor.execute("""
                INSERT INTO AccrTable (
                    surname, name, middle_name, birth_date, birth_place, registration, organization, position, status, blacklist
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                ON CONFLICT DO NOTHING;
            """, (
                data['Фамилия'], data['Имя'], data.get('Отчество'), data['Дата рождения'],
                data.get('Место рождения'), data.get('Регистрация'), data['Организация'], data.get('Должность'), status
            ))
            self.connection.commit()
            print(f"Сотрудник {data['Фамилия']} {data['Имя']} добавлен в AccrTable со статусом '{status}'.")

            return "добавлен в черный список"
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка добавления в AccrTable: {e}")

    def close(self):
        self.cursor.close()
        self.connection.close()

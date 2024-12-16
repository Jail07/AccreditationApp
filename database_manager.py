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
            # Создаем таблицы, если их нет
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS MainTable (
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
                blacklist BOOLEAN DEFAULT FALSE
            );
            """)

            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS AccrTable (
                id SERIAL PRIMARY KEY,
                main_table_id INT NOT NULL REFERENCES MainTable(id),
                request_date TIMESTAMP NOT NULL DEFAULT NOW(),
                status TEXT DEFAULT 'Pending'
            );
            """)

            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Records (
                id SERIAL PRIMARY KEY,
                main_table_id INT NOT NULL REFERENCES MainTable(id),
                operation_type TEXT NOT NULL,
                operation_date TIMESTAMP NOT NULL DEFAULT NOW()
            );
            """)

            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS AccreditationPeriodTable (
                id SERIAL PRIMARY KEY,
                main_table_id INT NOT NULL REFERENCES MainTable(id),
                start_date TIMESTAMP NOT NULL,
                end_date TIMESTAMP NOT NULL
            );
            """)

            self.connection.commit()
            print("Таблицы успешно созданы или уже существуют")
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка при создании таблиц: {e}")

    def add_person(self, surname, name, middle_name, birth_date, birth_place, registration, organization, position):
        try:
            print(surname, name, middle_name, birth_date, birth_place, registration, organization, position)
            accreditation_end_date = datetime.now() + timedelta(days=180)
            # accreditation_end_date = datetime.now() - timedelta(days=1)

            # Добавляем запись в MainTable
            self.cursor.execute("""
            INSERT INTO MainTable (surname, name, middle_name, birth_date, birth_place, registration, organization, 
                                   position, added_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
            """, (surname, name, middle_name, birth_date, birth_place, registration, organization, position, datetime.now()))

            person_id = self.cursor.fetchone()[0]
            try:
                # Добавляем запись в AccreditationPeriodTable
                self.cursor.execute("""
                INSERT INTO AccreditationPeriodTable (main_table_id, start_date, end_date)
                VALUES (%s, %s, %s);
                """, (person_id, datetime.now(), accreditation_end_date))
            except Exception as e:
                print(e)
            self.connection.commit()
            print(f"{person_id} Сотрудник {surname} {name} успешно добавлен.")
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка при добавлении сотрудника: {e}")

    def find_matches(self, surname, name, middle_name, birth_date, organization):
        try:
            self.cursor.execute("""
            SELECT id FROM MainTable
            WHERE surname = %s AND name = %s AND middle_name = %s AND birth_date = %s AND organization = %s;
            """, (surname, name, middle_name, birth_date, organization))
            return self.cursor.fetchone() is not None
        except Exception as e:
            print(f"Ошибка при поиске сотрудника: {e}")
            self.connection.rollback()
            return False

    def get_people_for_recheck(self):
        try:
            query = """
                        SELECT m.id, m.surname, m.name, m.middle_name, m.birth_date, a.end_date
                        FROM MainTable m
                        JOIN AccreditationPeriodTable a ON m.id = a.main_table_id
                        WHERE a.end_date < NOW() AND (m.blacklist IS NULL OR NOT m.blacklist);
                    """
            self.cursor.execute(query)
            recheck_people = self.cursor.fetchall()
            print("Данные для перепроверки:", recheck_people)

            return recheck_people
        except Exception as e:
            print(f"Ошибка при получении данных для перепроверки: {e}")
            return []

    def save_valid_data(self, data):
        for row in data:
            print(row)
            try:
                surname = row.get('Фамилия')
                name = row.get('Имя')
                middle_name = row.get('Отчество')
                birth_date_str = row.get('Дата рождения')
                birth_place = row.get('Место рождения')
                registration = row.get('Регистрация')
                organization = row.get('Организация')
                position = row.get('Должность')

                if surname and name and birth_date_str and organization:
                    # Конвертация строки в объект datetime.date
                    try:
                        birth_date = datetime.strptime(birth_date_str, '%d.%m.%Y').date()
                    except ValueError:
                        print(f"Ошибка: Неверный формат даты рождения {birth_date_str}")
                        continue  # Пропускаем строку, если формат даты неверный

                    if not self.find_matches(surname, name, middle_name, birth_date, organization):
                        self.add_person(surname, name, middle_name, birth_date, birth_place, registration, organization,
                                        position)
            except Exception as e:
                print(f"Ошибка при сохранении строки: {e}")
                self.connection.rollback()  # Откат транзакции

    def get_person_id(self, fio, birth_date):
        """
        Ищет идентификатор пользователя по ФИО и дате рождения.
        """
        try:
            fio_parts = fio.split(' ')
            if len(fio_parts) != 3:
                print(f"Ошибка: ФИО должно содержать Фамилию, Имя и Отчество. Получено: {fio}")
                return None

            surname, name, middle_name = fio_parts
            self.cursor.execute(
                """
                SELECT id FROM accredited
                WHERE surname = %s AND name = %s AND middle_name = %s AND birth_date = %s;
                """,
                (surname, name, middle_name, birth_date)
            )
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            print(f"Ошибка при поиске пользователя в базе данных: {e}")
            self.connection.rollback()
            return None

    def log_transaction(self, main_table_id, operation_type):
        try:
            self.cursor.execute("""
            INSERT INTO Records (main_table_id, operation_type)
            VALUES (%s, %s);
            """, (main_table_id, operation_type))
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка при записи транзакции: {e}")

    def add_person_to_blacklist(self, surname, name, middle_name, birth_date):
        try:
            self.cursor.execute("""
            SELECT id FROM MainTable
            WHERE surname = %s AND name = %s AND middle_name = %s AND birth_date = %s;
            """, (surname, name, middle_name, birth_date))

            result = self.cursor.fetchone()
            if not result:
                print("Сотрудник не найден в базе данных.")
                return

            person_id = result[0]
            self.cursor.execute("""
            UPDATE MainTable
            SET blacklist = TRUE
            WHERE id = %s;
            """, (person_id,))

            self.log_transaction(person_id, "Added to Blacklist")
            self.connection.commit()
            print(f"Сотрудник {surname} {name} {middle_name} успешно добавлен в черный список.")
        except Exception as e:
            self.connection.rollback()
            print(f"Ошибка при добавлении в черный список: {e}")

    def close(self):
        self.cursor.close()
        self.connection.close()


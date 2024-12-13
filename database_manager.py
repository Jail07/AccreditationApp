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
            # Создаем таблицу, если ее нет
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS accredited (
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
                last_check_date TIMESTAMP,
                accreditation_end_date TIMESTAMP,
                blacklist BOOLEAN DEFAULT FALSE
            );
            """)
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                accredited_id INT NOT NULL REFERENCES accredited(id),
                check_date TIMESTAMP NOT NULL DEFAULT NOW(),
                operation_type TEXT NOT NULL
            );
            """)
            self.connection.commit()
            print("Таблицы успешно созданы или уже существуют")
        except Exception as e:
            self.connection.rollback()  # Откатываем изменения при ошибке
            print(f"Ошибка при создании таблиц: {e}")


    def add_person(self, surname, name, middle_name, birth_date, birth_place, registration, organization, position):
        accreditation_end_date = datetime.now() + timedelta(days=180)
        self.cursor.execute("""
        INSERT INTO accredited (surname, name, middle_name, birth_date, birth_place, registration, organization, 
                                position, last_check_date, accreditation_end_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (surname, name, middle_name, birth_date, birth_place, registration, organization, position,
              datetime.now(), accreditation_end_date))
        self.connection.commit()

    def find_matches(self, surname, name, middle_name, birth_date, organization):
        """
        Проверяет, существует ли сотрудник в базе данных.
        """
        self.cursor.execute("""
        SELECT id FROM accredited
        WHERE surname = %s AND name = %s AND middle_name = %s AND birth_date = %s AND organization = %s;
        """, (surname, name, middle_name, birth_date, organization))
        return self.cursor.fetchone() is not None

    def save_valid_data(self, data):
        for row in data:
            surname = row.get('Фамилия')
            name = row.get('Имя')
            middle_name = row.get('Отчество')
            birth_date = row.get('Дата рождения')
            birth_place = row.get('Место рождения')
            registration = row.get('Регистрация')
            organization = row.get('Организация')
            position = row.get('Должность')

            if surname and name and birth_date and organization:
                if not self.find_matches(surname, name, middle_name, birth_date, organization):
                    self.add_person(surname, name, middle_name, birth_date, birth_place, registration, organization, position)

    def get_people_for_recheck(self):
        query = """
        SELECT id, surname, name, middle_name, birth_date, accreditation_end_date
        FROM accredited
        WHERE accreditation_end_date < NOW() AND NOT blacklist;
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def log_transaction(self, accredited_id, operation_type):
        self.cursor.execute(
            """
            INSERT INTO transactions (accredited_id, operation_type)
            VALUES (%s, %s);
            """,
            (accredited_id, operation_type)
        )
        self.connection.commit()

    def get_person_id(self, fio, birth_date):
        """
        Ищет идентификатор пользователя по ФИО и дате рождения.
        """
        try:
            surname, name, middle_name = fio.split(' ')
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
            return None



    def close(self):
        self.cursor.close()
        self.connection.close()

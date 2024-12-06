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
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS accredited (
            id SERIAL PRIMARY KEY,
            fio TEXT NOT NULL,
            birth_date DATE NOT NULL,
            added_date TIMESTAMP NOT NULL DEFAULT NOW(),
            last_check_date TIMESTAMP,
            accreditation_end_date TIMESTAMP,
            blacklist BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            accredited_id INT NOT NULL REFERENCES accredited(id),
            check_date TIMESTAMP NOT NULL DEFAULT NOW(),
            operation_type TEXT NOT NULL
        );
        """)
        self.connection.commit()

    def add_person(self, fio, birth_date, last_check_date=None):
        accreditation_end_date = datetime.now() + timedelta(days=180)
        self.cursor.execute("""
        INSERT INTO accredited (fio, birth_date, last_check_date, accreditation_end_date)
        VALUES (%s, %s, %s, %s)
        RETURNING id;
        """, (fio, birth_date, last_check_date, accreditation_end_date))
        person_id = self.cursor.fetchone()[0]
        self.connection.commit()
        return person_id

    def move_to_blacklist(self, person_id):
        self.cursor.execute("""
        UPDATE accredited
        SET blacklist = TRUE
        WHERE id = %s;
        """, (person_id,))
        self.connection.commit()

    def get_people_for_recheck(self):
        self.cursor.execute("""
        SELECT id, fio, birth_date, accreditation_end_date
        FROM accredited
        WHERE blacklist = FALSE AND accreditation_end_date < NOW();
        """)
        return self.cursor.fetchall()

    def log_transaction(self, person_id, operation_type):
        self.cursor.execute("""
        INSERT INTO transactions (accredited_id, operation_type)
        VALUES (%s, %s);
        """, (person_id, operation_type))
        self.connection.commit()

    def close(self):
        self.cursor.close()
        self.connection.close()

import pandas as pd
from faker import Faker
import random

fake = Faker('ru_RU')

# Генерация данных для 15 человек
data = []
for i in range(1, 16):
    full_name = fake.name().split()
    if len(full_name) == 3:
        last_name, first_name, middle_name = full_name
    else:
        # иногда faker генерирует имя без отчества
        last_name = full_name[0]
        first_name = full_name[1]
        middle_name = fake.first_name() + 'ович'  # создаем искусственное отчество

    birth_date = fake.date_of_birth(minimum_age=20, maximum_age=60).strftime('%d.%m.%Y')
    birth_place = fake.city()
    registration = fake.address().replace("\n", ", ")
    organization = fake.company()
    position = random.choice([
        "Инженер", "Менеджер", "Разработчик", "Аналитик", "Бухгалтер",
        "Тестировщик", "Системный администратор", "Маркетолог", "Проект-менеджер", "HR-специалист"
    ])

    data.append([
        i, last_name, first_name, middle_name, birth_date,
        birth_place, registration, organization, position
    ])

# Создание DataFrame
columns = [
    "№ п/п", "Фамилия", "Имя", "Отчество", "Дата рождения",
    "Место рождения", "Регистрация", "Организация", "Должность"
]
df = pd.DataFrame(data, columns=columns)

# Сохранение в Excel
file_path = "./files/Данные_на_проверку.xlsx"
df.to_excel(file_path, index=False)


# data_processing.py
from datetime import datetime, date

import pandas as pd
import re
import unicodedata
import logging
from config import get_logger

class DataProcessor:
    def __init__(self):
        self.logger = get_logger(__name__)
        # Паттерн для разрешенных символов в ФИО (кириллица, латиница, пробел, дефис)
        self.fio_whitelist_pattern = re.compile(r"[^a-zA-Zа-яА-ЯёЁ\s-]")
        # Паттерны для "подозрительных" имен
        self.suspicious_patterns = [
            re.compile(r"\s{2,}"),          # Два или более пробела подряд
            re.compile(r"-{2,}"),           # Два или более дефиса подряд
            re.compile(r"\s-"),            # Пробел перед дефисом
            re.compile(r"-\s"),            # Пробел после дефиса
            re.compile(r"^\s|\s$"),        # Пробел в начале или конце (после strip) - маловероятно
            re.compile(r"^-|-$"),          # Дефис в начале или конце
            re.compile(r"[^-a-zA-Zа-яА-ЯёЁ\s]"), # Любой символ, не входящий в базовый набор
        ]

    def clean_string(self, value, use_whitelist=False):
        """Очищает строку: удаляет доп. пробелы, нормализует дефисы и Unicode."""
        if not isinstance(value, str):
            # Попытка преобразовать в строку, если возможно
            try:
                value = str(value)
            except:
                 return value # Возвращаем исходное, если не строка и не конвертируется

        # 1. Удаление невидимых символов и нормализация Unicode
        value = value.replace('\u00A0', ' ') # Замена nbsp на обычный пробел
        try:
            # Нормализация к NFC для стандартного представления комбинированных символов
            value = unicodedata.normalize('NFC', value)
            # Удаление управляющих символов и других нежелательных (кроме пробела и дефиса)
            value = ''.join(ch for ch in value if unicodedata.category(ch)[0] != 'C' or ch in [' ', '-'])
        except Exception as e:
            self.logger.warning(f"Ошибка нормализации Unicode для '{value[:50]}...': {e}")
            # Продолжаем без нормализации в случае ошибки

        # 2. Нормализация пробелов и дефисов
        value = re.sub(r'\s+', ' ', value).strip() # Замена множественных пробелов на один, удаление по краям
        value = re.sub(r'\s*-\s*', '-', value) # Удаление пробелов вокруг дефиса

        # 3. (Опционально) Применение белого списка для ФИО
        if use_whitelist:
             original_value = value
             value = self.fio_whitelist_pattern.sub("", value) # Удаляем все, что не разрешено
             if value != original_value:
                 self.logger.debug(f"Применен whitelist: '{original_value}' -> '{value}'")
             # Повторная очистка пробелов после whitelist
             value = re.sub(r'\s+', ' ', value).strip()

        return value

    def normalize_date(self, date_input):
        """Нормализует дату к объекту date или возвращает None."""
        if pd.isna(date_input) or date_input == '':
            return None
        try:
            # Пытаемся обработать как строку
            if isinstance(date_input, str):
                # Удаляем лишние символы, оставляя цифры и точки/дефисы/слеши
                clean_date_str = re.sub(r'[^\d./-]', '', date_input).strip()
                # Пробуем разные форматы, включая dayfirst=True
                date_obj = pd.to_datetime(clean_date_str, dayfirst=True, errors='coerce')
                if pd.isna(date_obj): # Если не удалось с dayfirst=True, пробуем без
                    date_obj = pd.to_datetime(clean_date_str, errors='coerce')
            # Пытаемся обработать как уже существующую дату/время
            elif isinstance(date_input, (datetime, date)):
                 date_obj = pd.to_datetime(date_input, errors='coerce')
            # Обработка числовых форматов Excel
            elif isinstance(date_input, (int, float)):
                 date_obj = pd.to_datetime(date_input, unit='D', origin='1899-12-30', errors='coerce')
            else:
                date_obj = None

            # Проверка валидности и диапазона года
            if pd.notna(date_obj) and 1900 <= date_obj.year <= datetime.now().year + 1:
                return date_obj.date() # Возвращаем только дату
            else:
                 self.logger.warning(f"Не удалось нормализовать дату или она вне диапазона: {date_input}")
                 return None
        except Exception as e:
            self.logger.error(f"Ошибка при нормализации даты '{date_input}': {e}")
            return None

    def clean_dataframe(self, df):
        """Применяет очистку строк и нормализацию дат к DataFrame."""
        self.logger.info("Начало очистки DataFrame.")
        cleaned_df = df.copy() # Работаем с копией

        # Определение колонок для очистки строк (примерный список)
        string_columns = ['Фамилия', 'Имя', 'Отчество', 'Организация', 'Должность', 'Место рождения', 'Адрес регистрации']
        # Определение колонки с датой
        date_column = 'Дата рождения'

        for col in cleaned_df.columns:
            # Применяем очистку строк к текстовым колонкам
            if col in string_columns:
                # Применяем whitelist только к ФИО
                use_wl = col in ['Фамилия', 'Имя', 'Отчество']
                cleaned_df[col] = cleaned_df[col].apply(lambda x: self.clean_string(x, use_whitelist=use_wl))
                # Заменяем пустые строки на None для консистентности
                cleaned_df[col] = cleaned_df[col].replace('', None)

            # Применяем нормализацию даты
            elif col == date_column:
                cleaned_df[col] = cleaned_df[col].apply(self.normalize_date)

            # Можно добавить очистку для других типов колонок (числа и т.д.)

        self.logger.info("Очистка DataFrame завершена.")
        return cleaned_df

    def detect_unusual_names(self, df, columns=['Фамилия', 'Имя', 'Отчество']):
        """
        Ищет строки с "подозрительными" паттернами в указанных колонках ФИО.
        Возвращает список индексов таких строк.
        """
        suspicious_indices = set()
        self.logger.info("Поиск строк с необычными именами...")
        for col in columns:
            if col in df.columns:
                for index, value in df[col].dropna().items(): # Пропускаем NaN
                    if isinstance(value, str): # Убедимся, что это строка
                         for pattern in self.suspicious_patterns:
                            if pattern.search(value):
                                self.logger.warning(f"Обнаружен подозрительный паттерн '{pattern.pattern}' в '{col}': '{value}' (индекс: {index})")
                                suspicious_indices.add(index)
                                break # Переходим к следующему значению после первого совпадения
        self.logger.info(f"Найдено {len(suspicious_indices)} строк с потенциально необычными именами.")
        return list(suspicious_indices)

    def validate_data(self, df):
        """
        Проверяет наличие обязательных полей после очистки.
        Возвращает DataFrame с колонкой 'Validation_Errors'.
        """
        self.logger.info("Проверка наличия обязательных полей.")
        validated_df = df.copy()
        required_fields = ['Фамилия', 'Имя', 'Дата рождения', 'Организация']
        errors = []

        for index, row in validated_df.iterrows():
            row_errors = []
            for field in required_fields:
                if field not in row or pd.isna(row[field]) or str(row[field]).strip() == '':
                    row_errors.append(f"Отсутствует '{field}'")
            errors.append("; ".join(row_errors) if row_errors else None)

        validated_df['Validation_Errors'] = errors
        num_invalid = validated_df['Validation_Errors'].notna().sum()
        if num_invalid > 0:
             self.logger.warning(f"Обнаружено {num_invalid} строк с ошибками валидации.")
        else:
            self.logger.info("Ошибок валидации обязательных полей не найдено.")
        return validated_df
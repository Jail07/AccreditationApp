import pandas as pd
from datetime import datetime
import re
import unicodedata


class DataProcessor:

    def detectSequentialDates(self, df, date_column='Дата рождения', max_allowed_sequence=2):
        """
        Находит строки с последовательными датами (увеличение на день или год).
        Выделяет строки, где больше `max_allowed_sequence` последовательных дат.
        """
        if date_column not in df.columns:
            return []

        suspicious_indices = []
        sequential_count = 1

        for i in range(1, len(df)):
            try:
                current_date = pd.to_datetime(df.iloc[i][date_column], dayfirst=True, errors='coerce')
                prev_date = pd.to_datetime(df.iloc[i - 1][date_column], dayfirst=True, errors='coerce')

                if pd.notna(current_date) and pd.notna(prev_date):
                    # Проверка на последовательность дат
                    if (current_date - prev_date).days == 1 or (current_date.year - prev_date.year == 1):
                        sequential_count += 1
                    else:
                        sequential_count = 1

                    # Если последовательность превышает допустимый лимит, отмечаем строки
                    if sequential_count > max_allowed_sequence:
                        suspicious_indices.extend(range(i - max_allowed_sequence, i + 1))
                else:
                    sequential_count = 1
            except Exception:
                sequential_count = 1

        return list(set(suspicious_indices))

    def removeExtraSpaces(self, value):
        if not isinstance(value, str):
            return value
        value = value.replace('\u00A0', ' ')
        value = re.sub(r'\s*-\s*', '-', value)
        value = value.strip()
        value = re.sub(r'\s+', ' ', value)
        value = unicodedata.normalize('NFD', value)
        value = ''.join(ch for ch in value if not unicodedata.combining(ch))
        return value

    def cleanData(self, df):
        for col in df.columns:
            if col in ['№ п/п']:
                continue
            df[col] = df[col].apply(lambda x: self.removeExtraSpaces(str(x)) if pd.notna(x) else None)
        df['Дата рождения'] = df['Дата рождения'].apply(lambda x: self.normalizeDate(x) if pd.notna(x) else None)
        return df

    def normalizeDate(self, date_str):
        if pd.isna(date_str) or date_str == '':
            return None
        date_str = re.sub(r'[^\d.]', '', str(date_str)).strip()
        try:
            date_obj = pd.to_datetime(date_str, errors='coerce', dayfirst=True)
            if pd.notna(date_obj) and date_obj.year >= 1900:
                return date_obj.strftime('%d.%m.%Y')
            return None
        except Exception:
            return None


    def checkDuplicates(self, df):
        required_columns = ['Фамилия', 'Имя', 'Отчество', 'Дата рождения']
        if all(col in df.columns for col in required_columns):
            valid_rows = df.dropna(subset=required_columns)
            df['Повтор'] = df.index.isin(
                valid_rows[valid_rows.duplicated(subset=required_columns, keep=False)].index
            )
        return df

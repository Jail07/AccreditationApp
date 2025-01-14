import pandas as pd
import re
import unicodedata


class DataProcessor:
    def removeExtraSpaces(self, value):
        if not isinstance(value, str):
            return value
        value = value.replace('\u00A0', ' ')
        value = re.sub(r'\s*-\s*', '-', value)
        value = re.sub(r'\s+', ' ', value).strip()
        value = unicodedata.normalize('NFD', value)
        value = ''.join(ch for ch in value if not unicodedata.combining(ch))
        return value

    def normalizeDate(self, date_str):
        if pd.isna(date_str) or date_str == '':
            return None
        try:
            clean_date_str = re.sub(r'[^\d.]', '', str(date_str)).strip()
            date_obj = pd.to_datetime(clean_date_str, dayfirst=True, errors='coerce')
            if pd.notna(date_obj) and date_obj.year >= 1900:
                return date_obj.date()
            return None
        except Exception:
            return None

    def cleanData(self, df):
        for col in df.columns:
            if col in ['Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Организация']:
                df[col] = df[col].apply(lambda x: self.removeExtraSpaces(str(x)) if pd.notna(x) else None)
            if col == 'Дата рождения':
                df[col] = df[col].apply(self.normalizeDate)
        return df

    def checkDuplicates(self, df, ref_df=None, key_columns=None, log_func=None):
        if key_columns is None:
            key_columns = ['Фамилия', 'Имя', 'Отчество', 'Дата рождения', 'Организация']
        if ref_df is None:
            ref_df = pd.DataFrame(columns=key_columns)

        duplicates = df.duplicated(subset=key_columns, keep=False)
        for idx in df.index[duplicates]:
            log_func(f"Дублирование внутри файла: строка {idx + 1}. Удалена строка.")
        df = df[~duplicates | ~df.duplicated(subset=key_columns, keep='first')]

        ref_duplicates = df[key_columns].apply(tuple, axis=1).isin(ref_df[key_columns].apply(tuple, axis=1))
        for idx in df.index[ref_duplicates]:
            df.at[idx, 'Статус'] = "уже есть в БД"
            log_func(f"Дублирование с БД: строка {idx + 1}. Статус изменен на 'уже есть в БД'.")

        for idx in df.index[~duplicates & ~ref_duplicates]:
            df.at[idx, 'Статус'] = "Корректные данные"

        return df

    def detectSequentialDates(self, df, date_column='Дата рождения', max_allowed_sequence=2):
        if date_column not in df.columns:
            return []

        suspicious_indices = []
        sequential_count = 1
        duplicate_count = 1

        for i in range(1, len(df)):
            try:
                current_date = pd.to_datetime(df.iloc[i][date_column], dayfirst=True, errors='coerce')
                prev_date = pd.to_datetime(df.iloc[i - 1][date_column], dayfirst=True, errors='coerce')

                if pd.notna(current_date) and pd.notna(prev_date):
                    if (current_date - prev_date).days == 1 or (current_date.year - prev_date.year == 1):
                        sequential_count += 1
                    else:
                        sequential_count = 1

                    if current_date == prev_date:
                        duplicate_count += 1
                    else:
                        duplicate_count = 1

                    if sequential_count > max_allowed_sequence:
                        suspicious_indices.extend(range(i - max_allowed_sequence, i + 1))
                    if duplicate_count >= 3:
                        suspicious_indices.extend(range(i - duplicate_count + 1, i + 1))
                else:
                    sequential_count = 1
                    duplicate_count = 1
            except Exception:
                sequential_count = 1
                duplicate_count = 1

        return list(set(suspicious_indices))


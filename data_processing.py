import re
import pandas as pd
import unicodedata
from datetime import datetime


class DataProcessor:
    def cleanData(self, df):
        for col in df.columns:
            if col == '№ п/п':
                df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) and str(x).isdigit() else x)
            elif col == 'Дата рождения':
                df[col] = df[col].apply(lambda x: self.normalizeDate(x) if pd.notna(x) else None)
            else:
                df[col] = df[col].apply(lambda x: self.removeExtraSpaces(str(x)) if pd.notna(x) else None)
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

    def removeExtraSpaces(self, value):
        value = value.replace('\u00A0', ' ')
        value = unicodedata.normalize('NFD', value)
        value = ''.join(ch for ch in value if not unicodedata.combining(ch))
        value = value.strip()
        value = re.sub(r'\s*-\s*', '-', value)
        value = re.sub(r'\s+', ' ', value)
        return value

    def checkDuplicates(self, df):
        required_columns = ['Фамилия', 'Имя', 'Дата рождения']
        if all(col in df.columns for col in required_columns):
            valid_rows = df.dropna(subset=required_columns)
            df['Повтор'] = df.index.isin(
                valid_rows[valid_rows.duplicated(subset=required_columns, keep=False)].index
            )
        return df

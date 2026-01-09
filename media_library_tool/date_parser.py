"""
Парсинг дат из имён файлов и EXIF строк.
"""

import datetime
from pathlib import Path
from typing import Optional, Tuple

from .constants import (
    MIN_VALID_YEAR,
    RE_YMD_HMS_MS,
    RE_DDMMYYYY_HHMM,
    RE_YYYYMMDD_HHMMSS_X,
    RE_WHATSAPP,
    RE_SIGNAL_STYLE,
    RE_FULLY_HYPHENATED,
    RE_CONTINUOUS_14,
    RE_YMD_NOON,
    RE_8_DIGITS,
    RE_DMY_SEP,
    RE_TIMESTAMP_MS,
    RE_FOLDER_FULL_DATE,
    RE_FOLDER_MONTH,
    RE_FOLDER_YEAR,
)


class DateParser:
    """Парсер дат из имён файлов."""
    
    def __init__(self, max_year: Optional[int] = None):
        """
        Args:
            max_year: Максимально допустимый год (по умолчанию текущий + 1)
        """
        self._max_year = max_year or (datetime.datetime.now().year + 1)
    
    @property
    def max_year(self) -> int:
        return self._max_year
    
    def _valid_date(
        self, y: int, mo: int, d: int, h: int = 0, mi: int = 0, s: int = 0
    ) -> Optional[datetime.datetime]:
        """Создать datetime если параметры валидны."""
        try:
            if y < MIN_VALID_YEAR or y > self._max_year:
                return None
            return datetime.datetime(y, mo, d, h, mi, s)
        except (ValueError, OverflowError):
            return None
    
    def get_date_from_filename(self, path: Path) -> Optional[datetime.datetime]:
        """
        Извлечь дату из имени файла.
        
        Порядок приоритета паттернов:
        1. YMD HMS ms (17+ цифр)
        2. DDMMYYYY-HHmm (WhatsApp стиль)
        3. YYYYMMDD_HHmmSSx (Android)
        4. IMG-YYYYMMDD-WA (WhatsApp)
        5. YYYY-MM-DD-HHmmSS (Signal)
        6. Полностью дефисный формат
        7. YMD с разделителями
        8. 14 цифр подряд
        9. YMD только дата
        10. 8 цифр (YYYYMMDD или DDMMYYYY)
        11. DMY с разделителями
        12. Unix timestamp (13 цифр)
        """
        name = path.stem
        
        # 0. YMD HMS ms (Continuous 17+ digits)
        m = RE_YMD_HMS_MS.search(name)
        if m:
            d = self._valid_date(
                int(m.group(1)), int(m.group(2)), int(m.group(3)),
                int(m.group(4)), int(m.group(5)), int(m.group(6))
            )
            if d:
                return d
        
        # 1. DDMMYYYY-HHmm
        m = RE_DDMMYYYY_HHMM.search(name)
        if m:
            d = self._valid_date(
                int(m.group(3)), int(m.group(2)), int(m.group(1)),
                int(m.group(4)), int(m.group(5)), 0
            )
            if d:
                return d
        
        # 2. YYYYMMDD_HHmmSSx
        m = RE_YYYYMMDD_HHMMSS_X.search(name)
        if m:
            d = self._valid_date(
                int(m.group(1)), int(m.group(2)), int(m.group(3)),
                int(m.group(4)), int(m.group(5)), int(m.group(6))
            )
            if d:
                return d
        
        # 3. WhatsApp pattern
        m = RE_WHATSAPP.search(name)
        if m:
            s_val = m.group(1)
            y, mo, day = int(s_val[0:4]), int(s_val[4:6]), int(s_val[6:8])
            d = self._valid_date(y, mo, day, 12, 0, 0)
            if d:
                return d
        
        # 4. Signal style: YYYY-MM-DD-HHmmSS
        m = RE_SIGNAL_STYLE.search(name)
        if m:
            d = self._valid_date(*map(int, m.groups()))
            if d:
                return d
        
        # 5. Fully hyphenated: YYYY-MM-DD-HH-mm-SS
        m = RE_FULLY_HYPHENATED.search(name)
        if m:
            d = self._valid_date(*map(int, m.groups()))
            if d:
                return d
        
        # 6. Continuous 14 digits
        m = RE_CONTINUOUS_14.search(name)
        if m:
            d = self._valid_date(*map(int, m.groups()))
            if d:
                return d
        
        # 7. YMD noon (date only)
        m = RE_YMD_NOON.search(name)
        if m:
            d = self._valid_date(int(m.group(1)), int(m.group(2)), int(m.group(3)), 12, 0, 0)
            if d:
                return d
        
        # 8. 8 digits: try YYYYMMDD then DDMMYYYY
        m = RE_8_DIGITS.search(name)
        if m:
            s_val = m.group(1)
            # Try YYYYMMDD
            y, mo, day = int(s_val[0:4]), int(s_val[4:6]), int(s_val[6:8])
            d = self._valid_date(y, mo, day, 12, 0, 0)
            if d:
                return d
            # Try DDMMYYYY
            day, mo, y = int(s_val[0:2]), int(s_val[2:4]), int(s_val[4:8])
            d = self._valid_date(y, mo, day, 12, 0, 0)
            if d:
                return d
        
        # 9. DMY with separators
        m = RE_DMY_SEP.search(name)
        if m:
            d = self._valid_date(int(m.group(3)), int(m.group(2)), int(m.group(1)), 12, 0, 0)
            if d:
                return d
        
        # 10. Timestamp ms (13 digits)
        m = RE_TIMESTAMP_MS.search(name)
        if m:
            try:
                ms = int(m.group(1))
                ts = datetime.datetime.fromtimestamp(ms / 1000.0).replace(microsecond=0)
                if MIN_VALID_YEAR <= ts.year <= self._max_year:
                    return ts
            except (OSError, ValueError, OverflowError):
                pass
        
        return None
    
    def get_folder_date(self, folder_name: str) -> Tuple[Optional[datetime.datetime], Optional[str]]:
        """
        Извлечь дату из имени папки.
        
        Returns:
            (datetime, granularity) где granularity = "Day", "Month", или "Year"
            (None, None) если не удалось распарсить
        """
        # YYYY-MM-DD
        m = RE_FOLDER_FULL_DATE.match(folder_name)
        if m:
            y, mo, d = map(int, m.groups())
            if y >= MIN_VALID_YEAR:
                return datetime.datetime(y, mo, d, 12, 0, 0), "Day"
        
        # YYYY-MM
        m = RE_FOLDER_MONTH.match(folder_name)
        if m:
            y, mo = map(int, m.groups())
            if y >= MIN_VALID_YEAR:
                return datetime.datetime(y, mo, 1, 12, 0, 0), "Month"
        
        # YYYY
        m = RE_FOLDER_YEAR.match(folder_name)
        if m:
            y = int(m.group(1))
            if y >= MIN_VALID_YEAR:
                return datetime.datetime(y, 1, 1, 12, 0, 0), "Year"
        
        return None, None


def parse_exif_date(raw_str: Optional[str]) -> Optional[datetime.datetime]:
    """
    Распарсить дату из EXIF строки.
    
    Поддерживаемые форматы:
    - "2024:12:15 12:00:00"
    - "2024-12-15 12:00:00"
    """
    if not raw_str:
        return None
    
    s = str(raw_str)[:19]  # Обрезаем timezone suffix
    
    for fmt in ("%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.datetime.strptime(s, fmt)
        except ValueError:
            continue
    
    return None


def format_exif_datetime(dt: datetime.datetime) -> str:
    """Форматировать datetime для ExifTool."""
    return dt.strftime("%Y:%m:%d %H:%M:%S")

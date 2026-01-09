"""
Константы и прекомпилированные регулярные выражения.
"""

import re

# ---------------- Configuration ----------------
MIN_VALID_YEAR = 1900
SHORT_EVENT_DAYS = 3

# Extensions that are not media and should be ignored early
IGNORE_EXTENSIONS = frozenset({'.ini', '.db', '.tmp', '.xmp'})

# Files where ExifTool typically doesn't write EXIF; we will set filesystem times via PowerShell
FS_ONLY_EXTENSIONS = frozenset({'.avi', '.bmp', '.crq', '.thm', '.wav'})

# Assigning timestamps to missing-date files
DEFAULT_ASSIGN_STEP_SECONDS = 2
DEFAULT_ASSIGN_WINDOW_HOURS = 6

# ---------------- Precompiled Regex Patterns ----------------
# Все паттерны с границами для предотвращения ложных срабатываний

# 0. YMD HMS ms (Continuous 17+ digits): 201612151436015000
RE_YMD_HMS_MS = re.compile(r'(?<!\d)(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d+)(?!\d)')

# DDMMYYYY-HHmm (e.g. 21072022-1937) - Common in WhatsApp/Socials dump
RE_DDMMYYYY_HHMM = re.compile(r'(?<!\d)(\d{2})(\d{2})(\d{4})[-_](\d{2})(\d{2})(?!\d)')

# YYYYMMDD_HHmmSSx (e.g. 20150701_1651432)
RE_YYYYMMDD_HHMMSS_X = re.compile(r'(?<!\d)(\d{4})(\d{2})(\d{2})[-_](\d{2})(\d{2})(\d{2})(\d+)?(?!\d)')

# WhatsApp (IMG-YYYYMMDD-WA...) or similar "Date-Only" strong pattern
RE_WHATSAPP = re.compile(r'(?:IMG|VID)[-_](\d{8})[-_]WA', re.IGNORECASE)

# Hyphenated full with continuous time (Signal style): YYYY-MM-DD-HHmmSS
RE_SIGNAL_STYLE = re.compile(r'(\d{4})[-_](\d{2})[-_](\d{2})[-_](\d{2})(\d{2})(\d{2})')

# Fully hyphenated: YYYY-MM-DD-HH-mm-SS
RE_FULLY_HYPHENATED = re.compile(r'(\d{4})[-_](\d{2})[-_](\d{2})[-_](\d{2})[-_](\d{2})[-_](\d{2})')

# YMD HMS with separators
RE_YMD_HMS_SEP = re.compile(r'(\d{4})[-_.](\\d{2})[-_.](\\d{2})[-_.\s]+(\d{2})[-_.]?(\d{2})[-_.]?(\d{2})')

# Continuous 14 digits: 20180407160644 (bounds avoid hash matching)
RE_CONTINUOUS_14 = re.compile(r'(?<!\d)(\d{4})(\d{2})(\d{2})[-_]?(\d{2})(\d{2})(\d{2})(?!\d)')

# YMD noon (date only)
RE_YMD_NOON = re.compile(r'(?<!\d)(\d{4})[-_.](\d{2})[-_.](\d{2})(?!\d)')

# YMD continuous 8 digits OR DDMMYYYY continuous 8 digits
RE_8_DIGITS = re.compile(r'(?<!\d)(\d{8})(?!\d)')

# DMY with separators
RE_DMY_SEP = re.compile(r'(?<!\d)(\d{2})[-_.](\d{2})[-_.](\d{4})(?!\d)')

# Timestamp ms (13 digits, starting with 15/16/17)
RE_TIMESTAMP_MS = re.compile(r'(?<!\d)(1[5-7]\d{11})(?!\d)')

# Folder date patterns
RE_FOLDER_FULL_DATE = re.compile(r'^(\d{4})[-._](\d{2})[-._](\d{2})')
RE_FOLDER_MONTH = re.compile(r'^(\d{4})[-._](\d{2})')
RE_FOLDER_YEAR = re.compile(r'^(\d{4})')

# Folder name cleanup
RE_FOLDER_PREFIX = re.compile(r'^\d{4}([-._]\d{2})?([-._]\d{2})?(\s*[-._]\s*|\s+)?')

# Windows reserved filenames
WINDOWS_RESERVED_NAMES = frozenset(
    {"CON", "PRN", "AUX", "NUL"} | 
    {f"COM{i}" for i in range(1, 10)} | 
    {f"LPT{i}" for i in range(1, 10)}
)

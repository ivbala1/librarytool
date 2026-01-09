"""
Media Library Tool - модульная версия.

Инструмент для организации медиабиблиотеки:
- Исправление дат файлов на основе имени файла или EXIF
- Переименование папок событий
- Обнаружение и перемещение выбросов (outliers)
"""

from .app import MediaLibraryTool
from .processor import MediaProcessor
from .stats import Stats
from .logger import Logger

__version__ = "2.4.0"
__all__ = ["MediaLibraryTool", "MediaProcessor", "Stats", "Logger"]

#!/usr/bin/env python3
"""
MediaLibraryTool - Launcher

Инструмент для организации медиабиблиотеки:
- Исправление дат файлов на основе имени файла или EXIF
- Переименование папок событий  
- Обнаружение и перемещение выбросов (outliers)

Использование:
    python MediaLibraryTool.pyw
    или двойной клик по файлу
"""

from media_library_tool.app import MediaLibraryTool

if __name__ == "__main__":
    app = MediaLibraryTool()
    app.mainloop()

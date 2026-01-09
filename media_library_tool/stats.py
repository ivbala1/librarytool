"""
Thread-safe статистика обработки.
"""

import threading
from dataclasses import dataclass, field


@dataclass
class Stats:
    """
    Потокобезопасная статистика обработки медиафайлов.
    Все операции += защищены блокировкой.
    """
    
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    
    # Основные счётчики
    folders_processed: int = 0
    files_total: int = 0
    files_fixed_date: int = 0
    files_renamed_dng: int = 0
    files_ghost_ignored: int = 0
    folders_renamed: int = 0
    folders_merged: int = 0
    merge_conflicts: int = 0
    files_moved: int = 0
    errors: int = 0
    
    # Breakdown of date fixes
    files_fixed_filename: int = 0
    files_fixed_interpolated: int = 0
    files_fixed_shifted: int = 0
    files_sanitized: int = 0
    
    def reset(self) -> None:
        """Сброс всех счётчиков."""
        with self._lock:
            self.folders_processed = 0
            self.files_total = 0
            self.files_fixed_date = 0
            self.files_renamed_dng = 0
            self.files_ghost_ignored = 0
            self.folders_renamed = 0
            self.folders_merged = 0
            self.merge_conflicts = 0
            self.files_moved = 0
            self.errors = 0
            self.files_fixed_filename = 0
            self.files_fixed_interpolated = 0
            self.files_fixed_shifted = 0
            self.files_sanitized = 0
    
    def increment(self, field_name: str, value: int = 1) -> None:
        """Потокобезопасное увеличение счётчика."""
        with self._lock:
            current = getattr(self, field_name, 0)
            setattr(self, field_name, current + value)
    
    # Удобные методы для частых операций
    def add_error(self) -> None:
        self.increment('errors')
    
    def add_folder_processed(self) -> None:
        self.increment('folders_processed')
    
    def add_files_total(self, count: int = 1) -> None:
        self.increment('files_total', count)
    
    def add_fixed_date(self, count: int = 1) -> None:
        self.increment('files_fixed_date', count)
    
    def add_fixed_filename(self, count: int = 1) -> None:
        self.increment('files_fixed_filename', count)
    
    def add_fixed_interpolated(self, count: int = 1) -> None:
        self.increment('files_fixed_interpolated', count)
    
    def add_fixed_shifted(self, count: int = 1) -> None:
        self.increment('files_fixed_shifted', count)
    
    def add_sanitized(self, count: int = 1) -> None:
        self.increment('files_sanitized', count)
    
    def add_moved(self, count: int = 1) -> None:
        self.increment('files_moved', count)
    
    def add_folder_renamed(self) -> None:
        self.increment('folders_renamed')
    
    def add_folder_merged(self) -> None:
        self.increment('folders_merged')
    
    def add_merge_conflict(self) -> None:
        self.increment('merge_conflicts')
    
    def add_ghost_ignored(self) -> None:
        self.increment('files_ghost_ignored')
    
    def add_dng_renamed(self) -> None:
        self.increment('files_renamed_dng')

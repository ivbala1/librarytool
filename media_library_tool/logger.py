"""
Логирование в GUI и файл.
"""

import datetime
from pathlib import Path
from queue import Queue
from typing import Optional, Protocol, Tuple


class LogQueueProtocol(Protocol):
    """Протокол для очереди логов GUI."""
    def put(self, item: Tuple[str, Optional[str]]) -> None: ...


class Logger:
    """
    Логгер с записью в GUI очередь и файл.
    
    Decoupled от GUI через протокол LogQueueProtocol.
    """
    
    def __init__(self, log_queue: LogQueueProtocol, log_file: Path):
        """
        Args:
            log_queue: Очередь для сообщений GUI (thread-safe)
            log_file: Путь к файлу лога
        """
        self.log_queue = log_queue
        self.log_file = log_file
        self._file_error_shown = False
    
    def log(self, msg: str, color: Optional[str] = None) -> None:
        """
        Записать сообщение в лог.
        
        Args:
            msg: Текст сообщения
            color: Цвет для GUI (blue, red, green, orange, magenta, gray)
        """
        # 1) GUI queue
        self.log_queue.put((msg, color))
        
        # 2) File
        try:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(f"[{timestamp}] {msg}\n")
        except OSError:
            if not self._file_error_shown:
                self._file_error_shown = True
                try:
                    self.log_queue.put(("WARNING: Cannot write to log file!", "red"))
                except Exception:
                    pass
    
    def info(self, msg: str) -> None:
        """Информационное сообщение (синий)."""
        self.log(msg, "blue")
    
    def success(self, msg: str) -> None:
        """Успешное действие (зелёный)."""
        self.log(msg, "green")
    
    def warning(self, msg: str) -> None:
        """Предупреждение (оранжевый)."""
        self.log(msg, "orange")
    
    def error(self, msg: str) -> None:
        """Ошибка (красный)."""
        self.log(msg, "red")
    
    def debug(self, msg: str) -> None:
        """Отладочное сообщение (серый)."""
        self.log(msg, "gray")
    
    def action(self, msg: str) -> None:
        """Действие (пурпурный)."""
        self.log(msg, "magenta")

"""
GUI приложение MediaLibraryTool на базе CustomTkinter.
"""

import datetime
import json
import os
import queue
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext, simpledialog
from typing import Optional

import customtkinter as ctk

from .exif_handler import StopRequested
from .logger import Logger
from .processor import MediaProcessor
from .stats import Stats


class MediaLibraryTool(ctk.CTk):
    """
    Главное окно приложения MediaLibraryTool.
    
    Использует CustomTkinter для современного тёмного интерфейса.
    """
    
    def __init__(self):
        super().__init__()
        
        # CTK Configuration
        ctk.set_appearance_mode("Dark")
        ctk.set_default_color_theme("blue")
        
        self.title("MediaLibraryTool (Modern)")
        self.geometry("1100x800")
        
        self.script_dir = Path(__file__).parent.parent.absolute()
        self.default_exif = self.script_dir / "exiftool" / "exiftool.exe"
        
        self.log_queue: queue.Queue = queue.Queue()
        self.stop_requested = False
        self.worker_thread: Optional[threading.Thread] = None
        
        self.stats = Stats()
        self.logger = Logger(self.log_queue, self.script_dir / "MediaLibraryTool.log")
        
        self._init_ui()
        self._stats_error_shown = False
        self._show_welcome_message()
        self._check_queue()
        self._bind_global_shortcuts()
        
        # Load Config
        self.config_file = self.script_dir / "config.json"
        self.load_config()
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
    
    def load_config(self) -> None:
        """Загрузить конфигурацию из JSON файла."""
        if not self.config_file.exists():
            return
        try:
            with open(self.config_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            if "root" in data:
                self.entry_root.delete(0, tk.END)
                self.entry_root.insert(0, data["root"])
            if "exif" in data:
                self.entry_exif.delete(0, tk.END)
                self.entry_exif.insert(0, data["exif"])
            
            self.var_recursive.set(data.get("recursive", False))
            self.var_shift.set(data.get("shift", False))
            self.var_folder_priority.set(data.get("folder_priority", False))
            self.var_interactive.set(data.get("interactive", False))
            self.var_delete_empty.set(data.get("delete_empty", False))
            self.var_sanitize.set(data.get("sanitize", False))
            self.var_autoscroll.set(data.get("autoscroll", True))
            
            if "start_from" in data:
                self.entry_start_from.delete(0, tk.END)
                self.entry_start_from.insert(0, data["start_from"])
            
            # Geometry - Apply with slight delay
            def _restore_window():
                if "geometry" in data:
                    try:
                        self.geometry(data["geometry"])
                    except tk.TclError:
                        pass
                if data.get("maximized", False):
                    try:
                        self.state("zoomed")
                    except tk.TclError:
                        pass
            
            self.after(200, _restore_window)
            
        except (json.JSONDecodeError, OSError) as e:
            print(f"Config load error: {e}")
    
    def save_config(self) -> None:
        """Сохранить конфигурацию в JSON файл."""
        data = {
            "root": self.entry_root.get(),
            "exif": self.entry_exif.get(),
            "recursive": self.var_recursive.get(),
            "shift": self.var_shift.get(),
            "folder_priority": self.var_folder_priority.get(),
            "interactive": self.var_interactive.get(),
            "delete_empty": self.var_delete_empty.get(),
            "sanitize": self.var_sanitize.get(),
            "autoscroll": self.var_autoscroll.get(),
            "start_from": self.entry_start_from.get(),
            "geometry": self.geometry(),
            "maximized": (self.state() == "zoomed")
        }
        try:
            with open(self.config_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except OSError as e:
            print(f"Config save error: {e}")
    
    def on_closing(self) -> None:
        """Обработчик закрытия окна."""
        self.save_config()
        self.destroy()
    
    def _bind_global_shortcuts(self) -> None:
        """Привязать глобальные горячие клавиши."""
        self.bind_all("<Control-c>", self._on_copy)
        self.bind_all("<Control-v>", self._on_paste)
        self.bind_all("<Control-a>", self._on_select_all)
    
    def _on_copy(self, event) -> Optional[str]:
        """Обработчик Ctrl+C."""
        try:
            widget = self.focus_get()
            if isinstance(widget, (tk.Entry, tk.Text, scrolledtext.ScrolledText, ctk.CTkEntry)):
                text = ""
                if isinstance(widget, ctk.CTkEntry):
                    try:
                        text = widget._entry.selection_get()
                    except tk.TclError:
                        pass
                else:
                    try:
                        text = widget.selection_get()
                    except tk.TclError:
                        pass
                
                if text:
                    self.clipboard_clear()
                    self.clipboard_append(text)
        except tk.TclError:
            pass
        return None
    
    def _on_paste(self, event) -> Optional[str]:
        """Обработчик Ctrl+V."""
        try:
            widget = self.focus_get()
            if isinstance(widget, (tk.Text, scrolledtext.ScrolledText)):
                widget.insert("insert", self.clipboard_get())
                return "break"
            elif isinstance(widget, (tk.Entry, ctk.CTkEntry)):
                text = self.clipboard_get()
                if text:
                    widget.insert("insert", text)
                return "break"
        except tk.TclError:
            pass
        return None
    
    def _on_select_all(self, event) -> Optional[str]:
        """Обработчик Ctrl+A."""
        try:
            widget = self.focus_get()
            if isinstance(widget, (tk.Text, scrolledtext.ScrolledText)):
                widget.tag_add("sel", "1.0", "end")
                return "break"
            elif isinstance(widget, tk.Entry):
                widget.selection_range(0, tk.END)
                return "break"
        except tk.TclError:
            pass
        return None
    
    def log(self, msg: str, color: Optional[str] = None) -> None:
        """Добавить сообщение в лог."""
        self.logger.log(msg, color)
    
    # ---- Thread-safe dialogs ----
    def ask_yesno_threadsafe(self, title: str, text: str) -> bool:
        """Thread-safe диалог Yes/No."""
        if threading.current_thread() is threading.main_thread():
            return messagebox.askyesno(title, text)
        
        result = {"value": False}
        ev = threading.Event()
        
        def _do():
            try:
                result["value"] = messagebox.askyesno(title, text)
            finally:
                ev.set()
        
        self.after(0, _do)
        ev.wait()
        return result["value"]
    
    def ask_confirm_details_threadsafe(
        self, title: str, header: str, items: list
    ) -> bool:
        """Thread-safe диалог подтверждения со списком."""
        count = len(items)
        show_count = min(12, count)
        text = header + "\n\n"
        for i in range(show_count):
            text += f"{items[i]}\n"
        if count > show_count:
            text += f"... и еще {count - show_count} строк\n"
        text += "\nПродолжить?"
        return self.ask_yesno_threadsafe(title, text)
    
    def ask_string_threadsafe(
        self, title: str, prompt: str, initial_value: str
    ) -> Optional[str]:
        """Thread-safe диалог ввода строки."""
        if threading.current_thread() is threading.main_thread():
            return simpledialog.askstring(title, prompt, initialvalue=initial_value)
        
        result = {"value": None}
        ev = threading.Event()
        
        def _do():
            try:
                result["value"] = simpledialog.askstring(
                    title, prompt, initialvalue=initial_value
                )
            finally:
                ev.set()
        
        self.after(0, _do)
        ev.wait()
        return result["value"]
    
    # ---- UI ----
    def _init_ui(self) -> None:
        """Инициализировать UI компоненты."""
        # Configure Grid
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=0)
        self.grid_rowconfigure(0, weight=1)
        
        # --- LEFT COLUMN (Main) ---
        frame_left = ctk.CTkFrame(self, fg_color="transparent")
        frame_left.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        frame_left.grid_columnconfigure(0, weight=1)
        frame_left.grid_rowconfigure(5, weight=1)
        
        row = 0
        ctk.CTkLabel(frame_left, text="Корневая папка (Событие или Медиатека):").grid(
            row=row, column=0, sticky="w", pady=(0, 5)
        )
        row += 1
        
        frame_root = ctk.CTkFrame(frame_left)
        frame_root.grid(row=row, column=0, sticky="ew", pady=(0, 10))
        self.entry_root = ctk.CTkEntry(frame_root)
        self.entry_root.pack(side="left", fill="x", expand=True, padx=5, pady=5)
        self.entry_root.insert(0, str(self.script_dir))
        ctk.CTkButton(
            frame_root, text="...", width=40, command=self._browse_root
        ).pack(side="right", padx=5)
        
        # Recursive switch
        self.var_recursive = tk.BooleanVar(value=False)
        ctk.CTkSwitch(
            frame_left, text="Рекурсивно (все подпапки)", variable=self.var_recursive
        ).grid(row=row + 1, column=0, sticky="w", padx=5, pady=(0, 10))
        row += 2
        
        ctk.CTkLabel(frame_left, text="Путь к ExifTool:").grid(
            row=row, column=0, sticky="w", pady=(0, 5)
        )
        row += 1
        frame_exif = ctk.CTkFrame(frame_left)
        frame_exif.grid(row=row, column=0, sticky="ew", pady=(0, 10))
        self.entry_exif = ctk.CTkEntry(frame_exif)
        self.entry_exif.pack(side="left", fill="x", expand=True, padx=5, pady=5)
        self.entry_exif.insert(0, str(self.default_exif))
        ctk.CTkButton(
            frame_exif, text="...", width=40, command=self._browse_exif
        ).pack(side="right", padx=5)
        row += 1
        
        # Log Area
        self.log_text = scrolledtext.ScrolledText(
            frame_left, state="normal", font=("Consolas", 10)
        )
        self.log_text.grid(row=row, column=0, sticky="nsew", pady=10)
        self.log_text.config(
            bg="#1e1e1e", fg="#e0e0e0", insertbackground="white", relief="flat"
        )
        self.log_text.bind("<Key>", self._on_log_key)
        
        # Log Context Menu
        self.context_menu = tk.Menu(self, tearoff=0)
        self.context_menu.add_command(label="Копировать", command=self._copy_selection_context)
        self.log_text.bind("<Button-3>", self._show_context_menu)
        
        # Entry Context Menu
        self.context_menu_entry = tk.Menu(self, tearoff=0)
        self.context_menu_entry.add_command(
            label="Вырезать", command=lambda: self._entry_action("<<Cut>>")
        )
        self.context_menu_entry.add_command(
            label="Копировать", command=lambda: self._entry_action("<<Copy>>")
        )
        self.context_menu_entry.add_command(
            label="Вставить", command=lambda: self._entry_action("<<Paste>>")
        )
        self.context_menu_entry.add_separator()
        self.context_menu_entry.add_command(
            label="Выделить всё", command=lambda: self._entry_action("<<SelectAll>>")
        )
        
        row += 1
        
        # Search Bar
        frame_search = ctk.CTkFrame(frame_left)
        frame_search.grid(row=row, column=0, sticky="ew")
        ctk.CTkLabel(frame_search, text="Поиск:").pack(side="left", padx=10)
        self.entry_search = ctk.CTkEntry(frame_search)
        self.entry_search.pack(side="left", fill="x", expand=True, padx=5, pady=5)
        self.entry_search.bind("<Return>", lambda e: self._find_next())
        ctk.CTkButton(
            frame_search, text="Найти / Далее", width=100, command=self._find_next
        ).pack(side="right", padx=5)
        
        # --- RIGHT COLUMN (Controls & Stats) ---
        frame_right = ctk.CTkFrame(self, width=360, corner_radius=0)
        frame_right.grid(row=0, column=1, sticky="nsew", padx=(0, 0), pady=0)
        frame_right.pack_propagate(False)
        
        # 1. Controls Panel
        frame_ctrl = ctk.CTkFrame(frame_right)
        frame_ctrl.pack(fill="x", padx=10, pady=10)
        
        ctk.CTkLabel(
            frame_ctrl, text="Настройки и Запуск", 
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(pady=5)
        
        self.var_shift = tk.BooleanVar(value=False)
        self.var_folder_priority = tk.BooleanVar(value=False)
        self.var_interactive = tk.BooleanVar(value=False)
        self.var_delete_empty = tk.BooleanVar(value=False)
        self.var_autoscroll = tk.BooleanVar(value=True)
        
        ctk.CTkSwitch(
            frame_ctrl, text="Поиск сдвига дат", variable=self.var_shift
        ).pack(anchor="w", padx=10, pady=2)
        ctk.CTkSwitch(
            frame_ctrl, text="Приоритет дате папки", variable=self.var_folder_priority
        ).pack(anchor="w", padx=10, pady=2)
        ctk.CTkSwitch(
            frame_ctrl, text="Ручное подтверждение", variable=self.var_interactive
        ).pack(anchor="w", padx=10, pady=2)
        ctk.CTkSwitch(
            frame_ctrl, text="Удалять пустые", variable=self.var_delete_empty
        ).pack(anchor="w", padx=10, pady=2)
        
        # Sanitize option
        self.var_sanitize = tk.BooleanVar(value=False)
        ctk.CTkSwitch(
            frame_ctrl, text="Исправлять имена (emoji)", variable=self.var_sanitize
        ).pack(anchor="w", padx=10, pady=2)
        
        ctk.CTkSwitch(
            frame_ctrl, text="Авто-скролл", variable=self.var_autoscroll
        ).pack(anchor="w", padx=10, pady=2)
        
        # Start From
        f_start = ctk.CTkFrame(frame_ctrl, fg_color="transparent")
        f_start.pack(fill="x", padx=10, pady=10)
        ctk.CTkLabel(f_start, text="Начать с:").pack(side="left")
        self.entry_start_from = ctk.CTkEntry(f_start, width=100)
        self.entry_start_from.pack(side="right")
        
        # Buttons
        btn_font = ctk.CTkFont(size=14, weight="bold")
        
        self.btn_dryrun = ctk.CTkButton(
            frame_ctrl, text="ТЕСТ (Dry Run)", font=btn_font, command=self._start_dryrun
        )
        self.btn_dryrun.pack(fill="x", padx=10, pady=(5, 5))
        
        f_btns = ctk.CTkFrame(frame_ctrl, fg_color="transparent")
        f_btns.pack(fill="x", padx=10, pady=(0, 10))
        
        self.btn_apply = ctk.CTkButton(
            f_btns, text="ПУСК", font=btn_font, 
            fg_color="green", hover_color="darkgreen", command=self._start_apply
        )
        self.btn_apply.pack(side="left", fill="x", expand=True, padx=(0, 5))
        
        self.btn_stop = ctk.CTkButton(
            f_btns, text="СТОП", font=btn_font, 
            fg_color="gray", state="disabled", command=self._stop
        )
        self.btn_stop.pack(side="right", fill="x", expand=True)
        
        # 2. Status Bar
        self.lbl_status = ctk.CTkLabel(
            frame_right, text="Готов", height=30, 
            fg_color="#3a3a3a", corner_radius=6
        )
        self.lbl_status.pack(fill="x", padx=10, pady=5)
        
        # 3. Stats Panel
        frame_stats = ctk.CTkFrame(frame_right)
        frame_stats.pack(fill="both", expand=True, padx=10, pady=10)
        
        ctk.CTkLabel(
            frame_stats, text="Статистика (Live)", 
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(pady=5)
        
        self.lbl_stats = ctk.CTkLabel(
            frame_stats, text="Ожидание...", justify="left", anchor="nw",
            font=ctk.CTkFont(family="Consolas", size=12)
        )
        self.lbl_stats.pack(fill="both", expand=True, padx=5, pady=5)
        
        # Bind context menu to all entries
        for entry in (self.entry_root, self.entry_exif, self.entry_search, self.entry_start_from):
            self._bind_entry_right_click(entry)
    
    def _update_stats_panel(self) -> None:
        """Обновить панель статистики."""
        s = self.stats
        text = f"""
[ОБЩИЙ ПРОГРЕСС]
  Папок в обработке: ......... {s.folders_processed}
  Всего файлов найдено: ...... {s.files_total}

[КЛАССИФИКАЦИЯ ФАЙЛОВ]
  Имя файла содержит дату: ... {s.files_fixed_filename}
  Интерполяция (без даты): ... {s.files_fixed_interpolated}
  Сдвиг времени (Shift): ..... {s.files_fixed_shifted}
  Исправлено имен (Emoji): ... {s.files_sanitized}
  ВСЕГО ИЗМЕНЕНО ДАТ: ........ {s.files_fixed_date}
  
  Конвертация (DNG->JPG): .... {s.files_renamed_dng}
  Игнорировано (Ghost/Bad): .. {s.files_ghost_ignored}

[СТРУКТУРА ПАПОК]
  Папок переименовано: ....... {s.folders_renamed}
  Папок объединено (Merge): .. {s.folders_merged}
  Конфликтов слияния: ........ {s.merge_conflicts}

[ВЫБРОСЫ (Outliers)]
  Перемещено файлов: ......... {s.files_moved}

[СБОИ И ОШИБКИ]
  Критические ошибки: ........ {s.errors}
"""
        self.lbl_stats.configure(text=text)
    
    def _on_log_key(self, event) -> Optional[str]:
        """Ограничить редактирование лога."""
        if event.keysym in ("Up", "Down", "Left", "Right", "Home", "End", "Prior", "Next"):
            return None
        if (event.state & 4) and (event.keysym.lower() in ("c", "a")):
            return None
        return "break"
    
    def _show_context_menu(self, event) -> None:
        """Показать контекстное меню лога."""
        self.context_menu.post(event.x_root, event.y_root)
    
    def _bind_entry_right_click(self, widget) -> None:
        """Привязать правый клик к полю ввода."""
        try:
            widget._entry.bind("<Button-3>", self._show_entry_context_menu)
        except AttributeError:
            widget.bind("<Button-3>", self._show_entry_context_menu)
    
    def _show_entry_context_menu(self, event) -> None:
        """Показать контекстное меню поля ввода."""
        try:
            event.widget.focus_set()
            self.context_menu_target = event.widget
            self.context_menu_entry.post(event.x_root, event.y_root)
        except tk.TclError:
            pass
    
    def _entry_action(self, event_name: str) -> None:
        """Выполнить действие над полем ввода."""
        try:
            widget = self.focus_get()
            if widget:
                widget.event_generate(event_name)
        except tk.TclError:
            pass
    
    def _copy_selection_context(self) -> None:
        """Копировать выделенный текст из лога."""
        try:
            sel = self.log_text.get("sel.first", "sel.last")
            self.clipboard_clear()
            self.clipboard_append(sel)
        except tk.TclError:
            pass
    
    def _find_next(self) -> None:
        """Найти следующее вхождение текста в логе."""
        query = self.entry_search.get()
        if not query:
            return
        
        start = self.log_text.index("insert")
        pos = self.log_text.search(query, start, stopindex="end", nocase=True)
        if not pos:
            pos = self.log_text.search(query, "1.0", stopindex=start, nocase=True)
        
        if pos:
            end_pos = f"{pos}+{len(query)}c"
            self.log_text.tag_remove("search", "1.0", "end")
            self.log_text.tag_add("search", pos, end_pos)
            self.log_text.tag_config("search", background="yellow", foreground="black")
            self.log_text.see(pos)
            self.log_text.mark_set("insert", end_pos)
            self.entry_search.focus()
        else:
            messagebox.showinfo("Поиск", "Не найдено")
    
    def _browse_root(self) -> None:
        """Открыть диалог выбора папки."""
        p = filedialog.askdirectory()
        if p:
            p = os.path.normpath(p)
            self.entry_root.delete(0, tk.END)
            self.entry_root.insert(0, p)
    
    def _browse_exif(self) -> None:
        """Открыть диалог выбора ExifTool."""
        p = filedialog.askopenfilename(filetypes=[("ExifTool", "exiftool.exe")])
        if p:
            p = os.path.normpath(p)
            self.entry_exif.delete(0, tk.END)
            self.entry_exif.insert(0, p)
    
    def _check_queue(self) -> None:
        """Проверить очередь логов и обновить UI."""
        try:
            self._update_stats_panel()
        except Exception as e:
            if not self._stats_error_shown:
                self._stats_error_shown = True
                print(f"UI Stats Error: {e}", file=sys.stderr)
                try:
                    self.log_queue.put((f"UI Stats Error: {e}", "red"))
                except Exception:
                    pass
        
        # Color mapping for dark theme
        color_map = {
            "blue": "#64b5f6",
            "red": "#e57373",
            "green": "#81c784",
            "orange": "#ffb74d",
            "magenta": "#ba68c8",
            "gray": "#90a4ae"
        }
        
        while not self.log_queue.empty():
            msg, color = self.log_queue.get()
            tag = None
            if color:
                display_color = color_map.get(color, color)
                tag = color
                self.log_text.tag_config(tag, foreground=display_color)
            
            if msg.endswith("\n"):
                self.log_text.insert(tk.END, msg, tag)
            else:
                self.log_text.insert(tk.END, msg + "\n", tag)
            
            if self.var_autoscroll.get():
                self.log_text.see(tk.END)
        
        self.after(100, self._check_queue)
    
    def set_status(self, msg: str) -> None:
        """Установить текст статуса."""
        self.after(0, lambda: self.lbl_status.configure(text=msg))
    
    def _toggle_buttons(self, running: bool) -> None:
        """Переключить состояние кнопок."""
        if running:
            self.btn_dryrun.configure(state="disabled", fg_color="gray")
            self.btn_apply.configure(state="disabled", fg_color="gray")
            self.btn_stop.configure(state="normal", fg_color="red", hover_color="darkred")
            self.entry_root.configure(state="disabled")
            self.entry_exif.configure(state="disabled")
        else:
            self.btn_dryrun.configure(state="normal", fg_color=["#3a7ebf", "#1f538d"])
            self.btn_apply.configure(state="normal", fg_color="green", hover_color="darkgreen")
            self.btn_stop.configure(state="disabled", fg_color="gray")
            self.entry_root.configure(state="normal")
            self.entry_exif.configure(state="normal")
    
    def _start_dryrun(self) -> None:
        """Запустить тестовый прогон."""
        self._start_engine(apply=False)
    
    def _start_apply(self) -> None:
        """Запустить применение изменений."""
        if messagebox.askyesno("Подтверждение", "Применить изменения?"):
            self._start_engine(apply=True)
    
    def _stop(self) -> None:
        """Остановить обработку."""
        self.stop_requested = True
        self.log("\n[ЗАПРОС ОСТАНОВКИ]...", "red")
    
    def _check_stop(self) -> None:
        """Проверить запрос остановки (для передачи в processor)."""
        if self.stop_requested:
            raise StopRequested()
    
    def _start_engine(self, apply: bool) -> None:
        """Запустить движок обработки."""
        root = self.entry_root.get().strip()
        exif = self.entry_exif.get().strip()
        
        if not os.path.isdir(root):
            messagebox.showerror("Ошибка", "Папка не найдена")
            self.stop_requested = False
            return
        
        if not os.path.isfile(exif):
            messagebox.showerror("Ошибка", "ExifTool не найден по указанному пути")
            self.stop_requested = False
            return
        
        self.stop_requested = False
        self._toggle_buttons(True)
        self.log_text.delete(1.0, tk.END)
        
        self.log(f"\n{'=' * 40}\nЗАПУСК НОВОЙ СЕССИИ\n{'=' * 40}", "blue")
        
        def run_logic():
            try:
                processor = MediaProcessor(
                    root=Path(root),
                    exif_path=exif,
                    script_dir=self.script_dir,
                    logger=self.logger,
                    stats=self.stats,
                    check_stop=self._check_stop,
                    set_status=self.set_status,
                    ask_yesno=self.ask_yesno_threadsafe,
                    ask_string=self.ask_string_threadsafe,
                    apply=apply,
                    recursive=self.var_recursive.get(),
                    shift=self.var_shift.get(),
                    delete_empty=self.var_delete_empty.get(),
                    start_from=self.entry_start_from.get().strip(),
                    folder_priority=self.var_folder_priority.get(),
                    sanitize=self.var_sanitize.get(),
                    interactive=self.var_interactive.get(),
                )
                processor.run()
                
                s = self.stats
                summary = f"""
========================================
ИТОГОВАЯ СТАТИСТИКА:
----------------------------------------
Обработано папок:      {s.folders_processed}
Всего файлов:          {s.files_total}
Исправлено имен:       {s.files_sanitized}
Исправлено дат файлов: {s.files_fixed_date}
Исправлено расширений: {s.files_renamed_dng}
Игнорировано (Ghost):  {s.files_ghost_ignored}
----------------------------------------
Переименовано папок:   {s.folders_renamed}
Слито папок (Merge):   {s.folders_merged}
Перемещено выбросов:   {s.files_moved}
Конфликтов слияния:    {s.merge_conflicts}
ОШИБОК:                {s.errors}
========================================
"""
                self.log(summary, "blue")
                
            except StopRequested:
                self.log("Остановлено пользователем.", "orange")
            except Exception as e:
                self.log(f"КРИТИЧЕСКАЯ ОШИБКА: {e}", "red")
                self.stats.add_error()
            finally:
                self.set_status("Готово")
                self.after(0, lambda: self._toggle_buttons(False))
        
        self.worker_thread = threading.Thread(target=run_logic, daemon=True)
        self.worker_thread.start()
    
    def _show_welcome_message(self) -> None:
        """Показать приветственное сообщение."""
        msg = """
================================================================================
                           MEDIA LIBRARY TOOL v2.4
================================================================================

РУКОВОДСТВО ПОЛЬЗОВАТЕЛЯ

1. ЭЛЕМЕНТЫ ИНТЕРФЕЙСА
----------------------
[Корневая папка]    
  Папка, где лежат ваши события (напр. "E:\\Photos"). 
  Скрипт ищет папки формата "YYYY-MM Событие".

[Путь к ExifTool]
  Для работы с метаданными нужен exiftool.exe. Укажите путь к нему.

[Рекурсивно]
  Если включено, скрипт будет заходить во все вложенные папки.
  Если выключено — обработает только ту папку, что выбрана.

[Поиск сдвига дат]
  Полезно, если на камере была сбита дата (напр. стоит 2008 год вместо 2024).

[Приоритет дате папки]
  Включает СТРОГИЙ фильтр по дате папки.

[Ручное подтверждение]
  В режиме "ПУСК" скрипт будет спрашивать подтверждение.

[Удалять пустые папки и файлы]
  Очистка мусора.

[Начать с...]
  Позволяет пропустить старые папки.

[Кнопки]
  ТЕСТ      -> "Сухой прогон". Показывает, что БЫЛО БЫ сделано.
  ПУСК      -> Реальное переименование и запись метаданных.

================================================================================
"""
        self.log(msg)

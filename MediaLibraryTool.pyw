import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, simpledialog
import customtkinter as ctk  # NEW
import subprocess
import threading
import queue
import os
import re
import sys
import datetime
import shutil
import json
import time
import tempfile
import csv
from pathlib import Path

# ---------------- Configuration ----------------
MIN_VALID_YEAR = 1900
SHORT_EVENT_DAYS = 3

# Extensions that are not media and should be ignored early (manual filter)
IGNORE_EXTENSIONS = {'.ini', '.db', '.tmp'}

# Files where ExifTool typically doesn't write EXIF; we will set filesystem times via PowerShell
FS_ONLY_EXTENSIONS = {'.avi', '.bmp', '.crq', '.thm', '.wav'}

# Assigning timestamps to missing-date files
DEFAULT_ASSIGN_STEP_SECONDS = 2
DEFAULT_ASSIGN_WINDOW_HOURS = 6

# ---------------- Exceptions ----------------
class StopRequested(Exception):
    pass


# ---------------- Stats ----------------
class Stats:
    def __init__(self):
        self.reset()

    def reset(self):
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
        
        # Breakdown of date fixes
        self.files_fixed_filename = 0
        self.files_fixed_interpolated = 0
        self.files_fixed_shifted = 0
        self.files_sanitized = 0


# ---------------- Logger ----------------
class Logger:
    def __init__(self, gui):
        self.gui = gui
        self.log_file = gui.script_dir / "MediaLibraryTool.log"
        self.file_error_shown = False

    def log(self, msg, color=None):
        # 1) GUI
        self.gui.log_queue.put((msg, color))

        # 2) File
        try:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(f"[{timestamp}] {msg}\n")
        except Exception:
            if not self.file_error_shown:
                self.file_error_shown = True
                try:
                    self.gui.log_queue.put(("WARNING: Cannot write to log file!", "red"))
                except:
                    pass


# ---------------- GUI ----------------
class MediaLibraryTool(ctk.CTk):
    def __init__(self):
        super().__init__()
        
        # CTK Configuration
        ctk.set_appearance_mode("Dark")
        ctk.set_default_color_theme("blue")
        
        self.title("MediaLibraryTool (Modern)")
        self.geometry("1100x800")

        self.script_dir = Path(__file__).parent.absolute()
        self.default_exif = self.script_dir / "exiftool" / "exiftool.exe"

        self.log_queue = queue.Queue()
        self.stop_requested = False
        self.worker_thread = None

        self.stats = Stats()
        self.logger = Logger(self)

        self._init_ui()
        self._stats_error_shown = False
        # self._apply_dark_theme() # Removed, using CTK native theme
        self._show_welcome_message()
        self._check_queue()

    def log(self, msg, color=None):
        self.logger.log(msg, color)

    # ---- Thread-safe dialogs ----
    def ask_yesno_threadsafe(self, title, text):
        """
        Safe to call from worker thread: shows messagebox in Tk main thread and waits for result.
        """
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

    def ask_confirm_details_threadsafe(self, title, header, items):
        count = len(items)
        show_count = min(12, count)
        text = header + "\n\n"
        for i in range(show_count):
            text += f"{items[i]}\n"
        if count > show_count:
            text += f"... и еще {count - show_count} строк\n"
        text += "\nПродолжить?"
        return self.ask_yesno_threadsafe(title, text)

    def ask_string_threadsafe(self, title, prompt, initial_value):
        if threading.current_thread() is threading.main_thread():
            return simpledialog.askstring(title, prompt, initialvalue=initial_value)

        result = {"value": None}
        ev = threading.Event()

        def _do():
            try:
                result["value"] = simpledialog.askstring(title, prompt, initialvalue=initial_value)
            finally:
                ev.set()

        self.after(0, _do)
        ev.wait()
        return result["value"]

    # ---- UI ----
    def _init_ui(self):
        # Configure Grid
        self.grid_columnconfigure(0, weight=1) # Main content
        self.grid_columnconfigure(1, weight=0) # Right panel (fixed width approx)
        self.grid_rowconfigure(0, weight=1)    # Content expands

        # --- LEFT COLUMN (Main) ---
        frame_left = ctk.CTkFrame(self, fg_color="transparent")
        frame_left.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        frame_left.grid_columnconfigure(0, weight=1)
        frame_left.grid_rowconfigure(5, weight=1) # Log expands (row 5)

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
        ctk.CTkButton(frame_root, text="...", width=40, command=self._browse_root).pack(side="right", padx=5)
        
        # Recursive switch
        self.var_recursive = tk.BooleanVar(value=False)
        ctk.CTkSwitch(frame_left, text="Рекурсивно (все подпапки)", variable=self.var_recursive).grid(row=row+1, column=0, sticky="w", padx=5, pady=(0,10))
        row += 2

        ctk.CTkLabel(frame_left, text="Путь к ExifTool:").grid(row=row, column=0, sticky="w", pady=(0, 5))
        row += 1
        frame_exif = ctk.CTkFrame(frame_left)
        frame_exif.grid(row=row, column=0, sticky="ew", pady=(0, 10))
        self.entry_exif = ctk.CTkEntry(frame_exif)
        self.entry_exif.pack(side="left", fill="x", expand=True, padx=5, pady=5)
        self.entry_exif.insert(0, str(self.default_exif))
        ctk.CTkButton(frame_exif, text="...", width=40, command=self._browse_exif).pack(side="right", padx=5)
        row += 1

        # Log Area
        self.log_text = scrolledtext.ScrolledText(frame_left, state="normal", font=("Consolas", 10))
        self.log_text.grid(row=row, column=0, sticky="nsew", pady=10)
        # Style the standard Tk widget to match Dark Theme
        self.log_text.config(bg="#1e1e1e", fg="#e0e0e0", insertbackground="white", relief="flat")
        self.log_text.bind("<Key>", self._on_log_key)
        self.context_menu = tk.Menu(self, tearoff=0)
        self.context_menu.add_command(label="Копировать", command=self._copy_selection_context)
        self.log_text.bind("<Button-3>", self._show_context_menu)
        row += 1

        # Search Bar
        frame_search = ctk.CTkFrame(frame_left)
        frame_search.grid(row=row, column=0, sticky="ew")
        ctk.CTkLabel(frame_search, text="Поиск:").pack(side="left", padx=10)
        self.entry_search = ctk.CTkEntry(frame_search)
        self.entry_search.pack(side="left", fill="x", expand=True, padx=5, pady=5)
        self.entry_search.bind("<Return>", lambda e: self._find_next())
        ctk.CTkButton(frame_search, text="Найти / Далее", width=100, command=self._find_next).pack(side="right", padx=5)


        # --- RIGHT COLUMN (Controls & Stats) ---
        frame_right = ctk.CTkFrame(self, width=360, corner_radius=0)
        frame_right.grid(row=0, column=1, sticky="nsew", padx=(0,0), pady=0)
        frame_right.pack_propagate(False)
        
        # 1. Controls Panel
        frame_ctrl = ctk.CTkFrame(frame_right)
        frame_ctrl.pack(fill="x", padx=10, pady=10)
        
        ctk.CTkLabel(frame_ctrl, text="Настройки и Запуск", font=ctk.CTkFont(size=14, weight="bold")).pack(pady=5)

        self.var_shift = tk.BooleanVar(value=False)
        self.var_folder_priority = tk.BooleanVar(value=False)
        self.var_interactive = tk.BooleanVar(value=False)
        self.var_delete_empty = tk.BooleanVar(value=False)
        self.var_autoscroll = tk.BooleanVar(value=True)

        ctk.CTkSwitch(frame_ctrl, text="Поиск сдвига дат", variable=self.var_shift).pack(anchor="w", padx=10, pady=2)
        ctk.CTkSwitch(frame_ctrl, text="Приоритет дате папки", variable=self.var_folder_priority).pack(anchor="w", padx=10, pady=2)
        ctk.CTkSwitch(frame_ctrl, text="Ручное подтверждение", variable=self.var_interactive).pack(anchor="w", padx=10, pady=2)
        ctk.CTkSwitch(frame_ctrl, text="Удалять пустые", variable=self.var_delete_empty).pack(anchor="w", padx=10, pady=2)
        
        # New options
        self.var_sanitize = tk.BooleanVar(value=False)
        ctk.CTkSwitch(frame_ctrl, text="Исправлять имена (emoji)", variable=self.var_sanitize).pack(anchor="w", padx=10, pady=2)

        ctk.CTkSwitch(frame_ctrl, text="Авто-скролл", variable=self.var_autoscroll).pack(anchor="w", padx=10, pady=2)

        # Start From
        f_start = ctk.CTkFrame(frame_ctrl, fg_color="transparent")
        f_start.pack(fill="x", padx=10, pady=10)
        ctk.CTkLabel(f_start, text="Начать с:").pack(side="left")
        self.entry_start_from = ctk.CTkEntry(f_start, width=100)
        self.entry_start_from.pack(side="right")

        # Buttons
        self.btn_dryrun = ctk.CTkButton(frame_ctrl, text="ТЕСТ (Dry Run)", command=self._start_dryrun)
        self.btn_dryrun.pack(fill="x", padx=10, pady=(5,5))
        
        f_btns = ctk.CTkFrame(frame_ctrl, fg_color="transparent")
        f_btns.pack(fill="x", padx=10, pady=(0,10))
        
        self.btn_apply = ctk.CTkButton(f_btns, text="ПРИМЕНИТЬ", fg_color="green", hover_color="darkgreen", command=self._start_apply)
        self.btn_apply.pack(side="left", fill="x", expand=True, padx=(0, 5))
        
        self.btn_stop = ctk.CTkButton(f_btns, text="СТОП", fg_color="red", hover_color="darkred", state="disabled", command=self._stop)
        self.btn_stop.pack(side="right", fill="x", expand=True)

        # 2. Status Bar (Middle)
        self.lbl_status = ctk.CTkLabel(frame_right, text="Готов", height=30, fg_color="#3a3a3a", corner_radius=6)
        self.lbl_status.pack(fill="x", padx=10, pady=5)

        # 3. Stats Panel
        frame_stats = ctk.CTkFrame(frame_right)
        frame_stats.pack(fill="both", expand=True, padx=10, pady=10)
        
        ctk.CTkLabel(frame_stats, text="Статистика (Live)", font=ctk.CTkFont(size=14, weight="bold")).pack(pady=5)
        
        self.lbl_stats = ctk.CTkLabel(frame_stats, text="Ожидание...", justify="left", anchor="nw", 
                                      font=ctk.CTkFont(family="Consolas", size=12))
        self.lbl_stats.pack(fill="both", expand=True, padx=5, pady=5)


    def _update_stats_panel(self):
        s = self.stats
        
        # Detailed statistics description
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
  (Файлы, даты которых не подходят к папке)

[СБОИ И ОШИБКИ]
  Критические ошибки: ........ {s.errors}
"""
        self.lbl_stats.configure(text=text)

    def _on_log_key(self, event):
        if event.keysym in ("Up", "Down", "Left", "Right", "Home", "End", "Prior", "Next"):
            return None
        if (event.state & 4) and (event.keysym.lower() in ("c", "a")):
            return None
        return "break"

    def _show_context_menu(self, event):
        self.context_menu.post(event.x_root, event.y_root)

    def _copy_selection_context(self):
        try:
            sel = self.log_text.get("sel.first", "sel.last")
            self.clipboard_clear()
            self.clipboard_append(sel)
        except tk.TclError:
            pass

    def _find_next(self):
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

    def _browse_root(self):
        p = filedialog.askdirectory()
        if p:
            p = os.path.normpath(p)
            self.entry_root.delete(0, tk.END)
            self.entry_root.insert(0, p)

    def _browse_exif(self):
        p = filedialog.askopenfilename(filetypes=[("ExifTool", "exiftool.exe")])
        if p:
            p = os.path.normpath(p)
            self.entry_exif.delete(0, tk.END)
            self.entry_exif.insert(0, p)

    def _check_queue(self):
        try:
            self._update_stats_panel()  # Refresh stats UI
        except Exception as e:
            if not self._stats_error_shown:
                self._stats_error_shown = True
                print(f"UI Stats Error: {e}", file=sys.stderr)
                # Try to log to text if possible, but risky if that's broken
                try:
                    self.log_queue.put((f"UI Stats Error: {e}", "red"))
                except:
                    pass
        
        while not self.log_queue.empty():
            msg, color = self.log_queue.get()
            tag = None
            if color:
                # Dark Theme Colors Map
                color_map = {
                     "blue": "#64b5f6",   # Light Blue
                     "red": "#e57373",    # Light Red
                     "green": "#81c784",  # Light Green
                     "orange": "#ffb74d", # Orange
                     "magenta": "#ba68c8",# Purple
                     "gray": "#90a4ae"    # Blue Gray
                }
                display_color = color_map.get(color, color)
                tag = color # tag name uses original 'blue' string
                self.log_text.tag_config(tag, foreground=display_color)

            if msg.endswith("\n"):
                self.log_text.insert(tk.END, msg, tag)
            else:
                self.log_text.insert(tk.END, msg + "\n", tag)

            if self.var_autoscroll.get():
                self.log_text.see(tk.END)

        self.after(100, self._check_queue)

    def set_status(self, msg):
        self.after(0, lambda: self.lbl_status.configure(text=msg))

    def _toggle_buttons(self, running):
        state = "disabled" if running else "normal"
        stop_state = "normal" if running else "disabled"
        self.btn_dryrun.configure(state=state)
        self.btn_apply.configure(state=state)
        self.entry_root.configure(state=state)
        self.entry_exif.configure(state=state)
        self.btn_stop.configure(state=stop_state)

    def _start_dryrun(self):
        self._start_engine(apply=False)

    def _start_apply(self):
        if messagebox.askyesno("Подтверждение", "Применить изменения?"):
            self._start_engine(apply=True)

    def _stop(self):
        self.stop_requested = True
        self.log("\n[ЗАПРОС ОСТАНОВКИ]...", "red")

    def _start_engine(self, apply):
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

        args = {
            "root": root,
            "exif": exif,
            "apply": apply,
            "recursive": self.var_recursive.get(),
            "shift": self.var_shift.get(),
            "delete_empty": self.var_delete_empty.get(),
            "start_from": self.entry_start_from.get().strip(),
            "folder_priority": self.var_folder_priority.get(),
            "sanitize": self.var_sanitize.get(),
        }

        self.log(f"\n{'='*40}\nЗАПУСК НОВОЙ СЕССИИ\n{'='*40}", "blue")

        self.worker_thread = threading.Thread(target=self._run_logic, args=(args,), daemon=True)
        self.worker_thread.start()

    def _run_logic(self, args):
        try:
            processor = MediaProcessor(self, args)
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
            try:
                self.stats.errors += 1
            except Exception:
                pass
        finally:
            self.set_status("Готово")
            self.after(0, lambda: self._toggle_buttons(False))

    def _show_welcome_message(self):
        msg = """
================================================================================
                           MEDIA LIBRARY TOOL v2.3
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
  Если выключено — обработает только ту папку, что выбрана (или все папки 1-го уровня).

[Поиск сдвига дат]
  Полезно, если на камере была сбита дата (напр. стоит 2008 год вместо 2024).
  Скрипт найдет файлы, которые "похожи" на текущую папку, но отличаются на ровное число лет/часов.

[Приоритет дате папки (Игнор EXIF)]
  Включает СТРОГИЙ фильтр по дате папки.
  - Если в файле есть дата, и она совпадает с датой папки (Год/Месяц) -> Всё ок.
  - Если дата файла выходит за пределы папки (напр. 2006-12 в папке 2007-01) -> Дата считается ОШИБОЧНОЙ.
    Она будет пересчитана с нуля (интерполяцией) от начала папки.
  Используйте это, чтобы исправить файлы с неверным годом/месяцем, сохранив при этом верные файлы.

[Ручное подтверждение]
  В режиме "ПРИМЕНИТЬ" скрипт будет останавливаться перед каждым изменением 
  и спрашивать "Вы уверены?". Для массовой обработки лучше отключить.

[Удалять пустые папки и файлы]
  Очистка мусора. Удалит папки без файлов и файлы размером 0 байт.

[Начать с...]
  Позволяет пропустить старые папки. Введите "2020", чтобы начать обработку с 2020 года.

[Кнопки]
  ТЕСТ      -> "Сухой прогон". Показывает в логе, что БЫЛО БЫ сделано. Файлы не меняются.
  ПРИМЕНИТЬ -> Реальное переименование и запись метаданных. Будьте осторожны.

------------------------------------------------================================

2. АЛГОРИТМ РАБОТЫ И ПРИОРИТЕТЫ
-------------------------------
Главная задача: присвоить каждому файлу правильную дату съемки (DateTimeOriginal).

ЭТАП 1: Поиск "Истинной даты" для файла
Скрипт проверяет источники в строгом порядке (кто первый дал валидную дату, тот и главнее):

  1. ИМЯ ФАЙЛА (Highest Priority)
     Если имя содержит дату, мы верим ему безоговорочно.
     Поддерживаемые форматы (по убыванию приоритета):
     - DDMMYYYY-HHmm   (напр. 21072022-1937 -> 21 июля 2022, 19:37) *Новинка*
     - YYYYMMDD-HHmmSS (напр. 20220721-143000)
     - YYYYMMDD_HHmmSS (Android стандарт)
     - WP_YYYYMMDD...  (Windows Phone)
     - YYYYMMDD        (только дата, время будет 12:00)
     - Unix Timestamp  (13 цифр, начинающиеся на 15/16/17... - миллисекунды)

  2. EXIF МЕТАДАННЫЕ
     Если в имени даты нет (напр. "DSC_1234.jpg"), читаем теги внутри файла:
     - DateTimeOriginal
     - CreateDate
     - MediaCreateDate (для видео)

  3. ФАЙЛОВАЯ СИСТЕМА (Fallback)
     Если файл пустой на данные ("Noname.jpg" без EXIF), берется дата создания файла Windows.

ЭТАП 2: Устранение "Выбросов" (Outliers)
  Если файл датирован 2015 годом, а лежит в папке "2007-01 Эльбрус", он считается "чужим".
  Скрипт создаст папку "!Outliers/2015-..." и перенесет его туда.

ЭТАП 3: Интерполяция (Заполнение дыр)
  Если есть файлы без даты вообще, они получают дату "между" соседями.
  Пример:
    Файл_1 (10:00)
    Файл_2 (???)   -> получит 10:00 + шаг
    Файл_3 (???)   -> получит 10:00 + 2 шага
    Файл_4 (11:00) 
  Это сохраняет порядок сортировки.

ЭТАП 4: Запись (Только по кнопке ПРИМЕНИТЬ)
  - Если дата взята из Имени -> Она пишется в EXIF.
  - Если дата взята из EXIF -> Она пишется в имя (если включено переименование) и FS.
------------------------------------------------================================
"""
        self.log(msg)


# ---------------- Processor ----------------
class MediaProcessor:
    def __init__(self, gui: MediaLibraryTool, args: dict):
        self.gui = gui
        self.args = args # Store entire args dict
        self.root = Path(args["root"])
        self.exif_path = args["exif"]
        self.apply = args["apply"]
        self.recursive = args["recursive"]
        self.shift = args["shift"]
        self.delete_empty = args["delete_empty"]
        self.start_from = args.get("start_from", "").strip()
        self.folder_priority = args.get("folder_priority", False)

        self.stats = gui.stats
        self.stats.reset()

    @property
    def interactive(self):
        return self.gui.var_interactive.get()

    def log(self, msg, color=None):
        self.gui.log(msg, color)

    def check_stop(self):
        if self.gui.stop_requested:
            raise StopRequested()

    def _run_process_interruptible(self, cmd, **kwargs):
        """
        Runs subprocess with interrupt capability.
        Returns (stdout_bytes, stderr_bytes, returncode).
        Windows-only tweaks are OK (tool is Windows-only per user request).
        """
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        kwargs["startupinfo"] = si

        with tempfile.TemporaryFile() as out_tmp, tempfile.TemporaryFile() as err_tmp:
            kwargs["stdout"] = out_tmp
            kwargs["stderr"] = err_tmp
            p = subprocess.Popen(cmd, **kwargs)

            while p.poll() is None:
                if self.gui.stop_requested:
                    p.kill()
                    raise StopRequested()
                time.sleep(0.1)

            out_tmp.seek(0)
            err_tmp.seek(0)
            return out_tmp.read(), err_tmp.read(), p.returncode

    # ---- Date helpers ----
    def _get_max_year(self):
        return datetime.datetime.now().year + 1

    def get_folder_date(self, folder_name):
        # 2024-06-15
        m = re.match(r'^(\d{4})[-._](\d{2})[-._](\d{2})', folder_name)
        if m:
            y, mo, d = map(int, m.groups())
            if y < MIN_VALID_YEAR:
                return None, None
            return datetime.datetime(y, mo, d, 12, 0, 0), "Day"

        # 2024-06
        m = re.match(r'^(\d{4})[-._](\d{2})', folder_name)
        if m:
            y, mo = map(int, m.groups())
            if y < MIN_VALID_YEAR:
                return None, None
            return datetime.datetime(y, mo, 1, 12, 0, 0), "Month"

        # 2024
        m = re.match(r'^(\d{4})', folder_name)
        if m:
            y = int(m.group(1))
            if y < MIN_VALID_YEAR:
                return None, None
            return datetime.datetime(y, 1, 1, 12, 0, 0), "Year"

        return None, None

    def _valid_date(self, y, mo, d, h, mi, s):
        try:
            if y < MIN_VALID_YEAR:
                return None
            if y > self._get_max_year():
                return None
            return datetime.datetime(y, mo, d, h, mi, s)
        except Exception:
            return None

    def get_date_from_filename(self, path: Path):
        name = path.stem

        # 0. YMD HMS ms (Continuous 17+ digits): 201612151436015000
        # (\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d+)
        m = re.search(r'(?<!\d)(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d+)(?!\d)', name)
        if m:
            d = self._valid_date(int(m.group(1)), int(m.group(2)), int(m.group(3)), 
                                 int(m.group(4)), int(m.group(5)), int(m.group(6)))
            if d: return d

        # [NEW] DDMMYYYY-HHmm (e.g. 21072022-1937) - Common in WhatsApp/Socials dump
        # Priority over generic 14-digit or 8-digit to avoid partial matches later
        m = re.search(r'(?<!\d)(\d{2})(\d{2})(\d{4})[-_](\d{2})(\d{2})(?!\d)', name)
        if m:
            # d, m, y, H, M
            d = self._valid_date(int(m.group(3)), int(m.group(2)), int(m.group(1)),
                                 int(m.group(4)), int(m.group(5)), 0)
            if d:
                return d

        # [NEW 2] YYYYMMDD_HHmmSSx (e.g. 20150701_1651432)
        m = re.search(r'(?<!\d)(\d{4})(\d{2})(\d{2})[-_](\d{2})(\d{2})(\d{2})(\d+)?(?!\d)', name)
        if m:
            d = self._valid_date(int(m.group(1)), int(m.group(2)), int(m.group(3)),
                                 int(m.group(4)), int(m.group(5)), int(m.group(6)))
            if d:
                return d

        # [NEW 3] WhatsApp (IMG-YYYYMMDD-WA...) or similar "Date-Only" strong pattern
        # Matches: IMG-20190101-WA0001, VID-20190101-WA002
        m = re.search(r'(?:IMG|VID)[-_](\d{8})[-_]WA', name, re.IGNORECASE)
        if m:
            s_val = m.group(1)
            y, mo, day = int(s_val[0:4]), int(s_val[4:6]), int(s_val[6:8])
            d = self._valid_date(y, mo, day, 12, 0, 0)
            if d:
                return d

        # [NEW 4] Hyphenated full with continuous time (Signal style): YYYY-MM-DD-HHmmSS
        # 2023-01-01-140000
        m = re.search(r'(\d{4})[-_](\d{2})[-_](\d{2})[-_](\d{2})(\d{2})(\d{2})', name)
        if m:
             d = self._valid_date(*map(int, m.groups()))
             if d:
                 return d

        # [NEW 5] Fully hyphenated: YYYY-MM-DD-HH-mm-SS
        # 2023-01-01-14-00-00
        m = re.search(r'(\d{4})[-_](\d{2})[-_](\d{2})[-_](\d{2})[-_](\d{2})[-_](\d{2})', name)
        if m:
             d = self._valid_date(*map(int, m.groups()))
             if d:
                 return d

        # 1) YMD HMS with separators
        m = re.search(r'(\d{4})[-_.](\d{2})[-_.](\d{2})[-_.\s]+(\d{2})[-_.]?(\d{2})[-_.]?(\d{2})', name)
        if m:
            d = self._valid_date(*map(int, m.groups()))
            if d:
                return d

        # 2) Continuous 14 digits: 20180407160644 (bounds avoid hash matching)
        m = re.search(r'(?<!\d)(\d{4})(\d{2})(\d{2})[-_]?(\d{2})(\d{2})(\d{2})(?!\d)', name)
        if m:
            d = self._valid_date(*map(int, m.groups()))
            if d:
                return d

        # 3) YMD noon
        m = re.search(r'(?<!\d)(\d{4})[-_.](\d{2})[-_.](\d{2})(?!\d)', name)
        if m:
            d = self._valid_date(int(m.group(1)), int(m.group(2)), int(m.group(3)), 12, 0, 0)
            if d:
                return d

        # 4) YMD continuous 8 digits OR DDMMYYYY continuous 8 digits
        m = re.search(r'(?<!\d)(\d{8})(?!\d)', name)
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

        # 5) DMY with separators
        m = re.search(r'(?<!\d)(\d{2})[-_.](\d{2})[-_.](\d{4})(?!\d)', name)
        if m:
            d = self._valid_date(int(m.group(3)), int(m.group(2)), int(m.group(1)), 12, 0, 0)
            if d:
                return d

        # 6. Timestamp ms (13 digits, starting with 15/16/17)
        # MOVED TO END and ADDED BOUNDARIES (?<!\d) ... (?!\d)
        m_ts = re.search(r'(?<!\d)(1[5-7]\d{11})(?!\d)', name)
        if m_ts:
            try:
                ms = int(m_ts.group(1))
                ts = datetime.datetime.fromtimestamp(ms / 1000.0).replace(microsecond=0)
                if MIN_VALID_YEAR <= ts.year <= self._get_max_year():
                    return ts
            except Exception:
                pass

        return None

    def _parse_exif_date(self, raw_str):
        if not raw_str:
            return None
        # exiftool dates often look like "2024:12:15 12:00:00" or include timezone suffix
        s = str(raw_str)
        s = s[:19]
        for fmt in ("%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.datetime.strptime(s, fmt)
            except Exception:
                continue
        return None

    def _format_exif_datetime(self, dt: datetime.datetime):
        # ExifTool standard
        return dt.strftime("%Y:%m:%d %H:%M:%S")

    # ---- Exif read ----
    def get_exif_json(self, folder: Path):
        # Create argfile for robust path handling
        arg_file_path = None
        try:
            # Use 'utf-8' and tell ExifTool to expect it
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False) as f:
                f.write(str(folder))
                arg_file_path = f.name
        except Exception as e:
            self.log(f"Temp file error: {e}", "red")
            return []

        cmd = [
            str(self.exif_path),
            "-charset", "filename=utf8",
            "-charset", "utf8",
            "-m",
            "-r",
            "-fast2",
            # "-n", # REMOVED: Returns raw numbers for dates in video files (e.g. seconds since 1904)
            "-json",
            "-q", "-q",
            "-DateTimeOriginal",
            "-CreateDate",
            "-MediaCreateDate",
            "-FileCreateDate",
            "-FileModifyDate",
            "-SourceFile",
            "-@", arg_file_path
        ]

        try:
            self.check_stop()
            out_b, err_b, rc = self._run_process_interruptible(cmd)
            stdout = out_b.decode("utf-8", errors="replace") if out_b else ""
            stderr = err_b.decode("utf-8", errors="replace") if err_b else ""

            if stderr.strip():
                self.log(f"Предупреждение ExifTool: {stderr.strip()}", "orange")

            if not stdout.strip():
                return []

            data = json.loads(stdout)
            # Local filter for junk extensions
            filtered = []
            for item in data:
                try:
                    p = Path(item.get("SourceFile", ""))
                    if p.suffix.lower() in IGNORE_EXTENSIONS:
                        continue
                    filtered.append(item)
                except Exception:
                    filtered.append(item)
            return filtered

        except StopRequested:
            raise
        except Exception as e:
            self.log(f"Ошибка выполнения ExifTool: {e}", "red")
            return []
        finally:
            if arg_file_path and os.path.exists(arg_file_path):
                try:
                    os.unlink(arg_file_path)
                except:
                    pass

    # ---- Ordering key ----
    def _file_order_key(self, p: Path):
        try:
            st = p.stat()
            # Prefer Modification Time (more likely to be original) over Creation Time (copy date)
            return (st.st_mtime, st.st_ctime, p.name.lower())
        except Exception:
            return (float("inf"), float("inf"), p.name.lower())

    # ---- Missing-date assignment with interpolation ----
    def _assign_dates_for_to_fix(self, to_fix_paths, anchor_items, fallback_base_date):
        """
        to_fix_paths: list[Path] without usable dates
        anchor_items: list[{'path': Path, 'date': datetime}] known good dates (already filtered to event)
        fallback_base_date: datetime
        Returns list[{'path': Path, 'date': datetime}] for to_fix
        """
        if not to_fix_paths:
            return []

        anchors_by_path = {}
        for a in anchor_items:
            try:
                anchors_by_path[Path(a["path"])] = a["date"]
            except Exception:
                pass

        # Build combined list for ordering
        combined = []
        for p in to_fix_paths:
            combined.append((p, None))
        for p, dt in anchors_by_path.items():
            combined.append((p, dt))

        combined.sort(key=lambda x: self._file_order_key(x[0]))

        # Find indices of known dates
        known_idx = [i for i, (_, dt) in enumerate(combined) if dt is not None]

        # No anchors => distribute sequentially from fallback_base_date within a window
        if not known_idx:
            base = fallback_base_date.replace(microsecond=0)
            n = len(to_fix_paths)
            window_seconds = DEFAULT_ASSIGN_WINDOW_HOURS * 3600
            step = max(1, window_seconds // max(1, n - 1))
            step = max(step, DEFAULT_ASSIGN_STEP_SECONDS)

            out = []
            # Only in to_fix order (by file_order_key)
            ordered_fix = sorted(to_fix_paths, key=self._file_order_key)
            for i, p in enumerate(ordered_fix):
                out.append({"path": p, "date": base + datetime.timedelta(seconds=i * step)})
            return out

        # Helpers to ensure monotonic and avoid duplicates
        def ensure_gt(prev_dt, dt):
            if prev_dt is None:
                return dt
            if dt <= prev_dt:
                return prev_dt + datetime.timedelta(seconds=1)
            return dt

        result_map = {}  # path -> assigned dt for to_fix
        prev_dt_global = None

        # 1) Before first known: go backwards
        first_i = known_idx[0]
        first_dt = combined[first_i][1].replace(microsecond=0)
        step = datetime.timedelta(seconds=DEFAULT_ASSIGN_STEP_SECONDS)
        unknown_before = [combined[i][0] for i in range(0, first_i) if combined[i][1] is None]

        # Assign in order, but backwards in time so that earlier files get earlier timestamps
        for back_pos, p in enumerate(reversed(unknown_before), start=1):
            dt = first_dt - step * back_pos
            result_map[p] = dt

        # 2) Between known anchors: interpolate
        for k in range(len(known_idx) - 1):
            i0 = known_idx[k]
            i1 = known_idx[k + 1]
            t0 = combined[i0][1].replace(microsecond=0)
            t1 = combined[i1][1].replace(microsecond=0)

            unknown_between = [combined[i][0] for i in range(i0 + 1, i1) if combined[i][1] is None]
            n = len(unknown_between)
            if n == 0:
                continue

            if t1 > t0:
                total = (t1 - t0).total_seconds()
                step_sec = max(DEFAULT_ASSIGN_STEP_SECONDS, int(total // (n + 1)))
                for j, p in enumerate(unknown_between, start=1):
                    result_map[p] = t0 + datetime.timedelta(seconds=j * step_sec)
            else:
                # Same/invalid order => just step forward from t0
                for j, p in enumerate(unknown_between, start=1):
                    result_map[p] = t0 + datetime.timedelta(seconds=j * DEFAULT_ASSIGN_STEP_SECONDS)

        # 3) After last known: go forward
        last_i = known_idx[-1]
        last_dt = combined[last_i][1].replace(microsecond=0)
        unknown_after = [combined[i][0] for i in range(last_i + 1, len(combined)) if combined[i][1] is None]
        for fwd_pos, p in enumerate(unknown_after, start=1):
            result_map[p] = last_dt + step * fwd_pos

        # 4) Emit final list for to_fix in stable order; also enforce monotonic globally
        ordered_fix = sorted(to_fix_paths, key=self._file_order_key)
        out = []
        for p in ordered_fix:
            dt = result_map.get(p, fallback_base_date).replace(microsecond=0)
            dt = ensure_gt(prev_dt_global, dt)
            prev_dt_global = dt
            out.append({"path": p, "date": dt})
        return out

    # ---- File system dates via PowerShell ----
    def _update_fs_dates_powershell(self, item_list):
        if not item_list:
            return

        # Build PS script lines
        ps_lines = ["$ErrorActionPreference = 'Stop'"]
        for item in item_list:
            self.check_stop()
            p = str(item["path"].absolute()).replace("'", "''")
            d = item["date"].strftime("%Y-%m-%d %H:%M:%S")
            ps_lines.append(f"$p = '{p}'")
            ps_lines.append(f"$d = [datetime]::ParseExact('{d}', 'yyyy-MM-dd HH:mm:ss', $null)")
            ps_lines.append("(Get-Item -LiteralPath $p).CreationTime = $d")
            ps_lines.append("(Get-Item -LiteralPath $p).LastWriteTime = $d")
            # Optional:
            # ps_lines.append("(Get-Item -LiteralPath $p).LastAccessTime = $d")

        tempdir = self.gui.script_dir
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8-sig", delete=False, suffix=".ps1", dir=tempdir) as f:
            script_path = Path(f.name)
            f.write("\n".join(ps_lines))

        try:
            cmd = ["powershell", "-ExecutionPolicy", "Bypass", "-File", str(script_path)]
            out_b, err_b, rc = self._run_process_interruptible(cmd)
            if rc != 0:
                err = ""
                if err_b:
                    # cp866 often on Windows console tools; try both
                    try:
                        err = err_b.decode("cp866", errors="replace").strip()
                    except Exception:
                        err = err_b.decode("utf-8", errors="replace").strip()
                if err:
                    self.log(f"PowerShell ошибка: {err}", "red")
                else:
                    self.log("PowerShell ошибка (код != 0)", "red")
                self.stats.errors += 1
        except StopRequested:
            raise
        except Exception as e:
            self.log(f"PowerShell crash: {e}", "red")
            self.stats.errors += 1
        finally:
            try:
                script_path.unlink(missing_ok=True)
            except Exception:
                pass

    # ---- Exif update (CSV import) ----
    def run_exif_update(self, item_list, scan_root: Path):
        """
        item_list = [{'path': Path, 'date': datetime}]
        scan_root: folder to scan (ExifTool uses it to match SourceFile rows during CSV import).
        """
        if not item_list:
            return

        exif_list = []
        fs_list = []
        for item in item_list:
            ext = item["path"].suffix.lower()
            if ext in FS_ONLY_EXTENSIONS:
                fs_list.append(item)
            else:
                exif_list.append(item)

        # 1) ExifTool import
        if exif_list:
            tempdir = self.gui.script_dir
            with tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", delete=False, suffix=".csv", dir=tempdir) as csvfile:
                csv_path = Path(csvfile.name)
                writer = csv.DictWriter(csvfile, fieldnames=["SourceFile", "DateTimeOriginal", "CreateDate", "MediaCreateDate"])
                writer.writeheader()
                for item in exif_list:
                    self.check_stop()
                    dstr = self._format_exif_datetime(item["date"])
                    writer.writerow({
                        "SourceFile": str(item["path"].absolute()),
                        "DateTimeOriginal": dstr,
                        "CreateDate": dstr,
                        "MediaCreateDate": dstr,
                    })

            # Create argfile for scan_root
            arg_file_path = None
            try:
                # Use 'mbcs' (ANSI)
                with tempfile.NamedTemporaryFile(mode="w", encoding="mbcs", delete=False) as f:
                    f.write(str(scan_root))
                    arg_file_path = f.name
            except Exception:
                pass

            cmd = [
                str(self.exif_path),
                f"-csv={str(csv_path)}",
                "-overwrite_original",
                "-m",
                "-q", "-q",
                "-f",
                # Removed utf8 charsets
                "-r",
                "-@", str(arg_file_path) if arg_file_path else str(scan_root)
            ]

            try:
                self.log(f"ExifTool: обновление дат ({len(exif_list)} файлов)...", "blue")
                out_b, err_b, rc = self._run_process_interruptible(cmd)

                out_s = out_b.decode("utf-8", errors="replace") if out_b else ""
                err_s = err_b.decode("utf-8", errors="replace") if err_b else ""

                failed_files = []

                # Filter common benign warnings
                if err_s.strip():
                    lines = [ln.strip() for ln in err_s.splitlines() if ln.strip()]
                    relevant = [ln for ln in lines if "No SourceFile" not in ln]
                    
                    if relevant:
                        self.log("ExifTool stderr:", "orange")
                        for ln in relevant[:30]:
                            self.log(f"  {ln}", "orange")
                            # Detect failed files from error messages like:
                            # Error: Error reading OtherImageStart ... - E:/path/to/file.jpg
                            # Error: ... - E:/path/to/file.jpg
                            m_err = re.search(r' - ([:A-Za-z]:[\\/].+)$', ln)
                            if m_err:
                                fpath_str = m_err.group(1).strip()
                                try:
                                    failed_files.append(Path(fpath_str))
                                except:
                                    pass

                if rc != 0:
                    self.log(f"ExifTool завершился с кодом {rc} (частичный сбой)", "red")
                    self.stats.errors += 1
                else:
                    m = re.search(r'(\d+)\s+image files updated', out_s)
                    if m:
                        self.log(f"ExifTool: {m.group(1)} files updated", "green")
                    else:
                        first = (out_s.splitlines()[0].strip() if out_s.strip() else "")
                        if first:
                            self.log(f"ExifTool: {first}", "gray")

                # Fallback: if specific files failed in ExifTool, try to at least set FS dates
                if failed_files:
                    self.log(f"Попытка исправить {len(failed_files)} сбойных файлов через FS (PowerShell)...", "orange")
                    # Find original item data for these paths
                    fallback_items = []
                    # Create a quick lookup
                    lookup = {str(x["path"].absolute()).lower(): x for x in exif_list}
                    
                    for p in failed_files:
                        key = str(p.absolute()).lower()
                        if key in lookup:
                            fallback_items.append(lookup[key])
                    
                    if fallback_items:
                        self._update_fs_dates_powershell(fallback_items)

            except StopRequested:
                raise
            except Exception as e:
                self.log(f"ExifTool crash: {e}", "red")
                self.stats.errors += 1
            finally:
                try:
                    csv_path.unlink()
                except:
                    pass
                if arg_file_path and os.path.exists(arg_file_path):
                    try:
                        os.unlink(arg_file_path)
                    except:
                        pass

        # 2) FS-only import via PowerShell
        if fs_list:
            self.log(f"FS: обновление Creation/LastWrite ({len(fs_list)} файлов)...", "blue")
            self._update_fs_dates_powershell(fs_list)

    # ---- Folder cleanup ----
    def _remove_empty_recursive(self, path: Path):
        # Bottom-up: remove empty dirs, optionally remove 0-byte files too
        if not path.exists():
            return
        for _ in range(5):  # multiple passes
            deleted = 0
            for root, dirs, files in os.walk(path, topdown=False):
                # remove zero-byte files if enabled
                if self.delete_empty:
                    for name in files:
                        p = Path(root) / name
                        try:
                            if p.exists() and p.stat().st_size == 0:
                                if self.apply:
                                    p.unlink(missing_ok=True)
                                deleted += 1
                        except Exception:
                            pass

                # remove empty dirs
                for dname in dirs:
                    d = Path(root) / dname
                    try:
                        if d.exists() and not any(d.iterdir()):
                            if self.apply:
                                d.rmdir()
                            deleted += 1
                    except Exception:
                        pass

            if deleted == 0:
                break

    # ---- Outliers ----
    def _move_outlier(self, anchor, parent_root: Path):
        src = anchor["path"]
        dt = anchor["date"]
        target_name = dt.strftime("%Y-%m-%d")
        target_folder = parent_root / target_name
        target_file = target_folder / src.name

        self.log(f"       -> {src.name} в {target_name}", "orange")

        # DEBUG: WinError 123 diagnostics
        self.log(f"          [DEBUG_PATH] Src: {repr(str(src))}", "gray")
        self.log(f"          [DEBUG_PATH] Tgt: {repr(str(target_file))}", "gray")
        self.log(f"          [DEBUG_PATH] Dir: {repr(str(target_folder))}", "gray")

        if self.apply:
            try:
                target_folder.mkdir(parents=True, exist_ok=True)
                if target_file.exists():
                    self.log("          ОШИБКА: Файл уже существует в цели (пропуск)", "red")
                    self.stats.errors += 1
                    return
                shutil.move(str(src), str(target_file))
                self.stats.files_moved += 1
            except Exception as e:
                self.log(f"          ОШИБКА перемещения: {e}", "red")
                self.stats.errors += 1
        else:
            self.log("          (Тест) Будет перемещен", "gray")

    def _merge_folders(self, src: Path, dst: Path):
        if src == dst:
            return

        try:
            moved_count = 0
            for item in list(src.iterdir()):
                self.check_stop()
                target = dst / item.name
                if target.exists():
                    self.log(f"       КОНФЛИКТ: {item.name} уже есть в цели (пропуск)", "red")
                    self.stats.merge_conflicts += 1
                    continue
                try:
                    shutil.move(str(item), str(target))
                    moved_count += 1
                except Exception as e:
                    self.log(f"       Ошибка перемещения {item.name}: {e}", "red")
                    self.stats.errors += 1

            if not any(src.iterdir()):
                if self.apply:
                    src.rmdir()
                self.log(f"     СЛИЯНИЕ УСПЕШНО: '{src.name}' удалена (все перенесено)", "green")
                self.stats.folders_merged += 1
            else:
                rem = len(list(src.iterdir()))
                self.log(f"     СЛИЯНИЕ НЕПОЛНОЕ: Осталось {rem} элементов в '{src.name}'", "orange")

        except StopRequested:
            raise
        except Exception as e:
            self.log(f"     CRASH при слиянии: {e}", "red")
            self.stats.errors += 1

    # ---- Main run ----
    def run(self):
        self.log(f"=== Запущено: {datetime.datetime.now()} ===")
        if self.recursive:
            self.log(f"РЕКУРСИВНЫЙ РЕЖИМ: Сканирование {self.root}...", "blue")
            subfolders = sorted([f for f in self.root.iterdir() if f.is_dir() and re.match(r'^\d{4}', f.name)])

            if self.start_from:
                original_count = len(subfolders)
                subfolders = [f for f in subfolders if f.name >= self.start_from]
                self.log(f"ФИЛЬТР: Начать с '{self.start_from}' -> {len(subfolders)} из {original_count} папок", "blue")

            self.log(f"Найдено папок событий: {len(subfolders)}\n")

            for i, folder in enumerate(subfolders, start=1):
                self.check_stop()
                self.stats.folders_processed += 1
                self.gui.set_status(f"Обработка папки {i}/{len(subfolders)}: {folder.name}")
                # Log happens inside process_folder now
                self.process_folder(folder, progress_tag=f"[{i}/{len(subfolders)}] ")
        else:
            self.stats.folders_processed += 1
            self.gui.set_status(f"Обработка папки: {self.root.name}")
            self.process_folder(self.root, progress_tag="[1/1] ")

        self.log("\n========================================")
        self.log("Готово.")
        self.gui.set_status("Готово")

    # ---- Core folder processing ----
    def process_folder(self, folder: Path, progress_tag=""):
        folder_date, granularity = self.get_folder_date(folder.name)
        if not folder_date:
            self.log(f"ПРОПУСК: '{folder.name}' (Неверное имя или Год < {MIN_VALID_YEAR})", "gray")
            return 0

        # Double newline for separation from previous logs
        self.log("\n\n" + "="*80, "blue")
        self.log(f"ПАПКА: {progress_tag}{folder.name}", "blue")
        self.log(f"Дата:  {folder_date.strftime('%Y-%m-%d')}", "blue")
        self.log("="*80, "blue")

        # Pre-scan
        self.gui.set_status(f"Подсчет файлов в {folder.name}...")
        self.gui.update_idletasks()

        files = []
        
        # Sanitizer Check
        sanitize_enabled = self.args.get("sanitize", False)

        with os.scandir(str(folder)) as it:
            for entry in it:
                if entry.is_file():
                    p = Path(entry.path)
                    
                    # Apply sanitization if requested
                    if sanitize_enabled:
                        p = self._sanitize_filename(p)
                        
                    # Filter by extension
                    if p.suffix.lower() in IGNORE_EXTENSIONS:
                        continue
                    # If known safe extension or check signature? 
                    # We just let ExifTool decide or use whitelist?
                    # Script uses known Exts later.
                    # Check name for garbage date pattern
                    files.append(p)
        
        if not files:
            self.log("  [Нет медиа файлов]", "gray")
            return 0
        
        total_files = len(files)
        self.stats.files_total += total_files
        self.log(f"  Всего файлов: {total_files} (Запуск ExifTool...)", "blue")

        # Pre checks: ghost / empty / fake dng
        empty_files = []
        for f in files: # Changed from scan_list to files
            self.check_stop()

            # Ignore junk extensions early
            if f.suffix.lower() in IGNORE_EXTENSIONS:
                continue

            # AppleDouble / ghost files
            if f.name.startswith("._"):
                try:
                    if f.exists() and f.stat().st_size < 100 * 1024:
                        self.log(f"  Ghost-файл игнорирован: {f.name}", "gray")
                        self.stats.files_ghost_ignored += 1
                        continue
                except Exception:
                    pass

            # Empty files
            try:
                if f.exists() and f.stat().st_size == 0:
                    empty_files.append(f)
                    continue
            except Exception:
                pass

            # Fake DNG (JPEG header)
            if f.suffix.lower() == ".dng":
                is_fake = False
                try:
                    with open(f, "rb") as dngf:
                        head = dngf.read(3)
                        if head == b"\xFF\xD8\xFF":
                            is_fake = True
                except Exception as e:
                    self.log(f"    Ошибка проверки DNG: {e}", "red")

                if is_fake:
                    new_p = f.with_suffix(".jpg")
                    self.log(f"  ИСПРАВЛЕНИЕ: {f.name} -> .jpg (Fake DNG)", "magenta")
                    self.stats.files_renamed_dng += 1
                    if self.apply:
                        try:
                            f.rename(new_p)
                        except Exception as e:
                            self.log(f"    Ошибка переименования: {e}", "red")
                            self.stats.errors += 1
                    else:
                        self.log("    (Тест) Будет переименовано", "gray")

        if empty_files:
            if self.delete_empty:
                self.log(f"  Удаление пустых файлов (0 байт): {len(empty_files)} шт.", "orange")
                for f in empty_files:
                    try:
                        if self.apply:
                            f.unlink(missing_ok=True)
                    except Exception as e:
                        self.log(f"    Ошибка удаления {f.name}: {e}", "red")
                        self.stats.errors += 1
            else:
                self.log(f"  ВНИМАНИЕ: Найдено {len(empty_files)} файлов 0 байт (пропуск)", "orange")

        # Exif scan
        exif_data = self.get_exif_json(folder)
        if not exif_data:
            self.log("  [Нет медиа файлов]", "gray")
            return 0

        self.log(f"  Найдено файлов (ExifTool): {len(exif_data)}")

        anchors = []           # list of {'path': Path, 'date': datetime}
        filename_dates = []    # subset where filename provides date and needs update
        to_fix = []            # list[Path] with missing/invalid date

        # Build anchors/to_fix
        for item in exif_data:
            self.check_stop()
            try:
                path = Path(item.get("SourceFile", ""))
            except Exception:
                continue

            if not path.name:
                continue

            # Ignore junk extensions
            if path.suffix.lower() in IGNORE_EXTENSIONS:
                continue

            # 1) Filename date has priority
            fdate = self.get_date_from_filename(path)
            if fdate:
                raw_exif = item.get("DateTimeOriginal") or item.get("CreateDate")
                existing_dt = self._parse_exif_date(raw_exif)

                is_match = False
                if existing_dt and existing_dt == fdate:
                    is_match = True
                
                # DEBUG: Log if mismatch to see what's happening
                if not is_match:
                     self.log(f"   [DEBUG_DATE] File: {path.name}", "gray")
                     self.log(f"      Filename: {fdate} | Exif: {existing_dt} (Raw: '{raw_exif}')", "gray")

                if not is_match and not existing_dt and path.suffix.lower() in FS_ONLY_EXTENSIONS:
                    fs_raw = item.get("FileCreateDate")
                    fs_dt = self._parse_exif_date(fs_raw)
                    if fs_dt and fs_dt == fdate:
                        is_match = True

                anchors.append({"path": path, "date": fdate})
                if not is_match:
                    filename_dates.append({"path": path, "date": fdate})
                continue

            # 2) Exif date or FS fallback
            raw = item.get("DateTimeOriginal") or item.get("CreateDate") or item.get("MediaCreateDate")
            if not raw:
                raw = item.get("FileCreateDate") or item.get("FileModifyDate")

            dt_clean = self._parse_exif_date(raw)
            if dt_clean:
                # Validation logic
                is_valid = False
                
                if self.folder_priority:
                    # Strict check against folder scope (User Request)
                    # If Folder is 2007, 2006 is invalid.
                    # If Folder is 2007-01, 2007-02 is invalid (strict month).
                    # If Folder is 2007-01-01, we treat it as Month/Year constraint? 
                    # Usually "Day" folder implies Start Date.
                    
                    if granularity == "Year":
                        is_valid = (dt_clean.year == folder_date.year)
                    elif granularity == "Month":
                        is_valid = (dt_clean.year == folder_date.year and dt_clean.month == folder_date.month)
                    else: # Day
                        # For specific day, we enforce at least Same Month (to avoid 2006-12 vs 2007-01 issues)
                        # or just Same Year? User wants to fix 2006-12 vs 2007-01.
                        # So strict Month match is safer if provided.
                        is_valid = (dt_clean.year == folder_date.year and dt_clean.month == folder_date.month)
                        # If the event spans months (e.g. Jan 31 - Feb 2), this is too strict?
                        # But User explicitly asked for "Priority to Folder Name range".
                        # If Folder is "2007-01", it implies Jan numbers.
                        
                else:
                    # Normal logic (tolerant)
                    delta = abs((dt_clean - folder_date).days)
                    # Allow same year OR within 30 days (handles New Year boundaries)
                    is_valid = (dt_clean.year >= MIN_VALID_YEAR and (dt_clean.year == folder_date.year or delta <= 30))

                if is_valid:
                    anchors.append({"path": path, "date": dt_clean})
                else:
                    to_fix.append(path)
            else:
                to_fix.append(path)

        if filename_dates:
            self.log(f"  -> Файлов с датой в имени (нужно обновить): {len(filename_dates)}", "green")
            self.log("     (Exif/FS будет перезаписан датой из имени)", "orange")

        exif_only_count = len(anchors) - len(filename_dates)
        if exif_only_count > 0:
            self.log(f"  -> Файлов с корректной датой (якоря): {exif_only_count}", "green")

        if to_fix:
            self.log(f"  -> Файлов без корректной даты (to_fix): {len(to_fix)}", "orange")

        # [NEW] Time Shift Check
        if self.shift and anchors and folder_date:
            shift_delta = self._check_time_shift(anchors, folder_date)
            if shift_delta:
                self.log(f"\n  ОБНАРУЖЕН СДВИГ ДАТ (относительно {folder_date.date()}): {shift_delta.days} дней", "magenta")
                do_shift = True
                if self.interactive:
                    do_shift = self.gui.ask_yesno_threadsafe("Сдвиг дат", 
                        f"Обнаружено, что файлы смещены на {shift_delta.days} дней.\n"
                        f"Исправить даты у {len(anchors)} файлов?"
                    )
                
                if do_shift:
                    self.log(f"  ДЕЙСТВИЕ: Применение сдвига {shift_delta} к {len(anchors)} файлам...", "magenta")
                    shifted_list = []
                    for a in anchors:
                        new_date = a["date"] - shift_delta # Ops. Wait. Diff = File - Folder.
                        # If File is 2008, Folder is 2007. Diff = +1 year.
                        # We want to make File 2007. So New = File - Diff. Correct.
                        a["date"] = new_date
                        shifted_list.append(a)
                    
                    if self.apply:
                        self.run_exif_update(shifted_list, scan_root=folder)
                    self.stats.files_fixed_date += len(shifted_list)
                    self.stats.files_fixed_shifted += len(shifted_list)
                    self.log("  Даты скорректированы.", "green")
                else:
                    self.log("  Сдвиг отменен пользователем.", "gray")

        # 1) Apply filename-based updates first
        if filename_dates:
            self.log("\n  ДЕЙСТВИЕ: Обновление Exif/FS из имени файла...", "blue")
            if self.apply:
                self.run_exif_update(filename_dates, scan_root=folder)
            self.stats.files_fixed_date += len(filename_dates)
            self.stats.files_fixed_filename += len(filename_dates)

        # 2) Dominant year + outliers
        valid_anchors = []
        outliers = []
        ren_base_date = folder_date

        if anchors:
            anchors_sorted = sorted(anchors, key=lambda x: x["date"])
            year_counts = {}
            for a in anchors_sorted:
                y = a["date"].year
                year_counts[y] = year_counts.get(y, 0) + 1

            dom_year = sorted(year_counts.items(), key=lambda x: (x[1], x[0]), reverse=True)[0][0]
            self.log(f"  Анализ дат: Доминирующий год = {dom_year} ({year_counts[dom_year]} файлов)", "blue")

            for a in anchors_sorted:
                y = a["date"].year
                is_outlier = False
                diff = y - dom_year
                
                if abs(diff) > 1:
                    is_outlier = True
                elif abs(diff) == 1:
                    # Adjacent year is only valid if it's close to the boundary (New Year celebration)
                    # If file is from prev year, it must be late in year (e.g. Oct-Dec)
                    # If file is from next year, it must be early in year (e.g. Jan-Mar)
                    m = a["date"].month
                    if diff == -1: # File is older (e.g. 2020 vs Dom 2021)
                        if m < 10: # Allow only Oct, Nov, Dec
                            is_outlier = True
                    else: # File is newer (e.g. 2022 vs Dom 2021)
                        if m > 3: # Allow only Jan, Feb, Mar
                            is_outlier = True

                if is_outlier:
                    outliers.append(a)
                else:
                    valid_anchors.append(a)

            if outliers:
                self.log(f"\n  ДЕЙСТВИЕ: Перенос файлов-выбросов ({len(outliers)} шт)...", "magenta")
                for o in outliers:
                    self._move_outlier(o, folder.parent)

            if valid_anchors:
                valid_anchors.sort(key=lambda x: x["date"])
                ren_base_date = valid_anchors[0]["date"]
            else:
                ren_base_date = folder_date

        # 3) Fix to_fix by distributing dates by file order + interpolation between anchors
        if to_fix:
            self.log("\n  ДЕЙСТВИЕ: Назначение дат для to_fix (по порядку + интерполяция)...", "blue")

            # Use valid_anchors (after outliers removed) + include filename anchors too (they are valid dates)
            # But avoid using outliers; filename dates are already included in anchors list as dates from filename.
            anchor_for_assignment = valid_anchors if valid_anchors else anchors

            assigned = self._assign_dates_for_to_fix(
                to_fix_paths=sorted(list(dict.fromkeys(to_fix))),
                anchor_items=anchor_for_assignment,
                fallback_base_date=ren_base_date
            )

            # Preview a few
            assigned_sorted = sorted(assigned, key=lambda x: self._file_order_key(x["path"]))
            for i, it in enumerate(assigned_sorted[:8]):
                self.log(f"    {it['path'].name} -> {it['date']}", "gray")
            if len(assigned_sorted) > 8:
                self.log(f"    ... и еще {len(assigned_sorted) - 8}", "gray")

            if self.apply:
                self.run_exif_update(assigned_sorted, scan_root=folder)
            self.stats.files_fixed_date += len(assigned_sorted)
            self.stats.files_fixed_interpolated += len(assigned_sorted)

        # 4) Rename folder based on valid anchors duration
        anchors_for_rename = valid_anchors if valid_anchors else anchors
        new_prefix = ""
        if anchors_for_rename:
            anchors_for_rename.sort(key=lambda x: x["date"])
            delta = anchors_for_rename[-1]["date"] - anchors_for_rename[0]["date"]
            if delta.days < SHORT_EVENT_DAYS:
                new_prefix = ren_base_date.strftime("%Y-%m-%d")
            else:
                new_prefix = ren_base_date.strftime("%Y-%m")
        else:
            new_prefix = ren_base_date.strftime("%Y-%m")

        clean_name = re.sub(r'^\d{4}([-._]\d{2})?([-._]\d{2})?(\s*[-._]\s*|\s+)?', '', folder.name)
        new_name = f"{new_prefix} {clean_name}".strip() if clean_name else new_prefix

        effective_path = folder
        if new_name != folder.name:
            self.log("\n  ДЕЙСТВИЕ: Переименование папки", "magenta")
            self.log(f"     Старое: '{folder.name}'", "gray")
            self.log(f"     Новое:  '{new_name}'", "green")

            do_ren = True
            if self.apply and self.interactive:
                # Allow user to EDIT the name
                user_name = self.gui.ask_string_threadsafe(
                    "Переименование папки",
                    f"Текущее имя: {folder.name}\nПрограмма предлагает: {new_name}\n\nВведите новое имя (или Cancel для отмены):",
                    new_name
                )
                if user_name:
                    # Sanitize user input
                    safe_name = re.sub(r'[<>:"/\\|?*]', '_', user_name).strip()
                    if safe_name != user_name:
                         self.log(f"     Имя скорректировано (удалены спецсимволы): '{safe_name}'", "orange")
                    new_name = safe_name
                    self.log(f"     Пользователь изменил на: '{new_name}'", "blue")
                else:
                    do_ren = False

            if self.apply and do_ren:
                if new_name == folder.name:
                    self.log("     Имя совпадает с текущим (пропуск).", "gray")
                else:
                    new_path = folder.parent / new_name
                    if new_path.exists():
                        self.log(f"     ЦЕЛЬ СУЩЕСТВУЕТ: '{new_name}'", "orange")
                        self.log("     Запуск СЛИЯНИЯ папок...", "magenta")
                        self._merge_folders(folder, new_path)
                        effective_path = new_path
                    else:
                        try:
                            folder.rename(new_path)
                            effective_path = new_path
                            self.log("     Переименовано.", "green")
                            self.stats.folders_renamed += 1
                        except Exception as e:
                            self.log(f"     Ошибка: {e}", "red")
                            self.stats.errors += 1
            else:
                if not self.apply:
                    self.log("     (Тест) Переименование не выполняется", "gray")
                elif not do_ren:
                    self.log("     Отменено пользователем.", "orange")

        # 5) Cleanup empty
        if self.delete_empty:
            self._remove_empty_recursive(effective_path)

        return 0

    def _sanitize_filename(self, path: Path) -> Path:
        """
        Removes emojis / unusual chars from filename (stem).
        Keeps: Latin, Cyrillic, digits, space, -, _, (), and preserves extension.
        IMPORTANT: In Dry Run does NOT change returned path (to avoid breaking subsequent logic).
        """
        original_name = path.name
        stem = path.stem
        suffix = path.suffix  # keep extension intact

        # Whitelist for stem (no dots here; dot belongs to suffix)
        safe_stem = re.sub(r'[^a-zA-Z0-9а-яА-ЯёЁ\-\_\(\)\s]', '', stem)
        safe_stem = re.sub(r'\s+', ' ', safe_stem).strip()
        safe_stem = safe_stem.rstrip(' .')  # avoid Windows trailing dot/space issues

        # If stem became empty -> fallback
        if not safe_stem:
            safe_stem = "renamed_file"

        # Reserved device names on Windows
        upper = safe_stem.upper()
        reserved = {"CON", "PRN", "AUX", "NUL"} | {f"COM{i}" for i in range(1, 10)} | {f"LPT{i}" for i in range(1, 10)}
        if upper in reserved:
            safe_stem = f"_{safe_stem}"

        new_name = safe_stem + suffix
        if new_name == original_name:
            return path

        new_path = path.with_name(new_name)

        # Handle collision
        if new_path.exists():
            cnt = 1
            while new_path.exists():
                new_path = path.with_name(f"{safe_stem}_{cnt}{suffix}")
                cnt += 1

        self.stats.files_sanitized += 1

        # Apply or dry-run
        if self.apply:
            try:
                path.rename(new_path)
                self.log(f"  [Sanitize] {original_name} -> {new_path.name}", "orange")
                return new_path
            except Exception as e:
                self.log(f"  Ошибка переименования {original_name}: {e}", "red")
                return path
        else:
            self.log(f"  [Sanitize] {original_name} -> {new_path.name} (Dry Run)", "orange")
            return path

    # ---- Time Shift Check ----
    def _check_time_shift(self, anchors, folder_date):
        if not anchors or not folder_date:
            return None
        
        # Simplified Logic per user request:
        # Detect if anchors are shifted by exactly +/- N years or large hours from folder_date
        # Just use the Media of Difference?
        
        # If > 50% of files have same year, but distinct from folder year?
        anchors_valid = [a for a in anchors if a["date"].year >= MIN_VALID_YEAR]
        if not anchors_valid:
            return None
        
        # Dominant anchor timestamp
        anchors_valid.sort(key=lambda x: x["date"])
        median_date = anchors_valid[len(anchors_valid)//2]["date"]
        
        # Difference from folder date (assuming folder date is "approximate start")
        # If folder is "2007-01 Elbrus", folder_date is 2007-01-01.
        # If median file is 2006-12-31, diff is -1 day. (Not a shift)
        # If median file is 2008-01-01, diff is +1 year. (Shift!)
        
        diff = median_date - folder_date
        days = diff.days
        
        # Check for Year shift using Calendar math (handling leap years)
        # We assume the 'median_date' should match 'folder_date' in Month/Day if we shift years.
        
        try:
             # Calculate rough difference in years
            years_shift = round(days / 365.25)
            if years_shift == 0:
                 return None

            # Calculate precise target date by shifting year
            target_date = median_date.replace(year=median_date.year - years_shift)
            
            # The discrepancy between current file date and (folder date shifted to file's year)
            # Actually simplest: Calculate Delta = FileDate - (FileDate - Years)
            # No. We successfully shifted if (median_date - shift) ~= folder_date.
            
            # Let's verify if this year shift aligns well
            # If we apply this years_shift, the new date would be:
            new_date_candidate = median_date.replace(year=median_date.year - years_shift)
            
            # Distance from folder_date
            # Check if new_date_candidate is close to folder_date (within 30 days)
            diff_remaining = abs((new_date_candidate - folder_date).days)
            
            if diff_remaining < 30:
                # It is a clean year shift! 
                # We return the timedelta that REPRESENTS this shift for the median file.
                # This ensures we account for leap days correctly for the dominant group.
                exact_delta = median_date - new_date_candidate
                return exact_delta
        except ValueError:
            # Leap year error (e.g. Feb 29 mapped to non-leap), fallback to rough
            pass
            
        # Fallback to simple logic if calendar math fails or doesn't match
        if abs(years_shift) > 0 and abs(days - years_shift*365.25) < 30:
             return datetime.timedelta(days=years_shift*365)
             
        # Check for large offsets (User requested "Hours" or just large shifts)
        if abs(days) > 300:
             return datetime.timedelta(days=days)
             
        return None


if __name__ == "__main__":
    app = MediaLibraryTool()
    app.mainloop()

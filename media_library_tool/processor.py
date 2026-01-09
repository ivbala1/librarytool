"""
Основная логика обработки медиафайлов и папок событий.
"""

import datetime
import os
import re
import shutil
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from .constants import (
    DEFAULT_ASSIGN_STEP_SECONDS,
    DEFAULT_ASSIGN_WINDOW_HOURS,
    IGNORE_EXTENSIONS,
    MIN_VALID_YEAR,
    SHORT_EVENT_DAYS,
    RE_FOLDER_PREFIX,
    WINDOWS_RESERVED_NAMES,
)
from .date_parser import DateParser, parse_exif_date
from .exif_handler import ExifHandler, StopRequested
from .logger import Logger
from .stats import Stats


class MediaProcessor:
    """
    Основной процессор медиабиблиотеки.
    
    Обрабатывает папки событий:
    - Извлекает даты из имён файлов и EXIF
    - Обнаруживает и перемещает выбросы (outliers)
    - Интерполирует даты для файлов без дат
    - Переименовывает папки
    """
    
    def __init__(
        self,
        root: Path,
        exif_path: str,
        script_dir: Path,
        logger: Logger,
        stats: Stats,
        check_stop: Callable[[], None],
        set_status: Callable[[str], None],
        ask_yesno: Callable[[str, str], bool],
        ask_string: Callable[[str, str, str], Optional[str]],
        apply: bool = False,
        recursive: bool = False,
        shift: bool = False,
        delete_empty: bool = False,
        start_from: str = "",
        folder_priority: bool = False,
        sanitize: bool = False,
        interactive: bool = False,
    ):
        self.root = root
        self.logger = logger
        self.stats = stats
        self.check_stop = check_stop
        self.set_status = set_status
        self.ask_yesno = ask_yesno
        self.ask_string = ask_string
        
        self.apply = apply
        self.recursive = recursive
        self.shift = shift
        self.delete_empty = delete_empty
        self.start_from = start_from.strip()
        self.folder_priority = folder_priority
        self.sanitize = sanitize
        self.interactive = interactive
        
        self.date_parser = DateParser()
        self.exif_handler = ExifHandler(
            exif_path=exif_path,
            script_dir=script_dir,
            logger=logger,
            check_stop=check_stop,
            apply=apply
        )
        
        self.stats.reset()
    
    def log(self, msg: str, color: Optional[str] = None) -> None:
        """Логирование через Logger."""
        self.logger.log(msg, color)
    
    def _file_order_key(self, p: Path) -> Tuple:
        """Ключ сортировки файлов по времени модификации."""
        try:
            st = p.stat()
            return (st.st_mtime, st.st_ctime, p.name.lower())
        except OSError:
            return (float("inf"), float("inf"), p.name.lower())
    
    def _assign_dates_for_to_fix(
        self,
        to_fix_paths: List[Path],
        anchor_items: List[Dict],
        fallback_base_date: datetime.datetime
    ) -> List[Dict]:
        """
        Назначить даты файлам без дат через интерполяцию.
        
        Args:
            to_fix_paths: Файлы без дат
            anchor_items: Файлы с известными датами
            fallback_base_date: Базовая дата если нет якорей
            
        Returns:
            Список {'path': Path, 'date': datetime} для to_fix
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
        combined = [(p, None) for p in to_fix_paths]
        combined.extend((p, dt) for p, dt in anchors_by_path.items())
        combined.sort(key=lambda x: self._file_order_key(x[0]))
        
        known_idx = [i for i, (_, dt) in enumerate(combined) if dt is not None]
        
        # No anchors => distribute sequentially from fallback_base_date
        if not known_idx:
            base = fallback_base_date.replace(microsecond=0)
            n = len(to_fix_paths)
            window_seconds = DEFAULT_ASSIGN_WINDOW_HOURS * 3600
            step = max(1, window_seconds // max(1, n - 1))
            step = max(step, DEFAULT_ASSIGN_STEP_SECONDS)
            
            ordered_fix = sorted(to_fix_paths, key=self._file_order_key)
            return [
                {"path": p, "date": base + datetime.timedelta(seconds=i * step)}
                for i, p in enumerate(ordered_fix)
            ]
        
        def ensure_gt(prev_dt, dt):
            if prev_dt is None:
                return dt
            if dt <= prev_dt:
                return prev_dt + datetime.timedelta(seconds=1)
            return dt
        
        result_map = {}
        step = datetime.timedelta(seconds=DEFAULT_ASSIGN_STEP_SECONDS)
        
        # 1) Before first known: go backwards
        first_i = known_idx[0]
        first_dt = combined[first_i][1].replace(microsecond=0)
        unknown_before = [combined[i][0] for i in range(0, first_i) if combined[i][1] is None]
        
        for back_pos, p in enumerate(reversed(unknown_before), start=1):
            result_map[p] = first_dt - step * back_pos
        
        # 2) Between known anchors: interpolate
        for k in range(len(known_idx) - 1):
            i0 = known_idx[k]
            i1 = known_idx[k + 1]
            t0 = combined[i0][1].replace(microsecond=0)
            t1 = combined[i1][1].replace(microsecond=0)
            
            unknown_between = [
                combined[i][0] for i in range(i0 + 1, i1) if combined[i][1] is None
            ]
            n = len(unknown_between)
            if n == 0:
                continue
            
            if t1 > t0:
                total = (t1 - t0).total_seconds()
                step_sec = max(DEFAULT_ASSIGN_STEP_SECONDS, int(total // (n + 1)))
                for j, p in enumerate(unknown_between, start=1):
                    result_map[p] = t0 + datetime.timedelta(seconds=j * step_sec)
            else:
                for j, p in enumerate(unknown_between, start=1):
                    result_map[p] = t0 + datetime.timedelta(seconds=j * DEFAULT_ASSIGN_STEP_SECONDS)
        
        # 3) After last known: go forward
        last_i = known_idx[-1]
        last_dt = combined[last_i][1].replace(microsecond=0)
        unknown_after = [
            combined[i][0] for i in range(last_i + 1, len(combined)) if combined[i][1] is None
        ]
        for fwd_pos, p in enumerate(unknown_after, start=1):
            result_map[p] = last_dt + step * fwd_pos
        
        # 4) Emit final list in stable order; enforce monotonic
        ordered_fix = sorted(to_fix_paths, key=self._file_order_key)
        out = []
        prev_dt_global = None
        for p in ordered_fix:
            dt = result_map.get(p, fallback_base_date).replace(microsecond=0)
            dt = ensure_gt(prev_dt_global, dt)
            prev_dt_global = dt
            out.append({"path": p, "date": dt})
        return out
    
    def _remove_empty_recursive(self, path: Path) -> None:
        """Рекурсивно удалить пустые папки и файлы 0 байт."""
        if not path.exists():
            return
        
        for _ in range(5):
            deleted = 0
            for root, dirs, files in os.walk(path, topdown=False):
                if self.delete_empty:
                    for name in files:
                        p = Path(root) / name
                        try:
                            if p.exists() and p.stat().st_size == 0:
                                if self.apply:
                                    p.unlink(missing_ok=True)
                                deleted += 1
                        except OSError:
                            pass
                
                for dname in dirs:
                    d = Path(root) / dname
                    try:
                        if d.exists() and not any(d.iterdir()):
                            if self.apply:
                                d.rmdir()
                            deleted += 1
                    except OSError:
                        pass
            
            if deleted == 0:
                break
    
    def _move_outlier(self, anchor: Dict, parent_root: Path) -> None:
        """Переместить файл-выброс в отдельную папку по дате."""
        src = anchor["path"]
        dt = anchor["date"]
        target_name = dt.strftime("%Y-%m-%d")
        target_folder = parent_root / target_name
        target_file = target_folder / src.name
        
        self.log(f"       -> {src.name} в {target_name}", "orange")
        self.logger.debug(f"          [DEBUG_PATH] Src: {repr(str(src))}")
        self.logger.debug(f"          [DEBUG_PATH] Tgt: {repr(str(target_file))}")
        
        if self.apply:
            try:
                target_folder.mkdir(parents=True, exist_ok=True)
                if target_file.exists():
                    self.logger.error("          ОШИБКА: Файл уже существует в цели (пропуск)")
                    self.stats.add_error()
                    return
                shutil.move(str(src), str(target_file))
                self.stats.add_moved()
            except OSError as e:
                self.logger.error(f"          ОШИБКА перемещения: {e}")
                self.stats.add_error()
        else:
            self.logger.debug("          (Тест) Будет перемещен")
    
    def _merge_folders(self, src: Path, dst: Path) -> None:
        """Слить содержимое src в dst."""
        if src == dst:
            return
        
        try:
            moved_count = 0
            for item in list(src.iterdir()):
                self.check_stop()
                target = dst / item.name
                if target.exists():
                    self.logger.error(f"       КОНФЛИКТ: {item.name} уже есть в цели (пропуск)")
                    self.stats.add_merge_conflict()
                    continue
                try:
                    shutil.move(str(item), str(target))
                    moved_count += 1
                except OSError as e:
                    self.logger.error(f"       Ошибка перемещения {item.name}: {e}")
                    self.stats.add_error()
            
            if not any(src.iterdir()):
                if self.apply:
                    src.rmdir()
                self.logger.success(f"     СЛИЯНИЕ УСПЕШНО: '{src.name}' удалена")
                self.stats.add_folder_merged()
            else:
                rem = len(list(src.iterdir()))
                self.logger.warning(f"     СЛИЯНИЕ НЕПОЛНОЕ: Осталось {rem} элементов")
                
        except StopRequested:
            raise
        except Exception as e:
            self.logger.error(f"     CRASH при слиянии: {e}")
            self.stats.add_error()
    
    def _sanitize_filename(self, path: Path) -> Path:
        """
        Удалить emoji и необычные символы из имени файла.
        
        Сохраняет: латиницу, кириллицу, цифры, пробелы, -, _, ()
        """
        original_name = path.name
        stem = path.stem
        suffix = path.suffix
        
        safe_stem = re.sub(r'[^a-zA-Z0-9а-яА-ЯёЁ\-\_\(\)\s]', '', stem)
        safe_stem = re.sub(r'\s+', ' ', safe_stem).strip()
        safe_stem = safe_stem.rstrip(' .')
        
        if not safe_stem:
            safe_stem = "renamed_file"
        
        if safe_stem.upper() in WINDOWS_RESERVED_NAMES:
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
        
        self.stats.add_sanitized()
        
        if self.apply:
            try:
                path.rename(new_path)
                self.logger.warning(f"  [Sanitize] {original_name} -> {new_path.name}")
                return new_path
            except OSError as e:
                self.logger.error(f"  Ошибка переименования {original_name}: {e}")
                return path
        else:
            self.logger.warning(f"  [Sanitize] {original_name} -> {new_path.name} (Dry Run)")
            return path
    
    def _check_time_shift(
        self, anchors: List[Dict], folder_date: datetime.datetime
    ) -> Optional[datetime.timedelta]:
        """Проверить наличие сдвига дат относительно даты папки."""
        if not anchors or not folder_date:
            return None
        
        anchors_valid = [a for a in anchors if a["date"].year >= MIN_VALID_YEAR]
        if not anchors_valid:
            return None
        
        anchors_valid.sort(key=lambda x: x["date"])
        median_date = anchors_valid[len(anchors_valid) // 2]["date"]
        
        diff = median_date - folder_date
        days = diff.days
        
        try:
            years_shift = round(days / 365.25)
            if years_shift == 0:
                return None
            
            new_date_candidate = median_date.replace(year=median_date.year - years_shift)
            diff_remaining = abs((new_date_candidate - folder_date).days)
            
            if diff_remaining < 30:
                return median_date - new_date_candidate
        except ValueError:
            pass
        
        if abs(days) > 300:
            return datetime.timedelta(days=days)
        
        return None
    
    def run(self) -> None:
        """Запустить обработку."""
        self.log(f"=== Запущено: {datetime.datetime.now()} ===")
        
        if self.recursive:
            self.logger.info(f"РЕКУРСИВНЫЙ РЕЖИМ: Сканирование {self.root}...")
            subfolders = sorted([
                f for f in self.root.iterdir() 
                if f.is_dir() and re.match(r'^\d{4}', f.name)
            ])
            
            if self.start_from:
                original_count = len(subfolders)
                subfolders = [f for f in subfolders if f.name >= self.start_from]
                self.logger.info(
                    f"ФИЛЬТР: Начать с '{self.start_from}' -> "
                    f"{len(subfolders)} из {original_count} папок"
                )
            
            self.log(f"Найдено папок событий: {len(subfolders)}\n")
            
            for i, folder in enumerate(subfolders, start=1):
                self.check_stop()
                self.stats.add_folder_processed()
                self.set_status(f"Обработка папки {i}/{len(subfolders)}: {folder.name}")
                self.process_folder(folder, progress_tag=f"[{i}/{len(subfolders)}] ")
        else:
            self.stats.add_folder_processed()
            self.set_status(f"Обработка папки: {self.root.name}")
            self.process_folder(self.root, progress_tag="[1/1] ")
        
        self.log("\n" + "=" * 40)
        self.log("Готово.")
        self.set_status("Готово")
    
    def process_folder(self, folder: Path, progress_tag: str = "") -> int:
        """Обработать одну папку события."""
        folder_date, granularity = self.date_parser.get_folder_date(folder.name)
        if not folder_date:
            self.logger.debug(
                f"ПРОПУСК: '{folder.name}' (Неверное имя или Год < {MIN_VALID_YEAR})"
            )
            return 0
        
        self.logger.info("\n\n" + "=" * 80)
        self.logger.info(f"ПАПКА: {progress_tag}{folder.name}")
        self.logger.info(f"Дата:  {folder_date.strftime('%Y-%m-%d')}")
        self.logger.info("=" * 80)
        
        # Pre-scan
        self.set_status(f"Подсчет файлов в {folder.name}...")
        
        files = []
        for root_path, dirs, filenames in os.walk(str(folder)):
            for name in filenames:
                p = Path(root_path) / name
                
                # Skip ignored extensions BEFORE any processing
                if p.suffix.lower() in IGNORE_EXTENSIONS:
                    continue
                
                if self.sanitize:
                    p = self._sanitize_filename(p)
                
                files.append(p)
        
        if not files:
            self.logger.debug("  [Нет медиа файлов]")
            return 0
        
        total_files = len(files)
        self.stats.add_files_total(total_files)
        self.logger.info(f"  Всего файлов: {total_files} (Запуск ExifTool...)")
        
        # Pre checks: ghost / empty / fake dng
        empty_files = []
        for f in files:
            self.check_stop()
            
            if f.suffix.lower() in IGNORE_EXTENSIONS:
                continue
            
            # AppleDouble / ghost files
            if f.name.startswith("._"):
                try:
                    if f.exists() and f.stat().st_size < 100 * 1024:
                        self.logger.debug(f"  Ghost-файл игнорирован: {f.name}")
                        self.stats.add_ghost_ignored()
                        continue
                except OSError:
                    pass
            
            # Empty files
            try:
                if f.exists() and f.stat().st_size == 0:
                    empty_files.append(f)
                    continue
            except OSError:
                pass
            
            # Fake DNG (JPEG header)
            if f.suffix.lower() == ".dng":
                is_fake = False
                try:
                    with open(f, "rb") as dngf:
                        head = dngf.read(3)
                        if head == b"\xFF\xD8\xFF":
                            is_fake = True
                except OSError as e:
                    self.logger.error(f"    Ошибка проверки DNG: {e}")
                
                if is_fake:
                    new_p = f.with_suffix(".jpg")
                    self.logger.action(f"  ИСПРАВЛЕНИЕ: {f.name} -> .jpg (Fake DNG)")
                    self.stats.add_dng_renamed()
                    if self.apply:
                        try:
                            f.rename(new_p)
                        except OSError as e:
                            self.logger.error(f"    Ошибка переименования: {e}")
                            self.stats.add_error()
                    else:
                        self.logger.debug("    (Тест) Будет переименовано")
        
        if empty_files:
            if self.delete_empty:
                self.logger.warning(
                    f"  Удаление пустых файлов (0 байт): {len(empty_files)} шт."
                )
                for f in empty_files:
                    try:
                        if self.apply:
                            f.unlink(missing_ok=True)
                    except OSError as e:
                        self.logger.error(f"    Ошибка удаления {f.name}: {e}")
                        self.stats.add_error()
            else:
                self.logger.warning(
                    f"  ВНИМАНИЕ: Найдено {len(empty_files)} файлов 0 байт (пропуск)"
                )
        
        # Exif scan
        exif_data = self.exif_handler.get_exif_json(folder)
        if not exif_data:
            self.logger.debug("  [Нет медиа файлов]")
            return 0
        
        self.log(f"  Найдено файлов (ExifTool): {len(exif_data)}")
        
        anchors = []
        filename_dates = []
        to_fix = []
        
        # Build anchors/to_fix
        for item in exif_data:
            self.check_stop()
            try:
                path = Path(item.get("SourceFile", ""))
            except Exception:
                continue
            
            if not path.name:
                continue
            
            if path.suffix.lower() in IGNORE_EXTENSIONS:
                continue
            
            # 1) Filename date has priority
            fdate = self.date_parser.get_date_from_filename(path)
            if fdate:
                raw_exif = item.get("DateTimeOriginal") or item.get("CreateDate")
                existing_dt = parse_exif_date(raw_exif)
                
                is_match = False
                if existing_dt:
                    diff = abs((existing_dt - fdate).total_seconds())
                    if diff < 120:
                        is_match = True
                    else:
                        remainder = diff % 3600
                        if remainder < 120 or remainder > (3600 - 120):
                            is_match = True
                
                if not is_match:
                    self.logger.debug(f"   [DEBUG_DATE] File: {path.name}")
                    self.logger.debug(
                        f"      Filename: {fdate} | Exif: {existing_dt} (Raw: '{raw_exif}')"
                    )
                
                from .constants import FS_ONLY_EXTENSIONS
                if not is_match and not existing_dt and path.suffix.lower() in FS_ONLY_EXTENSIONS:
                    fs_raw = item.get("FileCreateDate")
                    fs_dt = parse_exif_date(fs_raw)
                    if fs_dt and fs_dt == fdate:
                        is_match = True
                
                anchors.append({"path": path, "date": fdate})
                if not is_match:
                    filename_dates.append({"path": path, "date": fdate})
                continue
            
            # 2) Exif date or FS fallback
            raw = (
                item.get("DateTimeOriginal") or 
                item.get("CreateDate") or 
                item.get("MediaCreateDate")
            )
            if not raw:
                raw = item.get("FileCreateDate") or item.get("FileModifyDate")
            
            dt_clean = parse_exif_date(raw)
            if dt_clean:
                is_valid = False
                
                if self.folder_priority:
                    if granularity == "Year":
                        is_valid = (dt_clean.year == folder_date.year)
                    elif granularity == "Month":
                        is_valid = (
                            dt_clean.year == folder_date.year and 
                            dt_clean.month == folder_date.month
                        )
                    else:
                        is_valid = (
                            dt_clean.year == folder_date.year and 
                            dt_clean.month == folder_date.month
                        )
                else:
                    delta = abs((dt_clean - folder_date).days)
                    is_valid = (
                        dt_clean.year >= MIN_VALID_YEAR and 
                        (dt_clean.year == folder_date.year or delta <= 30)
                    )
                
                if is_valid:
                    anchors.append({"path": path, "date": dt_clean})
                else:
                    to_fix.append(path)
            else:
                to_fix.append(path)
        
        if filename_dates:
            self.logger.success(
                f"  -> Файлов с датой в имени (нужно обновить): {len(filename_dates)}"
            )
            self.logger.warning("     (Exif/FS будет перезаписан датой из имени)")
        
        exif_only_count = len(anchors) - len(filename_dates)
        if exif_only_count > 0:
            self.logger.success(f"  -> Файлов с корректной датой (якоря): {exif_only_count}")
        
        if to_fix:
            self.logger.warning(f"  -> Файлов без корректной даты (to_fix): {len(to_fix)}")
        
        # Time Shift Check
        if self.shift and anchors and folder_date:
            shift_delta = self._check_time_shift(anchors, folder_date)
            if shift_delta:
                self.logger.action(
                    f"\n  ОБНАРУЖЕН СДВИГ ДАТ (относительно {folder_date.date()}): "
                    f"{shift_delta.days} дней"
                )
                do_shift = True
                if self.interactive:
                    do_shift = self.ask_yesno(
                        "Сдвиг дат",
                        f"Обнаружено, что файлы смещены на {shift_delta.days} дней.\n"
                        f"Исправить даты у {len(anchors)} файлов?"
                    )
                
                if do_shift:
                    self.logger.action(
                        f"  ДЕЙСТВИЕ: Применение сдвига {shift_delta} к {len(anchors)} файлам..."
                    )
                    shifted_list = []
                    for a in anchors:
                        new_date = a["date"] - shift_delta
                        a["date"] = new_date
                        shifted_list.append(a)
                    
                    if self.apply:
                        errors = self.exif_handler.run_exif_update(shifted_list, scan_root=folder)
                        if errors:
                            self.stats.increment('errors', errors)
                    
                    self.stats.add_fixed_date(len(shifted_list))
                    self.stats.add_fixed_shifted(len(shifted_list))
                    self.logger.success("  Даты скорректированы.")
                else:
                    self.logger.debug("  Сдвиг отменен пользователем.")
        
        # 1) Apply filename-based updates
        if filename_dates:
            self.logger.info(
                f"\n  ДЕЙСТВИЕ: Обновление Exif/FS из имени файла ({len(filename_dates)} шт)..."
            )
            
            for idx, item in enumerate(filename_dates):
                if idx < 15:
                    self.logger.debug(f"    -> {item['path'].name}  (Set: {item['date']})")
                elif idx == 15:
                    self.logger.debug(f"    ... и еще {len(filename_dates) - 15} файлов")
            
            if self.apply:
                errors = self.exif_handler.run_exif_update(filename_dates, scan_root=folder)
                if errors:
                    self.stats.increment('errors', errors)
            
            self.stats.add_fixed_date(len(filename_dates))
            self.stats.add_fixed_filename(len(filename_dates))
        
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
            
            dom_year = sorted(
                year_counts.items(), key=lambda x: (x[1], x[0]), reverse=True
            )[0][0]
            self.logger.info(
                f"  Анализ дат: Доминирующий год = {dom_year} ({year_counts[dom_year]} файлов)"
            )
            
            for a in anchors_sorted:
                y = a["date"].year
                is_outlier = False
                diff = y - dom_year
                
                if abs(diff) > 1:
                    is_outlier = True
                elif abs(diff) == 1:
                    m = a["date"].month
                    if diff == -1:
                        if m < 10:
                            is_outlier = True
                    else:
                        if m > 3:
                            is_outlier = True
                
                if is_outlier:
                    outliers.append(a)
                else:
                    valid_anchors.append(a)
            
            if outliers:
                self.logger.action(
                    f"\n  ДЕЙСТВИЕ: Перенос файлов-выбросов ({len(outliers)} шт)..."
                )
                for o in outliers:
                    self._move_outlier(o, folder.parent)
            
            if valid_anchors:
                valid_anchors.sort(key=lambda x: x["date"])
                ren_base_date = valid_anchors[0]["date"]
            else:
                ren_base_date = folder_date
        
        # 3) Fix to_fix by interpolation
        if to_fix:
            self.logger.info(
                "\n  ДЕЙСТВИЕ: Назначение дат для to_fix (по порядку + интерполяция)..."
            )
            
            anchor_for_assignment = valid_anchors if valid_anchors else anchors
            assigned = self._assign_dates_for_to_fix(
                to_fix_paths=sorted(list(dict.fromkeys(to_fix))),
                anchor_items=anchor_for_assignment,
                fallback_base_date=ren_base_date
            )
            
            assigned_sorted = sorted(assigned, key=lambda x: self._file_order_key(x["path"]))
            for i, it in enumerate(assigned_sorted[:8]):
                self.logger.debug(f"    {it['path'].name} -> {it['date']}")
            if len(assigned_sorted) > 8:
                self.logger.debug(f"    ... и еще {len(assigned_sorted) - 8}")
            
            if self.apply:
                errors = self.exif_handler.run_exif_update(assigned_sorted, scan_root=folder)
                if errors:
                    self.stats.increment('errors', errors)
            
            self.stats.add_fixed_date(len(assigned_sorted))
            self.stats.add_fixed_interpolated(len(assigned_sorted))
        
        # 4) Rename folder
        anchors_for_rename = valid_anchors if valid_anchors else anchors
        if anchors_for_rename:
            anchors_for_rename.sort(key=lambda x: x["date"])
            delta = anchors_for_rename[-1]["date"] - anchors_for_rename[0]["date"]
            if delta.days < SHORT_EVENT_DAYS:
                new_prefix = ren_base_date.strftime("%Y-%m-%d")
            else:
                new_prefix = ren_base_date.strftime("%Y-%m")
        else:
            new_prefix = ren_base_date.strftime("%Y-%m")
        
        clean_name = RE_FOLDER_PREFIX.sub('', folder.name)
        new_name = f"{new_prefix} {clean_name}".strip() if clean_name else new_prefix
        
        effective_path = folder
        if new_name != folder.name:
            self.logger.action("\n  ДЕЙСТВИЕ: Переименование папки")
            self.logger.debug(f"     Старое: '{folder.name}'")
            self.logger.success(f"     Новое:  '{new_name}'")
            
            do_ren = True
            if self.apply and self.interactive:
                user_name = self.ask_string(
                    "Переименование папки",
                    f"Текущее имя: {folder.name}\n"
                    f"Программа предлагает: {new_name}\n\n"
                    f"Введите новое имя (или Cancel для отмены):",
                    new_name
                )
                if user_name:
                    safe_name = re.sub(r'[<>:"/\\|?*]', '_', user_name).strip()
                    if safe_name != user_name:
                        self.logger.warning(
                            f"     Имя скорректировано (удалены спецсимволы): '{safe_name}'"
                        )
                    new_name = safe_name
                    self.logger.info(f"     Пользователь изменил на: '{new_name}'")
                else:
                    do_ren = False
            
            if self.apply and do_ren:
                if new_name == folder.name:
                    self.logger.debug("     Имя совпадает с текущим (пропуск).")
                else:
                    new_path = folder.parent / new_name
                    if new_path.exists():
                        self.logger.warning(f"     ЦЕЛЬ СУЩЕСТВУЕТ: '{new_name}'")
                        self.logger.action("     Запуск СЛИЯНИЯ папок...")
                        self._merge_folders(folder, new_path)
                        effective_path = new_path
                    else:
                        try:
                            folder.rename(new_path)
                            effective_path = new_path
                            self.logger.success("     Переименовано.")
                            self.stats.add_folder_renamed()
                        except OSError as e:
                            self.logger.error(f"     Ошибка: {e}")
                            self.stats.add_error()
            else:
                if not self.apply:
                    self.logger.debug("     (Тест) Переименование не выполняется")
                elif not do_ren:
                    self.logger.warning("     Отменено пользователем.")
        
        # 5) Cleanup empty
        if self.delete_empty:
            self._remove_empty_recursive(effective_path)
        
        return 0

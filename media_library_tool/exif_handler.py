"""
Работа с ExifTool и PowerShell для обновления дат файлов.
"""

import csv
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .constants import FS_ONLY_EXTENSIONS, IGNORE_EXTENSIONS
from .date_parser import format_exif_datetime
from .logger import Logger


class StopRequested(Exception):
    """Исключение для прерывания обработки по запросу пользователя."""
    pass


class ExifHandler:
    """
    Обработчик ExifTool и PowerShell для чтения/записи дат файлов.
    """
    
    def __init__(
        self,
        exif_path: str,
        script_dir: Path,
        logger: Logger,
        check_stop: Callable[[], None],
        apply: bool = False
    ):
        """
        Args:
            exif_path: Путь к exiftool.exe
            script_dir: Директория скрипта для временных файлов
            logger: Логгер
            check_stop: Функция проверки запроса остановки (raises StopRequested)
            apply: Применять изменения (True) или только логировать (False)
        """
        self.exif_path = exif_path
        self.script_dir = script_dir
        self.logger = logger
        self.check_stop = check_stop
        self.apply = apply
    
    def _run_process_interruptible(self, cmd: List[str], **kwargs) -> tuple:
        """
        Запуск subprocess с возможностью прерывания.
        
        Returns:
            (stdout_bytes, stderr_bytes, returncode)
        """
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        kwargs["startupinfo"] = si
        
        with tempfile.TemporaryFile() as out_tmp, tempfile.TemporaryFile() as err_tmp:
            kwargs["stdout"] = out_tmp
            kwargs["stderr"] = err_tmp
            p = subprocess.Popen(cmd, **kwargs)
            
            while p.poll() is None:
                self.check_stop()
                time.sleep(0.1)
            
            out_tmp.seek(0)
            err_tmp.seek(0)
            return out_tmp.read(), err_tmp.read(), p.returncode
    
    def get_exif_json(self, folder: Path) -> List[Dict[str, Any]]:
        """
        Получить EXIF данные для всех файлов в папке.
        
        Args:
            folder: Папка для сканирования
            
        Returns:
            Список словарей с EXIF данными
        """
        # Create argfile for robust path handling
        arg_file_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", delete=False
            ) as f:
                f.write(str(folder))
                arg_file_path = f.name
        except OSError as e:
            self.logger.error(f"Temp file error: {e}")
            return []
        
        cmd = [
            str(self.exif_path),
            "-charset", "filename=utf8",
            "-charset", "utf8",
            "-m",
            "-r",
            "-fast2",
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
                self.logger.warning(f"Предупреждение ExifTool: {stderr.strip()}")
            
            if not stdout.strip():
                return []
            
            data = json.loads(stdout)
            
            # Filter junk extensions
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
            self.logger.error(f"Ошибка выполнения ExifTool: {e}")
            return []
        finally:
            if arg_file_path and os.path.exists(arg_file_path):
                try:
                    os.unlink(arg_file_path)
                except OSError:
                    pass
    
    def update_fs_dates_powershell(self, item_list: List[Dict]) -> int:
        """
        Обновить даты файловой системы через PowerShell.
        
        Args:
            item_list: Список {'path': Path, 'date': datetime}
            
        Returns:
            Количество ошибок
        """
        if not item_list or not self.apply:
            return 0
        
        errors = 0
        
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
        
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8-sig", delete=False, suffix=".ps1", dir=self.script_dir
        ) as f:
            script_path = Path(f.name)
            f.write("\n".join(ps_lines))
        
        try:
            cmd = ["powershell", "-ExecutionPolicy", "Bypass", "-File", str(script_path)]
            out_b, err_b, rc = self._run_process_interruptible(cmd)
            
            if rc != 0:
                err = ""
                if err_b:
                    try:
                        err = err_b.decode("cp866", errors="replace").strip()
                    except Exception:
                        err = err_b.decode("utf-8", errors="replace").strip()
                
                if err:
                    self.logger.error(f"PowerShell ошибка: {err}")
                else:
                    self.logger.error("PowerShell ошибка (код != 0)")
                errors += 1
                
        except StopRequested:
            raise
        except Exception as e:
            self.logger.error(f"PowerShell crash: {e}")
            errors += 1
        finally:
            try:
                script_path.unlink(missing_ok=True)
            except OSError:
                pass
        
        return errors
    
    def run_exif_update(self, item_list: List[Dict], scan_root: Path) -> int:
        """
        Обновить EXIF даты через ExifTool (или FS для неподдерживаемых форматов).
        
        Args:
            item_list: Список {'path': Path, 'date': datetime}
            scan_root: Папка для сканирования ExifTool
            
        Returns:
            Количество ошибок
        """
        if not item_list:
            return 0
        
        errors = 0
        
        # Split by extension type
        exif_list = []
        fs_list = []
        for item in item_list:
            ext = item["path"].suffix.lower()
            if ext in FS_ONLY_EXTENSIONS:
                fs_list.append(item)
            else:
                exif_list.append(item)
        
        # 1) ExifTool import
        if exif_list and self.apply:
            with tempfile.NamedTemporaryFile(
                mode="w", newline="", encoding="utf-8", delete=False, 
                suffix=".csv", dir=self.script_dir
            ) as csvfile:
                csv_path = Path(csvfile.name)
                writer = csv.DictWriter(
                    csvfile, 
                    fieldnames=["SourceFile", "DateTimeOriginal", "CreateDate", "MediaCreateDate"]
                )
                writer.writeheader()
                
                for item in exif_list:
                    self.check_stop()
                    dstr = format_exif_datetime(item["date"])
                    writer.writerow({
                        "SourceFile": str(item["path"].absolute()),
                        "DateTimeOriginal": dstr,
                        "CreateDate": dstr,
                        "MediaCreateDate": dstr,
                    })
            
            # Create argfile for scan_root
            arg_file_path = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w", encoding="utf-8", delete=False
                ) as f:
                    f.write(str(scan_root))
                    arg_file_path = f.name
            except OSError:
                pass
            
            cmd = [
                str(self.exif_path),
                f"-csv={str(csv_path)}",
                "-overwrite_original",
                "-charset", "filename=utf8",
                "-charset", "utf8",
                "-m",
                "-q", "-q",
                "-f",
                "-r",
                "-@", str(arg_file_path) if arg_file_path else str(scan_root)
            ]
            
            try:
                self.logger.info(f"ExifTool: обновление дат ({len(exif_list)} файлов)...")
                out_b, err_b, rc = self._run_process_interruptible(cmd)
                
                out_s = out_b.decode("utf-8", errors="replace") if out_b else ""
                err_s = err_b.decode("utf-8", errors="replace") if err_b else ""
                
                if out_s.strip():
                    self.logger.debug(f"      ExifTool Result: {out_s.strip()}")
                
                failed_files = []
                
                # Filter common benign warnings
                if err_s.strip():
                    lines = [ln.strip() for ln in err_s.splitlines() if ln.strip()]
                    relevant = [ln for ln in lines if "No SourceFile" not in ln]
                    
                    if relevant:
                        self.logger.warning("ExifTool stderr:")
                        for ln in relevant[:30]:
                            self.logger.warning(f"  {ln}")
                            # Detect failed files
                            import re
                            m_err = re.search(r' - ([:A-Za-z]:[\\/].+)$', ln)
                            if m_err:
                                fpath_str = m_err.group(1).strip()
                                try:
                                    failed_files.append(Path(fpath_str))
                                except Exception:
                                    pass
                            
                            # Handle temp file errors
                            m_tmp = re.search(
                                r'Temporary file already exists:\s+(.+)$', ln, re.IGNORECASE
                            )
                            if m_tmp:
                                tmp_file = Path(m_tmp.group(1).strip())
                                try:
                                    self.logger.action(
                                        f"    Обнаружен зависший temp-файл. Удаление: {tmp_file.name}"
                                    )
                                    tmp_file.unlink(missing_ok=True)
                                except OSError as e:
                                    self.logger.error(f"    Не удалось удалить temp-файл: {e}")
                
                if rc != 0:
                    self.logger.error(f"ExifTool завершился с кодом {rc} (частичный сбой)")
                    errors += 1
                else:
                    import re
                    m = re.search(r'(\d+)\s+image files updated', out_s)
                    if m:
                        self.logger.success(f"ExifTool: {m.group(1)} files updated")
                    elif out_s.strip():
                        first = out_s.splitlines()[0].strip()
                        if first:
                            self.logger.debug(f"ExifTool: {first}")
                
                # Fallback: if specific files failed, try FS dates
                if failed_files:
                    self.logger.warning(
                        f"Попытка исправить {len(failed_files)} сбойных файлов через FS..."
                    )
                    lookup = {str(x["path"].absolute()).lower(): x for x in exif_list}
                    fallback_items = []
                    for p in failed_files:
                        key = str(p.absolute()).lower()
                        if key in lookup:
                            fallback_items.append(lookup[key])
                    
                    if fallback_items:
                        errors += self.update_fs_dates_powershell(fallback_items)
                
            except StopRequested:
                raise
            except Exception as e:
                self.logger.error(f"ExifTool crash: {e}")
                errors += 1
            finally:
                try:
                    csv_path.unlink()
                except OSError:
                    pass
                if arg_file_path and os.path.exists(arg_file_path):
                    try:
                        os.unlink(arg_file_path)
                    except OSError:
                        pass
        
        # 2) FS-only import via PowerShell
        if fs_list:
            self.logger.info(f"FS: обновление Creation/LastWrite ({len(fs_list)} файлов)...")
            errors += self.update_fs_dates_powershell(fs_list)
        
        return errors

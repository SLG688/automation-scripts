import os
import shutil
import hashlib
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

class AdvancedFileOrganizer:
    def __init__(self, target_dir: str = None):
        self.target_dir = Path(target_dir) if target_dir else Path.cwd()
        self.lock = threading.Lock()
        self.stats = {
            'moved': 0,
            'copied': 0,
            'deleted': 0,
            'errors': 0
        }
    
    EXTENSION_CATEGORIES = {
        '图片': ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.ico', '.tiff'],
        '文档': ['.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt', '.xls', '.xlsx', '.ppt', '.pptx'],
        '音频': ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a'],
        '视频': ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', '.m4v'],
        '代码': ['.py', '.js', '.html', '.css', '.java', '.cpp', '.c', '.h', '.go', '.rs', '.ts'],
        '压缩': ['.zip', '.rar', '.7z', '.tar', '.gz', '.bz2'],
        '数据': ['.json', '.xml', '.csv', '.sql', '.db', '.sqlite'],
        '电子书': ['.epub', '.mobi', '.azw', '.azw3'],
        '字体': ['.ttf', '.otf', '.woff', '.woff2'],
        '安装包': ['.exe', '.msi', '.dmg', '.pkg', '.deb', '.rpm']
    }
    
    def get_category(self, file_path: Path) -> str:
        ext = file_path.suffix.lower()
        for category, extensions in self.EXTENSION_CATEGORIES.items():
            if ext in extensions:
                return category
        return '其他'
    
    def organize_by_extension(self, target_dir: str = None, 
                            action: str = 'move', 
                            parallel: bool = True) -> Dict:
        target_dir = Path(target_dir) if target_dir else self.target_dir
        
        if parallel:
            return self._organize_parallel(target_dir, action)
        else:
            return self._organize_sequential(target_dir, action)
    
    def _organize_parallel(self, target_dir: Path, action: str) -> Dict:
        files = [f for f in target_dir.iterdir() if f.is_file()]
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for file_path in files:
                category = self.get_category(file_path)
                category_dir = target_dir / category
                category_dir.mkdir(exist_ok=True)
                
                future = executor.submit(
                    self._process_file,
                    file_path,
                    category_dir,
                    action
                )
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result == 'moved':
                        self.stats['moved'] += 1
                    elif result == 'copied':
                        self.stats['copied'] += 1
                except Exception as e:
                    self.stats['errors'] += 1
                    print(f"处理文件时出错: {e}")
        
        return self.stats
    
    def _organize_sequential(self, target_dir: Path, action: str) -> Dict:
        for file_path in target_dir.iterdir():
            if not file_path.is_file():
                continue
            
            category = self.get_category(file_path)
            category_dir = target_dir / category
            category_dir.mkdir(exist_ok=True)
            
            result = self._process_file(file_path, category_dir, action)
            
            with self.lock:
                if result == 'moved':
                    self.stats['moved'] += 1
                elif result == 'copied':
                    self.stats['copied'] += 1
        
        return self.stats
    
    def _process_file(self, file_path: Path, target_dir: Path, action: str) -> str:
        target_path = target_dir / file_path.name
        
        if target_path.exists():
            target_path = self._get_unique_name(target_path)
        
        if action == 'move':
            shutil.move(str(file_path), str(target_path))
            return 'moved'
        elif action == 'copy':
            shutil.copy2(str(file_path), str(target_path))
            return 'copied'
    
    def _get_unique_name(self, file_path: Path) -> Path:
        counter = 1
        while True:
            new_name = f"{file_path.stem}_{counter}{file_path.suffix}"
            new_path = file_path.parent / new_name
            if not new_path.exists():
                return new_path
            counter += 1
    
    def organize_by_date(self, target_dir: str = None, 
                        date_type: str = 'created',
                        action: str = 'move') -> Dict:
        target_dir = Path(target_dir) if target_dir else self.target_dir
        
        for file_path in target_dir.iterdir():
            if not file_path.is_file():
                continue
            
            if date_type == 'created':
                date = datetime.fromtimestamp(file_path.stat().st_ctime)
            elif date_type == 'modified':
                date = datetime.fromtimestamp(file_path.stat().st_mtime)
            else:
                date = datetime.fromtimestamp(file_path.stat().st_ctime)
            
            date_str = date.strftime('%Y-%m')
            date_dir = target_dir / date_str
            date_dir.mkdir(exist_ok=True)
            
            self._process_file(file_path, date_dir, action)
        
        return self.stats
    
    def batch_rename(self, pattern: str, replacement: str, 
                    target_dir: str = None, 
                    recursive: bool = False) -> List[Tuple[str, str]]:
        target_dir = Path(target_dir) if target_dir else self.target_dir
        renamed_files = []
        
        files = target_dir.rglob('*') if recursive else target_dir.iterdir()
        
        for file_path in files:
            if not file_path.is_file():
                continue
            
            old_name = file_path.stem
            new_name = old_name.replace(pattern, replacement)
            
            if new_name != old_name:
                new_path = file_path.parent / f"{new_name}{file_path.suffix}"
                file_path.rename(new_path)
                renamed_files.append((str(file_path), str(new_path)))
        
        return renamed_files
    
    def find_duplicates(self, target_dir: str = None, 
                       by_hash: bool = True) -> Dict[str, List[Path]]:
        target_dir = Path(target_dir) if target_dir else self.target_dir
        duplicates = {}
        
        if by_hash:
            hash_map = {}
            for file_path in target_dir.rglob('*'):
                if not file_path.is_file():
                    continue
                
                file_hash = self._calculate_hash(file_path)
                
                if file_hash in hash_map:
                    if file_hash not in duplicates:
                        duplicates[file_hash] = [hash_map[file_hash]]
                    duplicates[file_hash].append(file_path)
                else:
                    hash_map[file_hash] = file_path
        else:
            size_map = {}
            for file_path in target_dir.rglob('*'):
                if not file_path.is_file():
                    continue
                
                file_size = file_path.stat().st_size
                
                if file_size in size_map:
                    if file_size not in duplicates:
                        duplicates[file_size] = [size_map[file_size]]
                    duplicates[file_size].append(file_path)
                else:
                    size_map[file_size] = file_path
        
        return duplicates
    
    def _calculate_hash(self, file_path: Path, 
                       algorithm: str = 'md5', 
                       chunk_size: int = 8192) -> str:
        hash_func = hashlib.new(algorithm)
        
        with open(file_path, 'rb') as f:
            while chunk := f.read(chunk_size):
                hash_func.update(chunk)
        
        return hash_func.hexdigest()
    
    def backup_files(self, backup_dir: str, 
                    target_dir: str = None, 
                    incremental: bool = False,
                    compress: bool = False) -> Dict:
        target_dir = Path(target_dir) if target_dir else self.target_dir
        backup_dir = Path(backup_dir)
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        backup_info = {
            'timestamp': datetime.now().isoformat(),
            'source': str(target_dir),
            'backup_dir': str(backup_dir),
            'files_backed_up': 0,
            'total_size': 0
        }
        
        manifest_file = backup_dir / 'manifest.json'
        
        if incremental and manifest_file.exists():
            with open(manifest_file, 'r', encoding='utf-8') as f:
                previous_manifest = json.load(f)
        else:
            previous_manifest = {}
        
        current_manifest = {}
        
        for file_path in target_dir.rglob('*'):
            if not file_path.is_file():
                continue
            
            relative_path = file_path.relative_to(target_dir)
            backup_path = backup_dir / relative_path
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_hash = self._calculate_hash(file_path)
            file_size = file_path.stat().st_size
            
            current_manifest[str(relative_path)] = {
                'hash': file_hash,
                'size': file_size,
                'modified': datetime.fromtimestamp(
                    file_path.stat().st_mtime
                ).isoformat()
            }
            
            should_backup = True
            if incremental:
                if str(relative_path) in previous_manifest:
                    if previous_manifest[str(relative_path)]['hash'] == file_hash:
                        should_backup = False
            
            if should_backup:
                if compress:
                    shutil.make_archive(
                        str(backup_path.with_suffix('')),
                        'zip',
                        str(file_path.parent),
                        str(file_path.name)
                    )
                else:
                    shutil.copy2(str(file_path), str(backup_path))
                
                backup_info['files_backed_up'] += 1
                backup_info['total_size'] += file_size
        
        with open(manifest_file, 'w', encoding='utf-8') as f:
            json.dump(current_manifest, f, indent=2, ensure_ascii=False)
        
        return backup_info
    
    def clean_empty_dirs(self, target_dir: str = None) -> List[str]:
        target_dir = Path(target_dir) if target_dir else self.target_dir
        empty_dirs = []
        
        for root, dirs, files in os.walk(str(target_dir), topdown=False):
            for dir_name in dirs:
                dir_path = Path(root) / dir_name
                if not any(dir_path.iterdir()):
                    dir_path.rmdir()
                    empty_dirs.append(str(dir_path))
        
        return empty_dirs
    
    def get_directory_stats(self, target_dir: str = None) -> Dict:
        target_dir = Path(target_dir) if target_dir else self.target_dir
        
        stats = {
            'total_files': 0,
            'total_dirs': 0,
            'total_size': 0,
            'by_extension': {},
            'by_category': {}
        }
        
        for item in target_dir.rglob('*'):
            if item.is_file():
                stats['total_files'] += 1
                file_size = item.stat().st_size
                stats['total_size'] += file_size
                
                ext = item.suffix.lower()
                stats['by_extension'][ext] = stats['by_extension'].get(ext, 0) + 1
                
                category = self.get_category(item)
                stats['by_category'][category] = stats['by_category'].get(category, 0) + 1
            elif item.is_dir():
                stats['total_dirs'] += 1
        
        return stats
    
    def generate_report(self, target_dir: str = None, 
                       output_file: str = 'report.json') -> str:
        target_dir = Path(target_dir) if target_dir else self.target_dir
        output_path = target_dir / output_file
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'directory': str(target_dir),
            'statistics': self.get_directory_stats(target_dir),
            'duplicates': self.find_duplicates(target_dir),
            'empty_dirs': self.clean_empty_dirs(target_dir)
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        return str(output_path)

class SmartFileCleaner:
    def __init__(self, target_dir: str = None):
        self.target_dir = Path(target_dir) if target_dir else Path.cwd()
    
    TEMP_PATTERNS = [
        '*~', '*.tmp', '*.temp', '*.bak', '*.swp', 
        '*.cache', '*.log', '*.old', '*.orig'
    ]
    
    def clean_temp_files(self, target_dir: str = None, 
                        dry_run: bool = True) -> List[str]:
        target_dir = Path(target_dir) if target_dir else self.target_dir
        removed_files = []
        
        for pattern in self.TEMP_PATTERNS:
            for file_path in target_dir.rglob(pattern):
                if file_path.is_file():
                    if dry_run:
                        removed_files.append(str(file_path))
                    else:
                        file_path.unlink()
                        removed_files.append(str(file_path))
        
        return removed_files
    
    def clean_old_files(self, days: int = 30, 
                       target_dir: str = None,
                       dry_run: bool = True) -> List[str]:
        target_dir = Path(target_dir) if target_dir else self.target_dir
        removed_files = []
        cutoff_time = datetime.now().timestamp() - (days * 24 * 60 * 60)
        
        for file_path in target_dir.rglob('*'):
            if file_path.is_file():
                mtime = file_path.stat().st_mtime
                if mtime < cutoff_time:
                    if dry_run:
                        removed_files.append(str(file_path))
                    else:
                        file_path.unlink()
                        removed_files.append(str(file_path))
        
        return removed_files
    
    def clean_large_files(self, size_mb: float = 100, 
                         target_dir: str = None,
                         dry_run: bool = True) -> List[str]:
        target_dir = Path(target_dir) if target_dir else self.target_dir
        removed_files = []
        size_bytes = size_mb * 1024 * 1024
        
        for file_path in target_dir.rglob('*'):
            if file_path.is_file():
                file_size = file_path.stat().st_size
                if file_size > size_bytes:
                    if dry_run:
                        removed_files.append(str(file_path))
                    else:
                        file_path.unlink()
                        removed_files.append(str(file_path))
        
        return removed_files

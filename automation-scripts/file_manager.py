#!/usr/bin/env python3
import os
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import hashlib

class FileOrganizer:
    EXTENSION_CATEGORIES = {
        '图片': ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg'],
        '文档': ['.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt'],
        '表格': ['.xls', '.xlsx', '.csv', '.ods'],
        '演示': ['.ppt', '.pptx', '.odp'],
        '音频': ['.mp3', '.wav', '.flac', '.aac', '.ogg'],
        '视频': ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv'],
        '压缩': ['.zip', '.rar', '.7z', '.tar', '.gz'],
        '代码': ['.py', '.js', '.html', '.css', '.java', '.cpp', '.c', '.h'],
        '安装包': ['.exe', '.msi', '.dmg', '.pkg', '.deb', '.rpm'],
    }
    
    def __init__(self, source_dir: str):
        self.source_dir = Path(source_dir)
        if not self.source_dir.exists():
            raise ValueError(f"源目录不存在: {source_dir}")
    
    def organize_by_extension(self, target_dir: str = None):
        if target_dir is None:
            target_dir = str(self.source_dir / "organized")
        
        target_path = Path(target_dir)
        target_path.mkdir(exist_ok=True)
        
        moved_files = 0
        for file_path in self.source_dir.iterdir():
            if file_path.is_file():
                category = self._get_category(file_path.suffix.lower())
                if category:
                    category_dir = target_path / category
                    category_dir.mkdir(exist_ok=True)
                    
                    dest_path = category_dir / file_path.name
                    shutil.move(str(file_path), str(dest_path))
                    moved_files += 1
                    print(f"移动: {file_path.name} -> {category}/")
        
        print(f"\n完成！共移动 {moved_files} 个文件")
        return moved_files
    
    def organize_by_date(self, target_dir: str = None):
        if target_dir is None:
            target_dir = str(self.source_dir / "by_date")
        
        target_path = Path(target_dir)
        target_path.mkdir(exist_ok=True)
        
        moved_files = 0
        for file_path in self.source_dir.iterdir():
            if file_path.is_file():
                mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                date_dir = target_path / mod_time.strftime("%Y-%m")
                date_dir.mkdir(exist_ok=True)
                
                dest_path = date_dir / file_path.name
                shutil.move(str(file_path), str(dest_path))
                moved_files += 1
                print(f"移动: {file_path.name} -> {mod_time.strftime('%Y-%m')}/")
        
        print(f"\n完成！共移动 {moved_files} 个文件")
        return moved_files
    
    def batch_rename(self, pattern: str, replacement: str):
        renamed_files = 0
        for file_path in self.source_dir.iterdir():
            if file_path.is_file():
                old_name = file_path.stem
                new_name = old_name.replace(pattern, replacement)
                
                if old_name != new_name:
                    new_path = file_path.with_name(new_name + file_path.suffix)
                    file_path.rename(new_path)
                    renamed_files += 1
                    print(f"重命名: {old_name} -> {new_name}")
        
        print(f"\n完成！共重命名 {renamed_files} 个文件")
        return renamed_files
    
    def find_duplicates(self) -> Dict[str, List[Path]]:
        file_hashes = {}
        duplicates = {}
        
        for file_path in self.source_dir.rglob('*'):
            if file_path.is_file():
                file_hash = self._calculate_hash(file_path)
                
                if file_hash in file_hashes:
                    if file_hash not in duplicates:
                        duplicates[file_hash] = [file_hashes[file_hash]]
                    duplicates[file_hash].append(file_path)
                    print(f"发现重复: {file_path.name}")
                else:
                    file_hashes[file_hash] = file_path
        
        if duplicates:
            print(f"\n发现 {len(duplicates)} 组重复文件")
        else:
            print("\n未发现重复文件")
        
        return duplicates
    
    def backup_files(self, backup_dir: str):
        backup_path = Path(backup_dir)
        backup_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_folder = backup_path / f"backup_{timestamp}"
        backup_folder.mkdir(exist_ok=True)
        
        backed_up = 0
        for file_path in self.source_dir.iterdir():
            if file_path.is_file():
                dest_path = backup_folder / file_path.name
                shutil.copy2(str(file_path), str(dest_path))
                backed_up += 1
                print(f"备份: {file_path.name}")
        
        print(f"\n完成！共备份 {backed_up} 个文件到 {backup_folder}")
        return backed_up
    
    def _get_category(self, extension: str) -> str:
        for category, extensions in self.EXTENSION_CATEGORIES.items():
            if extension in extensions:
                return category
        return '其他'
    
    def _calculate_hash(self, file_path: Path, chunk_size: int = 8192) -> str:
        hash_obj = hashlib.md5()
        with open(file_path, 'rb') as f:
            while chunk := f.read(chunk_size):
                hash_obj.update(chunk)
        return hash_obj.hexdigest()

def main():
    print("=" * 60)
    print("文件管理工具")
    print("=" * 60)
    
    source_dir = input("请输入源目录路径: ").strip()
    if not source_dir:
        source_dir = "."
    
    try:
        organizer = FileOrganizer(source_dir)
        
        print("\n选择操作:")
        print("1. 按文件类型整理")
        print("2. 按日期整理")
        print("3. 批量重命名")
        print("4. 查找重复文件")
        print("5. 备份文件")
        print("0. 退出")
        
        choice = input("\n请输入选项: ").strip()
        
        if choice == "1":
            target_dir = input("目标目录 (默认: ./organized): ").strip() or "./organized"
            organizer.organize_by_extension(target_dir)
        
        elif choice == "2":
            target_dir = input("目标目录 (默认: ./by_date): ").strip() or "./by_date"
            organizer.organize_by_date(target_dir)
        
        elif choice == "3":
            pattern = input("要替换的文本: ").strip()
            replacement = input("替换为: ").strip()
            if pattern:
                organizer.batch_rename(pattern, replacement)
        
        elif choice == "4":
            duplicates = organizer.find_duplicates()
            if duplicates:
                remove = input("\n是否删除重复文件? (y/n): ").strip().lower()
                if remove == 'y':
                    for file_hash, files in duplicates.items():
                        for file_path in files[1:]:
                            file_path.unlink()
                            print(f"删除: {file_path.name}")
        
        elif choice == "5":
            backup_dir = input("备份目录路径: ").strip()
            if backup_dir:
                organizer.backup_files(backup_dir)
        
        elif choice == "0":
            print("退出程序")
        
        else:
            print("无效选项")
    
    except Exception as e:
        print(f"错误: {e}")

if __name__ == "__main__":
    main()

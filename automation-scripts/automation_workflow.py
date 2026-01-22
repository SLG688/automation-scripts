import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import schedule
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import json
from pathlib import Path
import requests
from abc import ABC, abstractmethod

class NotificationChannel(ABC):
    @abstractmethod
    def send(self, message: str, **kwargs) -> bool:
        pass

class EmailNotification(NotificationChannel):
    def __init__(self, smtp_server: str, smtp_port: int, 
                 username: str, password: str):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
    
    def send(self, message: str, to_email: str, 
              subject: str = "通知", **kwargs) -> bool:
        try:
            msg = MIMEMultipart()
            msg['From'] = self.username
            msg['To'] = to_email
            msg['Subject'] = subject
            
            msg.attach(MIMEText(message, 'plain', 'utf-8'))
            
            if 'attachments' in kwargs:
                for attachment in kwargs['attachments']:
                    self._attach_file(msg, attachment)
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)
            
            return True
        except Exception as e:
            print(f"发送邮件失败: {e}")
            return False
    
    def _attach_file(self, msg: MIMEMultipart, file_path: str):
        with open(file_path, 'rb') as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
        
        encoders.encode_base64(part)
        part.add_header(
            'Content-Disposition',
            f'attachment; filename= {Path(file_path).name}'
        )
        msg.attach(part)

class SlackNotification(NotificationChannel):
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
    
    def send(self, message: str, **kwargs) -> bool:
        try:
            payload = {
                'text': message,
                'username': kwargs.get('username', 'Bot'),
                'icon_emoji': kwargs.get('icon_emoji', ':robot_face:')
            }
            
            if 'channel' in kwargs:
                payload['channel'] = kwargs['channel']
            
            response = requests.post(self.webhook_url, json=payload)
            response.raise_for_status()
            
            return True
        except Exception as e:
            print(f"发送Slack通知失败: {e}")
            return False

class TelegramNotification(NotificationChannel):
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{bot_token}"
    
    def send(self, message: str, **kwargs) -> bool:
        try:
            url = f"{self.api_url}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': kwargs.get('parse_mode', 'HTML')
            }
            
            response = requests.post(url, json=payload)
            response.raise_for_status()
            
            return True
        except Exception as e:
            print(f"发送Telegram通知失败: {e}")
            return False

class NotificationManager:
    def __init__(self):
        self.channels: List[NotificationChannel] = []
    
    def add_channel(self, channel: NotificationChannel):
        self.channels.append(channel)
    
    def remove_channel(self, channel: NotificationChannel):
        if channel in self.channels:
            self.channels.remove(channel)
    
    def send_to_all(self, message: str, **kwargs):
        results = []
        for channel in self.channels:
            result = channel.send(message, **kwargs)
            results.append(result)
        return all(results)
    
    def send_to_channel(self, channel_index: int, message: str, **kwargs):
        if 0 <= channel_index < len(self.channels):
            return self.channels[channel_index].send(message, **kwargs)
        return False

class TaskScheduler:
    def __init__(self):
        self.tasks = []
        self.running = False
    
    def add_task(self, func, interval: int, unit: str = 'minutes'):
        task = {
            'func': func,
            'interval': interval,
            'unit': unit,
            'last_run': None
        }
        self.tasks.append(task)
    
    def add_daily_task(self, func, time_str: str):
        task = {
            'func': func,
            'type': 'daily',
            'time': time_str,
            'last_run': None
        }
        self.tasks.append(task)
    
    def add_weekly_task(self, func, day: str, time_str: str):
        task = {
            'func': func,
            'type': 'weekly',
            'day': day,
            'time': time_str,
            'last_run': None
        }
        self.tasks.append(task)
    
    def start(self):
        self.running = True
        
        for task in self.tasks:
            if 'interval' in task:
                interval = task['interval']
                unit = task['unit']
                
                if unit == 'seconds':
                    schedule.every(interval).seconds.do(task['func'])
                elif unit == 'minutes':
                    schedule.every(interval).minutes.do(task['func'])
                elif unit == 'hours':
                    schedule.every(interval).hours.do(task['func'])
                elif unit == 'days':
                    schedule.every(interval).days.do(task['func'])
            elif task['type'] == 'daily':
                schedule.every().day.at(task['time']).do(task['func'])
            elif task['type'] == 'weekly':
                getattr(schedule.every(), task['day']).at(task['time']).do(task['func'])
        
        while self.running:
            schedule.run_pending()
            time.sleep(1)
    
    def stop(self):
        self.running = False
        schedule.clear()

class AutomationWorkflow:
    def __init__(self, name: str):
        self.name = name
        self.steps = []
        self.variables = {}
    
    def add_step(self, step_func: callable, description: str = ""):
        self.steps.append({
            'func': step_func,
            'description': description
        })
    
    def set_variable(self, name: str, value: any):
        self.variables[name] = value
    
    def get_variable(self, name: str, default=None):
        return self.variables.get(name, default)
    
    def execute(self) -> Dict:
        results = {
            'workflow': self.name,
            'start_time': datetime.now().isoformat(),
            'steps': [],
            'success': True,
            'error': None
        }
        
        try:
            for i, step in enumerate(self.steps, 1):
                step_result = {
                    'step': i,
                    'description': step['description'],
                    'start_time': datetime.now().isoformat(),
                    'success': True,
                    'error': None
                }
                
                try:
                    result = step['func']()
                    step_result['result'] = str(result)
                except Exception as e:
                    step_result['success'] = False
                    step_result['error'] = str(e)
                    results['success'] = False
                    results['error'] = str(e)
                    break
                
                step_result['end_time'] = datetime.now().isoformat()
                results['steps'].append(step_result)
            
        except Exception as e:
            results['success'] = False
            results['error'] = str(e)
        
        results['end_time'] = datetime.now().isoformat()
        return results
    
    def save_results(self, results: Dict, filename: str):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

class DataPipeline:
    def __init__(self, name: str):
        self.name = name
        self.stages = []
    
    def add_stage(self, stage_func: callable, name: str):
        self.stages.append({
            'func': stage_func,
            'name': name
        })
    
    def execute(self, data: any) -> any:
        current_data = data
        
        for stage in self.stages:
            print(f"执行阶段: {stage['name']}")
            try:
                current_data = stage['func'](current_data)
            except Exception as e:
                print(f"阶段 {stage['name']} 执行失败: {e}")
                raise
        
        return current_data
    
    def execute_async(self, data: any) -> any:
        import asyncio
        
        async def async_execute():
            current_data = data
            
            for stage in self.stages:
                print(f"执行阶段: {stage['name']}")
                try:
                    if asyncio.iscoroutinefunction(stage['func']):
                        current_data = await stage['func'](current_data)
                    else:
                        current_data = stage['func'](current_data)
                except Exception as e:
                    print(f"阶段 {stage['name']} 执行失败: {e}")
                    raise
            
            return current_data
        
        return asyncio.run(async_execute())

class MonitoringSystem:
    def __init__(self):
        self.monitors = []
        self.alerts = []
    
    def add_monitor(self, check_func: callable, 
                   alert_func: callable, 
                   interval: int = 60):
        monitor = {
            'check_func': check_func,
            'alert_func': alert_func,
            'interval': interval,
            'last_check': None
        }
        self.monitors.append(monitor)
    
    def check_all(self):
        for monitor in self.monitors:
            try:
                result = monitor['check_func']()
                
                if not result:
                    monitor['alert_func']()
                    self.alerts.append({
                        'timestamp': datetime.now().isoformat(),
                        'message': f"监控检查失败"
                    })
                
                monitor['last_check'] = datetime.now()
            except Exception as e:
                print(f"监控检查出错: {e}")
    
    def start_monitoring(self):
        import schedule
        
        for monitor in self.monitors:
            schedule.every(monitor['interval']).seconds.do(
                self._check_monitor, monitor
            )
        
        while True:
            schedule.run_pending()
            time.sleep(1)
    
    def _check_monitor(self, monitor: Dict):
        try:
            result = monitor['check_func']()
            
            if not result:
                monitor['alert_func']()
                self.alerts.append({
                    'timestamp': datetime.now().isoformat(),
                    'message': f"监控检查失败"
                })
            
            monitor['last_check'] = datetime.now()
        except Exception as e:
            print(f"监控检查出错: {e}")
    
    def get_alerts(self, hours: int = 24) -> List[Dict]:
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [
            alert for alert in self.alerts
            if datetime.fromisoformat(alert['timestamp']) > cutoff_time
        ]

class BackupAutomation:
    def __init__(self, source_dir: str, backup_dir: str):
        self.source_dir = Path(source_dir)
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
    
    def create_backup(self, compress: bool = True) -> str:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = f"backup_{timestamp}"
        
        if compress:
            import shutil
            backup_path = self.backup_dir / f"{backup_name}.zip"
            shutil.make_archive(
                str(backup_path.with_suffix('')),
                'zip',
                str(self.source_dir)
            )
        else:
            backup_path = self.backup_dir / backup_name
            import shutil
            shutil.copytree(str(self.source_dir), str(backup_path))
        
        return str(backup_path)
    
    def restore_backup(self, backup_path: str, 
                      target_dir: str = None) -> bool:
        try:
            import shutil
            target = Path(target_dir) if target_dir else self.source_dir
            
            if backup_path.endswith('.zip'):
                shutil.unpack_archive(backup_path, str(target))
            else:
                shutil.copytree(backup_path, str(target))
            
            return True
        except Exception as e:
            print(f"恢复备份失败: {e}")
            return False
    
    def cleanup_old_backups(self, keep_days: int = 7) -> List[str]:
        cutoff_time = datetime.now() - timedelta(days=keep_days)
        removed = []
        
        for backup in self.backup_dir.iterdir():
            if backup.is_file():
                mtime = datetime.fromtimestamp(backup.stat().st_mtime)
                if mtime < cutoff_time:
                    backup.unlink()
                    removed.append(str(backup))
        
        return removed

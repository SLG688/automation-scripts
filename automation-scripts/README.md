# 🤖 Automation Scripts - 自动化脚本集合

一个功能强大的自动化脚本集合，涵盖文件管理、网络爬虫、任务调度、通知系统等多个领域，帮助提高工作效率，减少重复劳动。

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)
![Automation](https://img.shields.io/badge/Automation-Essential-orange.svg)
![Scripts](https://img.shields.io/badge/Scripts-50+-brightgreen.svg)

## ✨ 核心功能

### 📁 文件管理自动化
- **智能文件整理** - 按类型、日期自动分类文件
- **批量重命名** - 支持正则表达式批量重命名
- **重复文件检测** - 基于哈希值或文件大小查找重复
- **自动备份** - 增量备份、压缩备份
- **目录清理** - 清理空目录、临时文件
- **文件统计** - 生成详细的文件统计报告

### 🌐 网络爬虫
- **通用网页爬虫** - 支持多线程爬取
- **数据提取** - 提取文本、图片、表格、表单
- **API客户端** - RESTful API调用封装
- **社交媒体爬虫** - Twitter、LinkedIn等平台
- **数据清洗** - 提取邮箱、电话、URL等
- **数据导出** - 支持JSON、CSV格式

### ⏰ 任务调度
- **定时任务** - 支持秒、分、时、天、周
- **任务队列** - 异步任务执行
- **工作流编排** - 多步骤自动化流程
- **数据管道** - 数据处理流水线
- **监控系统** - 实时监控和告警

### 📧 通知系统
- **邮件通知** - SMTP邮件发送
- **Slack通知** - Webhook集成
- **Telegram通知** - Bot API集成
- **多渠道通知** - 同时发送到多个渠道
- **通知模板** - 自定义通知格式

## 🏗️ 系统架构

```
┌─────────────────────────────────────────────────────────┐
│                   用户接口层 (UI)                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ CLI界面  │  │ Web界面  │  │ API接口  │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘   │
└───────┼──────────────┼──────────────┼────────────┘
        │              │              │
┌───────┼──────────────┼──────────────┼────────────┐
│       │              │              │            │
│  ┌───▼──────────────▼──────────────▼────┐     │
│  │        自动化引擎层 (Engine)        │     │
│  │  ┌──────────────────────────────┐   │     │
│  │  │    TaskScheduler          │   │     │
│  │  │  - 定时任务调度            │   │     │
│  │  │  - 任务队列管理            │   │     │
│  │  └──────────────────────────────┘   │     │
│  │  ┌──────────────────────────────┐   │     │
│  │  │    AutomationWorkflow     │   │     │
│  │  │  - 工作流编排              │   │     │
│  │  │  - 变量管理                │   │     │
│  │  └──────────────────────────────┘   │     │
│  │  ┌──────────────────────────────┐   │     │
│  │  │    NotificationManager    │   │     │
│  │  │  - 多渠道通知              │   │     │
│  │  │  - 通知模板                │   │     │
│  │  └──────────────────────────────┘   │     │
│  └────────────────────────────────────┘     │
│                                          │
│  ┌────────────────────────────────────┐     │
│  │        功能模块层 (Modules)       │     │
│  │  ┌────────────────────────────┐  │     │
│  │  │  FileManager            │  │     │
│  │  │  - 文件整理              │  │     │
│  │  │  - 批量重命名            │  │     │
│  │  │  - 重复文件检测          │  │     │
│  │  └────────────────────────────┘  │     │
│  │  ┌────────────────────────────┐  │     │
│  │  │  WebScraper              │  │     │
│  │  │  - 网页爬取              │  │     │
│  │  │  - 数据提取              │  │     │
│  │  │  - API调用               │  │     │
│  │  └────────────────────────────┘  │     │
│  │  ┌────────────────────────────┐  │     │
│  │  │  MonitoringSystem        │  │     │
│  │  │  - 系统监控              │  │     │
│  │  │  - 告警通知              │  │     │
│  │  │  - 日志记录              │  │     │
│  │  └────────────────────────────┘  │     │
│  └────────────────────────────────────┘     │
└────────────────────────────────────────────┘
```

## 🚀 快速开始

### 安装依赖

```bash
pip install -r requirements.txt
```

### 运行脚本

```bash
# 文件管理
python file_manager.py

# 网络爬虫
python web_scraper.py

# 自动化工作流
python automation_workflow.py

# 运行测试
python -m pytest test_automation.py -v
```

## 📖 详细使用指南

### 1. 智能文件整理

```python
from advanced_file_manager import AdvancedFileOrganizer

# 创建文件整理器
organizer = AdvancedFileOrganizer("C:/Downloads")

# 按文件类型整理
stats = organizer.organize_by_extension(
    action='move',  # move 或 copy
    parallel=True   # 并行处理
)
print(f"移动了 {stats['moved']} 个文件")

# 按日期整理
stats = organizer.organize_by_date(
    date_type='created',  # created 或 modified
    action='move'
)

# 批量重命名
renamed = organizer.batch_rename(
    pattern='old',
    replacement='new',
    recursive=True
)

# 查找重复文件
duplicates = organizer.find_duplicates(by_hash=True)
for hash_val, files in duplicates.items():
    print(f"重复文件组: {len(files)} 个文件")

# 创建备份
backup_info = organizer.backup_files(
    backup_dir='C:/Backups',
    incremental=True,
    compress=True
)
```

### 2. 网络爬虫

```python
from web_scraper import WebScraper, AdvancedWebScraper, DataExtractor

# 基础爬虫
scraper = WebScraper('https://example.com', delay=1.0)

# 爬取网页
data = scraper.crawl(
    max_pages=10,
    selectors=['title', 'h1', 'p']
)

# 保存数据
scraper.save_to_json('output.json')
scraper.save_to_csv('output.csv')

# 高级爬虫
advanced_scraper = AdvancedWebScraper('https://example.com')

# 并行爬取
data = advanced_scraper.crawl_parallel(
    max_pages=20,
    max_workers=4
)

# 数据提取
extractor = DataExtractor()
text = "联系我们: contact@example.com 或 +86-138-0000-0000"

emails = extractor.extract_emails(text)
phones = extractor.extract_phone_numbers(text)
urls = extractor.extract_urls(text)

print(f"邮箱: {emails}")
print(f"电话: {phones}")
print(f"URL: {urls}")
```

### 3. 任务调度

```python
from automation_workflow import TaskScheduler

# 创建任务调度器
scheduler = TaskScheduler()

# 添加定时任务
def send_daily_report():
    print("发送日报")

scheduler.add_daily_task(send_daily_report, "09:00")

# 添加周期性任务
def cleanup_temp_files():
    print("清理临时文件")

scheduler.add_task(cleanup_temp_files, interval=1, unit='days')

# 启动调度器
scheduler.start()
```

### 4. 通知系统

```python
from automation_workflow import (
    EmailNotification, 
    SlackNotification, 
    TelegramNotification,
    NotificationManager
)

# 创建通知管理器
manager = NotificationManager()

# 添加邮件通知
email_notifier = EmailNotification(
    smtp_server='smtp.gmail.com',
    smtp_port=587,
    username='your_email@gmail.com',
    password='your_password'
)
manager.add_channel(email_notifier)

# 添加Slack通知
slack_notifier = SlackNotification(
    webhook_url='https://hooks.slack.com/services/...'
)
manager.add_channel(slack_notifier)

# 发送通知到所有渠道
manager.send_to_all(
    "任务已完成！",
    subject="自动化通知"
)
```

### 5. 自动化工作流

```python
from automation_workflow import AutomationWorkflow

# 创建工作流
workflow = AutomationWorkflow("数据处理流程")

# 添加步骤
def step1_fetch_data():
    print("步骤1: 获取数据")
    return {"data": [1, 2, 3, 4, 5]}

def step2_process_data(data):
    print("步骤2: 处理数据")
    return {"processed": [x * 2 for x in data['data']]}

def step3_save_data(data):
    print("步骤3: 保存数据")
    with open('output.json', 'w') as f:
        json.dump(data, f)

workflow.add_step(step1_fetch_data, "获取数据")
workflow.add_step(step2_process_data, "处理数据")
workflow.add_step(step3_save_data, "保存数据")

# 执行工作流
results = workflow.execute()
print(results)

# 保存结果
workflow.save_results(results, 'workflow_results.json')
```

### 6. 监控系统

```python
from automation_workflow import MonitoringSystem, EmailNotification

# 创建监控系统
monitor = MonitoringSystem()

# 添加监控
def check_disk_space():
    import shutil
    total, used, free = shutil.disk_usage('/')
    return free > 1024 * 1024 * 1024  # 至少1GB

def send_alert():
    email_notifier = EmailNotification(...)
    email_notifier.send(
        "磁盘空间不足！",
        to_email="admin@example.com",
        subject="系统告警"
    )

monitor.add_monitor(check_disk_space, send_alert, interval=60)

# 启动监控
monitor.start_monitoring()
```

## 🧪 测试

项目包含完整的单元测试：

```bash
# 运行所有测试
python -m pytest test_automation.py -v

# 运行特定测试
python -m pytest test_automation.py::TestFileManager -v

# 查看测试覆盖率
python -m pytest test_automation.py --cov=. --cov-report=html
```

测试覆盖：
- ✅ 文件管理功能
- ✅ 网络爬虫功能
- ✅ 任务调度功能
- ✅ 通知系统功能
- ✅ 工作流编排
- ✅ 监控系统

## 📊 实际应用场景

### 1. 自动化备份系统

```python
from automation_workflow import BackupAutomation, TaskScheduler, NotificationManager

# 创建备份任务
backup = BackupAutomation(
    source_dir='C:/Projects',
    backup_dir='C:/Backups'
)

def daily_backup():
    backup_path = backup.create_backup(compress=True)
    
    # 发送通知
    manager = NotificationManager()
    manager.add_channel(EmailNotification(...))
    manager.send_to_all(
        f"备份完成: {backup_path}",
        subject="每日备份"
    )

# 设置定时任务
scheduler = TaskScheduler()
scheduler.add_daily_task(daily_backup, "02:00")
scheduler.start()
```

### 2. 网站监控

```python
from web_scraper import WebScraper
from automation_workflow import MonitoringSystem

scraper = WebScraper('https://example.com')

def check_website():
    soup = scraper.fetch_page(scraper.base_url)
    return soup is not None and soup.title

def send_alert():
    print("网站无法访问！")

monitor = MonitoringSystem()
monitor.add_monitor(check_website, send_alert, interval=300)
monitor.start_monitoring()
```

### 3. 数据收集和处理

```python
from web_scraper import AdvancedWebScraper
from automation_workflow import DataPipeline

# 爬取数据
scraper = AdvancedWebScraper('https://example.com')
data = scraper.crawl_parallel(max_pages=50)

# 创建数据处理管道
pipeline = DataPipeline("数据处理")

def clean_data(data):
    cleaned = []
    for item in data:
        cleaned.append({
            'title': item.get('title', ''),
            'url': item.get('url', '')
        })
    return cleaned

def transform_data(data):
    transformed = []
    for item in data:
        transformed.append({
            'title': item['title'].upper(),
            'url_length': len(item['url'])
        })
    return transformed

def save_data(data):
    import json
    with open('processed_data.json', 'w') as f:
        json.dump(data, f)

pipeline.add_stage(clean_data, "数据清洗")
pipeline.add_stage(transform_data, "数据转换")
pipeline.add_stage(save_data, "数据保存")

# 执行管道
result = pipeline.execute(data)
```

## 🔧 配置选项

```python
# config.py
class Config:
    # 文件管理配置
    FILE_CATEGORIES = {
        '图片': ['.jpg', '.png', '.gif'],
        '文档': ['.pdf', '.doc', '.docx'],
        '音频': ['.mp3', '.wav'],
        '视频': ['.mp4', '.avi']
    }
    
    # 网络爬虫配置
    DEFAULT_DELAY = 1.0  # 请求延迟（秒）
    MAX_RETRIES = 3  # 最大重试次数
    TIMEOUT = 10  # 超时时间（秒）
    
    # 任务调度配置
    MAX_WORKERS = 4  # 最大工作线程数
    TASK_TIMEOUT = 3600  # 任务超时时间（秒）
    
    # 通知配置
    EMAIL_SMTP_SERVER = 'smtp.gmail.com'
    EMAIL_SMTP_PORT = 587
    
    # 备份配置
    BACKUP_COMPRESSION = True
    BACKUP_RETENTION_DAYS = 7
    
    # 监控配置
    MONITOR_INTERVAL = 60  # 监控间隔（秒）
    ALERT_COOLDOWN = 300  # 告警冷却时间（秒）
```

## 📁 项目结构

```
automation-scripts/
├── core/
│   ├── file_manager.py         # 文件管理
│   ├── web_scraper.py          # 网络爬虫
│   ├── automation_workflow.py   # 自动化工作流
│   └── notification.py         # 通知系统
├── utils/
│   ├── data_extractor.py       # 数据提取工具
│   ├── api_client.py          # API客户端
│   └── validators.py          # 数据验证
├── tests/
│   ├── test_file_manager.py    # 文件管理测试
│   ├── test_web_scraper.py     # 网络爬虫测试
│   └── test_automation.py      # 自动化测试
├── examples/
│   ├── backup_system.py        # 备份系统示例
│   ├── website_monitor.py      # 网站监控示例
│   └── data_pipeline.py        # 数据管道示例
├── config.py                   # 配置文件
├── main.py                     # 主程序入口
└── requirements.txt             # 依赖列表
```

## 🎓 技术亮点

### 1. 并发处理

- **多线程** - 提高文件处理效率
- **异步IO** - 网络请求优化
- **线程池** - 资源管理
- **锁机制** - 线程安全

### 2. 错误处理

- **重试机制** - 网络请求失败自动重试
- **异常捕获** - 完善的错误处理
- **日志记录** - 详细的执行日志
- **告警通知** - 异常情况及时通知

### 3. 扩展性

- **插件架构** - 易于扩展新功能
- **配置驱动** - 灵活的配置管理
- **模块化设计** - 独立的功能模块
- **接口抽象** - 统一的接口规范

## 🔮 未来计划

- [ ] Web管理界面
- [ ] 更多文件格式支持
- [ ] 云存储集成
- [ ] 机器学习集成
- [ ] 可视化监控面板
- [ ] 移动端通知
- [ ] 更多爬虫模板
- [ ] 分布式任务调度
- [ ] 工作流可视化编辑器

## 🤝 贡献指南

欢迎贡献代码、报告问题或提出建议！

1. Fork本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启Pull Request

## 📄 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件

## 🙏 致谢

感谢以下开源项目：
- [Python](https://www.python.org/)
- [Requests](https://requests.readthedocs.io/)
- [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/)
- [Schedule](https://schedule.readthedocs.io/)

## 📞 联系方式

- 项目主页: https://github.com/yourusername/automation-scripts
- 问题反馈: https://github.com/yourusername/automation-scripts/issues
- 邮箱: your.email@example.com

---

⭐ 如果这个项目对你有帮助，请给个Star！

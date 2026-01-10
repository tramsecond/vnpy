#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 数据更新配置文件

# 更新频率设置
UPDATE_FREQUENCY = {
    'daily': True,        # 每日更新
    'weekly': False,      # 每周更新
    'monthly': False      # 每月更新
}

# 批量处理设置
BATCH_SETTINGS = {
    'stock_batch_size': 20,      # 股票批次大小
    'index_batch_size': 5,       # 指数批次大小
    'stock_rest_time': 1,        # 股票间休息时间(秒)
    'index_rest_time': 2,        # 指数间休息时间(秒)
    'batch_rest_time': 5         # 批次间休息时间(秒)
}

# 数据源设置
DATA_SOURCE_SETTINGS = {
    'use_akshare': True,         # 使用akshare数据源
    'use_yfinance': False,       # 使用yfinance数据源
    'max_retries': 3,            # 最大重试次数
    'retry_delay': 2,            # 重试延迟(秒)
    'timeout_seconds': 30        # 超时时间(秒)
}

# 更新策略设置
UPDATE_STRATEGY = {
    'auto_backup': True,         # 自动备份
    'force_full_update': False,  # 强制完整更新
    'skip_existing': True,       # 跳过已存在的数据
    'update_indicators': True,   # 更新技术指标
    'regenerate_views': True     # 重新生成周线/月线视图
}

# 交易日判断设置
TRADING_DAY_SETTINGS = {
    'skip_weekends': True,       # 跳过周末
    'skip_holidays': False,      # 跳过节假日(需要实现)
    'trading_hours': {           # 交易时间
        'start': '09:30',
        'end': '15:00'
    }
}

# 日志设置
LOGGING_SETTINGS = {
    'log_level': 'INFO',         # 日志级别
    'log_to_file': True,         # 记录到文件
    'log_file': 'update_log.txt', # 日志文件名
    'max_log_size': 10 * 1024 * 1024,  # 最大日志文件大小(10MB)
    'backup_count': 5            # 备份文件数量
}

# 通知设置
NOTIFICATION_SETTINGS = {
    'enable_email': False,       # 启用邮件通知
    'enable_webhook': False,     # 启用webhook通知
    'email_settings': {
        'smtp_server': 'smtp.gmail.com',
        'smtp_port': 587,
        'username': '',
        'password': '',
        'to_email': ''
    },
    'webhook_url': ''
}

# 性能优化设置
PERFORMANCE_SETTINGS = {
    'use_multiprocessing': False,  # 使用多进程
    'max_workers': 4,              # 最大工作进程数
    'chunk_size': 1000,            # 数据分块大小
    'memory_limit': '2GB'          # 内存限制
}

# 错误处理设置
ERROR_HANDLING_SETTINGS = {
    'continue_on_error': True,     # 出错时继续处理
    'max_errors': 10,              # 最大错误数量
    'error_log_file': 'error_log.txt',  # 错误日志文件
    'send_error_notification': False    # 发送错误通知
}

# 数据质量检查设置
DATA_QUALITY_SETTINGS = {
    'check_data_integrity': True,  # 检查数据完整性
    'validate_price_range': True,   # 验证价格范围
    'check_missing_values': True,   # 检查缺失值
    'auto_fix_common_issues': True  # 自动修复常见问题
}

# 备份设置
BACKUP_SETTINGS = {
    'backup_before_update': True,   # 更新前备份
    'backup_after_update': False,   # 更新后备份
    'backup_directory': 'backups',  # 备份目录
    'max_backup_files': 10,         # 最大备份文件数
    'backup_retention_days': 30     # 备份保留天数
}

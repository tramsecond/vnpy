#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import subprocess
import sys
import os
import threading
import queue
from datetime import datetime

class TradingSystemGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("股票技术分析交易系统")
        self.root.geometry("800x600")
        
        # 设置窗口图标和样式
        self.setup_styles()
        
        # 创建主框架
        self.create_main_frame()
        
        # 输出队列用于线程间通信
        self.output_queue = queue.Queue()
        
        # 启动输出监控线程
        self.monitor_output()

    def setup_styles(self):
        """设置界面样式"""
        style = ttk.Style()
        style.theme_use('clam')
        
        # 配置样式
        style.configure('Title.TLabel', font=('Arial', 16, 'bold'))
        style.configure('Header.TLabel', font=('Arial', 12, 'bold'))
        style.configure('Script.TButton', font=('Arial', 10))
        style.configure('Status.TLabel', font=('Arial', 9))

    def create_main_frame(self):
        """创建主界面框架"""
        # 标题
        title_label = ttk.Label(self.root, text="股票技术分析交易系统", style='Title.TLabel')
        title_label.pack(pady=20)
        
        # 创建主容器
        main_container = ttk.Frame(self.root)
        main_container.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)
        
        # 左侧脚本选择区域
        self.create_script_panel(main_container)
        
        # 右侧输出显示区域
        self.create_output_panel(main_container)

    def create_script_panel(self, parent):
        """创建脚本选择面板"""
        script_frame = ttk.LabelFrame(parent, text="脚本选择", padding=10)
        script_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))
        
        # 脚本列表
        scripts = [
            {
                "name": "数据准备 - 股票数据",
                "description": "获取股票数据并计算技术指标",
                "script": "data_preparation.py",
                "category": "数据准备"
            },
            {
                "name": "数据准备 - 指数数据", 
                "description": "获取指数数据并计算技术指标",
                "script": "data_index_preparation.py",
                "category": "数据准备"
            },
            {
                "name": "数据准备 - 指数数据(国内版)", 
                "description": "使用国内数据源获取指数数据(更稳定可靠)",
                "script": "data_index_preparation_improved.py",
                "category": "数据准备"
            },
            {
                "name": "数据处理 - 指数信号分析", 
                "description": "为指数数据添加技术信号和下个周期涨跌幅分析",
                "script": "data_index.py",
                "category": "数据处理"
            },
            {
                "name": "数据处理 - 股票信号分析", 
                "description": "为股票数据添加技术信号和下个周期涨跌幅分析",
                "script": "data.py",
                "category": "数据处理"
            },
            {
                "name": "交易回测 - 测试1",
                "description": "使用trade_test1.py进行回测",
                "script": "trade_test1.py",
                "category": "交易回测"
            },
            {
                "name": "交易回测 - 测试2",
                "description": "使用trade_test2.py进行回测",
                "script": "trade_test2.py", 
                "category": "交易回测"
            },
            {
                "name": "交易回测 - 新策略",
                "description": "使用新策略进行回测(买入后根据信号和价格变化决定卖出)",
                "script": "trade_test1_new_strategy.py",
                "category": "交易回测"
            },
            {
                "name": "今日交易分析",
                "description": "生成今日交易分析报告",
                "script": "today_trade.py",
                "category": "交易分析"
            },
            {
                "name": "AKShare测试1",
                "description": "测试AKShare数据获取功能",
                "script": "akshareTest.py",
                "category": "数据测试"
            },
            {
                "name": "AKShare测试2",
                "description": "测试AKShare高级功能",
                "script": "akshareTest2.py",
                "category": "数据测试"
            },
            {
                "name": "AKShare测试3",
                "description": "测试AKShare完整功能",
                "script": "akshareTest3.py",
                "category": "数据测试"
            },
            {
                "name": "配置文件测试",
                "description": "测试配置文件是否正常工作",
                "script": "test_config.py",
                "category": "系统测试"
            },
            {
                "name": "新策略测试",
                "description": "测试新交易策略是否正常工作",
                "script": "test_new_strategy_simple.py",
                "category": "系统测试"
            }
        ]
        
        # 按类别分组显示
        categories = {}
        for script in scripts:
            cat = script["category"]
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(script)
        
        # 创建脚本按钮
        for category, cat_scripts in categories.items():
            # 类别标签
            cat_label = ttk.Label(script_frame, text=category, style='Header.TLabel')
            cat_label.pack(anchor=tk.W, pady=(10, 5))
            
            # 该类别下的脚本按钮
            for script in cat_scripts:
                btn_frame = ttk.Frame(script_frame)
                btn_frame.pack(fill=tk.X, pady=2)
                
                # 脚本按钮
                btn = ttk.Button(
                    btn_frame, 
                    text=script["name"],
                    command=lambda s=script: self.run_script(s),
                    style='Script.TButton'
                )
                btn.pack(side=tk.LEFT, fill=tk.X, expand=True)
                
                # 描述标签
                desc_label = ttk.Label(btn_frame, text=script["description"], 
                                     font=('Arial', 8), foreground='gray')
                desc_label.pack(side=tk.LEFT, padx=(5, 0))
        
        # 添加控制按钮
        control_frame = ttk.Frame(script_frame)
        control_frame.pack(fill=tk.X, pady=(20, 0))
        
        ttk.Button(control_frame, text="清空输出", 
                  command=self.clear_output).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(control_frame, text="打开结果文件夹", 
                  command=self.open_results_folder).pack(side=tk.LEFT)

    def create_output_panel(self, parent):
        """创建输出显示面板"""
        output_frame = ttk.LabelFrame(parent, text="运行输出", padding=10)
        output_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        
        # 输出文本框
        self.output_text = scrolledtext.ScrolledText(
            output_frame, 
            wrap=tk.WORD, 
            width=50, 
            height=20,
            font=('Consolas', 9)
        )
        self.output_text.pack(fill=tk.BOTH, expand=True)
        
        # 状态栏
        self.status_label = ttk.Label(output_frame, text="就绪", style='Status.TLabel')
        self.status_label.pack(anchor=tk.W, pady=(5, 0))

    def run_script(self, script_info):
        """运行选定的脚本"""
        script_name = script_info["name"]
        script_file = script_info["script"]
        
        # 检查脚本文件是否存在
        if not os.path.exists(script_file):
            messagebox.showerror("错误", f"脚本文件 {script_file} 不存在！")
            return
        
        # 更新状态
        self.status_label.config(text=f"正在运行: {script_name}")
        self.output_text.insert(tk.END, f"\n{'='*50}\n")
        self.output_text.insert(tk.END, f"开始运行: {script_name}\n")
        self.output_text.insert(tk.END, f"脚本文件: {script_file}\n")
        self.output_text.insert(tk.END, f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        self.output_text.insert(tk.END, f"{'='*50}\n")
        self.output_text.see(tk.END)
        
        # 在新线程中运行脚本
        thread = threading.Thread(target=self._run_script_thread, args=(script_file, script_name))
        thread.daemon = True
        thread.start()

    def _run_script_thread(self, script_file, script_name):
        """在后台线程中运行脚本"""
        try:
            # 运行Python脚本，设置编码为utf-8
            process = subprocess.Popen(
                [sys.executable, script_file],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                encoding='utf-8',
                errors='replace',
                bufsize=1
            )
            
            # 实时读取输出
            for line in iter(process.stdout.readline, ''):
                if line:
                    self.output_queue.put(line)
            
            # 等待进程完成
            return_code = process.wait()
            
            # 输出完成信息
            completion_msg = f"\n脚本执行完成，返回码: {return_code}\n"
            completion_msg += f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            completion_msg += f"{'='*50}\n"
            self.output_queue.put(completion_msg)
            
            # 更新状态
            if return_code == 0:
                self.output_queue.put("状态: 执行成功\n")
            else:
                self.output_queue.put("状态: 执行失败\n")
                
        except Exception as e:
            error_msg = f"运行脚本时出错: {str(e)}\n"
            self.output_queue.put(error_msg)

    def monitor_output(self):
        """监控输出队列并更新界面"""
        try:
            while True:
                # 非阻塞方式获取输出
                try:
                    line = self.output_queue.get_nowait()
                    # 确保输出文本是UTF-8编码
                    if isinstance(line, bytes):
                        try:
                            line = line.decode('utf-8')
                        except UnicodeDecodeError:
                            line = line.decode('gbk', errors='replace')
                    self.output_text.insert(tk.END, line)
                    self.output_text.see(tk.END)
                    self.output_text.update_idletasks()
                except queue.Empty:
                    break
        except Exception as e:
            # 记录错误但不中断程序
            print(f"输出监控错误: {str(e)}")
        
        # 每100ms检查一次输出队列
        self.root.after(100, self.monitor_output)

    def clear_output(self):
        """清空输出显示"""
        self.output_text.delete(1.0, tk.END)
        self.status_label.config(text="就绪")

    def open_results_folder(self):
        """打开结果文件夹"""
        try:
            # 尝试打开analyzed_results文件夹
            if os.path.exists("analyzed_results"):
                os.startfile("analyzed_results")
            elif os.path.exists("stock_data"):
                os.startfile("stock_data")
            elif os.path.exists("index_data"):
                os.startfile("index_data")
            else:
                os.startfile(".")
        except Exception as e:
            messagebox.showerror("错误", f"无法打开文件夹: {str(e)}")

def main():
    """主函数"""
    root = tk.Tk()
    app = TradingSystemGUI(root)
    
    # 设置窗口关闭事件
    def on_closing():
        if messagebox.askokcancel("退出", "确定要退出程序吗？"):
            root.destroy()
    
    root.protocol("WM_DELETE_WINDOW", on_closing)
    
    # 启动GUI
    root.mainloop()

if __name__ == "__main__":
    main()

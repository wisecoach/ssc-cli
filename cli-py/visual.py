import threading
import time
import random
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import matplotlib.gridspec as gridspec
import numpy as np
from collections import deque
import logging
from typing import Dict, List, Optional
import os

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ThreadMonitor:
    def __init__(self, update_interval=1.0):
        self.threads_data = {}
        self.lock = threading.Lock()
        self.update_interval = update_interval
        self.history = deque(maxlen=50)
        self.running = True
        self.next_thread_id = 0

    def register_thread(self, thread_name: str, target=100, task_type: str = "general") -> int:
        """注册新线程并返回线程ID"""
        with self.lock:
            thread_id = self.next_thread_id
            self.threads_data[thread_id] = {
                'name': thread_name,
                'type': task_type,
                'status': 'waiting',
                'start_time': None,
                'end_time': None,
                'progress': 0,
                'target': target,
                'message': '',
                'last_update': datetime.now(),
                'created_time': datetime.now()
            }
            self.next_thread_id += 1
        return thread_id

    def update_thread_status(self, thread_id: int, status: str, progress: int = 0, message: str = ''):
        """更新线程状态"""
        with self.lock:
            if thread_id in self.threads_data:
                self.threads_data[thread_id]['status'] = status
                self.threads_data[thread_id]['progress'] = progress
                self.threads_data[thread_id]['message'] = message
                self.threads_data[thread_id]['last_update'] = datetime.now()

                if status == 'running' and self.threads_data[thread_id]['start_time'] is None:
                    self.threads_data[thread_id]['start_time'] = datetime.now()
                elif status in ['completed', 'error'] and self.threads_data[thread_id]['end_time'] is None:
                    self.threads_data[thread_id]['end_time'] = datetime.now()

    def get_threads_data(self) -> Dict:
        """获取当前线程数据"""
        with self.lock:
            return self.threads_data.copy()

    def get_summary_stats(self) -> Dict:
        """获取统计摘要"""
        with self.lock:
            total = len(self.threads_data)
            status_count = {'waiting': 0, 'running': 0, 'completed': 0, 'error': 0}

            for thread in self.threads_data.values():
                if thread['status'] in status_count:
                    status_count[thread['status']] += 1

            return {
                'total_threads': total,
                'status_count': status_count,
                'waiting_threads': status_count['waiting'],
                'running_threads': status_count['running'],
                'completed_threads': status_count['completed'],
                'error_threads': status_count['error'],
                'timestamp': datetime.now()
            }

    def start_monitoring(self):
        """启动监控线程"""

        def monitor_loop():
            while self.running:
                try:
                    # 收集数据点
                    summary = self.get_summary_stats()
                    self.history.append(summary)
                    time.sleep(self.update_interval)
                except Exception as e:
                    logger.error(f"Monitoring error: {e}")

        monitor_thread = threading.Thread(target=monitor_loop, daemon=True, name="MonitorThread")
        monitor_thread.start()
        logger.info("Monitoring started")

    def stop_monitoring(self):
        """停止监控"""
        self.running = False
        logger.info("Monitoring stopped")


class ConsoleVisualizer:
    """控制台可视化工具，避免matplotlib问题"""

    def __init__(self, monitor: ThreadMonitor, display_completed=False, display_error=False):
        self.monitor = monitor
        self.running = True
        self.display_completed = display_completed
        self.display_error = display_error
        self.start_time = datetime.now()

    def clear_screen(self):
        """清屏"""
        os.system('clear')

    def display(self):
        self.clear_screen()
        print("=" * 100)
        print("THREAD EXECUTION MONITOR".center(100))
        print("=" * 100)

        data = self.monitor.get_threads_data()
        summary = self.monitor.get_summary_stats()

        # 显示摘要信息
        print(f"Total Threads: {summary['total_threads']} | "
              f"Waiting: {summary['waiting_threads']} | "
              f"Running: {summary['running_threads']} | "
              f"Completed: {summary['completed_threads']} | "
              f"Errors: {summary['error_threads']} | "
              f"TPS: {summary['completed_threads'] / (datetime.now() - self.start_time).total_seconds()} | ")
        print("=" * 100)

        # 显示线程详情
        print(f"{'INDEX':<4} {'ID':<4} {'Name':<15} {'Status':<10} {'Progress':<18} {'Duration/s':<20} {'Message'}")
        print("-" * 100)

        i = 0
        for thread_id, thread_data in sorted(data.items()):
            status = thread_data['status']
            if not self._display(status):
                continue
            # 使用颜色代码
            color_code = {
                'waiting': '\033[91m',  # 红色
                'running': '\033[92m',  # 绿色
                'completed': '\033[94m',  # 蓝色
                'error': '\033[93m'  # 黄色
            }.get(status, '\033[0m')

            reset_code = '\033[0m'

            print(f"{color_code} {i:<4} {thread_id:<4} {thread_data['name'][:15]:<15} "
                  f"{status:<10} {str(thread_data['progress']) + '/' + str(thread_data['target']) + 'O' if thread_data['end_time'] else 'X':<18} "
                  f"{(thread_data['end_time'] - thread_data['start_time']).total_seconds() if thread_data['start_time'] and thread_data['end_time'] else (datetime.now() - thread_data['start_time']).total_seconds() if thread_data['start_time'] else 0:<16.2f} "
                  f"{thread_data['message'][:50]}{reset_code}")
            i+=1

        print("=" * 100)

    def _display(self, status) -> bool:
        if status == 'completed' and not self.display_completed:
            return False
        if status == 'error' and not self.display_error:
            return False
        return True

    def run(self):
        """运行控制台可视化"""
        try:
            while self.running:
                self.display()
                time.sleep(1)  # 每秒更新一次
        except KeyboardInterrupt:
            self.running = False
            print("\nMonitoring stopped by user")


# 示例使用
def demo_worker(thread_id: int, monitor: ThreadMonitor, duration: int = 5):
    """示例工作线程"""
    thread_name = f"Worker-{thread_id}"
    registered_id = monitor.register_thread(thread_name, "processing")

    try:
        # 模拟等待
        monitor.update_thread_status(registered_id, 'waiting', 0, 'Waiting for execution')
        wait_time = random.uniform(0.5, 3.0)
        time.sleep(wait_time)

        # 开始执行
        monitor.update_thread_status(registered_id, 'running', 0, 'Processing started')

        # 模拟处理过程
        steps = random.randint(5, 10)
        for progress in range(0, 101, 100 // steps):
            time.sleep(duration / steps)
            monitor.update_thread_status(registered_id, 'running', progress,
                                         f'Processing step {progress // (100 // steps)}/{steps}')

        # 完成
        monitor.update_thread_status(registered_id, 'completed', 100, 'Task completed successfully')

    except Exception as e:
        monitor.update_thread_status(registered_id, 'error', 0, f'Error: {str(e)}')


def run_demo():
    """运行演示"""
    monitor = ThreadMonitor()
    monitor.start_monitoring()

    # 使用控制台可视化
    visualizer = ConsoleVisualizer(monitor)

    # 启动可视化（在单独线程中）
    viz_thread = threading.Thread(target=visualizer.run, daemon=True)
    viz_thread.start()

    # 给可视化界面一些启动时间
    time.sleep(1)

    # 启动一些示例工作线程
    threads = []
    for i in range(15):
        thread = threading.Thread(
            target=demo_worker,
            args=(i, monitor, random.randint(3, 8)),
            daemon=True
        )
        threads.append(thread)
        thread.start()
        time.sleep(random.uniform(0.1, 0.8))

    logger.info(f"Started {len(threads)} worker threads")

    # 等待所有线程完成或用户关闭窗口
    try:
        while any(thread.is_alive() for thread in threads):
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user")
    finally:
        monitor.stop_monitoring()
        visualizer.running = False
        print("Demo completed")


if __name__ == "__main__":
    print("Starting thread monitoring visualization...")
    print("Press Ctrl+C to stop")
    run_demo()
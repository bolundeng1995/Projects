import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import json
from pathlib import Path
from datetime import datetime, time
from typing import Dict, Any
from dataclasses import dataclass
import logging
from apscheduler.schedulers.background import BackgroundScheduler
import subprocess
from task_config import TaskManager

@dataclass
class TaskInfo:
    script_path: str
    schedule_time: time
    is_active: bool = True
    last_run: str = ""
    next_run: str = ""

class SchedulerGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Python Script Scheduler")
        self.root.geometry("800x600")
        
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        
        self.tasks: Dict[str, TaskInfo] = {}
        self.task_manager = TaskManager()
        self.setup_logging()
        self.create_gui()
        self.load_tasks()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scheduler.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def create_gui(self):
        # Create main frames
        self.create_task_frame = ttk.LabelFrame(self.root, text="Add New Task", padding=10)
        self.create_task_frame.pack(fill="x", padx=5, pady=5)
        
        self.tasks_frame = ttk.LabelFrame(self.root, text="Scheduled Tasks", padding=10)
        self.tasks_frame.pack(fill="both", expand=True, padx=5, pady=5)
        
        # Add Task Section
        ttk.Label(self.create_task_frame, text="Task Name:").grid(row=0, column=0, padx=5, pady=5)
        self.task_name = ttk.Entry(self.create_task_frame, width=30)
        self.task_name.grid(row=0, column=1, padx=5, pady=5)
        
        ttk.Label(self.create_task_frame, text="Script Path:").grid(row=0, column=2, padx=5, pady=5)
        self.script_path = ttk.Entry(self.create_task_frame, width=40)
        self.script_path.grid(row=0, column=3, padx=5, pady=5)
        
        ttk.Button(self.create_task_frame, text="Browse", command=self.browse_script).grid(
            row=0, column=4, padx=5, pady=5
        )
        
        ttk.Label(self.create_task_frame, text="Time (HH:MM):").grid(row=1, column=0, padx=5, pady=5)
        self.time_entry = ttk.Entry(self.create_task_frame, width=10)
        self.time_entry.grid(row=1, column=1, padx=5, pady=5)
        
        ttk.Button(self.create_task_frame, text="Add Task", command=self.add_task).grid(
            row=1, column=4, padx=5, pady=5
        )
        
        # Tasks List
        columns = ("Task Name", "Script Path", "Schedule Time", "Status", "Last Run", "Next Run")
        self.task_tree = ttk.Treeview(self.tasks_frame, columns=columns, show="headings")
        
        for col in columns:
            self.task_tree.heading(col, text=col)
            self.task_tree.column(col, width=100)
        
        self.task_tree.pack(fill="both", expand=True, padx=5, pady=5)
        
        # Control Buttons
        button_frame = ttk.Frame(self.tasks_frame)
        button_frame.pack(fill="x", padx=5, pady=5)
        
        ttk.Button(button_frame, text="Enable", command=self.enable_task).pack(side="left", padx=5)
        ttk.Button(button_frame, text="Disable", command=self.disable_task).pack(side="left", padx=5)
        ttk.Button(button_frame, text="Remove", command=self.remove_task).pack(side="left", padx=5)
        ttk.Button(button_frame, text="Run Now", command=self.run_task_now).pack(side="left", padx=5)
        
    def browse_script(self):
        filename = filedialog.askopenfilename(
            filetypes=[("Python Files", "*.py"), ("All Files", "*.*")]
        )
        if filename:
            self.script_path.delete(0, tk.END)
            self.script_path.insert(0, filename)
            
    def add_task(self):
        name = self.task_name.get().strip()
        script = self.script_path.get().strip()
        schedule_time = self.time_entry.get().strip()
        
        if not all([name, script, schedule_time]):
            messagebox.showerror("Error", "All fields are required!")
            return
            
        try:
            hour, minute = map(int, schedule_time.split(":"))
            schedule_time = time(hour, minute)
        except ValueError:
            messagebox.showerror("Error", "Invalid time format! Use HH:MM")
            return
            
        if not Path(script).exists():
            messagebox.showerror("Error", "Script file does not exist!")
            return
            
        task_info = TaskInfo(
            script_path=script,
            schedule_time=schedule_time
        )
        
        self.tasks[name] = task_info
        self.schedule_task(name, task_info)
        self.update_task_list()
        self.save_tasks()
        
        # Clear inputs
        self.task_name.delete(0, tk.END)
        self.script_path.delete(0, tk.END)
        self.time_entry.delete(0, tk.END)
        
    def schedule_task(self, name: str, task_info: TaskInfo):
        job = self.scheduler.add_job(
            self.run_script,
            'cron',
            args=[name, task_info.script_path],
            hour=task_info.schedule_time.hour,
            minute=task_info.schedule_time.minute,
            id=name
        )
        
        # Update next run time
        next_run = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")
        task_info.next_run = next_run
        
    def run_script(self, name: str, script_path: str):
        try:
            self.logger.info(f"Running script: {script_path}")
            subprocess.run(['python', script_path], check=True)
            
            # Update last run time
            self.tasks[name].last_run = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.update_task_list()
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error running script {script_path}: {str(e)}")
            
    def update_task_list(self):
        for item in self.task_tree.get_children():
            self.task_tree.delete(item)
            
        for name, task in self.tasks.items():
            status = "Active" if task.is_active else "Disabled"
            self.task_tree.insert("", "end", values=(
                name,
                task.script_path,
                f"{task.schedule_time.hour:02d}:{task.schedule_time.minute:02d}",
                status,
                task.last_run,
                task.next_run
            ))
            
    def enable_task(self):
        selected = self.task_tree.selection()
        if not selected:
            return
            
        task_name = self.task_tree.item(selected[0])['values'][0]
        task_info = self.tasks[task_name]
        task_info.is_active = True
        
        self.schedule_task(task_name, task_info)
        self.update_task_list()
        self.save_tasks()
        
    def disable_task(self):
        selected = self.task_tree.selection()
        if not selected:
            return
            
        task_name = self.task_tree.item(selected[0])['values'][0]
        self.tasks[task_name].is_active = False
        self.scheduler.remove_job(task_name)
        self.update_task_list()
        self.save_tasks()
        
    def remove_task(self):
        selected = self.task_tree.selection()
        if not selected:
            return
            
        task_name = self.task_tree.item(selected[0])['values'][0]
        self.scheduler.remove_job(task_name)
        del self.tasks[task_name]
        self.update_task_list()
        self.save_tasks()
        
    def run_task_now(self):
        selected = self.task_tree.selection()
        if not selected:
            return
            
        task_name = self.task_tree.item(selected[0])['values'][0]
        task_info = self.tasks[task_name]
        self.run_script(task_name, task_info.script_path)
        
    def save_tasks(self):
        tasks_data = {}
        for name, task in self.tasks.items():
            tasks_data[name] = {
                'script_path': task.script_path,
                'schedule_time': f"{task.schedule_time.hour:02d}:{task.schedule_time.minute:02d}",
                'is_active': task.is_active,
                'last_run': task.last_run,
                'next_run': task.next_run
            }
            
        for name, data in tasks_data.items():
            self.task_manager.save_task_info(name, data)
            
    def load_tasks(self):
        try:
            tasks_data = self.task_manager.load_config()
            
            for name, data in tasks_data.items():
                hour, minute = map(int, data['schedule_time'].split(':'))
                task_info = TaskInfo(
                    script_path=data['script_path'],
                    schedule_time=time(hour, minute),
                    is_active=data['is_active'],
                    last_run=data['last_run'],
                    next_run=data['next_run']
                )
                
                self.tasks[name] = task_info
                if task_info.is_active:
                    self.schedule_task(name, task_info)
                    
            self.update_task_list()
            
        except FileNotFoundError:
            pass
            
    def run(self):
        self.root.mainloop()
        
if __name__ == "__main__":
    app = SchedulerGUI()
    app.run() 
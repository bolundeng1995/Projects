"""
GUI Scheduler Application for managing and scheduling Python tasks.

This module provides a graphical interface for:
- Adding and editing scheduled tasks
- Managing task execution schedules
- Monitoring task execution status
- Enabling/disabling tasks
- Running tasks on demand

Usage:
    Run this script directly to launch the GUI:
    $ python src/gui_scheduler.py
"""

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
    """
    Data class for storing task configuration and status.
    
    Attributes:
        script_path (str): Path to the Python script to be executed
        schedule_time (time): Time of day to run the task
        frequency (str): Frequency of execution ('daily', 'weekly', 'monthly')
        days (str): Days to run the task:
            - For daily: 'mon-fri' or 'mon,wed,fri'
            - For weekly: '0-4' (Monday to Friday) or '1,3,5'
            - For monthly: '1,15' (1st and 15th of month)
        is_active (bool): Whether the task is currently active
        last_run (str): Timestamp of last execution
        next_run (str): Timestamp of next scheduled execution
        last_status (str): Status of last execution
    """
    script_path: str
    schedule_time: time
    frequency: str = 'daily'
    days: str = 'mon-fri'
    is_active: bool = True
    last_run: str = ""
    next_run: str = ""
    last_status: str = "Not run yet"

class SchedulerGUI:
    """
    Main GUI application for task scheduling and management.
    
    This class provides a graphical interface for:
    - Adding new tasks with custom schedules
    - Editing existing tasks
    - Enabling/disabling task execution
    - Monitoring task status
    - Running tasks on demand
    
    Usage:
        app = SchedulerGUI()
        app.run()
    """
    
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Python Script Scheduler")
        self.root.geometry("1000x700")
        
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        
        self.tasks: Dict[str, TaskInfo] = {}
        self.task_manager = TaskManager()
        self.setup_logging()
        self.frequency_options = {
            'daily': ['mon-fri', 'mon-sun', 'mon,wed,fri', 'tue,thu'],
            'weekly': ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'],
            'monthly': ['1', '15', '1,15', 'last']  # Common monthly schedules
        }
        
        # Add mapping for converting day names to numbers (used in scheduler)
        self.day_name_to_number = {
            'monday': '0',
            'tuesday': '1',
            'wednesday': '2',
            'thursday': '3',
            'friday': '4',
            'saturday': '5',
            'sunday': '6'
        }
        
        self.day_number_to_name = {v: k for k, v in self.day_name_to_number.items()}
        
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
        self.create_task_frame = ttk.LabelFrame(self.root, text="Add/Edit Task", padding=10)
        self.create_task_frame.pack(fill="x", padx=5, pady=5)
        
        self.tasks_frame = ttk.LabelFrame(self.root, text="Scheduled Tasks", padding=10)
        self.tasks_frame.pack(fill="both", expand=True, padx=5, pady=5)
        
        # Add/Edit Task Section
        ttk.Label(self.create_task_frame, text="Task Name:").grid(row=0, column=0, padx=5, pady=5)
        self.task_name = ttk.Entry(self.create_task_frame, width=30)
        self.task_name.grid(row=0, column=1, padx=5, pady=5)
        
        ttk.Label(self.create_task_frame, text="Script Path:").grid(row=0, column=2, padx=5, pady=5)
        self.script_path = ttk.Entry(self.create_task_frame, width=40)
        self.script_path.grid(row=0, column=3, padx=5, pady=5)
        
        ttk.Button(self.create_task_frame, text="Browse", command=self.browse_script).grid(
            row=0, column=4, padx=5, pady=5
        )
        
        # Schedule Configuration
        ttk.Label(self.create_task_frame, text="Time (HH:MM):").grid(row=1, column=0, padx=5, pady=5)
        self.time_entry = ttk.Entry(self.create_task_frame, width=10)
        self.time_entry.grid(row=1, column=1, padx=5, pady=5)
        
        # Frequency dropdown
        ttk.Label(self.create_task_frame, text="Frequency:").grid(row=1, column=2, padx=5, pady=5)
        self.frequency = ttk.Combobox(
            self.create_task_frame, 
            values=['daily', 'weekly', 'monthly'],
            state='readonly'
        )
        self.frequency.set('daily')
        self.frequency.grid(row=1, column=3, padx=5, pady=5)
        self.frequency.bind('<<ComboboxSelected>>', self.update_days_options)
        
        # Days dropdown
        ttk.Label(self.create_task_frame, text="Days:").grid(row=2, column=0, padx=5, pady=5)
        self.days = ttk.Combobox(
            self.create_task_frame, 
            width=28,
            state='readonly'
        )
        self.days.grid(row=2, column=1, columnspan=2, padx=5, pady=5)
        
        # Initialize days options
        self.update_days_options()
        
        ttk.Button(self.create_task_frame, text="Add/Update Task", command=self.add_task).grid(
            row=2, column=4, padx=5, pady=5
        )
        
        # Tasks List
        columns = ("Task Name", "Script Path", "Schedule Time", "Frequency", "Days", "Status", "Last Run", "Next Run", "Last Status")
        self.task_tree = ttk.Treeview(self.tasks_frame, columns=columns, show="headings")
        
        for col in columns:
            self.task_tree.heading(col, text=col)
            width = 100 if col not in ["Script Path", "Last Run", "Next Run"] else 200
            self.task_tree.column(col, width=width)
        
        self.task_tree.pack(fill="both", expand=True, padx=5, pady=5)
        self.task_tree.bind('<Double-1>', self.edit_task)
        
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
            
    def update_days_options(self, event=None):
        """Update days dropdown options based on selected frequency"""
        frequency = self.frequency.get()
        self.days['values'] = self.frequency_options[frequency]
        
        # Set default value for each frequency
        defaults = {
            'daily': 'mon-fri',
            'weekly': 'monday',  # Changed from '0' to 'monday'
            'monthly': '1'
        }
        self.days.set(defaults[frequency])
        
    def edit_task(self, event):
        """
        Handle double-click event to edit an existing task.
        
        Loads the selected task's details into the input fields for editing.
        Double-click any task in the list to edit its properties.
        """
        selected = self.task_tree.selection()
        if not selected:
            return
            
        item = self.task_tree.item(selected[0])
        values = item['values']
        
        # Populate fields with selected task's data
        self.task_name.delete(0, tk.END)
        self.task_name.insert(0, values[0])
        
        self.script_path.delete(0, tk.END)
        self.script_path.insert(0, values[1])
        
        self.time_entry.delete(0, tk.END)
        self.time_entry.insert(0, values[2])
        
        # Set frequency first so days options update
        self.frequency.set(values[3])
        self.update_days_options()  # Update days options before setting days value
        self.days.set(values[4])
        
    def add_task(self):
        """
        Add a new task or update an existing one.
        
        Validates and processes the input fields:
        - Task name (unique identifier)
        - Script path (must exist)
        - Schedule time (HH:MM format)
        - Frequency (daily/weekly/monthly)
        - Days (format depends on frequency)
        
        Shows error messages if validation fails.
        """
        name = self.task_name.get().strip()
        script = self.script_path.get().strip()
        schedule_time = self.time_entry.get().strip()
        frequency = self.frequency.get()
        days = self.days.get().strip()
        
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
            
        # Update existing task or create new one
        task_info = TaskInfo(
            script_path=script,
            schedule_time=schedule_time,
            frequency=frequency,
            days=days
        )
        
        # Remove old schedule if exists
        if name in self.tasks:
            self.scheduler.remove_job(name)
        
        self.tasks[name] = task_info
        self.schedule_task(name, task_info)
        self.update_task_list()
        self.save_tasks()
        
        # Clear inputs
        self.task_name.delete(0, tk.END)
        self.script_path.delete(0, tk.END)
        self.time_entry.delete(0, tk.END)
        self.frequency.set('daily')
        self.days.delete(0, tk.END)
        self.days.insert(0, 'mon-fri')
        
    def schedule_task(self, name: str, task_info: TaskInfo):
        """
        Schedule a task for execution.
        
        Args:
            name (str): Unique identifier for the task
            task_info (TaskInfo): Task configuration and schedule details
            
        Creates a cron job based on the frequency and schedule parameters.
        Updates the next run time in the task info.
        """
        schedule_params = {
            'hour': task_info.schedule_time.hour,
            'minute': task_info.schedule_time.minute,
        }
        
        if task_info.frequency == 'daily':
            schedule_params['day_of_week'] = task_info.days
        elif task_info.frequency == 'weekly':
            # Convert day name to number for scheduler
            day_number = self.day_name_to_number.get(task_info.days.lower())
            schedule_params['day_of_week'] = day_number
        elif task_info.frequency == 'monthly':
            schedule_params['day'] = task_info.days
            
        job = self.scheduler.add_job(
            self.run_script,
            'cron',
            args=[name, task_info.script_path],
            id=name,
            **schedule_params
        )
        
        # Update next run time
        next_run = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")
        task_info.next_run = next_run
        
    def run_script(self, name: str, script_path: str):
        """
        Execute a Python script.
        
        Args:
            name (str): Task identifier for logging
            script_path (str): Path to the Python script
            
        Logs execution status and updates last run time.
        """
        try:
            self.logger.info(f"Running script: {script_path}")
            result = subprocess.run(['python', script_path], check=True, 
                                 capture_output=True, text=True)
            
            # Update last run time and status
            self.tasks[name].last_run = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.tasks[name].last_status = "Success"
            
            # Update next run time
            job = self.scheduler.get_job(name)
            if job and job.next_run_time:
                self.tasks[name].next_run = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")
            
            self.update_task_list()
            self.save_tasks()
            
            # Log output
            self.logger.info(f"Task '{name}' completed successfully")
            if result.stdout:
                self.logger.info(f"Output: {result.stdout}")
            
        except subprocess.CalledProcessError as e:
            error_msg = f"Error: {e.stderr if e.stderr else str(e)}"
            self.tasks[name].last_status = f"Failed: {error_msg}"
            self.logger.error(f"Error running script {script_path}: {error_msg}")
            self.update_task_list()
            self.save_tasks()
            
    def update_task_list(self):
        for item in self.task_tree.get_children():
            self.task_tree.delete(item)
            
        for name, task in self.tasks.items():
            status = "Active" if task.is_active else "Disabled"
            self.task_tree.insert("", "end", values=(
                name,
                task.script_path,
                f"{task.schedule_time.hour:02d}:{task.schedule_time.minute:02d}",
                task.frequency,
                task.days,
                status,
                task.last_run,
                task.next_run,
                task.last_status
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
                'frequency': task.frequency,
                'days': task.days,
                'is_active': task.is_active,
                'last_run': task.last_run,
                'next_run': task.next_run,
                'last_status': task.last_status
            }
            
        for name, data in tasks_data.items():
            self.task_manager.save_task_info(name, data)
            
    def load_tasks(self):
        try:
            tasks_data = self.task_manager.load_config()
            
            for name, data in tasks_data.items():
                hour, minute = map(int, data['schedule_time'].split(':'))
                days = data.get('days', 'mon-fri')
                
                # Convert day number to name for weekly tasks
                if data.get('frequency') == 'weekly' and days in self.day_number_to_name:
                    days = self.day_number_to_name[days]
                
                task_info = TaskInfo(
                    script_path=data['script_path'],
                    schedule_time=time(hour, minute),
                    frequency=data.get('frequency', 'daily'),
                    days=days,
                    is_active=data['is_active'],
                    last_run=data['last_run'],
                    next_run=data['next_run'],
                    last_status=data.get('last_status', 'Not run yet')
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
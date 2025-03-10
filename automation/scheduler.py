import tkinter as tk
import ttkbootstrap
from tkinter import filedialog, messagebox, simpledialog, scrolledtext
from tkinter import ttk
from datetime import datetime
from threading import Lock
import os
import json
import schedule
import time
import threading
import subprocess
import logging
import traceback
from ttkbootstrap import Style
from functools import partial

SCRIPT_STORAGE_FILE = "scripts.json"
LOG_FILE = "execution_logs.txt"

class SchedulerStatus:
    RUNNING = "Running"
    PAUSED = "Paused"
    NOT_RUNNING = "Not Running"


class ScriptManager:
    """Handles script storage, retrieval, and management."""

    def __init__(self, storage_file=SCRIPT_STORAGE_FILE):
        self.storage_file = storage_file
        self.scripts = self.load_scripts()

    def load_scripts(self):
        """Load scripts from storage file with error handling."""
        if os.path.exists(self.storage_file):
            try:
                with open(self.storage_file, "r") as file:
                    return json.load(file)
            except (json.JSONDecodeError, IOError):
                logging.error("Error reading scripts.json. Returning an empty list.")
                return []
        return []

    def save_scripts(self):
        with open(self.storage_file, "w") as file:
            json.dump(self.scripts, file)

    def add_script(self, file_path):
        if not any(script["file_path"] == file_path for script in self.scripts):
            self.scripts.append({"file_path": file_path, "time": None})
            self.save_scripts()
            return True
        return False

    def remove_script(self, file_name):
        self.scripts = [s for s in self.scripts if os.path.basename(s["file_path"]) != file_name]
        self.save_scripts()

    def update_script(self, file_name, time=None, frequency=None, weekday=None):
        """Update the schedule information for a script."""
        for script in self.scripts:
            if os.path.basename(script["file_path"]) == file_name:
                if time is not None:
                    script["time"] = time
                if frequency is not None:
                    script["frequency"] = frequency
                if weekday is not None:
                    script["weekday"] = weekday
                self.save_scripts()
                return True
        return False


class SchedulerCore:
    def __init__(self, run_script_func):
        """
        Initialize the scheduler core.

        :param run_script_func: A reference to the run_script method in SchedulerApp
        """
        self.is_running = False  # Default state is Not Running
        self.jobs = []  # List of all scheduled jobs
        self.thread = None  # Thread for running the scheduler
        self.lock = Lock()  # Lock for thread safety
        self.paused_scripts = set()  # Store paused script file names
        self.run_script_func = run_script_func  # Reference to the run_script method

    def add_job_with_frequency(self, time, job_func, file_name, frequency, weekday=None):
        """
        Add a job to the scheduler with the specified frequency and time.

        :param time: Time in HH:MM format
        :param job_func: The function to execute
        :param file_name: The name of the script file
        :param frequency: The frequency (e.g., "daily" or "weekly")
        :param weekday: The weekday (if weekly frequency)
        """
        with self.lock:
            # Check if the job already exists
            for job in self.jobs:
                if (
                    job["file_name"] == file_name
                    and job["time"] == time
                    and job["frequency"] == frequency
                    and job.get("weekday") == weekday
                ):
                    logging.warning(f"Job already exists: {file_name} at {time} ({frequency} {weekday or ''})")
                    return

            # Schedule the job
            if frequency == "daily":
                schedule.every().day.at(time).do(job_func).tag(file_name)
            elif frequency == "weekly" and weekday:
                weekday_mapping = {
                    "monday": schedule.every().monday,
                    "tuesday": schedule.every().tuesday,
                    "wednesday": schedule.every().wednesday,
                    "thursday": schedule.every().thursday,
                    "friday": schedule.every().friday,
                    "saturday": schedule.every().saturday,
                    "sunday": schedule.every().sunday,
                }
                if weekday in weekday_mapping:
                    weekday_mapping[weekday].at(time).do(job_func).tag(file_name)

            # Store the job details
            self.jobs.append({"file_name": file_name, "time": time, "frequency": frequency, "weekday": weekday})

    def start_scheduler(self):
        """Start the scheduler."""
        self.is_running = True
        if not self.thread or not self.thread.is_alive():
            self.thread = threading.Thread(target=self.run_scheduler, daemon=True)
            self.thread.start()

    def run_scheduler(self):
        """Main loop for running the scheduler."""
        logging.info("Scheduler thread started.")
        while True:
            if self.is_running:
                with self.lock:
                    schedule.run_pending()  # Run scheduled jobs
            time.sleep(1)  # Sleep for 1 second between checks

    def pause_scheduler(self):
        """Pause the entire scheduler."""
        self.is_running = False

    def resume_scheduler(self):
        """Resume the entire scheduler."""
        self.is_running = True

    def clear_jobs(self):
        """Clear all scheduled jobs."""
        schedule.clear()
        self.jobs.clear()

    def pause_script(self, file_name):
        """
        Pause a specific script by its file name.

        :param file_name: The name of the script file to pause
        """
        with self.lock:
            self.paused_scripts.add(file_name)
            schedule.clear(file_name)  # Remove the script's job from the schedule

    def resume_script(self, file_name):
        with self.lock:
            # Exit early if the script is not in paused_scripts
            if file_name not in self.paused_scripts:
                logging.debug(f"Script '{file_name}' is not paused. Skipping resume.")
                return

            # Remove from paused list
            self.paused_scripts.remove(file_name)

            # Clear existing scheduled jobs for this file_name
            schedule.clear(file_name)

            # Re-schedule the job
            for job in self.jobs:
                if job["file_name"] == file_name:
                    job_func = partial(self.run_script_func, file_name=file_name)
                    if job["frequency"] == "daily":
                        schedule.every().day.at(job["time"]).do(job_func).tag(file_name)
                    elif job["frequency"] == "weekly" and job.get("weekday"):
                        weekday_mapping = {
                            "monday": schedule.every().monday,
                            "tuesday": schedule.every().tuesday,
                            "wednesday": schedule.every().wednesday,
                            "thursday": schedule.every().thursday,
                            "friday": schedule.every().friday,
                            "saturday": schedule.every().saturday,
                            "sunday": schedule.every().sunday,
                        }
                        weekday_mapping[job["weekday"]].at(job["time"]).do(job_func).tag(file_name)

                    logging.info(f"Resumed script: {file_name}")
                    return  "resume call"# Exit after rescheduling the job

    def _re_add_job(self, file_name):
        """Re-add a paused job to the scheduler."""
        for job in self.jobs:
            if job["file_name"] == file_name:
                job_func = partial(self.run_script_func, file_name=file_name)
                self.add_job_with_frequency(
                    job["time"],
                    job_func,
                    job["file_name"],
                    job["frequency"],
                    job.get("weekday"),
                )
                logging.info(f"Resumed script: {file_name}")

    def is_script_paused(self, file_name):
        """
        Check if a specific script is paused.

        :param file_name: The name of the script file
        :return: True if the script is paused, False otherwise
        """
        return file_name in self.paused_scripts

class SchedulerApp:
    def __init__(self, root):
        self.root = root
        self.style = Style(theme="flatly")
        self.script_manager = ScriptManager()
        self.scheduler = SchedulerCore(self.run_script)  # Pass run_script to SchedulerCore
        self.current_scheduler_status = SchedulerStatus.NOT_RUNNING  # Track the current status
        self.init_logging()
        self.build_gui()
        self.schedule_saved_scripts()  # Schedule saved scripts at startup
        self.auto_refresh_gui()
        self.update_scheduler_status(SchedulerStatus.NOT_RUNNING, color="red")  # Default to Not Running

    def refresh_gui(self):
        """Refresh the GUI components."""
        self.update_script_tree()
        self.update_scheduled_jobs_tree()

    def schedule_saved_scripts(self):
        """Schedule scripts with valid times, frequencies, and weekdays from scripts.json."""
        for script in self.script_manager.scripts:
            if not os.path.exists(script["file_path"]):
                logging.warning(f"Script not found during initialization: {script['file_path']}")
                continue
            if script["time"] and script.get("frequency"):  # Only schedule scripts with valid time and frequency
                try:
                    datetime.strptime(script["time"], "%H:%M")  # Validate time format
                    file_name = os.path.basename(script["file_path"])
                    job_func = partial(self.run_script, file_name=file_name)
                    self.scheduler.add_job_with_frequency(
                        script["time"], job_func, file_name, script["frequency"], script.get("weekday")
                    )
                except ValueError:
                    logging.error(f"Invalid time format for script: {script['file_path']}")
        # Update GUI after scheduling
        self.refresh_gui()

    def init_logging(self):
        logging.basicConfig(
            filename=LOG_FILE,
            level=logging.INFO,  # Set to DEBUG to capture all log levels
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        logging.debug("Logging initialized with DEBUG level.")

    def build_gui(self):
        # Apply theme
        style = Style(theme="flatly")  # Change theme if needed
        style.configure("success.TLabel", foreground="green", font=("Helvetica", 12, "bold"))
        style.configure("warning.TLabel", foreground="orange", font=("Helvetica", 12, "bold"))
        style.configure("danger.TLabel", foreground="red", font=("Helvetica", 12, "bold"))
        style.configure("TLabel", font=("Helvetica", 12))
        style.configure("TButton", padding=10)

        self.root.title("Python Script Scheduler")
        self.root.geometry("1000x700")

        # Build GUI sections
        self._build_search_section()
        self._build_script_section()
        self._build_buttons_section()
        self._build_scheduler_controls()
        self._build_jobs_section()
        self._build_output_panel()

    def _build_search_section(self):
        search_frame = ttk.Frame(self.root, padding=(10, 5))
        search_frame.pack(fill=tk.X)
        ttk.Label(search_frame, text="Search:", font=("Helvetica", 10)).pack(side=tk.LEFT, padx=5)
        search_entry = ttk.Entry(search_frame, width=40)
        search_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(search_frame, text="Search", style="primary.TButton",
                   command=lambda: self.search_scripts(search_entry.get())).pack(side=tk.LEFT, padx=5)
        ttk.Button(search_frame, text="Reset", style="primary.TButton", command=self.update_script_tree).pack(
            side=tk.LEFT, padx=5)

    def _build_script_section(self):
        script_frame = ttk.LabelFrame(self.root, text="Scripts", padding=(10, 5))
        script_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        script_list_frame = ttk.Frame(script_frame)
        script_list_frame.pack(fill=tk.BOTH, expand=True)

        # Treeview with Scrollbar
        script_scrollbar = ttk.Scrollbar(script_list_frame, orient=tk.VERTICAL)
        self.script_list_tree = ttk.Treeview(
            script_list_frame,
            columns=("File Name", "Time", "Frequency", "Weekday", "File Location"),
            show="headings",
            height=10,
            yscrollcommand=script_scrollbar.set
        )
        script_scrollbar.config(command=self.script_list_tree.yview)
        script_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.script_list_tree.pack(fill=tk.BOTH, expand=True)

        # Treeview Column Configuration
        self.script_list_tree.heading("File Name", text="File Name")
        self.script_list_tree.heading("Time", text="Time")
        self.script_list_tree.heading("Frequency", text="Frequency")
        self.script_list_tree.heading("Weekday", text="Weekday")
        self.script_list_tree.heading("File Location", text="File Location")
        self.script_list_tree.column("File Name", width=150, stretch=tk.YES)
        self.script_list_tree.column("Time", width=100, stretch=tk.YES)
        self.script_list_tree.column("Frequency", width=100, stretch=tk.YES)
        self.script_list_tree.column("Weekday", width=100, stretch=tk.YES)
        self.script_list_tree.column("File Location", width=500, stretch=tk.YES)

    def _build_buttons_section(self):
        buttons_frame = ttk.Frame(self.root, padding=(10, 5))
        buttons_frame.pack(fill=tk.X)

        ttk.Button(buttons_frame, text="Add Script", style="primary.TButton", command=self.add_script).pack(
            side=tk.LEFT, padx=5)
        ttk.Button(buttons_frame, text="Remove Selected", style="primary.TButton", command=self.remove_script).pack(
            side=tk.LEFT, padx=5)
        ttk.Button(buttons_frame, text="Schedule Selected", style="primary.TButton", command=self.schedule_script).pack(
            side=tk.LEFT, padx=5)
        ttk.Button(buttons_frame, text="Run Selected Now", style="primary.TButton", command=self.run_now).pack(
            side=tk.LEFT, padx=5)
        ttk.Button(buttons_frame, text="Pause Script", style="primary.TButton",
                   command=self.pause_selected_script).pack(side=tk.LEFT, padx=5)
        ttk.Button(buttons_frame, text="Resume Script", style="primary.TButton",
                   command=self.resume_selected_script).pack(side=tk.LEFT, padx=5)
        ttk.Button(buttons_frame, text="View Logs", style="primary.TButton", command=self.open_log_viewer).pack(
            side=tk.LEFT, padx=5)

    def _build_scheduler_controls(self):
        controls_frame = ttk.LabelFrame(self.root, text="Scheduler Controls", padding=(10, 5))
        controls_frame.pack(fill=tk.X, padx=10, pady=5)

        self.start_button = ttk.Button(controls_frame, text="Start Scheduler", style="primary.TButton",
                                       command=self.start_scheduler)
        self.start_button.pack(side=tk.LEFT, padx=5)

        self.pause_button = ttk.Button(controls_frame, text="Pause Scheduler", style="primary.TButton",
                                       command=self.pause_scheduler, state=tk.DISABLED)
        self.pause_button.pack(side=tk.LEFT, padx=5)

        self.resume_button = ttk.Button(controls_frame, text="Resume Scheduler", style="primary.TButton",
                                        command=self.resume_selected_script, state=tk.DISABLED)
        self.resume_button.pack(side=tk.LEFT, padx=5)

        self.clear_jobs_button = ttk.Button(controls_frame, text="Clear Scheduled Jobs",
                                            style="primary.TButton", command=self.clear_scheduled_jobs)
        self.clear_jobs_button.pack(side=tk.LEFT, padx=5)

        # Dark Mode Toggle
        self.dark_mode = tk.BooleanVar(value=False)  # Track dark mode state
        self.dark_mode_toggle = ttk.Checkbutton(
            controls_frame,
            text="Dark Mode",
            variable=self.dark_mode,
            style="primary.TCheckbutton",
            command=self.toggle_dark_mode
        )
        self.dark_mode_toggle.pack(side=tk.LEFT, padx=5)

        # Set default status to Not Running
        self.scheduler_status = ttk.Label(
            self.root,
            text="Scheduler Status: Not Running",
            foreground="red",
            font=("Helvetica", 12, "bold")
        )
        self.scheduler_status.pack(pady=5)

    def _build_jobs_section(self):
        jobs_frame = ttk.LabelFrame(self.root, text="Scheduled Jobs", padding=(10, 5))
        jobs_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        jobs_list_frame = ttk.Frame(jobs_frame)
        jobs_list_frame.pack(fill=tk.BOTH, expand=True)

        # Treeview with Scrollbar for Scheduled Jobs
        jobs_scrollbar = ttk.Scrollbar(jobs_list_frame, orient=tk.VERTICAL)
        self.scheduled_jobs_tree = ttk.Treeview(
            jobs_list_frame,
            columns=("File Name", "Time", "File Location"),
            show="headings",
            height=5,
            yscrollcommand=jobs_scrollbar.set
        )
        jobs_scrollbar.config(command=self.scheduled_jobs_tree.yview)
        jobs_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.scheduled_jobs_tree.pack(fill=tk.BOTH, expand=True)

        # Scheduled Jobs Treeview Column Configuration
        self.scheduled_jobs_tree.heading("File Name", text="File Name")
        self.scheduled_jobs_tree.heading("Time", text="Time")
        self.scheduled_jobs_tree.heading("File Location", text="File Location")
        self.scheduled_jobs_tree.column("File Name", width=200)
        self.scheduled_jobs_tree.column("Time", width=100)
        self.scheduled_jobs_tree.column("File Location", width=600)

    def _build_output_panel(self):
        output_frame = ttk.LabelFrame(self.root, text="Output Panel", padding=(10, 5))
        output_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        self.output_panel = scrolledtext.ScrolledText(output_frame, wrap=tk.WORD, height=10, state=tk.DISABLED)
        self.output_panel.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.output_panel.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    def pause_selected_script(self):
        """Pause the selected script."""
        selected = self.script_list_tree.selection()
        if selected:
            for item in selected:
                file_name = self.script_list_tree.item(item, "values")[0]
                self.scheduler.pause_script(file_name)
                logging.info(f"Paused script: {file_name}")
                messagebox.showinfo("Pause Script", f"Script '{file_name}' has been paused.")
            self.refresh_gui()
        else:
            messagebox.showwarning("Warning", "No script selected!")

    def resume_selected_script(self):
        """Resume the selected script."""
        selected = self.script_list_tree.selection()

        if selected:
            for item in selected:
                file_name = self.script_list_tree.item(item, "values")[0]

                # Check if the script is already active
                if not self.scheduler.is_script_paused(file_name):
                    messagebox.showinfo("Resume Script", f"Script '{file_name}' is already active.")
                    return

                # Resume the script
                self.scheduler.resume_script(file_name)  # Call resume_script only once
                messagebox.showinfo("Resume Script", f"Script '{file_name}' has been resumed.")
                self.refresh_gui()  # Update GUI
                return  # Exit after processing one script
        else:
            messagebox.showwarning("Warning", "No script selected!")

    def toggle_dark_mode(self):
        """Toggle between light and dark themes."""
        try:
            if self.dark_mode.get():
                self.style.theme_use("darkly")  # Switch to a dark theme
                logging.info("Switched to Dark Mode")
            else:
                self.style.theme_use("flatly")  # Switch to a light theme
                logging.info("Switched to Light Mode")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to switch themes: {str(e)}")

    def search_scripts(self, query):
        """Filter scripts by file name or path."""
        self.script_list_tree.delete(*self.script_list_tree.get_children())
        for script in self.script_manager.scripts:
            if query.lower() in os.path.basename(script["file_path"]).lower() or query.lower() in script[
                "file_path"].lower():
                file_name = os.path.basename(script["file_path"])
                time_display = script["time"] or "Unscheduled"
                self.script_list_tree.insert("", tk.END, values=(file_name, time_display, script["file_path"]))

    def auto_refresh_gui(self):
        """Automatically refresh the GUI."""
        self.refresh_gui()
        # Determine the current scheduler state and update the status only if it changes
        if self.scheduler.is_running:
            self.update_scheduler_status(SchedulerStatus.RUNNING, color="green")
        else:
            self.update_scheduler_status(SchedulerStatus.NOT_RUNNING, color="red")
        self.root.after(5000, self.auto_refresh_gui)

    def open_log_viewer(self):
        """Open a new window to display logs."""
        log_window = tk.Toplevel(self.root)
        log_window.title("Logs Viewer")
        log_window.geometry("800x600")

        ttk.Label(log_window, text="Execution Logs", font=("Arial", 14)).pack(pady=10)

        # ScrolledText widget to display logs
        log_text = scrolledtext.ScrolledText(log_window, wrap=tk.WORD, width=100, height=30, state=tk.DISABLED)
        log_text.pack(pady=10, padx=10)

        # Refresh logs button
        ttk.Button(log_window, text="Refresh Logs", style="primary.TButton", command=lambda: self.load_logs(log_text)).pack(side=tk.LEFT, padx=10)

        # Clear logs button
        ttk.Button(log_window, text="Clear Logs", style="primary.TButton", command=self.clear_logs).pack(side=tk.LEFT, padx=10)

        # Load logs initially
        self.load_logs(log_text)

    def clear_logs(self):
        """Clear the log file."""
        with open(LOG_FILE, "w") as log_file:
            log_file.write("")
        logging.info("Log file cleared.")
        messagebox.showinfo("Logs", "Log file has been cleared.")

    def load_logs(self, log_text_widget):
        """Load logs asynchronously."""
        thread = threading.Thread(target=self._load_logs, args=(log_text_widget,), daemon=True)
        thread.start()

    def _load_logs(self, log_text_widget):
        """Actual implementation for loading logs."""
        try:
            with open(LOG_FILE, "r") as log_file:
                logs = log_file.read()
            log_text_widget.config(state=tk.NORMAL)
            log_text_widget.delete(1.0, tk.END)
            log_text_widget.insert(tk.END, logs)
            log_text_widget.config(state=tk.DISABLED)
        except FileNotFoundError:
            log_text_widget.config(state=tk.NORMAL)
            log_text_widget.delete(1.0, tk.END)
            log_text_widget.insert(tk.END, "Log file not found. Logs will appear here after execution.")
            log_text_widget.config(state=tk.DISABLED)

    def update_scheduler_status(self, status, color="red"):
        """Update the scheduler status label and button states."""
        if status != self.current_scheduler_status:  # Only update if the status has changed
            self.current_scheduler_status = status  # Update the tracked status
            logging.info(f"Updating scheduler status to: {status}")
            self.scheduler_status.config(text=f"Scheduler Status: {status}", foreground=color)
            self.start_button.config(state=tk.DISABLED if status == SchedulerStatus.RUNNING else tk.NORMAL)
            self.pause_button.config(state=tk.NORMAL if status == SchedulerStatus.RUNNING else tk.DISABLED)
            self.resume_button.config(state=tk.NORMAL if status == SchedulerStatus.PAUSED else tk.DISABLED)

    def update_script_tree(self):
        """Refresh the script list and highlight scheduled scripts."""
        self.script_list_tree.delete(*self.script_list_tree.get_children())
        for script in sorted(self.script_manager.scripts, key=lambda s: s["time"] or "99:99"):
            file_name = os.path.basename(script["file_path"])
            time_display = script["time"] or "Unscheduled"
            frequency_display = script.get("frequency", "Not Set")
            weekday_display = script.get("weekday", "N/A")
            row_id = self.script_list_tree.insert(
                "", tk.END, values=(file_name, time_display, frequency_display, weekday_display, script["file_path"])
            )
            if self.scheduler.is_script_paused(file_name):
                self.script_list_tree.item(row_id, tags=("paused",))
            elif script["time"]:
                self.script_list_tree.item(row_id, tags=("scheduled",))
        self.script_list_tree.tag_configure("scheduled", background="lightgreen")
        self.script_list_tree.tag_configure("paused", background="lightcoral")

    def update_scheduled_jobs_tree(self):
        """Refresh the scheduled jobs list and exclude paused jobs."""
        self.scheduled_jobs_tree.delete(*self.scheduled_jobs_tree.get_children())  # Clear current entries
        for job in sorted(self.scheduler.jobs, key=lambda j: j["time"] or "99:99"):
            file_name = job["file_name"]

            # Skip paused jobs
            if self.scheduler.is_script_paused(file_name):
                continue

            weekday_display = job["weekday"].capitalize() if job["weekday"] else ""
            self.scheduled_jobs_tree.insert(
                "", tk.END,
                values=(file_name, job["time"], f"{job['frequency']} {weekday_display}")
            )

    def start_scheduler(self):
        """Start the scheduler."""
        self.scheduler.start_scheduler()
        if self.scheduler.is_running:
            self.update_scheduler_status(SchedulerStatus.RUNNING, color="green")
            logging.info("Scheduler started.")
        else:
            logging.error("Failed to start scheduler.")

    def pause_scheduler(self):
        """Pause the scheduler."""
        self.scheduler.pause_scheduler()
        self.update_scheduler_status("Paused", color="orange")
        self.pause_button.config(state=tk.DISABLED)
        self.resume_button.config(state=tk.NORMAL)
        logging.info("Scheduler paused.")

    def resume_scheduler(self):
        """Resume the scheduler."""
        self.scheduler.resume_scheduler()
        self.update_scheduler_status("Running", color="green")
        self.resume_button.config(state=tk.DISABLED)
        self.pause_button.config(state=tk.NORMAL)
        logging.info("Scheduler resumed.")

    def clear_scheduled_jobs(self):
        self.scheduler.clear_jobs()
        self.update_scheduler_status("Not Running", color="red")
        self.update_scheduled_jobs_tree()
        messagebox.showinfo("Clear Jobs", "All scheduled jobs have been cleared.")

    def schedule_script(self):
        """Schedule a script to run at a specific time with a specified frequency."""
        selected = self.script_list_tree.selection()
        if selected:
            for item in selected:
                file_name = self.script_list_tree.item(item, "values")[0]
                time = simpledialog.askstring("Schedule Time", "Enter time (HH:MM, 24-hour format):")
                if time:
                    try:
                        datetime.strptime(time, "%H:%M")  # Validate time format

                        # Ask for frequency
                        frequency = simpledialog.askstring(
                            "Frequency",
                            "Enter frequency (daily, weekly):",
                            initialvalue="daily"
                        ).lower()
                        if frequency not in ["daily", "weekly"]:
                            messagebox.showerror("Error", "Invalid frequency. Use 'daily' or 'weekly'.")
                            return

                        # For weekly frequency, ask for the weekday
                        weekday = None
                        if frequency == "weekly":
                            weekday = simpledialog.askstring(
                                "Weekday",
                                "Enter the weekday (e.g., monday, tuesday, ...):",
                                initialvalue="monday"
                            ).lower()
                            if weekday not in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
                                               "sunday"]:
                                messagebox.showerror("Error", "Invalid weekday. Use a valid day of the week.")
                                return

                        # Update script manager and schedule the job
                        if self.script_manager.update_script(file_name, time):
                            script = next((s for s in self.script_manager.scripts if
                                           os.path.basename(s["file_path"]) == file_name), None)
                            if script:
                                script["frequency"] = frequency  # Save frequency
                                script["weekday"] = weekday if frequency == "weekly" else None  # Save weekday if weekly
                                self.script_manager.save_scripts()  # Save changes to scripts.json

                            job_func = partial(self.run_script, file_name=file_name)
                            self.scheduler.add_job_with_frequency(time, job_func, file_name, frequency, weekday)
                            self.refresh_gui()
                            messagebox.showinfo("Success",
                                                f"Scheduled {file_name} at {time} ({frequency} {weekday if weekday else ''}).")
                    except ValueError:
                        messagebox.showerror("Error", "Invalid time format. Please enter time as HH:MM.")
        else:
            messagebox.showwarning("Warning", "No script selected!")

    def add_script(self):
        """Add a new script."""
        file_path = filedialog.askopenfilename(filetypes=[("Python Files", "*.py")])
        if file_path and file_path.endswith(".py") and os.path.exists(file_path):
            if self.script_manager.add_script(file_path):
                self.update_script_tree()
                logging.info(f"Added script: {file_path}")
                messagebox.showinfo("Success", "Script added successfully!")
            else:
                logging.warning(f"Script already exists: {file_path}")
                messagebox.showwarning("Warning", "Script already exists!")
        else:
            logging.error(f"Invalid file selected: {file_path}")
            messagebox.showerror("Error", "Invalid file. Please select a valid Python file.")

    def remove_script(self):
        """Remove the selected script."""
        selected = self.script_list_tree.selection()
        if selected:
            for item in selected:
                file_name = self.script_list_tree.item(item, "values")[0]
                self.script_manager.remove_script(file_name)
            self.update_script_tree()
            messagebox.showinfo("Success", "Selected script(s) removed!")
        else:
            messagebox.showwarning("Warning", "No script selected!")

    def run_now(self):
        """Run the selected script immediately."""
        selected = self.script_list_tree.selection()
        if selected:
            for item in selected:
                file_name = self.script_list_tree.item(item, "values")[0]
                self.run_script(file_name)
        else:
            messagebox.showwarning("Warning", "No script selected!")

    def run_script(self, file_name):
        """Run the specified script asynchronously and display its output."""
        thread = threading.Thread(target=self._run_script, args=(file_name,), daemon=True)
        thread.start()

    def _run_script(self, file_name):
        """Actual implementation for running a script."""
        script = next((s for s in self.script_manager.scripts if os.path.basename(s["file_path"]) == file_name), None)
        if script:
            if not os.path.exists(script["file_path"]):
                logging.error(f"Script not found: {script['file_path']}")
                messagebox.showerror("Error", f"Script not found: {script['file_path']}")
                return
            try:
                process = subprocess.Popen(
                    ["python", script["file_path"]],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                stdout, stderr = process.communicate()
                self.display_output(file_name, stdout, stderr)
                logging.info(f"Executed script: {file_name}")
            except Exception as e:
                error_trace = traceback.format_exc()
                logging.error(f"Failed to run script: {file_name}, Error: {e}\nTraceback: {error_trace}")
                messagebox.showerror("Error", f"Failed to run script: {file_name}. Check logs for details.")

    def display_output(self, file_name, stdout, stderr):
        """Display script output in the output panel."""
        self.output_panel.config(state=tk.NORMAL)
        self.output_panel.delete(1.0, tk.END)
        self.output_panel.insert(tk.END, f"Output for {file_name}:\n\n")
        if stdout:
            self.output_panel.insert(tk.END, f"STDOUT:\n{stdout}\n")
        if stderr:
            self.output_panel.insert(tk.END, f"STDERR:\n{stderr}\n", ("error",))
        self.output_panel.tag_config("error", foreground="red")
        self.output_panel.config(state=tk.DISABLED)



if __name__ == "__main__":
    root = tk.Tk()
    app = SchedulerApp(root)
    root.mainloop()
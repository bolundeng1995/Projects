import tkinter as tk
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
    def __init__(self):
        self.is_running = True
        self.jobs = []
        self.thread = None
        self.lock = Lock()

    def add_job_with_frequency(self, time, job_func, file_name, frequency, weekday=None):
        with self.lock:
            # Check if the job already exists
            for job in self.jobs:
                if job["file_name"] == file_name and job["time"] == time and job["frequency"] == frequency and job[
                    "weekday"] == weekday:
                    logging.warning(f"Job already exists: {file_name} at {time} ({frequency} {weekday or ''})")
                    return
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
            self.jobs.append({"file_name": file_name, "time": time, "frequency": frequency, "weekday": weekday})

    def start_scheduler(self):
        if not self.thread or not self.thread.is_alive():
            self.thread = threading.Thread(target=self.run_scheduler, daemon=True)
            self.thread.start()

    def run_scheduler(self):
        while True:
            if self.is_running:
                schedule.run_pending()
            time.sleep(1)

    def pause_scheduler(self):
        self.is_running = False

    def resume_scheduler(self):
        self.is_running = True

    def clear_jobs(self):
        schedule.clear()
        self.jobs.clear()


class SchedulerApp:
    """Main application class that combines GUI, script manager, and scheduler core."""

    def __init__(self, root):
        self.root = root
        self.script_manager = ScriptManager()
        self.scheduler = SchedulerCore()
        self.init_logging()
        self.build_gui()
        self.schedule_saved_scripts()  # Schedule saved scripts at startup
        self.auto_refresh_gui()

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
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

    def build_gui(self):
        self.root.title("Python Script Scheduler")
        self.root.geometry("1000x700")

        # Search Bar
        search_frame = tk.Frame(self.root)
        search_frame.pack(pady=5)
        tk.Label(search_frame, text="Search:").pack(side=tk.LEFT, padx=5)
        search_entry = tk.Entry(search_frame)
        search_entry.pack(side=tk.LEFT, padx=5)
        tk.Button(search_frame, text="Search", command=lambda: self.search_scripts(search_entry.get())).pack(
            side=tk.LEFT)
        tk.Button(search_frame, text="Reset", command=self.update_script_tree).pack(side=tk.LEFT, padx=5)

        # Script List
        self.script_list_tree = ttk.Treeview(
            self.root,
            columns=("File Name", "Time", "Frequency", "Weekday", "File Location"),
            show="headings",
            height=10
        )
        self.script_list_tree.heading("File Name", text="File Name")
        self.script_list_tree.heading("Time", text="Time")
        self.script_list_tree.heading("Frequency", text="Frequency")
        self.script_list_tree.heading("Weekday", text="Weekday")
        self.script_list_tree.heading("File Location", text="File Location")
        self.script_list_tree.column("File Name", width=150)
        self.script_list_tree.column("Time", width=100)
        self.script_list_tree.column("Frequency", width=100)
        self.script_list_tree.column("Weekday", width=100)
        self.script_list_tree.column("File Location", width=500)
        self.script_list_tree.pack(pady=10)

        # Buttons
        buttons_frame = tk.Frame(self.root)
        buttons_frame.pack(pady=10)
        tk.Button(buttons_frame, text="Add Script", command=self.add_script).grid(row=0, column=0, padx=5)
        tk.Button(buttons_frame, text="Remove Selected", command=self.remove_script).grid(row=0, column=1, padx=5)
        tk.Button(buttons_frame, text="Schedule Selected", command=self.schedule_script).grid(row=0, column=2, padx=5)
        tk.Button(buttons_frame, text="Run Selected Now", command=self.run_now).grid(row=0, column=3, padx=5)
        tk.Button(buttons_frame, text="View Logs", command=self.open_log_viewer).grid(row=0, column=4, padx=5)

        # Scheduler Controls
        self.start_button = tk.Button(buttons_frame, text="Start Scheduler", command=self.start_scheduler)
        self.start_button.grid(row=1, column=0, padx=5)

        self.pause_button = tk.Button(buttons_frame, text="Pause Scheduler", command=self.pause_scheduler,
                                      state=tk.DISABLED)
        self.pause_button.grid(row=1, column=1, padx=5)

        self.resume_button = tk.Button(buttons_frame, text="Resume Scheduler", command=self.resume_scheduler,
                                       state=tk.DISABLED)
        self.resume_button.grid(row=1, column=2, padx=5)

        self.clear_jobs_button = tk.Button(buttons_frame, text="Clear Scheduled Jobs",
                                           command=self.clear_scheduled_jobs)
        self.clear_jobs_button.grid(row=1, column=3, padx=5)

        # Scheduler Status
        self.scheduler_status = tk.Label(self.root, text="Scheduler Status: Not Running", fg="red",
                                         font=("Arial", 12, "bold"))
        self.scheduler_status.pack(pady=5)

        # Scheduled Jobs
        tk.Label(self.root, text="Scheduled Jobs:").pack(pady=5)
        self.scheduled_jobs_tree = ttk.Treeview(self.root, columns=("File Name", "Time", "File Location"),
                                                show="headings", height=5)
        self.scheduled_jobs_tree.heading("File Name", text="File Name")
        self.scheduled_jobs_tree.heading("Time", text="Time")
        self.scheduled_jobs_tree.heading("File Location", text="File Location")
        self.scheduled_jobs_tree.column("File Name", width=200)
        self.scheduled_jobs_tree.column("Time", width=100)
        self.scheduled_jobs_tree.column("File Location", width=600)
        self.scheduled_jobs_tree.pack(pady=10)

        # Add Output Panel
        tk.Label(self.root, text="Output Panel:").pack(pady=5)
        self.output_panel = scrolledtext.ScrolledText(self.root, wrap=tk.WORD, height=10, state=tk.DISABLED)
        self.output_panel.pack(pady=10)

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
        self.root.after(5000, self.auto_refresh_gui)

    def open_log_viewer(self):
        """Open a new window to display logs."""
        log_window = tk.Toplevel(self.root)
        log_window.title("Logs Viewer")
        log_window.geometry("800x600")

        tk.Label(log_window, text="Execution Logs", font=("Arial", 14)).pack(pady=10)

        # ScrolledText widget to display logs
        log_text = scrolledtext.ScrolledText(log_window, wrap=tk.WORD, width=100, height=30, state=tk.DISABLED)
        log_text.pack(pady=10, padx=10)

        # Refresh logs button
        tk.Button(log_window, text="Refresh Logs", command=lambda: self.load_logs(log_text)).pack(side=tk.LEFT, padx=10)

        # Clear logs button
        tk.Button(log_window, text="Clear Logs", command=self.clear_logs).pack(side=tk.LEFT, padx=10)

        # Load logs initially
        self.load_logs(log_text)

    def clear_logs(self):
        """Clear the log file."""
        with open(LOG_FILE, "w") as log_file:
            log_file.write("")
        logging.info("Log file cleared.")
        messagebox.showinfo("Logs", "Log file has been cleared.")

    def load_logs(self, log_text_widget):
        """Load logs from the log file and display them in the ScrolledText widget."""
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
        """Updates the scheduler status label and button states."""
        self.scheduler_status.config(text=f"Scheduler Status: {status}", fg=color)
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
            if script["time"]:
                self.script_list_tree.item(row_id, tags=("scheduled",))
        self.script_list_tree.tag_configure("scheduled", background="lightgreen")

    def update_scheduled_jobs_tree(self):
        """Refresh the scheduled jobs list."""
        self.scheduled_jobs_tree.delete(*self.scheduled_jobs_tree.get_children())
        for job in sorted(self.scheduler.jobs, key=lambda j: j["time"] or "99:99"):
            weekday_display = job["weekday"].capitalize() if job["weekday"] else ""
            self.scheduled_jobs_tree.insert("", tk.END, values=(
                job["file_name"], job["time"], f"{job['frequency']} {weekday_display}"))

    def start_scheduler(self):
        """Start the scheduler."""
        self.scheduler.start_scheduler()
        self.update_scheduler_status("Running", color="green")
        self.start_button.config(state=tk.DISABLED)
        self.pause_button.config(state=tk.NORMAL)
        logging.info("Scheduler started.")

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
        """Run the specified script and display its output."""
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
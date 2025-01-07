import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, Toplevel, scrolledtext
from tkinter import ttk
from datetime import datetime, timedelta
import os
import json
import schedule
import time
import threading
import subprocess

SCRIPT_STORAGE_FILE = "scripts.json"


class PythonScriptScheduler:
    def __init__(self, root):
        self.root = root
        self.root.title("Python Script Scheduler")
        self.root.geometry("900x700")

        self.scripts = []  # List of script dictionaries with file path and time
        self.logs = []  # List of logs for executed scripts
        self.jobs = []  # List of jobs with details
        self.is_scheduler_running = True  # Scheduler state

        # Load saved scripts from storage
        self.load_scripts()

        # GUI Components
        self.script_list_tree = ttk.Treeview(self.root, columns=("File Name", "Time"), show="headings", height=10)
        self.script_list_tree.heading("File Name", text="File Name")
        self.script_list_tree.heading("Time", text="Time")
        self.script_list_tree.column("File Name", width=300)
        self.script_list_tree.column("Time", width=100)
        self.script_list_tree.pack(pady=10)
        self.update_script_tree()  # Populate the Treeview with saved scripts

        self.add_script_button = tk.Button(self.root, text="Add Script", command=self.add_script)
        self.add_script_button.pack(pady=5)

        self.remove_script_button = tk.Button(self.root, text="Remove Selected", command=self.remove_script)
        self.remove_script_button.pack(pady=5)

        self.schedule_script_button = tk.Button(self.root, text="Schedule Selected", command=self.schedule_script)
        self.schedule_script_button.pack(pady=5)

        self.run_now_button = tk.Button(self.root, text="Run Selected Now", command=self.run_now)
        self.run_now_button.pack(pady=5)

        self.start_scheduler_button = tk.Button(self.root, text="Start Scheduler", command=self.start_scheduler)
        self.start_scheduler_button.pack(pady=5)

        self.pause_scheduler_button = tk.Button(self.root, text="Pause Scheduler", command=self.pause_scheduler)
        self.pause_scheduler_button.pack(pady=5)

        self.resume_scheduler_button = tk.Button(self.root, text="Resume Scheduler", command=self.resume_scheduler, state=tk.DISABLED)
        self.resume_scheduler_button.pack(pady=5)

        self.clear_jobs_button = tk.Button(self.root, text="Clear Scheduled Jobs", command=self.clear_scheduled_jobs)
        self.clear_jobs_button.pack(pady=5)

        # Scheduled Jobs Label
        self.scheduled_jobs_label = tk.Label(self.root, text="Scheduled Jobs:")
        self.scheduled_jobs_label.pack(pady=5)

        # Scheduled Jobs Treeview
        self.scheduled_jobs_tree = ttk.Treeview(self.root, columns=("File Name", "Time", "Location"), show="headings",
                                                height=8)
        self.scheduled_jobs_tree.heading("File Name", text="File Name")
        self.scheduled_jobs_tree.heading("Time", text="Scheduled Time")
        self.scheduled_jobs_tree.heading("Location", text="File Location")
        self.scheduled_jobs_tree.column("File Name", width=200)
        self.scheduled_jobs_tree.column("Time", width=100)
        self.scheduled_jobs_tree.column("Location", width=500)
        self.scheduled_jobs_tree.pack(pady=10)

        # Output Panel Label
        self.output_label = tk.Label(self.root, text="Script Output:")
        self.output_label.pack(pady=5)

        # Output Panel
        self.output_panel = scrolledtext.ScrolledText(self.root, wrap=tk.WORD, width=100, height=10, state=tk.DISABLED)
        self.output_panel.pack(pady=10)

        self.show_logs_button = tk.Button(self.root, text="Show Logs", command=self.show_logs)
        self.show_logs_button.pack(pady=5)

        self.about_button = tk.Button(self.root, text="About", command=self.show_about)
        self.about_button.pack(pady=5)

    def load_scripts(self):
        if os.path.exists(SCRIPT_STORAGE_FILE):
            with open(SCRIPT_STORAGE_FILE, "r") as file:
                self.scripts = json.load(file)

    def save_scripts(self):
        with open(SCRIPT_STORAGE_FILE, "w") as file:
            json.dump(self.scripts, file)

    def update_script_tree(self):
        self.script_list_tree.delete(*self.script_list_tree.get_children())
        for script in self.scripts:
            file_name = os.path.basename(script["file_path"])
            time_display = script["time"] if script["time"] else "Not Scheduled"
            self.script_list_tree.insert("", tk.END, values=(file_name, time_display))

    def add_script(self):
        file_path = filedialog.askopenfilename(filetypes=[("Python Files", "*.py")])
        if file_path and not any(script["file_path"] == file_path for script in self.scripts):
            self.scripts.append({"file_path": file_path, "time": None})
            self.save_scripts()
            self.update_script_tree()

    def remove_script(self):
        selected = self.script_list_tree.selection()
        if selected:
            for item in selected:
                file_name = self.script_list_tree.item(item, "values")[0]
                self.scripts = [s for s in self.scripts if os.path.basename(s["file_path"]) != file_name]
            self.save_scripts()
            self.update_script_tree()
        else:
            messagebox.showwarning("Warning", "No script selected")

    def schedule_script(self):
        selected = self.script_list_tree.selection()
        if selected:
            for item in selected:
                script = self.scripts[self.script_list_tree.index(item)]
                if not script["time"]:
                    script["time"] = self.get_valid_time()
                    if not script["time"]:
                        return
                self.schedule_job(script)
                messagebox.showinfo("Success", f"Scheduled {os.path.basename(script['file_path'])} at {script['time']}")
                self.update_script_tree()
        else:
            messagebox.showwarning("Warning", "No script selected")

    def get_valid_time(self):
        while True:
            time_input = simpledialog.askstring("Schedule Time", "Enter time (HH:MM 24-hour format):")
            if not time_input:
                return None
            try:
                hour, minute = map(int, time_input.split(":"))
                if 0 <= hour < 24 and 0 <= minute < 60:
                    return f"{hour:02d}:{minute:02d}"
            except ValueError:
                messagebox.showerror("Error", "Invalid time format. Please enter time as HH:MM.")

    def schedule_job(self, script):
        schedule.every().day.at(script["time"]).do(self.run_script_with_output, script["file_path"])
        file_name = os.path.basename(script["file_path"])
        self.jobs.append({"file_name": file_name, "time": script["time"], "location": script["file_path"]})

    def run_now(self):
        selected = self.script_list_tree.selection()
        if selected:
            for item in selected:
                file_name = self.script_list_tree.item(item, "values")[0]
                script = next(s for s in self.scripts if os.path.basename(s["file_path"]) == file_name)
                self.run_script_with_output(script["file_path"])
        else:
            messagebox.showwarning("Warning", "No script selected")

    def run_script_with_output(self, file_path):
        self.run_script(file_path, capture_output=True)

    def run_script(self, file_path, capture_output=False):
        try:
            process = subprocess.Popen(["python", file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            stdout, stderr = process.communicate()
            if capture_output:
                self.display_output(file_path, stdout, stderr)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_entry = f"{timestamp} - Executed {os.path.basename(file_path)}"
            self.logs.append(log_entry)
            with open("execution_logs.txt", "a") as log_file:
                log_file.write(log_entry + "\n")
            messagebox.showinfo("Success", f"Running {os.path.basename(file_path)}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to run script: {e}")

    def display_output(self, file_path, stdout, stderr):
        self.output_panel.config(state=tk.NORMAL)
        self.output_panel.delete(1.0, tk.END)
        file_name = os.path.basename(file_path)
        self.output_panel.insert(tk.END, f"Output for: {file_name}\n\n")
        if stdout:
            self.output_panel.insert(tk.END, f"Output:\n{stdout}\n")
        if stderr:
            self.output_panel.insert(tk.END, f"Error:\n{stderr}\n")
        self.output_panel.config(state=tk.DISABLED)

    def start_scheduler(self):
        thread = threading.Thread(target=self.scheduler_thread, daemon=True)
        thread.start()
        messagebox.showinfo("Scheduler", "Scheduler started in the background")

    def scheduler_thread(self):
        while True:
            if self.is_scheduler_running:
                schedule.run_pending()
            time.sleep(1)

    def pause_scheduler(self):
        self.is_scheduler_running = False
        self.pause_scheduler_button.config(state=tk.DISABLED)
        self.resume_scheduler_button.config(state=tk.NORMAL)
        messagebox.showinfo("Scheduler", "Scheduler paused")

    def resume_scheduler(self):
        self.is_scheduler_running = True
        self.pause_scheduler_button.config(state=tk.NORMAL)
        self.resume_scheduler_button.config(state=tk.DISABLED)
        messagebox.showinfo("Scheduler", "Scheduler resumed")

    def clear_scheduled_jobs(self):
        self.jobs.clear()
        schedule.clear()
        messagebox.showinfo("Clear Jobs", "All scheduled jobs have been cleared")

    def show_logs(self):
        logs_window = Toplevel(self.root)
        logs_window.title("Execution Logs")
        logs_window.geometry("600x400")
        log_text = scrolledtext.ScrolledText(logs_window, wrap=tk.WORD, width=70, height=20)
        log_text.pack(pady=10)
        log_text.insert(tk.END, "\n".join(self.logs))
        log_text.config(state=tk.DISABLED)

    def show_about(self):
        messagebox.showinfo("About", "Python Script Scheduler\nVersion 1.0\nEnhanced by ChatGPT")


if __name__ == "__main__":
    root = tk.Tk()
    app = PythonScriptScheduler(root)
    root.mainloop()
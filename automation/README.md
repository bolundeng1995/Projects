# Python Script Scheduler

## Overview

The **Python Script Scheduler** is a GUI-based tool built with Python and Tkinter to automate the scheduling, execution, and monitoring of Python scripts. This application allows users to manage their Python scripts efficiently by adding them to a schedule, running them manually, and reviewing their output and logs.

---

## Features

- Add Python scripts to the scheduler.
- Schedule scripts to run at specific times.
- Run scripts immediately without scheduling.
- View and manage scheduled jobs.
- Display output and errors from script execution.
- Pause, resume, and clear all scheduled jobs.
- View execution logs for all scripts.
- Simple and intuitive GUI.

---

## Application Tabs/Buttons and Their Functions

### **1. Script List**
- **Purpose:** Displays a list of all added scripts and their scheduled times.
- **Columns:**
  - **File Name:** The name of the Python script.
  - **Time:** The scheduled time for the script (if applicable).

---

### **2. Buttons**

#### **Add Script**
- **Function:** Add a new Python script to the list for scheduling or manual execution.
- **Usage:** Opens a file dialog to select a Python file (`.py`). The selected file is added to the script list.

#### **Remove Selected**
- **Function:** Remove the selected script(s) from the list.
- **Usage:** Select one or more scripts from the list and click this button to delete them.

#### **Schedule Selected**
- **Function:** Schedule the selected script(s) to run at a specific time.
- **Usage:** Prompts the user to enter a time in the `HH:MM` 24-hour format. The script will then be scheduled to run daily at the specified time.

#### **Run Selected Now**
- **Function:** Immediately execute the selected script(s).
- **Usage:** Select a script from the list and click this button to run it without scheduling.

#### **Start Scheduler**
- **Function:** Start the background scheduler to automatically run scheduled scripts at their specified times.
- **Usage:** This must be clicked to enable automatic script execution.

#### **Pause Scheduler**
- **Function:** Pause the background scheduler temporarily.
- **Usage:** Prevents scheduled jobs from running until resumed. This is useful if you need to make changes or troubleshoot.

#### **Resume Scheduler**
- **Function:** Resume the background scheduler after it has been paused.
- **Usage:** Allows scheduled jobs to resume running as configured.

#### **Clear Scheduled Jobs**
- **Function:** Remove all scheduled jobs.
- **Usage:** Clears the entire list of scheduled jobs and stops their execution.

#### **Show Logs**
- **Function:** Display a new window with execution logs for all scripts.
- **Usage:** Use this to review when each script was executed.

#### **About**
- **Function:** Show information about the application.
- **Usage:** Displays the version and credits for the tool.

---

### **3. Scheduled Jobs Panel**
- **Purpose:** Displays all currently scheduled jobs with the following details:
  - **File Name:** The name of the script.
  - **Scheduled Time:** The time the script is set to run.
  - **File Location:** The full file path to the script.

---

### **4. Output Panel**
- **Purpose:** Displays the output (or errors) of executed scripts.
- **Details:**
  - Shows the name of the script being executed.
  - Displays standard output (stdout) and error messages (stderr).

---

### **Logs**
- **Purpose:** Provides a history of executed scripts.
- **Location:** Logs are displayed in a separate window and saved in a file named `execution_logs.txt`.

---

## Getting Started

### **1. Run the Application**
```bash```
python script_scheduler.py

### **2. Add Scripts**
- **Function:** Add a new Python script to the list for scheduling or manual execution.
- **Usage:** Opens a file dialog to select a Python file (`.py`). The selected file is added to the script list.

### **3. Schedule a Script**
- **Function:** Schedule the selected script(s) to run at a specific time.
- **Usage:**
  1. Select a script from the list and click **"Schedule Selected"**.
  2. Enter the desired time in `HH:MM` format when prompted.
  3. The script will be scheduled to run daily at the specified time.

### **4. Start Scheduler**
- **Function:** Start the background scheduler to automatically run scheduled scripts at their specified times.
- **Usage:** Click **"Start Scheduler"** to enable automatic execution.

### **5. Monitor Jobs**
- **Function:** View and manage all scheduled jobs.
- **Usage:** Use the **"Scheduled Jobs"** panel to:
  - View all active jobs with details like script name, scheduled time, and file location.
  - Monitor the progress of scheduled scripts.

### **6. Pause/Resume Scheduler**
#### **Pause Scheduler**
- **Function:** Pause the background scheduler temporarily.
- **Usage:** Click **"Pause Scheduler"** to prevent scheduled jobs from running until resumed.

#### **Resume Scheduler**
- **Function:** Resume the background scheduler after it has been paused.
- **Usage:** Click **"Resume Scheduler"** to allow scheduled jobs to run as configured.

### **7. Clear Scheduled Jobs**
- **Function:** Remove all scheduled jobs from the scheduler.
- **Usage:** Click **"Clear Scheduled Jobs"** to stop all jobs and clear them from the **"Scheduled Jobs"** panel.

### **8. Run Selected Now**
- **Function:** Immediately execute the selected script(s).
- **Usage:** Select a script from the list and click **"Run Selected Now"** to execute it without scheduling.

### **9. Show Logs**
- **Function:** View the execution history of all scripts.
- **Usage:** Click **"Show Logs"** to open a new window displaying:
  - A timestamp for each executed script.
  - The name of the executed script.

### **10. Output Panel**
- **Purpose:** Displays the output (or errors) of executed scripts.
- **Details:**
  - Shows the name of the script being executed.
  - Displays both standard output (stdout) and error messages (stderr).

### **11. About**
- **Function:** Show information about the application.
- **Usage:** Click **"About"** to display details like:
  - Application name.
  - Version number.
  - Credits.

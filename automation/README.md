# Python Script Scheduler

The **Python Script Scheduler** is a GUI-based tool to schedule and manage Python scripts with flexible options for execution. Users can set scripts to run at specific times daily or weekly and specify weekdays for weekly schedules. The tool saves scheduled tasks to a file (`scripts.json`) and resumes them automatically when restarted.

---

## Features

- Add Python scripts to the scheduler.
- Schedule scripts to run **daily** or **weekly**.
- Specify the exact time and weekday for **weekly** schedules.
- Automatically load and schedule tasks saved in `scripts.json`.
- View and manage scheduled tasks via a user-friendly GUI.
- Logs script outputs and errors for debugging.
- Highlights scheduled scripts for better visibility.

---

## Requirements

- Python 3.7+
- Required Python libraries:
  - `tkinter` (for GUI)
  - `schedule` (for task scheduling)
  - `subprocess` (for script execution)

---

## Installation

1. Clone or download the repository to your local machine.
2. Install required libraries using pip:
   ```bash
   pip install schedule
   
## Usage

1. **Run the Scheduler**:
   ```bash
   python scheduler.py
   
2. **Add a Script**:
   - Click **Add Script** to select a Python script file.

3. **Schedule a Script**:
   - Select a script from the list and click **Schedule Selected**.
   - Enter the time in `HH:MM` format (24-hour clock).
   - Choose a frequency (`daily` or `weekly`).
   - For weekly schedules, specify the weekday (e.g., "Monday").

4. **Start the Scheduler**:
   - Click **Start Scheduler** to run scheduled tasks.

5. **Manage Tasks**:
   - View tasks in the **Scheduled Jobs** section.
   - Remove or reschedule tasks as needed.

6. **Logs and Output**:
   - View script outputs and errors in the **Logs and Output** section.

---

## `scripts.json` Format

Scheduled scripts are saved in `scripts.json`. Example structure:

```json
[
    {
        "file_path": "C:/Scripts/example1.py",
        "time": "14:30",
        "frequency": "daily",
        "weekday": null
    },
    {
        "file_path": "C:/Scripts/example2.py",
        "time": "16:00",
        "frequency": "weekly",
        "weekday": "monday"
    }
]

- **`file_path`**: Path to the Python script.
- **`time`**: Scheduled execution time in `HH:MM` (24-hour format).
- **`frequency`**: Execution frequency (`daily` or `weekly`).
- **`weekday`**: For weekly schedules, specifies the day of the week (e.g., `monday`).

---

## Key Features of the GUI

### Main Script List

- Displays all added scripts with their file paths and scheduled times.
- Highlights scheduled scripts in **green**.

### Scheduled Jobs

- Shows all active schedules with:
  - **File Name**
  - **Execution Time**
  - **Frequency** and **Weekday** (for weekly tasks)

### Buttons

- **Add Script**: Add a Python script to the list.
- **Remove Selected**: Remove a selected script from the scheduler.
- **Schedule Selected**: Schedule a script with specific time and frequency.
- **Start Scheduler**: Start running scheduled tasks.
- **Pause Scheduler**: Pause the scheduler.
- **Resume Scheduler**: Resume the scheduler.
- **Clear Scheduled Jobs**: Clear all schedules.

---

## Troubleshooting

- **Time Format Error**: Ensure the time is entered in `HH:MM` format (24-hour clock).
- **Invalid Weekday**: Use valid weekday names (e.g., `monday`, `tuesday`, etc.) for weekly schedules.
- **Logs Not Displayed**: Check `execution_logs.txt` for detailed logs.

---

## Contribution

Feel free to fork this repository and submit pull requests for enhancements or bug fixes.

---

## License

This project is open-source and licensed under the MIT License.
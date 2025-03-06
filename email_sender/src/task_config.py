from pathlib import Path
from typing import Dict, Any
import json
import logging

class TaskManager:
    def __init__(self):
        self.tasks_dir = Path(__file__).parent.parent / 'tasks'
        self.config_file = self.tasks_dir / 'task_config.json'
        self.setup_directories()
        self.init_config()
        self.logger = logging.getLogger(__name__)
        
    def setup_directories(self) -> None:
        """Create necessary directories if they don't exist"""
        # Create tasks directory
        self.tasks_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        (self.tasks_dir / 'scripts').mkdir(exist_ok=True)
        (self.tasks_dir / 'output').mkdir(exist_ok=True)
        (self.tasks_dir / 'logs').mkdir(exist_ok=True)
        
    def init_config(self) -> None:
        """Initialize config file if it doesn't exist"""
        if not self.config_file.exists():
            self.save_config({})
        
    def save_task_script(self, script_name: str, script_content: str) -> Path:
        """Save a new task script to the scripts directory"""
        script_path = self.tasks_dir / 'scripts' / f"{script_name}.py"
        script_path.write_text(script_content)
        return script_path
        
    def get_task_info(self, task_name: str) -> dict[str, Any]:
        """Get information about a specific task"""
        config = self.load_config()
        return config.get(task_name, {})
        
    def save_task_info(self, task_name: str, info: dict[str, Any]) -> None:
        """Save or update task information"""
        config = self.load_config()
        config[task_name] = info
        self.save_config(config)
        
    def remove_task(self, task_name: str) -> None:
        """Remove a task and its associated files"""
        config = self.load_config()
        
        if task_name in config:
            # Remove script file
            script_path = Path(config[task_name]['script_path'])
            if script_path.exists():
                script_path.unlink()
                
            # Remove from config
            del config[task_name]
            self.save_config(config)
            
    def load_config(self) -> dict[str, Any]:
        """Load task configuration"""
        if self.config_file.exists():
            try:
                return json.loads(self.config_file.read_text())
            except json.JSONDecodeError as e:
                self.logger.error(f"Error loading config: {e}")
                return {}
        return {}
        
    def save_config(self, config: dict[str, Any]) -> None:
        """Save task configuration"""
        try:
            self.config_file.write_text(json.dumps(config, indent=4))
        except Exception as e:
            self.logger.error(f"Error saving config: {e}") 
"""
Create the necessary directory structure for the project.
"""

import os

def create_directory_structure():
    """Create the directory structure for the project."""
    directories = [
        'data/raw',
        'data/processed',
        'notebooks',
        'src',
        'src/factors',
        'src/backtest',
        'src/portfolio',
        'src/visualization',
        'tests',
        'reports/figures'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")
    
    # Create empty __init__.py files in src directories
    init_files = [
        'src/__init__.py',
        'src/factors/__init__.py',
        'src/backtest/__init__.py',
        'src/portfolio/__init__.py',
        'src/visualization/__init__.py'
    ]
    
    for init_file in init_files:
        with open(init_file, 'w') as f:
            f.write("# Initialize module\n")
        print(f"Created file: {init_file}")
    
    print("Directory structure created successfully.")

if __name__ == "__main__":
    create_directory_structure() 
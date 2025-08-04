#!/usr/bin/env python3
"""
Quick Start Script for Freshdesk to Jira Migration Tool
Provides a fast way to test and configure the migration
"""

import sys
import os
from pathlib import Path
import subprocess
import json

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))


def run_command(command, description):
    """Run a command and handle errors"""
    print(f"\n{description}...")
    print(f"Running: {command}")
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print("✓ Success")
            if result.stdout:
                print(result.stdout)
        else:
            print("✗ Failed")
            if result.stderr:
                print(result.stderr)
            return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False
    
    return True


def check_prerequisites():
    """Check if all prerequisites are met"""
    print("="*60)
    print("CHECKING PREREQUISITES")
    print("="*60)
    
    # Check Python version
    python_version = sys.version_info
    if python_version.major < 3 or (python_version.major == 3 and python_version.minor < 8):
        print("✗ Python 3.8 or higher is required")
        return False
    print(f"✓ Python {python_version.major}.{python_version.minor}.{python_version.micro}")
    
    # Check if requirements are installed
    try:
        import requests
        import yaml
        import click
        import tqdm
        import loguru
        print("✓ Required packages are installed")
    except ImportError as e:
        print(f"✗ Missing package: {e}")
        print("Run: pip install -r requirements.txt")
        return False
    
    # Check if configuration exists
    config_file = Path("config/migration_config.yaml")
    if not config_file.exists():
        print("✗ Configuration file not found")
        print("Run: python scripts/setup_migration.py")
        return False
    print("✓ Configuration file found")
    
    return True


def quick_test():
    """Run a quick test of the migration tool"""
    print("\n" + "="*60)
    print("QUICK TEST")
    print("="*60)
    
    # Test validation
    if not run_command("python src/main.py validate", "Testing configuration validation"):
        return False
    
    # Test analysis with small limit
    if not run_command("python src/main.py analyze --limit 10", "Testing data analysis"):
        return False
    
    # Test dry run
    if not run_command("python src/main.py migrate --dry-run", "Testing dry run"):
        return False
    
    return True


def show_status():
    """Show current migration status"""
    print("\n" + "="*60)
    print("CURRENT STATUS")
    print("="*60)
    
    run_command("python src/main.py status", "Checking migration status")


def show_help():
    """Show available commands"""
    print("\n" + "="*60)
    print("AVAILABLE COMMANDS")
    print("="*60)
    
    commands = [
        ("python src/main.py --help", "Show all available commands"),
        ("python src/main.py validate", "Validate configuration and connectivity"),
        ("python src/main.py analyze --limit 100", "Analyze Freshdesk data structure"),
        ("python src/main.py migrate --dry-run", "Test migration without creating data"),
        ("python src/main.py migrate", "Execute full migration"),
        ("python src/main.py status", "Show migration progress"),
        ("python src/main.py resume", "Resume interrupted migration"),
    ]
    
    for command, description in commands:
        print(f"{command:<40} - {description}")


def main():
    """Main quick start function"""
    print("FRESHDESK TO JIRA MIGRATION TOOL - QUICK START")
    print("="*60)
    
    # Check prerequisites
    if not check_prerequisites():
        print("\nPlease fix the issues above before proceeding.")
        return
    
    # Show menu
    while True:
        print("\n" + "="*60)
        print("QUICK START MENU")
        print("="*60)
        print("1. Run quick test")
        print("2. Show current status")
        print("3. Show available commands")
        print("4. Exit")
        
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == "1":
            if quick_test():
                print("\n✓ Quick test completed successfully!")
                print("Your migration tool is ready to use.")
            else:
                print("\n✗ Quick test failed. Please check the errors above.")
        
        elif choice == "2":
            show_status()
        
        elif choice == "3":
            show_help()
        
        elif choice == "4":
            print("\nGoodbye!")
            break
        
        else:
            print("Invalid choice. Please enter 1-4.")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nQuick start cancelled by user.")
    except Exception as e:
        print(f"\nQuick start failed: {e}") 
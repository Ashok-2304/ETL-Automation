"""
🎨 STREAMLIT FRONTEND LAUNCHER
=============================

Simple launcher for the Streamlit frontend dashboard.
Connects to existing backend API server.
"""

import subprocess
import os
import sys

def get_python_executable():
    """Get the Python executable path for the virtual environment"""
    if os.name == 'nt':  # Windows
        return os.path.join("ETL_env", "Scripts", "python.exe")
    else:  # Unix/Linux/Mac
        return os.path.join("ETL_env", "bin", "python")

def main():
    """Launch Streamlit frontend"""
    print("🎨 Starting ETL Automation Dashboard...")
    print("🔗 Make sure backend is running: python api_controller.py")
    print("=" * 50)
    
    python_exe = get_python_executable()
    
    try:
        subprocess.run([
            python_exe, "-m", "streamlit", "run", "streamlit_frontend.py",
            "--server.port", "8501",
            "--browser.gatherUsageStats", "false"
        ])
    except KeyboardInterrupt:
        print("\n👋 Streamlit dashboard stopped.")
    except Exception as e:
        print(f"❌ Error starting dashboard: {e}")

if __name__ == "__main__":
    main()
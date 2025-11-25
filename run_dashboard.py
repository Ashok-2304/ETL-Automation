"""
🚀 FULLSTACK ETL AUTOMATION LAUNCHER
===================================

Launches both the FastAPI backend and Streamlit frontend for complete ETL automation experience.
Professional orchestration of backend API and frontend dashboard.
"""

import subprocess
import time
import sys
import os
import signal
import threading
from pathlib import Path

def get_python_executable():
    """Get the Python executable path for the virtual environment"""
    if os.name == 'nt':  # Windows
        return os.path.join("ETL_env", "Scripts", "python.exe")
    else:  # Unix/Linux/Mac
        return os.path.join("ETL_env", "bin", "python")

def start_backend():
    """Start the FastAPI backend server"""
    print("🔧 Starting Backend API Server...")
    python_exe = get_python_executable()
    
    try:
        backend_process = subprocess.Popen(
            [python_exe, "api_controller.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        print("✅ Backend API started on http://127.0.0.1:8000")
        print("📚 API Documentation: http://127.0.0.1:8000/docs")
        
        return backend_process
    
    except Exception as e:
        print(f"❌ Failed to start backend: {e}")
        return None

def start_frontend():
    """Start the Streamlit frontend"""
    print("🎨 Starting Streamlit Frontend...")
    python_exe = get_python_executable()
    
    # Wait a moment for backend to fully start
    time.sleep(3)
    
    try:
        frontend_process = subprocess.Popen(
            [python_exe, "-m", "streamlit", "run", "streamlit_frontend.py", 
             "--server.port", "8501", "--server.headless", "true"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        print("✅ Frontend Dashboard started on http://localhost:8501")
        print("🎯 Access your ETL Dashboard in the browser!")
        
        return frontend_process
    
    except Exception as e:
        print(f"❌ Failed to start frontend: {e}")
        return None

def cleanup_processes(backend_process, frontend_process):
    """Clean up running processes"""
    print("\\n🧹 Cleaning up processes...")
    
    if backend_process:
        try:
            backend_process.terminate()
            backend_process.wait(timeout=5)
            print("✅ Backend process terminated")
        except:
            backend_process.kill()
            print("🔨 Backend process killed")
    
    if frontend_process:
        try:
            frontend_process.terminate()
            frontend_process.wait(timeout=5)
            print("✅ Frontend process terminated")
        except:
            frontend_process.kill()
            print("🔨 Frontend process killed")

def main():
    """Main launcher function"""
    print("="*60)
    print("🚀 ETL AUTOMATION FULLSTACK LAUNCHER")
    print("="*60)
    print("🎯 Professional ETL Pipeline Control Dashboard")
    print("🔧 Backend: FastAPI REST API")
    print("🎨 Frontend: Streamlit Professional Dashboard")
    print("="*60)
    
    backend_process = None
    frontend_process = None
    
    try:
        # Start backend
        backend_process = start_backend()
        if not backend_process:
            print("❌ Cannot proceed without backend. Exiting.")
            return
        
        # Start frontend
        frontend_process = start_frontend()
        if not frontend_process:
            print("❌ Frontend failed to start, but backend is running.")
            print("🔗 You can still access the API at http://127.0.0.1:8000")
        
        print("\\n" + "="*60)
        print("🎉 ETL AUTOMATION DASHBOARD IS LIVE!")
        print("="*60)
        print("🌐 Dashboard URL: http://localhost:8501")
        print("🔧 Backend API: http://127.0.0.1:8000")
        print("📚 API Docs: http://127.0.0.1:8000/docs")
        print("="*60)
        print("💡 Press Ctrl+C to stop all services")
        print("="*60)
        
        # Keep the script running
        try:
            while True:
                # Check if processes are still running
                if backend_process and backend_process.poll() is not None:
                    print("❌ Backend process died unexpectedly")
                    break
                
                if frontend_process and frontend_process.poll() is not None:
                    print("❌ Frontend process died unexpectedly")
                    break
                
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\\n⏹️ Shutdown signal received...")
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    
    finally:
        cleanup_processes(backend_process, frontend_process)
        print("\\n🏁 ETL Automation services stopped.")
        print("👋 Thank you for using ETL Automation Dashboard!")

if __name__ == "__main__":
    main()
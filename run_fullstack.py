"""
🚀 FULL-STACK STARTUP SCRIPT
============================

Launches both the backend API server and Streamlit frontend for complete
ETL automation dashboard experience.

Usage:
    python run_fullstack.py

This will start:
1. Backend API server on http://localhost:8000
2. Streamlit frontend on http://localhost:8501
"""

import subprocess
import sys
import time
import os
from pathlib import Path

def start_backend():
    """Start the FastAPI backend server"""
    print("🔧 Starting Backend API Server...")
    backend_process = subprocess.Popen([
        sys.executable, "api_controller.py"
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Give backend time to start
    time.sleep(3)
    return backend_process

def start_frontend():
    """Start the Streamlit frontend"""
    print("🎨 Starting Streamlit Frontend...")
    frontend_process = subprocess.Popen([
        sys.executable, "-m", "streamlit", "run", "streamlit_frontend.py",
        "--server.port", "8501",
        "--server.address", "localhost"
    ])
    
    return frontend_process

def main():
    print("🚀 ETL Automation Full-Stack Startup")
    print("=" * 50)
    
    # Change to project directory
    project_dir = Path(__file__).parent
    os.chdir(project_dir)
    
    try:
        # Start backend
        backend_process = start_backend()
        print("✅ Backend API started on http://localhost:8000")
        print("📚 API documentation available at http://localhost:8000/docs")
        
        # Start frontend
        frontend_process = start_frontend()
        print("✅ Frontend dashboard starting on http://localhost:8501")
        
        print("\n🎉 Full-stack application launched successfully!")
        print("🌐 Open http://localhost:8501 in your browser")
        print("⏹️  Press Ctrl+C to stop both servers\n")
        
        # Wait for processes
        try:
            frontend_process.wait()
        except KeyboardInterrupt:
            print("\n🛑 Shutting down servers...")
            
            # Terminate processes
            try:
                frontend_process.terminate()
                backend_process.terminate()
                
                # Wait a bit for graceful shutdown
                time.sleep(2)
                
                # Force kill if still running
                try:
                    frontend_process.kill()
                    backend_process.kill()
                except:
                    pass
                    
            except Exception as e:
                print(f"Error during shutdown: {e}")
            
            print("✅ Servers stopped successfully!")
    
    except Exception as e:
        print(f"❌ Error starting application: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
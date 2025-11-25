#!/usr/bin/env python3
"""
🚀 ETL AUTOMATION STARTUP SCRIPT
================================

This script provides an easy way to start different components of the ETL system.

Options:
1. Run integrated ETL + Mining pipeline
2. Start API server for pipeline control  
3. Launch Streamlit frontend dashboard
4. Run full-stack application (API + Frontend)
5. Run demo integration
6. Install dependencies
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path

def run_pipeline(asins: str, pages: int = 1, headless: bool = True, mining: bool = True, debug: bool = False):
    """Run the integrated ETL + Mining pipeline"""
    print(" Starting ETL + Mining Pipeline...")
    
    python_exe = get_python_executable()
    cmd = [
        python_exe,
        "integrated_etl_pipeline.py",
        "--asins", asins,
        "--pages", str(pages)
    ]
    
    if headless:
        cmd.append("--headless")
    if mining:
        cmd.append("--mining")
    if debug:
        cmd.append("--debug")
    
    print(f"Command: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=get_project_root())

def start_api_server(host: str = "127.0.0.1", port: int = 8000):
    """Start the FastAPI server"""
    print(" Starting API Server...")
    
    python_exe = get_python_executable()
    cmd = [python_exe, "api_controller.py"]
    
    print(f"API will be available at: http://{host}:{port}")
    print(f"API docs will be available at: http://{host}:{port}/docs")
    print("Press Ctrl+C to stop the server")
    
    subprocess.run(cmd, cwd=get_project_root())

def start_streamlit_frontend(host: str = "localhost", port: int = 8501):
    """Start the Streamlit frontend dashboard"""
    print("🎨 Starting Streamlit Frontend Dashboard...")
    
    python_exe = get_python_executable()
    cmd = [
        python_exe, "-m", "streamlit", "run", "streamlit_frontend.py",
        "--server.port", str(port),
        "--server.address", host
    ]
    
    print(f"Frontend will be available at: http://{host}:{port}")
    print("Press Ctrl+C to stop the server")
    
    subprocess.run(cmd, cwd=get_project_root())

def start_fullstack():
    """Start both API server and Streamlit frontend"""
    print("🚀 Starting Full-Stack Application...")
    
    python_exe = get_python_executable()
    cmd = [python_exe, "run_fullstack.py"]
    
    print("This will start both backend API and frontend dashboard")
    subprocess.run(cmd, cwd=get_project_root())

def run_demo():
    """Run the integration demo"""
    print(" Starting Integration Demo...")
    
    python_exe = get_python_executable()
    cmd = [python_exe, "demo_integration.py"]
    
    subprocess.run(cmd, cwd=get_project_root())

def install_dependencies():
    """Install all required dependencies"""
    print(" Installing Dependencies...")
    
    python_exe = get_python_executable()
    pip_exe = python_exe.replace("python.exe", "pip.exe")
    
    # Install Python dependencies
    print("Installing Python packages...")
    subprocess.run([pip_exe, "install", "-r", "requirements.txt"], cwd=get_project_root())
    
    # Install Playwright
    print("Installing Playwright browsers...")
    subprocess.run([python_exe, "-m", "playwright", "install", "chromium"], cwd=get_project_root())
    
    print(" Dependencies installed successfully!")

def get_python_executable():
    """Get the correct Python executable path"""
    project_root = get_project_root()
    venv_python = project_root / "ETL_env" / "Scripts" / "python.exe"
    
    if venv_python.exists():
        return str(venv_python)
    else:
        return sys.executable

def get_project_root():
    """Get the project root directory"""
    return Path(__file__).parent

def show_menu():
    """Show interactive menu"""
    print("\n" + "="*60)
    print("🚀 ETL AUTOMATION & REVIEW MINING SYSTEM")
    print("="*60)
    print("1. Run ETL + Mining Pipeline")
    print("2. Start API Server")  
    print("3. Launch Streamlit Frontend")
    print("4. Start Full-Stack Application")
    print("5. Run Integration Demo")
    print("6. Install Dependencies")
    print("7. Show Recent Results")
    print("8. Exit")
    print("="*60)

def show_recent_results():
    """Show recent pipeline results"""
    print(" Recent Pipeline Results:")
    
    project_root = get_project_root()
    
    # Check for recent files
    directories = {
        "data/reviews": "Raw Reviews",
        "data/processed": "Enhanced Reviews", 
        "data/insights": "Insights Reports"
    }
    
    for dir_name, description in directories.items():
        dir_path = project_root / dir_name
        if dir_path.exists():
            files = list(dir_path.iterdir())
            if files:
                latest_file = max(files, key=lambda f: f.stat().st_mtime)
                size_mb = latest_file.stat().st_size / (1024*1024)
                print(f"    {description}: {latest_file.name} ({size_mb:.2f} MB)")
            else:
                print(f"    {description}: No files")
        else:
            print(f"    {description}: Directory not found")

def main():
    parser = argparse.ArgumentParser(description='ETL Automation Startup Script')
    parser.add_argument('--mode', choices=['pipeline', 'api', 'frontend', 'fullstack', 'demo', 'install', 'interactive'], 
                      help='Mode to run (default: auto-pipeline)')
    parser.add_argument('--asins', type=str, help='ASINs for pipeline mode (optional - uses defaults if not provided)')
    parser.add_argument('--pages', type=int, default=2, help='Pages per product (default: 2)')
    parser.add_argument('--no-headless', action='store_true', help='Run with visible browser')
    parser.add_argument('--no-mining', action='store_true', help='Disable review mining')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    # AUTO-RUN MODE: If no mode specified, automatically run pipeline with defaults
    if not args.mode:
        print(" Starting ETL + Mining Pipeline automatically...")
        print(" Use --mode interactive for menu, or --asins for custom products")
        print("="*60)
        
        # Use default ASINs if none provided
        default_asins = "B0CX59H5W7,B0FHB5V36G,B0F1D9LCK3,B0DDY9HMJG,B0D78X544X"
        asins_to_use = args.asins if args.asins else default_asins
        
        print(f" Configuration:")
        print(f"   ASINs: {asins_to_use}")
        print(f"   Pages per product: {args.pages}")
        print(f"   Headless mode: {not args.no_headless}")
        print(f"   Mining enabled: {not args.no_mining}")
        print(f"   Debug mode: {args.debug}")
        print("="*60)
        
        run_pipeline(
            asins=asins_to_use,
            pages=args.pages,
            headless=not args.no_headless,
            mining=not args.no_mining,
            debug=args.debug
        )
        return
    
    if args.mode == 'pipeline':
        if not args.asins:
            print(" --asins parameter required for explicit pipeline mode")
            sys.exit(1)
        run_pipeline(
            asins=args.asins,
            pages=args.pages,
            headless=not args.no_headless,
            mining=not args.no_mining,
            debug=args.debug
        )
    elif args.mode == 'api':
        start_api_server()
    elif args.mode == 'frontend':
        start_streamlit_frontend()
    elif args.mode == 'fullstack':
        start_fullstack()
    elif args.mode == 'demo':
        run_demo()
    elif args.mode == 'install':
        install_dependencies()
    elif args.mode == 'interactive':
        # Original interactive mode
        while True:
            try:
                show_menu()
                choice = input("Enter your choice (1-8): ").strip()
                
                if choice == '1':
                    print("\n🔧 Pipeline Configuration:")
                    asins = input("Enter ASINs (comma-separated): ").strip()
                    if not asins:
                        print("❌ ASINs required!")
                        continue
                    
                    pages_input = input("Pages per product (default 1): ").strip()
                    pages = int(pages_input) if pages_input else 1
                    
                    headless = input("Run in headless mode? (Y/n): ").strip().lower() != 'n'
                    mining = input("Enable review mining? (Y/n): ").strip().lower() != 'n'
                    debug = input("Enable debug mode? (y/N): ").strip().lower() == 'y'
                    
                    run_pipeline(asins, pages, headless, mining, debug)
                    
                elif choice == '2':
                    start_api_server()
                    
                elif choice == '3':
                    start_streamlit_frontend()
                    
                elif choice == '4':
                    start_fullstack()
                    
                elif choice == '5':
                    run_demo()
                    
                elif choice == '6':
                    install_dependencies()
                    
                elif choice == '7':
                    show_recent_results()
                    
                elif choice == '8':
                    print("👋 Goodbye!")
                    break
                    
                else:
                    print(" Invalid choice. Please try again.")
                    
            except KeyboardInterrupt:
                print("\n\n Goodbye!")
                break
            except Exception as e:
                print(f"\n Error: {e}")

if __name__ == "__main__":
    main()
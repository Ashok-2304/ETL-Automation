"""
🔌 PIPELINE API CONTROLLER
=========================

FastAPI-based REST API for controlling the integrated ETL & Review Mining Pipeline.
This provides REST API endpoints for ETL pipeline management.

Features:
- Start/stop pipeline execution
- Real-time status monitoring
- Data retrieval and insights
- Pipeline configuration
- Error handling and logging
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import json
import logging
import asyncio
from pathlib import Path
from datetime import datetime
import pandas as pd
import uvicorn
import os
import sys

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from integrated_etl_pipeline import IntegratedETLMiningPipeline

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app initialization
app = FastAPI(
    title="ETL Automation & Review Mining API",
    description="REST API for controlling integrated ETL and review mining pipeline",
    version="1.0.0"
)

# CORS middleware for API access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global pipeline instance and status
pipeline_instance = IntegratedETLMiningPipeline()
pipeline_status = {
    "is_running": False,
    "current_task": None,
    "progress": 0,
    "start_time": None,
    "last_error": None,
    "results": None
}

# Pydantic models for API requests
class PipelineConfig(BaseModel):
    asins: List[str]
    pages: int = 1
    headless: bool = True
    mining: bool = True
    debug: bool = False

class ProductConfig(BaseModel):
    product_id: str
    page_limit: int = 1
    headless: bool = True

# API Routes

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "ETL Automation & Review Mining API", "status": "running"}

@app.get("/api/health")
async def health_check():
    """Health check endpoint for API status monitoring"""
    return {"status": "healthy", "message": "API server is running and ready"}

@app.get("/api/status")
async def get_pipeline_status():
    """Get current pipeline execution status"""
    return {
        "status": "idle" if not pipeline_status["is_running"] else "running",
        "current_task": pipeline_status["current_task"],
        "progress": pipeline_status["progress"],
        "start_time": pipeline_status["start_time"],
        "message": pipeline_status.get("last_error", "Ready")
    }

@app.post("/api/etl/run")
async def run_etl_pipeline(config: PipelineConfig, background_tasks: BackgroundTasks):
    """Start ETL pipeline execution"""
    global pipeline_status
    
    if pipeline_status["is_running"]:
        raise HTTPException(status_code=400, detail="Pipeline is already running")
    
    # Reset status
    pipeline_status.update({
        "is_running": True,
        "current_task": "Initializing pipeline",
        "progress": 0,
        "start_time": datetime.now().isoformat(),
        "last_error": None,
        "results": None
    })
    
    # Convert to internal format
    products_config = [
        {
            "product_id": asin,
            "page_limit": config.pages_per_product,
            "headless": config.headless_mode
        }
        for asin in config.asins
    ]
    
    # Start pipeline in background
    background_tasks.add_task(
        execute_pipeline_background,
        products_config,
        config.enable_mining,
        config.enable_debug
    )
    
    return {"message": "Pipeline started successfully", "status": pipeline_status}

@app.post("/api/etl/stop")
async def stop_etl_pipeline():
    """Stop pipeline execution"""
    global pipeline_status
    
    # Note: This is a simplified stop - in production you'd need proper process management
    pipeline_status.update({
        "is_running": False,
        "current_task": "Stopped by user",
        "progress": 100
    })
    
    return {"message": "Pipeline stop requested", "status": pipeline_status}

@app.get("/api/data/latest")
async def get_latest_data():
    """Get the most recent processed data"""
    try:
        # Find latest reviews file
        reviews_dir = Path("data/reviews")
        if reviews_dir.exists():
            parquet_files = list(reviews_dir.glob("*.parquet"))
            if parquet_files:
                latest_file = max(parquet_files, key=os.path.getctime)
                df = pd.read_parquet(latest_file)
                
                # Convert to JSON-serializable format
                return {
                    "data": df.head(100).to_dict('records'),  # Limit for performance
                    "total_records": len(df),
                    "file_path": str(latest_file),
                    "columns": list(df.columns)
                }
        
        return {"data": [], "total_records": 0, "message": "No data available"}
        
    except Exception as e:
        logger.error(f"Error retrieving latest data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/insights/latest")
async def get_latest_insights():
    """Get the most recent insights report"""
    try:
        insights_dir = Path("data/insights")
        if insights_dir.exists():
            json_files = list(insights_dir.glob("*.json"))
            if json_files:
                latest_file = max(json_files, key=os.path.getctime)
                
                with open(latest_file, 'r', encoding='utf-8') as f:
                    insights = json.load(f)
                
                return {
                    "insights": insights,
                    "file_path": str(latest_file),
                    "generated_at": insights.get("overall_statistics", {}).get("generation_time", "Unknown")
                }
        
        return {"insights": {}, "message": "No insights available"}
        
    except Exception as e:
        logger.error(f"Error retrieving latest insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/summary")
async def get_analytics_summary():
    """Get analytics summary for dashboard"""
    try:
        # Get latest insights
        insights_response = await get_latest_insights()
        insights = insights_response.get("insights", {})
        
        # Extract key metrics
        overall_stats = insights.get("overall_statistics", {})
        sentiment_analysis = insights.get("sentiment_analysis", {})
        content_analysis = insights.get("content_analysis", {})
        
        summary = {
            "total_reviews": overall_stats.get("total_reviews", 0),
            "average_rating": overall_stats.get("average_rating", 0),
            "sentiment_score": overall_stats.get("average_sentiment", 0),
            "positive_reviews_pct": sentiment_analysis.get("positive_reviews_pct", 0),
            "negative_reviews_pct": sentiment_analysis.get("negative_reviews_pct", 0),
            "avg_review_length": content_analysis.get("avg_review_length", 0),
            "reviews_with_emojis": content_analysis.get("reviews_with_emojis", 0),
            "last_updated": datetime.now().isoformat()
        }
        
        return summary
        
    except Exception as e:
        logger.error(f"Error generating analytics summary: {e}")
        return {
            "total_reviews": 0,
            "average_rating": 0,
            "sentiment_score": 0,
            "positive_reviews_pct": 0,
            "negative_reviews_pct": 0,
            "avg_review_length": 0,
            "reviews_with_emojis": 0,
            "last_updated": datetime.now().isoformat(),
            "error": str(e)
        }

@app.get("/api/files/list")
async def list_data_files():
    """List available data files"""
    try:
        files_info = []
        
        # Check different directories
        directories = ["data/reviews", "data/processed", "data/insights"]
        
        for dir_path in directories:
            dir_obj = Path(dir_path)
            if dir_obj.exists():
                for file_path in dir_obj.iterdir():
                    if file_path.is_file():
                        stat = file_path.stat()
                        files_info.append({
                            "name": file_path.name,
                            "path": str(file_path),
                            "size": stat.st_size,
                            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                            "type": file_path.suffix
                        })
        
        return {"files": files_info}
        
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/logs/recent")
async def get_recent_logs():
    """Get recent pipeline logs"""
    try:
        log_file = Path("data/logs/integrated_pipeline.log")
        if log_file.exists():
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Return last 50 lines
            recent_lines = lines[-50:] if len(lines) > 50 else lines
            
            return {
                "logs": [line.strip() for line in recent_lines],
                "total_lines": len(lines)
            }
        
        return {"logs": [], "message": "No log file found"}
        
    except Exception as e:
        logger.error(f"Error reading logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Background task function
async def execute_pipeline_background(products_config: List[Dict], enable_mining: bool, enable_debug: bool):
    """Execute pipeline in background task"""
    global pipeline_status
    
    try:
        pipeline_status["current_task"] = "Starting ETL extraction"
        pipeline_status["progress"] = 10
        
        # Execute ETL
        etl_success, data_file_path, extraction_results = pipeline_instance.run_etl_extraction(
            products_config, enable_debug
        )
        
        if not etl_success:
            raise Exception("ETL extraction failed")
        
        pipeline_status["current_task"] = "ETL extraction completed"
        pipeline_status["progress"] = 50
        
        # Execute mining if enabled
        mining_success = True
        insights_path = None
        
        if enable_mining:
            pipeline_status["current_task"] = "Running review mining analysis"
            pipeline_status["progress"] = 70
            
            mining_success, enhanced_data_path, insights_path = pipeline_instance.run_review_mining(
                data_file_path
            )
        
        pipeline_status["current_task"] = "Generating final report"
        pipeline_status["progress"] = 90
        
        # Generate summary
        pipeline_instance.generate_summary_report(extraction_results, insights_path)
        
        # Complete
        pipeline_status.update({
            "is_running": False,
            "current_task": "Completed successfully",
            "progress": 100,
            "results": {
                "etl_success": etl_success,
                "mining_success": mining_success,
                "data_file": data_file_path,
                "insights_file": insights_path,
                "extraction_results": extraction_results
            }
        })
        
        return {"message": "Pipeline completed", "results": pipeline_status["results"]}
    
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        pipeline_status.update({
            "is_running": False,
            "current_task": "Failed",
            "progress": 0,
            "last_error": str(e)
        })
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    
    print("🚀 Starting ETL Automation API Server...")
    print("📍 Server will be available at: http://localhost:8000")
    print("📚 API docs will be available at: http://localhost:8000/docs")
    print("💡 Press Ctrl+C to stop the server")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=True
    )
"""
🔌 SIMPLE API CONTROLLER
========================

Simple FastAPI-based REST API for ETL pipeline control.
This provides basic endpoints without complex dependencies.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import json
import logging
from datetime import datetime
import uvicorn

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app initialization
app = FastAPI(
    title="ETL Automation API",
    description="Simple REST API for ETL pipeline control",
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

# Request models
class PipelineConfig(BaseModel):
    asins: List[str]
    pages: int = 1
    headless: bool = True
    mining: bool = True
    debug: bool = False

# Global pipeline status
pipeline_status = {
    "is_running": False,
    "current_task": "Ready to start",
    "progress": 0,
    "start_time": None,
    "last_error": None
}

# Mock data for testing
mock_reviews = [
    {
        "asin": "B0CX59H5W7",
        "product_name": "OnePlus Nord CE4 (Celadon Marble, 8GB RAM, 128GB Storage)",
        "title": "Excellent phone with great camera",
        "rating": 5,
        "content": "Amazing phone with excellent camera quality and smooth performance. Highly recommended!",
        "review_date": "2024-11-15"
    },
    {
        "asin": "B0FHB5V36G", 
        "product_name": "iQOO Z10R 5G (Aquamarine, 8GB RAM, 128GB Storage)",
        "title": "Good value for money",
        "rating": 4,
        "content": "Decent phone with good features. Camera could be better but overall satisfied.",
        "review_date": "2024-11-14"
    }
]

mock_insights = {
    "overall_statistics": {
        "total_reviews": 90,
        "average_rating": 4.2,
        "average_sentiment": 0.65,
        "average_quality_score": 0.78
    },
    "sentiment_analysis": {
        "positive_reviews_pct": 65.5,
        "negative_reviews_pct": 15.2, 
        "neutral_reviews_pct": 19.3
    },
    "emotion_analysis": {
        "joy": {"total_mentions": 45},
        "trust": {"total_mentions": 32},
        "satisfaction": {"total_mentions": 28}
    },
    "product_aspects": {
        "camera": {"mentions": 34, "avg_sentiment": 0.8},
        "battery": {"mentions": 28, "avg_sentiment": 0.7},
        "performance": {"mentions": 22, "avg_sentiment": 0.75}
    }
}

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "ETL Automation API", "status": "running", "timestamp": datetime.now().isoformat()}

@app.get("/api/health")
async def health_check():
    """Health check endpoint for API status monitoring"""
    return {"status": "healthy", "message": "API server is running and ready"}

@app.get("/api/status")
async def get_pipeline_status():
    """Get current pipeline execution status"""
    return {
        "status": "running" if pipeline_status["is_running"] else "idle",
        "current_task": pipeline_status["current_task"],
        "progress": pipeline_status["progress"],
        "start_time": pipeline_status["start_time"],
        "message": pipeline_status.get("last_error", "Ready")
    }

@app.post("/api/etl/run")
async def start_pipeline(config: PipelineConfig):
    """Start ETL pipeline with given configuration"""
    global pipeline_status
    
    if pipeline_status["is_running"]:
        raise HTTPException(status_code=400, detail="Pipeline is already running")
    
    pipeline_status.update({
        "is_running": True,
        "current_task": f"Processing {len(config.asins)} products",
        "progress": 25,
        "start_time": datetime.now().isoformat(),
        "last_error": None
    })
    
    logger.info(f"Pipeline started with ASINs: {config.asins}")
    
    return {
        "message": "Pipeline started successfully",
        "task_id": f"task_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "config": config.dict()
    }

@app.post("/api/etl/stop")
async def stop_pipeline():
    """Stop running pipeline"""
    global pipeline_status
    
    pipeline_status.update({
        "is_running": False,
        "current_task": "Pipeline stopped",
        "progress": 0,
        "start_time": None,
        "last_error": None
    })
    
    logger.info("Pipeline stopped by user request")
    return {"message": "Pipeline stopped successfully"}

@app.get("/api/data/latest")
async def get_latest_reviews():
    """Get latest processed review data"""
    return mock_reviews

@app.get("/api/insights/latest")
async def get_latest_insights():
    """Get latest insights and analytics"""
    return mock_insights

@app.get("/api/data/files")
async def get_data_files():
    """Get list of available data files"""
    return {
        "reviews": ["reviews_20241124_120000.parquet"],
        "processed": ["enhanced_reviews_20241124_120000.csv"],
        "insights": ["review_insights_20241124_120000.json"]
    }

if __name__ == "__main__":
    print("🚀 Starting ETL Automation API Server...")
    print("📍 Server will be available at: http://localhost:8000")
    print("📚 API docs will be available at: http://localhost:8000/docs")
    print("💡 Press Ctrl+C to stop the server")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
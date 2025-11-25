"""
🎯 ETL AUTOMATION STREAMLIT FRONTEND
===================================

Professional Streamlit dashboard for ETL automation and review mining pipeline.
Clean, modern interface with real-time monitoring and control capabilities.

Features:
- Pipeline control and monitoring
- Real-time status updates
- Data visualization and insights
- Clean, professional UI design
- Backend API integration
"""

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import time
from datetime import datetime
import os
from pathlib import Path

# Page configuration
st.set_page_config(
    page_title="ETL Automation Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        background: linear-gradient(90deg, #1f77b4, #ff7f0e);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    
    .status-running {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        padding: 0.5rem 1rem;
        border-radius: 20px;
        color: white;
        font-weight: bold;
    }
    
    .status-idle {
        background: linear-gradient(135deg, #fd746c 0%, #ff9068 100%);
        padding: 0.5rem 1rem;
        border-radius: 20px;
        color: white;
        font-weight: bold;
    }
    
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #2c3e50 0%, #34495e 100%);
    }
    
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.5rem 2rem;
        font-weight: bold;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 5px 15px rgba(0,0,0,0.2);
    }
</style>
""", unsafe_allow_html=True)

# Backend API configuration
API_BASE_URL = "http://localhost:8000"

class ETLDashboard:
    def __init__(self):
        self.api_url = API_BASE_URL
        
    def check_backend_connection(self):
        """Check if backend API is available"""
        try:
            response = requests.get(f"{self.api_url}/api/health", timeout=5)
            return response.status_code == 200
        except:
            return False
    
    def get_pipeline_status(self):
        """Get current pipeline status from backend"""
        try:
            response = requests.get(f"{self.api_url}/api/status", timeout=5)
            if response.status_code == 200:
                return response.json()
            return None
        except:
            return None
    
    def start_pipeline(self, config):
        """Start ETL pipeline with given configuration"""
        try:
            response = requests.post(f"{self.api_url}/api/etl/run", json=config, timeout=10)
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            st.error(f"Failed to start pipeline: {e}")
            return None
    
    def stop_pipeline(self):
        """Stop running pipeline"""
        try:
            response = requests.post(f"{self.api_url}/api/etl/stop", timeout=5)
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            st.error(f"Failed to stop pipeline: {e}")
            return None
    
    def get_latest_data(self):
        """Get latest processed data"""
        try:
            response = requests.get(f"{self.api_url}/api/data/latest", timeout=5)
            return response.json() if response.status_code == 200 else []
        except:
            return []
    
    def get_latest_insights(self):
        """Get latest insights and analytics"""
        try:
            response = requests.get(f"{self.api_url}/api/insights/latest", timeout=5)
            return response.json() if response.status_code == 200 else {}
        except:
            return {}

def main():
    dashboard = ETLDashboard()
    
    # Header
    st.markdown('<h1 class="main-header">🚀 ETL Automation Dashboard</h1>', unsafe_allow_html=True)
    
    # Sidebar for controls
    with st.sidebar:
        st.header("🎛️ Pipeline Controls")
        
        # Backend connection status
        backend_status = dashboard.check_backend_connection()
        if backend_status:
            st.success("🟢 Backend Connected")
        else:
            st.error("🔴 Backend Disconnected")
            st.warning("Please start the backend API server:\n`python api_controller.py`")
            return
        
        st.divider()
        
        # Pipeline Configuration
        st.subheader("📋 Configuration")
        
        # ASIN input
        asin_input = st.text_area(
            "Product ASINs (comma-separated)",
            value="B0CX59H5W7,B0FHB5V36G,B0F1D9LCK3",
            help="Enter Amazon product ASINs separated by commas"
        )
        
        # Number of pages
        pages = st.slider(
            "Pages per product", 
            min_value=1, 
            max_value=5, 
            value=2,
            help="Number of review pages to scrape per product"
        )
        
        # Advanced options
        with st.expander("⚙️ Advanced Options"):
            headless = st.checkbox("Headless mode", value=True, help="Run browser in headless mode")
            mining = st.checkbox("Enable mining analysis", value=True, help="Perform NLP analysis on reviews")
            debug = st.checkbox("Debug mode", value=False, help="Enable debug mode with screenshots")
        
        st.divider()
        
        # Pipeline controls
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🚀 Start Pipeline", use_container_width=True):
                asins = [asin.strip() for asin in asin_input.split(",") if asin.strip()]
                if asins:
                    config = {
                        "asins": asins,
                        "pages": pages,
                        "headless": headless,
                        "mining": mining,
                        "debug": debug
                    }
                    result = dashboard.start_pipeline(config)
                    if result:
                        st.success("Pipeline started successfully!")
                        st.rerun()
                    else:
                        st.error("Failed to start pipeline")
                else:
                    st.error("Please enter at least one ASIN")
        
        with col2:
            if st.button("🛑 Stop Pipeline", use_container_width=True):
                result = dashboard.stop_pipeline()
                if result:
                    st.success("Pipeline stopped successfully!")
                    st.rerun()
                else:
                    st.error("Failed to stop pipeline")
    
    # Main content area
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Status", "📈 Analytics", "📋 Data", "⚙️ Logs"])
    
    with tab1:
        # Pipeline Status
        status = dashboard.get_pipeline_status()
        
        if status:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                status_text = status.get('status', 'unknown')
                if status_text == 'running':
                    st.markdown(f'<div class="status-running">🏃‍♂️ {status_text.upper()}</div>', unsafe_allow_html=True)
                else:
                    st.markdown(f'<div class="status-idle">💤 {status_text.upper()}</div>', unsafe_allow_html=True)
            
            with col2:
                st.metric(
                    "Progress", 
                    f"{status.get('progress', 0)}%",
                    help="Current pipeline progress"
                )
            
            with col3:
                current_task = status.get('current_task', 'Ready')
                st.metric(
                    "Current Task",
                    current_task if len(current_task) < 20 else current_task[:17] + "...",
                    help=current_task
                )
            
            # Progress bar
            if status.get('progress', 0) > 0:
                st.progress(status.get('progress', 0) / 100)
            
            # Detailed status
            st.subheader("📋 Detailed Status")
            status_df = pd.DataFrame([
                {"Property": "Status", "Value": status.get('status', 'unknown')},
                {"Property": "Current Task", "Value": status.get('current_task', 'Ready')},
                {"Property": "Progress", "Value": f"{status.get('progress', 0)}%"},
                {"Property": "Start Time", "Value": status.get('start_time', 'Not started')},
                {"Property": "Message", "Value": status.get('message', 'No message')}
            ])
            st.dataframe(status_df, use_container_width=True, hide_index=True)
    
    with tab2:
        # Analytics and Insights
        st.subheader("📊 Review Analytics")
        
        insights = dashboard.get_latest_insights()
        
        if insights and 'overall_statistics' in insights:
            # Key metrics
            stats = insights['overall_statistics']
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Reviews", stats.get('total_reviews', 0))
            with col2:
                st.metric("Avg Rating", f"{stats.get('average_rating', 0):.1f}")
            with col3:
                st.metric("Avg Sentiment", f"{stats.get('average_sentiment', 0):.2f}")
            with col4:
                st.metric("Quality Score", f"{stats.get('average_quality_score', 0):.2f}")
            
            # Sentiment distribution
            if 'sentiment_analysis' in insights:
                sentiment_data = insights['sentiment_analysis']
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Pie chart for sentiment distribution
                    labels = ['Positive', 'Negative', 'Neutral']
                    values = [
                        sentiment_data.get('positive_reviews_pct', 0),
                        sentiment_data.get('negative_reviews_pct', 0),
                        sentiment_data.get('neutral_reviews_pct', 0)
                    ]
                    
                    fig_pie = px.pie(
                        values=values,
                        names=labels,
                        title="Sentiment Distribution",
                        color_discrete_sequence=['#00cc96', '#ef553b', '#636efa']
                    )
                    fig_pie.update_layout(height=400)
                    st.plotly_chart(fig_pie, use_container_width=True)
                
                with col2:
                    # Product aspects analysis
                    if 'product_aspects' in insights:
                        aspects = insights['product_aspects']
                        aspect_names = list(aspects.keys())
                        aspect_sentiments = [aspects[name]['avg_sentiment'] for name in aspect_names]
                        aspect_mentions = [aspects[name]['mentions'] for name in aspect_names]
                        
                        fig_aspects = go.Figure()
                        fig_aspects.add_trace(go.Scatter(
                            x=aspect_mentions,
                            y=aspect_sentiments,
                            mode='markers+text',
                            text=aspect_names,
                            textposition='top center',
                            marker=dict(size=15, color='#1f77b4'),
                            name='Product Aspects'
                        ))
                        
                        fig_aspects.update_layout(
                            title="Product Aspects: Mentions vs Sentiment",
                            xaxis_title="Number of Mentions",
                            yaxis_title="Average Sentiment",
                            height=400
                        )
                        st.plotly_chart(fig_aspects, use_container_width=True)
            
            # Emotion analysis
            if 'emotion_analysis' in insights:
                emotions = insights['emotion_analysis']
                emotion_names = list(emotions.keys())
                emotion_counts = [emotions[name]['total_mentions'] for name in emotion_names]
                
                fig_emotions = px.bar(
                    x=emotion_names,
                    y=emotion_counts,
                    title="Emotion Analysis",
                    color=emotion_counts,
                    color_continuous_scale='viridis'
                )
                fig_emotions.update_layout(height=400)
                st.plotly_chart(fig_emotions, use_container_width=True)
        
        else:
            st.info("No analytics data available. Run the pipeline to generate insights.")
    
    with tab3:
        # Latest Data
        st.subheader("📋 Latest Review Data")
        
        data = dashboard.get_latest_data()
        
        if data:
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Display summary
            st.write(f"**Total Reviews:** {len(df)}")
            
            # Display data table
            st.dataframe(df, use_container_width=True)
            
            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"reviews_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        else:
            st.info("No review data available. Run the pipeline to extract reviews.")
    
    with tab4:
        # Logs and System Info
        st.subheader("📊 System Information")
        
        # Check data files
        data_dir = Path("data")
        if data_dir.exists():
            col1, col2, col3 = st.columns(3)
            
            with col1:
                reviews_files = list((data_dir / "reviews").glob("*.parquet")) if (data_dir / "reviews").exists() else []
                st.metric("Review Files", len(reviews_files))
            
            with col2:
                processed_files = list((data_dir / "processed").glob("*.csv")) if (data_dir / "processed").exists() else []
                st.metric("Processed Files", len(processed_files))
            
            with col3:
                insight_files = list((data_dir / "insights").glob("*.json")) if (data_dir / "insights").exists() else []
                st.metric("Insight Files", len(insight_files))
        
        # Recent files
        st.subheader("📁 Recent Files")
        if data_dir.exists():
            all_files = []
            for subdir in ['reviews', 'processed', 'insights']:
                subpath = data_dir / subdir
                if subpath.exists():
                    files = list(subpath.glob("*"))
                    for file in files:
                        all_files.append({
                            "File": file.name,
                            "Type": subdir,
                            "Size": f"{file.stat().st_size / 1024:.1f} KB",
                            "Modified": datetime.fromtimestamp(file.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                        })
            
            if all_files:
                files_df = pd.DataFrame(all_files)
                files_df = files_df.sort_values("Modified", ascending=False)
                st.dataframe(files_df, use_container_width=True, hide_index=True)
            else:
                st.info("No data files found.")

    # Auto-refresh for real-time updates
    if st.sidebar.checkbox("🔄 Auto-refresh (5s)", value=False):
        time.sleep(5)
        st.rerun()

if __name__ == "__main__":
    main()
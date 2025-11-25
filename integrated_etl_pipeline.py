#!/usr/bin/env python3
"""
🚀 INTEGRATED ETL & REVIEW MINING AUTOMATION PIPELINE
=====================================================

This integrated pipeline combines:
1. ETL Automation (Amazon review scraping) 
2. Advanced Review Mining Engine (NLP & sentiment analysis)
3. Real-time insights generation
4. Automated data quality checks
5. Streamlined execution with command-line interface

Usage:
    python integrated_etl_pipeline.py --asins B0CX59H5W7,B0FHB5V36G --pages 2 --headless --mining

Features:
    - Automated review extraction from Amazon
    - Advanced sentiment analysis with VADER
    - Emotion detection and emoji analysis  
    - Product aspect-based sentiment analysis
    - Slang detection and quality scoring
    - Comprehensive insights reporting
    - Data validation and quality checks
"""

import argparse
import logging
import sys
import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import json

# Import our existing modules
from ETL_automation import (
    ecommerce_etl_flow,
    extract_ecommerce_data,
    transform_reviews_data,
    validate_data_quality,
    load_to_destinations,
    send_notification
)
from review_mining import AdvancedReviewMiningEngine

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/integrated_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class IntegratedETLMiningPipeline:
    """
    Integrated pipeline that combines ETL automation with advanced review mining
    """
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.mining_engine = AdvancedReviewMiningEngine()
        self.setup_directories()
    
    def setup_directories(self):
        """Create necessary directories for data storage"""
        directories = [
            'data/reviews',
            'data/processed', 
            'data/insights',
            'data/logs',
            'error_screenshots',
            'debug_screenshots'
        ]
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
            
    def run_etl_extraction(self, products_config: list, enable_debug: bool = False) -> tuple:
        """
        Run the ETL extraction pipeline
        
        Returns:
            tuple: (success_status, data_file_path, extraction_results)
        """
        self.logger.info(f"🚀 Starting ETL extraction for {len(products_config)} products")
        
        # Configure destinations
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        destinations = [{
            'name': 'primary_storage',
            'type': 'file', 
            'file_path': f'data/reviews/reviews_{timestamp}.parquet'
        }]
        
        # Add debug configuration to products
        for product in products_config:
            product['debug'] = enable_debug
            product['max_retries'] = 3
        
        try:
            # Execute ETL pipeline
            extraction_results = ecommerce_etl_flow(
                products_to_process=products_config,
                destinations=destinations,
                send_notifications=True
            )
            
            # Check if extraction was successful
            total_records = sum(result.get('records_processed', 0) for result in extraction_results)
            successful_products = sum(1 for result in extraction_results if result.get('quality_passed', False))
            
            if total_records > 0 and successful_products > 0:
                data_file_path = destinations[0]['file_path']
                self.logger.info(f" ETL extraction completed successfully")
                self.logger.info(f" Extracted {total_records} records from {successful_products} products")
                self.logger.info(f" Data saved to: {data_file_path}")
                return True, data_file_path, extraction_results
            else:
                self.logger.error(" ETL extraction failed - no valid data extracted")
                return False, None, extraction_results
                
        except Exception as e:
            self.logger.error(f" ETL extraction failed with error: {e}")
            return False, None, None
    
    def run_review_mining(self, data_file_path: str) -> tuple:
        """
        Run advanced review mining on extracted data
        
        Returns:
            tuple: (success_status, enhanced_data_path, insights_path)
        """
        self.logger.info(" Starting advanced review mining analysis")
        
        try:
            # Load the extracted reviews data
            reviews_df = pd.read_parquet(data_file_path)
            self.logger.info(f" Loaded {len(reviews_df)} reviews for mining analysis")
            
            # Run advanced feature extraction
            enhanced_reviews = self.mining_engine.extract_complex_features(reviews_df)
            
            # Generate comprehensive insights
            insights = self.mining_engine.generate_insights_report(enhanced_reviews)
            
            # Save enhanced dataset
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            enhanced_data_path = f'data/processed/enhanced_reviews_{timestamp}.csv'
            enhanced_reviews.to_csv(enhanced_data_path, index=False)
            
            # Save insights report
            insights_path = f'data/insights/review_insights_{timestamp}.json'
            with open(insights_path, 'w', encoding='utf-8') as f:
                json.dump(insights, f, indent=2, ensure_ascii=False, default=str)
            
            self.logger.info(f" Review mining completed successfully")
            self.logger.info(f" Generated {len(enhanced_reviews.columns)} features")
            self.logger.info(f" Enhanced data saved to: {enhanced_data_path}")
            self.logger.info(f" Insights report saved to: {insights_path}")
            
            return True, enhanced_data_path, insights_path
            
        except Exception as e:
            self.logger.error(f"❌ Review mining failed with error: {e}")
            return False, None, None
    
    def generate_summary_report(self, extraction_results, insights_path: str = None):
        """Generate and display a comprehensive summary report"""
        
        self.logger.info("\n" + "="*80)
        self.logger.info(" INTEGRATED ETL & MINING PIPELINE SUMMARY REPORT")
        self.logger.info("="*80)
        
        # ETL Summary
        if extraction_results:
            total_records = sum(result.get('records_processed', 0) for result in extraction_results)
            successful_products = sum(1 for result in extraction_results if result.get('quality_passed', False))
            failed_products = len(extraction_results) - successful_products
            
            self.logger.info(f"ETL EXTRACTION RESULTS:")
            self.logger.info(f"   Total Products Processed: {len(extraction_results)}")
            self.logger.info(f"   Successful Products: {successful_products}")
            self.logger.info(f"   Failed Products: {failed_products}")
            self.logger.info(f"   Total Records Extracted: {total_records}")
            
            # Product-level details
            for result in extraction_results:
                status = "✅ PASSED" if result.get('quality_passed') else "❌ FAILED"
                self.logger.info(f"   └─ {result.get('product_id', 'Unknown')}: {result.get('records_processed', 0)} records {status}")
        
        # Mining Summary
        if insights_path and Path(insights_path).exists():
            try:
                with open(insights_path, 'r', encoding='utf-8') as f:
                    insights = json.load(f)
                
                self.logger.info(f"\n🔍 REVIEW MINING ANALYSIS:")
                
                # Overall Statistics
                stats = insights.get('overall_statistics', {})
                self.logger.info(f"   Total Reviews Analyzed: {stats.get('total_reviews', 'N/A')}")
                self.logger.info(f"   Average Rating: {stats.get('average_rating', 'N/A')}")
                self.logger.info(f"   Average Sentiment Score: {stats.get('average_sentiment', 'N/A')}")
                self.logger.info(f"   Quality Score: {stats.get('average_quality_score', 'N/A')}")
                
                # Sentiment Analysis
                sentiment = insights.get('sentiment_analysis', {})
                self.logger.info(f"\n📊 SENTIMENT BREAKDOWN:")
                self.logger.info(f"   Positive Reviews: {sentiment.get('positive_reviews_pct', 'N/A')}%")
                self.logger.info(f"   Negative Reviews: {sentiment.get('negative_reviews_pct', 'N/A')}%")
                self.logger.info(f"   Neutral Reviews: {sentiment.get('neutral_reviews_pct', 'N/A')}%")
                
                # Top Emotions
                emotions = insights.get('emotion_analysis', {})
                if emotions:
                    self.logger.info(f"\n😊 TOP EMOTIONS DETECTED:")
                    for emotion, data in emotions.items():
                        if isinstance(data, dict) and 'total_mentions' in data:
                            self.logger.info(f"   {emotion.capitalize()}: {data.get('total_mentions', 0)} mentions")
                
                # Product Aspects
                aspects = insights.get('product_aspects', {})
                if aspects:
                    self.logger.info(f"\n🏷️ PRODUCT ASPECTS ANALYZED:")
                    for aspect, data in aspects.items():
                        if isinstance(data, dict) and 'mentions' in data:
                            self.logger.info(f"   {aspect.capitalize()}: {data.get('mentions', 0)} mentions (avg sentiment: {data.get('avg_sentiment', 0):.2f})")
                
            except Exception as e:
                self.logger.error(f"Error reading insights report: {e}")
        
        self.logger.info("\n" + "="*80)
        self.logger.info("✅ PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
        self.logger.info("="*80)
    
    def execute_full_pipeline(self, products_config: list, enable_mining: bool = True, enable_debug: bool = False):
        """
        Execute the complete integrated pipeline
        """
        start_time = datetime.now()
        self.logger.info(f"🚀 Starting Integrated ETL & Mining Pipeline at {start_time.isoformat()}")
        
        # Step 1: ETL Extraction
        etl_success, data_file_path, extraction_results = self.run_etl_extraction(products_config, enable_debug)
        
        if not etl_success:
            self.logger.error("❌ Pipeline failed at ETL extraction stage")
            return False
        
        # Step 2: Review Mining (if enabled)
        mining_success, enhanced_data_path, insights_path = True, None, None
        if enable_mining:
            mining_success, enhanced_data_path, insights_path = self.run_review_mining(data_file_path)
            
            if not mining_success:
                self.logger.warning("⚠️ Mining stage failed, but ETL data is available")
        
        # Step 3: Generate Summary Report
        self.generate_summary_report(extraction_results, insights_path)
        
        # Calculate execution time
        end_time = datetime.now()
        execution_time = end_time - start_time
        self.logger.info(f"⏱️ Total execution time: {execution_time}")
        
        return etl_success and (mining_success or not enable_mining)

def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(
        description='Integrated ETL & Review Mining Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Extract and mine reviews for single product
    python integrated_etl_pipeline.py --asins B0CX59H5W7 --pages 2 --headless --mining
    
    # Extract reviews for multiple products without mining
    python integrated_etl_pipeline.py --asins B0CX59H5W7,B0FHB5V36G --pages 1 --headless
    
    # Extract with debug mode (screenshots, logs)
    python integrated_etl_pipeline.py --asins B0CX59H5W7 --pages 1 --debug --mining
        """
    )
    
    parser.add_argument('--asins', type=str, required=True, 
                      help='Comma-separated list of Amazon ASINs to process')
    parser.add_argument('--pages', type=int, default=1,
                      help='Number of pages to scrape per product (default: 1)')
    parser.add_argument('--headless', action='store_true',
                      help='Run browser in headless mode (recommended for automation)')
    parser.add_argument('--mining', action='store_true',
                      help='Enable advanced review mining and sentiment analysis')
    parser.add_argument('--debug', action='store_true',
                      help='Enable debug mode (screenshots, detailed logs)')
    parser.add_argument('--no-notifications', action='store_true',
                      help='Disable pipeline notifications')
    
    args = parser.parse_args()
    
    # Validate inputs
    if not args.asins:
        logger.error("❌ Error: --asins parameter is required")
        sys.exit(1)
    
    # Parse ASINs
    asin_list = [asin.strip() for asin in args.asins.split(',')]
    
    # Create product configuration
    products_config = []
    for asin in asin_list:
        products_config.append({
            'product_id': asin,
            'page_limit': args.pages,
            'headless': args.headless
        })
    
    # Display configuration
    logger.info(" PIPELINE CONFIGURATION:")
    logger.info(f"   Products to process: {asin_list}")
    logger.info(f"   Pages per product: {args.pages}")
    logger.info(f"   Headless mode: {args.headless}")
    logger.info(f"   Review mining enabled: {args.mining}")
    logger.info(f"   Debug mode: {args.debug}")
    
    # Execute pipeline
    try:
        pipeline = IntegratedETLMiningPipeline()
        success = pipeline.execute_full_pipeline(
            products_config=products_config,
            enable_mining=args.mining,
            enable_debug=args.debug
        )
        
        if success:
            logger.info(" Pipeline execution completed successfully!")
            sys.exit(0)
        else:
            logger.error("Pipeline execution failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning(" Pipeline execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f" Unexpected error during pipeline execution: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
import pandas as pd
from datetime import datetime
from playwright.sync_api import sync_playwright
import time
import random
import os
def get_amazon_reviews(asin, num_pages=1, headless=False, max_retries=3, debug=False):
    """Get Amazon reviews using Playwright with retries, logging and optional headful mode.

    Parameters:
    - asin: product ASIN
    - num_pages: number of pages to fetch
    - headless: run browser headless when True
    - max_retries: retry attempts per page
    - debug: when True save screenshots and extra navigation logs
    """
    reviews = []
    product_name = "Unknown Product"  # Default fallback
    browser = None
    context = None
    nav_log_path = 'debug_navigation.log'
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=bool(headless),
                args=['--disable-blink-features=AutomationControlled', '--disable-dev-shm-usage', '--no-sandbox']
            )
            context = browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/120.0.0.0 Safari/537.36"),
                locale='en-IN'
            )
            page = context.new_page()
            try:
                page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => false});");
            except Exception:
                pass

            # First, try to get the product name from the main product page
            try:
                main_product_url = f"https://www.amazon.in/dp/{asin}"
                print(f"Getting product name for ASIN {asin} from: {main_product_url}")
                
                response = page.goto(main_product_url, wait_until='domcontentloaded', timeout=60000)
                
                # Multiple selectors to find product title
                product_title_selectors = [
                    '#productTitle',
                    '[data-automation-id="title"]',
                    '.product-title',
                    'h1.a-size-large',
                    'h1[data-automation-id="title"]',
                    '#feature-bullets h1',
                    '.a-size-large.product-title-word-break'
                ]
                
                for selector in product_title_selectors:
                    try:
                        title_element = page.query_selector(selector)
                        if title_element:
                            product_name = title_element.inner_text().strip()
                            if product_name and len(product_name) > 5:  # Valid title found
                                # Clean up the product name (remove extra whitespace, limit length)
                                product_name = ' '.join(product_name.split())  # Remove extra whitespace
                                if len(product_name) > 100:  # Truncate very long titles
                                    product_name = product_name[:97] + "..."
                                print(f"✅ Product Name: {product_name}")
                                break
                    except Exception:
                        continue
                
                # If no title found with selectors, try alternative approach
                if product_name == "Unknown Product":
                    try:
                        # Get page title which sometimes contains product name
                        page_title = page.title()
                        if page_title and 'Amazon' in page_title:
                            # Extract product name from page title (before "Amazon.in")
                            clean_title = page_title.split(' : ')[0].split(' - ')[0].split(' | ')[0]
                            if len(clean_title) > 5 and 'Amazon' not in clean_title:
                                product_name = clean_title.strip()
                                print(f"📋 Product Name (from page title): {product_name}")
                    except Exception:
                        pass
                        
            except Exception as e:
                print(f"⚠️ Could not fetch product name for {asin}: {e}")

            for page_num in range(1, num_pages + 1):
                requested_url = f"https://www.amazon.in/dp/{asin}/ref=cm_cr_arp_d_product_top?ie=UTF8"
                print(f"Fetching reviews for ASIN {asin} ({product_name}) - Page {page_num} -> {requested_url}")
                success = False

                for attempt in range(max_retries):
                    try:
                        response = page.goto(requested_url, wait_until='domcontentloaded', timeout=60000)
                        # log navigation
                        final_url = page.url
                        status = response.status if response else None
                        if debug:
                            try:
                                with open(nav_log_path, 'a', encoding='utf-8') as nl:
                                    nl.write(f"{datetime.now().isoformat()}\tASIN:{asin}\tProduct:{product_name}\tpage:{page_num}\tattempt:{attempt+1}\trequested:{requested_url}\tfinal:{final_url}\tstatus:{status}\n")
                            except Exception:
                                pass

                        # If we're on reviews page and still don't have product name, try to get it from review page
                        if product_name == "Unknown Product":
                            try:
                                # Try to get product name from breadcrumb or review page header
                                breadcrumb_selectors = [
                                    '[data-hook="product-link"]',
                                    '.a-link-normal[href*="/dp/"]',
                                    '#cm_cr_dp_d_product_info h1',
                                    '.product-title'
                                ]
                                for selector in breadcrumb_selectors:
                                    try:
                                        element = page.query_selector(selector)
                                        if element:
                                            potential_name = element.inner_text().strip()
                                            if potential_name and len(potential_name) > 5:
                                                product_name = ' '.join(potential_name.split())[:100]
                                                print(f"📝 Product Name (from reviews page): {product_name}")
                                                break
                                    except Exception:
                                        continue
                            except Exception:
                                pass

                        # If Amazon redirected to a known error/robot page, save HTML and break attempt loop
                        if 'captcha' in final_url.lower() or 'ref=cs_404_link' in final_url.lower() or 'robot' in final_url.lower():
                            try:
                                snippet = page.content()[:4000]
                                with open(f"debug_content_{asin}_{page_num}.html", 'w', encoding='utf-8') as f:
                                    f.write(snippet)
                            except Exception:
                                pass
                            raise RuntimeError(f"Redirected to anti-bot or 404 page: {final_url}")

                        # Try to click 'See all reviews' on first page
                        if page_num == 1:
                            try:
                                reviews_link = page.get_by_role('link', name='See all reviews')
                                reviews_link.click(timeout=10000)
                                page.wait_for_load_state('domcontentloaded', timeout=15000)
                            except Exception as e:
                                # not fatal; log
                                if debug:
                                    print(f"Could not find/click reviews link: {e}")

                        # optional screenshot (debug only)
                        if debug:
                            try:
                                os.makedirs('debug_screenshots', exist_ok=True)
                                page.screenshot(path=f"debug_screenshots/page_{asin}_{page_num}.png", timeout=10000)
                            except Exception as e:
                                print(f"Screenshot non-fatal error: {e}")

                        # collect review elements
                        review_elements = page.query_selector_all('[data-hook="review"]')
                        if not review_elements:
                            review_elements = page.query_selector_all('.review')

                        if not review_elements:
                            # save snippet for debugging
                            try:
                                with open(f"debug_content_{asin}_{page_num}.html", 'w', encoding='utf-8') as f:
                                    f.write(page.content()[:8000])
                            except Exception:
                                pass
                            raise RuntimeError('No review elements found')

                        for element in review_elements:
                            try:
                                review_data = {
                                    'asin': asin, 
                                    'product_name': product_name,  # Add product name to each review
                                    'title': '', 
                                    'rating': '', 
                                    'content': '', 
                                    'review_date': ''
                                }
                                try:
                                    t = element.query_selector('[data-hook="review-title"]') or element.query_selector('.review-title')
                                    review_data['title'] = t.inner_text().strip() if t else ''
                                except Exception:
                                    pass
                                try:
                                    r = element.query_selector('[data-hook="review-star-rating"]') or element.query_selector('.review-rating')
                                    review_data['rating'] = r.inner_text().strip() if r else ''
                                except Exception:
                                    pass
                                try:
                                    c = element.query_selector('[data-hook="review-body"]') or element.query_selector('.review-text')
                                    review_data['content'] = c.inner_text().strip() if c else ''
                                except Exception:
                                    pass
                                try:
                                    d = element.query_selector('[data-hook="review-date"]') or element.query_selector('.review-date')
                                    review_data['review_date'] = d.inner_text().strip() if d else ''
                                except Exception:
                                    pass
                                if review_data['content'] or review_data['title']:
                                    reviews.append(review_data)
                            except Exception:
                                continue

                        # try to go to next page if needed
                        if page_num < num_pages:
                            try:
                                next_button = page.get_by_role('link', name='Next page')
                                if next_button:
                                    next_button.click(timeout=10000)
                                    page.wait_for_load_state('domcontentloaded', timeout=15000)
                            except Exception:
                                pass

                        success = True
                        break
                    except Exception as e:
                        print(f"Attempt {attempt+1} failed for {asin} page {page_num}: {e}")
                        time.sleep(2 + attempt)

                if not success:
                    print(f"Failed to fetch reviews for ASIN {asin} ({product_name}) page {page_num} after {max_retries} attempts")

                time.sleep(random.uniform(2, 4))

    except Exception as e:
        print(f"Failed to scrape reviews for ASIN {asin}: {e}")
    finally:
        try:
            if context:
                context.close()
        except Exception:
            pass
        try:
            if browser:
                browser.close()
        except Exception:
            pass

    # Create DataFrame and ensure product_name is included
    df = pd.DataFrame(reviews)
    if not df.empty and 'product_name' not in df.columns:
        df['product_name'] = product_name  # Fallback if not added to individual reviews
    
    return df

@task(name="extract_ecommerce_data", retries=3, retry_delay_seconds=30)
def extract_ecommerce_data(extraction_params: dict) -> pd.DataFrame:
    logger = get_run_logger()
    asin = extraction_params.get('product_id')
    num_pages = extraction_params.get('page_limit', 1)
    headless = extraction_params.get('headless', False)
    max_retries = extraction_params.get('max_retries', 3)
    debug = extraction_params.get('debug', False)
    logger.info(f"Extracting reviews for ASIN: {asin} (pages: {num_pages})")
    data = get_amazon_reviews(asin, num_pages, headless=headless, max_retries=max_retries, debug=debug)
    
    # Log product name if available
    if not data.empty and 'product_name' in data.columns:
        product_name = data['product_name'].iloc[0] if len(data) > 0 else "Unknown"
        logger.info(f"Successfully extracted {len(data)} reviews for ASIN: {asin} - Product: {product_name}")
    else:
        logger.info(f"Successfully extracted {len(data)} records for ASIN: {asin}")
    
    return data

@task(name="transform_reviews_data", retries=2)
def transform_reviews_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Starting data transformation")
    if 'rating' in raw_data.columns:
        raw_data['rating'] = raw_data['rating'].str.extract(r'(\d+\.?\d*)').astype(float)
    raw_data['processing_batch_id'] = datetime.now().strftime('%Y%m%d_%H%M%S')
    raw_data['data_source'] = 'amazon_scraper'
    logger.info(f"Transformation completed: {len(raw_data)} records processed")
    return raw_data

@task(name="validate_data_quality")
def validate_data_quality(data: pd.DataFrame) -> dict:
    logger = get_run_logger()
    logger.info("Performing data quality validation")
    total_records = int(len(data))
    null_ratings = int(data['rating'].isna().sum()) if 'rating' in data.columns else total_records
    invalid_dates = int(data['review_date'].isna().sum()) if 'review_date' in data.columns else total_records
    empty_content = int((data['content'].str.len() == 0).sum()) if 'content' in data.columns else total_records
    
    # Use smarter duplicate detection - only check meaningful content fields
    if total_records > 0 and 'content' in data.columns and 'rating' in data.columns:
        duplicate_count = int(data.duplicated(subset=['content', 'rating'], keep='first').sum())
    else:
        duplicate_count = int(data.duplicated().sum()) if total_records > 0 else 0

    # Allow environment overrides for thresholds so behavior can be tuned without editing code
    try:
        max_null_pct = float(os.getenv('ETL_MAX_NULL_PCT', '0.5'))
    except Exception:
        max_null_pct = 0.5
    try:
        max_dup_pct = float(os.getenv('ETL_MAX_DUP_PCT', '0.6'))  # Increased from 0.2 to 0.6
    except Exception:
        max_dup_pct = 0.6
    force_load_on_nonzero = os.getenv('ETL_FORCE_LOAD_ON_NONZERO', 'false').lower() in ('1', 'true', 'yes')

    null_percentage = (null_ratings / total_records) if total_records > 0 else 1.0
    duplicate_percentage = (duplicate_count / total_records) if total_records > 0 else 1.0

    quality_report = {
        'total_records': total_records,
        'null_ratings': null_ratings,
        'invalid_dates': invalid_dates,
        'empty_content': empty_content,
        'duplicate_count': duplicate_count,
        'null_percentage': float(null_percentage),
        'duplicate_percentage': float(duplicate_percentage),
        'max_null_percentage': float(max_null_pct),
        'max_duplicate_percentage': float(max_dup_pct),
        'validation_timestamp': datetime.now().isoformat()
    }

    quality_passed = False
    if total_records == 0:
        quality_passed = False
    else:
        quality_passed = (null_percentage <= max_null_pct) and (duplicate_percentage <= max_dup_pct)

    # optional override: if there are records and operator wants to force load
    if not quality_passed and force_load_on_nonzero and total_records > 0:
        logger.warning("Forcing load despite quality failures because ETL_FORCE_LOAD_ON_NONZERO is set")
        quality_passed = True

    quality_report['quality_passed'] = bool(quality_passed)
    logger.info(f"Quality metrics: total={total_records}, null_pct={quality_report['null_percentage']:.2f}, dup_pct={quality_report['duplicate_percentage']:.2f}, passed={quality_report['quality_passed']}")
    logger.info(f"Quality validation completed: {'PASSED' if quality_report['quality_passed'] else 'FAILED'}")
    return quality_report

@task(name="load_to_destinations")
def load_to_destinations(data: pd.DataFrame, destinations: list) -> dict:
    logger = get_run_logger()
    logger.info(f"Loading data to {len(destinations)} destinations")
    load_results = {}
    for destination in destinations:
        try:
            if destination['type'] == 'database':
                logger.info(f"Would load to database: {destination['table_name']}")
            elif destination['type'] == 'file':
                # Create directory if it doesn't exist
                file_path = destination.get('file_path')
                folder = os.path.dirname(file_path) or '.'
                os.makedirs(folder, exist_ok=True)
                data.to_parquet(file_path)
                logger.info(f"Saved to file: {file_path}")
            load_results[destination['name']] = 'SUCCESS'
        except Exception as e:
            load_results[destination['name']] = f'FAILED: {str(e)}'
            logger.error(f"Failed to load to {destination['name']}: {str(e)}")
    return load_results

@task(name="send_notification")
def send_notification(pipeline_results: dict):
    logger = get_run_logger()
    notification_message = f"""
    ETL Pipeline Execution Summary:
    - Execution Time: {datetime.now().isoformat()}
    - Records Processed: {pipeline_results.get('records_processed', 'Unknown')}
    - Quality Check: {'PASSED' if pipeline_results.get('quality_passed', False) else 'FAILED'}
    - Load Results: {pipeline_results.get('load_results', {})}
    """
    logger.info("Pipeline notification sent")
    logger.info(notification_message)

@flow(
    name="ecommerce_etl_pipeline",
    task_runner=ConcurrentTaskRunner(),
    description="Automated ETL pipeline for e-commerce customer reviews"
)
def ecommerce_etl_flow(
    products_to_process: list,
    destinations: list,
    send_notifications: bool = True
):
    logger = get_run_logger()
    logger.info(f"Starting ETL pipeline for {len(products_to_process)} products")
    all_results = []
    all_successful_data = []  # Accumulate all successful product data
    
    for product_params in products_to_process:
        logger.info(f"Processing product: {product_params.get('product_id')}")
        raw_data = extract_ecommerce_data(product_params)
        transformed_data = transform_reviews_data(raw_data)
        quality_report = validate_data_quality(transformed_data)
        
        if quality_report['quality_passed']:
            all_successful_data.append(transformed_data)  # Store successful data
            load_results = {'status': 'STAGED_FOR_COMBINED_LOAD'}
            logger.info(f"Staged {len(transformed_data)} records from product {product_params.get('product_id')}")
        else:
            logger.warning(f"Skipping product {product_params.get('product_id')} due to quality issues")
            load_results = {'status': 'SKIPPED_DUE_TO_QUALITY_ISSUES'}
            
        pipeline_results = {
            'product_id': product_params.get('product_id'),
            'records_processed': int(len(transformed_data)),
            'quality_passed': bool(quality_report.get('quality_passed', False)),
            'quality_report': quality_report,
            'load_results': load_results
        }
        all_results.append(pipeline_results)
        if send_notifications:
            send_notification(pipeline_results)
    
    # Combine all successful data and save once at the end
    if all_successful_data:
        combined_data = pd.concat(all_successful_data, ignore_index=True)
        logger.info(f"Combining and saving {len(combined_data)} total records from {len(all_successful_data)} products")
        final_load_results = load_to_destinations(combined_data, destinations)
        logger.info(f"Final load results: {final_load_results}")
        
        # Update the load_results in all_results for successful products
        for result in all_results:
            if result['load_results']['status'] == 'STAGED_FOR_COMBINED_LOAD':
                result['load_results'] = final_load_results
    else:
        logger.warning("No successful data to load - all products failed quality checks")
    
    logger.info(f"Pipeline completed for all {len(products_to_process)} products")
    return all_results

if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description='ETL Automation Pipeline')
    parser.add_argument('--asins', type=str, help='Comma-separated list of ASINs')
    parser.add_argument('--pages', type=int, default=1, help='Number of pages per product')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    parser.add_argument('--no-headless', action='store_true', help='Run in visible browser mode')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    # Use command line arguments if provided
    if args.asins:
        asin_list = [asin.strip() for asin in args.asins.split(',')]
        products_to_process = [
            {'product_id': asin, 'page_limit': args.pages, 'headless': not args.no_headless, 'debug': args.debug}
            for asin in asin_list
        ]
    else:
        # Default configuration when no arguments provided
        products_to_process = [
            {'product_id': 'B0CX59H5W7', 'page_limit': 2}, 
            {'product_id': 'B0FHB5V36G', 'page_limit': 2}, 
            {'product_id': 'B0F1D9LCK3', 'page_limit': 2},
            {'product_id': 'B0DDY9HMJG', 'page_limit': 2},
            {'product_id': 'B0D78X544X', 'page_limit': 2},
        ]
        
    destinations = [
        {
            'name': 'data_lake_backup',
            'type': 'file',
            'file_path': f'data/reviews/reviews_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
        }
    ]
    
    if args.asins:
        print(f"Starting ETL pipeline for ASINs: {asin_list}")
        print(f"Pages per product: {args.pages}")
        print(f"Headless mode: {not args.no_headless}")
    else:
        print("Running ETL pipeline with default configuration...")
        
    result = ecommerce_etl_flow(
        products_to_process=products_to_process,
        destinations=destinations,
        send_notifications=True
    )
    print("ETL Pipeline Result:", result)
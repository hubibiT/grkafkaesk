import time
import re
import random
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from multiprocessing import Pool, cpu_count

# ==============================================================================
# Global variable and Initializer for persistent worker IDs
# ==============================================================================

worker_id = "MAIN_PROCESS"

def initialize_worker():
    """Initializer function called once for each new worker process."""
    global worker_id
    worker_id = str(os.getpid())

# ==============================================================================
# Worker Function (Unchanged)
# ==============================================================================

def worker_function(url):
    """
    Checks a single URL. Returns a tuple: (status, url, [optional_error])
    """
    # Staggered start to prevent "thundering herd"
    time.sleep(random.uniform(0, 3))

    keyword = "kafkaesque"
    process_id = worker_id
    
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
    options.add_argument("--headless")
    options.add_argument("--window-size=1920,1200")
    options.add_argument("--log-level=3") # Suppress console noise
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    driver = webdriver.Chrome(options=options)
    
    reviews_url = (url if '/reviews' in url else url.split('?')[0] + '/reviews')
    
    try:
        driver.get(reviews_url)
        wait = WebDriverWait(driver, 15)
        search_box = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Search review text']")))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_box)
        time.sleep(0.75)
        search_box.clear()
        search_box.send_keys(keyword + Keys.RETURN)
        time.sleep(1.5)
        
        WebDriverWait(driver, 6).until(EC.presence_of_element_located((By.CSS_SELECTOR, "article.ReviewCard")))
        
        driver.quit()
        return ('VALID_MATCH', url)
    except TimeoutException:
        driver.quit()
        return ('NO_MATCH', url)
    except Exception as e:
        error_type = type(e).__name__
        driver.quit()
        return ('FAILURE', url, error_type)

# ==============================================================================
# Main Orchestrator (Definitive Version with Correct State Management)
# ==============================================================================

if __name__ == '__main__':
    # --- FILENAMES ---
    INPUT_FILENAME = 'urls_final_unique.txt'
    VERIFIED_OUTPUT_FILENAME = 'urls_verified_kafkaesque.txt'
    FAILURE_OUTPUT_FILENAME = 'urls_failed_to_process.txt'
    NO_MATCH_OUTPUT_FILENAME = 'urls_no_match_found.txt'

    # --- CONFIGURATION ---
    NUM_WORKERS = max(1, cpu_count() - 2) # A more conservative default
    TASKS_PER_WORKER = 25 
    NUM_SUB_BATCHES = 3
    SUB_BATCH_SIZE = 50

    print("--- Goodreads Interactive Batch Scraper (V34) Initializing ---")
    
    # --- CORRECTED & FOOLPROOF PROGRESS TRACKING ---
    # The set of processed URLs is the UNION of all three output files.
    processed_urls = set()
    for filename in [VERIFIED_OUTPUT_FILENAME, FAILURE_OUTPUT_FILENAME, NO_MATCH_OUTPUT_FILENAME]:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                processed_urls.update(line.strip() for line in f if line.strip())
    
    print(f"Loaded {len(processed_urls)} URLs from all previous output files (verified, failed, and no_match).")
    
    try:
        with open(INPUT_FILENAME, 'r') as f:
            all_urls_from_file = set([line.strip() for line in f if line.strip()])
        # Correctly calculate the remaining work
        urls_to_process_full_list = list(all_urls_from_file - processed_urls)
        random.shuffle(urls_to_process_full_list)
        
        if not urls_to_process_full_list:
            print("\nAll URLs from the input file have already been processed. Nothing to do.")
            exit()
    except FileNotFoundError:
        print(f"CRITICAL ERROR: Input file '{INPUT_FILENAME}' not found.")
        exit()

    master_batch_size = NUM_SUB_BATCHES * SUB_BATCH_SIZE
    urls_for_this_run = urls_to_process_full_list[:master_batch_size]
    
    total_remaining = len(urls_to_process_full_list)
    print(f"Total unique URLs remaining to be processed: {total_remaining}")
    print(f"This run will process up to {len(urls_for_this_run)} URLs.")
    
    with Pool(processes=NUM_WORKERS, initializer=initialize_worker, maxtasksperchild=TASKS_PER_WORKER) as pool:
        results_iterator = pool.imap_unordered(worker_function, urls_for_this_run)
        
        run_successes = 0
        run_failures = 0
        run_no_matches = 0

        print("\n" + "="*60)
        print("--- Starting URL Processing (Results will appear as they complete) ---")
        print("="*60)

        for i, result in enumerate(results_iterator):
            status, url = result[0], result[1]
            
            if status == 'VALID_MATCH':
                print(f"Result {i+1}/{len(urls_for_this_run)}: [SUCCESS] Keyword found for {url}")
                with open(VERIFIED_OUTPUT_FILENAME, 'a') as f:
                    f.write(url + '\n')
                run_successes += 1
            elif status == 'NO_MATCH':
                print(f"Result {i+1}/{len(urls_for_this_run)}: [NO MATCH] Page checked for {url}")
                with open(NO_MATCH_OUTPUT_FILENAME, 'a') as f:
                    f.write(url + '\n')
                run_no_matches += 1
            elif status == 'FAILURE':
                error_type = result[2]
                print(f"Result {i+1}/{len(urls_for_this_run)}: [FAILURE] Error '{error_type}' for {url}")
                with open(FAILURE_OUTPUT_FILENAME, 'a') as f:
                    f.write(url + '\n')
                run_failures += 1

    print("\n" + "="*60)
    print("--- BATCH RUN COMPLETE ---")
    print(f"Processed {len(urls_for_this_run)} URLs in this run.")
    print(f"  - Relevant URLs found (SUCCESS): {run_successes}")
    print(f"  - Checked, no match found (NO MATCH): {run_no_matches}")
    print(f"  - Failures (errors): {run_failures}")
    
    remaining_after_this_run = total_remaining - len(urls_for_this_run)
    
    print("\nResults have been saved incrementally.")
    if remaining_after_this_run > 0:
        print(f"There are {remaining_after_this_run} URLs left to process.")
        print("You can run this script again to process the next set of batches.")
    else:
        print("All URLs have been processed!")
    print("="*60)
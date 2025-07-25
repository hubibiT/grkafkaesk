import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import os
import time


try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.chrome.service import Service as ChromeService
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    print("ERROR: A required library (selenium or webdriver-manager) is not installed.")
    print("Please run: pip3 install --upgrade selenium webdriver-manager")
    exit()


# To prevent a harmless error message on Windows
if os.name == 'nt':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ==============================================================================
# SECTION 1: WORKER FUNCTIONS (Unchanged)
# ==============================================================================

async def get_canonical_url_fast(session, initial_url):
    """Asynchronously tries to fetch and parse a URL."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }
    try:
        async with session.get(initial_url, headers=headers, timeout=10) as response:
            if response.status != 200: return None
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            meta_tag = soup.find('meta', property='og:url')
            return meta_tag.get('content') if meta_tag and meta_tag.get('content') else None
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return None

def get_canonical_url_slow(driver, initial_url):
    """Uses Selenium to reliably fetch the canonical URL."""
    try:
        driver.get(initial_url)
        wait = WebDriverWait(driver, 15)
        meta_tag = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'meta[property="og:url"]')))
        return meta_tag.get_attribute('content')
    except (TimeoutException, Exception) as e:
        print(f"  > SELENIUM FAILED for {initial_url}. Discarding. Reason: {type(e).__name__}")
        return None

# ==============================================================================
# SECTION 2: MAIN ORCHESTRATOR (This section is now fully corrected)
# ==============================================================================
async def main(input_filename='urls_verified_kafkaesque.txt', output_filename='xxurls_final_unique.txt'):
    """
    Orchestrates de-duplication with many asynchronous passes before
    falling back to a final Selenium pass.
    """
    MAX_ASYNC_ATTEMPTS = 50 
    DELAY_BETWEEN_ATTEMPTS = 5

    print("Initializing Exhaustive Iterative Scraper...")

    try:
        with open(input_filename, 'r', encoding='utf-8') as f:
            urls_to_process = list(set([line.strip() for line in f if line.strip()]))
        initial_count = len(urls_to_process)
        print(f"Found {initial_count} unique URLs to process from '{input_filename}'.")
    except FileNotFoundError:
        print(f"Error: Input file '{input_filename}' not found.")
        return

    unique_works = {}
    
    # --- MULTIPLE ASYNC PASSES ---
    for attempt in range(1, MAX_ASYNC_ATTEMPTS + 1):
        if not urls_to_process:
            print("\nAll URLs have been processed successfully by the asynchronous method!")
            break

        print(f"\n--- Starting Async Attack #{attempt}/{MAX_ASYNC_ATTEMPTS} on {len(urls_to_process)} URLs ---")
        
        current_failures = []
        
        async with aiohttp.ClientSession() as session:
            tasks = [get_canonical_url_fast(session, url) for url in urls_to_process]
            results = await asyncio.gather(*tasks)

        success_count_this_pass = 0
        for i, canonical_url in enumerate(results):
            original_url = urls_to_process[i]
            if canonical_url:
                match = re.search(r'/book/show/(\d+)', canonical_url)
                if match:
                    unique_id = match.group(1)
                    if unique_id not in unique_works:
                        unique_works[unique_id] = canonical_url
                        success_count_this_pass += 1
                else: current_failures.append(original_url)
            else: current_failures.append(original_url)

        print(f"> Pass #{attempt} complete. Successfully processed {success_count_this_pass} new URLs.")
        
        if success_count_this_pass == 0 and attempt > 1:
            print("> No progress made in the last pass. Moving directly to Selenium.")
            urls_to_process = current_failures
            break
            
        urls_to_process = current_failures

        if urls_to_process and attempt < MAX_ASYNC_ATTEMPTS:
            print(f"Waiting for {DELAY_BETWEEN_ATTEMPTS} seconds before next attempt...")
            await asyncio.sleep(DELAY_BETWEEN_ATTEMPTS)

    # --- FINAL SELENIUM PASS ---
    if urls_to_process:
        print(f"\n--- Starting Final Selenium Pass on {len(urls_to_process)} remaining URLs ---")
        
        # ***** THE ONE-LINE FIX IS HERE *****
        # Initialize driver to None to ensure the variable always exists.
        driver = None
        # ************************************
        
        try:
            print("NOTE: A Chrome window will open. This is expected.")
            options = webdriver.ChromeOptions()
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            
            print("Setting up Selenium and ChromeDriver...")
            driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
            print("Selenium is ready.")

            selenium_successes = []
            for i, book_url in enumerate(urls_to_process, 1):
                print(f"Processing Selenium retry {i}/{len(urls_to_process)}: {book_url}")
                canonical_url = get_canonical_url_slow(driver, book_url)
                if canonical_url:
                    selenium_successes.append(book_url)
                    match = re.search(r'/book/show/(\d+)', canonical_url)
                    if match:
                        unique_id = match.group(1)
                        if unique_id not in unique_works:
                            unique_works[unique_id] = canonical_url
                            print(f"  > Success on retry (ID: {unique_id}).")
                time.sleep(0.5)
            
            urls_to_process = [url for url in urls_to_process if url not in selenium_successes]

        except Exception as e:
            print(f"\nCRITICAL ERROR during Selenium setup or execution: {e}")
        
        finally:
            if driver:
                driver.quit()

    # --- FINAL RESULTS ---
    final_unique_urls = list(unique_works.values())
    print(f"\n--- De-duplication Complete ---")
    print(f"Original unique URL count: {initial_count}")
    print(f"Final unique work count: {len(final_unique_urls)}")
    print(f"URLs that could not be processed: {len(urls_to_process)}")
    
    with open(output_filename, 'w', encoding='utf-8') as f:
        for url in sorted(final_unique_urls):
            f.write(url + '\n')
            
    print(f"\nSuccessfully saved the final list to '{output_filename}'")


if __name__ == '__main__':
    asyncio.run(main())
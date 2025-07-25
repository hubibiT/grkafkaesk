import time
import pandas as pd
import re
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from multiprocessing import Pool, cpu_count

# ==============================================================================
# HELPER FUNCTIONS (These are called by each worker)
# ==============================================================================

def process_and_truncate_context(html_content, keyword, max_words=500):
    """(This helper function is unchanged)"""
    if not html_content: return None
    paragraph_delimiter = "|||PARAGRAPH|||"
    text_with_delimiters = re.sub(r'<br\s*/?>', paragraph_delimiter, html_content, flags=re.IGNORECASE)
    clean_text = re.sub(r'<.*?>', '', text_with_delimiters)
    paragraphs = [p.strip() for p in clean_text.split(paragraph_delimiter) if p.strip()]
    target_paragraph = next((p for p in paragraphs if keyword.lower() in p.lower()), None)
    if not target_paragraph: return None
    words = target_paragraph.split()
    if len(words) <= max_words: return target_paragraph
    try:
        keyword_pos = next(i for i, word in enumerate(words) if keyword.lower() in word.lower())
    except StopIteration: return " ".join(words[:max_words]) + "..."
    half_way = max_words // 2
    start_index = max(0, keyword_pos - half_way)
    end_index = min(len(words), start_index + max_words)
    if end_index == len(words): start_index = max(0, end_index - max_words)
    truncated_words = words[start_index:end_index]
    result = " ".join(truncated_words)
    if start_index > 0: result = "... " + result
    if end_index < len(words): result = result + " ..."
    return result

def handle_popups(driver):
    """(This helper function is unchanged)"""
    try:
        short_wait = WebDriverWait(driver, 3)
        close_button_xpath = "//button[@aria-label='Close']"
        close_button = short_wait.until(EC.element_to_be_clickable((By.XPATH, close_button_xpath)))
        driver.execute_script("arguments[0].click();", close_button)
        time.sleep(1)
    except: pass

def scrape_book_metadata(driver, main_book_url):
    """
    --- V21: MODIFIED to capture MULTIPLE authors. ---
    """
    wait = WebDriverWait(driver, 10)
    metadata = {}
    try:
        driver.get(main_book_url)
        handle_popups(driver)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.BookPage__mainContent")))

        # --- MODIFIED FOR MULTIPLE AUTHORS ---
        try:
            # find_elements (plural) gets a list of all matching author links
            author_elements = driver.find_elements(By.CSS_SELECTOR, "div.ContributorLinksList span.ContributorLink__name")
            authors = [elem.text for elem in author_elements if elem.text]
            metadata['author'] = " | ".join(authors) if authors else "Not Found"
        except NoSuchElementException: metadata['author'] = "Not Found"

        try:
            metadata['avg_rating'] = driver.find_element(By.CSS_SELECTOR, "div.RatingStatistics__rating").text
        except NoSuchElementException: metadata['avg_rating'] = "Not Found"
        try:
            community_reviews_link = driver.find_element(By.CSS_SELECTOR, "a[href*='#CommunityReviews']")
            match = re.search(r'([\d,]+)\s+reviews', community_reviews_link.text)
            metadata['total_reviews'] = match.group(1).replace(',', '') if match else "Not Found"
        except NoSuchElementException: metadata['total_reviews'] = "Not Found"
        try:
            metadata['release_date'] = driver.find_element(By.CSS_SELECTOR, "[data-testid='publicationInfo']").text.replace('First published ', '')
        except NoSuchElementException: metadata['release_date'] = "Not Found"
        try:
            wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-testid='genresList']")))
            genres_container = driver.find_element(By.CSS_SELECTOR, "div[data-testid='genresList']")
            genre_links = genres_container.find_elements(By.CSS_SELECTOR, "a.Button--tag")
            genres = [link.find_element(By.CSS_SELECTOR, "span.Button__labelItem").text for link in genre_links]
            metadata['genres'] = " | ".join([g for g in genres if g])
        except Exception: metadata['genres'] = "Not Found"
            
        return metadata
    except Exception as e:
        print(f"    - CRITICAL ERROR scraping metadata for {main_book_url}: {e}")
        return None

def scrape_goodreads_reviews(driver, reviews_url, book_name, keyword):
    """(This helper function is mostly unchanged)"""
    wait = WebDriverWait(driver, 15)
    scraped_data = []
    try:
        driver.get(reviews_url)
        handle_popups(driver)
        search_box_xpath = "//input[@placeholder='Search review text']"
        search_box = wait.until(EC.presence_of_element_located((By.XPATH, search_box_xpath)))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_box)
        time.sleep(1)
        search_box.clear()
        search_box.send_keys(keyword + Keys.RETURN)
        time.sleep(3)
        try:
            short_wait = WebDriverWait(driver, 5)
            short_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "article.ReviewCard")))
        except TimeoutException:
            return [] # No reviews found, return empty list
        page_count = 0
        scraped_review_ids = set()
        while True:
            page_count += 1
            # ... (inner scraping loop is identical to previous version) ...
            try:
                text_expand_buttons = driver.find_elements(By.XPATH, ".//button[span[text()='Show more']]")
                for button in text_expand_buttons:
                    try: driver.execute_script("arguments[0].click();", button)
                    except: pass
                time.sleep(0.5)
            except Exception: pass
            review_elements = driver.find_elements(By.CSS_SELECTOR, "article.ReviewCard")
            for review_element in review_elements:
                try:
                    review_link_element = review_element.find_element(By.XPATH, ".//a[contains(@href, '/review/show/')]")
                    review_id = review_link_element.get_attribute('href')
                    if review_id in scraped_review_ids: continue
                    date = review_link_element.text.strip()
                    text_container = review_element.find_element(By.CSS_SELECTOR, "span.Formatted")
                    review_html_content = text_container.get_attribute('innerHTML')
                    final_context = process_and_truncate_context(review_html_content, keyword)
                    if final_context is None: continue
                    scraped_review_ids.add(review_id)
                    try:
                        stars_element = review_element.find_element(By.CSS_SELECTOR, "span.RatingStars")
                        aria_label = stars_element.get_attribute('aria-label')
                        stars = re.search(r'\d+', aria_label).group(0) if aria_label and re.search(r'\d+', aria_label) else "Not rated"
                    except NoSuchElementException: stars = "Not rated"
                    scraped_data.append({"book_name": book_name, "stars": stars, "date": date, "context": final_context})
                except Exception: continue
            try:
                show_more_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[@data-testid='loadMore']/..")))
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", show_more_button)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", show_more_button)
                time.sleep(3)
            except TimeoutException:
                break # Reached the end
            except Exception:
                break # Error, exit
    except Exception as e:
        print(f"An unexpected critical error during review scraping for {book_name}: {type(e).__name__}")
    return scraped_data

# ==============================================================================
# WORKER FUNCTION (This is what each parallel process will run)
# ==============================================================================

def process_single_book(url):
    """
    Complete scraping process for a single book URL.
    This function is designed to be called by a multiprocessing Pool.
    """
    keyword = "kafkaesque"
    process_id = os.getpid() # Get the unique process ID for logging
    
    # --- URL and Book Name Setup ---
    reviews_url = (url if '/reviews' in url else url.split('?')[0] + '/reviews')
    main_book_url = reviews_url.replace('/reviews', '')
    book_name_match = re.search(r'/show/\d+\.([^/?]+)', main_book_url)
    book_name = book_name_match.group(1).replace('_', ' ').replace('-', ' ') if book_name_match else f"URL_ID_{process_id}"
    
    print(f"[Worker {process_id}] Starting task for: {book_name}")

    # --- Each worker gets its own browser instance ---
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument(f"user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.{process_id}")
    options.add_argument("--headless")
    options.add_argument("--window-size=1920,1200")
    
    driver = webdriver.Chrome(options=options)
    
    try:
        # --- EFFICIENT WORKFLOW ---
        # 1. Scrape reviews first to check for relevance
        reviews_for_this_book = scrape_goodreads_reviews(driver, reviews_url, book_name, keyword)
        
        # 2. If (and only if) relevant reviews were found, get the metadata
        if reviews_for_this_book:
            print(f"[Worker {process_id}] Found {len(reviews_for_this_book)} relevant reviews for '{book_name}'. Now getting metadata.")
            metadata = scrape_book_metadata(driver, main_book_url)
            
            if metadata:
                metadata['book_name'] = book_name
                metadata['kafkaesque_review_count'] = len(reviews_for_this_book)
                # Return a dictionary containing both results
                return {'reviews_data': reviews_for_this_book, 'summary_data': metadata}
        
        # If no reviews were found, we don't need to return anything
        print(f"[Worker {process_id}] No relevant reviews found for '{book_name}'. Task complete.")
        return None

    finally:
        # Crucially, each worker must close its own browser
        driver.quit()

# ==============================================================================
# MAIN ORCHESTRATOR
# ==============================================================================

if __name__ == '__main__':
    # --- CONFIGURATION ---
    INPUT_FILENAME = 'urls_verified_kafkaesque.txt' # Assumes you are using the pre-verified list
    REVIEWS_OUTPUT_FILENAME = 'goodreads_reviews_output.csv'
    SUMMARY_OUTPUT_FILENAME = 'goodreads_book_summary.csv'
    KEYWORD = "kafkaesque"
    # As requested, use 4 workers. cpu_count() is a good alternative for flexibility.
    NUM_WORKERS = 6

    print(f"--- Goodreads Parallel Scraper Initializing with {NUM_WORKERS} workers ---")
    
    try:
        with open(INPUT_FILENAME, 'r') as f:
            urls_to_process = list(set([line.strip() for line in f if line.strip()]))
        if not urls_to_process:
            print(f"Error: Input file '{INPUT_FILENAME}' is empty.")
            exit()
    except FileNotFoundError:
        print(f"CRITICAL ERROR: Input file '{INPUT_FILENAME}' not found.")
        exit()

    print(f"Found {len(urls_to_process)} unique URLs to process.")

    # --- Initialize lists to hold all results ---
    all_reviews_data = []
    all_books_summary_data = []

    # --- Create the multiprocessing Pool ---
    with Pool(processes=NUM_WORKERS) as pool:
        # imap_unordered is great for long tasks, as it provides results as they complete
        results_iterator = pool.imap_unordered(process_single_book, urls_to_process)
        
        for i, result in enumerate(results_iterator):
            print(f"--- Progress: {i+1}/{len(urls_to_process)} books complete ---")
            # Filter out None results (for books with no relevant reviews)
            if result:
                all_reviews_data.extend(result['reviews_data'])
                all_books_summary_data.append(result['summary_data'])

    print("\n" + "="*60)
    print("--- All Workers Finished. Aggregating and saving results. ---")
    print("="*60)

    # --- Save the final CSV files ---
    if all_reviews_data:
        print(f"\n--- FINAL REVIEWS RESULT ---\nSUCCESS: Found {len(all_reviews_data)} total relevant reviews.")
        reviews_df = pd.DataFrame(all_reviews_data)
        reviews_df.to_csv(REVIEWS_OUTPUT_FILENAME, index=False, encoding='utf-8')
        print(f"Detailed reviews data saved to '{REVIEWS_OUTPUT_FILENAME}'")
    else:
        print("\nFinished: No reviews with the specified context were found.")
        
    if all_books_summary_data:
        print(f"\n--- FINAL BOOK SUMMARY ---\nSUCCESS: Found {len(all_books_summary_data)} books with relevant reviews.")
        summary_df = pd.DataFrame(all_books_summary_data)
        summary_df = summary_df[['book_name', 'author', 'avg_rating', 'total_reviews', 'kafkaesque_review_count', 'release_date', 'genres']]
        print(summary_df)
        summary_df.to_csv(SUMMARY_OUTPUT_FILENAME, index=False, encoding='utf-8')
        print(f"Book summary data saved to '{SUMMARY_OUTPUT_FILENAME}'")
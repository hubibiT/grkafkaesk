import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

def discover_books_from_lists(start_url):
    """
    Scrapes Goodreads for all books found on lists matching a search query.
    --- V2: Uses a more robust JavaScript click to handle obscured elements. ---
    """
    print("Initializing Discovery Scraper...")
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
    options.add_argument("--headless")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)
    
    list_page_urls = []
    unique_book_urls = set()

    # --- Part 1: Collect URLs of all lists from the search results ---
    print(f"Starting discovery at: {start_url}")
    driver.get(start_url)
    
    page_num = 1
    while True:
        print(f"Scraping list search results page {page_num}...")
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'table.tableList')))
            list_elements = driver.find_elements(By.CSS_SELECTOR, 'a.listTitle')
            if not list_elements:
                print("  - No list elements found on this page.")
                break
            for elem in list_elements:
                list_page_urls.append(elem.get_attribute('href'))
            
            next_button = driver.find_element(By.CSS_SELECTOR, 'a.next_page')
            
            # ###############################################
            # ### START OF CORRECTED CODE BLOCK ###
            # ###############################################
            # Scroll the button into view and click with JavaScript for reliability
            driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
            time.sleep(0.5) # A brief pause to ensure the scroll is complete
            driver.execute_script("arguments[0].click();", next_button)
            # ###############################################
            # ### END OF CORRECTED CODE BLOCK ###
            # ###############################################

            time.sleep(2) 
            page_num += 1
            
        except TimeoutException:
            print("  - Timed out waiting for list table. Likely no results.")
            break
        except NoSuchElementException:
            print("  - Reached the last page of list search results.")
            break

    print(f"\nFound a total of {len(list_page_urls)} lists to process.")

    # --- Part 2: Visit each list and scrape the URLs of all books on it ---
    for i, list_url in enumerate(list_page_urls, 1):
        print(f"\nProcessing list {i}/{len(list_page_urls)}: {list_url}")
        driver.get(list_url)
        book_page_num = 1
        
        while True:
            print(f"  - Scraping page {book_page_num} of this list...")
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a.bookTitle')))
                book_elements = driver.find_elements(By.CSS_SELECTOR, 'a.bookTitle')
                
                new_urls_found = 0
                for book_elem in book_elements:
                    url = book_elem.get_attribute('href')
                    if url not in unique_book_urls:
                        unique_book_urls.add(url)
                        new_urls_found += 1
                print(f"    > Found {len(book_elements)} books. Added {new_urls_found} new unique URLs.")
                
                next_button = driver.find_element(By.CSS_SELECTOR, 'a.next_page')
                
                # Using the same robust click method here for consistency
                driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", next_button)
                
                time.sleep(2)
                book_page_num += 1
            
            except TimeoutException:
                print("   - Timed out waiting for book titles. Moving to next list.")
                break
            except NoSuchElementException:
                print("   - Reached the end of this list.")
                break
                
    driver.quit()
    
    print(f"\n--- Discovery Complete ---")
    print(f"Found a total of {len(unique_book_urls)} unique book URLs across all lists.")
    
    sorted_urls = sorted(list(unique_book_urls))
    output_filename = 'urls_to_scrape.txt'
    with open(output_filename, 'w', encoding='utf-8') as f:
        for url in sorted_urls:
            f.write(url + '\n')
            
    print(f"Successfully saved all unique URLs to '{output_filename}'")

if __name__ == '__main__':
    search_url = 'https://www.goodreads.com/search?q=kafka&search%5Bsource%5D=goodreads&search_type=lists&tab=lists'
    discover_books_from_lists(search_url)
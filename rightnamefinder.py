import pandas as pd
import re
import warnings
import os

# ==============================================================================
# CONFIGURATION
# ==============================================================================
SUMMARY_FILENAME_INPUT = 'goodreads_book_summary.csv'
REVIEWS_FILENAME_INPUT = 'goodreads_reviews_output.csv'

# --- NEW: Explicit output filenames ---
SUMMARY_FILENAME_OUTPUT = 'goodreads_book_summary_corrected.csv'
REVIEWS_FILENAME_OUTPUT = 'goodreads_reviews_output_corrected.csv'

# ==============================================================================
# HELPER FUNCTION (Unchanged)
# ==============================================================================
def extract_name_from_url(book_name_or_url):
    """
    Applies a series of robust regex patterns to extract a book name.
    """
    if not isinstance(book_name_or_url, str):
        return book_name_or_url

    url_match = re.search(r'(https?://[^\s]+)', book_name_or_url)
    url_to_parse = url_match.group(1) if url_match else book_name_or_url

    name_match = re.search(r'/show/\d+[\.\-]([^/?]+)', url_to_parse)
    if name_match:
        return name_match.group(1).replace('_', ' ').replace('-', ' ')

    id_match = re.search(r'/show/(\d+)', url_to_parse)
    if id_match:
        return f"Book_ID_{id_match.group(1)}"
        
    return book_name_or_url

# ==============================================================================
# EFFICIENT "FIXER" FUNCTION (Unchanged)
# ==============================================================================
def efficient_fix_names(df):
    """
    Finds unique incorrect names using a robust heuristic, creates a correction map, and applies it.
    """
    bad_name_pattern = r'[/_]|^(Unknown_Book_From_|URL_ID_)'
    
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        incorrect_rows = df[df['book_name'].astype(str).str.contains(bad_name_pattern, na=False)]
    
    if incorrect_rows.empty:
        print("No incorrect names found based on plausibility check.")
        return df, 0
        
    unique_incorrect_names = incorrect_rows['book_name'].unique()
    
    print(f"Found {len(unique_incorrect_names)} unique incorrect book names to correct.")
    
    correction_map = {
        bad_name: extract_name_from_url(bad_name)
        for bad_name in unique_incorrect_names
    }
    
    df['book_name'] = df['book_name'].replace(correction_map)
    
    print("Correction complete.")
    return df, len(incorrect_rows)

# ==============================================================================
# MAIN SCRIPT (With Safe Saving)
# ==============================================================================
if __name__ == '__main__':
    print("--- Starting Definitive Goodreads CSV Name Corrector (V5 - Safe Save) ---")

    # --- FIX THE SUMMARY FILE ---
    try:
        print(f"\nProcessing summary file: '{SUMMARY_FILENAME_INPUT}'...")
        if not os.path.exists(SUMMARY_FILENAME_INPUT):
            raise FileNotFoundError(f"Input file not found: {SUMMARY_FILENAME_INPUT}")
            
        summary_df = pd.read_csv(SUMMARY_FILENAME_INPUT)
        
        corrected_summary_df, num_fixed = efficient_fix_names(summary_df)
        
        if num_fixed > 0:
            # --- MODIFIED: Save to a NEW file ---
            corrected_summary_df.to_csv(SUMMARY_FILENAME_OUTPUT, index=False, encoding='utf-8')
            print(f"SUCCESS: Corrected {num_fixed} rows. NEW file saved to '{SUMMARY_FILENAME_OUTPUT}'.")
        else:
            print("No changes were needed for the summary file.")

    except FileNotFoundError as e:
        print(f"WARNING: {e}. Skipping.")
    except Exception as e:
        print(f"An error occurred while processing the summary file: {e}")

    # --- FIX THE REVIEWS FILE ---
    try:
        print(f"\nProcessing reviews file: '{REVIEWS_FILENAME_INPUT}'...")
        if not os.path.exists(REVIEWS_FILENAME_INPUT):
            raise FileNotFoundError(f"Input file not found: {REVIEWS_FILENAME_INPUT}")

        reviews_df = pd.read_csv(REVIEWS_FILENAME_INPUT)
        
        corrected_reviews_df, num_fixed = efficient_fix_names(reviews_df)
        
        if num_fixed > 0:
            # --- MODIFIED: Save to a NEW file ---
            corrected_reviews_df.to_csv(REVIEWS_FILENAME_OUTPUT, index=False, encoding='utf-8')
            print(f"SUCCESS: Corrected {num_fixed} rows. NEW file saved to '{REVIEWS_FILENAME_OUTPUT}'.")
        else:
            print("No changes were needed for the reviews file.")

    except FileNotFoundError as e:
        print(f"WARNING: {e}. Skipping.")
    except Exception as e:
        print(f"An error occurred while processing the reviews file: {e}")
        
    print("\n--- Correction process finished. ---")
    print("Please check the new '_corrected.csv' files for the results.")
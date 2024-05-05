from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from multiprocessing import Pool, cpu_count
import logging
import os
import pandas as pd
import sys
sys.path.append(os.getcwd())

from init_browser import _init_browser


def scrape_matches(browser, tournament_name, tournament_url):
    match_div_class = "event__match--twoLine"
    match_round_div_class = "event__round event__round--static"

    original_window = browser.current_window_handle
    wait = WebDriverWait(browser, 10)

    elems = browser.find_elements(By.XPATH, "//div[@class='sportName tennis']/*")
    round = ''
    rows = []

    for el in elems:
        if el.get_attribute('class') == match_round_div_class:
            round = el.text
        elif match_div_class in el.get_attribute('class'):
            match_date = el.find_element(By.CLASS_NAME, 'event__time').text
            el.click()

            wait.until(EC.number_of_windows_to_be(2))

            for window in browser.window_handles:
                if window != original_window:
                    browser.switch_to.window(window)
                    break
            
            url = browser.current_url
            url = url.removesuffix("/match-summary")
            url += "/point-by-point/0"

            rows.append({
                'tournament_name': tournament_name,
                'tournament_url': tournament_url,
                'round': round,
                'match_url': url,
                'match_date': match_date
            })

            browser.close()
            browser.switch_to.window(original_window)
        
        elif 'qualification' in el.text.lower():
            break

    if len(rows) == 0:
        return pd.DataFrame()
    else:
        return pd.DataFrame(rows)


def get_matches(chunk, headless, logging_fname):
    # Configure logging
    logging.basicConfig(
        filename=logging_fname,
        level=logging.INFO,
        format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s'
    )

    logging.info(f"Processing chunk: {chunk.index.min()} - {chunk.index.max()}")

    # Initialize a new WebDriver instance for each process
    browser = _init_browser(headless)

    dfs = []
    for _, row in chunk.iterrows():
        tournament_url = row['href']
        logging.info(f"Scraping tournament: {tournament_url}")

        browser.get(tournament_url)

        tournament_title = browser.find_element(By.CLASS_NAME, 'heading__name').text
        tournament_year = browser.find_element(By.CLASS_NAME, 'heading__info').text
        tournament_name = tournament_title + " - " + tournament_year

        try:
            df = scrape_matches(browser, tournament_name, tournament_url)
            dfs.append(df)
        except Exception as e:
            browser.close()
            browser = _init_browser(headless)
            logging.error(f"Error with {tournament_url}\n{type(e)} {e}")

    browser.quit()
    logging.info(f"Finished processing chunk: {chunk.index.min()} - {chunk.index.max()}")
    return pd.concat(dfs, ignore_index=True)


def main(tournaments_df, headless, logging_path):
    logging_fname = f"{logging_path}/logs/scrape_tournament_matches_log.log"

    if os.path.exists(logging_fname):
        os.remove(logging_fname)
    
    if len(tournaments_df) < cpu_count():
        num_processes = 1
    else:
        num_processes = cpu_count() // 2  # consider that this script spawns 2 windows per browser per process!
        
    # Split the dataframe into chunks
    chunk_size = len(tournaments_df) // num_processes

    args = []
    for i in range(0, len(tournaments_df), chunk_size):
        chunk = tournaments_df.iloc[i:i+chunk_size]
        arg = (chunk, headless, logging_fname)
        args.append(arg)

    pool = Pool(processes=num_processes)
    returned_dfs = pool.starmap(get_matches, args)
    print(type(returned_dfs[0]))

    return pd.concat(returned_dfs, ignore_index=True)

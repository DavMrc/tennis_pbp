from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from multiprocessing import Pool, cpu_count
import logging
import os
import pandas as pd
import sys
sys.path.append(os.getcwd())

from init_browser import _init_browser


def scrape_tournaments_href(headless):
    browser = _init_browser(headless)
    wait = WebDriverWait(browser, 10)

    # Click the drop down of the tournaments
    # to list all of them
    drop_down = wait.until(
        EC.presence_of_element_located((By.ID, 'lmenu_5724'))
    )
    browser.execute_script("arguments[0].scrollIntoView();", drop_down)
    drop_down.click()

    # parse the tournaments
    tournaments_div = wait.until(
        EC.presence_of_element_located((By.CLASS_NAME, "lmc__block.lmc__blockOpened"))
    )
    tournaments_list = tournaments_div.find_elements(By.CLASS_NAME, "lmc__templateHref")
    
    # Add all the tournaments' seasons there are info about
    tournaments_href = [t.get_attribute('href') for t in tournaments_list]

    browser.close()

    return tournaments_href


def scrape_tournaments_data(chunk, headless, logging_path):
    # Configure logging
    logging.basicConfig(
        filename=f"{logging_path}/logs/scrape_tournaments_log.log",
        level=logging.INFO,
        format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s'     
    )

    browser = _init_browser(headless)
    wait = WebDriverWait(browser, 10)

    tournaments_data = []
    for t_href in chunk:
        logging.info(f"Processing tournament {t_href}")
        browser.get(t_href)
        tournament_city_and_surface_div = "_link_1mowf_5._linkBase_1mowf_12._primary_1mowf_30.wclLeagueHeader__textColor"
        tournament_city_and_surface = browser.find_element(By.CLASS_NAME, tournament_city_and_surface_div).text

        browser.get(t_href+'archive/')

        image_url = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, 'heading__logo'))
        ).get_property('src')

        name = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, 'heading__name'))
        ).text

        seasons_divs = wait.until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, 'archive__season'))
        )
        for s in seasons_divs[1:]:  # Skip first div as it is only a header
            a_tag = s.find_element(By.CLASS_NAME, "archive__text")
            tournaments_data.append({
                'name': name,
                'href': t_href,
                'image_url': image_url,
                'city_and_surface': tournament_city_and_surface,
                'year': a_tag.text[-4:],
                'href': a_tag.get_property('href')+'results/'
            })
    
    browser.close()

    logging.info("Finished processing chunk")
    
    return pd.DataFrame(tournaments_data)


def main(headless, logging_path=os.getcwd()):
    tournaments_href = scrape_tournaments_href(headless)
    
    if len(tournaments_href) < cpu_count()-2:
        num_processes = 1
    else:
        num_processes = cpu_count()-2
    # Split the dataframe into chunks
    chunk_size = len(tournaments_href) // num_processes

    args = []
    for i in range(0, len(tournaments_href), chunk_size):
        chunk = tournaments_href[i:i+chunk_size]
        arg = (chunk, headless, logging_path)
        args.append(arg)

    pool = Pool(processes=num_processes)
    returned_dfs = pool.starmap(scrape_tournaments_data, args)

    return pd.concat(returned_dfs, ignore_index=True)


if __name__ == "__main__":
    df = main(True, os.getcwd())

    df.to_excel('./dada.xlsx', 'Sheet1', index=False)

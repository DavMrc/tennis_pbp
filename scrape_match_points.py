from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import selenium.common.exceptions as SeleniumExceptions
from multiprocessing import Pool, cpu_count
import logging
import os
import pandas as pd
import re
import sys
sys.path.append(os.getcwd())

from init_browser import _init_browser


# Check if page has pbp scores
def check_if_pbp_scores(browser):
    item_class = 'filterOver.filterOver--indent'
    found_elem = False
    
    try:
        browser.find_element(By.CLASS_NAME, item_class)
        found_elem = True
    except SeleniumExceptions.NoSuchElementException as e:
        found_elem = False

    return found_elem


# Find sets played
def scrape_sets(browser):
    sets_elems = WebDriverWait(browser, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "_tabs_1b0gr_5._tabsTertiary_1b0gr_75"))
    ).find_elements(    # find all a children of the above div
        By.TAG_NAME,
        "a",
    )
    # Get the href (used later for navigating) and set names
    sets_href = [e.get_attribute('href') for e in sets_elems]
    sets_names = [e.get_attribute('title')[-1] for e in sets_elems]

    return sets_href, sets_names


# Find games scores
def scrape_games_data(browser, set_id:str, players_data:dict):
    games_rows = browser.find_elements(By.CLASS_NAME, 'matchHistoryRow')
    games_data = []

    is_tiebreak = False
    for i, game in enumerate(games_rows, start=1):
        if is_tiebreak:
            game_id = 13
        else:
            game_id = i

        # Parse the game's score
        score_raw = game.find_element(
            By.CLASS_NAME,
            'matchHistoryRow__scoreBox'
        ).get_attribute('innerHTML')
        # removes the little top score that indicates
        # the final score of the tie break
        score_raw = re.sub(r"<sup>\d*</sup>", "", score_raw)
        score_raw = re.findall(r"\d{1,2}", score_raw)
        score = '-'.join(score_raw)

        if score == '7-6' or score == '6-7':
            # Will be evalued at the next iteration
            is_tiebreak = True

        # Parse if it's the home player's serve or the away player's serve
        home_serving = game.find_element(
            By.CLASS_NAME,
            'matchHistoryRow__servis.matchHistoryRow__home'
        ).get_attribute('innerHTML')

        away_serving = game.find_element(
            By.CLASS_NAME,
            'matchHistoryRow__servis.matchHistoryRow__away'
        ).get_attribute('innerHTML')

        # Parse if home player or away player has lost serve
        home_lost_serve = game.find_element(
            By.CLASS_NAME,
            'matchHistoryRow__lostServe.matchHistoryRow__home'
        ).text

        away_lost_serve = game.find_element(
            By.CLASS_NAME,
            'matchHistoryRow__lostServe.matchHistoryRow__away'
        ).text

        data = {
            'match_url': browser.current_url[:-2],
            'game_id': game_id,
            'set_id': set_id,
            'score': score,
            'home_serving': home_serving != '',
            'away_serving': away_serving != '',
            'home_lost_serve': home_lost_serve == 'LOST SERVE',
            'away_lost_serve': away_lost_serve == 'LOST SERVE'
        }
        data.update(players_data)

        # Generate a dict "row" and append it
        games_data.append(data)

    return games_data


# Find point-by-point scores
def scrape_games_fifteens_data(browser, set_id:str):
    games_fifteens_rows = browser.find_elements(By.CLASS_NAME, 'matchHistoryRow__fifteens')
    games_fifteens_data = []

    for i, game in enumerate(games_fifteens_rows, start=1):
        games_fifteens_data.append({
            'game_id': i,
            'set_id': set_id,
            'score': game.text
        })

    return games_fifteens_data


# Find players info
def scrape_player_detail(browser, match_url):
    browser.get(match_url)

    d = {}
    for pl_class in ["duelParticipant__home", "duelParticipant__away"]:
        player_div = browser.find_element(By.CLASS_NAME, pl_class)
        player_href = player_div.find_element(By.CLASS_NAME, "participant__participantLink").get_property("href")

        current_url = browser.current_url

        browser.get(player_href)

        player_type = "home" if "home" in pl_class else "away"
        player_name = browser.find_element(By.CLASS_NAME, "heading__name").text
        player_nationality = browser.find_element(By.CLASS_NAME, "breadcrumb__text").text
        player_img_url = browser.find_element(By.CLASS_NAME, "heading__logo.heading__logo--1").get_property("src")

        browser.get(current_url)

        d['players_match'] = match_url
        d[f'player_{player_type}'] = player_name
        d[f'player_{player_type}_nationality'] = player_nationality
        d[f'player_{player_type}_img_url'] = player_img_url
    
    return d


def assemble_df(games_data, games_fifteens_data):
    if len(games_data) < 1:
        return pd.DataFrame()
    else:
        games_data_df = pd.DataFrame(games_data)
        games_fifteens_df = pd.DataFrame(games_fifteens_data)

        df = pd.merge(
            games_data_df,
            games_fifteens_df,
            on=['game_id', 'set_id'],
            suffixes=['_left', '_right'],
            how='left'
        )

        df.rename({
            'score_left': 'game_score',
            'score_right': 'pbp_score'
        }, axis=1, inplace=True)

        return df


def scrape_match(chunk, headless, logging_fname):
    # Configure logging
    logging.basicConfig(
        filename=logging_fname,
        level=logging.INFO,
        format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s'
    )

    browser = _init_browser(headless)

    matches = []
    fails = []
    for _, row in chunk.iterrows():
        match_url = row['match_url']

        try:
            logging.info(f"Processing match {match_url}")
            browser.get(match_url)

            if check_if_pbp_scores(browser):
                sets_href, sets_names = scrape_sets(browser)
                players_data = scrape_player_detail(browser, match_url)

                games_data = []
                games_fifteens_data = []
                for ix in range(len(sets_href)):
                    href = sets_href[ix]
                    set_id = sets_names[ix]

                    browser.get(href)

                    game_data = scrape_games_data(browser, set_id, players_data)
                    game_fifteens_data = scrape_games_fifteens_data(browser, set_id)
                    
                    games_data.extend(game_data)
                    games_fifteens_data.extend(game_fifteens_data)
            
                matches.append(assemble_df(games_data, games_fifteens_data))
            
            else:
                logging.warn(f"Skipped {match_url} as it does not contain pbp data")
                fails.append({
                    'match_url': match_url,
                    'reason': 'Missing pbp data'
                })
 
        except Exception as e:
            browser.close()
            browser = _init_browser(headless)
            logging.error(f"Error with {match_url}\n{type(e)} {e}")

            fails.append({
                'match_url': match_url,
                'reason': str(e)
            })

    browser.close()

    logging.info("Finished processing batch")

    matches_df = None
    if len(matches) < 1:
        matches_df = pd.DataFrame()
    else:
        matches_df = pd.concat(matches, ignore_index=True)

    fails_df = pd.DataFrame(fails)
    return matches_df, fails_df


def main(matches, headless, logging_path=os.getcwd()):
    logging_fname = f"{logging_path}/logs/scrape_matches_points_log.log"

    if os.path.exists(logging_fname):
        os.remove(logging_fname)
    
    if len(matches) < cpu_count()-2:
        num_processes = 1
    else:
        num_processes = cpu_count()-2

    # Split the dataframe into chunks
    chunk_size = len(matches) // num_processes

    args = []
    for i in range(0, len(matches), chunk_size):
        chunk = matches[i:i+chunk_size]
        arg = (chunk, headless, logging_fname)
        args.append(arg)

    pool = Pool(processes=num_processes)
    returned_values = pool.starmap(scrape_match, args)

    matches = []
    fails = []  # matches that failed while scraping
    for tupla in returned_values:
        matches.append(tupla[0])
        fails.append(tupla[1])

    matches_df = None
    fails_df = None
    if len(matches) < 1:
        matches_df = pd.DataFrame()
    else:
        matches_df = pd.concat(matches, ignore_index=True)
    
    if len(fails) < 1:
        fails_df = pd.DataFrame()
    else:
        fails_df = pd.concat(fails, ignore_index=True)

    return matches_df, fails_df

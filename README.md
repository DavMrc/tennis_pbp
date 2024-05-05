# 🎾 Tennis Point By Point 🎾

## A quick consideration regarding privacy
I have consciously **omitted** the website that I used for scraping since legislation regarding web scraping is, at this time, unclear.

Therefore, the included scripts can only be used in conjunction with another script called `init_browser.py` that has been left out from all commits.

## Content of the scripts
In this repository I collect the Python scripts that I used for web scraping all tennis ATP singles tournaments data that I was able to find and that I think can be useful.

This does not include
- challenger and ITF matches
- doubles
- qualifications matches to the main draw of a tournament
- juniors, legends, beneficiary events and some more matches of various kind

What these scripts **do** collect is
- tournaments data: name, year and at-the-time logo
- tournaments matches: all matches played during a tournament from first round to finals
- match points: a row-by-row representation of all points played during a match between to ATP players, including details regarding
    - which player is serving
    - which player won the point
    - which player won the game
    - if the point is a break, set and/or match point
    - if the set ended in a tie-break
- players data: name, country and image (omitted ranking since it changes weekly and it wouldn't make sense with the rest of the data)


## Data collection and preparation
Data collection and preparation has been orchestrated using [Knime Analytics Platform](https://www.knime.com/).

Workflows are publicly available [at my Knime Community Hub public space](https://hub.knime.com/marcandavi2/spaces/Public/Tennis%20PBP~QNJRv_r4064ams4P/)

Data itself might be shared in the future.

## Possible additions
- Retrieve per-set statistics
- Retrieve pre-match odds
- Retrieve per-set
- Players head-to-head
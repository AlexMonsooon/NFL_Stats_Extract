import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment
import time
import os
from datetime import datetime

# Retrieve the HTML structure of the page
def get_soup(url):
    response = requests.get(url)
    time.sleep(5)  
    return BeautifulSoup(response.content, 'html.parser')


# Tables are stacked have to remove headings completely (decompose)
# We need to extract the href link (would just be "boxscore" without it)
# returns a df 
def clean_table_html(table, games):
    classes_to_remove = ['over_header', 'thead']
    for class_name in classes_to_remove:
        el = table.find_all(class_=class_name)
        if el:
            for e in el:
                e.decompose()
                
    if games == False:
        games_td = table.find_all("td", {"data-stat": "boxscore_word"})
        
        for game in games_td:
            anchor = game.find("a")
            if anchor:
                href = anchor['href']
                anchor.string =  href
    
    return pd.read_html(str(table))[0]


# the intital url we use to grab the season games does not use the comments...
# The tables are wrapped in comments on every href url retrieved
# We find the table by its id and put it in a dict
def extract_tables(soup, tnames, tables, use_comments=True):    
    if use_comments:
        comments = soup.find_all(text=lambda text: isinstance(text, Comment))
    else:
        comments = [None]  # Use a placeholder to ensure loop runs at least once
    
    for comment in comments:
        if comment is not None:
            comment_soup = BeautifulSoup(comment, 'html.parser')
        else:
            comment_soup = soup

        for x in tnames:
            table = comment_soup.find('table', {'id': x})
    
            if table:
                df = clean_table_html(table, use_comments)
                tables[x] = df
                break
            
    return tables


# extract other game info and scorebox
# return a dataframe 
def scorebox(soup):
    score_containers = soup.find(class_='scorebox')
    children = score_containers.findChildren("div", recursive=False)
    
    away_name, home_name = [child.find('strong').text.strip() for child in children[:2]]
    
    # Find the score values
    scores = score_containers.find_all(class_='score')
    away_score, home_score = [int(score.text.strip()) for score in scores]
    
    # Record
    scores_div = soup.find_all('div', class_='scores')
    away_records, home_records = [div.find_next_sibling('div').text.strip() for div in scores_div]
    
    # Find the coach names
    team_names = score_containers.find_all(class_='datapoint')
    away_coach, home_coach = [team_names[i].text.strip().split(": ")[1] for i in range(2)]
    
    # Other info
    meta = score_containers.find(class_='scorebox_meta')
    divs = meta.find_all('div')
    date = divs[0].text.strip()
    dateo = datetime.strptime(date, '%A %b %d, %Y')
    formatted_date = dateo.strftime('%Y-%m-%d')
    
    stadium = divs[2].text.strip().split(": ")[1]
    attendance = divs[3].text.strip().split(": ")[1]
    glen = divs[4].text.strip().split(": ")[1]
    
    # Determine the win/loss
    if away_score > home_score:
        away_result = 1
        home_result = 0
    elif away_score < home_score:
        away_result = 0
        home_result = 1
    else:
        away_result = 1
        home_result = 1
    
    # Create dictionaries for away and home teams
    away_data = {
        'PF': away_score,
        'Result': away_result,
        'PA': home_score,
        'Opp': home_name,
        'Record': away_records,
        'Stadium': stadium,
        'Attendance': attendance,
        'Game-Len': glen,
        'Coach': away_coach,
        'Date': formatted_date,
        'HA': 0
    }
    
    home_data = {
        'PF': home_score,
        'Result': home_result,
        'PA': away_score,
        'Opp': away_name,
        'Record': home_records,
        'Stadium': stadium,
        'Attendance': attendance,
        'Game-Len': glen,
        'Coach': home_coach,
        'Date': formatted_date,
        'HA': 1

    }
    
    return pd.DataFrame.from_dict({away_name: away_data, home_name: home_data}, orient='index')


#used to see comm cols between dfs
def common_columns(df1, df2):
    col_df1 = set(df1.columns)
    col_df2 = set(df2.columns)
    common_cols = list(col_df1.intersection(col_df2))
    return common_cols


# Start the cleaning and merging process of the dfs
# returns 2 dfs 
def logica(soup, dataframes):
    dfa = scorebox(soup)
    dfa.columns = dfa.columns.str.strip('[]')
    dfa.reset_index(inplace=True)
    dfa = dfa.rename(columns={'index': 'FullTeam'})

    gameinfo = dataframes['game_info'].T
    gameinfo.columns = gameinfo.iloc[0]
    gameinfo = gameinfo.iloc[1:]
    gameinfo = gameinfo.reset_index(drop=True)
    
    team_stats = dataframes['team_stats'].T
    team_stats.columns = team_stats.iloc[0]
    team_stats = team_stats.iloc[1:].reset_index()
    team_stats.reset_index()
    team_stats = team_stats.rename(columns={'index': 'Tm'})

    # Makes len amount of gameinfo so i can concat
    gameinfo2 = pd.concat([gameinfo] * len(team_stats), ignore_index=True)
    com = pd.concat([gameinfo2, team_stats], axis=1).reset_index(drop=True)
    combo = pd.concat([dfa, com], axis=1)
    
    home_starters = set(dataframes['home_starters']['Player'])
    vis_starters = set(dataframes['vis_starters']['Player'])
    
    # see what players are starters from snap counts and add 1/0
    dataframes['home_snap_counts']['Starter'] = dataframes['home_snap_counts']['Player'].apply(lambda player: 1 if player in home_starters else 0)
    dataframes['vis_snap_counts']['Starter'] = dataframes['vis_snap_counts']['Player'].apply(lambda player: 1 if player in vis_starters else 0)
    
    z = pd.concat([dataframes['home_snap_counts'], dataframes['vis_snap_counts']], axis=0)
    
    # speicifc dfs we are going to extract
    mergie = ['passing_advanced', 'rushing_advanced', 'receiving_advanced',
              'defense_advanced', 'kicking']
    
    merged = {}
    for df in mergie:
        merged_df = pd.merge(dataframes[df], z, on=common_columns(dataframes[df], z), how='left')
        merged[df] = merged_df
        
    return merged, combo

    
# if file exists append to it, else make a new one with that filename
def save(dataframe, filename):
    mode = 'a' if os.path.exists(filename) else 'w'
    dataframe.to_csv(filename, mode=mode, header=not os.path.exists(filename), index=False)
    
    
# enter year of the season you want
# returns 6 csv files with stats
def main(year):
    tables = {}
    url = f'https://www.pro-football-reference.com/years/{year}/games.htm'
    tnames = ['kicking', 'home_snap_counts', 'vis_snap_counts',
               'passing_advanced', 'rushing_advanced', 'receiving_advanced', 
               'defense_advanced', 'home_starters', 'vis_starters', 'game_info', 'team_stats']
    
    soup = get_soup(url)
    games = extract_tables(soup, ['games'], tables, use_comments=False)
    
    gg = games['games']
    gg = gg[gg['Date'] != 'Playoffs']
    gg = gg.rename(columns={'Unnamed: 7': 'Link'})
    gg['Date'] = pd.to_datetime(gg['Date'], format='%Y-%m-%d')

    # remove redundant info by slicing gg
    slices = [slice(0, 5), slice(6, 8)]
    gg = gg.iloc[:, [col for s in slices for col in range(*s.indices(gg.shape[1]))]]

    allem = list(set(gg['Link'].dropna()))
    
    # should be around 284 games including playoffs
    # 285 x 5 (sleep time) = 23 min 45 seconds for completion
    # DO NOT EXCEED 20 REQUESTS A MIN
    for x in allem:
        
        soup = get_soup('https://www.pro-football-reference.com' + x)
        dataframes = extract_tables(soup, tnames, tables, use_comments=True)
        tar, bar = logica(soup, dataframes)
        
        bar['Link'] = 'https://www.pro-football-reference.com/boxscores/202209180gnb.htm'
        bar['Date'] = pd.to_datetime(bar['Date'], format='%Y-%m-%d')
        
        wombo = pd.merge(bar, gg, on=common_columns(bar, gg), how='left')
        save(wombo, f'NFL-Games{year}.csv')
        
        for key, df in tar.items():
            date_value = wombo.iloc[0]['Date']
            link_value = wombo.iloc[0]['Link']
            
            df['Date'] = date_value
            df['Link'] = link_value
            
            save(df, f'{key}-{year}.csv')
    
main(2022)

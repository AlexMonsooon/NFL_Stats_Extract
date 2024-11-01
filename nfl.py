import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment
import time
import os
from datetime import datetime, timedelta
import numpy as np

################################################################################
# Retrieve the HTML structure of the page
def get_soup(url):
    response = requests.get(url)
    time.sleep(5)  
    return BeautifulSoup(response.content, 'html.parser')

###############################################################################
# Tables are stacked have to remove headings completely (decompose)
# We need to extract the href link (would just be "boxscore" without it)
# returns a df 
##################################################################################
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

#################################################################################

# USE ME INSTEAD
# from io import StringIO

# def clean_table_html(table):
#     for class_name in ['over_header', 'thead']:
#         for el in table.find_all(class_=class_name):
#             el.decompose()
#     return pd.read_html(StringIO(str(table)))[0]

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
################################################################################

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
    records_div = soup.find_all('div', class_='scores')
    away_records, home_records = [div.find_next_sibling('div').text.strip() for div in records_div]
    
    # Find the coach names
    coach_names = score_containers.find_all(class_='datapoint')
    away_coach, home_coach = [coach_names[i].text.strip().split(": ")[1] for i in range(2)]
    
    # Other info
    meta = score_containers.find(class_='scorebox_meta')
    divs = meta.find_all('div')
    date = divs[0].text.strip()
    dateo = datetime.strptime(date, '%A %b %d, %Y')
    formatted_date = dateo.strftime('%Y-%m-%d')
    stadium = divs[2].text.strip().split(": ")[1]

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
        'Coach': away_coach,
        'Date': formatted_date,
        'HA': 0,
    }
    
    home_data = {
        'PF': home_score,
        'Result': home_result,
        'PA': away_score,
        'Opp': away_name,
        'Record': home_records,
        'Stadium': stadium,
        'Coach': home_coach,
        'Date': formatted_date,
        'HA': 1,
    }
    
    return pd.DataFrame.from_dict({away_name: away_data, home_name: home_data}, orient='index')

################################################################################
#used to see comm cols between dfs
def common_columns(df1, df2):
    col_df1 = set(df1.columns)
    col_df2 = set(df2.columns)
    common_cols = list(col_df1.intersection(col_df2))
    return common_cols

################################################################################
# Start the cleaning and merging process of the dfs
# returns 2 dfs 
def clean_merge_df(soup, dataframes):
    scorebox_df = scorebox(soup)
    scorebox_df.columns = scorebox_df.columns.str.strip('[]')
    scorebox_df.reset_index(inplace=True)
    scorebox_df = scorebox_df.rename(columns={'index': 'FullTeam'})

    gameinfo_df = dataframes['game_info'].T
    gameinfo_df.columns = gameinfo_df.iloc[0]
    gameinfo_df = gameinfo_df.iloc[1:]
    gameinfo_df = gameinfo_df.reset_index(drop=True)

    nulla = ['Won Toss', 'Won OT Toss', 'Roof', 'Surface', 'Duration', 'Attendance', 'Weather', 'Vegas Line', 'Over/Under']
    
    for x in nulla:
        if x not in gameinfo_df.columns:
            gameinfo_df[x] = np.nan
            
    gameinfo_df = gameinfo_df[nulla]
       
    team_stats_df = dataframes['team_stats'].T
    team_stats_df.columns = team_stats_df.iloc[0]
    team_stats_df = team_stats_df.iloc[1:].reset_index()
    team_stats_df.reset_index()
    team_stats_df = team_stats_df.rename(columns={'index': 'Tm'})

    # Makes len amount of gameinfo so i can concat
    gameinfo2 = pd.concat([gameinfo_df] * len(team_stats_df), ignore_index=True)
    com = pd.concat([gameinfo2, team_stats_df], axis=1).reset_index(drop=True)
    stats_game_info_df = pd.concat([scorebox_df, com], axis=1)
    
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
        
    return merged, stats_game_info_df
################################################################################
    
# if file exists append to it, else make a new one with that filename
def save(dataframe, filename):
    mode = 'a' if os.path.exists(filename) else 'w'
    dataframe.to_csv(filename, mode=mode, header=not os.path.exists(filename), index=False)
    
###############################################################################
def slicey(ds):
    slices = [slice(0, 4), slice(7, 8)]
    df = ds.iloc[:, [col for s in slices for col in range(*s.indices(ds.shape[1]))]]
    df = df.rename(columns={'Unnamed: 7': 'Link'})
    df.dropna(inplace=True)
    return df
    
################################################################################
# enter year of the season you want
# returns 11 csv files with stats
def main(season, flag):
    tables = {}
    url = f'https://www.pro-football-reference.com/years/{season}/games.htm'
    tnames = ['kicking', 'home_snap_counts', 'vis_snap_counts',
               'passing_advanced', 'rushing_advanced', 'receiving_advanced', 
               'defense_advanced', 'home_starters', 'vis_starters', 'game_info', 'team_stats',
               'officials', 'home_drives', 'vis_drives', 'pbp', 'player_defense']
    
    gamepath = f'NFL_Games-{season}.csv'
    used_matchups = f'NFL_Matchups-{season}.csv'
    all_matchups = f'NFL_ALL_Matchups-{season}.csv'
    

      # whole season extract or up to todays date
    if flag:
        soup = get_soup(url)
        games = extract_tables(soup, ['games'], tables, use_comments=False)
        
        games_df = games['games']
        games_df = games_df[games_df['Date'] != 'Playoffs']
        games_df['Date'] = pd.to_datetime(games_df['Date'], format='%Y-%m-%d')
        games_df = games_df.rename(columns={'Unnamed: 7': 'Link'})
        
        games_df['season'] = season
        
        games_df = games_df[['Week','Day','Date','Link','season']]
        save(games_df, all_matchups)

        
        # Get today's date and subtract one day
        today = datetime.now()
        yesterday = today - timedelta(days=3)
        games_df = games_df[games_df['Date'] <= yesterday]
        
        
        allgames = list(set(games_df['Link'].dropna()))
        
        
        temp_games_df = games_df[['Date', 'Link']]
        temp_games_df['Extracted'] = np.nan
        save(temp_games_df, used_matchups)

        
    
    ### get unextracted records from matchups_path 
    else:        
        print('STARTING FILTERED SEASON')
        games_df = pd.read_csv(all_matchups)
        used_games_df = pd.read_csv(used_matchups)
        
        merged_df = pd.merge(games_df, used_games_df, on=['Date', 'Link'], how='left')
        games_df = merged_df[merged_df['Extracted'] != 1]
        
        today = datetime.now()
        yesterday = today - timedelta(days=3)
        
        # Filter the dataframe for dates earlier or equal to yesterday
        games_df['Date'] = pd.to_datetime(games_df['Date'], format='%Y-%m-%d')
        games_df = games_df[games_df['Date'] <= yesterday]
        
        allgames = list(set(games_df['Link'].dropna()))
        
        len(allgames)
        
###############################################################################


    for link_url in allgames:
        print(link_url)
        
        ### waits 5 seconds between loops
        soup = get_soup('https://www.pro-football-reference.com' + link_url)
        dataframes = extract_tables(soup, tnames, tables, use_comments=True)
        rush_rec_pass_def_kick_dict, stats_game_info_df = clean_merge_df(soup, dataframes)
        
        stats_game_info_df['Date'] = pd.to_datetime(stats_game_info_df['Date'], format='%Y-%m-%d')
        stats_game_info_df['Link'] = link_url
        stats_game_info_df['season'] = season


        wombo = pd.merge(stats_game_info_df, games_df, on=common_columns(stats_game_info_df, games_df), how='left')
        save(wombo, gamepath)            
        date_value = wombo.iloc[0]['Date']
        
        for key, df in rush_rec_pass_def_kick_dict.items():
            df['Date'] = date_value
            df['Link'] = link_url
            df['season'] = season
        
        off_dict = {}
        pass_rush_rec_df = extract_tables(soup, ['player_offense'], off_dict, use_comments=False)
        filtered_off_df = pass_rush_rec_df['player_offense'][['Player', 'Tm', 'TD', 'TD.1','TD.2', 'Int', 'Yds.1', 'Lng', 'Rate', 'Lng.1', 'Lng.2', 'Fmb', 'FL']]
        filtered_off_df = filtered_off_df.rename(columns={'TD': 'Pass_TDs', 'TD.1': 'Rushing_TDs', 'TD.2': 'Receiving_TDs', 'Int': 'QB_Int', 'Yds.1': 'QB_SackedYards'})
        filtered_off_df = filtered_off_df.rename(columns={'Lng': 'Pass_Lng', 'Rate': 'QB_Rate', 'Lng.1': 'Rushing_Lng', 'Lng.2': 'Receiving_Lng', 'Fmb': 'Off_Fmb', 'FL': 'Off_Fmb_Lost'})
        filtered_off_df['Link'] = link_url
        
            
        temp_pass = rush_rec_pass_def_kick_dict['passing_advanced']
        pcol = ['Player', 'Tm', 'Link', 'Pass_TDs', 'QB_Int', 'QB_SackedYards', 'Pass_Lng', 'QB_Rate', 'Off_Fmb', 'Off_Fmb_Lost']
        pass_filt_df = filtered_off_df[pcol]
        merged_df = pd.merge(temp_pass, pass_filt_df, on=['Player', 'Tm', 'Link'], how='left')
        save(merged_df, f'NFL_Passing-{season}.csv')
        
        
        temp_rush = rush_rec_pass_def_kick_dict['rushing_advanced']   
        temp_rush = temp_rush.rename(columns={'TD': 'Rushing_TDs'})
        rushcol = ['Player', 'Tm', 'Link', 'Rushing_TDs', 'Rushing_Lng', 'Off_Fmb', 'Off_Fmb_Lost']
        rush_filt_df = filtered_off_df[rushcol]
        
        merged_df = pd.merge(temp_rush, rush_filt_df, on=['Player', 'Tm', 'Link', 'Rushing_TDs'], how='left')
        save(merged_df, f'NFL_Rushing-{season}.csv')
            
            
        temp_rec = rush_rec_pass_def_kick_dict['receiving_advanced']    
        temp_rec = temp_rec.rename(columns={'TD': 'Receiving_TDs'})
        reccol = ['Player', 'Tm', 'Link', 'Receiving_TDs', 'Receiving_Lng', 'Off_Fmb', 'Off_Fmb_Lost']
        rec_filt_df = filtered_off_df[reccol]
        
        merged_df = pd.merge(temp_rec, rec_filt_df, on=['Player', 'Tm', 'Link', 'Receiving_TDs'], how='left')
        save(merged_df, f'NFL_Receiving-{season}.csv')     
            
            
    
        filtered_defense_df = dataframes['player_defense'][['Player', 'Tm', 'PD', 'TFL', 'QBHits', 'FR', 'FF']]
        filtered_defense_df['Link'] = link_url
            
        temp_df = rush_rec_pass_def_kick_dict['defense_advanced']         
        merged_df = pd.merge(temp_df, filtered_defense_df, on=['Player', 'Tm', 'Link'], how='left')
        save(merged_df, f'NFL_Defense-{season}.csv')


        ## save all dfs now
        kicking = dataframes['kicking']
        kicking['Date'] = date_value
        kicking['Link'] = link_url
        kicking['season'] = season
        save(kicking, f'NFL_Kicking-{season}.csv')

        officials = dataframes['officials']
        officials = officials.rename(columns={0: 'Pos', 1: 'Name'})
        officials['Date'] = date_value
        officials['Link'] = link_url
        officials['season'] = season
        save(officials, f'NFL_Officials-{season}.csv')
        
        home_starters = dataframes['home_starters']
        vis_starters = dataframes['vis_starters']

        starters = pd.concat([home_starters, vis_starters], ignore_index=True)
        starters['Date'] = date_value
        starters['Link'] = link_url
        starters['season'] = season
        save(starters, f'NFL_Starters-{season}.csv')

        
        #### merge starters together

        home_drives = dataframes['home_drives']
        vis_drives = dataframes['vis_drives']
        
        drives = pd.concat([home_drives, vis_drives], ignore_index=True)
        drives['Date'] = date_value
        drives['Link'] = link_url
        drives['season'] = season
        save(drives, f'NFL_Drives-{season}.csv')

        pbp_df = dataframes['pbp']
        pbp_df['Date'] = date_value
        pbp_df['Link'] = link_url
        pbp_df['season'] = season
        save(pbp_df, f'NFL_Pbp-{season}.csv')
        
        snap_counts_df = pd.concat([dataframes['home_snap_counts'], dataframes['vis_snap_counts']], ignore_index=True)
        snap_counts_df['Date'] = date_value
        snap_counts_df['Link'] = link_url
        snap_counts_df['season'] = season
        save(snap_counts_df, f'NFL_Snap_Counts-{season}.csv')
        
        temp_df_game = pd.read_csv(used_matchups)
        new_record = {'Date': date_value, 'Link': link_url, 'Extracted': 1.0}
        df = pd.concat([temp_df_game, pd.DataFrame([new_record])], ignore_index=True)
        
        df.to_csv(used_matchups, index=False)  # Overwrite the original file
        
###############################################################################
################################################################################


### will get - 3 days from current date... so it will get full seasons if previous years called
### will check matchups_path to see if getting data from current season
main(2024, False)







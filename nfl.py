# -*- coding: utf-8 -*-
"""
Created on Sat Dec 14 15:23:05 2024

@author: Alex
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment
import time
import os
from datetime import datetime, timedelta
import numpy as np
from io import StringIO

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
        classes_to_remove = table.find_all(class_=class_name)
        
        if classes_to_remove:
            for class_remove in classes_to_remove:
                class_remove.decompose()
    
    ## only run if use_comments=False            
    if games == False:
        games_td = table.find_all("td", {"data-stat": "boxscore_word"})
        
        for game in games_td:
            anchor = game.find("a")
            
            # This is for the Link col
            if anchor:
                href = anchor['href']
                anchor.string =  href
    
    with StringIO(str(table)) as buffer:
        return pd.read_html(buffer)[0]

#################################################################################

# the intital url we use to grab the season games does not use the comments...
# Some tables are wrapped in comments
# We find the table by its id and put it in a dict
def extract_tables(soup, tnames, tables, use_comments=True):    
    if use_comments:
        comments = [
            comment for comment in soup.find_all(string=lambda string: isinstance(string, Comment))
            if '<table' in str(comment)  # remove comments that don't have table tag
        ]
        
    else:
        comments = [None]  # ensure loop runs once if use_comments=False
    
    for comment in comments:
        if comment is not None:
            soup = BeautifulSoup(comment, 'html.parser')

        for x in tnames:
            table = soup.find('table', {'id': x})
    
            if table:
                df = clean_table_html(table, use_comments)
                tables[x] = df
            
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
    
    # extract date, start time and stadium
    meta = score_containers.find(class_='scorebox_meta')
    divs = meta.find_all('div')
    
    date = divs[0].text.strip()
    dateo = datetime.strptime(date, '%A %b %d, %Y')
    formatted_date = dateo.strftime('%Y-%m-%d')
    
    ### sometimes missing stadium and Start time, make sure to grab right value
    ### else just set value to nan
    start_time, stadium = np.nan, np.nan
    for div in divs:
        text = div.text.strip()
        if text.startswith("Start Time:"):
            start_time = text.split(": ")[1]
        elif text.startswith("Stadium:"):
            stadium = text.split(": ")[1]

    #start_time = divs[1].text.strip().split(": ")[1]
    #stadium = divs[2].text.strip().split(": ")[1]

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
        'Start_Time': start_time,
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
        'Start_Time': start_time,
        'Stadium': stadium,
        'Coach': home_coach,
        'Date': formatted_date,
        'HA': 1,
    }
    
    return pd.DataFrame.from_dict({away_name: away_data, home_name: home_data}, orient='index'), away_name, home_name

################################################################################
# common cols between 2 dfs
def common_columns(df1, df2):
    col_df1 = set(df1.columns)
    col_df2 = set(df2.columns)
    return list(col_df1.intersection(col_df2))

################################################################################
# Start the cleaning and merging process of the dfs
# returns dict with starter values, game info df, away team, and home team
def clean_merge_df(soup, dataframes):
    scorebox_df, away_name, home_name = scorebox(soup)
    scorebox_df.columns = scorebox_df.columns.str.strip('[]')
    scorebox_df = scorebox_df.reset_index().rename(columns={'index': 'FullTeam'})

    gameinfo_df = dataframes['game_info'].T
    gameinfo_df = gameinfo_df.set_axis(gameinfo_df.iloc[0], axis=1).iloc[1:].reset_index(drop=True)

    ## if col not found in df, make it nan
    nulla = ['Won Toss', 'Won OT Toss', 'Roof', 'Surface', 'Duration', 'Attendance', 'Weather', 'Vegas Line', 'Over/Under']
    for x in nulla:
        if x not in gameinfo_df.columns:
            gameinfo_df[x] = np.nan
            
    gameinfo_df = gameinfo_df[nulla]
       
    team_stats_df = dataframes['team_stats'].T
    team_stats_df = team_stats_df.set_axis(team_stats_df.iloc[0], axis=1).iloc[1:].reset_index().rename(columns={'index': 'Tm'})

    # Makes len amount of team stats to concat, should always be 2 
    gameinfo2 = pd.concat([gameinfo_df] * len(team_stats_df), ignore_index=True)
    com = pd.concat([gameinfo2, team_stats_df], axis=1).reset_index(drop=True)
    stats_game_info_df = pd.concat([scorebox_df, com], axis=1)
    
        
    return stats_game_info_df, away_name, home_name
################################################################################
    
# if file exists append to it, else make a new one with that filename
def save(dataframe, filename):
    mode = 'a' if os.path.exists(filename) else 'w'
    dataframe.to_csv(filename, mode=mode, header=not os.path.exists(filename), index=False)
    
###############################################################################
################################################################################
# enter year of the season you want
# saves 13 csv files with stats
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
    
    team_mapping = {
        'BAL': 'Baltimore Ravens', 'KAN': 'Kansas City Chiefs', 'GNB': 'Green Bay Packers',
        'PHI': 'Philadelphia Eagles', 'ATL': 'Atlanta Falcons', 'PIT': 'Pittsburgh Steelers',
        'ARI': 'Arizona Cardinals', 'BUF': 'Buffalo Bills', 'CHI': 'Chicago Bears',
        'TEN': 'Tennessee Titans', 'CIN': 'Cincinnati Bengals', 'NWE': 'New England Patriots',
        'CLE': 'Cleveland Browns', 'DAL': 'Dallas Cowboys', 'HOU': 'Houston Texans',
        'IND': 'Indianapolis Colts', 'DET': 'Detroit Lions', 'LAR': 'Los Angeles Rams',
        'JAX': 'Jacksonville Jaguars', 'MIA': 'Miami Dolphins', 'CAR': 'Carolina Panthers',
        'NOR': 'New Orleans Saints', 'MIN': 'Minnesota Vikings', 'NYG': 'New York Giants',
        'LAC': 'Los Angeles Chargers', 'LVR': 'Las Vegas Raiders', 'DEN': 'Denver Broncos',
        'SEA': 'Seattle Seahawks', 'TAM': 'Tampa Bay Buccaneers', 'WAS': 'Washington Commanders',
        'NYJ': 'New York Jets', 'SFO': 'San Francisco 49ers'
    }
    reverse_team_mapping = {v: k for k, v in team_mapping.items()}

    
    # whole season extract or up to todays date - 3 days
    if flag:
        soup = get_soup(url)
        games = extract_tables(soup, ['games'], tables, use_comments=False)
        
        games_df = games['games'][games['games']['Date'] != 'Playoffs']
        games_df.loc[:, 'Date'] = pd.to_datetime(games_df['Date'], format='%Y-%m-%d').dt.date
        
        games_df = games_df.rename(columns={'Unnamed: 7': 'Link'})
        games_df['Season'] = season
        
        games_df = games_df[['Week','Day','Date','Link','Season']]
        
        # Week,Day,Date,Link,season
        save(games_df, all_matchups)

        
        # Get today's date and subtract 3 days
        yesterday = (datetime.now() - timedelta(days=3)).date()
        games_df = games_df[games_df['Date'] <= yesterday]
        
        allgames = list(set(games_df['Link'].dropna()))
        
        temp_games_df = games_df[['Date', 'Link']].copy()
        temp_games_df.loc[:, 'Extracted'] = np.nan
        
        # Date,Link,Extracted
        save(temp_games_df, used_matchups)

        
    
    ### get unextracted records from matchups_path 
    else:        
        print('STARTING FILTERED SEASON')
        games_df = pd.read_csv(all_matchups)
        used_games_df = pd.read_csv(used_matchups)
        
        merged_df = pd.merge(games_df, used_games_df, on=['Date', 'Link'], how='left')
        games_df = merged_df[merged_df['Extracted'] != 1]
        
        yesterday = (datetime.now() - timedelta(days=3)).date()

        # Filter the dataframe for dates earlier or equal to 3 days ago
        games_df.loc[:, 'Date'] = pd.to_datetime(games_df['Date'], format='%Y-%m-%d').dt.date
        games_df = games_df[games_df['Date'] <= yesterday]
        
        allgames = list(set(games_df['Link'].dropna()))
                
###############################################################################

    for link_url in allgames:
        print(link_url)
        
        ### waits 5 seconds between loops
        soup = get_soup('https://www.pro-football-reference.com' + link_url)
        dataframes = extract_tables(soup, tnames, tables, use_comments=True)
        
        stats_game_info_df, away_name, home_name = clean_merge_df(soup, dataframes)
        
        stats_game_info_df['Date'] = pd.to_datetime(stats_game_info_df['Date'], format='%Y-%m-%d').dt.date
        stats_game_info_df['Link'] = link_url
        stats_game_info_df['Season'] = season
        
        nfl_games_df = pd.merge(stats_game_info_df, games_df, on=common_columns(stats_game_info_df, games_df), how='left')
        date_value = nfl_games_df.iloc[0]['Date']

        # FullTeam,PF,Result,PA,Opp,Record,Start_Time,Stadium,Coach,Date,HA,Won Toss,
        # Won OT Toss,Roof,Surface,Duration,Attendance,Weather,Vegas Line,Over/Under,
        # Tm,First Downs,Rush-Yds-TDs,Cmp-Att-Yd-TD-INT,Sacked-Yards,Net Pass Yards,
        # Total Yards,Fumbles-Lost,Turnovers,Penalties-Yards,Third Down Conv.,
        # Fourth Down Conv.,Time of Possession,Link,season,Week,Day
        
        save(nfl_games_df, gamepath)  
        
        #######################################################################
        # dataframes already used
        items_to_remove = ['game_info', 'team_stats']
        tnames = [item for item in tnames if item not in items_to_remove]
        
        # Loop through dfs and update only the keys present in new tnames
        for key in tnames:
            if key in dataframes:
                df = dataframes[key]
                df['Date'] = date_value
                df['Link'] = link_url
                df['Season'] = season
                
        #######################################################################
        home_starters = set(dataframes['home_starters']['Player'])
        vis_starters = set(dataframes['vis_starters']['Player'])
        
        dataframes['home_snap_counts']['Starter'] = dataframes['home_snap_counts']['Player'].apply(lambda player: 1 if player in home_starters else 0)
        dataframes['vis_snap_counts']['Starter'] = dataframes['vis_snap_counts']['Player'].apply(lambda player: 1 if player in vis_starters else 0)
        
        dataframes['home_snap_counts']['Tm'] = home_name
        dataframes['vis_snap_counts']['Tm'] = away_name
        
        snap_counts_df = pd.concat([dataframes['home_snap_counts'], dataframes['vis_snap_counts']], ignore_index=True)
        snap_counts_df['Tm'] = snap_counts_df['Tm'].replace(reverse_team_mapping)
        
        # Player,Pos,Num,Pct,Num.1,Pct.1,Num.2,Pct.2,Starter,Tm,Date,Link,season
        save(snap_counts_df, f'NFL_Snap_Counts-{season}.csv')

        ########################################################################   
        merged_dfs = ['passing_advanced', 'rushing_advanced', 'receiving_advanced','defense_advanced']
        
        # loops through merged_dfs and adds 1/0 to each df in Starter col
        for df_name in merged_dfs:
            merged_df = pd.merge(dataframes[df_name], snap_counts_df, on=common_columns(dataframes[df_name], snap_counts_df), how='left')
            dataframes[df_name] = merged_df

        #######################################################################
        off_dict = {}
        pass_rush_rec_df = extract_tables(soup, ['player_offense'], off_dict, use_comments=False)
        filtered_off_df = pass_rush_rec_df['player_offense'][['Player', 'Tm', 'TD', 'TD.1','TD.2', 'Int', 'Yds.1', 'Lng', 'Rate', 'Lng.1', 'Lng.2', 'Fmb', 'FL']]
        filtered_off_df = filtered_off_df.rename(columns={'TD': 'Pass_TDs', 'TD.1': 'Rushing_TDs', 'TD.2': 'Receiving_TDs', 'Int': 'QB_Int', 'Yds.1': 'QB_SackedYards'})
        filtered_off_df = filtered_off_df.rename(columns={'Lng': 'Pass_Lng', 'Rate': 'QB_Rate', 'Lng.1': 'Rushing_Lng', 'Lng.2': 'Receiving_Lng', 'Fmb': 'Off_Fmb', 'FL': 'Off_Fmb_Lost'})
        filtered_off_df['Link'] = link_url
        
        temp_pass = dataframes['passing_advanced']
        pcol = ['Player', 'Tm', 'Link', 'Pass_TDs', 'QB_Int', 'QB_SackedYards', 'Pass_Lng', 'QB_Rate', 'Off_Fmb', 'Off_Fmb_Lost']
        pass_filt_df = filtered_off_df[pcol]
        merged_df = pd.merge(temp_pass, pass_filt_df, on=['Player', 'Tm', 'Link'], how='left')
        
        # Player,Tm,Cmp,Att,Yds,1D,1D%,IAY,IAY/PA,CAY,CAY/Cmp,CAY/PA,YAC,YAC/Cmp,
        # Drops,Drop%,BadTh,Bad%,Sk,Bltz,Hrry,Hits,Prss,Prss%,Scrm,Yds/Scr,Pos,
        # Num,Pct,Num.1,Pct.1,Num.2,Pct.2,Starter,Date,Link,season,Pass_TDs,QB_Int,
        # QB_SackedYards,Pass_Lng,QB_Rate,Off_Fmb,Off_Fmb_Lost
        
        save(merged_df, f'NFL_Passing-{season}.csv')       
        
        #######################################################################
        temp_rush = dataframes['rushing_advanced']   
        temp_rush = temp_rush.rename(columns={'TD': 'Rushing_TDs'})
        rushcol = ['Player', 'Tm', 'Link', 'Rushing_TDs', 'Rushing_Lng', 'Off_Fmb', 'Off_Fmb_Lost']
        rush_filt_df = filtered_off_df[rushcol]
        merged_df = pd.merge(temp_rush, rush_filt_df, on=['Player', 'Tm', 'Link', 'Rushing_TDs'], how='left')
        
        # Player,Tm,Att,Yds,Rushing_TDs,1D,YBC,YBC/Att,YAC,YAC/Att,BrkTkl,Att/Br,
        # Pos,Num,Pct,Num.1,Pct.1,Num.2,Pct.2,Starter,Date,Link,season,Rushing_Lng,
        # Off_Fmb,Off_Fmb_Lost
        
        save(merged_df, f'NFL_Rushing-{season}.csv')
        
        #######################################################################            
        temp_rec = dataframes['receiving_advanced']    
        temp_rec = temp_rec.rename(columns={'TD': 'Receiving_TDs'})
        reccol = ['Player', 'Tm', 'Link', 'Receiving_TDs', 'Receiving_Lng', 'Off_Fmb', 'Off_Fmb_Lost']
        rec_filt_df = filtered_off_df[reccol]
        
        merged_df = pd.merge(temp_rec, rec_filt_df, on=['Player', 'Tm', 'Link', 'Receiving_TDs'], how='left')
        
        # Player,Tm,Tgt,Rec,Yds,Receiving_TDs,1D,YBC,YBC/R,YAC,YAC/R,ADOT,BrkTkl,
        # Rec/Br,Drop,Drop%,Int,Rat,Pos,Num,Pct,Num.1,Pct.1,Num.2,Pct.2,Starter,
        # Date,Link,season,Receiving_Lng,Off_Fmb,Off_Fmb_Lost
        
        save(merged_df, f'NFL_Receiving-{season}.csv')    
        
        #######################################################################
        filtered_defense_df = dataframes['player_defense'][['Player', 'Tm', 'PD', 'TFL', 'QBHits', 'FR', 'FF', 'Link']]
        temp_df = dataframes['defense_advanced']         
        merged_df = pd.merge(temp_df, filtered_defense_df, on=['Player', 'Tm', 'Link'], how='left')
        
        # Player,Tm,Int,Tgt,Cmp,Cmp%,Yds,Yds/Cmp,Yds/Tgt,TD,Rat,DADOT,Air,YAC,Bltz,
        # Hrry,QBKD,Sk,Prss,Comb,MTkl,MTkl%,Pos,Num,Pct,Num.1,Pct.1,Num.2,Pct.2,
        # Starter,Date,Link,season,PD,TFL,QBHits,FR,FF
        
        save(merged_df, f'NFL_Defense-{season}.csv')

        ########################################################################
        kicking = dataframes['kicking']
        # Player,Tm,XPM,XPA,FGM,FGA,Pnt,Yds,Y/P,Lng,Date,Link,season
        save(kicking, f'NFL_Kicking-{season}.csv')

        ########################################################################
        dataframes['home_starters']['Tm'] = home_name
        dataframes['vis_starters']['Tm'] = away_name

        starters = pd.concat([dataframes['home_starters'], dataframes['vis_starters']], ignore_index=True)
        starters['Tm'] = starters['Tm'].replace(reverse_team_mapping)
        
        # Player,Pos,Tm,Date,Link,season

        save(starters, f'NFL_Starters-{season}.csv')

        #########################################################################
        dataframes['home_drives']['Tm'] = home_name
        dataframes['vis_drives']['Tm'] = away_name
        
        drives = pd.concat([dataframes['home_drives'], dataframes['vis_drives']], ignore_index=True)
        
        drives = drives.rename(columns={'#': 'Drive_Num'})
        drives['Tm'] = drives['Tm'].replace(reverse_team_mapping)
        
        # Drive_Num,Quarter,Time,LOS,Plays,Length,Net Yds,Result,Tm,Date,Link,season
        save(drives, f'NFL_Drives-{season}.csv')

        #######################################################################
        officials = dataframes['officials']
        officials = officials.rename(columns={0: 'Pos', 1: 'Name'})
        
        # Pos,Name,Date,Link,season
        save(officials, f'NFL_Officials-{season}.csv')
        
        #######################################################################
        pbp_df = dataframes['pbp']
        columns_to_rename = pbp_df.columns[5:7] 
        pbp_df = pbp_df.rename(columns={columns_to_rename[0]: 'Away_Points', columns_to_rename[1]: 'Home_Points'})
        
        # Quarter,Time,Down,ToGo,Location,Away_Points,Home_Points,Detail,EPB,EPA,Date,Link,season
        save(pbp_df, f'NFL_Pbp-{season}.csv')
        
        #######################################################################
        # update record that matches Link and Date record 
        temp_df_game = pd.read_csv(used_matchups)
        temp_df_game['Date'] = pd.to_datetime(temp_df_game['Date']).dt.date
        mask = (temp_df_game['Date'] == date_value) & (temp_df_game['Link'] == link_url)
        temp_df_game.loc[mask, 'Extracted'] = 1.0
        
        temp_df_game.to_csv(used_matchups, index=False)
        
###############################################################################
################################################################################


### will get - 3 days from current date... so it will get full seasons if previous years called
### will check matchups_path to see if getting data from current season
### have to call true for first run of a new season if the whole season is not over
main(2024, False)





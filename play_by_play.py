import pandas as pd
import numpy as np
import os
import time
from BBRscrape_players import scrape_players
from BBRscrape_boxscores import scrape_boxscore



player_cols = ['Shooter', 'Assister', 'Blocker', 'Fouler', 'Fouled', 'Rebounder', 
               'ViolationPlayer', 'FreeThrowShooter', 'EnterGame', 'LeaveGame', 
               'TurnoverPlayer', 'TurnoverCauser','JumpballAwayPlayer','JumpballHomePlayer','JumpballPoss']
players_dict={}
lineup_miscounts = {}

def load_seasons(seasons, lineups=False, possessions=False, drop_lineup_miscount_games=True):
    df_list = []
    for season in seasons:
        df_list.append(load_season(season, lineups=lineups, possessions=possessions, drop_lineup_miscount_games=drop_lineup_miscount_games))
    return pd.concat(df_list, axis=0), players_dict


def load_season(season, lineups=False, possessions=False, drop_lineup_miscount_games=True):
    #lineup_miscounts = {}
    
    if isinstance(season, int):
        season = str(season)
    if len(season) == 2:
        season = '20'+season
    if len(season) == 4:
        season = f"{season[:2]}{int(season[2:4])-1}-{season[2:4]}"
        
    df_ = pd.read_csv(f'PBP_data/NBA_PBP_{season}.csv')
    
    df_['Date'] = pd.to_datetime(df_['Date']+ ' '+ df_['Time'])
    df_ = df_.rename(columns={'Time':'Season'})
    df_['Season'] = season
    
    # keep only bbr_id in play-identification columns
    df_.loc[:,player_cols] = df_[player_cols].apply(lambda x: x.str.split('-').str[-1].str.strip())
    # Replace coach (id ends with 'c') technical fouls with Team
    df_.loc[df_['Fouler'].str[-1] == 'c', 'Fouler'] = 'Team'
    
    df_.loc[df_['Fouled'] == 'NULL', 'Fouled'] = np.nan
    
    # For consistency of Play team was fouled
    df_.loc[df_['FoulType'].isin(['loose ball','offensive', 'technical'])
            , ['AwayPlay', 'HomePlay']
           ] = df_.loc[df_['FoulType'].isin(['loose ball','offensive', 'technical'])
                       , ['HomePlay', 'AwayPlay']
                      ].values  
        
    df_ = df_.groupby('URL', group_keys=False).apply(add_timestamps)
    
  
    #specific Corrections
    if season == '2015-16':
        # Misrecorded substitution, manually cross-checked with ESPN.com play-by-play
        df_.loc[53066, ['AwayPlay', 'EnterGame','LeaveGame']] = ['L. Thomas enters the game for J. Caldern',
                                                                 'thomala01',
                                                                 'caldejo01']
        
    if lineups:
        df_ = df_.groupby('URL', group_keys=False).apply(add_lineup_cols)
    if possessions:
        df_ = df_.groupby('URL', group_keys=False).apply(add_possessions)  
    if drop_lineup_miscount_games:
        df_ = df_[~ df_['URL'].isin(lineup_miscounts.keys())]
    print("Lineup Miscount Games: ", lineup_miscounts)
    return df_


def load_specific_games(season, game_url_list=[], lineups=True, possessions=True):
    df_season = load_season(season, lineups=False, possessions=False)
    if len(game_url_list) == 0:
        game_url_list = [df_season.at[0, 'URL']]
    df_ = df_season[df_season['URL'].isin(game_url_list)].copy()
    
    if lineups:
        df_ = df_.groupby('URL', group_keys=False).apply(add_lineup_cols)
    if possessions:
        df_ = df_.groupby('URL', group_keys=False).apply(add_possessions)
        
    return df_


def add_timestamps(df_):
    df_ = df_.iloc[:,:].sort_values(['Quarter', 'SecLeft'], ascending=[True, False])
    df_.insert(8, 'Timestamp', (~df_.duplicated(subset=['Quarter', 'SecLeft'])).cumsum())
    #df_.loc[(df_['Timestamp'] == 1) & (df_['SecLeft'] < 720), 'SecLeft'] = 720
    df_.insert(9, 'SecElapsed', -1 * df_['SecLeft'].diff(1))
    df_.loc[df_['SecLeft'] == 720, 'SecElapsed'] = 0
    df_.loc[df_['SecElapsed'] < 0, 'SecElapsed'] = df_.loc[df_['SecElapsed'] < 0, 'SecElapsed'] \
                                                   + np.where(df_.loc[df_['SecElapsed'] < 0, 'Quarter'] < 5, 
                                                              720, 
                                                              300)
    df_['SecElapsed'] = df_['SecElapsed'].astype(int, errors='ignore')
    return df_


def participants_by_qtr(df_pbp):
    player_cols = ['Shooter','Assister','Blocker','Fouler','Fouled','Rebounder', 
                    'TurnoverPlayer', 'TurnoverCauser', 'ViolationPlayer','FreeThrowShooter',
                    'EnterGame', 'LeaveGame','JumpballAwayPlayer','JumpballHomePlayer','JumpballPoss']
    participants_by_qtr = {}
    for qtr in range(1,df_pbp['Quarter'].max()+1):
        qtr_plays = df_pbp['Quarter'] == qtr
        not_technical_fouls = df_pbp['FoulType'] != 'technical'
        stat_qtr_players = set()

        for col in player_cols:
            stat_qtr_players.update(list(df_pbp.loc[qtr_plays & not_technical_fouls
                                                    ,col
                                                   ].value_counts().index
                                        )
                                   )
        stat_qtr_players.discard('Team')
        #print(qtr, stat_qtr_players)
        participants_by_qtr[qtr] = stat_qtr_players
        
    return participants_by_qtr


def get_game_participants(df_pbp):
    players_by_qtr = participants_by_qtr(df_pbp)
    game_players = set([player for quarter_participants in players_by_qtr.values() 
                                for player in quarter_participants
                                    if player != 'Team'])
    return list(game_players)


def get_home_players_and_load_players(df_game):
    participants = get_game_participants(df_game)

    for bbr_id in participants:
        if bbr_id in players_dict.keys():
            continue
        print('Loading',bbr_id, end='... ')
        players_dict[bbr_id] = {}
        player_dfs_to_load = ['adj_shooting','advanced','all_salaries', 'info',\
                              'pbp','per_minute','per_poss','shooting','totals']
        if (not os.path.isdir(f'Players/{bbr_id}')) or (len(os.listdir(f'Players/{bbr_id}')) == 0):

            os.makedirs(f'Players/{bbr_id}')
            scraped_player = scrape_players(bbr_id)
            time.sleep(np.pi)
            #print(type(scraped_player), scraped_player.keys())

            for table_name, df in scraped_player[bbr_id].items():
                #print(table_name, end=', ')
                df.to_csv(f'Players/{bbr_id}/{table_name}.csv')
                if table_name in player_dfs_to_load:
                    players_dict[bbr_id][table_name] = df

        else:
            print(' already exists locally')

            
            multiindex_col_dfs = ['playoffs_shooting','all_college_stats','pbp',
                                  'highs-playoffs','highs-reg-season','playoffs_pbp',
                                  'shooting','adj_shooting']
            for df_path in os.listdir(f'Players/{bbr_id}'):
                if df_path.replace('.csv','') in player_dfs_to_load:
                    if df_path.replace('.csv','') in multiindex_col_dfs:
                        header = [0,1]
                    else:
                        header = [0]
                    players_dict[bbr_id][df_path.replace('.csv','')] = pd.read_csv(f'Players/{bbr_id}/{df_path}', header = header, index_col=0)

    def is_player_on_hometeam(bbr_id):
        try:
            if 'Tm' in players_dict[bbr_id].get('per_poss').columns:
                players_dict[bbr_id]['per_poss'].rename({'Tm':'Team'}, axis='columns', inplace=True)
        except:
            print(bbr_id, players_dict[bbr_id].keys())
        player_teams_in_game_season = players_dict[bbr_id]['per_poss'].loc[(players_dict[bbr_id]['per_poss']['Season'] \
                                                                             == df_game['Season'].value_counts().index[0]) \
                                                                            ,'Team' \
                                                                           ].values
        #print(bbr_id,player_teams_in_game_season)
        away_player, home_player = False, False
        for team in player_teams_in_game_season:
            if team == df_game['AwayTeam'].value_counts().index[0]:
                away_player = True
            if team == df_game['HomeTeam'].value_counts().index[0]:
                home_player = True
        if (away_player and home_player) or (not (away_player or home_player)):
            if df_game.loc[(df_game['EnterGame'] == bbr_id) | (df_game['LeaveGame'] == bbr_id), 'HomePlay'].notnull().sum() > 0:
                home_player = True
            if df_game.loc[(df_game['EnterGame'] == bbr_id) | (df_game['LeaveGame'] == bbr_id), 'AwayPlay'].notnull().sum() > 0:
                home_player = False

        return home_player


    return {k:is_player_on_hometeam(k) for k in participants}


def add_empty_lineup_cols(df_pbp):
    df_pbp = df_pbp.iloc[:,:]
    print(df_pbp['URL'].value_counts().index[0], end=', ')

    hometeam_bool_dict = get_home_players_and_load_players(df_pbp)

    player_height_mass_team = {p:(players_dict[p]['info']['Height_cm'].values[0],
                                  players_dict[p]['info']['Mass_kg'].values[0], 
                                  hometeam_bool_dict[p]) 
                               for p in hometeam_bool_dict.keys()}
    
    # Sort players by away/home and short/tall before adding player columns
    player_height_mass_team = dict(sorted(player_height_mass_team.items(), key=lambda x: (x[1][2], x[1][0], x[1][1])))
    
    # Add player column with tag for away/home
    for p in player_height_mass_team.keys():
        if not hometeam_bool_dict[p]:
            df_pbp.loc[:,f"{p}_a"] = False
        else:
            df_pbp.loc[:,f"{p}_h"] = False

    return df_pbp


def add_lineup_cols(df_pbp, drop_player_cols = True):
    players_per_quarter = participants_by_qtr(df_pbp)
    df_empty_lineup = add_empty_lineup_cols(df_pbp).copy()
    substitution_rows = df_empty_lineup['EnterGame'].notnull()
    lineup_cols = {col.split('_')[0]:col for col in df_empty_lineup.columns if '_' in col}

    home_lineup_cols = [col for col in df_empty_lineup.columns if '_h' in col]
    away_lineup_cols = [col for col in df_empty_lineup.columns if '_a' in col]

    start_quarter_idxs = {n+1:idx for n,idx in enumerate(df_empty_lineup.groupby('Quarter').head(1).index.values)}
    end_quarter_idxs = {n+1:idx for n, idx in enumerate(df_empty_lineup.groupby('Quarter').tail(1).index.values)}
    

    
    for qtr, players in players_per_quarter.items():
        df_qtr_subs = df_empty_lineup.loc[(df_empty_lineup['Quarter'] == qtr) & substitution_rows, :]

        for p in players:
            
            p_enters = df_qtr_subs.loc[df_qtr_subs['EnterGame'].str.contains(p)].index.values
            p_leaves = df_qtr_subs[df_qtr_subs['LeaveGame'].str.contains(p)].index.values
            
            # if player had one stint lasting entire quarter
            if (len(p_enters) == 0) and (len(p_leaves) == 0):
                df_empty_lineup.loc[df_empty_lineup['Quarter'] == qtr, lineup_cols[p]] = True
                
            
            while (len(p_enters) > 0) or (len(p_leaves) > 0):
            
                # if player had one stint which started quarter
                if len(p_enters) == 0:
                    df_empty_lineup.loc[start_quarter_idxs[qtr]:min(p_leaves), lineup_cols[p]] = True
                    p_leaves = np.delete(p_leaves, np.argmin(p_leaves))

                # if player had one stint which ended quarter
                elif (len(p_leaves) == 0):
                    df_empty_lineup.loc[min(p_enters)+1:end_quarter_idxs[qtr], lineup_cols[p]] = True
                    p_enters = np.delete(p_enters, np.argmin(p_enters))


                elif min(p_enters) < min(p_leaves):
                    df_empty_lineup.loc[min(p_enters)+1:min(p_leaves), lineup_cols[p]] = True
                    p_enters = np.delete(p_enters, np.argmin(p_enters))
                    p_leaves = np.delete(p_leaves, np.argmin(p_leaves))
                    
                else:
                    df_empty_lineup.loc[start_quarter_idxs[qtr]:min(p_leaves), lineup_cols[p]] = True
                    p_leaves = np.delete(p_leaves, np.argmin(p_leaves))
                    #print(p, ' first left the quarter at idx ', min(p_leaves))


    home_lineup_overcounts = df_empty_lineup[home_lineup_cols].sum(axis=1) >5
    home_lineup_undercounts = df_empty_lineup[home_lineup_cols].sum(axis=1) < 5
    away_lineup_overcounts = df_empty_lineup[away_lineup_cols].sum(axis=1) > 5
    away_lineup_undercounts = df_empty_lineup[away_lineup_cols].sum(axis=1) < 5
    home_lineup_miscounts = home_lineup_overcounts | home_lineup_undercounts
    away_lineup_miscounts = away_lineup_overcounts | away_lineup_undercounts

    if sum(home_lineup_miscounts | away_lineup_miscounts) > 0:
        
        
        if sum(home_lineup_miscounts) > 0:
            home_miscounts_by_qtr = df_empty_lineup[home_lineup_miscounts].value_counts('Quarter')
            home_overcounts_by_qtr = df_empty_lineup[home_lineup_overcounts].value_counts('Quarter')
            home_undercounts_by_qtr = df_empty_lineup[home_lineup_undercounts].value_counts('Quarter')
            
            
            for qtr in home_miscounts_by_qtr.index:
                if home_miscounts_by_qtr[qtr] == df_empty_lineup[df_empty_lineup['Quarter'] == qtr].shape[0]:
                    #entire qtr has miscount
                    print(f'entire quarter {qtr} is missing home_player')
                    if home_undercounts_by_qtr.get(qtr):
                        bool_undercount = True
                    if home_overcounts_by_qtr.get(qtr):
                        bool_undercount = False
                    missing_player = find_who_is_missing_from_game_qtr(df_pbp=df_empty_lineup, 
                                                                       qtr=qtr,
                                                                       team="home",
                                                                       undercount=bool_undercount)
                    if isinstance(missing_player, str):
                        if bool_undercount:
                            df_empty_lineup.loc[df_empty_lineup['Quarter'] == qtr, lineup_cols[missing_player]] = True
                        else:
                            df_empty_lineup.loc[df_empty_lineup['Quarter'] == qtr, lineup_cols[missing_player]] = False
                        
                else:
                    print(f'some subset of quarter {qtr} is missing away_player')

        if sum(away_lineup_miscounts) > 0:
            away_miscounts_by_qtr = df_empty_lineup[away_lineup_miscounts].value_counts('Quarter')
            away_overcounts_by_qtr = df_empty_lineup[away_lineup_overcounts].value_counts('Quarter')
            away_undercounts_by_qtr = df_empty_lineup[away_lineup_undercounts].value_counts('Quarter')

            
            for qtr in away_miscounts_by_qtr.index:
                if away_miscounts_by_qtr[qtr] == df_empty_lineup[df_empty_lineup['Quarter'] == qtr].shape[0]:
                    #entire qtr has miscount
                    print(f'entire quarter {qtr} is missing away_player')
                    if away_undercounts_by_qtr.get(qtr):
                        bool_undercount = True
                    if away_overcounts_by_qtr.get(qtr):
                        bool_undercount = False
                    missing_player = find_who_is_missing_from_game_qtr(df_pbp=df_empty_lineup, 
                                                                       qtr=qtr,
                                                                       team="away",
                                                                      undercount=bool_undercount)
                    if isinstance(missing_player, str):
                        if bool_undercount:
                            df_empty_lineup.loc[df_empty_lineup['Quarter'] == qtr, lineup_cols[missing_player]] = True
                        else:
                            df_empty_lineup.loc[df_empty_lineup['Quarter'] == qtr, lineup_cols[missing_player]] = False
                else:
                    print(f'some subset of quarter {qtr} is missing away_player')
        
        home_lineup_miscounts = df_empty_lineup[home_lineup_cols].sum(axis=1) != 5
        away_lineup_miscounts = df_empty_lineup[away_lineup_cols].sum(axis=1) != 5
        if sum(away_lineup_miscounts) + sum(home_lineup_miscounts) == 0:
            print('Lineup miscount rectified')
        else:
            lineup_miscounts[df_empty_lineup["URL"].value_counts().index[0]] = sum(home_lineup_miscounts) + sum(away_lineup_miscounts)
            print('Lineup miscount persists')

    df_empty_lineup.loc[:,'AwayLineup'] = df_empty_lineup.apply(lambda row: [col[:-2] for col in away_lineup_cols if row[col]] \
                                                                 , axis=1).to_list()
    df_empty_lineup.loc[:,'HomeLineup'] = df_empty_lineup.apply(lambda row: [col[:-2] for col in home_lineup_cols if row[col]] \
                                                                 , axis=1).to_list()
                    
                    
    df_empty_lineup['HomeLineup'] = df_empty_lineup['HomeLineup'].apply(lambda x: ','.join(x))
    df_empty_lineup['AwayLineup'] = df_empty_lineup['AwayLineup'].apply(lambda x: ','.join(x))
    
    if drop_player_cols:
        df_empty_lineup = df_empty_lineup.drop(columns=home_lineup_cols+away_lineup_cols)
    return df_empty_lineup


def find_who_is_missing_from_game_qtr(df_pbp, qtr, team, undercount):
    if team.lower() == 'home':
        lineup_cols = [col for col in df_pbp.columns if col[-2:] == '_h']
        team = df_pbp.value_counts('HomeTeam').index[0]
    else:
        lineup_cols = [col for col in df_pbp.columns if col[-2:] == '_a']
        team = df_pbp.value_counts('AwayTeam').index[0]
    if qtr > 4:
        qtr_minutes = 5
    else: qtr_minutes = 12
    
    team_pbp_minutes = {}
    for p_col in lineup_cols:
        
        team_pbp_minutes[p_col[:-2]] =  df_pbp.loc[df_pbp[p_col], 'SecElapsed'].sum() / 60
    

    game_boxscore = scrape_boxscore(df_pbp.value_counts('URL').index[0])
    box_pbp_minute_discrepancy = game_boxscore[team
                                              ].iloc[:-1,:2
                                                    ].apply(lambda x: (x['Players'], x['MP'] - team_pbp_minutes[x['Players']])
                                                            , axis=1
                                                           ).values
    box_pbp_minute_discrepancy = sorted(box_pbp_minute_discrepancy, key=lambda x: x[1], reverse=True)
    
    if undercount:
        missing_players = [p for p, minutes in box_pbp_minute_discrepancy if abs(minutes - qtr_minutes) < 0.25]
    else:
        missing_players = [p for p, minutes in box_pbp_minute_discrepancy if abs(minutes + qtr_minutes) < 0.25]
        
    if len(missing_players) == 1:
        return missing_players[0]
    
    else:
        return box_pbp_minute_discrepancy
    
    

def bool_hometeam_in_possession_at_idx(df_pbp, idx):
    game_url = df_pbp.value_counts('URL').index[0]
    HomePossEnd_idxs = df_pbp[(df_pbp['URL'] == game_url) & df_pbp['HomePossEnd']].index
    AwayPossEnd_idxs = df_pbp[(df_pbp['URL'] == game_url) & df_pbp['AwayPossEnd']].index
    
    if len(AwayPossEnd_idxs[AwayPossEnd_idxs < idx]) > 0:
        last_AwayPossEnd_idx = AwayPossEnd_idxs[AwayPossEnd_idxs < idx][-1]
    else:
        last_AwayPossEnd_idx = -1
    if len(HomePossEnd_idxs[HomePossEnd_idxs < idx]) > 0:
        last_HomePossEnd_idx = HomePossEnd_idxs[HomePossEnd_idxs < idx][-1]
    else:
        last_HomePossEnd_idx = -1
        
    # if neither team has used a possession yet in game    
    if (last_AwayPossEnd_idx == -1) and (last_HomePossEnd_idx == -1):
        # did hometeam win tip?
        opening_jump_possessor = df_pbp.loc[df_pbp['Timestamp'] ==1, 'JumpballPoss'].values[0]
        participant_homebool_dict = get_home_players_and_load_players(df_pbp)
        if participant_homebool_dict.get(opening_jump_possessor):
            return True
        else:
            return False
    if last_AwayPossEnd_idx > last_HomePossEnd_idx:
        return True
    else:
        return False

def add_possessions(df_pbp):
    df_ = df_pbp.copy()

    df_.insert(12,'AwayPossEnd', False)
    df_.insert(16,'HomePossEnd', False)

    away_plays = df_['AwayPlay'].notnull()
    home_plays = df_['HomePlay'].notnull()

    three_point_shots = df_['ShotType'].apply(lambda x: '3' in str(x))
    two_point_shots = df_['ShotType'].apply(lambda x: '2' in str(x))
    free_throws = df_['FreeThrowNum'].notnull() 

    makes = (df_['ShotOutcome'] == 'make') |  (df_['FreeThrowOutcome'] == 'make')
    misses = (df_['ShotOutcome'] == 'miss') |  (df_['FreeThrowOutcome'] == 'miss')    
    
    defensive_rebounds = df_['ReboundType'] == 'defensive'
    turnovers = df_['TurnoverType'].notnull()

    final_free_throws = df_['FreeThrowNum'].isin([f"{n} of {n}" for n in range(1,4)])
    end_quarters = df_['AwayPlay'].apply(lambda x: ('End of' in str(x)) and ('Game' not in str(x)))

    get_cotemporal_plays = lambda condition: df_['Timestamp'].isin(df_.loc[condition, 'Timestamp'].values)

    and_one_cotemporal = get_cotemporal_plays(df_['FreeThrowNum'] == '1 of 1')
    end_quarter_cotemporal = get_cotemporal_plays(end_quarters)


    start_qtr = (df_pbp['SecLeft'] == 720) & (df_pbp['Quarter'] <= 4)
    start_ot = (df_pbp['SecLeft'] == 300) & (df_pbp['Quarter'] > 4)
    jumpballs = df_pbp['JumpballAwayPlayer'].notnull()
    
    # Every defensive rebound concludes a possession for the opposite team
    df_.loc[defensive_rebounds & away_plays, 'HomePossEnd'] = True
    df_.loc[defensive_rebounds & home_plays, 'AwayPossEnd'] = True

    # Every turnover concludes a possession for the same team
    df_.loc[turnovers & away_plays, 'AwayPossEnd'] = True
    df_.loc[turnovers & home_plays, 'HomePossEnd'] = True

    # Every made free throw n_of_n concludes a possession for the same team
    df_.loc[final_free_throws & makes & away_plays, 'AwayPossEnd'] = True
    df_.loc[final_free_throws & makes & home_plays, 'HomePossEnd'] = True

    # Every made shot (without an and_one) concludes a possession for the same team
    df_.loc[makes & (three_point_shots | two_point_shots) & ~and_one_cotemporal & away_plays, 'AwayPossEnd'] = True
    df_.loc[makes & (three_point_shots | two_point_shots) & ~and_one_cotemporal & home_plays, 'HomePossEnd'] = True
    



    for idx in df_.loc[end_quarters].index.values:
        
        # EndQuarter (if no cotemporal possession conclusion) concludes possession for less recent possessor
        if df_.loc[df_['Timestamp'] == df_.at[idx, 'Timestamp'], ['AwayPossEnd','HomePossEnd']].sum().sum() == 0:
            if bool_hometeam_in_possession_at_idx(df_, idx):
                df_.loc[idx, 'HomePossEnd'] = True
            else:
                df_.loc[idx, 'AwayPossEnd'] = True
    
    
    
    # JumpBall (mid-quarter only) concludes possession for less recent possessor
    mid_qtr_jumpball_idxs = df_.loc[jumpballs & ~start_qtr & ~start_ot].index
    for idx in mid_qtr_jumpball_idxs:
        if bool_hometeam_in_possession_at_idx(df_, idx):
            df_.loc[idx, 'HomePossEnd'] = True
        else:
            df_.loc[idx, 'AwayPossEnd'] = True
    

    
    df_.insert(13,'AwayPoss', df_['AwayPossEnd'].cumsum())
    df_.insert(14,'AwayPts', df_['AwayScore'].diff().fillna(0).astype(int))
    df_.insert(19,'HomePoss', df_['HomePossEnd'].cumsum())
    df_.insert(20,'HomePts', df_['HomeScore'].diff().fillna(0).astype(int))
    df_.insert(22,'Margin', df_['HomeScore'] - df_['AwayScore'])
    df_.insert(23, 'FinalMargin', df_['Margin'].values[-1])
    df_.insert(23, 'ClosestRemainingMargin', df_.apply(lambda row: df_.loc[row.name:,'Margin'].abs().min(), axis=1))

    return df_


def get_player_attr_value(bbr_id, df_pbp, table_name='info', col_name='age', seasons_ago=1):
    '''
    
    '''
    #print(f"getting {bbr_id}")
    season_year = df_pbp['Season'].value_counts().index[0]
    if col_name.lower() == 'age':
        
        season_year = season_year[:2]+ season_year[-2:]
        
        player_age = (pd.to_datetime(f"1/1/{season_year}") \
                      - pd.to_datetime(players_dict[bbr_id]['info'].at[0,'Birthdate']) \
                     ) / np.timedelta64(1, 'Y')
        return np.round(player_age, 2)
    
        
    if col_name.lower() == 'salary':
        try:
            if 'all_salaries' not in players_dict.get(bbr_id).keys():
                return 20e3
            else:
                if season_year not in players_dict[bbr_id]['all_salaries'].value_counts('Season').index.values:
                    print(f'no salary data for year for {bbr_id}')
                    return 20e3
                else:
                    return players_dict.get(bbr_id
                                           ).get('all_salaries'
                                                ).loc[players_dict.get(bbr_id).get('all_salaries')['Season'] == season_year
                                                      , 'Salary'
                                                      ].str.replace('$','',regex=False
                                                                   ).str.replace(',','',regex=False
                                                                                ).str.replace('(TW)','',regex=False
                                                                                             ).str.replace('< Minimum','20000',regex=False
                                                                                                          ).str.strip().astype(int
                                                                                                                              ).values[0]
                
            
        except ValueError as ve: 
            print(ve, bbr_id)
            return 20e3
        
        except IndexError as ie:
            print(ie, bbr_id)
            return 20e3
    
    if table_name == 'info':
        return players_dict[bbr_id][table_name].at[0,col_name]
    
   

    season_to_grab = f"{season_year[:2]}{int(season_year[2:4])-abs(seasons_ago)}-{int(season_year[-2:])-abs(seasons_ago)}" 
    

    df_table_season = players_dict[bbr_id][table_name][players_dict[bbr_id][table_name]['Season'] == season_to_grab].copy()
    
    if df_table_season.shape[0] > 1:
        if 'Tm' in df_table_season.columns:
            return df_table_season.loc[df_table_season['Tm'] == 'TOT',col_name].values[0]
        elif 'Team' in df_table_season.columns:
            return df_table_season.loc[df_table_season['Team'] == 'TOT',col_name].values[0]
        else:
            print(f"SOMETHING FUNKY HAPPENED GETTING {bbr_id} {table_name} {col_name}")
            return f"SOMETHING FUNKY HAPPENED GETTING {bbr_id} {table_name} {col_name}"
    
    elif df_table_season.shape[0] == 1:
        return df_table_season[col_name].values[0]
    
    else:
        return np.nan
    
def get_lineup_feature(df_g, table_name='info', col_name='age', seasons_ago=1, agg='list', delta_AwayHome=False):
    '''
    agg can be ('list', 'mean', 'median', 'min', 'max', 'range', 'std')
    
    '''
    df_game = df_g.copy()

    get_player_val = lambda x: get_player_attr_value(bbr_id=x, 
                                                     df_pbp=df_game, 
                                                     table_name=table_name, 
                                                     col_name=col_name, 
                                                     seasons_ago=seasons_ago)
    
    
    
    # For each row, get the 5 players in the row
    if 'AwayLineup' in df_game.columns:
        
        player_attr_dict = {p:get_player_val(p) 
                            for p in set(df_game['AwayLineup'].str.split(',').sum() 
                                         + df_game['HomeLineup'].str.split(',').sum()
                                        )
                           }
        
        away_lineup_vals = df_game.apply(lambda row: [player_attr_dict[player]
                                                      for player in row['AwayLineup'].split(',')]
                                         , axis=1).to_list()
        home_lineup_vals = df_game.apply(lambda row: [player_attr_dict[player] 
                                                      for player in row['HomeLineup'].split(',')]
                                         , axis=1).to_list()

        if '_' in col_name:
            col_name = col_name.split('_')[-1]
        if agg == 'list':
            df_game.loc[:,[f"Away{pos}_{col_name}" for pos in range(1,6)]] = away_lineup_vals
            df_game.loc[:,[f"Home{pos}_{col_name}" for pos in range(1,6)]] = home_lineup_vals
            if delta_AwayHome:
                for pos in range(1,6):
                    df_game.loc[:,f"delta{pos}_{col_name}"] = df_game[f"Home{pos}_{col_name}"] - df_game[f"Away{pos}_{col_name}"]
                    df_game.drop(columns=[f"Home{pos}_{col_name}", f"Away{pos}_{col_name}"], inplace=True)
            return df_game

        if agg == 'mean':

            df_game.loc[:,f"AwayMean_{col_name}"] = [np.mean(lineup) for lineup in away_lineup_vals]
            df_game.loc[:,f"HomeMean_{col_name}"] = [np.mean(lineup) for lineup in home_lineup_vals]
            if delta_AwayHome:
                df_game.loc[:,f"deltaMean_{col_name}"] = df_game[f"HomeMean_{col_name}"] - df_game[f"AwayMean_{col_name}"]
                df_game.drop(columns=[f"HomeMean_{col_name}", f"AwayMean_{col_name}"], inplace=True)
            return df_game


        if agg == 'median':

            df_game.loc[:,f"AwayMed_{col_name}"] = [np.median(lineup) for lineup in away_lineup_vals]
            df_game.loc[:,f"HomeMed_{col_name}"] = [np.median(lineup) for lineup in home_lineup_vals]
            if delta_AwayHome:
                df_game.loc[:,f"deltaMed_{col_name}"] = df_game[f"HomeMed_{col_name}"] - df_game[f"AwayMed_{col_name}"]
                df_game.drop(columns=[f"HomeMed_{col_name}", f"AwayMed_{col_name}"], inplace=True)
            return df_game

        if agg == 'min':

            df_game.loc[:,f"AwayMin_{col_name}"] = [np.min(lineup) for lineup in away_lineup_vals]
            df_game.loc[:,f"HomeMin_{col_name}"] = [np.min(lineup) for lineup in home_lineup_vals]
            if delta_AwayHome:
                df_game.loc[:,f"deltaMin_{col_name}"] = df_game[f"HomeMin_{col_name}"] - df_game[f"AwayMin_{col_name}"]
                df_game.drop(columns=[f"HomeMin_{col_name}", f"AwayMin_{col_name}"], inplace=True)
            return df_game

        if agg == 'max':

            df_game.loc[:,f"AwayMax_{col_name}"] = [np.max(lineup) for lineup in away_lineup_vals]
            df_game.loc[:,f"HomeMax_{col_name}"] = [np.max(lineup) for lineup in home_lineup_vals]
            if delta_AwayHome:
                df_game.loc[:,f"deltaMax_{col_name}"] = df_game[f"HomeMax_{col_name}"] - df_game[f"AwayMax_{col_name}"]
                df_game.drop(columns=[f"HomeMax_{col_name}", f"AwayMax_{col_name}"], inplace=True)
            return df_game

        if agg == 'range':

            df_game.loc[:,f"AwayRange_{col_name}"] = [np.max(lineup) - np.min(lineup) for lineup in away_lineup_vals]
            df_game.loc[:,f"HomeRange_{col_name}"] = [np.max(lineup) - np.min(lineup) for lineup in home_lineup_vals]
            if delta_AwayHome:
                df_game.loc[:,f"deltaRange_{col_name}"] = df_game[f"HomeRange_{col_name}"] - df_game[f"AwayRange_{col_name}"]
                df_game.drop(columns=[f"HomeRange_{col_name}", f"AwayRange_{col_name}"], inplace=True)
            return df_game

        if agg == 'std':

            df_game.loc[:,f"AwayStd_{col_name}"] = [np.std(lineup) for lineup in away_lineup_vals]
            df_game.loc[:,f"HomeStd_{col_name}"] = [np.std(lineup) for lineup in home_lineup_vals]
            if delta_AwayHome:
                df_game.loc[:,f"deltaStd_{col_name}"] = df_game[f"HomeStd_{col_name}"] - df_game[f"AwayStd_{col_name}"]
                df_game.drop(columns=[f"HomeStd_{col_name}", f"AwayStd_{col_name}"], inplace=True)
            return df_game
    else:
        
        player_attr_dict = {p:get_player_val(p) 
                            for p in set(df_game['Lineup'].str.split(',').sum()
                                        )
                           }
        
        lineup_vals = df_game.apply(lambda row: [player_attr_dict[player]
                                                 for player in row['Lineup'].split(',')]
                                               , axis=1
                                   )
        if '_' in col_name:
            col_name = col_name.split('_')[-1]
        if agg == 'list':
            df_game.loc[:,[f"Lineup{pos}_{col_name}" for pos in range(1,6)]] = lineup_vals.to_list()
            
            return df_game

        if agg == 'mean':

            df_game.loc[:,f"LineupMean_{col_name}"] = lineup_vals.apply(lambda x: np.mean(x))
            return df_game


        if agg == 'median':

            df_game.loc[:,f"LineupMed_{col_name}"] = lineup_vals.apply(lambda x: np.median(x))
            return df_game

        if agg == 'min':

            df_game.loc[:,f"LineupMin_{col_name}"] = lineup_vals.apply(lambda x: np.min(x))
            return df_game

        if agg == 'max':

            df_game.loc[:,f"LineupMax_{col_name}"] = lineup_vals.apply(lambda x: np.max(x))
            return df_game

        if agg == 'range':

            df_game.loc[:,f"LineupRange_{col_name}"] = lineup_vals.apply(lambda x: np.max(x) - np.min(x))
            return df_game

        if agg == 'std':

            df_game.loc[:,f"LineupStd_{col_name}"] = lineup_vals.apply(lambda x: np.std(x))
            return df_game

        
def get_lineup_results(df_pbp, return_lineup_matchups=True):
    
    df_game_lineup_results = df_pbp.groupby(['Season','AwayLineup','HomeLineup'], as_index=False)[['SecElapsed','AwayPossEnd','HomePossEnd', 'AwayPts', 'HomePts']].agg('sum')
    non_zero_lineups = df_game_lineup_results.sum(axis=1, numeric_only=True) > 0
    df_lineup_results = df_game_lineup_results[non_zero_lineups].copy()
    if return_lineup_matchups:
        
        df_lineup_results['HomePPP'] = df_lineup_results['HomePts'] / df_lineup_results['HomePossEnd']
        df_lineup_results['AwayPPP'] = df_lineup_results['AwayPts'] / df_lineup_results['AwayPossEnd']
        df_lineup_results['TotalPossessions'] = df_lineup_results['AwayPossEnd'] + df_lineup_results['HomePossEnd']
        sorted_idxs = df_lineup_results.sort_values(['TotalPossessions','SecElapsed'],ascending=False).index
        df_lineup_results = df_lineup_results.loc[sorted_idxs,:].reset_index(drop=True)

        df_lineup_results['SecElapsedCumDist'] = df_lineup_results['SecElapsed'].cumsum() / df_lineup_results['SecElapsed'].sum()
        df_lineup_results['TotPossCumDist'] = df_lineup_results['TotalPossessions'].cumsum() / df_lineup_results['TotalPossessions'].sum()

        return df_lineup_results

    df_home_lineups = df_lineup_results.groupby(['Season','HomeLineup'], as_index=False).agg('sum', numeric_only=True)
    df_home_lineups = df_home_lineups.rename(columns={'HomeLineup':'Lineup'})
    df_home_lineups['Home'] = True

    df_away_lineups = df_lineup_results.groupby(['Season','AwayLineup'], as_index=False).agg('sum', numeric_only=True)
    df_away_lineups = df_away_lineups.rename(columns={'AwayLineup':'Lineup'})
    df_away_lineups['Home'] = False
    df_lineups = pd.concat([df_away_lineups, df_home_lineups], axis=0).iloc[:,:]
    df_lineups = df_lineups.reset_index(drop=True)
    df_lineups[['OffPoss', 'DefPoss', 'PtsScored', 'PtsAllowed']] = 0
    df_lineups.loc[df_lineups['Home'], 
                   ['OffPoss', 'DefPoss', 'PtsScored', 'PtsAllowed']
                  ] = df_lineups.loc[df_lineups['Home'], 
                                     ['HomePossEnd', 'AwayPossEnd', 'HomePts', 'AwayPts']].values
    df_lineups.loc[~ df_lineups['Home'], 
                   ['OffPoss', 'DefPoss', 'PtsScored', 'PtsAllowed']
                  ] = df_lineups.loc[~ df_lineups['Home'], 
                                     ['AwayPossEnd', 'HomePossEnd', 'AwayPts', 'HomePts']].values
    
    df_lineups['OffPPP'] = df_lineups['PtsScored'] / df_lineups['OffPoss']
    df_lineups['DefPPP'] = df_lineups['PtsAllowed'] / df_lineups['DefPoss']
    df_lineups['TotalPossessions'] = df_lineups['AwayPossEnd'] + df_lineups['HomePossEnd']
    sorted_idxs = df_lineups.sort_values(['TotalPossessions','SecElapsed'],ascending=False).index
    df_lineups = df_lineups.loc[sorted_idxs,:].reset_index(drop=True)

    df_lineups['SecElapsedCumDist'] = df_lineups['SecElapsed'].cumsum() / df_lineups['SecElapsed'].sum()
    df_lineups['TotPossCumDist'] = df_lineups['TotalPossessions'].cumsum() / df_lineups['TotalPossessions'].sum()
    
    df_lineups = df_lineups.drop(columns = ['AwayPossEnd', 'HomePossEnd', 'AwayPts', 'HomePts'])
    
    return df_lineups
    
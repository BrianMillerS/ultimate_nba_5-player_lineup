import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re


def scrape_boxscore(game_url):
    if '.' in game_url:
        game_url = game_url.split('.')[0]
    if '/' in game_url:
        game_url = game_url.split('/')[-1]
    URL = f'https://www.basketball-reference.com/boxscores/{game_url}.html'
    page = requests.get(URL)  # requests gets the html page
    page_sans_comments = str(page.content, encoding='utf-8').replace('-->',' ').replace('<!--',' ')
    soup = BeautifulSoup(page_sans_comments, 'lxml')
    
    dict_df_boxscores = {'URL': game_url}
    for t in soup.find_all('table'):
        if t.get('id'):
            if 'game-basic' in t.get('id'):
                tbody = t.find('tbody')

                title = re.findall('[A-Z]{3}', t['id'])[0]
                dict_player_name_id = {'Team Totals':title}
                for tr in tbody.find_all('tr'):
                    th = tr.find('th')
                    if th.get('data-append-csv'):
                        #print(th['data-append-csv'], th.text)
                        dict_player_name_id[th.text] = th['data-append-csv']

                df_temp = pd.read_html(str(t))[0]
                df_temp.columns = df_temp.columns.droplevel(0)
                df_temp = df_temp[(df_temp['MP'] != 'MP') & (~ df_temp['MP'].str.lower().str.contains('not'))]
                df_temp.loc[~df_temp['MP'].str.contains(':'), 'MP'] = df_temp.loc[~df_temp['MP'].str.contains(':'), 'MP'] + ':0'
                df_temp['MP'] = df_temp['MP'].str.split(':').apply(lambda x: int(x[0])+ int(x[1])/60)
                df_temp = df_temp.rename(columns={'Starters':'Players'})
                df_temp['Players'] = df_temp['Players'].apply(lambda x: dict_player_name_id[x])
                dict_df_boxscores[title] = df_temp
    
    return dict_df_boxscores
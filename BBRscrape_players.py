import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time


def scrape_player(bbr_id=None):
    
    URL = f'https://www.basketball-reference.com/players/{bbr_id[0]}/{bbr_id}.html'
    page = requests.get(URL)  # requests gets the html page
    #page_sans_comments = re.sub(r'<!.*?->','', str(page.content))
    page_sans_comments = str(page.content, encoding='utf-8').replace('-->',' ').replace('<!--',' ')
    #print(page.content)
    #soup = BeautifulSoup(page.content, "html.parser")
    #re.sub(r'<!.*?->','', soup)
    soup = BeautifulSoup(page_sans_comments, 'lxml')
    
    info = soup.find('div', attrs={'id':'meta'}).find_all('p')
    #for n, i in enumerate(info):
    #    print(n, i, '\n')
    player_dict = {}
    info_dict = {'BBR_id':bbr_id}
    if 'Pronunciation' in info[0].text:
        info_dict['Pronunciation'] = info[0].text.split(':')[-1].strip()
    else:
        info.insert(0,'')
    name_socials = [item.replace('\n','').strip() for item in info[1].text.split('▪')]

    for item in name_socials:
        item = item.split(':')
        if len(item) == 1:
            info_dict['Name'] = item[0]
        elif len(item) == 2:
            info_dict[item[0].strip()] = item[1].strip()
        else:
            print("ERROR ERROR ",item)
    
    if 'Position' in info[2].text:
        info.insert(2,'')
        info_dict['Nicknames'] = []
    else:
        if '(born ' in info[2].text:
            info_dict['Birthname'] = info[2].text.split('born ')[-1].split(')')[0]
        else:
            info_dict['Birthname'] = info_dict['Name']
            info_dict['Nicknames'] = info[2].text.strip().split(',')
    
    if 'Position' in info[3].text:
        info.insert(3,'')
    else: 
        info_dict['Nicknames'] = info[3].text.strip().split(',')
        
    position_shoots = [item.replace('\n','').strip().split(':') 
                     for item in info[4].text.split('▪')]
    
    info_dict['Positions'] = [item.strip() for item in position_shoots[0][1].strip().split('and')]
    info_dict['Shoots'] = position_shoots[1][1].strip()

    height_mass = info[5].text.replace('\xa0',''
                                      ).replace(',', ''
                                               ).split('(')[-1].split('kg')[0].split('cm')
    height_mass = [int(num) for num in height_mass]

    info_dict['Height_cm'] = height_mass[0]
    info_dict['Mass_kg'] = height_mass[1]
    for segment in info[6:]:
        if "Born" in segment.text:
            
            birth_data = [span for span in segment.find_all('span') if 'class' not in span.attrs]
            for item in birth_data:
                if 'data-birth' in item.attrs:
                    info_dict['Birthdate'] = item['data-birth']
                else:
                    try:
                        info_dict['Birth_town'], info_dict['Birthplace'] = 'in'.join(item.text.strip().replace('\xa0', '').split('in')[1:]).split(',')
                    except ValueError:
                        info_dict['Birthplace'] = 'in'.join(item.text.strip().replace('\xa0', '').split('in')[1:]).split(',')

        if 'Draft' in segment.text:
            draft_data = [item.strip() for item in segment.text.strip().split(':')[-1].split(',')]

            info_dict['Draft_team'] = draft_data[0]
            info_dict['Draft_position'] = draft_data[1].split(' (') + [draft_data[2].replace(')','')]


        if 'Debut' in segment.text:
            
            info_dict['NBA_debut'] = segment.text.split(':')[-1].strip()
    
        if 'College' in segment.text:
            info_dict['College'] = segment.text.split(':')[-1].strip()
        
        if 'Recruiting' in segment.text:
            year, rank = segment.text.split(',')[-1].split(':')[-1].strip().replace(')','').split('(')
            info_dict['HS_RSCI'] = {year:rank}
        
        
    salary_table = soup.find('table', attrs={'id':'all_salaries'})
    if salary_table is not None:
        #print(salary_table)
        salary_cols = [th.text for th in salary_table.find('thead').find_all('th')]
        #print(salary_cols)
        salary_dict = {col:[] for col in salary_cols}
        for tr in salary_table.find('tbody').find_all('tr'):
            salary_dict[salary_cols[0]].append(tr.find('th').text)
            for n, td in enumerate(tr.find_all('td')):
                 salary_dict[salary_cols[n+1]].append(td.text.replace('$','').replace(',',''))
        df_salary = pd.DataFrame(salary_dict)
        #player_dict['Salary_history'] = df_salary.apply(pd.to_numeric, errors='ignore')

    contract_info = soup.find('div', attrs={'id':'div_contract'})

    try:
        contract_notes = [li.text for li in contract_info.find_all('li')]
        info_dict['Contract_notes'] = contract_notes
    except AttributeError:
        pass

    transactions = []
    transaction_data = soup.find('div', attrs={'id':'all_transactions'})
    if transaction_data is not None:
        for s in transaction_data.find_all('span'):
            transaction_detail = s.text
            for a in s.find_all('a'):
                if len(a.attrs) == 2:
                    try:
                        code_name = a['href'].replace('/teams/','').split('/')[0]
                    except KeyError:
                        code_name = ''
                    #print(code_name)
                    idx_name = transaction_detail.find(a.text) + len(a.text)
                    transaction_detail = transaction_detail[:idx_name] + f" ({code_name})" + transaction_detail[idx_name:]
                else:
                    try:
                        code_name = a['href'].split('.')[0].split('/')[-1]
                    except KeyError:
                        code_name = ''
                    #print(code_name)
                    idx_name = transaction_detail.find(a.text) + len(a.text)
                    transaction_detail = transaction_detail[:idx_name] + f" ({code_name})" + transaction_detail[idx_name:]

            transactions.append(transaction_detail.strip())

        info_dict['Transactions'] = transactions
        
    player_dict['info'] = pd.DataFrame({k:[info_dict[k]] for k, v in info_dict.items()})
    
    for t in soup.find_all('table'):
        if t.get('id') not in [None,'stathead_insights']:

            pd_df = pd.read_html(str(t))[0]
    
            if isinstance(pd_df.columns, pd.MultiIndex):
                unnamed_cols = [(idx1, idx2) for idx1, idx2 in pd_df.columns if ('Unnamed:' in idx1) and ('Unnamed:' in idx2)]
            else:
                unnamed_cols = [col for col in pd_df.columns if 'Unnamed:' in col]

            for unnamed_col in unnamed_cols:
                if pd_df.loc[:,unnamed_col].isnull().sum() == pd_df.shape[0]:
                    pd_df = pd_df.drop(unnamed_col, axis=1)
            
            player_dict[f"{t.get('id')}"] = pd_df[pd_df.shape[1] != pd_df.isnull().sum(axis=1)]
    
    return player_dict

def scrape_players(list_of_bbr_ids, sleep_timer=np.pi):
    dict_of_player_dicts = {}
    if isinstance(list_of_bbr_ids, str):
        list_of_bbr_ids = [list_of_bbr_ids]
    else:
        list_of_bbr_ids = list(set(list_of_bbr_ids))
    print(f"Scraping BBR for: {list_of_bbr_ids}")
    for n, bbr_id in enumerate(list_of_bbr_ids):
        dict_of_player_dicts[bbr_id] = scrape_player(bbr_id)
        print(f"{dict_of_player_dicts[bbr_id]['info']['Name'][0]:>40} is Sleeping...", end='')
        if n < len(list_of_bbr_ids) - 1:
            time.sleep(sleep_timer)
        print(" and now is Awake")
        

    return dict_of_player_dicts

if __name__ == '__main__':
    pass
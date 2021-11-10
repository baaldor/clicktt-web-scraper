from numpy import NaN
import requests
from bs4 import BeautifulSoup
import pandas
import logging
import requests
from requests.sessions import session
import configparser
import time

def getCredentials(file:str):
    # parsing values from file
    config = configparser.ConfigParser()
    config.read(file)
    username = config.get("credentials", "username")
    password = config.get("credentials", "password")
    return (username, password)

def loginToMyTischtennis(username, password):    
    URL = "https://www.mytischtennis.de"
    LOGIN_ROUTE = "/community/login/"
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36', 
        'origin': URL, 
        'referer': URL + LOGIN_ROUTE
        }

    s = requests.session()
       
    login_payload = {
        'userNameB': username,
        'userPassWordB': password, 
        'permalogin': '1',
        'targetPage': 'https://www.mytischtennis.de/public/home?fromlogin=1',
        'goLogin': 'Einloggen'
        }

    login_req = s.post(URL + LOGIN_ROUTE, headers = HEADERS, data = login_payload)
    
    logging.info("Login Status code " + str(login_req.status_code))
    #cookies = login_req.cookies

    return s

def readClickttIdsFromFile(clickttids_file_name):
    df = pandas.read_csv(clickttids_file_name, header = 0, sep = '\t')
    return df

def generateURLForClickttId(clickttid:str):
    url = "https://www.mytischtennis.de/community/matches?clickttid=" + clickttid + "&statisticType=all&timeInterval=all&matchType=all"
    return url

def extractMatchesFrom(url, aTTDEsession:requests.session):
    logging.info("Extracting match result tables from " + url)

    #df = pandas.DataFrame()

    page = aTTDEsession.get(url)
    #Wait some time, because myTischtennis.de needs some time to assemble and provide data. This is a poor man's approach.
    time.sleep(10)
    page = aTTDEsession.get(url)
    soup = BeautifulSoup(page.content, "html5lib")

    if soup:
        tables = soup.find_all("table")
    else:        
        logging.info("Found no soup at all!")    
    
    if len(tables)>0:
        table = tables[0]
    else:
        logging.info("Found no table at all!")
    
    if table:        
        [df,] = pandas.read_html(table.prettify(),flavor='html5lib',header=0)
        
        all_a = table.find_all("a", {"data-tooltipdata": True})
        if all_a:
            clickttids_Kontrahent = [str.split(a['data-tooltipdata'], sep=';', maxsplit=1)[0] for a in all_a]
            df['clickttid Kontrahent'] = clickttids_Kontrahent
        
        print(df)

        if "Keine Daten vorhanden!" in df.values:
            df = None
        
    return df

def prettifyDataframe(df:pandas.DataFrame):        
    df[['Datum', 'Matchtyp']] = df['Datum  Details'].str.split(' ', 1, expand=True) # Spalte Datum/Details auftrennen
    df['Datum'] = pandas.to_datetime(df['Datum'],format="%d.%m.%y") #konvertiere Datumsspalte von String zu datetime
    
    df['TTRDiff'] = df['TTRDiff'].where( df['GW'] > 0.5, df['TTRDiff']*(-1))

    df[['Satz 1 Spieler', 'Satz 1 Gegner']] = df['1'].str.split(':', 1, expand=True) # Spalte Satz 1 auftrennen
    df[['Satz 2 Spieler', 'Satz 2 Gegner']] = df['2'].str.split(':', 1, expand=True) # Spalte Satz 2 auftrennen
    df[['Satz 3 Spieler', 'Satz 3 Gegner']] = df['3'].str.split(':', 1, expand=True) # Spalte Satz 3 auftrennen
        
    if df['4'].isnull().all():
        #df.insert(column='Satz 4 Spieler', loc=18,value=NaN)
        #df.insert(column='Satz 4 Gegner', loc=19,value=NaN)        
        df[['Satz 4 Spieler','Satz 4 Gegner']] = NaN
    else:        
        df[['Satz 4 Spieler', 'Satz 4 Gegner']] = df['4'].str.split(':', 1, expand=True) # Spalte Satz 4 auftrennen
    
    if df['5'].isnull().all():      
        #df.insert(column='Satz 5 Spieler', loc=20,value=NaN)
        #df.insert(column='Satz 5 Gegner', loc=21,value=NaN)        
        df[['Satz 5 Spieler','Satz 5 Gegner']] = NaN
    else:        
        df[['Satz 5 Spieler', 'Satz 5 Gegner']] = df['5'].str.split(':', 1, expand=True) # Spalte Satz 5 auftrennen
    
    if df['6'].isnull().all():      
        #df.insert(column='Satz 6 Spieler', loc=22,value=NaN)
        #df.insert(column='Satz 6 Gegner', loc=23,value=NaN)        
        df[['Satz 6 Spieler','Satz 6 Gegner']] = NaN
    else:        
        df[['Satz 6 Spieler', 'Satz 6 Gegner']] = df['6'].str.split(':', 1, expand=True) # Spalte Satz 6 auftrennen

    if df['7'].isnull().all():      
        #df.insert(column='Satz 7 Spieler', loc=24,value=NaN)
        #df.insert(column='Satz 7 Gegner', loc=25,value=NaN)        
        df[['Satz 7 Spieler','Satz 7 Gegner']] = NaN
    else:        
        df[['Satz 7 Spieler', 'Satz 7 Gegner']] = df['7'].str.split(':', 1, expand=True) # Spalte Satz 7 auftrennen 
    
    df[['Sätze Spieler', 'Sätze Gegner']] = df['Ergebnis  Erg.'].str.split(':', 1, expand=True) # Spalte Sätze auftrennen

    # TODO: split column 'Begegnung' in 'Team Spieler' and 'Team Kontrahent'
        
    df.drop(columns=['Datum  Details', '1', '2', '3', '4', '5', '6', '7', 'Ergebnis  Erg.'], inplace=True) #alte Spalten mit den kombinierten Werten wegdonnern
    
def addMasterdataOfCurrentPlayer(df, row):
    df_matches['clickttid Spieler'] = row['clickttid']
    df_matches['Spieler'] = row['Spieler']
    df_matches['Geschlecht Spieler'] = row['Geschlecht']

cred_file = '.secrets.cfg'
logging_file_name = 'debug_myttde_matches.log'
clickttids_file_name = "./data/clickttids.csv"
output_file_name = "./data/all_matches.csv"

logging.basicConfig(format = '%(asctime)s - %(message)s', level = logging.INFO, filename = logging_file_name)

creds = getCredentials(cred_file)
myTTDEsession = loginToMyTischtennis(*creds)

all_missed_clickttids = []
df_clickttids = readClickttIdsFromFile(clickttids_file_name)

with open(output_file_name, mode = 'a', encoding = 'utf-8') as f_output:
    for index, row in df_clickttids.iterrows():
        try:            
            url = generateURLForClickttId(str(row['clickttid']))
            df_matches = extractMatchesFrom(url, myTTDEsession)
            if df_matches is not None:
                prettifyDataframe(df_matches)
                addMasterdataOfCurrentPlayer(df_matches, row)
                df_matches.to_csv(f_output, header = f_output.tell()==0, sep = '\t', encoding = 'utf-8', index = False)
        except:
            all_missed_clickttids.append(row['clickttid'])
            
print(all_missed_clickttids)
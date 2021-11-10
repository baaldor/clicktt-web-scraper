from numpy import NaN
import requests
from bs4 import BeautifulSoup
import pandas
import logging
import requests
from requests.sessions import session
import configparser
import time

def generateURLForRankingPage(ttrVon:str, ttrBis:str, geburtsjahrVon:str, geburtsjahrBis:str):
    url_ttr_rangliste = "https://www.mytischtennis.de/community/ajax/_rankingList?kontinent=Europa&land=DE&deutschePlusGleichgest=false&alleSpielberechtigen=&verband=&bezirk=&kreis=&regionPattern123=&regionPattern4=&regionPattern5=&geschlecht=&geburtsJahrVon=" + geburtsjahrVon + "&geburtsJahrBis=" + geburtsjahrBis + "&ttrVon=" + ttrVon + "&ttrBis=" + ttrBis +"&ttrQuartalorAktuell=aktuell&anzahlErgebnisse=500&vorname=&nachname=&verein=&vereinId=&vereinPersonenSuche=&vereinIdPersonenSuche=&ligen=&groupId=&showGroupId=&deutschePlusGleichgest2=false&ttrQuartalorAktuell2=aktuell&showmyfriends=0"
    #"https://www.mytischtennis.de/community/ranking?panel=1&kontinent=Europa&land=DE&verband=&bezirk=&kreis=&vereinId=&geschlecht=&geburtsJahrVon=&geburtsJahrBis=&ttrVon=" + str(ttrVon) + "&ttrBis=" + str(ttrBis) + "&ttrQuartalorAktuell=aktuell&anzahlErgebnisse=500&goAssistent=Anzeigen"
    return url_ttr_rangliste

def extractRankingTableFrom(url:str, aTTDEsession:requests.session):
    logging.debug("Extracting ranking tables from " + url)

    clickttids = []
    names = []
    sex = []

    page = aTTDEsession.get(url)
    soup = BeautifulSoup(page.content, "html5lib", from_encoding = 'utf-8')
    
    if soup:
        [table,] = soup.find_all("table")
    
    if table:
        all_a = table.find_all("a", {"data-tooltipdata": True})
        all_img = table.find_all("img")
    
    if all_a:
        clickttids = [str.split(a['data-tooltipdata'], sep = ';', maxsplit = 1)[0] for a in all_a]
        names = [', '.join(list(a.stripped_strings)[::-1]) for a in all_a]
    
    if all_img:    
        sex = [str.split(img['title'], sep = ',', maxsplit = 1)[0] for img in all_img]
    
    df = pandas.DataFrame(
        {
            'clickttid': clickttids,
            'Spieler': names,
            'Geschlecht': sex
        }
    )
    return df

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
    
    logging.debug("Login Status code " + str(login_req.status_code))
    #cookies = login_req.cookies

    return s

def getCredentials(file:str):
    # parsing values from file
    config = configparser.ConfigParser()
    config.read(file)
    username = config.get("credentials", "username")
    password = config.get("credentials", "password")
    return (username, password)

output_file_name = "./data/clickttids.csv"
cred_file = '.secrets.cfg'
logging_file_name = 'debug_clickttids.log'
ttrStep = 100
pause_scraping_duration = 10

logging.basicConfig(format = '%(asctime)s - %(message)s', level = logging.DEBUG, filename = logging_file_name)

creds = getCredentials(cred_file)
myTTDEsession = loginToMyTischtennis(*creds)

df_all_players = pandas.DataFrame()
queries_that_exceed_500 = []
queries_that_found_no_players = []
queries_that_throw_exceptions = []

with open(output_file_name, mode = 'a', encoding = 'utf-8') as f_output:
    for currentYearOfBirth in range(1921,2017): #min and max value from LoV of ranking list's dropdown field #range(1980, 1982):
        for currentTTR in range(500, 2800, ttrStep): #range(800, 1000, ttrStep):        
            try:
                time.sleep(pause_scraping_duration) #keep the traffic low
                url = generateURLForRankingPage(str(currentTTR), str(currentTTR + ttrStep), str(currentYearOfBirth), str(currentYearOfBirth))
                df_players = extractRankingTableFrom(url, myTTDEsession)
                if len(df_players.index) == 0:
                    print("Found no entries in the current table at all. We probably missed some data: TTR = " + str(currentTTR) + ", Year of Birth = " + str(currentYearOfBirth))
                    queries_that_found_no_players.append((currentYearOfBirth, currentTTR))
                else:
                    df_players['Geburtsjahr'] = currentYearOfBirth
                if len(df_players.index) == 500:
                    print("Found 500 entries (max load) in the current table. We probably missed some data: TTR = " + str(currentTTR) + ", Year of Birth = " + str(currentYearOfBirth))
                    queries_that_exceed_500.append((currentYearOfBirth, currentTTR))
                #df_all_players = df_all_players.append(df_players)
                df_players.to_csv(f_output, header = f_output.tell()==0, sep = '\t', encoding = 'utf-8', index = False)
            except:
                queries_that_throw_exceptions.append((currentYearOfBirth, currentTTR))
#df_all_players.to_csv(f_output, sep = '\t', encoding = 'utf-8', index = False)

print("Queries that found no players:")
print(queries_that_found_no_players)
print("Queries that hit the maximum size of the result of 500:")
print(queries_that_exceed_500)
print("Queries that throw exception:")
print(queries_that_throw_exceptions)
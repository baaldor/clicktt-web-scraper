from numpy import NaN
import requests
from bs4 import BeautifulSoup
import pandas
import logging

def extractSeasonDataFromTable(df:pandas.DataFrame) -> pandas.DataFrame:
    df.rename(columns={"Unnamed: 3": "Satz 1","Unnamed: 4": "Satz 2","Unnamed: 5": "Satz 3","Unnamed: 6": "Satz 4","Unnamed: 7": "Satz 5"},inplace=True)
    df.dropna(subset=['Sätze'], inplace=True) # komplett leere Zeilen rausfiltern, die durch formatierungsbedingte Leerzeilen im HTML entstehen
    
    # Matches, bei denen der Gegner nicht antritt werden aufgeführt mit 3x 11:0 --> rausschmeißen
    df.drop(df[df['Gegner'] == "nicht anwesend/angetreten,"].index , inplace=True)

    df[['Datum / Spiel']] = df[['Datum / Spiel']].fillna(method="ffill") # fehlende Werte in Spalte Datum / Spiel auffüllen
    df[['Datum', 'Verein']] = df['Datum / Spiel'].str.split(' ', 1, expand=True) # Spalte Datum/Spiel auftrennen
    df['Datum'] = pandas.to_datetime(df['Datum'],format="%d.%m.%Y") #konvertiere Datumsspalte von String zu datetime
    df[['Satz 1 Spieler', 'Satz 1 Gegner']] = df['Satz 1'].str.split(':', 1, expand=True) # Spalte Satz 1 auftrennen
    df[['Satz 2 Spieler', 'Satz 2 Gegner']] = df['Satz 2'].str.split(':', 1, expand=True) # Spalte Satz 2 auftrennen
    df[['Satz 3 Spieler', 'Satz 3 Gegner']] = df['Satz 3'].str.split(':', 1, expand=True) # Spalte Satz 3 auftrennen
        
    if df['Satz 4'].isnull().all():
        df.insert(column='Satz 4 Spieler', loc=18,value=NaN)
        df.insert(column='Satz 4 Gegner', loc=19,value=NaN)        
    else:        
        df[['Satz 4 Spieler', 'Satz 4 Gegner']] = df['Satz 4'].str.split(':', 1, expand=True) # Spalte Satz 4 auftrennen
    
    if df['Satz 5'].isnull().all():      
        df.insert(column='Satz 5 Spieler', loc=20,value=NaN)
        df.insert(column='Satz 5 Gegner', loc=21,value=NaN)        
    else:        
        df[['Satz 5 Spieler', 'Satz 5 Gegner']] = df['Satz 5'].str.split(':', 1, expand=True) # Spalte Satz 5 auftrennen
    
    df[['Sätze Spieler', 'Rest']] = df['Sätze'].str.split(':', 1, expand=True) # Spalte Sätze auftrennen
    df[['Sätze Gegner', 'Rest2']] = df['Rest'].str.split(' ', 1, expand=True) # Spalte Sätze auftrennen
    
    df.drop(columns=['Datum / Spiel', 'Satz 1', 'Satz 2', 'Satz 3', 'Satz 4', 'Satz 5', 'Sätze', 'Spiele', 'Rest', 'Rest2'], inplace=True) #alte Spalten mit den kombinierten Werten wegdonnern

def extractMatchResultTablesFrom(url:str):
    logging.debug("Extracting match result tables from " + url)

    dfs = []
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html5lib")
    abschnitt_mit_einzeln = soup.find(id="single") # Tabelle mit den Ergebnissen der Doppelspiele rausfiltern
    
    # TODO: Saison merken oder extrahieren und jedem Spiel als Attribut mitgeben

    countTable = 0
    countSpielklassen = 0

    if abschnitt_mit_einzeln is not None: #.is_empty_element() == False:
        spielklassen_mit_staffel = [a_h3_tag.get_text() for a_h3_tag in abschnitt_mit_einzeln.find_all('h3')]
        spielklassen = [" ".join(a.split(" ", 2)[:2]) for a in spielklassen_mit_staffel]
        countSpielklassen = len(spielklassen)        

        dfs = pandas.read_html(abschnitt_mit_einzeln.prettify(),flavor='html5lib',header=0) #Tabelle/Dataframe aus HTML extrahieren
        countTable = len(dfs)    

        if countSpielklassen != countTable:
            logging.debug("Anzahl der Spielklassen ungleich Anzahl der Ergebnistabellen")

        dfs_spielklassen = zip(dfs, spielklassen)

        for (df,spielklasse) in dfs_spielklassen:
            df['Spielklasse'] = spielklasse
    
    logging.debug("Anzahl gefundener Spielklassen: " + str(countSpielklassen))
    logging.debug("Anzahl gefundener Tabellen: " + str(countTable))
    
    return dfs

def generateURL(spieler_id:str,years:list):
    verband = "TTVB"
    current_season = "21-22"
    myURLs=[]
    all_seasons = [str(y) + "%2F" + str(y+1) for y in years]
    myURLs = ["https://www.mytischtennis.de/clicktt/" + verband + "/" + current_season + "/spieler/" + spieler_id + "/spielerportrait/?spielersaison=" + lookup_season for lookup_season in all_seasons]
    return myURLs

# spieler_id = "90000112883" # stef
spieler_id = "10739" # ich
file_name = "./data/tt-data.csv"
logging_file_name = 'debug_spielerportrait.log'

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG, filename=logging_file_name)

df_all_matches = pandas.DataFrame()

# TODO: log which data has been fetched (player id, season)

# TODO: append new data to already fetched data

urls_for_all_seasons = generateURL(spieler_id, list(range(16,22))) #range(16,22)

for URL in urls_for_all_seasons:
    dfs = extractMatchResultTablesFrom(URL) # dfs kann 0-n Tabellen enthalten (pro gespielter Liga Hin- und Rückrunde der jeweiligen Saison)
    
    # relevante Daten aus den Tabellen/Dataframes extrahieren
    for df in dfs:
        extractSeasonDataFromTable(df)

    # Daten für die Saison zusammenführen und in    
    if dfs:
        df_complete_season = pandas.concat(dfs)        
        df_all_matches = df_all_matches.append(df_complete_season)
    
    print(df_all_matches)
    
    df_all_matches.to_csv(file_name, sep='\t', encoding='utf-8', index=False)
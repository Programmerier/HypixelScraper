from pathlib import Path
import sys

FOLDER_DICT = Path(r"C:\Users\Hans\Desktop\lunarFiles")

def getLogText(logPath) -> str:
    with open(logPath, "r") as f:
        return f.read()

def getLogs(path) -> list[Path]:
    logList = []
    for log in path.iterdir():
            if log.name.endswith('.log'):
                  logList.append(log)
    return logList       


#Spieler welche Bridge Fight gewinnen
def extractVoidKills(incomedict) -> dict[str,int]:
    voidDict: dict[str, int] = {}

    for log in incomedict:
        for rawLine in incomedict[log].splitlines():
            if "void" in rawLine and ' by ' in rawLine:
                splittext = rawLine.split() 
                index_was = splittext.index("by")
                playername = splittext[index_was + 1].rstrip('.')
                if playername not in voidDict:
                    voidDict[playername] = 0
                voidDict[playername] += 1         
                
    
    return voidDict 


def extractVoidDeaths(incomedict) -> dict[str,int]:
    voidDict: dict[str, int] = {}

    for log in incomedict:
        for rawLine in incomedict[log].splitlines():
            if "void" in rawLine and ' by ' in rawLine:
                splittext = rawLine.split() 
                index_was = splittext.index("was")
                playername = splittext[index_was - 1]
                if playername not in voidDict:
                    voidDict[playername] = 0
                voidDict[playername] += 1         
                
    
    return voidDict 

def get_kd(player: str, kills: dict, deaths: dict) -> float:
    k = kills.get(player, 0)
    d = deaths.get(player, 0)
    return k / d if d > 0 else float(k)


def get_synced_dict(voidKillerDict, voidDeathDict):
    alleKiller = [name.rstrip('.') for name in voidKillerDict.keys()]
    alleTote   = list(voidDeathDict.keys())

    kombiListe = list(set(alleKiller + alleTote))
    return kombiListe


from datetime import datetime
def bedwarsSessionStart(log, logPath) -> dict:  # ← logPath neu
    sessionCount = 0
    current_session_chat = ""
    sessionDict = {}
    inSession = False
    starttime_object = None
    lines = log.splitlines()
    for i, line in enumerate(lines): 
        if '"gametype":"BEDWARS","mode":' in line:
            sessionCount += 1
            starttime_object = datetime.strptime(line[1:9], '%H:%M:%S')
            inSession = True
        elif '"gametype":"BEDWARS","lobbyname":' in line:
            inSession = False
            endtime_object = datetime.strptime(line[1:9], '%H:%M:%S')
            sessionDict[(logPath, sessionCount)] = current_session_chat, starttime_object, endtime_object  
            current_session_chat = ""
        if inSession:
            current_session_chat += line + "\n"

    return sessionDict



def main():
    logs = getLogs(FOLDER_DICT)
    logDictionary: dict[Path, str] = {}
    for log in logs:
        logDictionary[log] = getLogText(log)
        
    voidKillerDict = extractVoidKills(logDictionary)
    voidDeathDict = extractVoidDeaths(logDictionary)
    """
    print("-*60")
    print("KillerListe: \n")
    print(voidKillerDict)
    print("-"*60)
    print("TodesListe: \n")
    print(voidDeathDict)
    print("-"*60 + "\n")
    """
    snyc_list = get_synced_dict(voidKillerDict, voidDeathDict)
    #print("KombiListe: \n")
    #print(snyc_list)
    #print("-"*60 + "\n")

    player_dict: dict[str, float,int,int] = {}
    for spieler in snyc_list:
        
        kd_player = get_kd(spieler, voidKillerDict,voidDeathDict)
        #print(f"Spieler: {spieler}: \nHat eine: {kd_player}K/D Kills: {voidKillerDict.get(spieler, 0)} Deaths: {voidDeathDict.get(spieler, 0)}\n")
        player_dict[spieler] = kd_player,voidKillerDict.get(spieler, 0),voidDeathDict.get(spieler, 0)

       
    
    #TestZwecke
    #sessionDict = bedwarsSessionStart(logDictionary[Path(r"C:\Users\Hans\Desktop\lunarFiles\latest.log")])
    sessionDict: dict[int, object] = {}  

    for log in logDictionary:
        result = bedwarsSessionStart(logDictionary[log], log)  # ← log (Path) mitgeben
        if result:
            sessionDict.update(result)

        print(len(sessionDict), log)



if __name__ == "__main__":
    main()
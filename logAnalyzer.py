from pathlib import Path


FOLDER_DICT = Path(r"C:\Users\*\Desktop\lunarFiles")

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

def getKD(spieler, killer_dict,death_dict):
    try:
        kills = killer_dict[spieler]
    except:
        kills = 0
    
    try:
        deaths = death_dict[spieler]
    except:
        deaths = 0

    try:  
        return kills / deaths
    except:
        if kills == 0:
            return 0
        elif deaths == 0:
            return kills


def get_synced_dict(voidKillerDict, voidDeathDict):
    alleKiller = [name.rstrip('.') for name in voidKillerDict.keys()]
    alleTote   = list(voidDeathDict.keys())

    kombiListe = list(set(alleKiller + alleTote))
    return kombiListe



def main():
    logs = getLogs(FOLDER_DICT)

    #TemporaryChecker
    testSet = logs[2:20]
    logDictionary: dict[Path, str] = {}
    for log in logs:
        logDictionary[log] = getLogText(log)
        
    voidKillerDict = extractVoidKills(logDictionary)
    voidDeathDict = extractVoidDeaths(logDictionary)
    print("-*60")
    print("KillerListe: \n")
    print(voidKillerDict)
    print("-"*60)
    print("TodesListe: \n")
    print(voidDeathDict)
    print("-"*60 + "\n")

    snyc_list = get_synced_dict(voidKillerDict, voidDeathDict)
    print("KombiListe: \n")
    print(snyc_list)
    print("-"*60 + "\n")

    player_dict: dict[str, float,int,int] = {}
    for spieler in snyc_list:
        
        kd_player = getKD(spieler, voidKillerDict,voidDeathDict)
        #print(f"Spieler: {spieler}: \nHat eine: {kd_player}K/D Kills: {voidKillerDict.get(spieler, 0)} Deaths: {voidDeathDict.get(spieler, 0)}\n")
        player_dict[spieler] = kd_player,voidKillerDict.get(spieler, 0),voidDeathDict.get(spieler, 0)

    print(player_dict)
    print(player_dict["KeinProblemlol"])
    
if __name__ == "__main__":
    main()
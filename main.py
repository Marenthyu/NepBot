#!/usr/bin/python3
# coding=utf-8
import pymysql
import pydle
import random
from random import choice
import datetime
import time
from threading import Timer, Thread
import urllib.request
import requests
import json
import threading
import math
import functools
from string import ascii_letters

from twisted.internet import reactor
from autobahn.twisted.websocket import WebSocketClientProtocol
from autobahn.twisted.websocket import WebSocketClientFactory
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.ssl import optionsForClientTLS

import sys
import re

games = ['Hyperdimension Neptunia Re;Birth1', 'Hyperdimension Neptunia Re;Birth2: Sisters Generation',
         'Four Goddesses Online: Cyber Dimension Neptune', 'Hyperdimension Neptunia mk2',
         'Hyperdimension Neptunia Victory', 'Megadimension Neptunia VII', 'Superdimension Neptune vs Sega Hard Girls',
         'Hyperdimension Neptunia Re;Birth 3: V Century', 'Hyperdimension Neptunia: Producing Perfection',
         'Hyperdimension Neptunia U: Action Unleashed', 'MegaTagmension Blanc + Neptune VS Zombies',
         'Hyperdimension Neptunia']
gamesdict = {"Opening SetUp":games[11], "Hyperdimension Neptunia Rebirth 1":games[0],
             "Hyperdimension Neptunia Rebirth 2: Sister's Generation":games[1], "Four Goddess: Cyber Neptune":games[2],
             "Hyperdimension Neptunia MK2":games[3], "Hyperdimension Neptunia Victory":games[4],
             "Megadimension Neptunia Victory II":games[5], "Superdimension Neptune Vs Sega Hard Girls":games[6],
             "Hyperdimension Neptunia Rebirth 2":games[1], "Hyperdimension Neptunia Rebirth 3":games[7],
             "Hyperdimension Neptunia: Producing Perfection":games[8], "Hyperdimension Neptunia : Action Unleashed":games[9],
             "MegaTagmension Blanc + Neptune VS Zombies":games[10]}
ffzws = 'wss://andknuckles.frankerfacez.com'
pool = pydle.ClientPool()
current_milli_time = lambda: int(round(time.time() * 1000))
pymysql.install_as_MySQLdb()
global dbpw
dbpw = None
global hdnoauth
hdnoauth = None
global streamlabsclient
streamlabsclient = None
# read config values from file (db login etc)
try:
    f = open("nepbot.cfg", "r")
    lines = f.readlines()
    for line in lines:
        name, value = line.split("=")
        value = str(value).strip("\n")
        print("Reading config value '{name}' = '<redacted>'".format(name=name))
        if name == "password":
            dbpw = value
        if name == "hdnoauth":
            hdnoauth = value
        if name == "streamlabsclient":
            streamlabsclient = value
    if dbpw == None:
        print("Database password not set. Please add it to the config file, with 'password=<pw>'")
        sys.exit(1)
    if hdnoauth == None:
        print("HDNMarathon Channel oauth not set. Please add it to the conig file, with 'hdnoauth=<pw>'")
        sys.exit(1)
    f.close()
except:
    print("Error reading config file (nepbot.cfg), aborting.")
    sys.exit(1)


db = pymysql.connect(host="localhost", user="nepbot", passwd=dbpw, db="nepbot", autocommit="True", charset="utf8mb4")
trades = {}
activitymap = {}
blacklist = []
visiblepacks = ""

busyLock = threading.Lock()
streamlabsauthurl = "https://www.streamlabs.com/api/v1.0/authorize?client_id=" + streamlabsclient + "&redirect_uri=http://marenthyu.de/cgi-bin/waifucallback.cgi&response_type=code&scope=alerts.create&state="
streamlabsalerturl = "https://streamlabs.com/api/v1.0/alerts"
alertheaders = {"Content-Type":"application/json", "User-Agent":"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"}
time_regex = re.compile('(?P<hours>[0-9]*):(?P<minutes>[0-9]{2}):(?P<seconds>[0-9]{2})(\.(?P<ms>[0-9]{1,3}))?')
waifu_regex = re.compile('(\[(?P<id>[0-9]+?)\])?(?P<name>.+?) ?- ?(?P<series>.+?) ?- ?(?P<rarity>[0-6]) ?- ?(?P<link>.+?)')
validalertconfigvalues = ["color", "alertChannel", "defaultLength", "defaultSound", "rarity4Length", "rarity4Sound", "rarity5Length", "rarity5Sound", "rarity6Length", "rarity6Sound"]

def placeBet(channel, userid, betms):
    cur = db.cursor()
    cur.execute("SELECT id FROM bets WHERE channel = %s AND status = 'open' LIMIT 1", [channel])
    row = cur.fetchone()
    if row is None:
        cur.close()
        return False
    cur.execute("REPLACE INTO placed_bets (betid, userid, bet, updated) VALUE (%s, %s, %s, %s)", [row[0], userid, betms, current_milli_time()])
    cur.close()
    return True

def endBet(channel):
    # find started bet data
    cur = db.cursor()
    cur.execute("SELECT id FROM bets WHERE channel = %s AND status = 'started' LIMIT 1", [channel])
    row = cur.fetchone()
    if row is None:
        cur.close()
        return None
    
    # mark the bet as closed
    endTime = current_milli_time()
    cur.execute("UPDATE bets SET status = 'completed', endTime = %s WHERE id = %s", [endTime, row[0]])
    
    # calculate preliminary results
    cur.close()
    return getBetResults(row[0])
    
    
def getBetResults(betid):
    # get bet data
    cur = db.cursor()
    cur.execute("SELECT status, startTime, endTime FROM bets WHERE id = %s", [betid])
    betrow = cur.fetchone()
    if betrow is None:
        cur.close()
        return None
        
    if betrow[0] != 'completed' and betrow[0] != 'paid':
        cur.close()
        return None
        
    timeresult = betrow[2] - betrow[1]
    cur.execute("SELECT bet, userid, users.name FROM placed_bets INNER JOIN users ON placed_bets.userid = users.id WHERE betid = %s ORDER BY updated ASC", [betid])
    rows = cur.fetchall()
    placements = sorted(rows, key=lambda row: abs(int(row[0]) - timeresult))
    actualwinners = [{"id": row[1], "name": row[2], "bet": row[0], "timedelta": row[0] - timeresult} for row in placements]
    cur.close()
    return {"result": timeresult, "winners": actualwinners}
    

def startBet(channel):
    cur = db.cursor()
    cur.execute("SELECT id FROM bets WHERE channel = %s AND status = 'open' LIMIT 1", [channel])
    row = cur.fetchone()
    if row is not None:
        cur.execute("UPDATE bets SET startTime = %s, status = 'started' WHERE id = %s", [current_milli_time(), row[0]])
        cur.close()
        return True
    else:
        cur.close()
        return False

def openBet(channel):
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) FROM bets WHERE channel = %s AND status IN('open', 'started')", [channel])
    result = cur.fetchone()[0] or 0
    if result > 0:
        cur.close()
        return False
    else:
        cur.execute("INSERT INTO bets(channel, status) VALUE (%s, 'open')", [channel])
        cur.close()
        return True
        
def cancelBet(channel):
    cur = db.cursor()
    affected = cur.execute("UPDATE bets SET status = 'cancelled' WHERE channel = %s AND status IN('open', 'started')", [channel])
    cur.close()
    return affected > 0

def getHand(twitchid):
    try:
        tID = int(twitchid)
    except:
        print("Got non-integer id for getHand. Aborting.")
        return []
    cur = db.cursor()
    cur.execute("SELECT amount, waifus.name, waifus.id, rarity, series, image FROM has_waifu JOIN waifus ON has_waifu.waifuid = waifus.id WHERE has_waifu.userid = %s ORDER BY (rarity < 7) DESC, waifus.id ASC", [tID])
    rows = cur.fetchall()
    cur.close()
    return [{"name":row[1], "amount":row[0], "id":row[2], "rarity":row[3], "series":row[4], "image":row[5]} for row in rows]

def search(query):
    cur = db.cursor()
    cur.execute("SELECT id, name, series, rarity FROM waifus WHERE Name LIKE %s", ["%" + str(query) + "%"])
    rows = cur.fetchall()
    ret = []
    for row in rows:
        ret.append({'id':row[0], 'name':row[1], 'series':row[2], 'rarity':row[3]})
    return ret

def whoHas(id):
    try:
        a = int(id)
    except: return []
    cur = db.cursor()
    cur.execute("SELECT users.name FROM has_waifu INNER JOIN users ON has_waifu.userid = users.id WHERE has_waifu.waifuid = %s", str(id))
    rows = cur.fetchall()
    ret = []
    for row in rows:
        ret.append(row[0])
    cur.close()
    return ret

def handLimit(userid):
    cur = db.cursor()
    cur.execute("SELECT handLimit FROM users WHERE id = %s", [userid])
    res = cur.fetchone()
    limit = int(res[0])
    cur.close()
    return limit

def paidHandUpgrades(userid):
    cur = db.cursor()
    cur.execute("SELECT paidHandUpgrades FROM users WHERE id = %s", [userid])
    res = cur.fetchone()
    limit = int(res[0])
    cur.close()
    return limit

def upgradeHand(userid, gifted=False):
    cur = db.cursor()
    cur.execute("UPDATE users SET handLimit = handLimit + 1, paidHandUpgrades = paidHandUpgrades + %s WHERE id = %s", [0 if gifted else 1, userid])
    cur.close()

def addDisplayToken(token, waifus):
    if (waifus == None or len(waifus) == 0):
        return
    valuesstring = "('{token}', {waifu})".format(waifu=str(waifus[0]), token=str(token))
    for w in waifus[1:]:
        valuesstring = valuesstring + ", ('{token}', {waifu})".format(waifu=str(w), token=str(token))

    cur = db.cursor()
    cur.execute("INSERT INTO displayTokens(token, waifuid) VALUES {valuesstring}".format(valuesstring=valuesstring))
    cur.close()

def getHoraro():
    "https://horaro.org/-/api/v1/schedules/3911mu51ljb1wf7a5e/ticker"
    r = requests.get("https://horaro.org/-/api/v1/schedules/{horaroid}/ticker?hiddenkey=NepSmug".format(horaroid=config["horaroID"]))
    try:
        j = r.json()
        #("got horaro ticker: " + str(j))
        return j
    except:
        print("Horaro Error:")
        print(str(r.status_code))
        print(r.text)

def updateBoth(game, title):
    myheaders = headers.copy()
    myheaders["Authorization"] = "OAuth " + str(hdnoauth).replace("oauth:", "")
    myheaders["Content-Type"] = "application/json"
    body = {"channel": {"status": str(title), "game": str(game)}}
    # print("headers: " + str(myheaders))
    # print("body: " + str(body))
    r = requests.put("https://api.twitch.tv/kraken/channels/143262392", headers=myheaders, json=body)
    try:
        j = r.json()
        # print("tried to update channel title, response: " + str(j))
    except:
        print(str(r.status_code))
        print(r.text)

def updateTitle(title):
    myheaders = headers.copy()
    myheaders["Authorization"] = "OAuth " + str(hdnoauth).replace("oauth:", "")
    myheaders["Content-Type"] = "application/json"
    body = {"channel":{"status":str(title)}}
    #print("headers: " + str(myheaders))
    #print("body: " + str(body))
    r = requests.put("https://api.twitch.tv/kraken/channels/143262392", headers=myheaders, json=body)
    try:
        j = r.json()
        #print("tried to update channel title, response: " + str(j))
    except:
        print(str(r.status_code))
        print(r.text)

def updateGame(game):
    myheaders = headers.copy()
    myheaders["Authorization"] = "OAuth " + str(hdnoauth).replace("oauth:", "")
    myheaders["Content-Type"] = "application/json"
    body = {"channel":{"game":str(game)}}
    #print("headers: " + str(myheaders))
    #print("body: " + str(body))
    r = requests.put("https://api.twitch.tv/kraken/channels/143262392", headers=myheaders, json=body)
    try:
        j = r.json()
        #print("tried to update channel title, response: " + str(j))
    except:
        print(str(r.status_code))
        print(r.text)

def setFollows(user):
    MyClientProtocol.instance.setFollowButtons(user)
    
def sendStreamlabsAlert(channel, data):
    # assumes busyLock is already reserved
    cur = db.cursor()
    if '#' in channel:
        channel = channel[1:]
    cur.execute("SELECT alertkey FROM channels WHERE name = %s LIMIT 1", [channel])
    tokenRow = cur.fetchone()
    if tokenRow is not None and tokenRow[0] is not None:
        data['access_token'] = tokenRow[0]
        try:
            req = requests.post(streamlabsalerturl, headers=alertheaders, json=data)
            if req.status_code != 200:
                print("response for streamlabs alert: " + str(req.status_code) + "; " + str(req.text))
        except:
            print("Tried to send a Streamlabs alert to %s, but failed." % channel)
            print("Error: " + str(sys.exc_info()))
            
    cur.close()
    
def sendDiscordAlert(data):
    # assumes busyLock is already reserved
    cur = db.cursor()
    cur.execute("SELECT url FROM discordHooks")
    discordhooks = cur.fetchall()

    for row in discordhooks:
        url = row[0]
        req2 = requests.post(
            url,
            json=data)
        while req2.status_code == 429:
            time.sleep((req2.headers["Retry-After"] / 1000) + 1)
            req2 = requests.post(
                url,
                json=data)

    cur.close()

def sendDrawAlert(channel, waifu, user, discord=True):
    print("Alerting for waifu " + str(waifu))
    with busyLock:
        message = "*{user}* drew [*{rarity}*] {name}!".format(user=str(user),
                                                          rarity=str(config["rarity" + str(waifu["rarity"]) + "Name"]),
                                                          name=str(waifu["name"]))
        cur = db.cursor()
        chanOwner = str(channel).replace("#", "")
        cur.execute("SELECT config, val FROM alertConfig WHERE channelName = %s", [chanOwner])
        rows = cur.fetchall()

        colorKey = "rarity" + str(waifu["rarity"]) + "EmbedColor"
        colorInt = int(config[colorKey])
        # Convert RGB int to RGB values
        blue = colorInt & 255
        green = (colorInt >> 8) & 255
        red = (colorInt >> 16) & 255

        alertconfig = {}
        for row in rows:
            alertconfig[row[0]] = row[1]
        keys = alertconfig.keys()
        alertChannel = "donation" if "alertChannel" not in keys else alertconfig["alertChannel"]
        defaultSound = config["alertSound"] if "defaultSound" not in keys else alertconfig["defaultSound"]
        alertSound = defaultSound if str("rarity" + str(waifu["rarity"]) + "Sound") not in keys else alertconfig[str("rarity" + str(waifu["rarity"]) + "Sound")]
        defaultLength = config["alertDuration"] if "defaultLength" not in keys else alertconfig["defaultLength"]
        alertLength = defaultLength if str("rarity" + str(waifu["rarity"]) + "Length") not in keys else alertconfig[str("rarity" + str(waifu["rarity"]) + "Length")]
        alertColor = "default" if "color" not in keys else alertconfig["color"]



        if "id" in waifu.keys():
            cur.execute("SELECT sound, length FROM waifuAlerts WHERE waifuid=%s", [waifu["id"]])
            rows = cur.fetchall()
            if len(rows) == 1:
                alertLength = int(rows[0][1])
                alertSound = str(rows[0][0])
        cur.close()
        alertbody = {"type": alertChannel, "image_href": waifu["image"],
                     "sound_href": alertSound, "duration": int(alertLength), "message": message}
        if alertColor == "rarity":
            alertbody["special_text_color"] = "rgb({r}, {g}, {b})".format(r=str(red), g=str(green), b=str(blue))

        sendStreamlabsAlert(channel, alertbody)
        if discord:
            discordbody = {"username": "Waifu TCG", "embeds": [
                {
                    "title": "A {rarity} waifu has been dropped!".format(
                        rarity=str(config["rarity" + str(waifu["rarity"]) + "Name"]))
                },
                {
                    "type": "rich",
                    "title": "{user} dropped {name}!".format(user=str(user), name=str(waifu["name"])),
                    "url": "https://twitch.tv/{name}".format(name=str(channel).replace("#", "").lower()),
                    "footer": {
                        "text": "Waifu TCG by Marenthyu"
                    },
                    "image": {
                        "url": str(waifu["image"])
                    },
                    "provider": {
                        "name": "Marenthyu",
                        "url": "http://marenthyu.de"
                    }
                }
            ]}
            if colorKey in config:
                discordbody["embeds"][0]["color"] = int(config[colorKey])
                discordbody["embeds"][1]["color"] = int(config[colorKey])
            sendDiscordAlert(discordbody)
        
def sendDisenchantAlert(channel, waifu, user):
    with busyLock:
        # no streamlabs alert for now
        # todo maybe make a b&w copy of the waifu image
        discordbody = {"username": "Waifu TCG", "embeds": [
            {
                "title": "A {rarity} waifu has been disenchanted!".format(
                    rarity=str(config["rarity" + str(waifu["rarity"]) + "Name"]))
            },
            {
                "type": "rich",
                "title": "{name} has been disenchanted! Press F to pay respects.".format(name=str(waifu["name"])),
                "footer": {
                    "text": "Waifu TCG by Marenthyu"
                },
                "image": {
                    "url": str(waifu["image"])
                },
                "provider": {
                    "name": "Marenthyu",
                    "url": "http://marenthyu.de"
                }
            }
        ]}
        colorKey = "rarity" + str(waifu["rarity"]) + "EmbedColor"
        if colorKey in config:
            discordbody["embeds"][0]["color"] = int(config[colorKey])
            discordbody["embeds"][1]["color"] = int(config[colorKey])
        sendDiscordAlert(discordbody)
        
def sendPromotionAlert(channel, waifu, user):
    print("Alerting for waifu " + str(waifu))
    with busyLock:
        message = "{user} promoted {name} to god!".format(user=str(user),
                                                          name=str(waifu["name"]))
        alertbody = {"type": "donation", "image_href": waifu["image"],
                     "sound_href": config["alertSound"], "duration": int(config["alertDuration"]), "message": message}
        sendStreamlabsAlert(channel, alertbody)
        discordbody = {"username": "Waifu TCG", "embeds": [
            {
                "title": "A waifu has been upgraded to god rarity!",
                "color": int(config["rarity6EmbedColor"])
            },
            {
                "type": "rich",
                "title": "{user} promoted {name} to god rarity!".format(user=str(user), name=str(waifu["name"])),
                "url": "https://twitch.tv/{name}".format(name=str(channel).replace("#", "").lower()),
                "color": int(config["rarity6EmbedColor"]),
                "footer": {
                    "text": "Waifu TCG by Marenthyu"
                },
                "image": {
                    "url": str(waifu["image"])
                },
                "provider": {
                    "name": "Marenthyu",
                    "url": "http://marenthyu.de"
                }
            }
        ]}
        sendDiscordAlert(discordbody)

def followsme(userid):
    try:
        r = requests.get("https://api.twitch.tv/kraken/users/{twitchid}/follows/{myid}".format(twitchid=str(userid), myid=str(config["twitchid"])), headers=headers)
        j = r.json()
        return j["status"] != "404"
    except:
        return False

def getWaifuById(id):
    try:
        id = int(id)
        if id == 0:
            return None
    except:
        #print("Someone tried to ask for ID "+ str(id) + " - not happening.")
        return {"name":"Mr. Astley", "series":"You know the Rules, and so do I.", "id":0, "image":"https://youtu.be/DLzxrzFCyOs", "rarity":"6"}
    cur = db.cursor()
    cur.execute("SELECT id, Name, image, rarity, series FROM waifus WHERE id=%s", [id])
    row = cur.fetchone()
    ret = {"id":row[0], "name":row[1], "image":row[2], "rarity":row[3], "series":row[4]}
    cur.close()
    #print("Fetched Waifu from id: " + str(ret))
    return ret

def hasPoints(userid, amount):
    cur = db.cursor()
    cur.execute("SELECT points FROM users WHERE id = %s", [userid])
    ret = int(cur.fetchone()[0]) >= int(amount)
    cur.close()
    return ret

def addPoints(userid, amount):
    cur = db.cursor()
    cur.execute("UPDATE users SET points = points + %s WHERE id = %s", [amount, userid])
    cur.close()

def currentCards(userid):
    cur = db.cursor()
    cur.execute("SELECT SUM(amount) AS totalCards FROM has_waifu INNER JOIN waifus ON has_waifu.waifuid = waifus.id WHERE has_waifu.userid = %s AND waifus.rarity < 7", [userid])
    ret = cur.fetchone()[0] or 0
    cur.close()
    return ret

def maxWaifuID():
    cur = db.cursor()
    cur.execute("SELECT MAX(id) FROM waifus")
    ret = int(cur.fetchone()[0])
    cur.close()
    return ret

def dropCard(rarity=-1, upgradeChances=None):
    random.seed()
    if rarity == -1:
        if upgradeChances is None:
            upgradeChances = [float(config["rarity%dUpgradeChance" % i]) for i in range(6)]
        i = 0
        rarity = 0
        while (i < 6):
            r = random.random()
            if r <= upgradeChances[i]:
                rarity = rarity + 1
                i = i + 1
                continue
            break
        return dropCard(rarity)
    else:
        #print("Dropping card of rarity " + str(rarity))
        cur = db.cursor()
        raritymax = int(config["rarity" + str(rarity) + "Max"])
        while True:
            cur.execute("SELECT id FROM waifus WHERE rarity = %s ORDER BY RAND() LIMIT 1", (rarity,))
            retid = cur.fetchone()[0]
            if raritymax == 0:
                break
            cur.execute("SELECT SUM(amount) FROM has_waifu WHERE waifuid = %s", (retid,))
            retcount = cur.fetchone()[0] or 0
            if raritymax > retcount:
                break
        cur.close()
        #print("Dropping ID " + str(retid))
        return retid

def giveCard(userid, id):
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) FROM has_waifu WHERE userid = %s AND waifuid = %s", [userid, id])
    hasWaifu = cur.fetchone()[0] == 1
    if hasWaifu:
        cur.execute("UPDATE has_waifu SET amount = amount + 1 WHERE userid = %s AND waifuid = %s", [userid, id])
    else:
        cur.execute("INSERT INTO has_waifu(userid, waifuid, amount) VALUES(%s, %s, %s)", [userid, id, 1])
    cur.close()
    
def logDrop(userid, waifuid, rarity, source, channel, isWhisper):
    trueChannel = "$$whisper$$" if isWhisper else channel
    cur = db.cursor()
    cur.execute("INSERT INTO drops(userid, waifuid, rarity, source, channel, timestamp) VALUES(%s, %s, %s, %s, %s, %s)", (userid, waifuid, rarity, source, trueChannel, current_milli_time()))
    cur.close()
    
def formatRank(rankNum):
    if (rankNum % 100) // 10 == 1 or rankNum % 10 == 0 or rankNum % 10 > 3:
        return "%dth" % rankNum
    elif rankNum % 10 == 1:
        return "%dst" % rankNum
    elif rankNum % 10 == 2:
        return "%dnd" % rankNum
    else:
        return "%drd" % rankNum
        
def formatTimeDelta(ms):
    baseRepr = str(datetime.timedelta(milliseconds=ms, microseconds=0))
    return baseRepr[:-3] if "." in baseRepr else baseRepr
    

# From https://github.com/Shizmob/pydle/issues/35
class PrivMessageTagSupport(pydle.features.ircv3.TaggedMessageSupport):
  def on_raw_privmsg(self, message):
    """ PRIVMSG command. """
    nick, metadata = self._parse_user(message.source)
    tags = message.tags
    target, message = message.params

    self._sync_user(nick, metadata)

    self.on_message(target, nick, message, tags)
    if self.is_channel(target):
        self.on_channel_message(target, nick, message, tags)
    else:
        self.on_private_message(nick, message, tags)

# End Github code
NepBotClass = pydle.featurize(pydle.Client, PrivMessageTagSupport)
class NepBot(NepBotClass):
    config = {}
    mychannels = []
    myadmins = []
    instance = None
    autoupdate = False
    pw = None
    nomodalerted = []
    addchannels = []
    leavechannels = []

    def __init__(self, config, channels, admins):
        super().__init__(config["username"])
        self.config = config
        self.myadmins = admins
        self.mychannels = channels
        NepBot.instance = self

    def on_clearchat(self, message):
        # print("Got clear chat message: " + str(message))
        nick, metadata = self._parse_user(message.source)
        tags = message.tags
        params = message.params
        # print("nick: {nick}; metadata: {metadata}; params: {params}; tags: {tags}".format(nick=nick, metadata=metadata, params=params, tags=tags))
        u = params[1]
        chan = params[0]
        reason = str(tags["ban-reason"]).replace("\\s", " ")
        if "ban-duration" in tags.keys():
            duration = tags["ban-duration"]
            print("{user} got timed out for {duration} seconds in {channel} for: {reason}".format(user=u, channel=chan, reason=reason, duration = duration))
        else:
            print("{user} got permanently banned from {channel}. Reason: {reason}".format(user=u, channel=chan, reason=reason))
        return

    def on_hosttarget(self, message):
        #print("Got Host Target: " + str(message))
        parts = str(message).split(" ")
        sourcechannel = parts[2].strip("#")
        target = parts[3].strip(":")
        print("{source} is now hosting {target}".format(source=sourcechannel, target=target))
        return

    def on_userstate(self, message):
        # print("Userstate...")
        nick, metadata = self._parse_user(message.source)
        tags = message.tags
        params = message.params
        # print("nick: {nick}; metadata: {metadata}; params: {params}; tags: {tags}".format(nick=nick, metadata=metadata, params=params, tags=tags))
        if tags["display-name"] == "Nepnepbot" and params[0] != "#nepnepbot" and tags["mod"] != '1' and params[0] not in self.nomodalerted:
            print("No Mod in " + str(params[0]) + "!")
            self.nomodalerted.append(params[0])
            self.message(params[0], "Hey! I noticed i am not a mod here! Please do mod me to avoid any issues!")
        return

    def on_roomstate(self, message):
        #print("Got Room State: " + str(message))
        return

    def on_raw_421(self, message):
        # print("Got raw 421:" + str(message))
        # Ignore twitch not knowing WHOIS
        if str(message).find("WHOIS") > -1:
            return
        super().on_raw_421(message)

    def on_whisper(self, message):
        nick, metadata = self._parse_user(message.source)
        tags = message.tags
        params = message.params
        # print("WHISPER received: nick: {nick}; metadata: {metadata}; params: {params}; tags: {tags}".format(nick=nick, metadata=metadata, params=params, tags=tags))
        self.on_message("#" + str(nick), str(nick), str(params[1]), tags, isWhisper=True)

    def on_unknown(self, message):
        if str(message).find("CLEARCHAT") > -1:
            self.on_clearchat(message)
            return
        if str(message).find("HOSTTARGET") > -1:
            self.on_hosttarget(message)
            return
        if str(message).find("USERSTATE") > -1:
            self.on_userstate(message)
            return
        if str(message).find("ROOMSTATE") > -1:
            self.on_roomstate(message)
            return
        if str(message).find("USERNOTICE") > -1:
            print("PogChamp! Someone subbed to someone! here's the message: " + str(message))
            return
        if str(message).find("WHISPER") > -1:
            self.on_whisper(message)
            return
        super().on_unknown(message)

    def start(self, password):
        pool.connect(self, "irc.twitch.tv", 6667, tls=False, password=password)
        self.pw = password
        print("Connecting...")
        def timer():
            with busyLock:
                global t
                t = Timer(int(config["cycleLength"]), timer)
                t.start()
                print("Refreshing Database Connection...")
                global db
                try:
                    db.close()
                except:
                    print("Error closing db connection cleanly, ignoring.")
                try:
                    db = pymysql.connect(host="localhost", user="nepbot", passwd=dbpw, db="nepbot", autocommit="True", charset="utf8mb4")
                except:
                    print("Error Reconnecting to DB. Skipping Timer Cycle.")
                    return
            print("Checking live status of channels...")
            with busyLock:
                cur = db.cursor()
                cur.execute("SELECT users.name, users.id FROM channels join users ON channels.name = users.name")
                rows = cur.fetchall()
                isLive = {}
                channelids = []
                idtoname = {}
                requrl = "https://api.twitch.tv/helix/streams?type=live&user_id="
                for row in rows:
                    channelids.append(str(row[1]))
                    idtoname[str(row[1])] = row[0]
                    isLive[str(row[0])] = False
            requrl += "&user_id=".join(channelids)
            twitchheader = {"Client-ID":config["clientID"]}
            with requests.get(requrl, headers=twitchheader) as response:
                data = response.json()["data"]
                #print("got data from live check:")
                #print(data)
                for element in data:
                    isLive[idtoname[str(element["user_id"])]] = True
                    print("{user} is live!".format(user=idtoname[str(element["user_id"])]))



            print("Catching all viewers...")
            for c in self.addchannels:
                self.mychannels.append(c)
            self.addchannels = []
            for c in self.leavechannels:
                try:
                    self.mychannels.remove(c)
                except:
                    print("Couldn't remove channel " + str(c) + " from channels, it wasn't found. Channel list: " + str(self.mychannels))
            self.leavechannels = []
            try:
                global activitymap
                #print("Activitymap: " + str(activitymap))
                doneusers = []
                validactivity = []
                for channel in self.channels:
                    #print("Fetching for channel " + str(channel))
                    channelName = str(channel).replace("#", "")
                    try:
                        with urllib.request.urlopen('https://tmi.twitch.tv/group/user/' + channelName + '/chatters') as response:
                            data = json.loads(response.read().decode())
                            chatters = data["chatters"]
                            a = chatters["moderators"] + chatters["staff"] + chatters["admins"] + chatters["global_mods"] + chatters["viewers"]

                            for viewer in a:
                                if viewer not in doneusers:
                                    doneusers.append(viewer)
                                if isLive[channelName] and viewer not in validactivity:
                                    validactivity.append(viewer)
                    except:
                        print("Error fetching chatters for %s, skipping their chat for this cycle" % channelName)
                        print("Error: " + str(sys.exc_info()))
                cur = db.cursor()
                # process all users
                print("Caught users, giving points and creating accounts")
                with busyLock:
                    for viewer in doneusers:
                        lviewer = str(viewer).lower()
                        cur.execute("SELECT COUNT(*) FROM users WHERE name = %s", [lviewer])
                        if int(cur.fetchone()[0]) == 0:
                            print("Creating account for " + lviewer)
                            r = requests.get("https://api.twitch.tv/kraken/users", headers=headers,
                                             params={"login": lviewer})
                            j = r.json()
                            try:
                                twitchid = j["users"][0]["_id"]
                            except:
                                twitchid = 0
                            cur.execute("SELECT COUNT(*) FROM users WHERE id = %s", [str(twitchid)])
                            if (cur.fetchone()[0] or 0) > 0:
                                print("Twitch ID already exists. Updating row with new name")
                                cur.execute("UPDATE users SET name = %s WHERE id = %s", [lviewer, str(twitchid)])
                            else:
                                cur.execute("INSERT INTO users (id, name, points, lastFree) VALUES(%s, %s, 0, 0)", [twitchid, lviewer])
                            #print("Success?")
                        pointGain = int(config["passivePoints"])
                        if lviewer in activitymap and lviewer in validactivity:
                            pointGain += max(10 - int(activitymap[lviewer]), 0)
                            
                        pointGain = round(pointGain * float(config["pointsMultiplier"]))
                        cur.execute("UPDATE users SET points = points + %s WHERE name = %s", [pointGain, lviewer])
                    cur.close()


                    for user in activitymap.keys():
                        activitymap[user] = activitymap[user] + 1
            except:
                print("We had an error during passive point gain. skipping this cycle.")
                print("Error: " + str(sys.exc_info()))

            if self.autoupdate:
                print("Updating Title and Game with horaro info")
                schedule = getHoraro()
                try:
                    data = schedule["data"]
                    ticker = data["ticker"]
                    current = ticker["current"]
                    wasNone = False
                    if current == None:
                        current = ticker["next"]
                        wasNone = True
                    current = current["data"]
                    game = current[0]
                    category = current[1]
                    runners = []
                    for runner in current[2:]:
                        if runner != None:
                            parts = str(runner).split("](")
                            p = parts[0].replace("[", "")
                            runners.append(p)
                    title = "{comingup}HDN MARATHON - {game}{category}{runners} - !schedule".format(game=str(game),
                                                                                                    category=(
                                                                                                    " (" + str(
                                                                                                        category) + ")") if category != None else "",
                                                                                                    comingup="COMING UP: " if wasNone else "",
                                                                                                    runners=(
                                                                                                    " by " + ", ".join(
                                                                                                        runners)) if runners != [] else "")
                    updateBoth(str(gamesdict[str(game)]) if game in gamesdict.keys() else "Hyperdimension Neptunia", title=title)
                    setFollows(runners)
                except:
                    print("Error updating from Horaro. Skipping this cycle.")
                    print("Error: " + str(sys.exc_info()))

            with busyLock:
                cur = db.cursor()
                try:
                    #print("Deleting outdated displayTokens")

                    a = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
                    beforeSeconds = int((a - datetime.datetime(1970,1,1)).total_seconds())
                    #print("Using " + "DELETE FROM displayTokens WHERE unix_timestamp(timestamp) < {ts}".format(ts=str(beforeSeconds)))
                    cur.execute("DELETE FROM displayTokens WHERE unix_timestamp(timestamp) < {ts}".format(ts=str(beforeSeconds)))

                except:
                    print("Error deleting old tokens. skipping this cycle.")
                cur.close()

        timer()

    def on_capability_twitch_tv_membership_available(self, nothing=None):
        print("WE HAS TWITCH MEMBERSHIP AVAILABLE! ... but we dont want it.")
        return False

    def on_capability_twitch_tv_membership_enabled(self, nothing = None):
        print("WE HAS TWITCH MEMBERSHIP ENABLED!")
        return

    def on_capability_twitch_tv_tags_available(self, nothing = None):
        print("WE HAS TAGS AVAILABLE!")
        return True

    def on_capability_twitch_tv_tags_enabled(self, nothing = None):
        print("WE HAS TAGS ENABLED!")
        return

    def on_capability_twitch_tv_commands_available(self, nothing = None):
        print("WE HAS COMMANDS AVAILABLE!")
        return True

    def on_capability_twitch_tv_commands_enabled(self, nothing = None):
        print("WE HAS COMMANDS ENABLED!")
        return

    def on_raw_cap_ls(self, params):
        print("Got CAP LS")
        # print(str(params))
        # print(str(self._capabilities))
        # for capab in params[0].split():
        #     capab, value = self._capability_normalize(capab)
        #     print("Capab: {capab}, value: {value}".format(capab=str(capab), value=str(value)))
        #     attr = 'on_capability_' + pydle.protocol.identifierify(capab) + '_available'
        #     print("attr: " + str(attr))
        #     h = hasattr(self, attr)
        #     print("has attr: " + str(h))
        #     if h:
        #         g = getattr(self, attr)(value)
        #         print("get attr: " + str(g))
        super().on_raw_cap_ls(params)

    def on_disconnect(self, expected):
        print("Disconnected, reconnecting. Was it expected? " + str(expected))
        pool.connect(self, "irc.twitch.tv", 6667, tls=False, password=self.pw, reconnect=True)

    def on_connect(self):
        print("Connected! joining channels...")
        super().on_connect()
        for channel in self.mychannels:
            channel = channel.lower()
            print("Joining " + channel + "...")
            self.join(channel)

    def on_raw(self, message):
        # print("Raw message: " + str(message))
        super().on_raw(message)

    def on_private_message(self, nick, message, tags):
        super().on_private_message(nick, message)
        return

    def on_channel_message(self, target, nick, message, tags):
        super().on_channel_message(target, nick, message)
        return

    def on_message(self, source, target, message, tags, isWhisper=False):
        if isWhisper:
            print("whisper: " + str(target) + ", " + message)
        else:
            print("message: " + str(source) + ", " + str(target) + ", " + message)
        # print("Tags: " + str(tags))
        
        sender = str(target).lower()
        channelowner = str(source).lower().replace("#", "")
        
        # verify tags
        # do nothing if twitch id is somehow missing
        if 'user-id' not in tags:
            return
        
        # failsafe since display-name can (very rarely) be null for certain Twitch accounts
        if 'display-name' not in tags or not tags['display-name']:
            tags['display-name'] = sender
        
        global activitymap
        global blacklist
        if sender not in blacklist and "bot" not in sender:
            activitymap[sender] = 0
            activitymap[channelowner] = 0
            with busyLock:
                cur = db.cursor()
                cur.execute("SELECT name FROM users WHERE id = %s", [tags['user-id']])
                user = cur.fetchone()
                if user is None:
                    cur.execute("INSERT INTO users (id, name, points) VALUE (%s, %s, %s)", [tags['user-id'], sender, 0])
                    print("{name} didn't have an account, created it.".format(name=tags['display-name']))
                elif user[0] != sender:
                    print("{oldname} got a new name, changing: {newname}".format(oldname=user[0], newname=sender))
                    cur.execute("UPDATE users SET name = %s WHERE id = %s", [sender, tags['user-id']])
                cur.close()
                
            if message.startswith("!"):
                parts = message.split()
                self.do_command(parts[0][1:].lower(), parts[1:], target, source, tags, isWhisper=isWhisper)
        elif message.startswith("!"):
            self.message(source, "Bad Bot. No.")
            return
            
        # if str(message).find("HDNVert")>=0:
        #     cur = db.cursor()
        #     cur.execute("UPDATE consoleWar SET count=count+1 WHERE Name='Vert'")
        #     cur.close()
        # if str(message).find("HDNNoire")>=0:
        #     cur = db.cursor()
        #     cur.execute("UPDATE consoleWar SET count=count+1 WHERE Name='Noire'")
        #     cur.close()
        # if str(message).find("HDNNeptune")>=0:
        #     cur = db.cursor()
        #     cur.execute("UPDATE consoleWar SET count=count+1 WHERE Name='Neptune'")
        #     cur.close()
        # if str(message).find("HDNBlanc")>=0:
        #     cur = db.cursor()
        #     cur.execute("UPDATE consoleWar SET count=count+1 WHERE Name='Blanc'")
        #     cur.close()

    def message(self, channel, message, isWhisper=False):
        print("sending message %s %s %s" % (channel, message, "Y" if isWhisper else "N"))
        if isWhisper:
            super().message("#jtv", "/w " + str(channel).replace("#", "") + " " +str(message))
        else:
            super().message(channel, message)


    def do_command(self, command, args, sender, channel, tags, isWhisper=False):
        print("Got command: " + command + " with arguments " + str(args))
        with busyLock:
            if command == "quit" and sender in self.myadmins:
                print("Quitting from admin command.")
                pool.disconnect(client=self, expected=True)
                # sys.exit(0)
            if command == "checkhand":
                #print("Checking hand for " + sender)
                cards = getHand(tags['user-id'])
                if len(cards) == 0:
                    self.message(channel, "%s, you don't currently have any waifus! Get your first one with !freewaifu" % tags['display-name'], isWhisper=isWhisper)
                    return
                
                # message or link?
                dropLink = "http://waifus.de/hand?user=%s" % sender
                if (len(args) == 0 or args[0].lower() != "public") and followsme(tags['user-id']):
                    messages = ["%s, you have the following waifus: " % tags['display-name']]
                    for row in cards:
                        row['amount'] = "(x%d)" % row['amount'] if row['amount'] > 1 else ""
                        row['rarity'] = config["rarity%sName" % row['rarity']]
                        waifumsg = '[{id}][{rarity}] {name} from {series} - {image}{amount}; '.format(**row)
                        if len(messages[-1]) + len(waifumsg) > 400:
                            messages.append(waifumsg)
                        else:
                            messages[-1] += waifumsg
                    
                    self.message("#jtv", "/w %s %s" % (sender, dropLink))
                    for message in messages:
                        self.message("#jtv", "/w %s %s" % (sender, message))
                else:
                    limit = handLimit(tags['user-id'])
                    self.message(channel, "{user}, you can have {limit} waifus (currently held: {curr}) and your current hand is: {link}".format(user=tags['display-name'], limit=limit, link=dropLink, curr=currentCards(tags['user-id'])), isWhisper=isWhisper)
                return
            if command == "points":
                #print("Checking points for " + sender)
                cur = db.cursor()
                cur.execute("SELECT points FROM users WHERE id = %s", [tags['user-id']])
                self.message(channel, str(tags['display-name']) + ", you have " + str(cur.fetchone()[0]) + " points!", isWhisper=isWhisper)
                cur.close()
                return
            if command == "freewaifu":
                #print("Checking free waifu egliability for " + str(sender))
                cur = db.cursor()
                cur.execute("SELECT lastFree, handLimit FROM users WHERE id = %s", [tags['user-id']])
                res = cur.fetchone()
                nextFree = 79200000 + int(res[0])
                limit = int(res[1])
                freeAvailable = nextFree < current_milli_time()
                if freeAvailable and currentCards(tags['user-id']) < limit:
                    #print("egliable, dropping card.")
                    cur.execute("SELECT id, Name, image, rarity, series FROM waifus WHERE id='{0}'".format(dropCard()))
                    row = cur.fetchone()
                    if int(row[3]) >= int(config["drawAlertMinimumRarity"]):
                        threading.Thread(target=sendDrawAlert, args=(channel, {"name":row[1], "rarity":row[3], "image":row[2], "id": row[0]}, str(tags["display-name"]))).start()
                    self.message(channel, tags['display-name'] + ', you dropped a new Waifu: [{id}][{rarity}] {name} from {series} - {link}'.format(
                        id=str(row[0]), rarity=config["rarity" + str(row[3]) + "Name"], name=row[1], series=row[4],
                        link=row[2]), isWhisper=isWhisper)
                    giveCard(tags['user-id'], row[0])
                    id = str(row[0])
                    cur.execute("UPDATE users SET lastFree = %s WHERE id = %s", [current_milli_time(), tags['user-id']])
                    logDrop(str(tags['user-id']), id, row[3], "freewaifu", channel, isWhisper)
                elif freeAvailable:
                    #print("too many cards")
                    self.message(channel, str(tags['display-name']) + ", your hand is full! !disenchant something or upgrade your hand!", isWhisper=isWhisper)
                else:
                    a = datetime.timedelta(milliseconds=nextFree - current_milli_time(), microseconds=0)
                    datestring = "{0}".format(a).split(".")[0]
                    self.message(channel,
                                 str(tags['display-name']) + ", you need to wait {0} for your next free drop!".format(datestring), isWhisper=isWhisper)
                cur.close()
                return
            if command == "disenchant":
                if len(args) == 0 or (len(args) == 1 and len(args[0]) == 0):
                    self.message(channel, "Usage: !disenchant <list of IDs>", isWhisper=isWhisper)
                    return
                ids = []
                try:
                    for arg in args:
                        ids.append(int(arg))
                except:
                    self.message(channel, "Could not decipher one or more of the waifu IDs you provided.", isWhisper=isWhisper)
                    return
                    
                try:
                    inStrings = ",".join(["%s"] * len(ids))
                    cur = db.cursor()
                    cur.execute("SELECT has_waifu.waifuid, has_waifu.amount, waifus.rarity, waifus.name, waifus.image FROM has_waifu INNER JOIN waifus ON has_waifu.waifuid = waifus.id WHERE has_waifu.userid = %s AND waifuid IN({0})".format(inStrings), [tags['user-id']] + ids)
                    hasInfo = cur.fetchall()
                    
                    # work out if any waifu is actually missing from their hand.
                    missing = ids[:]
                    for row in hasInfo:
                        missing.remove(int(row[0]))
                    
                    if len(missing) > 0:
                        if len(missing) == 1:
                            self.message(channel, "You don't own waifu %d." % missing[0], isWhisper=isWhisper)
                        else:
                            self.message(channel, "You don't own the following waifus: %s" % ", ".join([str(id) for id in missing]), isWhisper=isWhisper)
                        return
                    
                    # handle disenchants appropriately
                    pointsGain = 0
                    for row in hasInfo:
                        pointsGain += int(config["rarity" + str(row[2]) + "Value"])
                        if row[2] >= int(config["disenchantAlertMinimumRarity"]):
                            # valuable waifu disenchanted
                            threading.Thread(target=sendDisenchantAlert, args=(channel, {"name":row[3], "rarity":row[2], "image":row[4]}, str(tags["display-name"]))).start()
                        if row[1] == 1:
                            # delet this
                            cur.execute("DELETE FROM has_waifu WHERE waifuid = %s AND userid = %s", (row[0], tags['user-id']))
                        else:
                            cur.execute("UPDATE has_waifu SET amount = amount - 1 WHERE waifuid = %s AND userid = %s", (row[0], tags['user-id']))
                            
                    addPoints(tags['user-id'], pointsGain)
                    
                    if len(ids) == 1:
                        self.message(channel, "Successfully disenchanted waifu %d and added %d points to %s's account" % (ids[0], pointsGain, str(tags['display-name'])), isWhisper=isWhisper)
                    else:
                        self.message(channel, "Successfully disenchanted %d waifus and added %d points to %s's account" % (len(ids), pointsGain, str(tags['display-name'])), isWhisper=isWhisper)
                    
                    cur.close()
                    return
                except:
                    self.message(channel, "Usage: !disenchant <list of IDs>", isWhisper=isWhisper)
                    return
            if command == "giveme":
                self.message(channel, "No.", isWhisper=isWhisper)
                return
            if command == "buy":
                if len(args) != 1:
                    self.message(channel, "Usage: !buy <rarity> (So !buy 1 for an uncommon)", isWhisper=isWhisper)
                    return
                if currentCards(tags['user-id']) >= handLimit(tags['user-id']):
                    self.message(channel, "{sender}, you have too many cards to buy one! !disenchant some or upgrade your hand!".format(sender=str(tags['display-name'])), isWhisper=isWhisper)
                    return
                try:
                    rarity = int(args[0])
                except:
                    if args[0] in revrarity.keys():
                        rarity = revrarity[args[0]]
                    else:
                        self.message(channel, "Unknown rarity. Usage: !buy <rarity> (So !buy 1 for an uncommon)", isWhisper=isWhisper)
                        return
                if rarity < 0 or rarity > 6:
                    self.message(channel, "Usage: !buy <rarity> (So !buy 1 for an uncommon)", isWhisper=isWhisper)
                    return
                price = int(config["rarity" + str(rarity) + "Value"]) * 5
                if not hasPoints(tags['user-id'], price):
                    self.message(channel, "You do not have enough points to buy a " + str(config["rarity" + str(rarity) + "Name"]) + " waifu. You need " + str(price) + " points.", isWhisper=isWhisper)
                    return
                addPoints(tags['user-id'], 0 - price)
                cur = db.cursor()
                cur.execute("SELECT id, Name, image, rarity, series FROM waifus WHERE id='{0}'".format(dropCard(rarity)))
                row = cur.fetchone()
                self.message(channel, str(
                    tags['display-name']) + ', you bought a new Waifu for {price}: [{id}][{rarity}] {name} from {series} - {link}'.format(
                    id=str(row[0]), rarity=config["rarity" + str(row[3]) + "Name"], name=row[1], series=row[4],
                    link=row[2], price=str(price)), isWhisper=isWhisper)
                giveCard(tags['user-id'], str(row[0]))
                cur.close()
                logDrop(str(tags['user-id']), str(row[0]), rarity, "buy", channel, isWhisper)
                if row[3] >= int(config["drawAlertMinimumRarity"]):
                    threading.Thread(target=sendDrawAlert, args=(channel, {"name":row[1], "rarity":row[3], "image":row[2], "id": row[0]}, str(tags["display-name"]))).start()
                return
            if command == "booster":
                if len(args) < 1:
                    self.message(channel, "Usage: !booster buy <%s> OR !booster select <take/disenchant> (for each waifu) OR !booster show" % visiblepacks, isWhisper=isWhisper)
                    return
                    
                cmd = args[0].lower()
                # even more shorthand shortcut for disenchant all
                if cmd == "trash":
                    cmd = "select"
                    args = ["select", "deall"]
                
                cur = db.cursor()
                cur.execute("SELECT id FROM boosters_opened WHERE userid = %s AND status = 'open'", [tags['user-id']])
                boosterinfo = cur.fetchone()
                
                if (cmd == "show" or cmd == "select") and boosterinfo is None:
                    self.message(channel, tags['display-name'] + ", you do not have an open booster. Buy one using !booster buy <%s>" % visiblepacks, isWhisper=isWhisper)
                    cur.close()
                    return
                
                if cmd == "show":
                    cur.execute("SELECT waifuid FROM boosters_cards WHERE boosterid = %s", [boosterinfo[0]])
                    cardrows = cur.fetchall()
                    cards = [row[0] for row in cardrows]
                    token = ''.join(choice(ascii_letters) for v in range(10))
                    addDisplayToken(token, cards)
                    droplink = "http://waifus.de/booster?token=" + token
                    self.message(channel, "{user}, your current open booster pack: {droplink}".format(user=tags['display-name'], droplink=droplink), isWhisper=isWhisper)
                    cur.close()
                    return
                
                if cmd == "select":
                    cur.execute("SELECT waifuid FROM boosters_cards WHERE boosterid = %s", [boosterinfo[0]])
                    cardrows = cur.fetchall()
                    cards = [row[0] for row in cardrows]
                    # check for shorthand syntax
                    if len(args) == 2:
                        if args[1].lower() == 'deall' or args[1].lower() == 'disenchantall':
                            selectArgs = ["disenchant"] * len(cards)
                        else:
                            selectArgs = []
                            for letter in args[1].lower():
                                if letter != 'd' and letter != 'k':
                                    self.message(channel, "When using shorthand booster syntax, please only use the letters d and k.", isWhisper=isWhisper)
                                    cur.close()
                                    return
                                elif letter == 'd':
                                    selectArgs.append("disenchant")
                                else:
                                    selectArgs.append("keep")
                    else:
                        selectArgs = args[1:]
                    
                    if len(selectArgs) != len(cards):
                        self.message(channel, "You did not specify the correct amount of keep/disenchant. Please provide " + str(len(openbooster[sender])), isWhisper=isWhisper)
                        cur.close()
                        return
                    keeping = 0
                    for arg in selectArgs:
                        if not (arg.lower() == "keep" or arg.lower() == "disenchant"):
                            self.message(channel, "Sorry, but " + arg.lower() + " is not a valid option. Use keep or disenchant", isWhisper=isWhisper)
                            cur.close()
                            return
                        if arg.lower() == "keep":
                            keeping += 1
                    currCards = currentCards(tags['user-id'])
                    if keeping + currCards > handLimit(tags['user-id']) and keeping != 0:
                        self.message(channel, "You can't keep that many waifus! !disenchant some!", isWhisper=isWhisper)
                        cur.close()
                        return
                    gottenpoints = 0
                    response = "You take your booster pack and: "
                    c = 2
                    cur = db.cursor()
                    keeps = []
                    des = []
                    for arg in selectArgs:
                        if str(arg).lower() == "keep":
                            giveCard(tags['user-id'], cards[c-2])
                            keeps.append(cards[c-2])
                        else:
                            # Disenchant
                            id = cards[c-2]
                            cur.execute("SELECT rarity, name, image FROM waifus WHERE id = %s", [id])
                            waifu = cur.fetchone()
                            if waifu[0] >= int(config["disenchantAlertMinimumRarity"]):
                                # valuable waifu being disenchanted
                                threading.Thread(target=sendDisenchantAlert, args=(channel, {"name":waifu[1], "rarity":waifu[0], "image":waifu[2]}, str(tags["display-name"]))).start()
                            value = int(config["rarity" + str(waifu[0]) + "Value"])
                            des.append(id)
                            gottenpoints += value
                        c += 1
                    addPoints(tags['user-id'], gottenpoints)
                    if len(keeps) > 0:
                        response += " keep " + ', '.join(str(x) for x in keeps) + ";"
                    if len(des) > 0:
                        response += " disenchant " + ', '.join(str(x) for x in des) + ";"
                    self.message(channel, response + " netting a total of " + str(gottenpoints) + " points.", isWhisper=isWhisper)
                    cur.execute("UPDATE boosters_opened SET status = 'closed', updated = %s WHERE id = %s", [current_milli_time(), boosterinfo[0]])
                    cur.close()
                    return
                if cmd == "buy":
                    if boosterinfo is not None:
                        self.message(channel, "You already have an open booster. Select the waifus you want to keep or disenchant first!", isWhisper=isWhisper)
                        cur.close()
                        return
                    if len(args) < 2:
                        self.message(channel, "Usage: !booster buy <%s>" % visiblepacks, isWhisper=isWhisper)
                        cur.close()
                        return
                    
                    packname = args[1].lower()
                    cur.execute("SELECT cost, numCards, guaranteedSCrarity, rarity0UpgradeChance, rarity1UpgradeChance, rarity2UpgradeChance, rarity3UpgradeChance, rarity4UpgradeChance, rarity5UpgradeChance FROM boosters WHERE name = %s AND buyable = 1", [packname])
                    packinfo = cur.fetchone()
                    
                    if packinfo is None:
                        self.message(channel, "Invalid booster type. Packs available right now: %s." % visiblepacks, isWhisper=isWhisper)
                        cur.close()
                        return
                        
                    if not hasPoints(tags['user-id'], packinfo[0]):
                        self.message(channel, "{user}, sorry, you don't have enough points to buy a {name} booster pack. You need {points}.".format(user=tags['display-name'], name=packname, points=str(packinfo[0])), isWhisper=isWhisper)
                        cur.close()
                        return
                        
                    addPoints(tags['user-id'], -packinfo[0])
                    
                    normalChances = packinfo[3:]
                    if packinfo[2] == 0:
                        firstPullChances = normalChances
                    elif packinfo[2] == 6:
                        firstPullChances = [1, 1, 1, 1, 1, 1]
                    else:
                        minFR = packinfo[2]
                        firstPullChances = ([1] * minFR) + [functools.reduce((lambda x, y: x*y), normalChances[:minFR+1])] + list(normalChances[minFR+1:])
                    
                    cards = []
                    for i in range(packinfo[1]):
                        while True:
                            ca = int(dropCard(upgradeChances=(firstPullChances if len(cards) == 0 else normalChances)))
                            if ca not in cards:
                                cards.append(ca)
                                break
                                
                    cards = sorted(cards)
                    alertwaifus = []
                    
                    for card in cards:
                        cur.execute("SELECT name, rarity, image FROM waifus WHERE id = %s", [card])
                        row = cur.fetchone()
                        
                        if row[1] >= int(config["drawAlertMinimumRarity"]):
                            alertwaifus.append( {"name":str(row[0]), "rarity":int(row[1]), "image":str(row[2]), "id": card})
                            
                        logDrop(str(tags['user-id']), str(card), row[1], "boosters.%s" % packname, channel, isWhisper)
                        
                        if card == 120:
                            self.message(channel, "I hear thou cry, so here i am...", isWhisper=isWhisper)
                            
                    # insert opened booster
                    cur.execute("INSERT INTO boosters_opened (userid, boostername, paid, created, status) VALUES(%s, %s, %s, %s, 'open')", [tags['user-id'], packname, packinfo[0], current_milli_time()])
                    boosterid = cur.lastrowid
                    cur.executemany("INSERT INTO boosters_cards (boosterid, waifuid) VALUES(%s, %s)", [(boosterid, card) for card in cards])
                    cur.close()

                    token = ''.join(choice(ascii_letters) for v in range(10))
                    addDisplayToken(token, cards)
                    droplink = "http://waifus.de/booster?token=" + token
                    self.message(channel, "{user}, you open a {type} booster pack and you get: {droplink}".format(user=tags['display-name'], type=packname, droplink=droplink), isWhisper=isWhisper)
                    for w in alertwaifus:
                        threading.Thread(target=sendDrawAlert, args=(channel, w, str(tags["display-name"]))).start()
                    return
            if command == "trade":
                ourid = int(tags['user-id'])
                if len(args) < 2:
                    self.message(channel, "Usage: !trade <check/accept/decline> <user> OR !trade <user> <have> <want> [points]", isWhisper=isWhisper)
                    return
                subarg = args[0].lower()
                if subarg in ["check", "accept", "decline"]:
                    otherparty = str(args[1]).lower()
                    cur = db.cursor()
                    cur.execute("SELECT id FROM users WHERE name = %s", [otherparty])
                    otheridrow = cur.fetchone()
                    cur.close()
                    if otheridrow is None:
                        self.message(channel, "I don't recognize that username.", isWhisper=isWhisper)
                        return
                    otherid = int(otheridrow[0])
                    
                    if otherid not in trades or ourid not in trades[otherid]:
                        self.message(channel, otherparty + " did not send you a trade. Send one with !trade " + otherparty + " <have> <want> [points]", isWhisper=isWhisper)
                        return
                        
                    trade = trades[otherid][ourid]
                    want = trade["want"]
                    have = trade["have"]
                    tradepoints = int(trade["points"])
                    
                    if subarg == "check":
                        wantwaifu = getWaifuById(want)
                        havewaifu = getWaifuById(have)
                        wantname = wantwaifu["name"]
                        havename = havewaifu["name"]
                        payer = "they will pay you" if otherid == trade["payup"] else "you will pay them"
                        if trade["points"] > 0:
                            self.message(channel, "{other} wants to trade their ({have}) {havename} for your ({want}) {wantname} and {payer} {points} points. Accept it with !trade accept {other}".format(other=otherparty, have=str(have), havename=havename, want=str(want), wantname=wantname, payer=payer, points=tradepoints), isWhisper=isWhisper)
                        else:
                            self.message(channel, "{other} wants to trade their ({have}) {havename} for your ({want}) {wantname}. Accept it with !trade accept {other}".format(other=str(args[1]).lower(), have=str(have), havename=havename, want=str(want), wantname=wantname, payer=payer), isWhisper=isWhisper)
                        return
                    elif subarg == "decline":
                        trades[otherid].pop(ourid)
                        self.message(channel, "Trade declined.", isWhisper=isWhisper)
                        return
                    else:
                        # accept
                        cost = int(config["tradingFee"])
                        
                        payup = trade["payup"]
                        nonpayer = ourid if trade["payup"] == otherid else otherid
                        
                        if not hasPoints(payup, cost + tradepoints):
                            self.message(channel, "Sorry, but %s cannot cover the fair trading fee." % ("you" if payup == ourid else otherparty), isWhisper=isWhisper)
                            return
                            
                        if not hasPoints(nonpayer, cost - tradepoints):
                            self.message(channel, "Sorry, but %s cannot cover the base trading fee." % ("you" if nonpayer == ourid else otherparty), isWhisper=isWhisper)
                            return
                        
                        cur = db.cursor()
                        cur.execute("SELECT SUM(amount) FROM has_waifu WHERE waifuid = %s AND userid = %s", [want, ourid])
                        wantamount = cur.fetchone()[0] or 0
                        cur.execute("SELECT SUM(amount) FROM has_waifu WHERE waifuid = %s AND userid = %s", [have, otherid])
                        haveamount = cur.fetchone()[0] or 0


                        if wantamount == 0:
                            self.message(channel, "{sender}, you don't have waifu {waifu}, so you can not accept this trade. Deleting it.".format(sender=tags['display-name'], waifu=str(want)), isWhisper=isWhisper)
                            trades[otherid].pop(ourid)
                            cur.close()
                            return
                        
                        if haveamount == 0:
                            self.message(channel, "{sender}, {other} doesn't have waifu {waifu}, so you can not accept this trade. Deleting it.".format(sender=tags['display-name'], waifu=str(have), other=otherparty), isWhisper=isWhisper)
                            trades[otherid].pop(ourid)
                            cur.close()
                            return
                            
                        # give cards
                        giveCard(ourid, have)
                        giveCard(otherid, want)
                        
                        # take cards
                        if wantamount == 1:
                            cur.execute("DELETE FROM has_waifu WHERE waifuid = %s AND userid = %s", [want, ourid])
                        else:
                            cur.execute("UPDATE has_waifu SET amount = amount - 1 WHERE waifuid = %s AND userid = %s", [want, ourid])
                        if haveamount == 1:
                            cur.execute("DELETE FROM has_waifu WHERE waifuid = %s AND userid = %s", [have, otherid])
                        else:
                            cur.execute("UPDATE has_waifu SET amount = amount - 1 WHERE waifuid = %s AND userid = %s", [have, otherid])
                            
                        # points
                        addPoints(payup, -(tradepoints + cost))
                        addPoints(nonpayer, tradepoints - cost)
                        
                        # done
                        trades[otherid].pop(ourid)
                        
                        self.message(channel, "Trade executed!", isWhisper=isWhisper)
                        cur.close()
                        return
                
                if len(args) != 3 and len(args) != 4:
                    self.message(channel, "Usage: !trade <accept/decline> <user> OR !trade <user> <have> <want> [points]", isWhisper=isWhisper)
                    return

                other = args[0]
                
                cur = db.cursor()
                cur.execute("SELECT id FROM users WHERE name = %s", [other])
                otheridrow = cur.fetchone()
                cur.close()
                if otheridrow is None:
                    self.message(channel, "I don't recognize that username.", isWhisper=isWhisper)
                    return
                otherid = int(otheridrow[0])
                    
                have = args[1]
                want = args[2]
                try:
                    int(args[1])
                    int(args[2])
                    if len(args) == 4:
                        int(args[3])
                except:
                    self.message(channel, "Only whole numbers/IDs please.", isWhisper=isWhisper)
                    return
                maxi = maxWaifuID()
                if int(have) <= 0 or int(want) <= 0 or int(have) > int(maxi) or int(want) > int(maxi):
                    self.message(channel, "Invalid ID. Must be a number from 1 to " + str(maxi), isWhisper=isWhisper)
                    return

                havewaifu = getWaifuById(have)
                wantwaifu = getWaifuById(want)
                

                points = 0
                payup = ourid
                if havewaifu["rarity"] != wantwaifu["rarity"]:
                    if int(havewaifu["rarity"]) == 7 or int(wantwaifu["rarity"]) == 7:
                        self.message(channel, "Sorry, special-rarity cards can only be traded for other special-rarity cards.", isWhisper=isWhisper)
                        return
                    if len(args) != 4:
                        self.message(channel, "To trade waifus of different rarities, please append a point value the owner of the lower tier card has to pay to the command to make the trade fair. (see !help)", isWhisper=isWhisper)
                        return
                    points = int(args[3])
                    highercost = int(config["rarity" + str(max(int(havewaifu["rarity"]), int(wantwaifu["rarity"]))) + "Value"])
                    lowercost = int(config["rarity" + str(min(int(havewaifu["rarity"]), int(wantwaifu["rarity"]))) + "Value"])
                    costdiff = highercost - lowercost
                    mini = int(costdiff/2)
                    maxi = int(costdiff)
                    if points < mini:
                        self.message(channel, "Minimum points to trade this difference in rarity is " + str(mini), isWhisper=isWhisper)
                        return
                    if points > maxi:
                        self.message(channel, "Maximum points to trade this difference in rarity is " + str(maxi), isWhisper=isWhisper)
                        return
                    if int(wantwaifu["rarity"]) < int(havewaifu["rarity"]):
                        payup = otherid
                        
                if ourid not in trades:
                    trades[ourid] = {}
                trades[ourid][otherid] = {"have":have, "want":want, "points":points, "payup":payup}
                
                paying = ""
                if points > 0:
                    if payup == ourid:
                        paying = " with you paying them " + str(points) + " points"
                    else:
                        paying = " with them paying you " + str(points) + " points"
                self.message(channel, "Offered {other} to trade your {have} for their {want}{paying}".format(other=str(other), have=str(have), want=str(want), paying = paying), isWhisper=isWhisper)
                print(repr(trades))
                return
            if command == "lookup":
                if len(args) != 1:
                    self.message(channel, "Usage: !lookup <id>", isWhisper=isWhisper)
                    return
                cur = db.cursor()
                cur.execute("SELECT lastLookup FROM users WHERE id = %s", [tags['user-id']])
                nextFree = 1800000 + int(cur.fetchone()[0])
                lookupAvailable = nextFree < current_milli_time()
                
                if lookupAvailable:
                    try:
                        waifu = getWaifuById(args[0])
                        assert waifu is not None
                        owned = whoHas(args[0])
                        if len(owned) > 4:
                            owned = owned[0:3] + ["%d others" % (len(owned) - 3)]
                        
                        waifu["rarity"] = config["rarity%dName" % waifu["rarity"]]
                        waifu["owned"] = (" - owned by " + ", ".join(owned)) if len(owned) > 0 else " (not dropped so far)"
                        
                        self.message(channel, '[{id}][{rarity}] {name} from {series} - {image}{owned}'.format(**waifu), isWhisper=isWhisper)
                        
                        if sender not in self.myadmins:
                            cur.execute("UPDATE users SET lastLookup = %s WHERE id = %s", [current_milli_time(), tags['user-id']])
                    except:
                        self.message(channel, "Invalid waifu ID.", isWhisper=isWhisper)
                else:
                    a = datetime.timedelta(milliseconds=nextFree - current_milli_time(), microseconds=0)
                    datestring = "{0}".format(a).split(".")[0]
                    self.message(channel, "Sorry, {user}, please wait {t} until you lookup again.".format(user=str(sender), t=datestring), isWhisper=isWhisper)

                cur.close()
                return
            if command == "whisper":
                if followsme(tags['user-id']):
                    self.message("#jtv", "/w {user} This is a test whisper.".format(user=sender), isWhisper=False)
                    self.message(channel, "Attempted to send test whisper.", isWhisper=isWhisper)
                else:
                    self.message(channel, "{user}, you need to be following me so i can send you whispers!".format(user=str(tags['display-name'])), isWhisper=isWhisper)
                return
            if command == "help":
                self.message(channel, "http://waifus.de/help", isWhisper=isWhisper)
            if command == "alerts" or command=="alert":
                if len(args) < 1:
                    self.message(channel, "Usage: !alerts setup OR !alerts test <rarity> OR !alerts config <config Name> <config Value>", isWhisper=isWhisper)
                    return
                sender = sender.lower()
                subcmd = str(args[0]).lower()
                if subcmd == "setup":
                    cur = db.cursor()
                    cur.execute("SELECT alertkey FROM channels WHERE name=%s", [sender])
                    row = cur.fetchone();
                    if row[0] is None:
                        self.message("#jtv",
                                     "/w {user} Please go to the following link and allow access: {link}{user}".format(
                                         user=sender, link=str(streamlabsauthurl)), isWhisper=False)
                        self.message(channel, "Sent you a whisper with a link to set up alerts. If you didnt receive a whisper, try !whisper", isWhisper=isWhisper)
                    else:
                        self.message(channel, "Alerts seem to already be set up for your channel! Use !alerts test to test them!", isWhisper)
                    cur.close()
                    return
                if subcmd == "test":
                    try:
                        rarity = int(args[1])
                    except:
                        rarity = 6
                    cur = db.cursor()
                    cur.execute("SELECT alertkey FROM channels WHERE name=%s", [sender])
                    row = cur.fetchone();
                    cur.close()
                    if row[0] is None:
                        self.message(channel, "Alerts do not seem to be set up for your channel, please set them up using !alerts setup", isWhisper=isWhisper)
                    else:
                        threading.Thread(target=sendDrawAlert, args=(
                        sender, {"name": "Test Alert, please ignore", "rarity": rarity, "image": "http://t.fuelr.at/k6g"},
                        str(tags["display-name"]), False)).start()
                        self.message(channel, "Test Alert sent.", isWhisper=isWhisper)
                    return
                if subcmd == "config":
                    try:
                        configName = args[1]
                    except:
                        self.message(channel, "Valid alert config options: " + ", ".join(validalertconfigvalues), isWhisper=isWhisper)
                        return
                    if configName == "reset":
                        cur = db.cursor()
                        cur.execute("DELETE FROM alertConfig WHERE channelName = %s", [sender])
                        cur.close()
                        self.message(channel, "Removed all custom alert config for your channel. #NoireScremRules", isWhisper=isWhisper)
                        return
                    if configName not in validalertconfigvalues:
                        self.message(channel, "Valid alert config options: " + ", ".join(validalertconfigvalues),
                                     isWhisper=isWhisper)
                        return
                    try:
                        configValue = args[2]
                    except:
                        cur = db.cursor()
                        cur.execute("SELECT val FROM alertConfig WHERE channelName=%s AND config = %s", [sender, configName])
                        rows = cur.fetchall()
                        if len(rows) != 1:
                            self.message(channel, 'Alert config "' + configName + '" is unset for your channel.', isWhisper=isWhisper)
                        else:
                            configValue = rows[0][0]
                            self.message(channel, 'Alert config "' + configName + '" is set to "' + configValue + '" for your channel.', isWhisper=isWhisper)
                        cur.close()
                        return
                    cur = db.cursor()
                    cur.execute("SELECT val FROM alertConfig WHERE channelName=%s AND config = %s",
                                [sender, configName])
                    rows = cur.fetchall()
                    if configValue == "reset":
                        cur.execute("DELETE FROM alertConfig WHERE channelName=%s AND config=%s", [sender, configName])
                        cur.close()
                        self.message(channel, 'Reset custom alert config "' + configName + '" for your channel.', isWhisper=isWhisper)
                        return
                    if configName == "alertChannel" and configValue not in ["host", "donation", "follow", "reset", "subscription"]:
                        self.message(channel, 'Valid options for alertChannel: "host", "donation", "follow", "subscription", "reset"')
                        cur.close()
                        return
                    if len(rows) == 1:
                        cur.execute("UPDATE alertConfig SET val=%s WHERE channelName=%s AND config = %s", [configValue, sender, configName])
                    else:
                        cur.execute("INSERT INTO alertConfig(val, channelName, config) VALUE (%s, %s, %s)", [configValue, sender, configName])
                    cur.close()
                    self.message(channel, 'Set alert config value "' + configName + '" to "' + configValue + '"', isWhisper=isWhisper)
                    return
                self.message(channel,
                    "Usage: !alerts setup OR !alerts test <rarity> OR !alerts config <config Name> <config Value>",
                    isWhisper=isWhisper)
                return
            if command == "followtest" and sender.lower() in self.myadmins:
                self.message(channel, "Attempting to set follow buttons to hdnmarathon and nepnepbot", isWhisper=isWhisper)
                setFollows(["hdnmarathon", "nepnepbot"])
                return
            # if command == "follow" and sender.lower() in self.myadmins:
            #     self.message(channel, "Setting follow Button for hdnmarathon to " + (", ".join(args)))
            #     setFollows(args)
            #     return
            # if command == "title" and sender.lower() in self.myadmins:
            #     self.message(channel, "Setting title for hdnmarathon to " + " ".join(args))
            #     updateTitle(" ".join(args))
            #     return
            # if command == "game" and sender.lower() in self.myadmins:
            #     self.message(channel, "Setting game for hdnmarathon to " + " ".join(args))
            #     updateGame(" ".join(args))
            #     return
            if command == "togglehoraro" and sender.lower() in self.myadmins:
                self.autoupdate = not self.autoupdate
                if self.autoupdate:
                    self.message(channel, "Enabled Horaro Auto-update.", isWhisper=isWhisper)
                else:
                    self.message(channel, "Disabled Horaro Auto-update.", isWhisper=isWhisper)
                return
            if command == "war":
                cur = db.cursor()
                cur.execute("SELECT * FROM consoleWar")
                r = cur.fetchall()
                # msg = "Console War: "
                msg = "THE CONSOLE WAR HAS BEEN DECIDED: "
                for row in r:
                    msg += "HDN" + str(row[0]) + " " + str(row[1]) + " "
                msg += "IrisGrin 9001 Comfa 9001 Salutezume 9001"
                self.message(channel, msg, isWhisper=isWhisper)
                cur.close()
                return
            if command == "nepjoin" and sender.lower() in self.myadmins:
                if len(args) != 1:
                    self.message(channel, "Usage: !nepjoin <channelname>", isWhisper=isWhisper)
                    return
                chan = str(args[0]).replace("'", "").lower()
                if ('#' + chan) in self.mychannels or ('#' + chan) in self.addchannels:
                    self.message(channel, "Already in that channel!", isWhisper=isWhisper)
                    return
                try:
                    cur = db.cursor()
                    cur.execute("SELECT COUNT(*) FROM users WHERE name=%s", [str(chan)])
                    if (cur.fetchone()[0] or 0) < 1:
                        self.message(channel, "That user is not yet in the database! Let them talk in a channel the Bot is in to change that!", isWhisper=isWhisper)
                        cur.close()
                        return
                    cur.execute("INSERT INTO channels(name) VALUES (%s)", [str(chan)])
                    self.join("#" + chan)
                    self.message("#" + chan, "Hi there!", isWhisper=False)
                    self.addchannels.append('#' + chan)
                    self.message(channel, "Joined #" + chan, isWhisper=isWhisper)
                    cur.close()
                    return
                except:
                    self.message(channel, "Tried joining, failed. Tell Marenthyu the following: " + str(sys.exc_info()), isWhisper=isWhisper)
                    print("Error: " + str(sys.exc_info()))
                    return
            if command == "nepleave" and (sender.lower() in self.myadmins or ("#" + str(sender.lower())) == str(channel)):
                if len(args) > 0:
                    self.message(channel, "nepleave doesn't take in argument. Type it in the channel to leave.", isWhisper=isWhisper)
                    return
                try:
                    cur = db.cursor()
                    cur.execute("DELETE FROM channels WHERE name = %s", [channel[1:]])
                    self.leavechannels.append(str(channel))
                    # self.mychannels.remove(str(channel))
                    self.message(channel, "ByeBye!", isWhisper=False)
                    self.part(channel)
                    cur.close()
                    return
                except:
                    self.message(channel, "Tried to leave but failed D:", isWhisper=isWhisper)
                    print("Error: " + str(sys.exc_info()))
                    return
            if command == "reload" and (sender.lower() in self.myadmins):
                #print("in reload command")
                cur = db.cursor()
                cur.execute("SELECT * FROM config")
                global config
                config = {}
                print("Importing config from database")
                for row in cur.fetchall():
                    config[row[0]] = row[1]
                global revrarity
                revrarity = {}
                i = 0
                while i <= 6:
                    n = config["rarity" + str(i) + "Name"]
                    revrarity[n] = i
                    i += 1
                global blacklist
                cur.execute("SELECT name FROM blacklist")
                blacklist = []
                for row in cur.fetchall():
                    blacklist.append(row[0])
                global visiblepacks
                # visible packs
                cur.execute("SELECT name FROM boosters WHERE listed = 1 AND buyable = 1 ORDER BY sortIndex ASC")
                packrows = cur.fetchall()
                visiblepacks = "/".join(row[0] for row in packrows)
                cur.close()
                self.message(channel, "Config reloaded.", isWhisper=isWhisper)
                return
            if command == "redeem":
                if len(args) != 1:
                    self.message(channel, "Usage: !redeem <token>", isWhisper=isWhisper)
                    return
                cur = db.cursor()
                cur.execute("SELECT points FROM pointTokens WHERE token=%s", (str(args[0])))
                pointrow = cur.fetchone()
                cur.execute("SELECT waifuid FROM waifuTokens WHERE token=%s", (str(args[0])))
                waifurows = cur.fetchall()

                givenPoints = 0
                if pointrow is not None:
                    addPoints(tags['user-id'], int(pointrow[0]))
                    givenPoints = int(pointrow[0])
                waifusadded = []

                for row in waifurows:
                    giveCard(tags['user-id'], row[0])
                    waifusadded.append(str(row[0]))
                if len(waifusadded) == 0 and givenPoints == 0:
                    self.message(channel, "Unknown token.", isWhisper=isWhisper)
                    cur.close()
                    return
                cur.execute("DELETE FROM waifuTokens WHERE token=%s", (str(args[0])))
                cur.execute("DELETE FROM pointTokens WHERE token=%s", (str(args[0])))
                cur.close()
                self.message(channel, "Successfully redeemed token, added {points} points and these waifus to your account: {waifus}".format(points=str(givenPoints), waifus="None" if len(waifusadded) == 0 else ",".join(waifusadded)), isWhisper=isWhisper)
                return
            if command == "wars":
                if (len(args) != 0 and len(args) != 4) or (len(args)==4 and args[0] != "vote"):
                    self.message(channel, "Usage: !wars OR !wars vote <warID> <optionID> <points>", isWhisper=isWhisper)
                    return
                cur = db.cursor()
                try:
                    cur.execute("SELECT id, name, neutralAmount FROM bidWars")
                    wars = cur.fetchall()
                    warlist = []
                    for war in wars:
                        warObject = {"id": war[0], "name": war[1], "options": [], "neutral":war[2]}
                        cur.execute("SELECT optionID, optionName, optionAmount, isWinner FROM bidWarValues WHERE warID=%s",
                                    (war[0]))
                        opts = cur.fetchall()
                        optArray = []
                        for opt in opts:
                            optArray.append({"id": opt[0], "name": opt[1], "amount": opt[2], "winner": opt[3]})
                        warObject["options"] = optArray
                        warlist.append(warObject)
                    if len(args) == 0:
                        cur.close()
                        msg = "The following wars are going on: "
                        for war in warlist:
                            msg += "[{war[id]}]{war[name]}: [".format(war=war)
                            for opt in war["options"]:
                                msg += "[{opt[id]}] {opt[name]}: {opt[amount]}{lead}, ".format(opt=opt, lead=" (In the lead!)" if opt["winner"] == 1 else "")
                            msg += "]; "
                        self.message(channel, msg, isWhisper=isWhisper)
                        return
                    if int(args[3]) < 1:
                        self.message(channel, "No. I wont exploit it. Shrug off!", isWhisper=isWhisper)
                        cur.close()
                        return
                    selectedWar = None
                    for war in warlist:
                        if str(war["id"]) == args[1]:
                            selectedWar = war
                            break

                    if selectedWar is None:
                        self.message(channel, "That war does not exist. Try again using a valid ID (Check !wars)", isWhisper=isWhisper)
                        cur.close()
                        return
                    selectedOption = None
                    currentWinner = None
                    for opt in selectedWar["options"]:
                        if str(opt["id"]) == args[2]:
                            selectedOption = opt
                        if opt["winner"] == 1:
                            currentWinner = opt

                    if selectedOption is None:
                        self.message(channel, "That option does not exist. Try again using a valid ID (Check !wars)", isWhisper=isWhisper)
                        cur.close()
                        return
                    try:
                        if not hasPoints(tags['user-id'], int(args[3])):
                            self.message(channel, "Sorry, you do not have enough points to invest " + str(args[3]), isWhisper=isWhisper)
                            return
                    except:
                        cur.close()
                        self.message(channel, "Sorry, but {} is not a valid number!".format(str(args[3])), isWhisper=isWhisper)
                        return
                    addPoints(tags['user-id'], -1 * int(args[3]))
                    cur.execute("UPDATE bidWarValues SET optionAmount=optionAmount + %s WHERE optionID=%s AND warID=%s", (str(args[3]), str(args[2]), str(args[1])))
                    if int(selectedOption["amount"]) + int(args[3]) > int(currentWinner["amount"]) + int(selectedWar["neutral"]):
                        cur.execute("UPDATE bidWarValues SET isWinner='0' WHERE optionID=%s AND warID=%s",
                                    (str(currentWinner["id"]), str(args[1])))
                        cur.execute("UPDATE bidWarValues SET isWinner='1' WHERE optionID=%s AND warID=%s",
                                    ( str(args[2]), str(args[1])))

                    cur.close()
                    self.message(channel, "Successfully voted to '{war[name]}' Option '{option[name]}' using {amount} points!".format(war=selectedWar, option=selectedOption, amount=str(args[3])), isWhisper=isWhisper)
                    return
                except:
                    self.message(channel, "Usage: !wars OR !wars vote <warID> <optionID> <points>", isWhisper=isWhisper)
                    cur.close()
                    return
            if command == "upgrade":
                user = tags['user-id']
                limit = handLimit(user)
                purchased = paidHandUpgrades(user)
                price = int(int(config["firstUpgradeCost"]) * math.pow(2, purchased))
                if len(args) != 1:
                    self.message(channel, "{user}, your current hand limit is {limit}. To add a slot for {price} points, use !upgrade buy".format(user=tags['display-name'], limit=str(limit), price=str(price)), isWhisper=isWhisper)
                    return
                if args[0] == "buy":
                    if hasPoints(user, price):
                        addPoints(user, price * -1)
                        upgradeHand(user, gifted = False)
                        self.message(channel, "Successfully upgraded {user}'s hand for {price} points!".format(user=tags['display-name'], price=str(price)), isWhisper=isWhisper)
                        return
                    else:
                        self.message(channel, "{user}, you do not have enough points to upgrade your hand - it costs {price}".format(user=tags['display-name'], price=str(price)), isWhisper=isWhisper)
                        return
                self.message(channel, "Usage: !upgrade - checks your limit and the price for an upgrade; !upgrade buy - buys an additional slot for your hand", isWhisper=isWhisper)
                return
            if command == "announce":
                if not (sender in self.myadmins):
                    self.message(channel, "Admin Only Command.", isWhisper=isWhisper)
                    return
                if len(args) < 1:
                    self.message(channel, "Usage: !announce <message>", isWhisper=isWhisper)
                    return
                msg = " ".join(args)
                for ch in self.mychannels:
                    self.message(ch, msg, isWhisper=False)
                self.message(channel, "Sent Announcement to all channels.", isWhisper=isWhisper)
                return
            if command == "search":
                if len(args) < 1:
                    self.message(channel, "Usage: !search <name>", isWhisper=isWhisper)
                    return
                cur = db.cursor()
                cur.execute("SELECT lastSearch FROM users WHERE id = %s", [tags['user-id']])
                nextFree = 1800000 + int(cur.fetchone()[0])
                lookupAvailable = nextFree < current_milli_time()
                if lookupAvailable:
                    q = " ".join(args)
                    result = search(q)
                    #print(result)
                    if len(result) == 0:
                        self.message(channel, "No waifu found with that name.", isWhisper=isWhisper)
                        return
                    if len(result) == 1:
                        self.message(channel,
                                     "Found one waifu: [{w[id]}][{rarity}]{w[name]} from {w[series]} (use !lookup {w[id]} for more info)".format(
                                         w=result[0], rarity=config['rarity' + str(result[0]['rarity']) + 'Name']), isWhisper=isWhisper)
                        return
                    if len(result) > 8:
                        self.message(channel, "Too many results! ({amount}) - try a longer search query.".format(
                            amount=str(len(result))), isWhisper=isWhisper)
                        return
                    else:
                        self.message(channel, "Multiple results (Use !lookup for more details): " + ", ".join(
                            map(lambda waifu: str(waifu['id']), result)), isWhisper=isWhisper)
                    if sender not in self.myadmins:
                        cur.execute("UPDATE users SET lastSearch = %s WHERE id = %s", [current_milli_time(), tags['user-id']])
                    return
                else:
                    a = datetime.timedelta(milliseconds=nextFree - current_milli_time(), microseconds=0)
                    datestring = "{0}".format(a).split(".")[0]
                    self.message(channel, "Sorry, {user}, please wait {t} until you can search again.".format(user=tags['display-name'], t=datestring), isWhisper=isWhisper)
            if command == "promote":
                if len(args) != 1:
                    self.message(channel, "Usage: !promote <id>", isWhisper)
                    return
                try:
                    waifuid = int(args[0])
                except:
                    self.message(channel, "Please provide a whole number, an id, not anything else!", isWhisper)
                    return
                maxID = maxWaifuID()
                if waifuid <= 0 or waifuid > maxID:
                    self.message(channel, "Please provide an id between 1 and " + str(maxID), isWhisper)
                    return

                w = getWaifuById(waifuid)
                needamount = int(config["rarity" + str(w["rarity"]) + "Max"])
                if (needamount <= 1):
                    self.message(channel, "Sorry, you cannot upgrade " + config["rarity" + str(w["rarity"]) + "Name"] + " waifus.", isWhisper)
                    return
                hand = getHand(tags["user-id"])
                #print("got hand: " + str(hand))
                hasall = False
                haveamount = 0
                for slot in hand:
                    if int(slot["id"]) == int(waifuid):
                        haveamount = int(slot["amount"])
                        if haveamount == needamount:
                            hasall = True
                        break
                if not hasall:
                    self.message(channel, "Sorry, {user}, you only have {have}/{need} copies of that waifu.".format(user=tags["display-name"], have=str(haveamount), need=str(needamount)), isWhisper)
                    return
                cur = db.cursor()
                cur.execute("UPDATE waifus SET rarity = '6' WHERE id = %s", [str(waifuid)])
                cur.execute("UPDATE has_waifu SET amount = '1' WHERE waifuid = %s", [str(waifuid)])
                cur.close()
                self.message(channel, "Successfully promoted " + w["name"] + " to god rarity! May Miku have mercy on their soul...", isWhisper)
                threading.Thread(target=sendPromotionAlert, args=(channel, w, str(tags["display-name"]))).start()
                return
            if command == "bet":
                if len(args) < 1:
                    self.message(channel, "Usage: !bet <time> OR !bet status OR (as channel owner) !bet open OR !bet start OR !bet end OR !bet cancel OR !bet results", isWhisper)
                    return
                canManageBets = str(tags["badges"]).find("broadcaster") > -1 or sender in self.myadmins
                match = time_regex.fullmatch(args[0])
                if match:
                    bet = match.groupdict()
                    ms = 0
                    if bet["ms"] is None:
                        bet["ms"] = "0"
                    while len(bet["ms"]) < 3:
                        bet["ms"] = bet["ms"] + "0"
                    ms = int(bet["ms"])
                    betms = int(bet["hours"]) * 3600000 + int(bet["minutes"]) * 60000 + int(bet["seconds"]) * 1000 + ms
                    if sender == channel[1:]:
                        self.message(channel, "You can't bet in your own channel, sorry!", isWhisper)
                        return
                    open = placeBet(channel, tags["user-id"], betms)
                    if open:
                        self.message(channel,
                                     "Successfully entered {name}'s bet: {h}h {min}min {s}s {ms}ms".format(h=bet["hours"],
                                                                                                 min=bet["minutes"],
                                                                                                 s=bet["seconds"],
                                                                                                 ms=str(betms%1000),
                                                                                                 name=tags['display-name']),
                                     isWhisper)
                    else:
                        self.message(channel, "The bets aren't open right now, sorry!", isWhisper)
                    return
                else:
                    subcmd = str(args[0]).lower()
                    if canManageBets and subcmd == "open":
                        if openBet(channel):
                            self.message(channel, "Bets are now open! Use !bet HH:MM:SS(.ms) to submit your bet!")
                        else:
                            self.message(channel, "There is already a prediction contest in progress in your channel! Use !bet status to check what to do next!")
                        return
                    elif canManageBets and subcmd == "start":
                        if startBet(channel):
                            self.message(channel, "Taking current time as start time! Good Luck! Bets are now closed.")
                        else:
                            self.message(channel, "There wasn't an open prediction contest in your channel! Use !bet status to check current contest status.")
                        return
                    elif canManageBets and subcmd == "end":
                        resultData = endBet(str(channel).lower())
                        if resultData is None:
                            self.message(channel, "There wasn't a prediction contest in progress in your channel! Use !bet status to check current contest status.")
                        else:
                            formattedTime = formatTimeDelta(resultData["result"])
                            winners = resultData["winners"]
                            winnerNames = []
                            for n in range(3):
                                winnerNames.append(winners[n]["name"] if len(winners) > n else "No-one")
                            self.message(channel, "Contest has ended in {time}! The top 3 closest were: {first}, {second}, {third}".format(time=formattedTime, first=winnerNames[0], second=winnerNames[1], third=winnerNames[2]))
                        return
                    elif canManageBets and subcmd == "cancel":
                        if cancelBet(channel):
                            self.message(channel, "Cancelled the current prediction contest! Start a new one with !bet open.")
                        else:
                            self.message(channel, "There was no open or in-progress prediction contest in your channel! Start a new one with !bet open.")
                        return
                    elif subcmd == "status":
                        # check for most recent betting
                        cur = db.cursor()
                        cur.execute("SELECT id, status, startTime, endTime FROM bets WHERE channel = %s ORDER BY id DESC LIMIT 1", [channel])
                        betRow = cur.fetchone()
                        if betRow is None:
                            if canManageBets:
                                self.message(channel, "No time prediction contests have been done in this channel yet. Use !bet open to open one.")
                            else:
                                self.message(channel, "No time prediction contests have been done in this channel yet.")
                        elif betRow[1] == 'cancelled':
                            if canManageBets:
                                self.message(channel, "No time prediction contest in progress. The most recent contest was cancelled. Use !bet open to open a new one.")
                            else:
                                self.message(channel, "No time prediction contest in progress. The most recent contest was cancelled.")
                        else:
                            cur.execute("SELECT COUNT(*) FROM placed_bets WHERE betid = %s", [betRow[0]])
                            numBets = cur.fetchone()[0] or 0
                            if betRow[1] == 'open':
                                if canManageBets:
                                    self.message(channel, "Bets are currently open for a new contest. %d bets have been placed so far. !bet start to close bets and start the run timer." % numBets)
                                else:
                                    self.message(channel, "Bets are currently open for a new contest. %d bets have been placed so far." % numBets)
                            elif betRow[1] == 'started':
                                elapsed = current_milli_time() - betRow[2]
                                formattedTime = formatTimeDelta(elapsed)
                                if canManageBets:
                                    self.message(channel, "Run in progress - elapsed time %s. %d bets were placed. !bet end to end the run timer and determine results." % (formattedTime, numBets))
                                else:
                                    self.message(channel, "Run in progress - elapsed time %s. %d bets were placed." % (formattedTime, numBets))
                            else:
                                formattedTime = formatTimeDelta(betRow[3] - betRow[2])
                                if canManageBets:
                                    self.message(channel, "No time prediction contest in progress. The most recent contest ended in %s with %d bets placed. Use !bet results to see full results or !bet open to open a new one." % (formattedTime, numBets))
                                else:
                                    self.message(channel, "No time prediction contest in progress. The most recent contest ended in %s with %d bets placed." % (formattedTime, numBets))
                        cur.close()
                        return
                    elif canManageBets and subcmd == "results":
                        cur = db.cursor()
                        cur.execute("SELECT id, status FROM bets WHERE channel = %s ORDER BY id DESC LIMIT 1", [channel])
                        betRow = cur.fetchone()
                        if betRow is None:
                            self.message(channel, "No time prediction contests have been done in this channel yet.", isWhisper)
                        elif betRow[1] == 'cancelled':
                            self.message(channel, "The most recent contest in this channel was cancelled.", isWhisper)
                        elif betRow[1] == 'open' or betRow[1] == 'started':
                            self.message(channel, "There is a contest currently in progress in this channel, check !bet status.", isWhisper)
                        else:
                            resultData = getBetResults(betRow[0])
                            if resultData is None:
                                self.message(channel, "Error retrieving results.", isWhisper)
                                cur.close()
                                return
                                
                            formattedTime = formatTimeDelta(resultData["result"])
                            messages = ["The most recent contest finished in %s." % formattedTime]
                            if len(resultData["winners"]) == 0:
                                messages[0] += " There were no bets placed."
                            else:
                                messages[0] += " Results: "
                                place = 0
                                for row in resultData["winners"]:
                                    place += 1
                                    formattedDelta = ("-" if row["timedelta"] < 0 else "+") + formatTimeDelta(abs(row["timedelta"]))
                                    formattedBet = formatTimeDelta(row["bet"])
                                    entry = "({place}) {name} - {time} ({delta}); ".format(place=place, name=row["name"], time=formattedBet, delta=formattedDelta)
                                    if len(entry) + len(messages[-1]) > 400:
                                        messages.append(entry)
                                    else:
                                        messages[-1] += entry
                            
                            first = True
                            for message in messages:
                                if not first:
                                    time.sleep(0.5)
                                self.message(channel, message, isWhisper)
                                first = False
                        cur.close()
                        return
                    elif sender in self.myadmins and subcmd == "payout":
                        # pay out most recent bet in this channel
                        cur = db.cursor()
                        cur.execute("SELECT id, status FROM bets WHERE channel = %s ORDER BY id DESC LIMIT 1", [channel])
                        betRow = cur.fetchone()
                        if betRow is None or (betRow[1] != 'paid' and betRow[1] != 'completed'):
                            self.message(channel, "There is no pending time prediction contest to be paid out for this channel.", isWhisper)
                        elif betRow[1] == 'paid':
                            self.message(channel, "The most recent contest in this channel was already paid out.", isWhisper)
                        else:
                            # do the thing
                            resultData = getBetResults(betRow[0])
                            if resultData is None:
                                self.message(channel, "Error retrieving results.", isWhisper)
                                cur.close()
                                return
                                
                            numEntries = len(resultData["winners"])
                                
                            if numEntries < 2:
                                self.message(channel, "This contest had 0 or 1 entrants, no payout.", isWhisper)
                                cur.close()
                                return
                            
                            # calculate prize multiplier based on run length
                            # uses varying log depending on > or < 2h
                            if resultData["result"] < 7200000:
                                prizeMultiplier = 1 / (math.log(7200000.0 / resultData["result"], 5) + 1)
                            else:
                                prizeMultiplier = math.log(resultData["result"] / 7200000.0, 5) + 1
                                
                            # cutoff for half prize is delta of 1/15th of run length
                            halfCutoff = resultData["result"] / 15
                            
                            # pick prize pool based on number of entrants, max 12 different prizes
                            prizePool = [10000, 9000, 8000, 7000, 6000, 5000, 4000, 3000, 2000, 1000, 750, 500][-min(numEntries, 12):]
                                
                            # calculate first run of prizes
                            prizes = []
                            place = 0
                            for winner in resultData["winners"]:
                                place += 1
                                prize = prizePool[min(place - 1, 11)]
                                    
                                # apply multipliers
                                prize *= prizeMultiplier
                                
                                if abs(winner["timedelta"]) > halfCutoff:
                                    prize *= 0.5
                                elif abs(winner["timedelta"]) < 10:
                                    # what a lucky SoB
                                    prize *= 10
                                elif abs(winner["timedelta"]) < 1000:
                                    # 3x multiplier for within a second
                                    prize *= 3
                                elif int(winner["bet"] / 60000.0) == int(resultData["result"] / 60000.0) and resultData["result"] > 3600000:
                                    # 2x multiplier if hour and minute match, only if run was longer than 1hr
                                    prize *= 2
                                    
                                # make our prizes nice, also set a minimum prize of 50 per place
                                roundNum = 100.0 if prize > 5000 else 50.0
                                prize = max(int(round(prize/roundNum)*roundNum), 50 * (numEntries + 1 - place))
                                    
                                prizes.append(prize)
                                addPoints(winner["id"], prize)
                                cur.execute("UPDATE placed_bets SET prize = %s WHERE betid = %s AND userid = %s", [prize, betRow[0], winner["id"]])
                                
                            paidOut = sum(prizes)
                                
                            # broadcaster prize for runs > 1h
                            # run length in hours * 1000, capped to match first place prize
                            # minimum = 1/3rd of first place prize
                            if resultData["result"] >= 3600000:
                                bcPrize = min(max(resultData["result"] / 3600.0, max(prizes) / 3.0, 50), max(prizes))
                                bcPrize = int(round(bcPrize / 50.0) * 50)
                                
                                cur.execute("UPDATE users SET points = points + %s WHERE name = %s", [bcPrize, channel[1:]])
                                paidOut += bcPrize
                            else:
                                bcPrize = 0
                                
                            cur.execute("UPDATE bets SET status = 'paid', totalPaid = %s, paidBroadcaster = %s WHERE id = %s", [paidOut, bcPrize, betRow[0]])
                            
                            # take away points from the bot account
                            cur.execute("UPDATE users SET points = points - %s WHERE name = %s", [paidOut, config["username"]])
                                
                            messages = ["Paid out %d total points in prizes. Payouts: " % paidOut]
                            for i in range(numEntries):
                                msg = "{name} ({place}) - {points} points; ".format(name=resultData["winners"][i]["name"], place=formatRank(i+1), points=prizes[i])
                                if len(messages[-1] + msg) > 400:
                                    messages.append(msg)
                                else:
                                    messages[-1] += msg
                                    
                            if bcPrize > 0:
                                msg = "{name} (broadcaster) - {points} points".format(name=channel[1:], points=bcPrize)
                                if len(messages[-1] + msg) > 400:
                                    messages.append(msg)
                                else:
                                    messages[-1] += msg
                                    
                            first = True
                            for message in messages:
                                if not first:
                                    time.sleep(0.5)
                                self.message(channel, message, isWhisper)
                                first = False
                            
                        cur.close()
                        return
                    else:
                        self.message(channel,
                                     "Usage: !bet <time> OR !bet status OR (as channel owner) !bet open OR !bet start OR !bet end OR !bet cancel OR !bet results",
                                     isWhisper)
                    return
            if command == "import" and sender in self.myadmins:
                if len(args) != 1:
                    self.message(channel, "Usage: !import url", isWhisper)
                    return
                    
                url = args[0]
                if "pastebin.com" in url and "/raw/" not in url:
                    url = url.replace("pastebin.com/", "pastebin.com/raw/")
                
                try:
                    r = requests.get(url)
                    data = r.text.splitlines()
                    lineno = 0
                    errorlines = []
                    addwaifus = []
                    for line in data:
                        lineno += 1
                        if not line.strip():
                            continue
                        match = waifu_regex.fullmatch(line.strip())
                        if match:
                            addwaifus.append(match.groupdict())
                        else:
                            errorlines.append(lineno)
                            
                    if len(errorlines) > 0:
                        self.message(channel, "Error processing waifu data from lines: %s. Please fix formatting and try again." % ", ".join(str(lineno) for lineno in errorlines), isWhisper)
                        return
                    else:
                        cur = db.cursor()
                        cur.executemany("INSERT INTO waifus (Name, image, rarity, series) VALUES(%s, %s, %s, %s)", [(waifu["name"], waifu["link"], int(waifu["rarity"]), waifu["series"]) for waifu in addwaifus])
                        cur.close()
                        self.message(channel, "Successfully added %d waifus to the database." % len(addwaifus), isWhisper)
                        return
                except:
                    self.message(channel, "Error loading waifu data.", isWhisper)
                    print("Error: " + str(sys.exc_info()))
                    return
            if command == "sets" or command == "set":
                if (len(args) == 0):
                    self.message(channel, "Available sets: http://waifus.de/sets?user=" + str(sender).lower(), isWhisper=isWhisper)
                    return
                subcmd = args[0]
                if subcmd != "claim":
                    self.message(channel, "Usage: !sets OR !sets claim", isWhisper=isWhisper)
                    return
                else:
                    cur = db.cursor()
                    cur.execute("SELECT DISTINCT sets.id, sets.name, sets.reward FROM sets WHERE sets.claimed_by IS NULL AND sets.id NOT IN (SELECT DISTINCT setID FROM set_cards LEFT OUTER JOIN (SELECT * FROM has_waifu JOIN users ON has_waifu.userid = users.id WHERE users.id = %s) as a ON waifuid = cardID JOIN sets ON set_cards.setID = sets.id JOIN waifus ON cardID = waifus.id WHERE a.name IS NULL)", [tags["user-id"]])
                    rows = cur.fetchall()
                    if len(rows) == 0:
                        cur.close()
                        self.message(channel, "You do not have any completed sets that are available to be claimed.", isWhisper=isWhisper)
                        return
                    else:
                        for row in rows:
                            cur.execute("UPDATE sets SET claimed_by = %s, claimed_at = %s WHERE sets.id = %s", [tags["user-id"], current_milli_time(), row[0]])
                            addPoints(tags["user-id"], int(row[2]))
                            self.message(channel, "Successfully claimed the Set {set} and rewarded {user} with {reward} points!".format(set=row[1], user=tags["display-name"], reward=row[2]))
                        cur.close()
                        return
            if command == "debug" and sender in self.myadmins:
                upgradeHand(tags["user-id"], gifted=True)
                self.message(channel, "DEBUG: Upgraded your hand for free.", isWhisper=isWhisper)
                return

class HDNBot(pydle.Client):
    instance = None
    pw=None

    def __init__(self):
        super().__init__("hdnmarathon")
        HDNBot.instance = self

    def start(self, password):
        pool.connect(self, "irc.twitch.tv", 6667, tls=False, password=password)
        self.pw = password
        print("Connecting hdn...")

    def on_disconnect(self, expected):
        print("HDN Disconnected, reconnecting....")
        pool.connect(self, "irc.twitch.tv", 6667, tls=False, password=self.pw, reconnect=True)

    def on_connect(self):
        super().on_connect()
        print("HDN Joining")
        #self.join("#marenthyu")
        #self.join("#frankerfacezauthorizer")
        #self.message("#hdnmarathon", "This is a test message")
        print("Setting up WS")
        factory = MyClientFactory(str(ffzws) + ':' + str(443))
        factory.protocol = MyClientProtocol
        hostname = str(ffzws).replace("ws://", '').replace("wss://", '')
        print('[Websocket] Hostname: ' + hostname)
        reactor.connectSSL(hostname,
                           int(443),
                           factory, contextFactory=optionsForClientTLS(hostname=hostname))
        thread = Thread(target=reactor.run, kwargs={'installSignalHandlers': 0})

        thread.start()

    def on_message(self, source, target, message):
        print("message on #hdnmarathon: " + str(source) + ", " + str(target) + ", " + message)



class MyClientProtocol(WebSocketClientProtocol):
    instance = None
    msgnum = 1

    def onOpen(self):
        WebSocketClientProtocol.onOpen(self)

        authmsg = 'hello ["NepNepBot",false]'
        #print('[Websocket] OnOpen. Sending: ' + authmsg)
        self.sendWrap(authmsg)

        authmsg = 'setuser "hdnmarathon"'
        #print('[Websocket] OnOpen. Sending: ' + authmsg)
        self.sendWrap(authmsg)

        authmsg = 'sub "room.hdnmarathon"'
       #print('[Websocket] OnOpen. Sending: ' + authmsg)
        self.sendWrap(authmsg)

        authmsg = 'sub "channel.hdnmarathon"'
        #print('[Websocket] OnOpen. Sending: ' + authmsg)
        self.sendWrap(authmsg)

        authmsg = 'ready 0'
        #print('[Websocket] OnOpen. Sending: ' + authmsg)
        self.sendWrap(authmsg)

        MyClientProtocol.instance = self


    def onMessage(self, payload, isBinary):
        if isBinary:
            print("[Websocket] Binary message received: {0} bytes".format(len(payload)))


        else:
            print("[Websocket] Text message received: {0}".format(payload.decode('utf8')))
            parts = str(payload).split(" ")
            if len(parts) == 3 and parts[1]=="do_authorize":
                print("[Websocket] Catched required auth, authorizing with " + "AUTH " + (parts[2].replace('"', ''))[:-1])
                HDNBot.instance.message("#frankerfacezauthorizer", "AUTH " + (parts[2].replace('"', ''))[:-1])


    def onClose(self, wasClean, code, reason):
        # print('onclose')
        print('[Websocket] Closed connection with Websocket. Reason: ' + str(reason))
        WebSocketClientProtocol.onClose(self, wasClean, code, reason)

    def sendWrap(self, msg):
        print("[Websocket] Sending msg: " + str(str(MyClientProtocol.msgnum) + " " + msg))
        self.sendMessage(str(str(MyClientProtocol.msgnum) + " " + msg).encode('utf-8'))
        MyClientProtocol.msgnum += 1

    def setFollowButtons(self, users):
        formattedusers = []
        for user in users:
            formattedusers.append('"{0}"'.format(str(user)))

        userstring = "[" + (",".join(formattedusers)) + "]"
        msg = 'update_follow_buttons ["{name}",{users}]'.format(name="hdnmarathon", users=userstring)
        self.sendWrap(msg)



class MyClientFactory(ReconnectingClientFactory, WebSocketClientFactory):
    protocol = MyClientProtocol

    def startedConnecting(self, connector):
        print('[Websocket] Started to connect.')
        ReconnectingClientFactory.startedConnecting(self, connector)

    def clientConnectionLost(self, connector, reason):
        print('[Websocket]  Lost connection. Reason: {}'.format(reason))
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        print('[Websocket]  Connection failed. Reason: {}'.format(reason))
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

    def retry(self, connector=None):
        print('[Websocket] Reconnecting to API Websocket in ' + str(int(self.delay)) + ' seconds...')
        #ReconnectingClientFactory.retry(self)



curg = db.cursor()

curg.execute("SELECT * FROM config")
config = {}
print("Importing config from database")
for row in curg.fetchall():
    config[row[0]] = row[1]

print("Config: " + str(config))
print("Fetching channel list...")
curg.execute("SELECT * FROM channels")
channels = []
for row in curg.fetchall():
    channels.append("#" + row[0])
print("Channels: " + str(channels))
print("Fetching admin list...")
curg.execute("SELECT * FROM admins")
admins = []
for row in curg.fetchall():
    admins.append(row[0])
print("Admins: " + str(admins))
revrarity = {}
i = 0
while i<=6:
    n = config["rarity" + str(i) + "Name"]
    revrarity[n] = i
    i += 1
curg.execute("SELECT name FROM blacklist")
rows = curg.fetchall()
for row in rows:
    blacklist.append(row[0])
    
# visible packs
curg.execute("SELECT name FROM boosters WHERE listed = 1 AND buyable = 1 ORDER BY sortIndex ASC")
packrows = curg.fetchall()
visiblepacks = "/".join(row[0] for row in packrows)
curg.close()

headers = {"Client-ID":str(config["clientID"]), "Accept":"application/vnd.twitchtv.v5+json"}
r = requests.get("https://api.twitch.tv/kraken/users", headers=headers, params={"login":str(config["username"]).lower()})
j = r.json()
try:
    twitchid = j["users"][0]["_id"]
except:
    twitchid = 0
config["twitchid"] = str(twitchid)
b = NepBot(config, channels, admins)
b.start(config["oauth"])

print("past start")

if hdnoauth:
    hdnb = HDNBot()
    hdnb.start(hdnoauth)

def startPubSub(nepbot):



    class PubSubClientProtocol(WebSocketClientProtocol):
        bot = None
        instance = None
        msgnum = 1
        tinstance = None

        def onOpen(self):
            print("[PubSub] onOpen")
            super().onOpen()
            PubSubClientProtocol.instance = self
            def sendPing(payload=None):
                self.sendMessage(str('{"type": "PING' + (('", "nonce": ' + str(payload)) if payload is not None else '') + '"}').encode('utf-8'), False)
                random.seed()
                r = random.random()
                self.tinstance = Timer(60 + r*5, sendPing)
                self.tinstance.start()
            self.tinstance = Timer(60, sendPing)
            self.tinstance.start()
            msg = str(u'{"type":"LISTEN", "data":{"topics":["whispers.100891505"], "auth_token":"' + str(config["oauth"].replace("oauth:", "")) + '"}}')
            self.sendMessage(payload=msg.encode('utf-8'), isBinary=False)



        def onMessage(self, payload, isBinary):
            if isBinary:
                print("[PubSub] Binary message received: {0} bytes".format(len(payload)))


            else:
                msg = payload.decode('utf8')
                print("[PubSub] Text message received: {0}".format(str(msg).replace("\n", "")))
                j = json.loads(msg)
                print("Got Message Type: " + str(j["type"]).replace("\n", ""))
                if j["type"] == "MESSAGE":
                    data = j["data"]
                    message = data["message"]
                    try:
                        jmsg = json.loads(message)
                        # print("Json message: " + str(jmsg))
                        do = jmsg["data_object"]
                        body = do["body"]
                        sender = do["tags"]["login"]
                        print("[PubSub] Got whisper from {sender}: {msg}".format(sender=sender, msg=body))
                        if str(body).startswith("!"):
                            parts = str(body).split(" ")
                            cmd = parts[0].replace("!", "")
                            args = parts[1:]
                            PubSubClientProtocol.bot.do_command(cmd, args, "#" + sender, sender, tags=do["tags"], isWhisper=True)
                    except:
                        print("Error converting PubSub message.")
                        print("Error: " + str(sys.exc_info()))



        def onClose(self, wasClean, code, reason):
            print('[PubSub] Closed connection with Websocket. Reason: ' + str(reason))
            super().onClose(wasClean, code, reason)




    class PubSubFactory(ReconnectingClientFactory, WebSocketClientFactory):
        protocol = PubSubClientProtocol

        def startedConnecting(self, connector):
            print('[PubSub] Started to connect.')
            super().startedConnecting(connector)

        def clientConnectionLost(self, connector, reason):
            print('[PubSub]  Lost connection. Reason: {}'.format(reason))
            ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

        def clientConnectionFailed(self, connector, reason):
            print('[PubSub]  Connection failed. Reason: {}'.format(reason))
            ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

        def retry(self, connector=None):
            print('[PubSub] Reconnecting to PubSub Websocket in ' + str(int(self.delay)) + ' seconds...')
            #ReconnectingClientFactory.retry(self)

    PubSubClientProtocol.bot = nepbot
    pubsubaddr = "wss://pubsub-edge.twitch.tv"
    factory = PubSubFactory(str(pubsubaddr))
    factory.protocol = PubSubClientProtocol
    hostname = str(pubsubaddr).replace("ws://", '').replace("wss://", '')
    print('[PubSub] Hostname: ' + hostname)
    reactor.connectSSL(hostname,
                       int(443),
                       factory, contextFactory=optionsForClientTLS(hostname=hostname))
    thread = Thread(target=reactor.run, kwargs={'installSignalHandlers': 0})
    thread.start()

#startPubSub(b)
pool.handle_forever()
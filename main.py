#!/usr/bin/python
# coding=utf-8
import pymysql
import atexit
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
from string import ascii_letters

from twisted.internet import reactor
from autobahn.twisted.websocket import WebSocketClientProtocol
from autobahn.twisted.websocket import WebSocketClientFactory
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.ssl import optionsForClientTLS

import sys

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


db = pymysql.connect(host="localhost", user="nepbot", passwd=dbpw, db="nepbot", autocommit="True")
openbooster = {}
trades = {}
activitymap = {}
blacklist = []
busy = False
streamlabsauthurl = "https://www.streamlabs.com/api/v1.0/authorize?client_id=" + streamlabsclient + "&redirect_uri=http://marenthyu.de/cgi-bin/waifucallback.cgi&response_type=code&scope=alerts.create&state="
streamlabsalerturl = "https://streamlabs.com/api/v1.0/alerts"
alertheaders = {"Content-Type":"application/json", "User-Agent":"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"}

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
    cur.execute("SELECT users.name FROM waifus JOIN has_waifu ON waifus.id = has_waifu.waifuid JOIN users ON has_waifu.userid = users.id WHERE waifus.id = %s", str(id))
    rows = cur.fetchall()
    ret = []
    for row in rows:
        ret.append(row[0])
    cur.close()
    return ret

def handLimit(user):
    cur = db.cursor()
    cur.execute("SELECT handLimit FROM users WHERE name = '{0}'".format(str(user).lower()))
    res = cur.fetchone()
    limit = int(res[0])
    cur.close()
    return limit

def upgradeHand(user):
    cur = db.cursor()
    cur.execute("UPDATE users SET handLimit = handLimit + 1 WHERE name = %s", (str(user)))
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

alertbusy = False
alertcounter = 0
currentAlert = 0
def sendalert(channel, waifu, user):
    print("Alerting for waifu " + str(waifu))
    global currentAlert
    global alertcounter
    myalert = alertcounter
    alertcounter += 1

    global alertbusy
    while alertbusy or currentAlert < myalert:
        time.sleep(1)
    alertbusy = True
    cur = db.cursor()
    cur.execute("SELECT alertkey FROM channels WHERE channels.name='{name}'".format(name=str(channel).replace("#", "").lower()))
    message = "{user} drew [{rarity}] {name}!".format(user=str(user),
                                                      rarity=str(config["rarity" + str(waifu["rarity"]) + "Name"]),
                                                      name=str(waifu["name"]))
    alertbody = {"type": "donation", "image_href": waifu["image"],
                 "sound_href": config["alertSound"], "duration": int(config["alertDuration"]), "message": message}
    discordbody = {"username": "Waifu TCG", "embeds": [
        {
            "title": "A {rarity} waifu has been dropped!".format(
                rarity=str(config["rarity" + str(waifu["rarity"]) + "Name"])),
            "color": int(config["rarity" + str(waifu["rarity"]) + "EmbedColor"])
        },
        {
            "type": "rich",
            "title": "{user} dropped {name}!".format(user=str(user), name=str(waifu["name"])),
            "url": "https://twitch.tv/{name}".format(name=str(channel).replace("#", "").lower()),
            "color": int(config["rarity" + str(waifu["rarity"]) + "EmbedColor"]),
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
    try:
        token = str(cur.fetchall()[0][0])
        alertbody.update({"access_token": token})
        req = requests.post(streamlabsalerturl, headers=alertheaders, json=alertbody)
    except:
        print("Tried to alert for " + str(channel) + ", " + str(waifu) + ", " + str(user) + ", but failed. Continuing with discord alerts.")

    cur.execute("SELECT url FROM discordHooks")
    discordhooks = cur.fetchall()

    for row in discordhooks:
        url = row[0]
        req2 = requests.post(
            url,
            json=discordbody)
        while req2.status_code == 429:
            time.sleep((req2.headers["Retry-After"] / 1000) + 1)
            req2 = requests.post(
                url,
                json=discordbody)

    cur.close()
    currentAlert += 1
    alertbusy = False

def followsme(name):
    cur = db.cursor()
    cur.execute("SELECT twitchID FROM users WHERE name='{user}'".format(user=str(name)))
    try:
        twitchid = cur.fetchone()[0]
        r = requests.get("https://api.twitch.tv/kraken/users/{twitchid}/follows/{myid}".format(twitchid=str(twitchid), myid=str(config["twitchid"])), headers=headers)
        j = r.json()
        cur.close()
        return j["status"] != "404"
    except:
        cur.close()
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
    cur.execute("SELECT * FROM waifus WHERE id='" + str(id) + "'")
    row = cur.fetchone()
    ret = {"id":row[0], "name":row[1], "image":row[2], "rarity":row[3], "series":row[4]}
    cur.close()
    #print("Fetched Waifu from id: " + str(ret))
    return ret

def hasPoints(name, amount):
    cur = db.cursor()
    cur.execute("SELECT points FROM users WHERE name = '" + str(name) + "'")
    ret = int(cur.fetchone()[0]) >= int(amount)
    cur.close()
    return ret

def addPoints(name, amount):
    cur = db.cursor()
    cur.execute("UPDATE users SET points = points + '" + str(amount) + "' WHERE name='" + str(name) + "'")
    cur.close()

def currentCards(name):
    cur = db.cursor()
    cur.execute(
        "SELECT SUM(amount) AS totalCards FROM has_waifu JOIN users on has_waifu.userid = users.id WHERE users.name = '{0}'".format(
            str(name)))
    ret = cur.fetchone()[0] or 0
    cur.close()
    return ret

def maxWaifuID():
    cur = db.cursor()
    cur.execute("SELECT MAX(id) FROM waifus")
    ret = int(cur.fetchone()[0])
    cur.close()
    return ret

def dropCard(rarity=-1, super=False, ultra=False):
    random.seed()
    if rarity == -1:
        i = 0
        rarity = 0
        while (i < 6):
            r = random.random()
            if r <= (float(config["rarity{rarity}UpgradeChance{super}".format(rarity=str(rarity), super=("Super" if super else ("Ultimate" if ultra else "")))])):
                rarity = rarity + 1
                i = i + 1
                continue
            if super and rarity == 0:
                rarity = 1
            break
        return dropCard(rarity)
    else:
        #print("Dropping card of rarity " + str(rarity))
        cur = db.cursor()
        cur.execute("SELECT * FROM waifus WHERE rarity='{0}'".format(rarity))
        rows = cur.fetchall()
        max = len(rows)
        raritymax = int(config["rarity" + str(rarity) + "Max"])
        retcount = 0
        retid = 0
        while retid == 0 or retcount >= raritymax:

            r = random.randint(0, max - 1)
            retid = rows[r][0]

            cur.execute(
                "SELECT SUM(amount) AS totalCards FROM has_waifu JOIN users on has_waifu.userid = users.id WHERE has_waifu.waifuid = '{0}'".format(
                    str(retid)))
            retcount = cur.fetchone()[0] or 0
            #print("retid: " + str(retid) + ", retcount: " + str(retcount) + ", raritymax: " + str(raritymax))
            if raritymax == 0:
                break
        cur.close()
        #print("Dropping ID " + str(retid))
        return retid


def giveCard(user, id):
    cur = db.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM has_waifu JOIN users on has_waifu.userid = users.id WHERE users.name = '{name}' AND has_waifu.waifuid = '{id}'".format(
            name=str(user), id=str(id)))
    hasWaifu = cur.fetchone()[0] == 1
    if hasWaifu:
        cur.execute(
            "UPDATE has_waifu JOIN users on has_waifu.userid = users.id SET amount = amount+'1' WHERE users.name='{name}' AND waifuid='{id}'".format(
                name=str(user), id=str(id)))
    else:
        cur.execute("SELECT id FROM users WHERE name='{name}'".format(name=str(user)))
        userid = cur.fetchone()[0]
        #print("giving using:")
        #print("INSERT INTO has_waifu(userid, waifuid, amount) VALUE ('{userid}', '{waifuid}', '1')".format(userid=userid, waifuid=str(id)))
        cur.execute("INSERT INTO has_waifu(userid, waifuid, amount) VALUE ('{userid}', '{waifuid}', '1')".format(userid=userid, waifuid=str(id)))
    cur.close()

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
            global busy
            busy = True
            global t
            t = Timer(300, timer)
            t.start()
            print("Refreshing Database Connection...")
            global db
            try:
                db.close()
            except:
                print("Error closing db connection cleanly, ignoring.")
            try:
                db = pymysql.connect(host="localhost", user="nepbot", passwd=dbpw, db="nepbot", autocommit="True")
            except:
                print("Error Reconnecting to DB. Skipping Timer Cycle.")
                t = Timer(300, timer)
                t.start()
                return
            busy = False
            print("Checking live status of channels...")
            cur = db.cursor()
            cur.execute("SELECT users.name, users.twitchID FROM channels join users ON channels.name = users.name")
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
            try:
                global activitymap
                #print("Activitymap: " + str(activitymap))
                doneusers = []
                validactivity = []
                for channel in self.channels:
                    #print("Fetching for channel " + str(channel))
                    with urllib.request.urlopen('https://tmi.twitch.tv/group/user/' + str(channel).replace("#", "") + '/chatters') as response:
                        data = json.loads(response.read().decode())
                        chatters = data["chatters"]
                        mods = chatters["moderators"]
                        staff = chatters["staff"]
                        admins = chatters["admins"]
                        globalmods = chatters["global_mods"]
                        viewers = chatters["viewers"]

                        a = []
                        a.append(mods)
                        a.append(staff)
                        a.append(admins)
                        a.append(globalmods)
                        a.append(viewers)

                        for sub in a:
                            for viewer in sub:
                                if viewer not in doneusers:
                                    doneusers.append(viewer)
                                if isLive[str(channel).replace("#", "")] and viewer not in validactivity:
                                    validactivity.append(viewer)
                cur = db.cursor()
                # process all users
                print("Catched users, giving points and creating accounts")
                busy = True
                for viewer in doneusers:
                    cur.execute("SELECT COUNT(*) FROM users WHERE name='{0}'".format(str(viewer).lower()))
                    if int(cur.fetchone()[0]) == 0:
                        print("Creating account for " + str(viewer).lower())
                        r = requests.get("https://api.twitch.tv/kraken/users", headers=headers,
                                         params={"login": str(viewer).lower()})
                        j = r.json()
                        #print("Setting twitch id")
                        #print(str(j))
                        try:
                            twitchid = j["users"][0]["_id"]
                        except:
                            twitchid = 0
                        #print("Inserting into db")
                        #print(
                        #    "INSERT INTO users (name, points, lastFree, twitchID) VALUE ('{name}', 0, 0, {twitchID})".format(
                        #        name=str(viewer).lower(), twitchID=str(twitchid)))
                        cur.execute(
                            "INSERT INTO users (name, points, lastFree, twitchID) VALUE ('{name}', 0, 0, {twitchID})".format(
                                name=str(viewer).lower(), twitchID=str(twitchid)))
                        #print("Success?")
                    cur.execute("UPDATE users SET points = points + '{points}' WHERE name = '{name}'".format(points=
                        str(
                            int(
                                config["passivePoints"]) +
                                    max((10 -
                                         (10
                                          if (
                                              (str(viewer).lower() not in activitymap.keys())
                                              or
                                              (str(viewer).lower() not in validactivity))
                                          else
                                            int(activitymap[str(viewer).lower()])
                                          )
                                         ), 0)
                        ), name=str(viewer).lower()))
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


            cur = db.cursor()
            busy = True
            try:
                #print("Deleting outdated displayTokens")

                a = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
                beforeSeconds = int((a - datetime.datetime(1970,1,1)).total_seconds())
                #print("Using " + "DELETE FROM displayTokens WHERE unix_timestamp(timestamp) < {ts}".format(ts=str(beforeSeconds)))
                cur.execute("DELETE FROM displayTokens WHERE unix_timestamp(timestamp) < {ts}".format(ts=str(beforeSeconds)))

            except:
                print("Error deleting old tokens. skipping this cycle.")
            cur.close()
            busy = False

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
        global activitymap
        global blacklist
        if sender not in blacklist and str(sender).find("bot") == -1:
            activitymap[sender] = 0
            activitymap[channelowner] = 0
            global busy
            if not busy:
                cur = db.cursor()
                cur.execute("SELECT name FROM users WHERE twitchID = %s", [str(tags['user-id'])])
                userswithid = cur.fetchall()
                if len(userswithid) < 1:
                    cur.execute("INSERT INTO users(name, points, twitchID) VALUE (%s, %s, %s)", [str(sender).lower(), str(0), str(tags['user-id'])])
                    print("{name} didnt have an account, created it.".format(name=str(tags['display-name'])))
                elif str(userswithid[0][0]).lower() != str(sender).lower():
                    print("{oldname} got a new name, changing: {newname}".format(oldname=str(userswithid[0][0]).lower(), newname=str(sender).lower()))
                    cur.execute("UPDATE users SET name = %s WHERE users.twitchID = %s", [str(sender).lower(), str(tags['user-id'])])
                cur.close()

        if str(message).startswith("!"):
            if sender in blacklist or str(sender).find("bot") > -1:
                self.message(str(source), "Bad Bot. No.")
                return
            parts = str(message).split(" ")
            pastbase = False
            args = []
            for part in parts:
                if not pastbase:
                    pastbase = True
                    continue
                args.append(part)
            self.do_command(str(parts[0]).replace("!", ""), args, target, source, tags, isWhisper=isWhisper)
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
        if isWhisper:
            super().message("#jtv", "/w " + str(channel).replace("#", "") + " " +str(message))
        else:
            super().message(channel, message)


    def do_command(self, command, args, sender, channel, tags, isWhisper=False):
        print("Got command: " + command + " with arguments " + str(args))
        if busy:
            self.message(channel, "I'm currently busy, try again in a moment!", isWhisper=isWhisper)
            return
        if str(command).lower() == "quit" and sender.lower() in self.myadmins:
            print("Quitting from admin command.")
            pool.disconnect(client=self, expected=True)
            # sys.exit(0)
        if str(command).lower() == "checkhand":
            #print("Checking hand for " + sender)
            cur = db.cursor()
            cur.execute(
                "SELECT waifus.*, amount, handLimit FROM waifus JOIN has_waifu ON waifus.id = has_waifu.waifuid JOIN users ON has_waifu.userid = users.id WHERE users.twitchID = \'{0}\'".format(
                    str(tags['user-id'])))
            message = str(tags['display-name']) + ", you have the following waifus: "
            rows = cur.fetchall()
            secondmessage = ""
            thirdmessage = ""
            cards=[]
            if (len(rows) == 0):
                self.message(channel, "{user}, you don't currently have any waifus! Get your first one with !freewaifu".format(user=str(sender)), isWhisper=isWhisper)
                return
            for row in rows:
                cards.append(int(row[0]))
                toadd = '[{id}][{rarity}] {name} from {series} - {link}{amount}; '.format(id=str(row[0]),
                                                                                             rarity=config[
                                                                                                 "rarity" + str(
                                                                                                     row[3]) + "Name"],
                                                                                             name=row[1], series=row[4],
                                                                                             link=row[2], amount=(
                        (" (x" + str(row[5]) + ")") if row[5] > 1 else ""))
                if len(secondmessage) + len(toadd) > 400:
                    thirdmessage += toadd
                elif len(message) + len(toadd) > 400:
                    secondmessage += toadd
                else:
                    message += toadd
            whisper = True

            droplink = "http://waifus.de/hand?user=" + str(sender).lower()
            if len(args) >= 1:
                whisper = str(args[0]) != "public"
            if followsme(str(sender).lower()) and whisper:
                self.message("#jtv", "/w " + str(sender).lower() + " " + str(droplink), isWhisper=False)
                self.message("#jtv", "/w " + str(sender).lower() + " " + message, isWhisper=False)
                if secondmessage != "":
                    self.message("#jtv", "/w " + str(sender).lower() + " " + secondmessage, isWhisper=False)
                if thirdmessage != "":
                    self.message("#jtv", "/w " + str(sender).lower() + " " + thirdmessage, isWhisper=False)
            else:
                if len(cards) == 0:
                    self.message(channel, "{user}, your hand is empty!".format(user=str(sender)))
                self.message(channel, "{user}, you can have {limit} waifus and your current hand is: {droplink}".format(user=str(sender), droplink=droplink, limit=str(rows[0][6])))
                # self.message(channel, message)
                # if secondmessage != "":
                #     self.message(channel, secondmessage)
                # if thirdmessage != "":
                #     self.message(channel, thirdmessage)
            cur.close()
            return
        if str(command).lower() == "points":
            #print("Checking points for " + sender)
            cur = db.cursor()
            cur.execute("SELECT points FROM users WHERE twitchID = '{0}'".format(str(tags['user-id'])))
            self.message(channel, str(tags['display-name']) + ", you have " + str(cur.fetchone()[0]) + " points!", isWhisper=isWhisper)
            cur.close()
            return
        if str(command).lower() == "freewaifu":
            #print("Checking free waifu egliability for " + str(sender))
            cur = db.cursor()
            cur.execute("SELECT lastFree, handLimit FROM users WHERE twitchID = '{0}'".format(str(tags['user-id'])))
            res = cur.fetchone()
            nextFree = 79200000 + int(res[0])
            limit = int(res[1])
            freeAvailable = nextFree < current_milli_time()
            if freeAvailable and currentCards(str(sender)) < limit:
                #print("egliable, dropping card.")
                cur.execute("SELECT * FROM waifus WHERE id='{0}'".format(dropCard()))
                row = cur.fetchone()
                if int(row[3])>3:
                    threading.Thread(target=sendalert, args=(channel, {"name":row[1], "rarity":row[3], "image":row[2]}, str(tags["display-name"]))).start()
                self.message(channel, str(
                    sender) + ', you dropped a new Waifu: [{id}][{rarity}] {name} from {series} - {link}'.format(
                    id=str(row[0]), rarity=config["rarity" + str(row[3]) + "Name"], name=row[1], series=row[4],
                    link=row[2]), isWhisper=isWhisper)
                giveCard(str(sender).lower(), str(row[0]))
                id = str(row[0])
                cur.execute(
                        "UPDATE users SET lastFree='{timestamp}' WHERE twitchID='{name}'".format(name=str(tags['user-id']),
                                                                                         timestamp=current_milli_time()))
                cur.execute("INSERT INTO drops(userid, waifuid) VALUE (%s, %s)", [str(tags['user-id']), id])
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
        if str(command).lower() == "disenchant":
            if len(args) != 1:
                self.message(channel, "Usage: !disenchant <ID>", isWhisper=isWhisper)
                return
            try:
                id = int(args[0])
                cur = db.cursor()
                cur.execute(
                    "SELECT SUM(amount) AS totalCards FROM has_waifu JOIN users on has_waifu.userid = users.id WHERE users.twitchID = '{0}' AND has_waifu.waifuid = '{1}'".format(
                        str(tags['user-id']), str(id)))
                try:
                    amount = int(cur.fetchone()[0])
                except:
                    amount = 0
                if amount == 0:
                    self.message(channel, "You do not have that waifu!", isWhisper=isWhisper)
                    cur.close()
                    return
                elif amount == 1:
                    cur.execute("SELECT id FROM users WHERE name='{name}'".format(name=str(sender).lower()))
                    userid = cur.fetchone()[0]
                    cur.execute("DELETE FROM has_waifu WHERE waifuid = '{waifuid}' AND userid = '{userid}'".format(userid=userid, waifuid=id))
                else:
                    cur.execute(
                        "UPDATE has_waifu JOIN users on has_waifu.userid = users.id SET amount = amount-'1' WHERE users.name='{name}' AND waifuid='{id}'".format(
                            name=str(sender).lower(), id=str(id)))
                cur.execute("SELECT rarity FROM waifus WHERE id = '{0}'".format(id))
                rarity = cur.fetchone()[0]
                value = int(config["rarity" + str(rarity) + "Value"])
                cur.execute("UPDATE users SET points = points+'{value}' WHERE twitchID='{name}'".format(value=str(value), name=str(tags['user-id'])))
                self.message(channel, "Successfully disenchanted waifu {waifu} and added {value} points to {user}'s account".format(value=value, waifu=str(id), user=str(tags['display-name'])), isWhisper=isWhisper)
                cur.close()
                return
            except:
                self.message("Usage: !disenchant <ID>", isWhisper=isWhisper)
                return
        if str(command).lower() == "giveme" and not sender.lower() in self.myadmins:
            self.message(channel, "Sorry, you are not an admin.", isWhisper=isWhisper)
            return
        if str(command).lower() == "buy":
            if len(args) != 1:
                self.message(channel, "Usage: !buy <rarity> (So !buy 1 for an uncommon)", isWhisper=isWhisper)
                return
            if currentCards(str(sender).lower()) + 1 > handLimit(str(sender).lower()):
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
            if not hasPoints(str(sender).lower(), price):
                self.message(channel, "You do not have enought points to buy a " + str(config["rarity" + str(rarity) + "Name"]) + " waifu. You need " + str(price) + " points.", isWhisper=isWhisper)
                return
            addPoints(str(sender).lower(), 0 - price)
            cur = db.cursor()
            cur.execute("SELECT * FROM waifus WHERE id='{0}'".format(dropCard(rarity)))
            row = cur.fetchone()
            self.message(channel, str(
                tags['display-name']) + ', you bought a new Waifu for {price}: [{id}][{rarity}] {name} from {series} - {link}'.format(
                id=str(row[0]), rarity=config["rarity" + str(row[3]) + "Name"], name=row[1], series=row[4],
                link=row[2], price=str(price)), isWhisper=isWhisper)
            giveCard(str(sender).lower(), str(row[0]))
            cur.close()
            return
        if str(command).lower() == "giveme" and sender.lower() in self.myadmins:
            cur = db.cursor()
            cur.execute("SELECT * FROM waifus WHERE id='{0}'".format(dropCard(args[0])))
            row = cur.fetchone()
            self.message(channel, str(
                sender) + ', you dropped a new Waifu: [{id}][{rarity}] {name} from {series} - {link}'.format(
                id=str(row[0]), rarity=config["rarity" + str(row[3]) + "Name"], name=row[1], series=row[4],
                link=row[2]), isWhisper=isWhisper)
            giveCard(str(sender).lower(), str(row[0]))
            cur.close()
            return
        if str(command).lower() == "booster":
            if len(args) < 1:
                self.message(channel, "Usage: !booster buy <standard/super> OR !booster select <take/disenchant> (for each waifu) OR !booster show", isWhisper=isWhisper)
                return
            if args[0] == "show":
                if str(sender).lower() not in openbooster.keys():
                    self.message(channel, str(sender).lower() + ", you do not have an open booster. Buy one using !booster buy <standard/super>", isWhisper=isWhisper)
                    return
                # cur = db.cursor()
                cards = openbooster[str(sender).lower()]
                cardstring = ""
                secondcardstring = ""
                thirdcardstring = ""
                # for card in cards:
                #     cur.execute("SELECT id, name, rarity, series, image FROM waifus WHERE id='" + str(card) + "'")
                #     row = cur.fetchone()
                #     toadd = "[{id}][{rarity}] {name} from {series} - {image}; ".format(id=row[0], rarity=str(
                #         config["rarity" + str(row[2]) + "Name"]), name=row[1], series=row[3], image=row[4])
                #     if len(secondcardstring) + len(toadd) > 400:
                #         thirdcardstring += toadd
                #     elif len(cardstring) + len(toadd) > 400:
                #         secondcardstring += toadd
                #     else:
                #         cardstring += toadd
                #
                # cur.close()
                token = ''.join(choice(ascii_letters) for v in range(10))
                addDisplayToken(token, cards)
                droplink = "http://waifus.de/booster?token=" + token
                self.message(channel, "{user}, your current open booster pack: {droplink}".format(user=str(sender).lower(), droplink=droplink), isWhisper=isWhisper)
                # self.message(channel, cardstring)
                # if secondcardstring != "":
                #     self.message(channel, secondcardstring)
                # if thirdcardstring != "":
                #     self.message(channel, thirdcardstring)
                return
            if args[0] == "select":
                if str(sender).lower() not in openbooster.keys():
                    self.message(channel, "{user}, you currently do not have an open booster. Buy one using !booster buy <standard/super>".format(user=str(sender)), isWhisper=isWhisper)
                    return
                if len(args) - 1 != len(openbooster[str(sender).lower()]):
                    self.message(channel, "You did not specify the correct amount of keep/disenchant. Please provide " + str(len(openbooster[str(sender).lower()])), isWhisper=isWhisper)
                    return
                keeping = 0
                for arg in args[1:]:
                    if not (str(arg).lower() == "keep" or str(arg).lower() == "disenchant"):
                        self.message(channel, "Sorry, but " + str(arg).lower() + " is not a valid option. Use keep or disenchant", isWhisper=isWhisper)
                        return
                    if str(arg).lower() == "keep":
                        keeping += 1
                currCards = currentCards(str(sender).lower())
                #self.message(channel, "keeping: " + str(keeping) + ", currCards: " + str(currCards) + ", limit: " + str(config["handLimit"]))
                if keeping + currCards > handLimit(str(sender).lower()) and keeping != 0:
                    self.message(channel, "You can't keep that many waifus! !disenchant some!", isWhisper=isWhisper)
                    return
                cards = openbooster[str(sender).lower()]
                gottenpoints = 0
                response = "You take your booster pack and: "
                c = 2
                cur = db.cursor()
                keeps = []
                des = []
                for arg in args[1:]:
                    if str(arg).lower() == "keep":
                        #print("Cards: " + str(cards))
                        #print("Current c: " + str(c))
                        #print("Current card: " + str(cards[c-2]))
                        giveCard(str(sender).lower(), cards[c-2])
                        keeps.append(cards[c-2])
                    else:
                        # Disenchant
                        id = cards[c-2]
                        cur.execute("SELECT rarity FROM waifus WHERE id = '{0}'".format(id))
                        rarity = cur.fetchone()[0]
                        value = int(config["rarity" + str(rarity) + "Value"])
                        cur.execute(
                            "UPDATE users SET points = points+'{value}' WHERE name='{name}'".format(value=str(value),
                                                                                                    name=str(
                                                                                                        sender).lower()))
                        des.append(id)
                        gottenpoints += value
                    c += 1
                if len(keeps) > 0:
                    response += " keep " + ', '.join(str(x) for x in keeps) + ";"
                if len(des) > 0:
                    response += " disenchant " + ', '.join(str(x) for x in des) + ";"
                self.message(channel, response + " netting a total of " + str(gottenpoints) + " points.", isWhisper=isWhisper)
                cur.close()
                openbooster.pop(str(sender).lower())
                return
            if args[0] == "buy":
                if str(sender).lower() in openbooster.keys():
                    self.message(channel, "You already have an open booster. Select the waifus you want to keep or disenchant first!", isWhisper=isWhisper)
                    return
                if len(args) < 2:
                    self.message(channel, "Usage: !booster buy <standard/super/ultimate>", isWhisper=isWhisper)
                    return
                if args[1] == "standard":
                    if not hasPoints(str(sender).lower(), config["packPrice"]):
                        self.message(channel, str(sender).lower() + ", sorry, you don't have enough points to buy a booster pack. You need " + str(config["packPrice"]), isWhisper=isWhisper)
                        return
                    addPoints(str(sender).lower(), 0 - int(config["packPrice"]))

                    cards = []
                    i = 0
                    while i < 6:
                        while True:
                            ca = dropCard(super=False)
                            if ca not in cards:
                                cards.append(ca)
                                break
                        i += 1
                    cards = sorted(cards)
                    openbooster[str(sender).lower()] = cards
                    cardstring = ""
                    secondcardstring = ""
                    thirdcardstring = ""
                    bestwaifu = {"rarity":0}
                    alertwaifus = []
                    cur = db.cursor()
                    for card in cards:
                        cur.execute("SELECT id, name, rarity, series, image FROM waifus WHERE id='" + str(card) + "'")
                        row = cur.fetchone()
                        toadd = "[{id}][{rarity}] {name} from {series} - {image}; ".format(id=row[0], rarity=str(
                            config["rarity" + str(row[2]) + "Name"]), name=row[1], series=row[3], image=row[4])
                        if int(row[2]) > 3:
                            alertwaifus.append( {"name":str(row[1]), "rarity":int(row[2]), "image":str(row[4])})
                            #print(alertwaifus)
                        if int(row[2]) > 2 and int(row[2]) > int(bestwaifu["rarity"]):
                            bestwaifu = {"name":str(row[1]), "rarity":int(row[2]), "image":str(row[4])}
                        if len(secondcardstring) + len(toadd) > 400:
                            thirdcardstring += toadd
                        elif len(cardstring) + len(toadd) > 400:
                            secondcardstring += toadd
                        else:
                            cardstring += toadd

                        cur.execute("INSERT INTO drops(userid, waifuid) VALUE (%s, %s)", [str(tags['user-id']), str(card)])
                    cur.close()

                    token = ''.join(choice(ascii_letters) for v in range(10))
                    addDisplayToken(token, cards)
                    droplink = "http://waifus.de/booster?token=" + token
                    self.message(channel, "{user}, you open a standard booster pack and you get: {droplink}".format(user=str(sender).lower(), droplink=droplink), isWhisper=isWhisper)
                    for w in alertwaifus:
                        threading.Thread(target=sendalert, args=(channel, w, str(tags["display-name"]))).start()
                    # self.message(channel, cardstring)
                    # if secondcardstring != "":
                    #     self.message(channel, secondcardstring)
                    # if thirdcardstring != "":
                    #     self.message(channel, thirdcardstring)
                    return
                if str(args[1]).lower() == "super":
                    if not hasPoints(str(sender).lower(), config["superPackPrice"]):
                        self.message(channel, str(sender).lower() + ", sorry, you don't have enough points to buy a booster pack. You need " + str(config["superPackPrice"]), isWhisper=isWhisper)
                        return
                    addPoints(str(sender).lower(), 0 - int(config["superPackPrice"]))

                    cards = []
                    i = 0
                    while i < 5:
                        while True:
                            ca = dropCard(super=True)
                            if ca not in cards:
                                cards.append(ca)
                                break
                        i += 1

                    cardstring = ""
                    secondcardstring = ""
                    thirdcardstring = ""

                    cur = db.cursor()
                    gotuncommon = False
                    newcard = 0
                    oldcard = 0
                    alertwaifus = []
                    i = 0
                    bestwaifu = {"rarity":0}
                    for card in cards:
                        cur.execute("SELECT id, name, rarity, series, image FROM waifus WHERE id='" + str(card) + "'")
                        row = cur.fetchone()
                        i += 1
                        if int(row[2]) > 3:
                            alertwaifus.append( {"name":str(row[1]), "rarity":int(row[2]), "image":str(row[4])})

                        if int(row[2]) >= 1:
                            gotuncommon = True
                        if i==6 and not gotuncommon:
                            oldcard = card
                            card = dropCard(1)
                            cur.execute(
                                "SELECT id, name, rarity, series, image FROM waifus WHERE id='" + str(card) + "'")
                            row = cur.fetchone()
                            newcard = card

                        toadd = "[{id}][{rarity}] {name} from {series} - {image}; ".format(id=row[0], rarity=str(
                            config["rarity" + str(row[2]) + "Name"]), name=row[1], series=row[3], image=row[4])
                        if int(row[2]) > 2 and int(row[2]) > int(bestwaifu["rarity"]):
                            bestwaifu = {"name":str(row[1]), "rarity":int(row[2]), "image":str(row[4])}
                        if len(secondcardstring) + len(toadd) > 400:
                            thirdcardstring += toadd
                        elif len(cardstring) + len(toadd) > 400:
                            secondcardstring += toadd
                        else:
                            cardstring += toadd

                        cur.execute("INSERT INTO drops(userid, waifuid) VALUE (%s, %s)", [str(tags['user-id']), str(card)])
                    if not gotuncommon:
                        try:
                            cards.remove(cards[0])
                        except:
                            print("Wow. This shouldn't happen. like srsly.")
                        cards.append(newcard)
                    cards = sorted(cards)
                    openbooster[str(sender).lower()] = cards
                    cur.close()

                    token = ''.join(choice(ascii_letters) for v in range(10))
                    addDisplayToken(token, cards)
                    droplink = "http://waifus.de/booster?token=" + token
                    self.message(channel, "{user}, you open a super booster pack and you get: {droplink}".format(user=str(sender).lower(), droplink=droplink), isWhisper=isWhisper)
                    for w in alertwaifus:
                        threading.Thread(target=sendalert, args=(channel, w, str(tags["display-name"]))).start()

                    return
                if str(args[1]).lower() == "ultimate":
                    if not hasPoints(str(sender).lower(), config["ultimatePackPrice"]):
                        self.message(channel, str(sender).lower() + ", sorry, you don't have enough points to buy an ultimate booster pack. You need " + str(config["ultimatePackPrice"]), isWhisper=isWhisper)
                        return
                    addPoints(str(sender).lower(), 0 - int(config["ultimatePackPrice"]))

                    cards = []
                    i = 0
                    while i < 3:
                        while True:
                            ca = dropCard(ultra=True)
                            if ca not in cards:
                                cards.append(ca)
                                break
                        i += 1

                    cardstring = ""
                    secondcardstring = ""
                    thirdcardstring = ""

                    cur = db.cursor()
                    newcard = 0
                    alertwaifus = []
                    i = 0
                    bestwaifu = {"rarity":0}
                    for card in cards:
                        cur.execute("SELECT id, name, rarity, series, image FROM waifus WHERE id='" + str(card) + "'")
                        row = cur.fetchone()
                        i += 1
                        if int(row[2]) > 3:
                            alertwaifus.append( {"name":str(row[1]), "rarity":int(row[2]), "image":str(row[4])})
                        toadd = "[{id}][{rarity}] {name} from {series} - {image}; ".format(id=row[0], rarity=str(
                            config["rarity" + str(row[2]) + "Name"]), name=row[1], series=row[3], image=row[4])
                        if int(row[2]) > 2 and int(row[2]) > int(bestwaifu["rarity"]):
                            bestwaifu = {"name":str(row[1]), "rarity":int(row[2]), "image":str(row[4])}
                        if len(secondcardstring) + len(toadd) > 400:
                            thirdcardstring += toadd
                        elif len(cardstring) + len(toadd) > 400:
                            secondcardstring += toadd
                        else:
                            cardstring += toadd

                        cur.execute("INSERT INTO drops(userid, waifuid) VALUE (%s, %s)", [str(tags['user-id']), str(card)])

                    cards = sorted(cards)
                    openbooster[str(sender).lower()] = cards
                    cur.close()

                    token = ''.join(choice(ascii_letters) for v in range(10))
                    addDisplayToken(token, cards)
                    droplink = "http://waifus.de/booster?token=" + token
                    self.message(channel, "{user}, you open an ultimate booster pack and you get: {droplink}".format(user=str(sender).lower(), droplink=droplink), isWhisper=isWhisper)
                    for w in alertwaifus:
                        threading.Thread(target=sendalert, args=(channel, w, str(tags["display-name"]))).start()

                    return
                self.message(channel, "Invalid booster type. Try standard, super or ultimate.", isWhisper=isWhisper)
                return
        if str(command).lower() == "trade":
            if len(args) < 2:
                self.message(channel, "Usage: !trade <check/accept/decline> <user> OR !trade <user> <have> <want> [points]", isWhisper=isWhisper)
                return
            if str(args[0]).lower() == "accept":
                if str(args[1]).lower() not in trades.keys() or str(sender).lower() not in trades[str(args[1]).lower()].keys():
                    self.message(channel, "Sorry, " + str(args[1]).lower() + " did not send you a trade. Send one with !trade " + str(args[1]).lower() + " <have> <want> [points]", isWhisper=isWhisper)
                    return
                trade = trades[str(args[1]).lower()][str(sender).lower()]
                want = trade["want"]
                have = trade ["have"]
                tradepoints = int(trade["points"])
                cost = int(config["tradingFee"])
                if trade["payup"] == str(sender).lower():
                    if not hasPoints(str(sender).lower(), cost + int(config["tradingFee"])):
                        self.message(channel, "Sorry, but you cannot cover the fair trading fee.", isWhisper=isWhisper)
                        return
                    if not hasPoints(str(args[1]).lower(), int(config["tradingFee"]) - tradepoints):
                        self.message(channel, "Sorry, but " + str(args[1]).lower() + " can not cover the base trading fee.", isWhisper=isWhisper)
                        return

                else:
                    if not hasPoints(str(args[1]).lower(), tradepoints + int(config["tradingFee"])):
                        self.message(channel, "Sorry, but " + str(args[1]).lower() + " does not have enough points to cover the fair trading fee.", isWhisper=isWhisper)
                        return
                    if not hasPoints(str(sender).lower(), int(config["tradingFee"]) - tradepoints):
                        self.message(channel, "Sorry, but you can not cover the base trading fee.", isWhisper=isWhisper)
                        return
                cur = db.cursor()
                wantconfirmed = False

                cur.execute(
                "SELECT COUNT(*) FROM has_waifu JOIN users on has_waifu.userid = users.id WHERE users.name = '{0}' AND waifuid = '{1}'".format(
                    str(sender).lower(), str(want)))
                if cur.fetchone()[0] >= 1:
                    wantconfirmed = True

                haveconfirmed = False
                cur.execute(
                    "SELECT COUNT(*) FROM has_waifu JOIN users on has_waifu.userid = users.id WHERE users.name = '{0}' AND waifuid = '{1}'".format(
                        str(str(args[1]).lower()).lower(), str(have)))
                if cur.fetchone()[0] >= 1:
                    haveconfirmed = True


                if not wantconfirmed:
                    self.message(channel, "{sender}, you don't have waifu {waifu}, so you can not accept this trade. Deleting it.".format(sender=str(sender), waifu=str(want)), isWhisper=isWhisper)
                    trades[str(args[1]).lower()].pop(str(sender).lower())
                    cur.close()
                    return
                if not haveconfirmed:
                    self.message(channel, "{sender}, {other} doesn't have waifu {waifu}, so you can not accept this trade. Deleting it.".format(sender=str(sender), waifu=str(have), other=str(args[1]).lower()), isWhisper=isWhisper)
                    trades[str(args[1]).lower()].pop(str(sender).lower())
                    cur.close()
                    return

                if int(have) != 0:
                    giveCard(str(sender).lower(), have)
                    id = have
                    cur.execute(
                        "SELECT SUM(amount) AS totalCards FROM has_waifu JOIN users on has_waifu.userid = users.id WHERE users.name = '{0}' AND has_waifu.waifuid = '{1}'".format(
                            str(args[1]).lower(), str(id)))
                    amount = int(cur.fetchone()[0]) or 0
                    if int(amount) == 1:
                        cur.execute("SELECT id FROM users WHERE name='{name}'".format(name=str(args[1]).lower()))
                        userid = cur.fetchone()[0]
                        cur.execute("DELETE FROM has_waifu WHERE waifuid = '{waifuid}' AND userid = '{userid}'".format(
                            userid=userid, waifuid=id))
                    else:
                        cur.execute(
                            "UPDATE has_waifu JOIN users on has_waifu.userid = users.id SET amount = amount-'1' WHERE users.name='{name}' AND waifuid='{id}'".format(
                                name=str(args[1]).lower(), id=str(id)))
                if int(want) != 0:
                    giveCard(str(args[1]).lower(), want)
                    id = want
                    cur.execute(
                        "SELECT SUM(amount) AS totalCards FROM has_waifu JOIN users on has_waifu.userid = users.id WHERE users.name = '{0}' AND has_waifu.waifuid = '{1}'".format(
                            str(sender).lower(), str(id)))
                    amount = int(cur.fetchone()[0]) or 0
                    if int(amount) == 1:
                        cur.execute("SELECT id FROM users WHERE name='{name}'".format(name=str(sender).lower()))
                        userid = cur.fetchone()[0]
                        cur.execute("DELETE FROM has_waifu WHERE waifuid = '{waifuid}' AND userid = '{userid}'".format(
                            userid=userid, waifuid=id))
                    else:
                        cur.execute(
                            "UPDATE has_waifu JOIN users on has_waifu.userid = users.id SET amount = amount-'1' WHERE users.name='{name}' AND waifuid='{id}'".format(
                                name=str(sender).lower(), id=str(id)))


                if trade["payup"] == str(sender).lower():
                    addPoints(str(args[1]).lower(), tradepoints - cost)
                    addPoints(str(sender).lower(), 0 - (tradepoints + cost))
                else:
                    addPoints(str(sender).lower(), tradepoints - cost)
                    addPoints(str(args[1]).lower(), 0 - (tradepoints + cost))
                trades[str(args[1]).lower()].pop(str(sender).lower())
                self.message(channel, "Trade executed!", isWhisper=isWhisper)
                cur.close()
                return
            if str(args[0]).lower() == "decline":
                if str(args[1]).lower() not in trades.keys() or str(sender).lower() not in trades[str(args[1]).lower()].keys():
                    self.message(channel, "Sorry, " + str(args[1]).lower() + " did not send you a trade. Can't decline it. (If you just want to delete a trade you sent, overwrite it with !trade <user> 0 0", isWhisper=isWhisper)
                    return
                trades[str(args[1]).lower()].pop(str(sender).lower())
                self.message(channel, "Trade declined.", isWhisper=isWhisper)
                return
            if str(args[0]).lower() == "check":
                if str(args[1]).lower() not in trades.keys() or str(sender).lower() not in trades[str(args[1]).lower()].keys():
                    self.message(channel, "Sorry, " + str(args[1]).lower() + " did not send you a trade.", isWhisper=isWhisper)
                    return
                trade = trades[str(args[1]).lower()][str(sender).lower()]
                want = trade["want"]
                have = trade["have"]
                points = str(trade["points"])
                wantwaifu = getWaifuById(want)
                havewaifu = getWaifuById(have)
                wantname = wantwaifu["name"]
                havename = havewaifu["name"]
                payer = "they"
                payed = "you"
                if int(wantwaifu["rarity"]) < int(havewaifu["rarity"]):
                    payer = "you"
                    payed = "them"
                self.message(channel, "{other} wants to trade his ({have}) {havename} for your ({want}) {wantname} and {payer} paying {payed} {points} points. Accept it with !trade accept {other}".format(other=str(args[1]).lower(), have=str(have), havename=havename, want=str(want), wantname=wantname, payer=payer, payed=payed, points=points), isWhisper=isWhisper)
                return
            if len(args) != 3 and len(args) != 4:
                self.message(channel, "Usage: !trade <accept/decline> <user> OR !trade <user> <have> <want> [points]", isWhisper=isWhisper)
                return

            other = args[0]
            have = args[1]
            want = args[2]
            try:
                int(args[1])
                int(args[2])
                if len(args) == 4:
                    int(args[3])
            except:
                self.message(channel, "Only numbers/IDs please.", isWhisper=isWhisper)
                return
            maxi = maxWaifuID()
            if int(args[1]) <= 0 or int(args[2]) <= 0 or int(args[1]) > int(maxi) or int(args[1]) > int(maxi):
                self.message(channel, "Invalid ID. Must be a number from 1 to " + str(maxi), isWhisper=isWhisper)
                return

            havewaifu = getWaifuById(have)
            wantwaifu = getWaifuById(want)

            points = 0
            payup = str(sender).lower()
            if havewaifu["rarity"] != wantwaifu["rarity"]:
                if len(args) != 4:
                    self.message(channel, "To trade waifus of different rarities, please append a point value the owner of the lower tier card has to pay to the command to make the trade fair. (see !help)", isWhisper=isWhisper)
                    return
                points = int(args[3])
                highercost = int(config["rarity" + str(max(int(havewaifu["rarity"]), int(wantwaifu["rarity"]))) + "Value"])
                lowercost = int(config["rarity" + str(min(int(havewaifu["rarity"]), int(wantwaifu["rarity"]))) + "Value"])
                costdiff = highercost - lowercost
                mini = int(costdiff/2)
                if points < mini:
                    self.message(channel, "Minimum points to trade this difference in rarity is " + str(mini), isWhisper=isWhisper)
                    return
                if int(wantwaifu["rarity"]) < int(wantwaifu["rarity"]):
                    payup = other

            try:
                sub = trades[str(sender).lower()] or {}
            except:
                sub = {}


            sub[str(other).lower()] = {"have":have, "want":want, "points":points, "payup":payup}
            trades[str(sender).lower()] = sub
            paying = ""
            if points > 0:
                if payup == str(sender).lower():
                    paying = " with you paying them " + str(points) + " points"
                else:
                    paying = " with them paying you " + str(points) + " points"
            self.message(channel, "Offered {other} to trade your {have} for their {want}{paying}".format(other=str(other), have=str(have), want=str(want), paying = paying), isWhisper=isWhisper)
            #print("Trades: " + str(trades))
            return
        if str(command).lower() == "lookup":
            if len(args) != 1:
                self.message(channel, "Usage: !lookup <id>", isWhisper=isWhisper)
                return
            cur = db.cursor()
            cur.execute("SELECT lastLookup FROM users WHERE name = '{0}'".format(str(sender).lower()))
            nextFree = 1800000 + int(cur.fetchone()[0])
            lookupAvailable = nextFree < current_milli_time()
            if lookupAvailable:
                try:
                    if int(args[0]) == 0:
                        self.message(channel, "ID 0 does not exist.", isWhisper=isWhisper)
                        return
                except:
                    print("LUL")
                waifu = getWaifuById(args[0])
                owned = whoHas(args[0])
                if len(owned) > 3:
                    newowned = []
                    i = 0
                    while len(newowned) < 3:
                        newowned.append(owned[i])
                        i += 1
                    newowned.append(str(len(owned) - 3) + " others")
                    owned = newowned
                self.message(channel, '[{id}][{rarity}] {name} from {series} - {link}{owned}'.format(id=str(waifu["id"]),
                                                                                                 rarity=config[
                                                                                                     "rarity" + str(
                                                                                                         waifu["rarity"]) + "Name"],
                                                                                                 name=str(waifu["name"]), series=str(waifu["series"]),
                                                                                             link=str(waifu["image"]), owned = (" - owned by " + ", ".join(owned)) if len(owned) > 0 else " (not dropped so far)" ), isWhisper=isWhisper)
                if str(sender).lower() not in self.myadmins:
                    cur.execute(
                    "UPDATE users SET lastLookup='{timestamp}' WHERE name='{name}'".format(name=str(sender).lower(),
                                                                                         timestamp=current_milli_time()))
            else:
                a = datetime.timedelta(milliseconds=nextFree - current_milli_time(), microseconds=0)
                datestring = "{0}".format(a).split(".")[0]
                self.message(channel, "Sorry, {user}, please wait {t} until you lookup again.".format(user=str(sender), t=datestring), isWhisper=isWhisper)

            cur.close()
            return
        if str(command).lower() == "whisper":
            if followsme(str(sender).lower()):
                self.message("#jtv", "/w {user} This is a test whisper.".format(user=str(sender).lower()), isWhisper=False)
                self.message(channel, "Attempted to send test whisper.", isWhisper=isWhisper)
            else:
                self.message(channel, "{user}, you need to be following me so i can send you whispers!".format(user=str(tags['display-name'])), isWhisper=isWhisper)
            return
        if str(command).lower() == "help":
            self.message(channel, "http://waifus.de/help", isWhisper=isWhisper)
        if str(command).lower() == "alerts":
            if len(args) != 1:
                self.message("Usage: !alerts <setup/test>", isWhisper=isWhisper)
                return
            if str(args[0]).lower() != "test" or str(args[0]).lower() != "setup":
                try:
                    cur = db.cursor()
                    cur.execute("SELECT alertkey FROM channels WHERE name='{user}'".format(user=str(sender).lower()))
                    key = cur.fetchall()[0][0]
                    if key != None and str(args[0]).lower() == "setup":
                        self.message(channel, "{user}, Alerts seem to already be set up for your channel.".format(user=str(sender)), isWhisper=isWhisper)
                        return
                    if key == None and str(args[0]).lower() == "test":
                        self.message(channel, "{user}, Alerts are not yet set up for your channel. Do !alerts setup".format(user=str(sender)), isWhisper=isWhisper)
                        return
                    if str(args[0]).lower() == "test":
                        threading.Thread(target=sendalert, args=(str(sender).lower(), {"name":"Test Alert, please ignore", "rarity":6, "image":"http://t.fuelr.at/k6g"}, str(tags["display-name"]))).start()
                        self.message(channel, "Test Alert sent.", isWhisper=isWhisper)
                        return
                    self.message("#jtv", "/w {user} Please go to the following link and allow access: {link}{user}".format(user=str(sender).lower(), link=str(streamlabsauthurl)), isWhisper=False)
                    cur.close()
                    return
                except:
                    self.message(channel, "The bot is not configured for your channel or other error. Usage: !alerts <test/setup>", isWhisper=isWhisper)
                    print("Error: " + str(sys.exc_info()[0]))
                    cur.close()
                    return
        if str(command).lower() == "followtest" and sender.lower() in self.myadmins:
            self.message(channel, "Attempting to set follow buttons to hdnmarathon and nepnepbot", isWhisper=isWhisper)
            setFollows(["hdnmarathon", "nepnepbot"])
            return
        # if str(command).lower() == "follow" and sender.lower() in self.myadmins:
        #     self.message(channel, "Setting follow Button for hdnmarathon to " + (", ".join(args)))
        #     setFollows(args)
        #     return
        # if str(command).lower() == "title" and sender.lower() in self.myadmins:
        #     self.message(channel, "Setting title for hdnmarathon to " + " ".join(args))
        #     updateTitle(" ".join(args))
        #     return
        # if str(command).lower() == "game" and sender.lower() in self.myadmins:
        #     self.message(channel, "Setting game for hdnmarathon to " + " ".join(args))
        #     updateGame(" ".join(args))
        #     return
        if str(command).lower() == "togglehoraro" and sender.lower() in self.myadmins:

            self.autoupdate = not self.autoupdate
            if self.autoupdate:
                self.message(channel, "Enabled Horaro Auto-update.", isWhisper=isWhisper)
            else:
                self.message(channel, "Disabled Horaro Auto-update.", isWhisper=isWhisper)
            return
        if str(command).lower() == "war":
            cur = db.cursor()
            cur.execute("SELECT * FROM consoleWar")
            r = cur.fetchall()
            # msg = "Console War: "
            msg = "THE CONSOLE WAR HAS BEEN DEDCIDED: "
            for row in r:
                msg += "HDN" + str(row[0]) + " " + str(row[1]) + " "
            msg += "IrisGrin 9001 Comfa 9001 Salutezume 9001"
            self.message(channel, msg, isWhisper=isWhisper)
            cur.close()
            return
        if str(command).lower() == "nepjoin" and sender.lower() in self.myadmins:
            if len(args) != 1:
                self.message(channel, "Usage: !nepjoin <channelname>", isWhisper=isWhisper)
                return
            chan = str(args[0]).replace("'", "")
            if ('#' + chan) in self.mychannels:
                self.message(channel, "Already in that channel!", isWhisper=isWhisper)
                return
            try:
                cur = db.cursor()
                cur.execute("INSERT INTO channels(name) VALUE (%s)", [str(chan)])
                self.join("#" + chan)
                self.mychannels.append('#' + chan)
                self.message("#" + chan, "Hi there!", isWhisper=False)
                self.message(channel, "Joined #" + chan, isWhisper=isWhisper)
                cur.close()
                return
            except:
                self.message(channel, "Tried joining, failed.", isWhisper=isWhisper)
                print("Error: " + str(sys.exc_info()))
                return
        if str(command).lower() == "nepleave" and (sender.lower() in self.myadmins or ("#" + str(sender.lower())) == str(channel)):
            try:
                cur = db.cursor()
                cur.execute("DELETE FROM channels WHERE name = '{channel}'".format(channel=str(channel).replace("#", "")))
                self.mychannels.remove(str(channel))
                self.message(channel, "ByeBye!", isWhisper=False)
                self.part(channel)
                cur.close()
                return
            except:
                self.message(channel, "Tried to leave but failed D:", isWhisper=isWhisper)
                print("Error: " + str(sys.exc_info()))
                return
        if str(command).lower() == "reload" and (sender.lower() in self.myadmins):
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
            cur.close()
            self.message(channel, "Config reloaded.", isWhisper=isWhisper)
            return
        if str(command).lower() == "redeem":
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
                addPoints(sender.lower(), int(pointrow[0]))
                givenPoints = int(pointrow[0])
            waifusadded = []

            for row in waifurows:
                giveCard(sender.lower(), row[0])
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
        if str(command).lower() == "wars":
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
                    if not hasPoints(str(sender).lower(), int(args[3])):
                        self.message(channel, "Sorry, you do not have enough points to invest " + str(args[3]), isWhisper=isWhisper)
                        return
                except:
                    cur.close()
                    self.message(channel, "Sorry, but {} is not a valid number!".format(str(args[3])), isWhisper=isWhisper)
                    return
                addPoints(str(sender).lower(), -1 * int(args[3]))
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
        if str(command).lower() == "upgrade":
            user = str(sender).lower()
            limit = handLimit(user)
            price = int(250 * (math.pow(2, (limit - 7))))
            if len(args) != 1:
                self.message(channel, "{user}, your current hand limit is {limit}. To add a slot for {price} points, use !upgrade buy".format(user=user, limit=str(limit), price=str(price)), isWhisper=isWhisper)
                return
            if args[0] == "buy":
                if hasPoints(user, price):
                    addPoints(user, price * -1)
                    upgradeHand(user)
                    self.message(channel, "Successfully upgraded {user}'s hand for {price} points!".format(user=str(user), price=str(price)), isWhisper=isWhisper)
                    return
                else:
                    self.message(channel, "{user}, you do not have enough points to upgrade your hand - it costs {price}".format(user=str(user), price=str(price)), isWhisper=isWhisper)
                    return
            self.message(channel, "Usage: !upgrade - checks your limit and the price for an upgrade; !upgrade buy - buys an additional slot for your hand", isWhisper=isWhisper)
            return
        if str(command).lower() == "announce":
            if not (sender.lower() in self.myadmins):
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
        if str(command).lower() == "search":
            if len(args) < 1:
                self.message(channel, "Usage: !search <name>", isWhisper=isWhisper)
                return
            cur = db.cursor()
            cur.execute("SELECT lastSearch FROM users WHERE name = '{0}'".format(str(sender).lower()))
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
                if str(sender).lower() not in self.myadmins:
                    cur.execute(
                    "UPDATE users SET lastSearch='{timestamp}' WHERE name='{name}'".format(name=str(sender).lower(),
                                                                                         timestamp=current_milli_time()))
                return
            else:
                a = datetime.timedelta(milliseconds=nextFree - current_milli_time(), microseconds=0)
                datestring = "{0}".format(a).split(".")[0]
                self.message(channel, "Sorry, {user}, please wait {t} until you can search again.".format(user=str(sender), t=datestring), isWhisper=isWhisper)



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
curg.close()


def closeonexit():
    print("Exiting. timestamp: " + str(datetime.datetime.now()))
    import pdb
    pdb.pm()
    db.close()


atexit.register(closeonexit)
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
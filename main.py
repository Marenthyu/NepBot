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
from collections import defaultdict

import sys
import re
import logging

formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s] %(message)s')
logger = logging.getLogger('nepbot')
logger.setLevel(logging.DEBUG)
logger.propagate = False
fh = logging.handlers.TimedRotatingFileHandler('debug.log', when='midnight')
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)

logging.getLogger('tornado.application').addHandler(fh)
logging.getLogger('tornado.application').addHandler(ch)

gamesdict = {'Dark Rose Valkyrie': 'Black Rose Valkyrie', 'Megadimension Neptunia VIIR': 'Megadimension Neptunia VII',
             'Intro': 'Hyperdimension Neptunia'}

ffzws = 'wss://andknuckles.frankerfacez.com'
pool = pydle.ClientPool()
current_milli_time = lambda: int(round(time.time() * 1000))
pymysql.install_as_MySQLdb()
dbpw = None
dbname = None
dbhost = None
dbuser = None
silence = False
debugMode = False
hdnoauth = None
streamlabsclient = None
twitchclientsecret = None
bannedWords = []
t = None
# read config values from file (db login etc)
try:
    f = open("nepbot.cfg", "r")
    lines = f.readlines()
    for line in lines:
        name, value = line.split("=")
        value = str(value).strip("\n")
        logger.info("Reading config value '%s' = '<redacted>'", name)
        if name == "dbpassword":
            dbpw = value
        if name == "database":
            dbname = value
        if name == "dbhost":
            dbhost = value
        if name == "dbuser":
            dbuser = value
        if name == "hdnoauth":
            hdnoauth = value
        if name == "streamlabsclient":
            streamlabsclient = value
        if name == "twitchclientsecret":
            twitchclientsecret = value
        if name == "log":
            logger.info("Setting new console log level to %s", value)
            ch.setLevel(logging.getLevelName(value))
        if name == "silent" and value == "True":
            logger.warning("Silent mode enabled")
            silence = True
        if name == "debugMode" and value == "True":
            logger.warning("Debug mode enabled, !as command is available")
            debugMode = True
        if name == "bannedWords":
            bannedWords = [word.lower() for word in value.split(",")]
    if dbpw is None:
        logger.error("Database password not set. Please add it to the config file, with 'dbpassword=<pw>'")
        sys.exit(1)
    if dbname is None:
        logger.error("Database name not set. Please add it to the config file, with 'database=<name>'")
        sys.exit(1)
    if dbhost is None:
        logger.error("Database host not set. Please add it to the config file, with 'dbhost=<host>'")
        sys.exit(1)
    if dbuser is None:
        logger.error("Database user not set. Please add it to the config file, with 'dbuser=<user>'")
        sys.exit(1)
    if hdnoauth is None:
        logger.error("HDNMarathon Channel oauth not set. Please add it to the conig file, with 'hdnoauth=<pw>'")
        sys.exit(1)
    if twitchclientsecret is None:
        logger.error("Twitch Client Secret not set. Please add it to the conig file, with 'twitchclientsecret=<pw>'")
        sys.exit(1)
    f.close()
except Exception:
    logger.error("Error reading config file (nepbot.cfg), aborting.")
    sys.exit(1)

db = pymysql.connect(host=dbhost, user=dbuser, passwd=dbpw, db=dbname, autocommit="True", charset="utf8mb4")
admins = []
superadmins = []
activitymap = {}
blacklist = []
config = {}
emotewaremotes = []
revrarity = {}
visiblepacks = ""
validalertconfigvalues = []
discordhooks = []

busyLock = threading.Lock()
discordLock = threading.Lock()
streamlabsLock = threading.Lock()
streamlabsauthurl = "https://www.streamlabs.com/api/v1.0/authorize?client_id=" + streamlabsclient + "&redirect_uri=https://marenthyu.de/cgi-bin/waifucallback.cgi&response_type=code&scope=alerts.create&state="
streamlabsalerturl = "https://streamlabs.com/api/v1.0/alerts"
alertheaders = {"Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"}
time_regex = re.compile('(?P<hours>[0-9]*):(?P<minutes>[0-9]{2}):(?P<seconds>[0-9]{2})(\.(?P<ms>[0-9]{1,3}))?')
waifu_regex = None


def loadConfig():
    global revrarity, blacklist, visiblepacks, admins, superadmins, validalertconfigvalues, waifu_regex, emotewaremotes, discordhooks
    with db.cursor() as curg:
        curg.execute("SELECT * FROM config")
        logger.info("Importing config from database")
        for row in curg.fetchall():
            config[row[0]] = row[1]
        logger.debug("Config: %s", str(config))
        if int(config["emoteWarStatus"]) == 1:
            # emote war active, get its emotes
            curg.execute("SELECT name FROM emoteWar")
            emotewaremotes = [row[0] for row in curg.fetchall()]
        else:
            emotewaremotes = []
        alertRarityRange = range(int(config["drawAlertMinimumRarity"]), int(config["numNormalRarities"]))
        validalertconfigvalues = ["color", "alertChannel", "defaultLength", "defaultSound", "setClaimSound", "setClaimLength"] \
            + ["rarity%dLength" % rarity for rarity in alertRarityRange] \
            + ["rarity%dSound" % rarity for rarity in alertRarityRange]
        waifu_regex = re.compile('(\[(?P<id>[0-9]+?)])?(?P<name>.+?) *- *(?P<series>.+) *- *(?P<rarity>[0-' + str(
            int(config["numNormalRarities"]) - 1) + ']) *- *(?P<link>.+?)$')
        logger.debug("Alert config values: %s", str(validalertconfigvalues))
        logger.debug("Waifu regex: %s", str(waifu_regex))
        logger.info("Fetching admin list...")
        curg.execute("SELECT name, super FROM admins")
        admins = []
        superadmins = []
        for row in curg.fetchall():
            admins.append(row[0])
            if row[1] != 0:
                superadmins.append(row[0])
        logger.debug("Admins: %s", str(admins))
        logger.debug("SuperAdmins: %s", str(superadmins))
        revrarity = {config["rarity" + str(i) + "Name"]: i for i in
                     range(int(config["numNormalRarities"]) + int(config["numSpecialRarities"]))}
        curg.execute("SELECT name FROM blacklist")
        rows = curg.fetchall()
        blacklist = []
        for row in rows:
            blacklist.append(row[0])

        # visible packs
        curg.execute("SELECT name FROM boosters WHERE listed = 1 AND buyable = 1 ORDER BY sortIndex ASC")
        packrows = curg.fetchall()
        visiblepacks = "/".join(row[0] for row in packrows)

        # discord hooks
        with discordLock:
            curg.execute("SELECT url FROM discordHooks ORDER BY priority DESC")
            discrows = curg.fetchall()
            discordhooks = [row[0] for row in discrows]


def checkAndRenewAppAccessToken():
    global config, headers
    krakenHeaders = {"Authorization": "OAuth %s" % config["appAccessToken"]}
    r = requests.get("https://api.twitch.tv/kraken", headers=krakenHeaders)
    resp = r.json()

    if "identified" not in resp or not resp["identified"]:
        # app access token has expired, get a new one
        logger.debug("Requesting new token")
        url = 'https://api.twitch.tv/kraken/oauth2/token?client_id=%s&client_secret=%s&grant_type=client_credentials' % (
            config["clientID"], twitchclientsecret)
        r = requests.post(url)
        try:
            jsondata = r.json()
            if 'access_token' not in jsondata or 'expires_in' not in jsondata:
                raise ValueError("Invalid Twitch API response, can't get an app access token.")
            config["appAccessToken"] = jsondata['access_token']
            logger.debug("request done")
            cur = db.cursor()
            cur.execute("UPDATE config SET value = %s WHERE name = 'appAccessToken'", [jsondata['access_token']])
            cur.close()
            headers = {"Authorization": "Bearer %s" % config["appAccessToken"]}
        except ValueError as error:
            logger.error("Access Token renew/get request was not successful")
            raise error


def placeBet(channel, userid, betms):
    cur = db.cursor()
    cur.execute("SELECT id FROM bets WHERE channel = %s AND status = 'open' LIMIT 1", [channel])
    row = cur.fetchone()
    if row is None:
        cur.close()
        return False
    cur.execute("REPLACE INTO placed_bets (betid, userid, bet, updated) VALUE (%s, %s, %s, %s)",
                [row[0], userid, betms, current_milli_time()])
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
    cur.execute(
        "SELECT bet, userid, users.name FROM placed_bets INNER JOIN users ON placed_bets.userid = users.id WHERE betid = %s ORDER BY updated ASC",
        [betid])
    rows = cur.fetchall()
    placements = sorted(rows, key=lambda row: abs(int(row[0]) - timeresult))
    actualwinners = [{"id": row[1], "name": row[2], "bet": row[0], "timedelta": row[0] - timeresult} for row in
                     placements]
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
    affected = cur.execute("UPDATE bets SET status = 'cancelled' WHERE channel = %s AND status IN('open', 'started')",
                           [channel])
    cur.close()
    return affected > 0


def getHand(twitchid):
    try:
        tID = int(twitchid)
    except Exception:
        logger.error("Got non-integer id for getHand. Aborting.")
        return []
    cur = db.cursor()
    cur.execute(
        "SELECT amount, waifus.name, waifus.id, rarity, series, image, base_rarity FROM has_waifu JOIN waifus ON has_waifu.waifuid = waifus.id WHERE has_waifu.userid = %s ORDER BY (rarity < %s) DESC, waifus.id ASC",
        [tID, int(config["numNormalRarities"])])
    rows = cur.fetchall()
    cur.close()
    return [{"name": row[1], "amount": row[0], "id": row[2], "rarity": row[3], "series": row[4], "image": row[5],
             "base_rarity": row[6]} for row in rows]


def search(query, series=None):
    cur = db.cursor()
    if series is None:
        cur.execute("SELECT id, Name, series, base_rarity FROM waifus WHERE can_lookup = 1 AND Name LIKE %s",
                    ["%" + query + "%"])
    else:
        cur.execute(
            "SELECT id, Name, series, base_rarity FROM waifus WHERE can_lookup = 1 AND Name LIKE %s AND series LIKE %s",
            ["%" + query + "%", "%" + series + "%"])
    rows = cur.fetchall()
    ret = []
    for row in rows:
        ret.append({'id': row[0], 'name': row[1], 'series': row[2], 'base_rarity': row[3]})
    return ret


def handLimit(userid):
    with db.cursor() as cur:
        cur.execute("SELECT 7 + paidHandUpgrades + freeUpgrades FROM users WHERE id = %s", [userid])
        res = cur.fetchone()
        limit = int(res[0])

    return limit


def paidHandUpgrades(userid):
    cur = db.cursor()
    cur.execute("SELECT paidHandUpgrades FROM users WHERE id = %s", [userid])
    res = cur.fetchone()
    limit = int(res[0])
    cur.close()
    return limit


def currentCards(userid, verbose=False):
    cur = db.cursor()
    cur.execute(
        "SELECT (SELECT COALESCE(SUM(amount), 0) FROM has_waifu WHERE userid = %s AND rarity < %s), (SELECT COUNT(*) FROM bounties WHERE userid = %s AND status = 'open')",
        [userid, int(config["numNormalRarities"]), userid])
    result = cur.fetchone()
    cur.close()
    if verbose:
        return {"hand": result[0], "bounties": result[1], "total": result[0] + result[1]}
    else:
        return result[0] + result[1]


def upgradeHand(userid, gifted=False):
    cur = db.cursor()
    cur.execute("UPDATE users SET paidHandUpgrades = paidHandUpgrades + %s, freeUpgrades = freeUpgrades + %s WHERE id = %s",
                [0 if gifted else 1, 1 if gifted else 0, userid])
    cur.close()


def attemptBountyFill(bot, waifuid):
    # return profit from the bounty
    with db.cursor() as cur:
        cur.execute(
            "SELECT bounties.id, bounties.userid, users.name, bounties.amount, waifus.name, waifus.base_rarity FROM bounties JOIN users ON bounties.userid = users.id JOIN waifus ON bounties.waifuid = waifus.id WHERE bounties.waifuid = %s AND bounties.status = 'open' ORDER BY bounties.amount DESC LIMIT 1",
            [waifuid])
        order = cur.fetchone()

        if order is not None:
            # fill their order instead of actually disenchanting
            giveCard(order[1], waifuid, order[5])
            bot.message('#%s' % order[2],
                        "Your bounty for [%d] %s for %d points has been filled and they have been added to your hand." % (
                            waifuid, order[4], order[3]), True)
            cur.execute("UPDATE bounties SET status = 'filled', updated = %s WHERE id = %s",
                        [current_milli_time(), order[0]])
            # alert people with lower bounties but above the cap?
            rarity_cap = int(config["rarity" + str(order[5]) + "MaxBounty"])
            cur.execute(
                "SELECT users.name FROM bounties JOIN users ON bounties.userid = users.id WHERE bounties.waifuid = %s AND bounties.status = 'open' AND bounties.amount > %s",
                [waifuid, rarity_cap])
            for userrow in cur.fetchall():
                bot.message('#%s' % userrow[0],
                            "A higher bounty for [%d] %s than yours was filled, so you can now cancel yours and get full points back provided you don't change it." % (
                                waifuid, order[4]), True)
            # give the disenchanter appropriate profit
            base_value = int(config["rarity" + str(order[5]) + "Value"])
            if order[3] > rarity_cap:
                return (order[3] - rarity_cap) // 4 + (rarity_cap - base_value) // 2
            else:
                return max(math.floor((order[3] - base_value) * 0.5), 2)
        else:
            # no bounty
            return 0


def setFavourite(userid, waifu):
    with db.cursor() as cur:
        cur.execute("UPDATE users SET favourite=%s WHERE id = %s", [waifu, userid])


def setDescription(userid, newDesc):
    with db.cursor() as cur:
        cur.execute("UPDATE users SET profileDescription=%s WHERE id = %s", [newDesc, userid])


def getBadgeByID(id):
    logger.debug("Getting badge for id %s", id)
    try:
        id = int(id)
        if id < 1 or id > maxBadgeID():
            logger.debug("ID was smaller than 1 or bigger than max.")
            return None
    except ValueError:
        logger.debug("ValueError, not an int")
        return None
    cur = db.cursor()
    cur.execute("SELECT id, name, description, image FROM badges WHERE id=%s",
                [id])
    row = cur.fetchone()
    ret = {"id": row[0], "name": row[1], "image": row[3], "description": row[2]}
    cur.close()
    logger.debug("Fetched Badge from id: %s", ret)
    return ret


def addBadge(name, description, image):
    """Adds a new Badge to the database"""
    with db.cursor() as cur:
        cur.execute("INSERT INTO badges(name, description, image) VALUES(%s, %s, %s)", [name, description, image])
        return cur.lastrowid


def giveBadge(userid, badge):
    """Gives a user a badge"""
    badgeObj = getBadgeByID(badge)
    if badgeObj is None:
        return False
    else:
        try:
            with db.cursor() as cur:
                cur.execute("INSERT INTO has_badges(userID, badgeID) VALUES(%s, %s)", [userid, badge])
        except:
            logger.debug("Had an error.")
            return False
        return True


def getHoraro():
    "https://horaro.org/-/api/v1/schedules/3911mu51ljb1wf7a5e/ticker"
    r = requests.get(
        "https://horaro.org/-/api/v1/schedules/{horaroid}/ticker?hiddenkey=NepSmug".format(horaroid=config["horaroID"]))
    try:
        j = r.json()
        # ("got horaro ticker: " + str(j))
        return j
    except Exception:
        logger.error("Horaro Error:")
        logger.error(str(r.status_code))
        logger.error(r.text)


def updateBoth(game, title):
    myheaders = headers.copy()
    myheaders["Authorization"] = "OAuth " + str(hdnoauth).replace("oauth:", "")
    myheaders["Content-Type"] = "application/json"
    myheaders["Accept"] = "application/vnd.twitchtv.v5+json"
    body = {"channel": {"status": str(title), "game": str(game)}}
    # print("headers: " + str(myheaders))
    # print("body: " + str(body))
    r = requests.put("https://api.twitch.tv/kraken/channels/143262392", headers=myheaders, json=body)
    try:
        j = r.json()
        # print("tried to update channel title, response: " + str(j))
    except Exception:
        logger.error(str(r.status_code))
        logger.error(r.text)


def updateTitle(title):
    myheaders = headers.copy()
    myheaders["Authorization"] = "OAuth " + str(hdnoauth).replace("oauth:", "")
    myheaders["Content-Type"] = "application/json"
    myheaders["Accept"] = "application/vnd.twitchtv.v5+json"
    body = {"channel": {"status": str(title)}}
    # print("headers: " + str(myheaders))
    # print("body: " + str(body))
    r = requests.put("https://api.twitch.tv/kraken/channels/143262392", headers=myheaders, json=body)
    try:
        j = r.json()
        # print("tried to update channel title, response: " + str(j))
    except Exception:
        logger.error(str(r.status_code))
        logger.error(r.text)


def updateGame(game):
    myheaders = headers.copy()
    myheaders["Authorization"] = "OAuth " + str(hdnoauth).replace("oauth:", "")
    myheaders["Content-Type"] = "application/json"
    myheaders["Accept"] = "application/vnd.twitchtv.v5+json"
    body = {"channel": {"game": str(game)}}
    # print("headers: " + str(myheaders))
    # print("body: " + str(body))
    r = requests.put("https://api.twitch.tv/kraken/channels/143262392", headers=myheaders, json=body)
    try:
        j = r.json()
        # print("tried to update channel title, response: " + str(j))
    except Exception:
        logger.error(str(r.status_code))
        logger.error(r.text)


def sendStreamlabsAlert(channel, data):
    if '#' in channel:
        channel = channel[1:]

    with busyLock:
        with db.cursor() as cur:
            cur.execute("SELECT alertkey FROM channels WHERE name = %s LIMIT 1", [channel])
            tokenRow = cur.fetchone()

    if tokenRow is not None and tokenRow[0] is not None:
        data['access_token'] = tokenRow[0]
        with streamlabsLock:
            try:
                req = requests.post(streamlabsalerturl, headers=alertheaders, json=data)
                if req.status_code != 200:
                    logger.debug("response for streamlabs alert: %s; %s", str(req.status_code), str(req.text))
            except Exception:
                logger.error("Tried to send a Streamlabs alert to %s, but failed." % channel)
                logger.error("Error: %s", str(sys.exc_info()))


def sendDiscordAlert(data):
    with discordLock:
        for url in discordhooks:
            req2 = requests.post(
                url,
                json=data)
            while req2.status_code == 429:
                time.sleep((int(req2.headers["Retry-After"]) / 1000) + 1)
                req2 = requests.post(
                    url,
                    json=data)


def sendDrawAlert(channel, waifu, user, discord=True):
    logger.info("Alerting for waifu %s", str(waifu))
    with busyLock:
        cur = db.cursor()
        # check for first time drop
        first_time = "pulls" in waifu and waifu['pulls'] == 0
        message = "*{user}* drew {first_time}[*{rarity}*] {name}!".format(user=str(user),
                                                                          rarity=str(config["rarity" + str(
                                                                              waifu["base_rarity"]) + "Name"]),
                                                                          name=str(waifu["name"]),
                                                                          first_time=(
                                                                              "the first ever " if first_time else ""))

        chanOwner = str(channel).replace("#", "")
        cur.execute("SELECT config, val FROM alertConfig WHERE channelName = %s", [chanOwner])
        rows = cur.fetchall()

        colorKey = "rarity" + str(waifu["base_rarity"]) + "EmbedColor"
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
        alertSound = defaultSound if str("rarity" + str(waifu["base_rarity"]) + "Sound") not in keys else alertconfig[
            str("rarity" + str(waifu["base_rarity"]) + "Sound")]
        defaultLength = config["alertDuration"] if "defaultLength" not in keys else alertconfig["defaultLength"]
        alertLength = defaultLength if str("rarity" + str(waifu["base_rarity"]) + "Length") not in keys else \
            alertconfig[str("rarity" + str(waifu["base_rarity"]) + "Length")]
        alertColor = "default" if "color" not in keys else alertconfig["color"]

        if "id" in waifu:
            cur.execute("SELECT sound, length FROM waifuAlerts WHERE waifuid=%s", [waifu["id"]])
            rows = cur.fetchall()
            if len(rows) == 1:
                alertLength = int(rows[0][1])
                alertSound = str(rows[0][0])

        alertbody = {"type": alertChannel, "image_href": waifu["image"],
                     "sound_href": alertSound, "duration": int(alertLength), "message": message}
        if alertColor == "rarity":
            alertbody["special_text_color"] = "rgb({r}, {g}, {b})".format(r=str(red), g=str(green), b=str(blue))

        cur.close()

    threading.Thread(target=sendStreamlabsAlert, args=(channel, alertbody)).start()
    if discord:
        # check for first time drop
        rarityName = str(config["rarity" + str(waifu["base_rarity"]) + "Name"])
        discordbody = {"username": "Waifu TCG", "embeds": [
            {
                "title": "A{n} {rarity} waifu has been dropped{first_time}!".format(
                    rarity=rarityName,
                    first_time=(" for the first time" if first_time else ""),
                    n='n' if rarityName[0] in ('a', 'e', 'i', 'o', 'u') else '')
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
                    "url": "https://marenthyu.de"
                }
            }
        ]}
        if colorKey in config:
            discordbody["embeds"][0]["color"] = int(config[colorKey])
            discordbody["embeds"][1]["color"] = int(config[colorKey])
        threading.Thread(target=sendDiscordAlert, args=(discordbody,)).start()


def sendDisenchantAlert(channel, waifu, user):
    # no streamlabs alert for now
    # todo maybe make a b&w copy of the waifu image
    discordbody = {"username": "Waifu TCG", "embeds": [
        {
            "title": "A {rarity} waifu has been disenchanted!".format(
                rarity=str(config["rarity" + str(waifu["base_rarity"]) + "Name"]))
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
                "url": "https://marenthyu.de"
            }
        }
    ]}
    colorKey = "rarity" + str(waifu["base_rarity"]) + "EmbedColor"
    if colorKey in config:
        discordbody["embeds"][0]["color"] = int(config[colorKey])
        discordbody["embeds"][1]["color"] = int(config[colorKey])
    threading.Thread(target=sendDiscordAlert, args=(discordbody,)).start()


def sendPromotionAlert(userid, waifuid, new_rarity):
    with busyLock:
        # check for duplicate alert and don't send it
        # UNLESS this is a promotion to MAX rarity
        if new_rarity != int(config["numNormalRarities"]) - 1:
            with db.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM promotion_alerts_sent WHERE userid = %s AND waifuid = %s AND rarity >= %s",
                    [userid, waifuid, new_rarity])
                result = cur.fetchone()[0]
                if result > 0:
                    return

        # get data necessary for the alert and note that we sent it
        # TODO maybe use display name instead
        waifu = getWaifuById(waifuid)
        with db.cursor() as cur:
            cur.execute("SELECT name FROM users WHERE id = %s", [userid])
            username = cur.fetchone()[0]
            cur.execute("REPLACE INTO promotion_alerts_sent (userid, waifuid, rarity) VALUES(%s, %s, %s)",
                        [userid, waifuid, new_rarity])

    # compile alert
    discordbody = {"username": "Waifu TCG", "embeds": [
        {
            "title": "A waifu has been promoted!",
            "color": int(config["rarity%dEmbedColor" % new_rarity])
        },
        {
            "type": "rich",
            "title": "{user} promoted {name} to {rarity} rarity!".format(user=username, name=waifu["name"],
                                                                         rarity=config[
                                                                             "rarity%dName" % new_rarity]),
            "color": int(config["rarity%dEmbedColor" % new_rarity]),
            "footer": {
                "text": "Waifu TCG by Marenthyu"
            },
            "image": {
                "url": waifu["image"]
            },
            "provider": {
                "name": "Marenthyu",
                "url": "https://marenthyu.de"
            }
        }
    ]}
    threading.Thread(target=sendDiscordAlert, args=(discordbody,)).start()


def naturalJoinNames(names):
    if len(names) == 1:
        return names[0]
    return ", ".join(names[:-1]) + " and " + names[-1]


def sendSetAlert(channel, user, name, waifus, discord=True):
    logger.info("Alerting for set claim %s", name)
    with busyLock:
        with db.cursor() as cur:
            chanOwner = str(channel).replace("#", "")
            cur.execute("SELECT config, val FROM alertConfig WHERE channelName = %s", [chanOwner])
            rows = cur.fetchall()
            
    alertconfig = {row[0]:row[1] for row in rows}
    alertChannel = "donation" if "alertChannel" not in alertconfig else alertconfig["alertChannel"]
    defaultSound = config["alertSound"] if "defaultSound" not in alertconfig else alertconfig["defaultSound"]
    alertSound = defaultSound if "setClaimSound" not in alertconfig else alertconfig["setClaimSound"]
    defaultLength = config["alertDuration"] if "defaultLength" not in alertconfig else alertconfig["defaultLength"]
    alertLength = defaultLength if "setClaimLength" not in alertconfig else alertconfig["setClaimLength"]
    message = "{user} claimed the set {name}!".format(user=user, name=name)
    alertbody = {"type": alertChannel, "sound_href": alertSound, "duration": int(alertLength), "message": message}
    threading.Thread(target=sendStreamlabsAlert, args=(channel, alertbody)).start()

    discordbody = {"username": "Waifu TCG", "embeds": [
        {
            "title": "A set has been completed!",
            "color": int(config["rarity" + str(int(config["numNormalRarities"]) - 1) + "EmbedColor"])
        },
        {
            "type": "rich",
            "title": "{user} gathered {waifus} to complete the set {name}!".format(user=str(user),
                                                                                   waifus=naturalJoinNames(waifus),
                                                                                   name=name),
            "url": "https://twitch.tv/{name}".format(name=str(channel).replace("#", "").lower()),
            "color": int(config["rarity" + str(int(config["numNormalRarities"]) - 1) + "EmbedColor"]),
            "footer": {
                "text": "Waifu TCG by Marenthyu"
            },
            "provider": {
                "name": "Marenthyu",
                "url": "https://marenthyu.de"
            }
        }
    ]}
    if discord:
        threading.Thread(target=sendDiscordAlert, args=(discordbody,)).start()


def followsme(userid):
    try:
        krakenHeaders = {"Authorization": "OAuth %s" % config["appAccessToken"],
                         "Accept": "application/vnd.twitchtv.v5+json"}
        r = requests.get(
            "https://api.twitch.tv/kraken/users/{twitchid}/follows/channels/{myid}".format(twitchid=str(userid),
                                                                                           myid=str(
                                                                                               config["twitchid"])),
            headers=krakenHeaders)
        j = r.json()
        return "channel" in j and "_id" in j["channel"] and int(config["twitchid"]) == int(j["channel"]["_id"])
    except Exception:
        return False


def getWaifuById(id):
    try:
        id = int(id)
        if id < 1 or id > maxWaifuID():
            return None
    except ValueError:
        return None
    cur = db.cursor()
    cur.execute("SELECT id, Name, image, base_rarity, series, can_lookup, pulls, last_pull FROM waifus WHERE id=%s",
                [id])
    row = cur.fetchone()
    ret = {"id": row[0], "name": row[1], "image": row[2], "base_rarity": row[3], "series": row[4], "can_lookup": row[5],
           "pulls": row[6], "last_pull": row[7]}
    cur.close()
    # print("Fetched Waifu from id: " + str(ret))
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


def maxWaifuID():
    cur = db.cursor()
    cur.execute("SELECT MAX(id) FROM waifus")
    ret = int(cur.fetchone()[0])
    cur.close()
    return ret


def maxBadgeID():
    cur = db.cursor()
    cur.execute("SELECT MAX(id) FROM badges")
    ret = int(cur.fetchone()[0])
    cur.close()
    return ret


def getUniqueCards(userid):
    with db.cursor() as cur:
        uniqueRarities = [rarity for rarity in range(int(config["numNormalRarities"])) if
                          int(config["rarity%dMax" % rarity]) == 1]
        if len(uniqueRarities) == 0:
            return []
        else:
            inStr = ",".join(["%s"] * len(uniqueRarities))
            cur.execute("SELECT waifuid FROM has_waifu WHERE userid = %s AND rarity IN ({0})".format(inStr),
                        [userid] + uniqueRarities)
            rows = cur.fetchall()
            return [row[0] for row in rows]


def dropCard(rarity=-1, upgradeChances=None, useEventWeightings=False, allowDowngrades=True, bannedCards=None):
    random.seed()
    if rarity == -1:
        maxrarity = int(config["numNormalRarities"]) - 1
        if upgradeChances is None:
            upgradeChances = [float(config["rarity%dUpgradeChance" % i]) for i in range(maxrarity)]
        else:
            assert len(upgradeChances) == maxrarity
        rarity = 0
        while (rarity < maxrarity):
            if random.random() < upgradeChances[rarity]:
                rarity += 1
            else:
                break
        return dropCard(rarity=rarity, useEventWeightings=useEventWeightings, allowDowngrades=allowDowngrades,
                        bannedCards=bannedCards)
    else:
        with db.cursor() as cur:
            if bannedCards is not None and len(bannedCards) > 0:
                banClause = " AND id NOT IN(" + ",".join(["%s"] * len(bannedCards)) + ")"
            else:
                banClause = ""
                bannedCards = []
            raritymax = int(config["rarity" + str(rarity) + "Max"])
            weighting_column = "(event_weighting*normal_weighting)" if useEventWeightings else "normal_weighting"
            if raritymax > 0:
                cur.execute(
                    "SELECT id FROM waifus WHERE base_rarity = %s{1} AND (SELECT COALESCE(SUM(amount), 0) FROM has_waifu WHERE waifuid = waifus.id) + (SELECT COUNT(*) FROM boosters_cards JOIN boosters_opened ON boosters_cards.boosterid=boosters_opened.id WHERE boosters_cards.waifuid = waifus.id AND boosters_opened.status = 'open') < %s ORDER BY -LOG(1-RAND())/{0} LIMIT 1".format(
                        weighting_column, banClause), [rarity] + bannedCards + [raritymax])
            else:
                cur.execute(
                    "SELECT id FROM waifus WHERE base_rarity = %s{1} ORDER BY -LOG(1-RAND())/{0} LIMIT 1".format(
                        weighting_column, banClause), [rarity] + bannedCards)
            result = cur.fetchone()
            if result is None:
                # no waifus left at this rarity
                logger.info("No droppable waifus left at rarity %d" % rarity)
                if allowDowngrades:
                    return dropCard(rarity=rarity - 1, useEventWeightings=useEventWeightings, bannedCards=bannedCards)
                else:
                    return None
            else:
                return result[0]


def recordPullMetrics(*cards):
    with db.cursor() as cur:
        inString = ",".join(["%s"] * len(cards))
        pullTime = current_milli_time()
        cur.execute(
            "UPDATE waifus SET normal_weighting = normal_weighting / %s, pulls = pulls + 1, last_pull = %s WHERE id IN({0}) AND normal_weighting <= 1".format(
                inString), [float(config["weighting_increase_amount"]), pullTime] + list(cards))
        cur.execute(
            "UPDATE waifus SET normal_weighting = 1, pulls = pulls + 1, last_pull = %s WHERE id IN({0}) AND normal_weighting > 1".format(
                inString), [pullTime] + list(cards))


def giveCard(userid, id, rarity, amount=1):
    with db.cursor() as cur:
        cur.execute("SELECT COALESCE(SUM(amount), 0) FROM has_waifu WHERE userid = %s AND waifuid = %s AND rarity = %s",
                    [userid, id, rarity])
        currentAmount = cur.fetchone()[0]

        if currentAmount != 0:
            cur.execute("UPDATE has_waifu SET amount = amount + %s WHERE userid = %s AND waifuid = %s AND rarity = %s",
                        [amount, userid, id, rarity])
        else:
            cur.execute("INSERT INTO has_waifu(userid, waifuid, rarity, amount) VALUES(%s, %s, %s, %s)",
                        [userid, id, rarity, amount])


def attemptPromotions(*cards):
    promosDone = {}
    with db.cursor() as cur:
        for waifuid in cards:
            while True:
                usersThisCycle = []
                cur.execute(
                    "SELECT userid, rarity, amount FROM has_waifu JOIN waifus ON has_waifu.waifuid = waifus.id WHERE has_waifu.waifuid = %s AND has_waifu.amount > 1 AND waifus.can_promote = 1 ORDER BY has_waifu.rarity ASC, RAND() ASC",
                    [waifuid])
                candidates = cur.fetchall()
                for row in candidates:
                    if row[0] in usersThisCycle:
                        continue

                    userid = row[0]
                    rarity = row[1]
                    amount = row[2]

                    if rarity < int(config["numNormalRarities"]) - 1 and amount >= int(
                            config["rarity%dPromoteAmount" % rarity]):
                        promoteAmount = int(config["rarity%dPromoteAmount" % rarity])
                        amountToMake = amount // promoteAmount

                        # limit check?
                        newRarityLimit = int(config["rarity%dMax" % (rarity + 1)])
                        if newRarityLimit != 0:
                            cur.execute(
                                "SELECT COALESCE(SUM(amount), 0) FROM has_waifu WHERE waifuid = %s AND rarity >= %s",
                                [waifuid, rarity + 1])
                            currentOwned = cur.fetchone()[0]
                            amountToMake = max(min(amountToMake, newRarityLimit - currentOwned), 0)

                        if amountToMake != 0:
                            usersThisCycle.append(userid)
                            leftAtCurrentRarity = amount - (amountToMake * promoteAmount)

                            # fix quantity of current rarity
                            if leftAtCurrentRarity == 0:
                                cur.execute("DELETE FROM has_waifu WHERE userid = %s AND waifuid = %s AND rarity = %s",
                                            [userid, waifuid, rarity])
                            else:
                                cur.execute(
                                    "UPDATE has_waifu SET amount = %s WHERE userid = %s AND waifuid = %s AND rarity = %s",
                                    [leftAtCurrentRarity, userid, waifuid, rarity])

                            # give card(s) at promoted rarity
                            giveCard(userid, waifuid, rarity + 1, amountToMake)

                            # update promosDone
                            if userid not in promosDone:
                                promosDone[userid] = {}
                            if waifuid not in promosDone[userid] or promosDone[userid][waifuid] < rarity + 1:
                                promosDone[userid][waifuid] = rarity + 1

                if len(usersThisCycle) == 0:
                    # nothing changed, we're done
                    break

    # promo alerts
    for user in promosDone:
        for waifu in promosDone[user]:
            if promosDone[user][waifu] >= int(config["promotionAlertMinimumRarity"]):
                threading.Thread(target=sendPromotionAlert, args=(user, waifu, promosDone[user][waifu])).start()


def takeCard(userid, id, rarity, amount=1):
    with db.cursor() as cur:
        cur.execute("SELECT COALESCE(SUM(amount), 0) FROM has_waifu WHERE userid = %s AND waifuid = %s AND rarity = %s",
                    [userid, id, rarity])
        currentAmount = cur.fetchone()[0]
        if currentAmount > amount:
            cur.execute("UPDATE has_waifu SET amount = amount - %s WHERE userid = %s AND waifuid = %s AND rarity = %s",
                        [amount, userid, id, rarity])
        elif currentAmount == amount:
            cur.execute("DELETE FROM has_waifu WHERE userid = %s AND waifuid = %s AND rarity = %s",
                        [userid, id, rarity])
        else:
            raise ValueError(
                "Couldn't remove %d of waifu %s at rarity %s from user %s as they don't own it/that many!" % (
                    amount, str(id), str(rarity), str(userid)))


def logDrop(userid, waifuid, rarity, source, channel, isWhisper):
    trueChannel = "$$whisper$$" if isWhisper else channel
    cur = db.cursor()
    cur.execute("INSERT INTO drops(userid, waifuid, rarity, source, channel, timestamp) VALUES(%s, %s, %s, %s, %s, %s)",
                (userid, waifuid, rarity, source, trueChannel, current_milli_time()))
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


memes = ["🤔", "🏆", "✌", "🌲", "🍀", "🖐", "👌", "🤑", "🤣", "🎄"]


def formatTimeDelta(ms):
    baseRepr = str(datetime.timedelta(milliseconds=ms, microseconds=0))
    output = baseRepr[:-3] if "." in baseRepr else baseRepr
    if "memeMode" in config and config["memeMode"] == "meme":
        for i in range(10):
            output = output.replace(str(i), memes[i])
    return output


def parseRarity(input):
    try:
        rarity = int(input)
    except Exception:
        if input.lower() in revrarity:
            rarity = revrarity[input.lower()]
        else:
            raise ValueError(input)
    if rarity < 0 or rarity >= int(config["numNormalRarities"]) + int(config["numSpecialRarities"]):
        raise ValueError(input)
    return rarity


def parseBetTime(input):
    match = time_regex.fullmatch(input)
    if not match:
        return None

    bet = match.groupdict()
    if bet["ms"] is None:
        bet["ms"] = "0"
    ms = int(bet["ms"] + ("0" * max(3 - len(bet["ms"]), 0)))
    total = int(bet["hours"]) * 3600000 + int(bet["minutes"]) * 60000 + int(bet["seconds"]) * 1000 + ms
    return {"hours": total // 3600000, "minutes": (total // 60000) % 60, "seconds": (total // 1000) % 60,
            "ms": total % 1000, "total": total}


class CardNotInHandException(Exception):
    pass


class CardRarityNotInHandException(CardNotInHandException):
    pass


class AmbiguousRarityException(Exception):
    pass


# given a string specifying a card id + optional rarity, return id+rarity of the hand card matching it
# throw various exceptions for invalid format / card not in hand / ambiguous rarity
def parseHandCardSpecifier(hand, specifier):
    if "-" in specifier:
        id = int(specifier.split("-", 1)[0])
        rarity = parseRarity(specifier.split("-", 1)[1])

        foundID = False

        for waifu in hand:
            if waifu['id'] == id and waifu['rarity'] == rarity:
                # done
                return {"id": id, "base_rarity": waifu['base_rarity'], "rarity": rarity}
            elif waifu['id'] == id:
                foundID = True

        if foundID:
            raise CardRarityNotInHandException()
        else:
            raise CardNotInHandException()
    else:
        id = int(specifier)
        rarity = None
        base_rarity = None
        for waifu in hand:
            if waifu['id'] == id:
                if rarity is None:
                    rarity = waifu['rarity']
                    base_rarity = waifu['base_rarity']
                else:
                    raise AmbiguousRarityException()

        if rarity is None:
            raise CardNotInHandException()
        else:
            return {"id": id, "base_rarity": base_rarity, "rarity": rarity}


class InvalidBoosterException(Exception):
    pass


class CantAffordBoosterException(Exception):
    def __init__(self, cost):
        super(CantAffordBoosterException, self).__init__()
        self.cost = cost


def getPackStats(userid):
    with db.cursor() as cur:
        cur.execute(
            "SELECT bo.boostername, COUNT(*), SUM(IF(bo.paid > 0, bo.paid, boosters.cost)) FROM (SELECT * FROM boosters_opened WHERE userid = %s UNION SELECT * FROM archive_boosters_opened WHERE userid = %s) AS bo JOIN boosters ON bo.boostername = boosters.name WHERE boosters.cost > 0 GROUP BY bo.boostername ORDER BY COUNT(*) DESC",
            [userid] * 2)
        packstats = cur.fetchall()
        return packstats

def getSpendings(userid):
    with db.cursor() as cur:
        cur.execute("SELECT spending FROM users WHERE id = %s", [userid])
        result = cur.fetchall()
        return int(result[0][0])

def getHandUpgradeLUT():
    with db.cursor() as cur:
        cur.execute("SELECT slot, spendings FROM handupgrades")
        lut = cur.fetchall()
        return lut

def getNextUpgradeSpendings(userid):

    lut = getHandUpgradeLUT()
    currSlots = paidHandUpgrades(userid)
    paidSlots = currSlots

    nextSpendings = 0

    while currSlots >= len(lut) - 1:
        currSlots -= 1
        nextSpendings += 1000000

    nextSpendings += lut[currSlots+1][1]
    return nextSpendings

def checkHandUpgrade(userid):
    userid = int(userid)
    nextSpendings = getNextUpgradeSpendings(userid)
    spendings = getSpendings(userid)

    logger.debug("next spendings: %d", nextSpendings)
    logger.debug("current spendings: %d", spendings)


    if spendings >= nextSpendings:
        upgradeHand(userid)
        logger.debug("Upgraded Hand for %d", userid)
        return True
    return False
    
def messageForHandUpgrade(userid, username, bot, channel, isWhisper):
    bot.message(channel, "%s, you just got a new hand space from booster spending! naroYay" % username, isWhisper)


def addSpending(userid, amount):
    with db.cursor() as cur:
        cur.execute("UPDATE users SET spending=spending + %s WHERE id = %s", [amount, userid])


def openBooster(userid, username, channel, isWhisper, packname, buying=True):
    with db.cursor() as cur:
        rarityColumns = ", ".join(
            "rarity" + str(i) + "UpgradeChance" for i in range(int(config["numNormalRarities"]) - 1))

        if buying:
            cur.execute(
                "SELECT listed, buyable, cost, numCards, guaranteeRarity, guaranteeCount, useEventWeightings, " + rarityColumns + " FROM boosters WHERE name = %s AND buyable = 1",
                [packname])
        else:
            cur.execute(
                "SELECT listed, buyable, cost, numCards, guaranteeRarity, guaranteeCount, useEventWeightings, " + rarityColumns + " FROM boosters WHERE name = %s",
                [packname])

        packinfo = cur.fetchone()

        if packinfo is None:
            raise InvalidBoosterException()

        listed = packinfo[0]
        buyable = packinfo[1]
        cost = packinfo[2]
        numCards = packinfo[3]
        pgRarity = packinfo[4]
        pgCount = packinfo[5]
        useEventWeightings = packinfo[6] != 0
        normalChances = packinfo[7:]

        if buying:
            if not hasPoints(userid, cost):
                raise CantAffordBoosterException(cost)

            addPoints(userid, -cost)

        minScalingRarity = int(config["pullScalingMinRarity"])
        maxScalingRarity = int(config["pullScalingMaxRarity"])
        numScalingRarities = maxScalingRarity - minScalingRarity + 1
        scalingThresholds = [int(config["pullScalingRarity%dThreshold" % rarity]) for rarity in
                             range(minScalingRarity, maxScalingRarity + 1)]

        cur.execute("SELECT pullScalingData FROM users WHERE id = %s", [userid])
        scalingRaw = cur.fetchone()[0]
        if scalingRaw is None:
            scalingData = [0] * numScalingRarities
        else:
            scalingData = [int(n) for n in scalingRaw.split(':')]

        cards = []
        alertwaifus = []
        uniques = getUniqueCards(userid)
        for i in range(numCards):
            # scale chances of the card appropriately
            currentChances = list(normalChances)
            guaranteedRarity = 0
            if listed and buyable:
                for rarity in range(maxScalingRarity, minScalingRarity - 1, -1):
                    scaleIdx = rarity - minScalingRarity
                    if scalingData[scaleIdx] >= scalingThresholds[scaleIdx] * 2:
                        # guarantee this rarity drops now
                        if rarity == int(config["numNormalRarities"]) - 1:
                            currentChances = [1] * len(currentChances)
                        else:
                            currentChances = ([1] * rarity) + [
                                functools.reduce((lambda x, y: x * y), currentChances[:rarity + 1])] + list(
                                currentChances[rarity + 1:])
                        guaranteedRarity = rarity
                        break
                    elif scalingData[scaleIdx] > scalingThresholds[scaleIdx]:
                        # make this rarity more likely to drop
                        oldPromoChance = currentChances[rarity - 1]
                        currentChances[rarity - 1] = min(currentChances[rarity - 1] * (
                                (scalingData[scaleIdx] / scalingThresholds[scaleIdx] - 1) * 2 + 1), 1)
                        if rarity != int(config["numNormalRarities"]) - 1:
                            # make rarities above this one NOT more likely to drop
                            currentChances[rarity] /= currentChances[rarity - 1] / oldPromoChance

            # account for minrarity for some cards in the pack
            if i < pgCount and pgRarity > guaranteedRarity:
                if pgRarity == int(config["numNormalRarities"]) - 1:
                    currentChances = [1] * len(currentChances)
                else:
                    currentChances = ([1] * pgRarity) + [
                        functools.reduce((lambda x, y: x * y), currentChances[:pgRarity + 1])] + list(
                        currentChances[pgRarity + 1:])

            logger.debug("using odds for card %d: %s", i, str(currentChances))

            # actually drop the card
            card = int(dropCard(upgradeChances=currentChances, useEventWeightings=useEventWeightings,
                                bannedCards=uniques + cards))
            cards.append(card)

            # check its rarity and adjust scaling data
            waifu = getWaifuById(card)

            if waifu['base_rarity'] >= int(config["drawAlertMinimumRarity"]):
                alertwaifus.append(waifu)

            if listed and buyable:
                for r in range(numScalingRarities):
                    if r + minScalingRarity != waifu['base_rarity']:
                        scalingData[r] += cost / numCards
                    else:
                        scalingData[r] = 0

            logDrop(str(userid), str(card), waifu['base_rarity'], "boosters.%s" % packname, channel, isWhisper)

        cards.sort()
        recordPullMetrics(*cards)
        addSpending(userid, cost)

        # pity pull data update
        cur.execute("UPDATE users SET pullScalingData = %s WHERE id = %s",
                    [":".join(str(round(n)) for n in scalingData), userid])

        # insert opened booster
        cur.execute(
            "INSERT INTO boosters_opened (userid, boostername, paid, created, status) VALUES(%s, %s, %s, %s, 'open')",
            [userid, packname, cost if buying else 0, current_milli_time()])
        boosterid = cur.lastrowid
        cur.executemany("INSERT INTO boosters_cards (boosterid, waifuid) VALUES(%s, %s)",
                        [(boosterid, card) for card in cards])

        # alerts
        for w in alertwaifus:
            threading.Thread(target=sendDrawAlert, args=(channel, w, str(username))).start()

        return boosterid


def infoCommandAvailable(userid, username, displayName, bot, channel, isWhisper):
    with db.cursor() as cur:
        private = isWhisper or channel == '#' + config['username'] or channel == '#' + username
        columnName = "Private" if private else "Public"
        cur.execute("SELECT infoUsed{0}, infoLastReset{0} FROM users WHERE id = %s".format(columnName), [userid])
        limitData = list(cur.fetchone())

        timeUntilReset = limitData[1] - (current_milli_time() - int(config["infoResetPeriod"]) * 60000)

        if timeUntilReset <= 0:
            limitData[0] = 0
            cur.execute("UPDATE users SET infoUsed{0} = 0, infoLastReset{0} = %s WHERE id = %s".format(columnName),
                        [current_milli_time(), userid])

        limit = int(config["infoLimit%s" % columnName])
        if limitData[0] < limit:
            return True
        else:
            timeDiff = formatTimeDelta(timeUntilReset)
            if private:
                bot.message(channel,
                            "%s, you have hit the rate limit for info commands. Please wait %s to use more." % (
                                displayName, timeDiff), isWhisper)
            else:
                bot.message(channel,
                            "%s, you have hit the rate limit for info commands in public chats. Please wait %s to use more or use them via whisper or in the bot's own chat." % (
                                displayName, timeDiff), isWhisper)
            return False


def useInfoCommand(userid, username, channel, isWhisper):
    with db.cursor() as cur:
        private = isWhisper or channel == '#' + config['username'] or channel == '#' + username
        columnName = "Private" if private else "Public"
        cur.execute("UPDATE users SET infoUsed{0} = infoUsed{0} + 1 WHERE id = %s".format(columnName), [userid])


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
    instance = None
    autoupdate = False
    pw = None
    nomodalerted = []
    addchannels = []
    leavechannels = []
    emotecooldowns = {}

    def __init__(self, config, channels):
        super().__init__(config["username"])
        self.config = config
        self.mychannels = channels
        NepBot.instance = self

    def on_clearchat(self, message):
        # print("Got clear chat message: " + str(message))
        nick, metadata = self._parse_user(message.source)
        tags = message.tags
        params = message.params
        logger.debug(
            "nick: {nick}; metadata: {metadata}; params: {params}; tags: {tags}".format(nick=nick, metadata=metadata,
                                                                                        params=params, tags=tags))
        if len(params) == 1:
            logger.info("Chat in %s has been cleared by a moderator.", params[0])
            return
        u = params[1]
        chan = params[0]
        reason = "" if "ban-reason" not in tags else str(tags["ban-reason"]).replace("\\s", " ")
        if "ban-duration" in tags.keys():
            duration = tags["ban-duration"]
            logger.info("%s got timed out for %s seconds in %s for: %s", u, duration, chan, reason)
        else:
            logger.info("%s got permanently banned from %s. Reason: %s", u, chan, reason)
        return

    def on_hosttarget(self, message):
        # print("Got Host Target: " + str(message))
        parts = str(message).split(" ")
        sourcechannel = parts[2].strip("#")
        target = parts[3].strip(":")
        if target == "-":
            logger.info("%s has stopped hosting", sourcechannel)
        else:
            logger.info("%s is now hosting %s", sourcechannel, target)
        return

    def on_userstate(self, message):
        # print("Userstate...")
        nick, metadata = self._parse_user(message.source)
        tags = message.tags
        params = message.params
        logger.debug(
            "nick: {nick}; metadata: {metadata}; params: {params}; tags: {tags}".format(nick=nick, metadata=metadata,
                                                                                        params=params, tags=tags))
        if config["username"].lower() == "nepnepbot" and tags["display-name"] == "Nepnepbot" and params[
            0] != "#nepnepbot" and tags["mod"] != '1' and params[0] not in self.nomodalerted:
            logger.info("No Mod in %s!", str(params[0]))
            self.nomodalerted.append(params[0])
            self.message(params[0], "Hey! I noticed i am not a mod here! Please do mod me to avoid any issues!")
        return

    def on_roomstate(self, message):
        # print("Got Room State: " + str(message))
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
        if str(message).find("WHISPER") > -1:
            self.on_whisper(message)
            return
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
            logger.info("PogChamp! Someone subbed to someone! here's the message: %s", str(message))
            return
        super().on_unknown(message)

    def start(self, password):
        pool.connect(self, "irc.twitch.tv", 6667, tls=False, password=password)
        self.pw = password
        logger.info("Connecting...")

        def timer():
            with busyLock:
                global t
                t = Timer(int(config["cycleLength"]), timer)
                t.start()
                logger.debug("Refreshing Database Connection...")
                global db
                try:
                    db.close()
                except Exception:
                    logger.warning("Error closing db connection cleanly, ignoring.")
                try:
                    db = pymysql.connect(host=dbhost, user=dbuser, passwd=dbpw, db=dbname, autocommit="True",
                                         charset="utf8mb4")
                except Exception:
                    logger.error("Error Reconnecting to DB. Skipping Timer Cycle.")
                    return
                with db.cursor() as cur:
                    # open packs?
                    cur.execute(
                        "SELECT boosters_opened.id, boosters_opened.userid, users.name FROM boosters_opened JOIN users ON boosters_opened.userid = users.id WHERE status = 'open' AND created <= %s",
                        [current_milli_time() - int(config["boosterTimeout"])])
                    packsToClose = cur.fetchall()
                    for pack in packsToClose:
                        userid = pack[1]
                        cur.execute("SELECT waifuid FROM boosters_cards WHERE boosterid = %s ORDER BY waifuid ASC",
                                    [pack[0]])
                        cardIDs = [row[0] for row in cur.fetchall()]
                        cards = [getWaifuById(card) for card in cardIDs]
                        # keep the best cards
                        cards.sort(key=lambda waifu: -waifu['base_rarity'])
                        numKeep = int(min(max(handLimit(userid) - currentCards(userid), 0), len(cards)))
                        keeps = cards[:numKeep]
                        des = cards[numKeep:]
                        logger.info("Expired pack for user %s (%d): keeping %s, disenchanting %s", pack[2], userid,
                                    str(keeps), str(des))
                        for waifu in keeps:
                            giveCard(userid, waifu['id'], waifu['base_rarity'])
                        gottenpoints = 0
                        for waifu in des:
                            baseValue = int(config["rarity" + str(waifu['base_rarity']) + "Value"])
                            profit = attemptBountyFill(self, waifu['id'])
                            gottenpoints += baseValue + profit
                        addPoints(userid, gottenpoints)
                        attemptPromotions(*cardIDs)
                        cur.execute("UPDATE boosters_opened SET status='closed', updated = %s WHERE id = %s",
                                    [current_milli_time(), pack[0]])

                    # increase weightings
                    if int(config["last_weighting_update"]) < current_milli_time() - int(
                            config["weighting_increase_cycle"]):
                        logger.debug("Increasing card weightings...")
                        cur.execute("UPDATE waifus SET normal_weighting = normal_weighting * %s WHERE base_rarity < %s",
                                    [float(config["weighting_increase_amount"]), int(config["numNormalRarities"])])
                        config["last_weighting_update"] = str(current_milli_time())
                        cur.execute("UPDATE config SET value = %s WHERE name = 'last_weighting_update'",
                                    [config["last_weighting_update"]])
            logger.debug("Checking live status of channels...")
            checkAndRenewAppAccessToken()

            with busyLock:
                cur = db.cursor()
                cur.execute("SELECT users.name, users.id FROM channels JOIN users ON channels.name = users.name")
                rows = cur.fetchall()
                cur.close()

            channelids = []
            idtoname = {}
            isLive = {}
            viewerCount = {}
            for row in rows:
                channelids.append(str(row[1]))
                idtoname[str(row[1])] = row[0]
                isLive[str(row[0])] = False

            while len(channelids) > 0:
                currentSlice = channelids[:100]
                response = requests.get("https://api.twitch.tv/helix/streams", headers=headers,
                                        params={"type": "live", "user_id": currentSlice})
                data = response.json()["data"]
                for element in data:
                    chanName = idtoname[str(element["user_id"])]
                    isLive[chanName] = True
                    logger.debug("%s is live!", idtoname[str(element["user_id"])])
                    viewerCount[chanName] = element["viewer_count"]
                channelids = channelids[100:]

            logger.debug("Catching all viewers...")
            for c in self.addchannels:
                self.mychannels.append(c)
            self.addchannels = []
            for c in self.leavechannels:
                try:
                    self.mychannels.remove(c)
                except Exception:
                    logger.warning("Couldn't remove channel %s from channels, it wasn't found. Channel list: %s",
                                   str(c), str(self.mychannels))
            self.leavechannels = []
            try:
                # print("Activitymap: " + str(activitymap))
                doneusers = []
                validactivity = []
                for channel in self.channels:
                    # print("Fetching for channel " + str(channel))
                    channelName = str(channel).replace("#", "")
                    try:
                        a = []
                        if channelName in viewerCount and viewerCount[channelName] >= 800:
                            logger.debug("%s had more than 800 viewers, catching from chatters endpoint", channelName)
                            with urllib.request.urlopen(
                                    'https://tmi.twitch.tv/group/user/' + channelName + '/chatters') as response:
                                data = json.loads(response.read().decode())
                                chatters = data["chatters"]
                                a = chatters["moderators"] + chatters["staff"] + chatters["admins"] + chatters[
                                    "global_mods"] + chatters["viewers"]
                        else:
                            for viewer in self.channels[channel]['users']:
                                a.append(viewer)
                        for viewer in a:
                            if viewer not in doneusers:
                                doneusers.append(viewer)
                            if isLive[channelName] and viewer not in validactivity:
                                validactivity.append(viewer)
                    except Exception:
                        logger.error("Error fetching chatters for %s, skipping their chat for this cycle" % channelName)
                        logger.error("Error: %s", str(sys.exc_info()))

                # process all users
                logger.debug("Caught users, giving points and creating accounts, amount to do = %d" % len(doneusers))
                newUsers = []

                maxPointsInactive = int(config["maxPointsInactive"])
                overflowPoints = 0

                while len(doneusers) > 0:
                    currentSlice = doneusers[:100]
                    with busyLock:
                        cur = db.cursor()
                        cur.execute("SELECT name, points, lastActiveTimestamp FROM users WHERE name IN(%s)" % ",".join(
                            ["%s"] * len(currentSlice)), currentSlice)

                        foundUsersData = cur.fetchall()
                        cur.close()
                    foundUsers = [row[0] for row in foundUsersData]
                    newUsers += [user for user in currentSlice if user not in foundUsers]
                    if len(foundUsers) > 0:
                        updateData = []
                        for viewerInfo in foundUsersData:
                            pointGain = int(config["passivePoints"])
                            if viewerInfo[0] in activitymap and viewerInfo[0] in validactivity:
                                pointGain += max(10 - int(activitymap[viewerInfo[0]]), 0)
                            pointGain = int(pointGain * float(config["pointsMultiplier"]))
                            if viewerInfo[2] is None:
                                maxPointGain = max(maxPointsInactive - viewerInfo[1], 0)
                                if pointGain > maxPointGain:
                                    overflowPoints += pointGain - maxPointGain
                                    pointGain = maxPointGain
                            if pointGain > 0:
                                updateData.append((pointGain, viewerInfo[0]))

                        with busyLock:
                            cur = db.cursor()
                            cur.executemany("UPDATE users SET points = points + %s WHERE name = %s", updateData)
                            cur.close()

                    doneusers = doneusers[100:]

                if overflowPoints > 0:
                    logger.debug("Paying %d overflow points to the bot account" % overflowPoints)
                    with busyLock:
                        cur = db.cursor()
                        cur.execute("UPDATE users SET points = points + %s WHERE name = %s",
                                    [overflowPoints, config["username"]])
                        cur.close()

                # now deal with user names that aren't already in the DB
                if len(newUsers) > 10000:
                    logger.warning(
                        "DID YOU LET ME JOIN GDQ CHAT OR WHAT?!!? ... capping new user accounts at 10k. Sorry, bros!")
                    newUsers = newUsers[:10000]
                while len(newUsers) > 0:
                    currentSlice = newUsers[:100]
                    r = requests.get("https://api.twitch.tv/helix/users", headers=headers,
                                     params={"login": currentSlice})
                    if r.status_code == 429:
                        logger.warning("Rate Limit Exceeded! Skipping account creation!")
                        r.raise_for_status()
                    j = r.json()
                    if "data" not in j:
                        # error, what do?
                        r.raise_for_status()

                    currentIdMapping = {int(row["id"]): row["login"] for row in j["data"]}
                    with busyLock:
                        cur = db.cursor()
                        cur.execute("SELECT id FROM users WHERE id IN(%s)" % ",".join(["%s"] * len(currentIdMapping)),
                                    [id for id in currentIdMapping])
                        foundIdsData = cur.fetchall()
                        cur.close()
                    localIds = [row[0] for row in foundIdsData]

                    # users to update the names for (id already exists)
                    updateNames = [(currentIdMapping[id], id) for id in currentIdMapping if id in localIds]
                    if len(updateNames) > 0:
                        with busyLock:
                            cur = db.cursor()
                            cur.executemany("UPDATE users SET name = %s WHERE id = %s", updateNames)
                            cur.close()

                    # new users (id does not exist)
                    newAccounts = [(id, currentIdMapping[id]) for id in currentIdMapping if id not in localIds]
                    if len(newAccounts) > 0:
                        with busyLock:
                            cur = db.cursor()
                            cur.executemany("INSERT INTO users (id, name, points, lastFree) VALUES(%s, %s, 0, 0)",
                                            newAccounts)
                            cur.close()
                    # actually give points
                    updateData = []
                    for id in currentIdMapping:
                        viewer = currentIdMapping[id]
                        pointGain = int(config["passivePoints"])
                        if viewer in activitymap and viewer in validactivity:
                            pointGain += max(10 - int(activitymap[viewer]), 0)
                        pointGain = int(pointGain * float(config["pointsMultiplier"]))
                        updateData.append((pointGain, viewer))

                    with busyLock:
                        cur = db.cursor()
                        cur.executemany("UPDATE users SET points = points + %s WHERE name = %s", updateData)
                        cur.close()

                    # done with this slice
                    newUsers = newUsers[100:]

                for user in activitymap:
                    activitymap[user] = activitymap[user] + 1
            except Exception:
                logger.warning("We had an error during passive point gain. skipping this cycle.")
                logger.warning("Error: ", str(sys.exc_info()))

            if self.autoupdate:
                logger.debug("Updating Title and Game with horaro info")
                schedule = getHoraro()
                try:
                    data = schedule["data"]
                    ticker = data["ticker"]
                    current = ticker["current"]
                    wasNone = False
                    if current is None:
                        current = ticker["next"]
                        wasNone = True
                    current = current["data"]
                    game = current[0]
                    category = current[2]
                    runners = [runner for runner in current[3:] if runner is not None]
                    args = {"game": game}
                    args["category"] = " (%s)" % category if category is not None else ""
                    args["comingup"] = "COMING UP: " if wasNone else ""
                    args["runners"] = (" by " + ", ".join(runners)) if len(runners) > 0 else ""
                    title = "{comingup}HDNMarathon mk2 - {game}{category}{runners} - !mk2 in chat".format(**args)

                    updateBoth(gamesdict[game] if game in gamesdict else game, title=title)
                except Exception:
                    logger.warning("Error updating from Horaro. Skipping this cycle.")
                    logger.warning("Error: ", str(sys.exc_info()))

            if config["marathonHelpAutopost"] == 'on':
                nextPost = int(config["marathonHelpAutopostLast"]) + int(config["marathonHelpAutopostPeriod"]) * 1000
                if nextPost <= current_milli_time():
                    self.message(config["marathonChannel"], config["marathonHelpCommandText"], False)
                    config["marathonHelpAutopostLast"] = str(current_milli_time())
                    with busyLock:
                        with db.cursor() as cur:
                            cur.execute("UPDATE config SET value = %s WHERE name = 'marathonHelpAutopostLast'",
                                        [config["marathonHelpAutopostLast"]])

        if t is None:
            timer()

    def on_capability_twitch_tv_membership_available(self, nothing=None):
        logger.debug("WE HAS TWITCH MEMBERSHIP AVAILABLE!")
        return True

    def on_capability_twitch_tv_membership_enabled(self, nothing=None):
        logger.debug("WE HAS TWITCH MEMBERSHIP ENABLED!")
        return

    def on_capability_twitch_tv_tags_available(self, nothing=None):
        logger.debug("WE HAS TAGS AVAILABLE!")
        return True

    def on_capability_twitch_tv_tags_enabled(self, nothing=None):
        logger.debug("WE HAS TAGS ENABLED!")
        return

    def on_capability_twitch_tv_commands_available(self, nothing=None):
        logger.debug("WE HAS COMMANDS AVAILABLE!")
        return True

    def on_capability_twitch_tv_commands_enabled(self, nothing=None):
        logger.debug("WE HAS COMMANDS ENABLED!")
        return

    def on_disconnect(self, expected):
        logger.error("Disconnected, reconnecting. Was it expected? %s", str(expected))
        pool.connect(self, "irc.twitch.tv", 6667, tls=False, password=self.pw, reconnect=True)

    def on_connect(self):
        logger.info("Connected! joining channels...")
        super().on_connect()
        for channel in self.mychannels:
            channel = channel.lower()
            logger.debug("Joining %s...", channel)
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
            logger.debug("whisper: %s, %s", str(target), message)
        else:
            logger.debug("message: %s, %s, %s", str(source), str(target), message)
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

        activeCommands = ["checkhand", "points", "freewaifu", "de", "disenchant", "buy", "booster", "trade", "lookup",
                          "alerts", "redeem", "upgrade", "search", "promote", "bet", "sets", "set", "giveaway",
                          "bounty", "emotewar", "wars", "war", "vote", "profile"]

        if sender not in blacklist and "bot" not in sender:
            activitymap[sender] = 0
            activitymap[channelowner] = 0
            with busyLock:
                with db.cursor() as cur:
                    # War?
                    if int(config["emoteWarStatus"]) == 1:
                        if sender not in self.emotecooldowns:
                            self.emotecooldowns[sender] = defaultdict(int)
                        for emote in emotewaremotes:
                            if emote in message and self.emotecooldowns[sender][emote] <= current_milli_time() - 60000:
                                cur.execute("UPDATE emoteWar SET `count` = `count` + 1 WHERE name = %s", [emote])
                                self.emotecooldowns[sender][emote] = current_milli_time()

                    cur.execute("SELECT name FROM users WHERE id = %s", [tags['user-id']])
                    user = cur.fetchone()
                    if user is None:
                        cur.execute("INSERT INTO users (id, name, points) VALUE (%s, %s, %s)",
                                    [tags['user-id'], sender, 0])
                        logger.info("%s didn't have an account, created it.", tags['display-name'])
                    elif user[0] != sender:
                        logger.info("%s got a new name, changing it to: %s", user[0], sender)
                        cur.execute("UPDATE users SET name = %s WHERE id = %s", [sender, tags['user-id']])

            if message.startswith("!"):
                parts = message.split()
                command = parts[0][1:].lower()
                if command in activeCommands:
                    with busyLock:
                        with db.cursor() as cur:
                            cur.execute(
                                "UPDATE users SET lastActiveTimestamp = %s, lastActiveChannel = %s WHERE id = %s",
                                [current_milli_time(), "$$whisper$$" if isWhisper else source, tags['user-id']])
                self.do_command(command, parts[1:], target, source, tags, isWhisper=isWhisper)
        elif message.startswith("!") and message.split()[0][1:].lower() in activeCommands:
            self.message(source, "Bad Bot. No. (account banned from playing TCG)", isWhisper)
            return

    def message(self, channel, message, isWhisper=False):
        logger.debug("sending message %s %s %s" % (channel, message, "Y" if isWhisper else "N"))
        if isWhisper:
            super().message("#jtv", "/w " + str(channel).replace("#", "") + " " + str(message))
        elif not silence:
            super().message(channel, message)
        else:
            logger.debug("Message not sent as not Whisper and Silent Mode enabled")

    def do_command(self, command, args, sender, channel, tags, isWhisper=False):
        logger.debug("Got command: %s with arguments %s", command, str(args))
        isMarathonChannel = channel == config['marathonChannel'] and not isWhisper
        if command == "as" and debugMode and sender in superadmins:
            if len(args) < 2 or len(args[1]) == 0:
                self.message(channel, "Usage: !as <user> <command>", isWhisper)
                return
            with busyLock:
                with db.cursor() as cur:
                    cur.execute("SELECT id FROM users WHERE name = %s", [args[0]])
                    row = cur.fetchone()
                    if row is None:
                        self.message(channel, "User not found.")
                        return
                    userid = row[0]
            self.do_command(args[1][1:].lower(), args[2:], args[0].lower(), channel,
                            {'display-name': args[0], 'user-id': userid, 'badges': []}, isWhisper)

            return
        with busyLock:
            if command == config["marathonHelpCommand"] and isMarathonChannel:
                self.message(channel, config["marathonHelpCommandText"], isWhisper)
                return
            if command == "quit" and sender in superadmins:
                logger.info("Quitting from admin command.")
                pool.disconnect(client=self, expected=True)
                # sys.exit(0)
                return
            if command == "checkhand":
                # print("Checking hand for " + sender)
                cards = getHand(tags['user-id'])
                if len(cards) == 0:
                    self.message(channel,
                                 "%s, you don't currently have any waifus! Get your first one with !freewaifu" % tags[
                                     'display-name'], isWhisper=isWhisper)
                    return

                currentData = currentCards(tags['user-id'], True)
                limit = handLimit(tags['user-id'])
                dropLink = "%s/hand?user=%s" % (config["siteHost"], sender)
                msgArgs = {"user": tags['display-name'], "limit": limit, "curr": currentData['hand'],
                           "bounties": currentData['bounties'], "link": dropLink}

                # verbose mode if it's a whisper or they request it
                if len(args) > 0 and args[0].lower() == "verbose":
                    if isWhisper or followsme(tags['user-id']):
                        whisperChannel = "#%s" % sender
                        if currentData['bounties'] > 0:
                            self.message(whisperChannel,
                                         "{user}, you can have {limit} waifus (currently held: {curr} waifus and {bounties} active bounties) and your current hand is: {link}".format(
                                             **msgArgs), True)
                        else:
                            self.message(whisperChannel,
                                         "{user}, you can have {limit} waifus (currently held: {curr}) and your current hand is: {link}".format(
                                             **msgArgs), True)
                        messages = ["Your current hand is: "]
                        for row in cards:
                            row['amount'] = "(x%d)" % row['amount'] if row['amount'] > 1 else ""
                            row['rarity'] = config["rarity%sName" % row['rarity']]
                            waifumsg = '[{id}][{rarity}] {name} from {series} - {image}{amount}; '.format(**row)
                            if len(messages[-1]) + len(waifumsg) > 400:
                                messages.append(waifumsg)
                            else:
                                messages[-1] += waifumsg

                        for message in messages:
                            self.message(whisperChannel, message, True)
                    elif not isWhisper:
                        self.message(channel,
                                     "%s, you can't use verbose checkhand because you don't follow the bot! Follow it and try again." %
                                     tags['display-name'])
                else:
                    if currentData['bounties'] > 0:
                        self.message(channel,
                                     "{user}, you can have {limit} waifus (currently held: {curr} waifus and {bounties} active bounties) and your current hand is: {link}".format(
                                         **msgArgs), isWhisper)
                    else:
                        self.message(channel,
                                     "{user}, you can have {limit} waifus (currently held: {curr}) and your current hand is: {link}".format(
                                         **msgArgs), isWhisper)
                return
            if command == "points":
                # print("Checking points for " + sender)
                cur = db.cursor()
                cur.execute("SELECT points FROM users WHERE id = %s", [tags['user-id']])
                self.message(channel, str(tags['display-name']) + ", you have " + str(cur.fetchone()[0]) + " points!",
                             isWhisper=isWhisper)
                cur.close()
                return
            if command == "freewaifu":
                # print("Checking free waifu egliability for " + str(sender))
                with db.cursor() as cur:
                    cur.execute("SELECT lastFree FROM users WHERE id = %s", [tags['user-id']])
                    res = cur.fetchone()
                    nextFree = 79200000 + int(res[0])
                    if nextFree > current_milli_time():
                        a = datetime.timedelta(milliseconds=nextFree - current_milli_time(), microseconds=0)
                        datestring = "{0}".format(a).split(".")[0]
                        self.message(channel,
                                     str(tags['display-name']) + ", you need to wait {0} for your next free drop!".format(
                                         datestring), isWhisper=isWhisper)
                        return
                        
                    storeInPack = False
                    
                    if len(args) > 0 and args[0].lower() == "pack":
                        cur.execute("SELECT COUNT(*) FROM boosters_opened WHERE userid = %s AND status = 'open'", [tags['user-id']])
                        bct = cur.fetchone()[0]
                        if bct > 0:
                            self.message(channel, "%s, you can't use !freewaifu pack while you have an open booster! You might be able to use !freewaifu instead." % tags['display-name'], isWhisper)
                            return
                        storeInPack = True
                    elif currentCards(tags['user-id']) >= handLimit(tags['user-id']):
                        self.message(channel, "%s, your hand is full! Disenchant something, !upgrade your hand or use !freewaifu pack instead." % tags['display-name'], isWhisper)
                        return
                    
                    # good to get freewaifu
                    row = getWaifuById(dropCard(bannedCards=getUniqueCards(tags['user-id'])))
                    recordPullMetrics(row['id'])
                    logDrop(str(tags['user-id']), row['id'], row['base_rarity'], "freewaifu", channel, isWhisper)
                    if row['base_rarity'] >= int(config["drawAlertMinimumRarity"]):
                        threading.Thread(target=sendDrawAlert, args=(channel, row, str(tags["display-name"]))).start()
                    
                    droplink = config["siteHost"] + "/booster?user=" + sender
                    msgArgs = {"username": tags['display-name'], "id": row['id'], "rarity": config["rarity%dName" % row['base_rarity']],
                        "name": row['name'], "series": row['series'], "link": row['image'] if not storeInPack else "", "pack": " ( %s )" % droplink if storeInPack else ""}
                    
                    if storeInPack:
                        cur.execute("INSERT INTO boosters_opened (userid, boostername, paid, created, status) VALUES(%s, 'freewaifu', 0, %s, 'open')",
                        [tags['user-id'], current_milli_time()])
                        boosterid = cur.lastrowid
                        cur.execute("INSERT INTO boosters_cards (boosterid, waifuid) VALUES(%s, %s)", [boosterid, row['id']])
                    else:
                        giveCard(tags['user-id'], row['id'], row['base_rarity'])
                        attemptPromotions(row['id'])
                    
                    cur.execute("UPDATE users SET lastFree = %s WHERE id = %s", [current_milli_time(), tags['user-id']])
                    self.message(channel, "{username}, you dropped a new waifu: [{id}][{rarity}] {name} from {series} - {link}{pack}".format(**msgArgs), isWhisper)
                    return
            if command == "disenchant" or command == "de":
                if len(args) == 0 or (len(args) == 1 and len(args[0]) == 0):
                    self.message(channel, "Usage: !disenchant <list of IDs>", isWhisper=isWhisper)
                    return

                # check for confirmation
                hasConfirmed = False
                if args[-1].lower() == "yes":
                    hasConfirmed = True
                    args = args[:-1]

                disenchants = []
                dontHave = []
                hand = getHand(tags['user-id'])

                for arg in args:
                    # handle disenchanting
                    try:
                        deTarget = parseHandCardSpecifier(hand, arg)
                        if deTarget in disenchants:
                            self.message(channel, "You can't disenchant the same waifu twice at once!", isWhisper)
                            return
                        if deTarget['rarity'] >= int(config["numNormalRarities"]) and not hasConfirmed:
                            self.message(channel,
                                         "%s, you are trying to disenchant one or more special waifus! Special waifus do not take up any hand space and disenchant for 0 points. If you are sure you want to do this, append \" yes\" to the end of your command." %
                                         tags['display-name'], isWhisper)
                            return
                        if deTarget['rarity'] >= int(
                                config["disenchantRequireConfirmationRarity"]) and not hasConfirmed:
                            confirmRarityName = config["rarity%sName" % config["disenchantRequireConfirmationRarity"]]
                            self.message(channel,
                                         "%s, you are trying to disenchant one or more waifus of %s rarity or higher! If you are sure you want to do this, append \" yes\" to the end of your command." % (
                                             tags['display-name'], confirmRarityName), isWhisper)
                            return
                        if deTarget['rarity'] != deTarget['base_rarity'] and not hasConfirmed:
                            self.message(channel,
                                         "%s, you are trying to disenchant one or more promoted waifus! If you are sure you want to do this, append \" yes\" to the end of your command." %
                                         tags['display-name'], isWhisper)
                            return
                        disenchants.append(deTarget)
                    except CardNotInHandException:
                        dontHave.append(arg)
                    except AmbiguousRarityException:
                        self.message(channel,
                                     "You have more than one rarity of waifu %s in your hand. Please specify a rarity as well by appending a hyphen and then the rarity e.g. !disenchant %s-god" % (
                                         arg, arg), isWhisper)
                        return
                    except ValueError:
                        self.message(channel, "Could not decipher one or more of the waifu IDs you provided.",
                                     isWhisper)
                        return

                if len(dontHave) > 0:
                    if len(dontHave) == 1:
                        self.message(channel, "You don't own waifu %s." % dontHave[0], isWhisper)
                    else:
                        self.message(channel,
                                     "You don't own the following waifus: %s" % ", ".join([id for id in dontHave]),
                                     isWhisper)
                    return

                # handle disenchants appropriately
                pointsGain = 0
                ordersFilled = 0
                checkPromos = []
                for row in disenchants:
                    if row['id'] not in checkPromos:
                        checkPromos.append(row['id'])
                    takeCard(tags['user-id'], row['id'], row['rarity'])

                    baseValue = int(config["rarity" + str(row['rarity']) + "Value"])
                    profit = attemptBountyFill(self, row['id'])
                    pointsGain += baseValue + profit
                    if profit > 0:
                        ordersFilled += 1
                    elif row['rarity'] >= int(config["disenchantAlertMinimumRarity"]):
                        # valuable waifu disenchanted
                        waifuData = getWaifuById(row['id'])
                        waifuData['base_rarity'] = row['rarity']  # cheat to make it show any promoted rarity override
                        threading.Thread(target=sendDisenchantAlert,
                                         args=(channel, waifuData, tags["display-name"])).start()

                addPoints(tags['user-id'], pointsGain)
                attemptPromotions(*checkPromos)

                if len(disenchants) == 1:
                    buytext = " (bounty filled)" if ordersFilled > 0 else ""
                    self.message(channel, "Successfully disenchanted waifu %d%s and added %d points to %s's account" % (
                        disenchants[0]['id'], buytext, pointsGain, str(tags['display-name'])), isWhisper=isWhisper)
                else:
                    buytext = " (%d bounties filled)" % ordersFilled if ordersFilled > 0 else ""
                    self.message(channel,
                                 "Successfully disenchanted %d waifus%s and added %d points to %s's account" % (
                                     len(disenchants), buytext, pointsGain, str(tags['display-name'])),
                                 isWhisper=isWhisper)

                return
            if command == "giveme":
                self.message(channel, "No.", isWhisper=isWhisper)
                return
            if command == "buy":
                if len(args) != 1:
                    if len(args) > 0 and args[0].lower() == "booster":
                        self.message(channel, "%s -> Did you mean !booster buy?" % tags['display-name'], isWhisper)
                    else:
                        self.message(channel, "Usage: !buy <rarity> (So !buy uncommon for an uncommon)",
                                     isWhisper=isWhisper)
                    return
                if currentCards(tags['user-id']) >= handLimit(tags['user-id']):
                    self.message(channel,
                                 "{sender}, you have too many cards to buy one! !disenchant some or upgrade your hand!".format(
                                     sender=str(tags['display-name'])), isWhisper=isWhisper)
                    return
                try:
                    rarity = parseRarity(args[0])
                except Exception:
                    self.message(channel, "Unknown rarity. Usage: !buy <rarity> (So !buy uncommon for an uncommon)",
                                 isWhisper=isWhisper)
                    return
                if rarity >= int(config["numNormalRarities"]) or int(config["rarity" + str(rarity) + "Max"]) == 1:
                    self.message(channel, "You can't buy that rarity of waifu.", isWhisper=isWhisper)
                    return
                price = int(config["rarity" + str(rarity) + "Value"]) * 5
                if not hasPoints(tags['user-id'], price):
                    self.message(channel, "You do not have enough points to buy a " + str(
                        config["rarity" + str(rarity) + "Name"]) + " waifu. You need " + str(price) + " points.",
                                 isWhisper=isWhisper)
                    return
                chosenWaifu = dropCard(rarity=rarity, allowDowngrades=False,
                                       bannedCards=getUniqueCards(tags['user-id']))
                if chosenWaifu is not None:
                    addPoints(tags['user-id'], 0 - price)
                    row = getWaifuById(chosenWaifu)
                    self.message(channel, str(
                        tags[
                            'display-name']) + ', you bought a new Waifu for {price} points: [{id}][{rarity}] {name} from {series} - {link}'.format(
                        id=str(row['id']), rarity=config["rarity" + str(row['base_rarity']) + "Name"], name=row['name'],
                        series=row['series'],
                        link=row['image'], price=str(price)), isWhisper=isWhisper)
                    recordPullMetrics(row['id'])
                    giveCard(tags['user-id'], row['id'], row['base_rarity'])
                    logDrop(str(tags['user-id']), row['id'], rarity, "buy", channel, isWhisper)
                    if row['base_rarity'] >= int(config["drawAlertMinimumRarity"]):
                        threading.Thread(target=sendDrawAlert, args=(channel, row, str(tags["display-name"]))).start()
                    attemptPromotions(row['id'])
                    return
                else:
                    self.message(channel, "You can't buy a %s waifu right now. Try again later." % config[
                        "rarity" + str(rarity) + "Name"], isWhisper)
                    return
            if command == "booster":
                if len(args) < 1:
                    self.message(channel,
                                 "Usage: !booster buy <%s> OR !booster select <take/disenchant> (for each waifu) OR !booster show" % visiblepacks,
                                 isWhisper=isWhisper)
                    return

                # check for confirmation
                hasConfirmed = False
                if args[-1].lower() == "yes":
                    hasConfirmed = True
                    args = args[:-1]

                cmd = args[0].lower()
                # even more shorthand shortcut for disenchant all
                if cmd == "trash":
                    cmd = "select"
                    args = ["select", "deall"]

                cur = db.cursor()
                cur.execute("SELECT id FROM boosters_opened WHERE userid = %s AND status = 'open'", [tags['user-id']])
                boosterinfo = cur.fetchone()

                if (cmd == "show" or cmd == "select") and boosterinfo is None:
                    self.message(channel, tags[
                        'display-name'] + ", you do not have an open booster. Buy one using !booster buy <%s>" % visiblepacks,
                                 isWhisper=isWhisper)
                    cur.close()
                    return

                if cmd == "show":
                    if len(args) > 1 and args[1].lower() == "verbose":
                        # TODO
                        pass
                    else:
                        droplink = config["siteHost"] + "/booster?user=" + sender
                        self.message(channel, "{user}, your current open booster pack: {droplink}".format(
                            user=tags['display-name'], droplink=droplink), isWhisper=isWhisper)
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
                                    self.message(channel,
                                                 "When using shorthand booster syntax, please only use the letters d and k.",
                                                 isWhisper=isWhisper)
                                    cur.close()
                                    return
                                elif letter == 'd':
                                    selectArgs.append("disenchant")
                                else:
                                    selectArgs.append("keep")
                    else:
                        selectArgs = args[1:]

                    if len(selectArgs) != len(cards):
                        self.message(channel, "You did not specify the correct amount of keep/disenchant.",
                                     isWhisper=isWhisper)
                        cur.close()
                        return

                    for arg in selectArgs:
                        if not (arg.lower() == "keep" or arg.lower() == "disenchant"):
                            self.message(channel,
                                         "Sorry, but " + arg.lower() + " is not a valid option. Use keep or disenchant",
                                         isWhisper=isWhisper)
                            cur.close()
                            return

                    # check card info for rarities etc
                    keepCards = []
                    deCards = []
                    keepingCount = 0
                    for i in range(len(cards)):
                        waifu = getWaifuById(cards[i])
                        if selectArgs[i].lower() == "keep":
                            keepCards.append(waifu)
                            if waifu['base_rarity'] < int(config["numNormalRarities"]):
                                keepingCount += 1
                        else:
                            # disenchant
                            if waifu['base_rarity'] >= int(
                                    config["disenchantRequireConfirmationRarity"]) and not hasConfirmed:
                                confirmRarityName = config[
                                    "rarity%sName" % config["disenchantRequireConfirmationRarity"]]
                                self.message(channel,
                                             "%s, you are trying to disenchant one or more waifus of %s rarity or higher! If you are sure you want to do this, append \" yes\" to the end of your command." % (
                                                 tags['display-name'], confirmRarityName), isWhisper)
                                return
                            deCards.append(waifu)

                    if keepingCount + currentCards(tags['user-id']) > handLimit(tags['user-id']) and keepingCount != 0:
                        self.message(channel, "You can't keep that many waifus! !disenchant some!", isWhisper=isWhisper)
                        cur.close()
                        return

                    # if we made it through the whole pack without tripping confirmation, we can actually do it now
                    for waifu in keepCards:
                        giveCard(tags['user-id'], waifu['id'], waifu['base_rarity'])
                    gottenpoints = 0
                    ordersFilled = 0
                    for waifu in deCards:
                        baseValue = int(config["rarity" + str(waifu['base_rarity']) + "Value"])
                        profit = attemptBountyFill(self, waifu['id'])
                        gottenpoints += baseValue + profit
                        if profit > 0:
                            ordersFilled += 1
                        elif waifu['base_rarity'] >= int(config["disenchantAlertMinimumRarity"]):
                            # valuable waifu being disenchanted
                            threading.Thread(target=sendDisenchantAlert,
                                             args=(channel, waifu, str(tags["display-name"]))).start()
                    addPoints(tags['user-id'], gottenpoints)
                    attemptPromotions(*cards)

                    # compile the message to be sent in chat
                    response = "You take your booster pack and: "

                    if len(keepCards) > 0:
                        response += " keep " + ', '.join(str(x['id']) for x in keepCards) + ";"
                    if len(deCards) > 0:
                        response += " disenchant " + ', '.join(str(x['id']) for x in deCards)

                    if ordersFilled > 0:
                        response += " (%d bounties filled);" % ordersFilled
                    elif len(deCards) > 0:
                        response += ";"

                    self.message(channel, response + " netting a total of " + str(gottenpoints) + " points.",
                                 isWhisper=isWhisper)
                    cur.execute("UPDATE boosters_opened SET status = 'closed', updated = %s WHERE id = %s",
                                [current_milli_time(), boosterinfo[0]])
                    cur.close()
                    return
                if cmd == "buy":
                    if boosterinfo is not None:
                        self.message(channel,
                                     "You already have an open booster. Select the waifus you want to keep or disenchant first!",
                                     isWhisper=isWhisper)
                        cur.close()
                        return
                    if len(args) < 2:
                        self.message(channel, "Usage: !booster buy <%s>" % visiblepacks, isWhisper=isWhisper)
                        cur.close()
                        return

                    packname = args[1].lower()
                    try:
                        openBooster(tags['user-id'], tags['display-name'], channel, isWhisper, packname, True)
                        if checkHandUpgrade(tags['user-id']):
                            messageForHandUpgrade(tags['user-id'], tags['display-name'], self, channel, isWhisper)

                        droplink = config["siteHost"] + "/booster?user=" + sender
                        self.message(channel, "{user}, you open a {type} booster pack and you get: {droplink}".format(
                            user=tags['display-name'], type=packname, droplink=droplink), isWhisper=isWhisper)
                    except InvalidBoosterException:
                        self.message(channel, "Invalid booster type. Packs available right now: %s." % visiblepacks,
                                     isWhisper=isWhisper)
                    except CantAffordBoosterException as exc:
                        self.message(channel,
                                     "{user}, sorry, you don't have enough points to buy a {name} booster pack. You need {points}.".format(
                                         user=tags['display-name'], name=packname, points=exc.cost),
                                     isWhisper=isWhisper)

                    cur.close()
                    return
            if command == "trade":
                ourid = int(tags['user-id'])
                with db.cursor() as cur:
                    # expire old trades
                    currTime = current_milli_time()
                    cur.execute(
                        "UPDATE trades SET status = 'expired', updated = %s WHERE status = 'open' AND created <= %s",
                        [currTime, currTime - 86400000])
                    if len(args) < 2:
                        self.message(channel,
                                     "Usage: !trade <check/accept/decline> <user> OR !trade <user> <have> <want> [points]",
                                     isWhisper=isWhisper)
                        return
                    subarg = args[0].lower()
                    if subarg in ["check", "accept", "decline"]:
                        otherparty = args[1].lower()

                        cur.execute("SELECT id FROM users WHERE name = %s", [otherparty])
                        otheridrow = cur.fetchone()
                        if otheridrow is None:
                            self.message(channel, "I don't recognize that username.", isWhisper=isWhisper)
                            return
                        otherid = int(otheridrow[0])

                        # look for trade row
                        cur.execute(
                            "SELECT id, want, have, points, payup, want_rarity, have_rarity FROM trades WHERE fromid = %s AND toid = %s AND status = 'open' LIMIT 1",
                            [otherid, ourid])
                        trade = cur.fetchone()

                        if trade is None:
                            self.message(channel,
                                         otherparty + " did not send you a trade. Send one with !trade " + otherparty + " <have> <want> [points]",
                                         isWhisper=isWhisper)
                            return

                        want = trade[1]
                        have = trade[2]
                        tradepoints = trade[3]
                        payup = trade[4]
                        want_rarity = trade[5]
                        have_rarity = trade[6]

                        if subarg == "check":
                            wantdata = getWaifuById(want)
                            havedata = getWaifuById(have)
                            haveStr = "[%d][%s] %s" % (
                                have, config["rarity" + str(have_rarity) + "Name"], havedata['name'])
                            wantStr = "[%d][%s] %s" % (
                                want, config["rarity" + str(want_rarity) + "Name"], wantdata['name'])
                            payer = "they will pay you" if otherid == payup else "you will pay them"
                            if tradepoints > 0:
                                self.message(channel,
                                             "{other} wants to trade their {have} for your {want} and {payer} {points} points. Accept it with !trade accept {other}".format(
                                                 other=otherparty, have=haveStr, want=wantStr, payer=payer,
                                                 points=tradepoints), isWhisper=isWhisper)
                            else:
                                self.message(channel,
                                             "{other} wants to trade their {have} for your {want}. Accept it with !trade accept {other}".format(
                                                 other=otherparty, have=haveStr, want=wantStr, payer=payer),
                                             isWhisper=isWhisper)
                            return
                        elif subarg == "decline":
                            cur.execute("UPDATE trades SET status = 'declined', updated = %s WHERE id = %s",
                                        [current_milli_time(), trade[0]])
                            self.message(channel, "Trade declined.", isWhisper=isWhisper)
                            return
                        else:
                            # accept
                            # check that cards are still in place
                            ourhand = getHand(ourid)
                            otherhand = getHand(otherid)

                            try:
                                parseHandCardSpecifier(ourhand, "%d-%d" % (want, want_rarity))
                            except CardRarityNotInHandException:
                                self.message(channel,
                                             "%s, the rarity of waifu %d in your hand has changed! Trade cancelled." % (
                                                 tags['display-name'], want), isWhisper)
                                cur.execute("UPDATE trades SET status = 'invalid', updated = %s WHERE id = %s",
                                            [current_milli_time(), trade[0]])
                                return
                            except CardNotInHandException:
                                self.message(channel, "%s, you no longer own waifu %d! Trade cancelled." % (
                                    tags['display-name'], want), isWhisper)
                                cur.execute("UPDATE trades SET status = 'invalid', updated = %s WHERE id = %s",
                                            [current_milli_time(), trade[0]])
                                return

                            try:
                                parseHandCardSpecifier(otherhand, "%d-%d" % (have, have_rarity))
                            except CardRarityNotInHandException:
                                self.message(channel,
                                             "%s, the rarity of %s's copy of waifu %d has changed! Trade cancelled." % (
                                                 tags['display-name'], otherparty, have), isWhisper)
                                cur.execute("UPDATE trades SET status = 'invalid', updated = %s WHERE id = %s",
                                            [current_milli_time(), trade[0]])
                                return
                            except CardNotInHandException:
                                self.message(channel, "%s, %s no longer owns waifu %d! Trade cancelled." % (
                                    tags['display-name'], otherparty, have), isWhisper)
                                cur.execute("UPDATE trades SET status = 'invalid', updated = %s WHERE id = %s",
                                            [current_milli_time(), trade[0]])
                                return

                            cost = int(config["tradingFee"])

                            nonpayer = ourid if payup == otherid else otherid

                            if not hasPoints(payup, cost + tradepoints):
                                self.message(channel, "Sorry, but %s cannot cover the %s trading fee." % (
                                    "you" if payup == ourid else otherparty, "fair" if tradepoints > 0 else "base"),
                                             isWhisper=isWhisper)
                                return

                            if not hasPoints(nonpayer, cost - tradepoints):
                                self.message(channel, "Sorry, but %s cannot cover the base trading fee." % (
                                    "you" if nonpayer == ourid else otherparty), isWhisper=isWhisper)
                                return

                            # take cards
                            takeCard(ourid, want, want_rarity)
                            takeCard(otherid, have, have_rarity)

                            # give cards
                            giveCard(ourid, have, have_rarity)
                            giveCard(otherid, want, want_rarity)

                            attemptPromotions(want, have)

                            # points
                            addPoints(payup, -(tradepoints + cost))
                            addPoints(nonpayer, tradepoints - cost)

                            # done
                            cur.execute("UPDATE trades SET status = 'accepted', updated = %s WHERE id = %s",
                                        [current_milli_time(), trade[0]])

                            self.message(channel, "Trade executed!", isWhisper=isWhisper)
                            return

                    if len(args) not in [3, 4]:
                        self.message(channel,
                                     "Usage: !trade <accept/decline> <user> OR !trade <user> <have> <want> [points]",
                                     isWhisper=isWhisper)
                        return

                    other = args[0]

                    cur.execute("SELECT id FROM users WHERE name = %s", [other])
                    otheridrow = cur.fetchone()
                    if otheridrow is None:
                        self.message(channel, "I don't recognize that username.", isWhisper=isWhisper)
                        return

                    otherid = int(otheridrow[0])
                    ourhand = getHand(ourid)
                    otherhand = getHand(otherid)

                    try:
                        have = parseHandCardSpecifier(ourhand, args[1])
                    except CardRarityNotInHandException:
                        self.message(channel, "%s, you don't own that waifu at that rarity!" % tags['display-name'],
                                     isWhisper)
                        return
                    except CardNotInHandException:
                        self.message(channel, "%s, you don't own that waifu!" % tags['display-name'], isWhisper)
                        return
                    except AmbiguousRarityException:
                        self.message(channel,
                                     "%s, you own more than one rarity of waifu %s! Please specify a rarity as well by appending a hyphen and then the rarity, e.g. %s-god" % (
                                         tags['display-name'], args[1], args[1]), isWhisper)
                        return
                    except ValueError:
                        self.message(channel, "Only whole numbers/IDs + rarities please.", isWhisper)
                        return

                    try:
                        want = parseHandCardSpecifier(otherhand, args[2])
                    except CardRarityNotInHandException:
                        self.message(channel,
                                     "%s, %s doesn't own that waifu at that rarity!" % (tags['display-name'], other),
                                     isWhisper)
                        return
                    except CardNotInHandException:
                        self.message(channel, "%s, %s doesn't own that waifu!" % (tags['display-name'], other),
                                     isWhisper)
                        return
                    except AmbiguousRarityException:
                        self.message(channel,
                                     "%s, %s owns more than one rarity of waifu %s! Please specify a rarity as well by appending a hyphen and then the rarity, e.g. %s-god" % (
                                         tags['display-name'], other, args[2], args[2]), isWhisper)
                        return
                    except ValueError:
                        self.message(channel, "Only whole numbers/IDs + rarities please.", isWhisper)
                        return

                    points = 0
                    if len(args) == 4:
                        try:
                            points = int(args[3])
                        except ValueError:
                            self.message(channel, "Only whole numbers/IDs + rarities please.", isWhisper)
                            return

                    payup = ourid
                    firstSpecialRarity = int(config["numNormalRarities"])
                    canTradeDirectly = (want["rarity"] == have["rarity"]) or (
                            want["rarity"] >= firstSpecialRarity and have["rarity"] >= firstSpecialRarity)
                    if not canTradeDirectly:
                        if have["rarity"] >= firstSpecialRarity or want["rarity"] >= firstSpecialRarity:
                            self.message(channel,
                                         "Sorry, special-rarity cards can only be traded for other special-rarity cards.",
                                         isWhisper=isWhisper)
                            return
                        if len(args) != 4:
                            self.message(channel,
                                         "To trade waifus of different rarities, please append a point value the owner of the lower tier card has to pay to the command to make the trade fair. (see !help)",
                                         isWhisper=isWhisper)
                            return
                        highercost = int(config["rarity" + str(max(have["rarity"], want["rarity"])) + "Value"])
                        lowercost = int(config["rarity" + str(min(have["rarity"], want["rarity"])) + "Value"])
                        costdiff = highercost - lowercost
                        mini = int(costdiff / 2)
                        maxi = int(costdiff)
                        if points < mini:
                            self.message(channel, "Minimum points to trade this difference in rarity is " + str(mini),
                                         isWhisper=isWhisper)
                            return
                        if points > maxi:
                            self.message(channel, "Maximum points to trade this difference in rarity is " + str(maxi),
                                         isWhisper=isWhisper)
                            return
                        if want["rarity"] < have["rarity"]:
                            payup = otherid

                    elif points > 0:
                        self.message(channel, "You cannot attach points on same-rarity trades.", isWhisper)
                        return

                    # cancel any old trades with this pairing
                    cur.execute(
                        "UPDATE trades SET status = 'cancelled', updated = %s WHERE fromid = %s AND toid = %s AND status = 'open'",
                        [current_milli_time(), ourid, otherid])

                    # insert new trade
                    tradeData = [ourid, otherid, want['id'], want['rarity'], have['id'], have['rarity'], points, payup,
                                 current_milli_time(), "$$whisper$$" if isWhisper else channel]
                    cur.execute(
                        "INSERT INTO trades (fromid, toid, want, want_rarity, have, have_rarity, points, payup, status, created, originChannel) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, 'open', %s, %s)",
                        tradeData)

                    havedata = getWaifuById(have['id'])
                    wantdata = getWaifuById(want['id'])
                    haveStr = "[%d][%s] %s" % (
                        have['id'], config["rarity" + str(have['rarity']) + "Name"], havedata['name'])
                    wantStr = "[%d][%s] %s" % (
                        want['id'], config["rarity" + str(want['rarity']) + "Name"], wantdata['name'])

                    paying = ""
                    if points > 0:
                        if payup == ourid:
                            paying = " with you paying them " + str(points) + " points"
                        else:
                            paying = " with them paying you " + str(points) + " points"
                    self.message(channel,
                                 "Offered {other} to trade your {have} for their {want}{paying}".format(other=other,
                                                                                                        have=haveStr,
                                                                                                        want=wantStr,
                                                                                                        paying=paying),
                                 isWhisper=isWhisper)
                    return
            if command == "lookup":
                if len(args) != 1:
                    self.message(channel, "Usage: !lookup <id>", isWhisper=isWhisper)
                    return

                if infoCommandAvailable(tags['user-id'], sender, tags['display-name'], self, channel, isWhisper):
                    try:
                        waifu = getWaifuById(args[0])
                        assert waifu is not None
                        assert waifu['can_lookup'] == 1

                        with db.cursor() as cur:
                            baseRarityName = config["rarity%dName" % waifu["base_rarity"]]
                            cur.execute(
                                "SELECT users.name, has_waifu.rarity, has_waifu.amount FROM has_waifu JOIN users ON has_waifu.userid = users.id WHERE has_waifu.waifuid = %s",
                                [waifu['id']])
                            allOwners = cur.fetchall()

                        # compile per-owner data
                        ownerData = {}
                        ownedByOwner = {}
                        for row in allOwners:
                            if row[0] not in ownerData:
                                ownerData[row[0]] = {}
                                ownedByOwner[row[0]] = 0
                            ownerData[row[0]][config["rarity%dName" % row[1]]] = row[2]
                            ownedByOwner[row[0]] += row[2]

                        ownerDescriptions = []
                        for owner in ownerData:
                            if len(ownerData[owner]) != 1 or baseRarityName not in ownerData[owner] or ownedByOwner[
                                owner] > 1:
                                # verbose
                                if ownedByOwner[owner] > 1:
                                    ownerDescriptions.append(owner + " (" + ", ".join(
                                        "%d %s" % (ownerData[owner][rarity], rarity) for rarity in
                                        ownerData[owner]) + ")")
                                else:
                                    ownerDescriptions.append(
                                        owner + " (" + "".join(rarity for rarity in ownerData[owner]) + ")")
                            else:
                                ownerDescriptions.append(owner)

                        waifu["rarity"] = baseRarityName
                        
                        # check for packs
                        with db.cursor() as cur:
                            cur.execute("SELECT users.name FROM boosters_cards JOIN boosters_opened ON boosters_cards.boosterid = boosters_opened.id JOIN users ON boosters_opened.userid = users.id WHERE boosters_cards.waifuid = %s AND boosters_opened.status = 'open'", [waifu['id']])
                            packholders = [row[0] for row in cur.fetchall()]
                        
                        if len(ownerDescriptions) > 0:
                            waifu["owned"] = " - owned by " + ", ".join(ownerDescriptions)
                            if len(packholders) > 0:
                                waifu["owned"] += "; currently in a pack for: " + ", ".join(packholders)
                        elif len(packholders) > 0:
                            waifu["owned"] = " - currently in a pack for: " + ", ".join(packholders)
                        elif waifu["pulls"] > 0:
                            waifu["owned"] = " (not currently owned or in a pack)"
                        else:
                            waifu["owned"] = " (not dropped yet)"
                            
                        # bounty info
                        with db.cursor() as cur:
                            cur.execute("SELECT COUNT(*), COALESCE(MAX(amount), 0) FROM bounties WHERE waifuid = %s AND status='open'", [waifu['id']])
                            allordersinfo = cur.fetchone()
                            
                            if allordersinfo[0] > 0:
                                cur.execute("SELECT amount FROM bounties WHERE userid = %s AND waifuid = %s AND status='open'", [tags['user-id'], waifu['id']])
                                myorderinfo = cur.fetchone()
                                minfo = {"count": allordersinfo[0], "highest": allordersinfo[1]}
                                if myorderinfo is not None:
                                    minfo["mine"] = myorderinfo[0]
                                    if myorderinfo[0] == allordersinfo[1]:
                                        waifu["bountyinfo"] = "This waifu currently has {count} bounties, you are the highest bidder at {highest} points.".format(**minfo)
                                    else:
                                        waifu["bountyinfo"] = "This waifu currently has {count} bounties, your bid of {mine} points is lower than the highest bid of {highest} points.".format(**minfo)
                                else:
                                    waifu["bountyinfo"] = "This waifu currently has {count} bounties, out of which the highest bid is {highest} points. You don't have a bounty on this waifu right now.".format(**minfo)
                            else:
                                waifu["bountyinfo"] = "There are no current bounties on this waifu."
                                
                        # last pull
                        if waifu["pulls"] == 0 or waifu["last_pull"] is None or waifu["base_rarity"] >= int(config["numNormalRarities"]):
                            waifu["lp"] = ""
                        else:
                            lpdiff = (current_milli_time() - waifu["last_pull"]) // 86400000
                            if lpdiff == 0:
                                waifu["lp"] = " This waifu was last pulled less than a day ago."
                            elif lpdiff == 1:
                                waifu["lp"] = " This waifu was last pulled 1 day ago."
                            else:
                                waifu["lp"] = " This waifu was last pulled %d days ago." % lpdiff

                        self.message(channel, '[{id}][{rarity}] {name} from {series} - {image}{owned}. {bountyinfo}{lp}'.format(**waifu),
                                     isWhisper=isWhisper)

                        if sender not in superadmins:
                            useInfoCommand(tags['user-id'], sender, channel, isWhisper)
                    except Exception:
                        self.message(channel, "Invalid waifu ID.", isWhisper=isWhisper)

                return
            if command == "whisper":
                if followsme(tags['user-id']):
                    self.message("#jtv", "/w {user} This is a test whisper.".format(user=sender), isWhisper=False)
                    self.message(channel, "Attempted to send test whisper.", isWhisper=isWhisper)
                else:
                    self.message(channel, "{user}, you need to be following me so I can send you whispers!".format(
                        user=str(tags['display-name'])), isWhisper=isWhisper)
                return
            if command == "help":
                self.message(channel, config["siteHost"] + "/help", isWhisper=isWhisper)
            if command == "alerts" or command == "alert":
                if len(args) < 1:
                    self.message(channel,
                                 "Usage: !alerts setup OR !alerts test <rarity/set> OR !alerts config <config Name> <config Value>",
                                 isWhisper=isWhisper)
                    return
                sender = sender.lower()
                subcmd = str(args[0]).lower()
                if subcmd == "setup":
                    cur = db.cursor()
                    cur.execute("SELECT alertkey FROM channels WHERE name=%s", [sender])
                    row = cur.fetchone();
                    if row is None:
                        self.message(channel, "The bot is not in your channel, so alerts can't be set up for you. Ask an admin to let it join!", iswhisper=isWhisper)
                        return
                    if row[0] is None:
                        self.message("#jtv",
                                     "/w {user} Please go to the following link and allow access: {link}{user}".format(
                                         user=sender.strip(), link=str(streamlabsauthurl).strip()), isWhisper=False)
                        self.message(channel,
                                     "Sent you a whisper with a link to set up alerts. If you didnt receive a whisper, try !whisper",
                                     isWhisper=isWhisper)
                    else:
                        self.message(channel,
                                     "Alerts seem to already be set up for your channel! Use !alerts test to test them!",
                                     isWhisper)
                    cur.close()
                    return
                if subcmd == "test":
                    isSet = False
                    if args[1] == "set":
                        rarity = int(config["numNormalRarities"]) - 1
                        isSet = True
                    else:
                        try:
                            rarity = parseRarity(args[1])
                        except Exception:
                            rarity = int(config["numNormalRarities"]) - 1
                    cur = db.cursor()
                    cur.execute("SELECT alertkey FROM channels WHERE name=%s", [sender])
                    row = cur.fetchone();
                    cur.close()
                    if row[0] is None:
                        self.message(channel,
                                     "Alerts do not seem to be set up for your channel, please set them up using !alerts setup",
                                     isWhisper=isWhisper)
                    else:
                        if isSet:
                            threading.Thread(target=sendSetAlert, args=(sender, sender, "Test Set", ["Neptune", "Nepgear", "Some other test waifu"], False)).start()
                        else:
                            threading.Thread(target=sendDrawAlert, args=(
                                sender, {"name": "Test Alert, please ignore", "base_rarity": rarity,
                                         "image": "http://t.fuelr.at/k6g"},
                            str(tags["display-name"]), False)).start()
                        self.message(channel, "Test Alert sent.", isWhisper=isWhisper)
                    return
                if subcmd == "config":
                    try:
                        configName = args[1]
                    except Exception:
                        self.message(channel, "Valid alert config options: " + ", ".join(validalertconfigvalues),
                                     isWhisper=isWhisper)
                        return
                    if configName == "reset":
                        cur = db.cursor()
                        cur.execute("DELETE FROM alertConfig WHERE channelName = %s", [sender])
                        cur.close()
                        self.message(channel, "Removed all custom alert config for your channel. #NoireScreamRules",
                                     isWhisper=isWhisper)
                        return
                    if configName not in validalertconfigvalues:
                        self.message(channel, "Valid alert config options: " + ", ".join(validalertconfigvalues),
                                     isWhisper=isWhisper)
                        return
                    try:
                        configValue = args[2]
                    except Exception:
                        cur = db.cursor()
                        cur.execute("SELECT val FROM alertConfig WHERE channelName=%s AND config = %s",
                                    [sender, configName])
                        rows = cur.fetchall()
                        if len(rows) != 1:
                            self.message(channel, 'Alert config "' + configName + '" is unset for your channel.',
                                         isWhisper=isWhisper)
                        else:
                            configValue = rows[0][0]
                            self.message(channel,
                                         'Alert config "' + configName + '" is set to "' + configValue + '" for your channel.',
                                         isWhisper=isWhisper)
                        cur.close()
                        return
                    cur = db.cursor()
                    cur.execute("SELECT val FROM alertConfig WHERE channelName=%s AND config = %s",
                                [sender, configName])
                    rows = cur.fetchall()
                    if configValue == "reset":
                        cur.execute("DELETE FROM alertConfig WHERE channelName=%s AND config=%s", [sender, configName])
                        cur.close()
                        self.message(channel, 'Reset custom alert config "' + configName + '" for your channel.',
                                     isWhisper=isWhisper)
                        return
                    if configName == "alertChannel" and configValue not in ["host", "donation", "follow", "reset",
                                                                            "subscription"]:
                        self.message(channel,
                                     'Valid options for alertChannel: "host", "donation", "follow", "subscription", "reset"')
                        cur.close()
                        return
                    if len(rows) == 1:
                        cur.execute("UPDATE alertConfig SET val=%s WHERE channelName=%s AND config = %s",
                                    [configValue, sender, configName])
                    else:
                        cur.execute("INSERT INTO alertConfig(val, channelName, config) VALUE (%s, %s, %s)",
                                    [configValue, sender, configName])
                    cur.close()
                    self.message(channel, 'Set alert config value "' + configName + '" to "' + configValue + '"',
                                 isWhisper=isWhisper)
                    return
                self.message(channel,
                             "Usage: !alerts setup OR !alerts test <rarity> OR !alerts config <config Name> <config Value>",
                             isWhisper=isWhisper)
                return
            if command == "togglehoraro" and sender in admins:
                self.autoupdate = not self.autoupdate
                if self.autoupdate:
                    self.message(channel, "Enabled Horaro Auto-update.", isWhisper=isWhisper)
                else:
                    self.message(channel, "Disabled Horaro Auto-update.", isWhisper=isWhisper)
                return
            if sender in admins and command in ["status", "title"] and isMarathonChannel:
                updateTitle(" ".join(args))
                self.message(channel, "%s -> Title updated to %s." % (tags['display-name'], " ".join(args)))
                return
            if sender in admins and command == "game" and isMarathonChannel:
                updateGame(" ".join(args))
                self.message(channel, "%s -> Game updated to %s." % (tags['display-name'], " ".join(args)))
                return
            if command == "emotewar":
                if int(config["emoteWarStatus"]) == 0:
                    self.message(channel, "The Emote War is not active right now.", isWhisper)
                    return
                with db.cursor() as cur:
                    cur.execute("SELECT `name`, `count` FROM emoteWar ORDER BY `count` DESC")
                    r = cur.fetchall()
                    msg = "Current War: " if int(config["emoteWarStatus"]) == 1 else "THE WAR HAS BEEN DECIDED: "
                    for row in r:
                        msg += str(row[0]) + " " + str(row[1]) + " "
                    msg += ". Spamming DOES NOT COUNT, spammers will get timed out."
                    self.message(channel, msg, isWhisper=isWhisper)
                    return
            if command == "nepjoin" and sender.lower() in superadmins:
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
                        self.message(channel,
                                     "That user is not yet in the database! Let them talk in a channel the Bot is in to change that!",
                                     isWhisper=isWhisper)
                        cur.close()
                        return
                    cur.execute("INSERT INTO channels(name) VALUES (%s)", [str(chan)])
                    self.join("#" + chan)
                    self.message("#" + chan, "Hi there!", isWhisper=False)
                    self.addchannels.append('#' + chan)
                    self.message(channel, "Joined #" + chan, isWhisper=isWhisper)
                    cur.close()
                    return
                except Exception:
                    self.message(channel, "Tried joining, failed. Tell Marenthyu the following: " + str(sys.exc_info()),
                                 isWhisper=isWhisper)
                    logger.error("Error Joining channel %s: %s", chan, str(sys.exc_info()))
                    return
            if command == "nepleave" and (sender in superadmins or ("#" + sender) == str(channel)):
                if len(args) > 0:
                    self.message(channel, "nepleave doesn't take in argument. Type it in the channel to leave.",
                                 isWhisper=isWhisper)
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
                except Exception:
                    self.message(channel, "Tried to leave but failed D:", isWhisper=isWhisper)
                    logger.error("Error leaving %s: %s", channel, str(sys.exc_info()))
                    return
            if command == "reload" and sender in superadmins:
                # print("in reload command")
                loadConfig()
                self.message(channel, "Config reloaded.", isWhisper=isWhisper)
                return
            if command == "redeem":
                if len(args) != 1:
                    self.message(channel, "Usage: !redeem <token>", isWhisper=isWhisper)
                    return

                cur = db.cursor()
                # Are they a DeepDigger?

                cur.execute(
                    "SELECT id, points, waifuid, boostername, type, badgeID FROM tokens WHERE token=%s AND claimable=1 AND (only_redeemable_by IS NULL OR only_redeemable_by = %s) AND (not_redeemable_by IS NULL OR not_redeemable_by != %s) LIMIT 1",
                    [args[0], tags['user-id'], tags['user-id']])
                redeemablerows = cur.fetchall()

                if len(redeemablerows) == 0:
                    self.message(channel, "Unknown token.", isWhisper)
                    cur.close()
                    return

                if len(redeemablerows) > 1:
                    self.message(channel, "Go tell an admin that token %s is broken (duplicate token name)." % args[0],
                                 isWhisper)
                    cur.close()
                    return

                redeemdata = redeemablerows[0]

                # already claimed by this user?
                cur.execute("SELECT COUNT(*) FROM tokens_claimed WHERE tokenid = %s AND userid = %s",
                            [redeemdata[0], tags['user-id']])
                claimed = cur.fetchone()[0] or 0

                if claimed > 0:
                    self.message(channel, "%s, you have already claimed this token!" % tags['display-name'], isWhisper)
                    cur.close()
                    return

                # booster?
                packid = None
                received = []
                if redeemdata[3] is not None:
                    # check for an open booster in their account
                    # checked first because it's the only way a redeem can be blocked entirely
                    cur.execute("SELECT COUNT(*) FROM boosters_opened WHERE userid = %s AND status = 'open'",
                                [tags['user-id']])
                    boosteropen = cur.fetchone()[0] or 0

                    if boosteropen > 0:
                        self.message(channel,
                                     "%s, you can't claim this token while you have an open booster! !booster show to check it." %
                                     tags['display-name'], isWhisper)
                        cur.close()
                        return

                    try:
                        packid = openBooster(tags['user-id'], tags['display-name'], channel, isWhisper, redeemdata[3],
                                             False)
                        if checkHandUpgrade(tags['user-id']):
                            messageForHandUpgrade(tags['user-id'], tags['display-name'], self, channel, isWhisper)
                        received.append("a free booster: %s/booster?user=%s" % (config["siteHost"], sender))
                    except InvalidBoosterException:
                        self.message(channel,
                                     "Go tell an admin that token %s is broken (invalid booster attached)." % args[0],
                                     isWhisper)
                        cur.close()
                        return

                # waifu?
                if redeemdata[2] is not None:
                    waifuinfo = getWaifuById(redeemdata[2])
                    giveCard(tags['user-id'], waifuinfo['id'], waifuinfo['base_rarity'])
                    if waifuinfo['base_rarity'] < int(config["numNormalRarities"]) - 1:
                        attemptPromotions(waifuinfo['id'])
                    waifuinfo['rarity'] = config["rarity%dName" % waifuinfo['base_rarity']]
                    received.append("A waifu: [{id}][{rarity}] {name} from {series}".format(**waifuinfo))

                # points
                if redeemdata[1] != 0:
                    addPoints(tags['user-id'], redeemdata[1])
                    received.append("%d points" % redeemdata[1])

                # badge?
                if redeemdata[5] is not None:
                    badge = getBadgeByID(redeemdata[5])
                    success = giveBadge(tags['user-id'], badge["id"])
                    if success:
                        received.append("A shiny new Badge: %s" % badge["name"])
                    else:
                        received.append("An invalid badge, or a badge you already had: %s" % badge["name"])

                cur.execute(
                    "INSERT INTO tokens_claimed (tokenid, userid, points, waifuid, boostername, boosterid, timestamp, badgeID) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)",
                    [redeemdata[0], tags['user-id'], redeemdata[1], redeemdata[2], redeemdata[3], packid,
                     current_milli_time(), redeemdata[5]])

                # single use?
                if redeemdata[4] == 'single':
                    cur.execute("UPDATE tokens SET claimable = 0 WHERE id = %s", [redeemdata[0]])

                # show results
                self.message(channel,
                             "%s -> Successfully redeemed the token %s, added the following to your account -> %s" % (
                                 tags['display-name'], args[0], " and ".join(received[::-1])), isWhisper)
                cur.close()
                return
            if command == "wars":
                with db.cursor() as cur:
                    cur.execute("SELECT id, title FROM bidWars WHERE status = 'open'")

                    wars = []
                    warnum = 0
                    for war in cur.fetchall():
                        warnum += 1
                        wars.append("%s%s (!war %s)" % ("; " if warnum > 1 else "", war[1], war[0]))

                    if len(wars) == 0:
                        self.message(channel,
                                     "%s, there are no bidwars currently open right now." % tags['display-name'],
                                     isWhisper)
                    else:
                        messages = ["Current Bidwars: "]
                        for war in wars:
                            if len(messages[-1]) + len(war) > 400:
                                messages.append(war)
                            else:
                                messages[-1] += war
                        for message in messages:
                            self.message(channel, message, isWhisper)

                    return

            if command == "war":
                if len(args) != 1:
                    self.message(channel, "Usage: !war <id>", isWhisper)
                    return

                with db.cursor() as cur:
                    cur.execute(
                        "SELECT id, title, status, openEntry, openEntryMinimum, openEntryMaxLength FROM bidWars WHERE id = %s",
                        [args[0]])
                    war = cur.fetchone()

                    if war is None:
                        self.message(channel, "%s -> Invalid bidwar specified." % tags['display-name'], isWhisper)
                        return

                    warid = war[0]
                    title = war[1]
                    status = war[2]
                    openEntry = war[3] != 0
                    openEntryMinimum = war[4]
                    openEntryMaxLength = war[5]

                    # get choices
                    cur.execute(
                        "SELECT choice, amount FROM bidWarChoices WHERE warID = %s ORDER BY amount DESC, choice ASC",
                        [warid])
                    choices = cur.fetchall()

                    # render
                    if len(choices) == 0:
                        if openEntry and status == 'open':
                            self.message(channel,
                                         "The %s bidwar has no choices defined yet! Add your own for %d or more points with !vote %s <choice> <points>" % (
                                             title, openEntryMinimum, warid), isWhisper)
                        else:
                            # this bidwar was never setup properly, ignore it exists
                            self.message(channel, "%s -> Invalid bidwar specified." % tags['display-name'], isWhisper)
                        return

                    if status == 'closed':
                        # does the "first place" actually have any votes?
                        if choices[0][1] == 0:
                            # no, so this bid war hasn't started yet, don't let on it exists
                            self.message(channel, "%s -> Invalid bidwar specified." % tags['display-name'], isWhisper)
                        else:
                            runnersup = ", ".join("%s (%d points)" % (choice[0], choice[1]) for choice in choices[1:])
                            self.message(channel,
                                         "The %s bidwar is over! The winner was %s with %d points. Runners up: %s" % (
                                             title, choices[0][0], choices[0][1], runnersup), isWhisper)
                    else:
                        # open war
                        choicesStr = ", ".join("%s (%d points)" % (choice[0], choice[1]) for choice in choices)
                        msg = "The %s bidwar is currently open! Current votes: %s. !vote %s <choice> <points> to have your say." % (
                            title, choicesStr, warid)
                        if openEntry:
                            msg += " You can add a new choice by contributing at least %d points (%d characters maximum)." % (
                                openEntryMinimum, openEntryMaxLength)
                        self.message(channel, msg, isWhisper)

                    return

            if command == "vote":
                if len(args) < 3:
                    self.message(channel, "Usage: !vote <warid> <choice> <points>", isWhisper)
                    return

                if not isMarathonChannel:
                    self.message(channel, "You can only vote in wars in the HDNMarathon channel.", isWhisper)
                    return

                with db.cursor() as cur:
                    cur.execute(
                        "SELECT id, title, status, openEntry, openEntryMinimum, openEntryMaxLength FROM bidWars WHERE id = %s",
                        [args[0]])
                    war = cur.fetchone()

                    if war is None:
                        self.message(channel, "%s -> Invalid bidwar specified." % tags['display-name'], isWhisper)
                        return

                    warid = war[0]
                    title = war[1]
                    status = war[2]
                    openEntry = war[3] != 0
                    openEntryMinimum = war[4]
                    openEntryMaxLength = war[5]

                    if status == 'closed':
                        self.message(channel, "%s -> That bidwar is currently closed." % tags['display-name'],
                                     isWhisper)
                        return

                    # check their points entry
                    try:
                        points = int(args[-1])
                    except ValueError:
                        self.message(channel, "%s -> Invalid amount of points entered." % tags['display-name'],
                                     isWhisper)
                        return

                    if points <= 0:
                        self.message(channel, "%s -> Invalid amount of points entered." % tags['display-name'],
                                     isWhisper)
                        return

                    if not hasPoints(tags['user-id'], points):
                        self.message(channel, "%s -> You don't have that many points!" % tags['display-name'],
                                     isWhisper)
                        return

                    cur.execute(
                        "SELECT choice, amount FROM bidWarChoices WHERE warID = %s ORDER BY amount DESC, choice ASC",
                        [warid])
                    choices = cur.fetchall()
                    choiceslookup = [choice[0].lower() for choice in choices]
                    theirchoice = " ".join(args[1:-1]).strip()
                    theirchoiceL = theirchoice.lower()

                    if theirchoiceL not in choiceslookup:
                        # deal with custom choice entry
                        if not openEntry:
                            self.message(channel, "%s -> That isn't a valid choice for the %s bidwar." % (
                                tags['display-name'], title), isWhisper)
                            return

                        for word in bannedWords:
                            if word in theirchoiceL:
                                self.message(channel, ".timeout %s 300" % sender, isWhisper)
                                self.message(channel,
                                             "%s -> No vulgar choices allowed (warning)" % tags['display-name'],
                                             isWhisper)
                                return

                        if points < openEntryMinimum:
                            self.message(channel,
                                         "%s -> You must contribute at least %d points to add a new choice to this bidwar!" % (
                                             tags['display-name'], openEntryMinimum), isWhisper)
                            return

                        if len(theirchoice) > openEntryMaxLength:
                            self.message(channel,
                                         "%s -> The maximum length of a choice in the %s bidwar is %d characters." % (
                                             tags['display-name'], title, openEntryMaxLength), isWhisper)
                            return

                        # all clear, add it
                        addPoints(tags['user-id'], -points)
                        actionTime = current_milli_time()
                        qargs = [warid, theirchoice, points, actionTime, tags['user-id'], actionTime, tags['user-id']]
                        cur.execute(
                            "INSERT INTO bidWarChoices (warID, choice, amount, created, creator, lastVote, lastVoter) VALUES(%s, %s, %s, %s, %s, %s, %s)",
                            qargs)
                    else:
                        # already existing choice, just vote for it
                        addPoints(tags['user-id'], -points)
                        qargs = [points, current_milli_time(), tags['user-id'], warid, theirchoiceL]
                        cur.execute(
                            "UPDATE bidWarChoices SET amount = amount + %s, lastVote = %s, lastVoter = %s WHERE warID = %s AND choice = %s",
                            qargs)

                    self.message(channel, "%s -> Successfully added %d points to %s in the %s bidwar." % (
                        tags['display-name'], points, theirchoice, title), isWhisper)
                    return

            if command == "incentives" and (isMarathonChannel or isWhisper):
                with db.cursor() as cur:
                    cur.execute("SELECT id, title, amount, required FROM incentives WHERE status = 'open'")
                    incentives = []
                    incnum = 0
                    for ic in cur.fetchall():
                        incnum += 1
                        if ic[2] >= ic[3]:
                            incentives.append("%s%s (%s) - MET!" % ("; " if incnum > 1 else "", ic[1], ic[0]))
                        else:
                            incentives.append(
                                "%s%s (%s) - %d/%d points" % ("; " if incnum > 1 else "", ic[1], ic[0], ic[2], ic[3]))

                    if len(incentives) == 0:
                        self.message(channel,
                                     "%s, there are no incentives currently open right now." % tags['display-name'],
                                     isWhisper)
                    else:
                        incentives.append(
                            ". !donate <id> <points> to contribute to an incentive (id is the text in brackets)")
                        messages = ["Current Open Incentives: "]
                        for inc in incentives:
                            if len(messages[-1]) + len(inc) > 400:
                                messages.append(inc)
                            else:
                                messages[-1] += inc
                        for message in messages:
                            self.message(channel, message, isWhisper)

                    return

            if command == "donate" and isMarathonChannel:
                if len(args) != 2:
                    self.message(channel,
                                 "Usage: !donate <id> <points> (!incentives to see a list of incentives / IDs)",
                                 isWhisper)
                    return

                with db.cursor() as cur:
                    cur.execute("SELECT id, title, amount, required FROM incentives WHERE id = %s", [args[0]])
                    incentive = cur.fetchone()

                    if incentive is None:
                        self.message(channel, "%s -> Invalid incentive ID." % tags['display-name'], isWhisper)
                        return

                    incid = incentive[0]
                    title = incentive[1]
                    currAmount = incentive[2]
                    required = incentive[3]

                    if currAmount >= required:
                        self.message(channel,
                                     "%s -> The %s incentive has already been met!" % (tags['display-name'], title),
                                     isWhisper)
                        return

                    try:
                        points = int(args[1])
                    except ValueError:
                        self.message(channel, "%s -> Invalid amount of points entered." % tags['display-name'],
                                     isWhisper)
                        return

                    if points <= 0:
                        self.message(channel, "%s -> Invalid amount of points entered." % tags['display-name'],
                                     isWhisper)
                        return

                    points = min(points, required - currAmount)

                    if not hasPoints(tags['user-id'], points):
                        self.message(channel, "%s -> You don't have that many points!" % tags['display-name'],
                                     isWhisper)
                        return

                    addPoints(tags['user-id'], -points)
                    cur.execute(
                        "UPDATE incentives SET amount = amount + %s, lastContribution = %s, lastContributor = %s WHERE id = %s",
                        [points, current_milli_time(), tags['user-id'], incid])

                    if points + currAmount >= required:
                        self.message(channel, "%s -> You successfully donated %d points and met the %s incentive!" % (
                            tags['display-name'], points, title), isWhisper)
                    else:
                        self.message(channel,
                                     "%s -> You successfully donated %d points towards the %s incentive. It needs %d more to be met." % (
                                         tags['display-name'], points, title, required - currAmount - points),
                                     isWhisper)

                    return

            if command == "upgrade":
                user = tags['user-id']

                if checkHandUpgrade(user):
                    messageForHandUpgrade(tags['user-id'], tags['display-name'], self, channel, isWhisper)
                    return
                    
                spendingsToNext = getNextUpgradeSpendings(user) - getSpendings(user)
                multiplier = 0.5 # TODO: Make multiplier configurable
                directPrice = max(int(spendingsToNext * multiplier), 1)

                if len(args) > 0 and args[0] == "buy":
                    if hasPoints(user, directPrice):
                        addPoints(user, directPrice * -1)
                        addSpending(user, spendingsToNext)
                        upgradeHand(user, gifted=False)
                        self.message(channel, "Successfully upgraded {user}'s hand for {price} points!".format(
                            user=tags['display-name'], price=str(directPrice)), isWhisper=isWhisper)
                        return
                    else:
                        self.message(channel,
                                     "{user}, you do not have enough points to force a hand upgrade now. It currently would cost you {price} points.".format(
                                         user=tags['display-name'], price=str(directPrice)), isWhisper=isWhisper)
                        return
                        
                currLimit = handLimit(tags['user-id'])
                msgArgs = (tags['display-name'], currLimit, spendingsToNext, currLimit + 1, directPrice)
                self.message(channel, ("%s, you have currently earnt a hand size of %d from pack spending. "+
                "Spend another %d points on boosters to earn space #%d, or use !upgrade buy to jump there directly for %d points.") % msgArgs, isWhisper)
                
                return
            if command == "announce":
                if not (sender in superadmins):
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
                    self.message(channel, "Usage: !search <name>[ from <series>]", isWhisper=isWhisper)
                    return
                if infoCommandAvailable(tags['user-id'], sender, tags['display-name'], self, channel, isWhisper):
                    try:
                        from_index = [arg.lower() for arg in args].index("from")
                        q = " ".join(args[:from_index])
                        series = " ".join(args[from_index + 1:])
                    except ValueError:
                        q = " ".join(args)
                        series = None
                    result = search(q, series)
                    if len(result) == 0:
                        self.message(channel, "No waifu found with that name.", isWhisper=isWhisper)
                        return

                    if len(result) > 8:
                        self.message(channel, "Too many results! ({amount}) - try a longer search query.".format(
                            amount=str(len(result))), isWhisper=isWhisper)
                        return

                    if len(result) == 1:
                        self.message(channel,
                                     "Found one waifu: [{w[id]}][{rarity}]{w[name]} from {w[series]} (use !lookup {w[id]} for more info)".format(
                                         w=result[0], rarity=config['rarity' + str(result[0]['base_rarity']) + 'Name']),
                                     isWhisper=isWhisper)
                    else:
                        self.message(channel, "Multiple results (Use !lookup for more details): " + ", ".join(
                            map(lambda waifu: str(waifu['id']), result)), isWhisper=isWhisper)

                    if sender not in superadmins:
                        useInfoCommand(tags['user-id'], sender, channel, isWhisper)

                return
            if command == "promote":
                self.message(channel,
                             "Promotion is now automatic when you gather enough copies of a waifu at the same rarity in your hand.",
                             isWhisper)
                return
            if command == "recheckpromos" and sender in superadmins:
                with db.cursor() as cur:
                    cur.execute("SELECT DISTINCT waifuid FROM has_waifu WHERE amount >= 2")
                    rows = cur.fetchall()
                    ids = [row[0] for row in rows]
                    attemptPromotions(*ids)
                    self.message(channel, "Rechecked promotions for %d waifus" % len(ids))
                return
            if command == "changepromos" and sender in superadmins:
                # assumes that the new promotion thresholds have already been inserted
                if "promoschanged" in config:
                    self.message(channel, "Already done.")
                    return
                with db.cursor() as cur:
                    cur.execute("SELECT has_waifu.userid, has_waifu.waifuid, has_waifu.rarity, has_waifu.amount, waifus.base_rarity FROM has_waifu JOIN waifus ON has_waifu.waifuid=waifus.id WHERE rarity < 7")
                    oldhands = cur.fetchall()
                    cur.execute("DELETE FROM has_waifu WHERE rarity < 7")
                    # recalculate qty
                    for oldrow in oldhands:
                        qty = oldrow[3] * (3 ** (oldrow[2] - oldrow[4]))
                        giveCard(oldrow[0], oldrow[1], oldrow[4], qty)
                    # recheck promos
                    cur.execute("SELECT DISTINCT waifuid FROM has_waifu WHERE amount >= 2")
                    rows = cur.fetchall()
                    ids = [row[0] for row in rows]
                    attemptPromotions(*ids)
                    # .done
                    config["promoschanged"] = "yes"
                    cur.execute("REPLACE INTO config(name, value) VALUES('promoschanged', 'yes')")
                return
                            
            if command == "bet":
                if len(args) < 1:
                    self.message(channel,
                                 "Usage: !bet <time> OR !bet status OR !bet packs OR (as channel owner) !bet open OR !bet start OR !bet end OR !bet cancel OR !bet results",
                                 isWhisper)
                    return
                canAdminBets = sender in superadmins or (sender in admins and isMarathonChannel)
                canManageBets = canAdminBets or str(tags["badges"]).find("broadcaster") > -1

                bet = parseBetTime(args[0])
                if bet:
                    if sender == channel[1:]:
                        self.message(channel, "You can't bet in your own channel, sorry!", isWhisper)
                        return
                    open = placeBet(channel, tags["user-id"], bet["total"])
                    if open:
                        self.message(channel,
                                     "Successfully entered {name}'s bet: {h}h {min}min {s}s {ms}ms".format(
                                         h=bet["hours"],
                                         min=bet["minutes"],
                                         s=bet["seconds"],
                                         ms=bet["ms"],
                                         name=tags['display-name']),
                                     isWhisper)
                    else:
                        self.message(channel, "The bets aren't open right now, sorry!", isWhisper)
                    return
                else:
                    subcmd = str(args[0]).lower()
                    betPrizeNames = {config["betPrizeTier%dToken" % tier]: config["betPrizeTier%dName" % tier] for tier
                                     in range(1, 8)}

                    if canManageBets and subcmd == "open":
                        if openBet(channel):
                            self.message(channel, "Bets are now open! Use !bet HH:MM:SS(.ms) to submit your bet!")
                        else:
                            self.message(channel,
                                         "There is already a prediction contest in progress in your channel! Use !bet status to check what to do next!")
                        return
                    elif canManageBets and subcmd == "start":
                        if startBet(channel):
                            self.message(channel, "Taking current time as start time! Good Luck! Bets are now closed.")
                        else:
                            self.message(channel,
                                         "There wasn't an open prediction contest in your channel! Use !bet status to check current contest status.")
                        return
                    elif canManageBets and subcmd == "end":
                        resultData = endBet(str(channel).lower())
                        if resultData is None:
                            self.message(channel,
                                         "There wasn't a prediction contest in progress in your channel! Use !bet status to check current contest status.")
                        else:
                            formattedTime = formatTimeDelta(resultData["result"])
                            winners = resultData["winners"]
                            winnerNames = []
                            for n in range(3):
                                winnerNames.append(winners[n]["name"] if len(winners) > n else "No-one")
                            self.message(channel,
                                         "Contest has ended in {time}! The top 3 closest were: {first}, {second}, {third}".format(
                                             time=formattedTime, first=winnerNames[0], second=winnerNames[1],
                                             third=winnerNames[2]))
                        return
                    elif canManageBets and subcmd == "cancel":
                        if cancelBet(channel):
                            self.message(channel,
                                         "Cancelled the current prediction contest! Start a new one with !bet open.")
                        else:
                            self.message(channel,
                                         "There was no open or in-progress prediction contest in your channel! Start a new one with !bet open.")
                        return
                    elif subcmd == "status":
                        # check for most recent betting
                        cur = db.cursor()
                        cur.execute(
                            "SELECT id, status, startTime, endTime FROM bets WHERE channel = %s ORDER BY id DESC LIMIT 1",
                            [channel])
                        betRow = cur.fetchone()
                        if betRow is None:
                            if canManageBets:
                                self.message(channel,
                                             "No time prediction contests have been done in this channel yet. Use !bet open to open one.")
                            else:
                                self.message(channel, "No time prediction contests have been done in this channel yet.")
                        elif betRow[1] == 'cancelled':
                            if canManageBets:
                                self.message(channel,
                                             "No time prediction contest in progress. The most recent contest was cancelled. Use !bet open to open a new one.")
                            else:
                                self.message(channel,
                                             "No time prediction contest in progress. The most recent contest was cancelled.")
                        else:
                            cur.execute("SELECT COUNT(*) FROM placed_bets WHERE betid = %s", [betRow[0]])
                            numBets = cur.fetchone()[0]
                            cur.execute("SELECT bet FROM placed_bets WHERE userid = %s AND betid = %s", [tags["user-id"], betRow[0]])
                            placedBets = cur.fetchall()
                            placedBet = None if len(placedBets) == 0 else placedBets[0][0]
                            hasBet = placedBet is not None
                            if betRow[1] == 'open':
                                if canManageBets:
                                    if hasBet:
                                        self.message(channel,
                                                     "Bets are currently open for a new contest. %d bets have been placed so far. !bet start to close bets and start the run timer. Your bet currently is %s" % (numBets, formatTimeDelta(placedBet)))

                                    else:
                                        self.message(channel,
                                                     "Bets are currently open for a new contest. %d bets have been placed so far. !bet start to close bets and start the run timer. You have not bet yet." % numBets)


                                else:
                                    if hasBet:
                                        self.message(channel,
                                                 "Bets are currently open for a new contest. %d bets have been placed so far. Your bet currently is %s" % (numBets, formatTimeDelta(placedBet)))
                                    else:
                                        self.message(channel,
                                                 "Bets are currently open for a new contest. %d bets have been placed so far." % numBets)
                                        


                            elif betRow[1] == 'started':
                                elapsed = current_milli_time() - betRow[2]
                                formattedTime = formatTimeDelta(elapsed)
                                if canManageBets:
                                    if hasBet:
                                        self.message(channel,
                                                 "Run in progress - elapsed time %s. %d bets were placed. !bet end to end the run timer and determine results. Your bet is %s" % (
                                                     formattedTime, numBets, formatTimeDelta(placedBet)))
                                    else:
                                        self.message(channel,
                                                     "Run in progress - elapsed time %s. %d bets were placed. !bet end to end the run timer and determine results. You did not bet." % (
                                                         formattedTime, numBets))
                                else:
                                    if hasBet:
                                        self.message(channel, "Run in progress - elapsed time %s. %d bets were placed. Your bet is %s" % (
                                        formattedTime, numBets, formatTimeDelta(placedBet)))
                                    else:
                                        self.message(channel,
                                                     "Run in progress - elapsed time %s. %d bets were placed. You did not bet." % (
                                                         formattedTime, numBets))
                            else:
                                formattedTime = formatTimeDelta(betRow[3] - betRow[2])
                                if canManageBets:
                                    self.message(channel,
                                                 "No time prediction contest in progress. The most recent contest ended in %s with %d bets placed. Use !bet results to see full results or !bet open to open a new one." % (
                                                     formattedTime, numBets))
                                else:
                                    self.message(channel,
                                                 "No time prediction contest in progress. The most recent contest ended in %s with %d bets placed." % (
                                                     formattedTime, numBets))
                        cur.close()
                        return
                    elif canManageBets and subcmd == "results":
                        cur = db.cursor()
                        cur.execute("SELECT id, status FROM bets WHERE channel = %s ORDER BY id DESC LIMIT 1",
                                    [channel])
                        betRow = cur.fetchone()
                        if betRow is None:
                            self.message(channel, "No time prediction contests have been done in this channel yet.",
                                         isWhisper)
                        elif betRow[1] == 'cancelled':
                            self.message(channel, "The most recent contest in this channel was cancelled.", isWhisper)
                        elif betRow[1] == 'open' or betRow[1] == 'started':
                            self.message(channel,
                                         "There is a contest currently in progress in this channel, check !bet status.",
                                         isWhisper)
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
                                    formattedDelta = ("-" if row["timedelta"] < 0 else "+") + formatTimeDelta(
                                        abs(row["timedelta"]))
                                    formattedBet = formatTimeDelta(row["bet"])
                                    entry = "({place}) {name} - {time} ({delta}); ".format(place=place,
                                                                                           name=row["name"],
                                                                                           time=formattedBet,
                                                                                           delta=formattedDelta)
                                    if len(entry) + len(messages[-1]) > 400:
                                        messages.append(entry)
                                    else:
                                        messages[-1] += entry

                            for message in messages:
                                self.message(channel, message, isWhisper)
                        cur.close()
                        return
                    elif subcmd == "forcereset" and canAdminBets:
                        # change a started bet to open, preserving all current bets made
                        with db.cursor() as cur:
                            cur.execute("SELECT id, status FROM bets WHERE channel = %s ORDER BY id DESC LIMIT 1",
                                        [channel])
                            betRow = cur.fetchone()

                            if betRow is None or betRow[1] != 'started':
                                self.message(channel, "There is no bet in progress in this channel.", isWhisper)
                            else:
                                cur.execute("UPDATE bets SET status = 'open', startTime = NULL WHERE id = %s",
                                            [betRow[0]])
                                self.message(channel, "Reset the bet in progress in this channel to open status.",
                                             isWhisper)
                        return
                    elif subcmd == "changetime" and canAdminBets:
                        # change the completion time of a completed bet
                        if len(args) < 2:
                            self.message(channel, "Usage: !bet changetime <time> (same format as !bet)", isWhisper)
                            return

                        ctdata = parseBetTime(args[1])
                        if not ctdata:
                            self.message(channel, "Usage: !bet changetime <time> (same format as !bet)", isWhisper)
                            return

                        with db.cursor() as cur:
                            cur.execute("SELECT id, status FROM bets WHERE channel = %s ORDER BY id DESC LIMIT 1",
                                        [channel])
                            betRow = cur.fetchone()

                            if betRow is None or betRow[1] != 'completed':
                                self.message(channel, "There is no just-completed bet in this channel.", isWhisper)
                            else:
                                cur.execute("UPDATE bets SET endTime = startTime + %s WHERE id = %s",
                                            [ctdata["total"], betRow[0]])
                                self.message(channel,
                                             "Successfully changed end time to: {h}h {min}min {s}s {ms}ms".format(
                                                 h=ctdata["hours"],
                                                 min=ctdata["minutes"],
                                                 s=ctdata["seconds"],
                                                 ms=ctdata["ms"]),
                                             isWhisper)
                        return
                    elif subcmd == "forceenter" and canAdminBets:
                        # enter another user into a bet
                        if len(args) < 3:
                            self.message(channel, "Usage: !bet forceenter <username> <time>", isWhisper)
                            return

                        tdata = parseBetTime(args[2])
                        if not tdata:
                            self.message(channel, "Usage: !bet forceenter <username> <time>", isWhisper)
                            return

                        enteruser = args[1].strip().lower()

                        if enteruser == sender:
                            self.message(channel, "You can't force-enter your own time, pls.", isWhisper)
                            return

                        with db.cursor() as cur:
                            cur.execute("SELECT id, status FROM bets WHERE channel = %s ORDER BY id DESC LIMIT 1",
                                        [channel])
                            betRow = cur.fetchone()

                            if betRow is None or betRow[1] not in ("open", "started"):
                                self.message(channel,
                                             "There is not a bet in this channel that is eligible for force-entries.",
                                             isWhisper)
                            else:
                                # check username
                                cur.execute("SELECT id FROM users WHERE name = %s", [enteruser])
                                enteridrow = cur.fetchone()
                                if enteridrow is None:
                                    self.message(channel, "I don't recognize that username.", isWhisper=isWhisper)
                                    return
                                enterid = int(enteridrow[0])
                                cur.execute(
                                    "REPLACE INTO placed_bets (betid, userid, bet, updated) VALUE (%s, %s, %s, %s)",
                                    [betRow[0], enterid, tdata["total"], current_milli_time()])
                                self.message(channel,
                                             "Successfully entered {user}'s bet: {h}h {min}min {s}s {ms}ms".format(
                                                 h=tdata["hours"],
                                                 min=tdata["minutes"],
                                                 s=tdata["seconds"],
                                                 ms=tdata["ms"],
                                                 user=enteruser),
                                             isWhisper)
                        return
                    elif subcmd == "payout" and canAdminBets:
                        # pay out most recent bet in this channel
                        cur = db.cursor()
                        cur.execute("SELECT COALESCE(MAX(paidAt), 0) FROM bets WHERE channel = %s LIMIT 1", [channel])
                        lastPayout = cur.fetchone()[0]
                        currTime = current_milli_time()
                        if lastPayout > currTime - 79200000 and not isMarathonChannel:
                            a = datetime.timedelta(milliseconds=lastPayout + 79200000 - currTime, microseconds=0)
                            datestring = "{0}".format(a).split(".")[0]
                            self.message(channel, "Bet payout may be used again in this channel in %s." % datestring,
                                         isWhisper)
                            cur.close()
                            return

                        cur.execute("SELECT id, status, endTime FROM bets WHERE channel = %s AND status IN('completed', 'paid', 'cancelled') ORDER BY id DESC LIMIT 1",
                                    [channel])
                        betRow = cur.fetchone()
                        if betRow is None or (betRow[1] != 'paid' and betRow[1] != 'completed'):
                            self.message(channel,
                                         "There is no pending time prediction contest to be paid out for this channel.",
                                         isWhisper)
                        elif betRow[1] == 'paid':
                            self.message(channel, "The most recent contest in this channel was already paid out.",
                                         isWhisper)
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

                            # calculate first run of prizes
                            prizes = defaultdict(list)
                            place = 0
                            for winner in resultData["winners"]:
                                place += 1

                                if abs(winner["timedelta"]) < 10 and resultData["result"] >= 1800000:
                                    prizeTier = 7
                                elif abs(winner["timedelta"]) < 1000 and resultData["result"] >= 1800000:
                                    prizeTier = 6
                                else:
                                    prizeTier = 1

                                    if place == 1:
                                        prizeTier += 1
                                    if place <= 3 and numEntries >= 10:
                                        prizeTier += 1
                                    if place <= numEntries // 2:
                                        prizeTier += 1
                                    if abs(winner["timedelta"]) < 60000 and resultData["result"] >= 3600000:
                                        prizeTier += 1
                                    if isMarathonChannel and prizeTier < 5:
                                        prizeTier += 1

                                prizeToken = config["betPrizeTier%dToken" % prizeTier]
                                prizePack = config["betPrizeTier%dBooster" % prizeTier]
                                prizeName = config["betPrizeTier%dName" % prizeTier]

                                prizes[prizeToken].append(winner["name"])
                                cur.execute(
                                    "INSERT INTO tokens (token, boostername, claimable, bet_prize, type, only_redeemable_by) VALUES(%s, %s, 1, 1, 'single', %s)",
                                    [prizeToken, prizePack, winner["id"]])
                                cur.execute("UPDATE placed_bets SET prizeToken = %s WHERE betid = %s AND userid = %s",
                                            [prizeToken, betRow[0], winner["id"]])

                            # broadcaster prize
                            # run length in hours * 500, rounded to nearest 50
                            # scales up a bit as the hours go on
                            runHours = resultData["result"] / 3600000.0
                            bcPrize = min(runHours, 5) * 500 + min(max(runHours - 5, 0), 5) * 750 + max(runHours - 10,
                                                                                                        0) * 1000
                            bcPrize = max(int(round(bcPrize / 50.0) * 50), 50)

                            cur.execute("UPDATE users SET points = points + %s WHERE name = %s", [bcPrize, channel[1:]])
                            # start cooldown for next bet payout at max(endTime, lastPayout + 22h)
                            payoutTime = max(betRow[2], lastPayout + 79200000)
                            cur.execute(
                                "UPDATE bets SET status = 'paid', paidBroadcaster = %s, paidAt = %s WHERE id = %s",
                                [bcPrize, payoutTime, betRow[0]])

                            messages = ["Paid out the following prizes: "]
                            for prizeToken in prizes:
                                msg = betPrizeNames[prizeToken] + " - " + ", ".join(prizes[prizeToken]) + "; "
                                if len(messages[-1] + msg) > 400:
                                    messages.append(msg)
                                else:
                                    messages[-1] += msg

                            msgBC = "{points} points - {name} (broadcaster)".format(name=channel[1:], points=bcPrize)
                            if len(messages[-1] + msgBC) > 400:
                                messages.append(msgBC)
                            else:
                                messages[-1] += msgBC

                            for message in messages:
                                self.message(channel, message, isWhisper)

                            # alert each person individually as well
                            # sent after the messages to the channel itself deliberately
                            for prizeToken in prizes:
                                for winnerName in prizes[prizeToken]:
                                    whisperArgs = (betPrizeNames[prizeToken], channel[1:], prizeToken)
                                    self.message('#' + winnerName,
                                                 "You won a %s from the bet in %s's channel. Redeem it in any chat with !redeem %s" % whisperArgs,
                                                 True)

                        cur.close()
                        return
                    elif subcmd == "packs":
                        # check any packs they might have left to claim
                        cur = db.cursor()
                        cur.execute(
                            "SELECT token, COUNT(*) FROM tokens WHERE claimable = 1 AND bet_prize = 1 AND only_redeemable_by = %s GROUP BY token",
                            [tags['user-id']])
                        prizes = cur.fetchall()

                        if len(prizes) > 0:
                            prizeStr = ", ".join("%s%s (!redeem %s)" % (
                            betPrizeNames[row[0]], " x%d" % row[1] if row[1] > 1 else "", row[0]) for row in prizes)
                            self.message(channel, "%s, you have the following unclaimed bet prizes: %s" % (
                            tags['display-name'], prizeStr), isWhisper)
                        else:
                            self.message(channel,
                                         "%s, you have no unclaimed bet prizes right now. Participate in more bets to earn free packs!" %
                                         tags['display-name'], isWhisper)

                        cur.close()
                        return
                    else:
                        self.message(channel,
                                     "Usage: !bet <time> OR !bet status OR (as channel owner) !bet open OR !bet start OR !bet end OR !bet cancel OR !bet results",
                                     isWhisper)
                    return
            if command == "import" and sender in superadmins:
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
                        self.message(channel,
                                     "Error processing waifu data from lines: %s. Please fix formatting and try again." % ", ".join(
                                         str(lineno) for lineno in errorlines), isWhisper)
                        return
                    else:
                        cur = db.cursor()
                        cur.executemany("INSERT INTO waifus (Name, image, base_rarity, series) VALUES(%s, %s, %s, %s)",
                                        [(waifu["name"], waifu["link"], int(waifu["rarity"]), waifu["series"].strip())
                                         for waifu in addwaifus])
                        cur.close()
                        self.message(channel, "Successfully added %d waifus to the database." % len(addwaifus),
                                     isWhisper)
                        return
                except Exception:
                    self.message(channel, "Error loading waifu data.", isWhisper)
                    logger.error("Error importing waifus: %s", str(sys.exc_info()))
                    return
            if command == "sets" or command == "set":
                if len(args) == 0:
                    self.message(channel,
                                 "Available sets: %s/sets?user=%s . !sets claim to claim all sets you are eligible for." % (
                                     config["siteHost"], sender.lower()), isWhisper=isWhisper)
                    return
                subcmd = args[0].lower()
                if subcmd == "rarity":
                    self.message(channel, "Rarity sets have been suspended for the time being. They may return in some form at some point.", isWhisper)
                    return
                elif subcmd == "claim":
                    cur = db.cursor()
                    claimed = 0

                    # normal sets
                    cur.execute(
                        "SELECT DISTINCT sets.id, sets.name, sets.reward FROM sets WHERE sets.claimed_by IS NULL AND sets.id NOT IN (SELECT DISTINCT setID FROM set_cards LEFT OUTER JOIN (SELECT * FROM has_waifu JOIN users ON has_waifu.userid = users.id WHERE users.id = %s) AS a ON waifuid = cardID JOIN sets ON set_cards.setID = sets.id JOIN waifus ON cardID = waifus.id WHERE a.name IS NULL)",
                        [tags["user-id"]])
                    rows = cur.fetchall()
                    for row in rows:
                        claimed += 1
                        cur.execute("UPDATE sets SET claimed_by = %s, claimed_at = %s WHERE sets.id = %s",
                                    [tags["user-id"], current_milli_time(), row[0]])
                        addPoints(tags["user-id"], int(row[2]))
                        badgeid = addBadge(row[1], config["setBadgeDescription"], config["setBadgeDefaultImage"])
                        giveBadge(tags['user-id'], badgeid)
                        self.message(channel,
                                     "Successfully claimed the Set {set} and rewarded {user} with {reward} points!".format(
                                         set=row[1], user=tags["display-name"], reward=row[2]), isWhisper)
                        cur.execute(
                            "SELECT waifus.name FROM set_cards INNER JOIN waifus ON set_cards.cardID = waifus.id WHERE setID = %s",
                            [row[0]])
                        cards = [sc[0] for sc in cur.fetchall()]
                        threading.Thread(target=sendSetAlert,
                                         args=(channel, tags["display-name"], row[1], cards)).start()

                    if claimed == 0:
                        self.message(channel,
                                     "You do not have any completed sets that are available to be claimed. !sets to check progress.",
                                     isWhisper=isWhisper)
                        return

                    cur.close()
                    return
                else:
                    self.message(channel, "Usage: !sets OR !sets claim", isWhisper=isWhisper)
                    return
            if command == "debug" and sender in superadmins:
                if debugMode:
                    updateBoth("Hyperdimension Neptunia", "Testing title updates.")
                    self.message(channel, "Title and game updated for testing purposes")
                else:
                    self.message(channel, "Debug mode is off. Debug command disabled.")
                return
            if command == "nepcord":
                self.message(channel,
                             "To join the discussion in the official Waifu TCG Discord Channel, go to %s/discord" %
                             config["siteHost"], isWhisper=isWhisper)
                return
            if command == "giveaway":
                cur = db.cursor()
                if len(args) == 0 or args[0].lower() == 'enter':
                    # check for a giveaway to enter
                    cur.execute("SELECT id, status FROM giveaways ORDER BY id DESC LIMIT 1")
                    giveaway_info = cur.fetchone()
                    if giveaway_info is None or giveaway_info[1] == 'closed':
                        self.message(channel, "There is not an open giveaway right now.", isWhisper)
                        cur.close()
                        return

                    # look for our own entry already existing
                    cur.execute("SELECT COUNT(*) FROM giveaway_entries WHERE giveawayid = %s AND userid = %s",
                                [giveaway_info[0], tags['user-id']])
                    entry_count = cur.fetchone()[0] or 0
                    if entry_count != 0:
                        self.message(channel,
                                     "%s -> You have already entered the current giveaway." % tags["display-name"],
                                     isWhisper)
                        cur.close()
                        return

                    # add an entry
                    cur.execute("INSERT INTO giveaway_entries (giveawayid, userid, timestamp) VALUES(%s, %s, %s)",
                                [giveaway_info[0], tags['user-id'], current_milli_time()])
                    self.message(channel,
                                 "%s -> You have been entered into the current giveaway." % tags["display-name"],
                                 isWhisper)
                    cur.close()
                    return

                if sender not in superadmins:
                    return
                subcmd = args[0].lower()
                if subcmd == 'open':
                    cur.execute("SELECT id, status FROM giveaways ORDER BY id DESC LIMIT 1")
                    giveaway_info = cur.fetchone()
                    if giveaway_info is not None and giveaway_info[1] != 'closed':
                        self.message(channel, "There is already an open giveaway right now.", isWhisper)
                        cur.close()
                        return
                    # create a new giveaway
                    cur.execute("INSERT INTO giveaways (opened, creator, status) VALUES(%s, %s, 'open')",
                                [current_milli_time(), tags['user-id']])
                    self.message(channel, "Started a new giveaway!", isWhisper)
                    cur.close()
                    return
                if subcmd == 'close':
                    cur.execute("SELECT id, status FROM giveaways ORDER BY id DESC LIMIT 1")
                    giveaway_info = cur.fetchone()
                    if giveaway_info is None or giveaway_info[1] == 'closed':
                        self.message(channel, "There is not an open giveaway right now.", isWhisper)
                        cur.close()
                        return
                    cur.execute("UPDATE giveaways SET closed = %s, status = 'closed' WHERE id = %s",
                                [current_milli_time(), giveaway_info[0]])
                    self.message(channel, "Closed entries for the current giveaway!", isWhisper)
                    cur.close()
                    return
                if subcmd == 'pick':
                    cur.execute("SELECT id, status FROM giveaways ORDER BY id DESC LIMIT 1")
                    giveaway_info = cur.fetchone()
                    if giveaway_info is None:
                        self.message(channel, "There hasn't been a giveaway yet.", isWhisper)
                        cur.close()
                        return

                    if len(args) < 2:
                        self.message(channel, "Usage: !giveaway pick <amount of winners>", isWhisper)
                        cur.close()
                        return

                    try:
                        num_winners = int(args[1])
                    except Exception:
                        self.message(channel, "Usage: !giveaway pick <amount of winners>", isWhisper)
                        cur.close()
                        return

                    cur.execute(
                        "SELECT giveaway_entries.userid, users.name FROM giveaway_entries INNER JOIN users ON giveaway_entries.userid = users.id WHERE giveaway_entries.giveawayid = %s AND giveaway_entries.winner = 0 ORDER BY RAND() LIMIT " + str(
                            num_winners), [giveaway_info[0]])
                    winners = cur.fetchall()

                    if len(winners) != num_winners:
                        self.message(channel,
                                     "There aren't enough entrants left to pick %d more winners! Try %d or fewer." % (
                                         num_winners, len(winners)), isWhisper)
                        cur.close()
                        return

                    winner_ids = [row[0] for row in winners]
                    inTemplate = ",".join(["%s"] * len(winner_ids))
                    winner_names = ", ".join(row[1] for row in winners)
                    cur.execute(
                        "UPDATE giveaway_entries SET winner = 1, when_won = %s WHERE giveawayid = %s AND userid IN (" + inTemplate + ")",
                        [current_milli_time(), giveaway_info[0]] + winner_ids)
                    self.message(channel, "Picked %d winners for the giveaway: %s!" % (num_winners, winner_names),
                                 isWhisper)
                    cur.close()
                    return
            if command == "raffle":
                with db.cursor() as cur:
                    cur.execute("SELECT id, status, ticket_price, max_tickets FROM raffles ORDER BY id DESC LIMIT 1")
                    raffle_info = cur.fetchone()

                    if len(args) == 0:
                        # check for info
                        if raffle_info is None or raffle_info[1] == 'done':
                            self.message(channel, "No raffle is open at this time.", isWhisper)
                            return
                        else:
                            cur.execute(
                                "SELECT num_tickets, num_winners, won_grand FROM raffle_tickets WHERE raffleid = %s AND userid = %s",
                                [raffle_info[0], tags['user-id']])
                            my_tickets = cur.fetchone()

                            if raffle_info[1] == 'open':
                                if my_tickets is None:
                                    self.message(channel,
                                                 "There is a raffle currently open. You can buy up to %d tickets for %d points each using !raffle buy <amount>. You don't have any tickets right now." % (
                                                 raffle_info[3], raffle_info[2]), isWhisper)
                                elif my_tickets[0] < raffle_info[3]:
                                    self.message(channel,
                                                 "There is a raffle currently open. You have bought %d tickets so far. You can buy up to %d more for %d points each using !raffle buy <amount>." % (
                                                 my_tickets[0], raffle_info[3] - my_tickets[0], raffle_info[2]),
                                                 isWhisper)
                                else:
                                    self.message(channel,
                                                 "There is a raffle currently open. You are already at the limit of %d tickets." % (
                                                 raffle_info[3]), isWhisper)
                            else:
                                # raffle in process of drawing
                                if my_tickets is None:
                                    self.message(channel,
                                                 "The current raffle is in the process of being drawn. Unfortunately, you didn't buy any tickets! Try again next raffle.")
                                else:
                                    if my_tickets[2] != 0:
                                        self.message(channel,
                                                     "The current raffle is in the process of being drawn. So far you have won %d minor prizes and a grand prize from your %d tickets!" % (
                                                     my_tickets[1] - 1, my_tickets[0]))
                                    else:
                                        self.message(channel,
                                                     "The current raffle is in the process of being drawn. So far, you have won %d minor prizes and no grand prize from your %d tickets." % (
                                                     my_tickets[1], my_tickets[0]))

                            return

                    subcmd = args[0].lower()
                    if subcmd == 'buy':
                        if raffle_info[1] != 'open':
                            self.message(channel,
                                         "Raffle ticket purchases aren't open right now. Use !raffle to check the overall status.")
                            return

                        if len(args) < 2:
                            self.message(channel, "Usage: !raffle buy <amount>", isWhisper)
                            return

                        try:
                            tickets = int(args[1])
                            assert tickets >= 0
                        except Exception:
                            self.message(channel, "Invalid amount of tickets specified.", isWhisper)
                            return

                        cur.execute(
                            "SELECT num_tickets, num_winners, won_grand FROM raffle_tickets WHERE raffleid = %s AND userid = %s",
                            [raffle_info[0], tags['user-id']])
                        my_tickets = cur.fetchone()

                        can_buy = raffle_info[3] if my_tickets is None else raffle_info[3] - my_tickets[0]
                        cost = tickets * raffle_info[2]

                        if tickets > can_buy:
                            if can_buy == 0:
                                self.message(channel,
                                             "%s, you're already at the maximum of %d tickets for this raffle. Please wait for the drawing." % (
                                             tags['display-name'], raffle_info[3]), isWhisper)
                            else:
                                self.message(channel,
                                             "%s, you can only buy %d more tickets for this raffle. Please adjust your purchase." % (
                                             tags['display-name'], can_buy), isWhisper)
                            return

                        if not hasPoints(tags['user-id'], cost):
                            self.message(channel, "%s, you don't have the %d points required to buy %d tickets." % (
                            tags['display-name'], cost, tickets), isWhisper)
                            return

                        # okay, buy the tickets
                        addPoints(tags['user-id'], -cost)
                        if my_tickets is None:
                            cur.execute(
                                "INSERT INTO raffle_tickets (raffleid, userid, num_tickets, created) VALUES(%s, %s, %s, %s)",
                                [raffle_info[0], tags['user-id'], tickets, current_milli_time()])
                        else:
                            cur.execute(
                                "UPDATE raffle_tickets SET num_tickets = num_tickets + %s, updated = %s WHERE raffleid = %s AND userid = %s",
                                [tickets, current_milli_time(), raffle_info[0], tags['user-id']])

                        self.message(channel, "%s, you successfully bought %d raffle tickets for %d points." % (
                        tags['display-name'], tickets, cost), isWhisper)
                        return

                    if sender not in superadmins:
                        self.message(channel, "Usage: !raffle / !raffle buy <amount>", isWhisper)
                        return

                    if subcmd == 'open':
                        if raffle_info is not None and raffle_info[1] != 'done':
                            self.message(channel, "There is already an incomplete raffle right now.", isWhisper)
                            return
                        if len(args) < 3:
                            self.message(channel, "Usage: !raffle open <points per ticket> <max tickets>", isWhisper)
                            return
                        try:
                            points_per_ticket = int(args[1])
                            max_tickets = int(args[2])
                            assert max_tickets > 0 and max_tickets < 100
                            assert points_per_ticket >= 100
                        except Exception:
                            self.message(channel,
                                         "Invalid arguments. Usage: !raffle open <points per ticket> <max tickets>",
                                         isWhisper)
                            return

                        # create a new raffle
                        cur.execute(
                            "INSERT INTO raffles (opened, creator, status, ticket_price, max_tickets) VALUES(%s, %s, 'open', %s, %s)",
                            [current_milli_time(), tags['user-id'], points_per_ticket, max_tickets])
                        self.message(channel, "Started a new raffle!", isWhisper)
                        cur.close()
                        return

                    if subcmd == 'close':
                        if raffle_info is None or raffle_info[1] != 'open':
                            self.message(channel, "There is not an open raffle right now.", isWhisper)
                            return
                        cur.execute("UPDATE raffles SET closed = %s, status = 'drawing' WHERE id = %s",
                                    [current_milli_time(), raffle_info[0]])
                        self.message(channel, "Closed ticket purchases for the current raffle!", isWhisper)
                        return

                    if subcmd == 'complete':
                        if raffle_info is None or raffle_info[1] != 'drawing':
                            self.message(channel, "There is not a raffle in the process of drawing right now.",
                                         isWhisper)
                            return
                        cur.execute("UPDATE raffles SET status = 'done' WHERE id = %s",
                                    [current_milli_time(), raffle_info[0]])
                        self.message(channel, "Closed drawing for the current raffle!", isWhisper)
                        return

                    if subcmd == 'pick' or subcmd == 'draw':
                        if raffle_info is None or raffle_info[1] != 'drawing':
                            self.message(channel, "There is not a raffle in the process of drawing right now.",
                                         isWhisper)
                            return

                        if len(args) < 2:
                            self.message(channel, "Usage: !raffle pick <amount of winners>", isWhisper)
                            return

                        winners = []

                        try:
                            num_winners = int(args[1])
                            assert num_winners > 0
                        except Exception:
                            self.message(channel, "Usage: !raffle pick <amount of winners>", isWhisper)
                            return

                        for i in range(num_winners):
                            cur.execute(
                                "SELECT raffle_tickets.userid, users.name FROM raffle_tickets INNER JOIN users ON raffle_tickets.userid = users.id WHERE raffle_tickets.raffleid = %s AND raffle_tickets.num_winners < raffle_tickets.num_tickets ORDER BY -LOG(1-RAND())/(num_tickets - num_winners) LIMIT 1",
                                [raffle_info[0]])
                            winner = cur.fetchone()
                            if winner is None:
                                # completely out of non-winning tickets
                                break

                            # add their name to the winner list
                            winners.append(winner[1])

                            # update their ticket entry
                            cur.execute(
                                "UPDATE raffle_tickets SET num_winners = num_winners + 1, updated = %s WHERE raffleid = %s AND userid = %s",
                                [current_milli_time(), raffle_info[0], winner[0]])

                        if len(winners) == 0:
                            self.message(channel,
                                         "Drew no new minor prize winners - the system is out of non-winning tickets!",
                                         isWhisper)
                        elif len(winners) < num_winners:
                            self.message(channel, "Drew %d minor prize winners (truncated) - %s !" % (
                            len(winners), ", ".join(winners)), isWhisper)
                        else:
                            self.message(channel,
                                         "Drew %d minor prize winners - %s !" % (len(winners), ", ".join(winners)),
                                         isWhisper)

                        return

                    if subcmd == 'pickgrand' or subcmd == 'drawgrand':
                        if raffle_info is None or raffle_info[1] != 'drawing':
                            self.message(channel, "There is not a raffle in the process of drawing right now.",
                                         isWhisper)
                            return

                        if len(args) >= 2:
                            self.message(channel, "!raffle drawgrand only draws one winner at once.", isWhisper)
                            return

                        cur.execute(
                            "SELECT raffle_tickets.userid, users.name FROM raffle_tickets INNER JOIN users ON raffle_tickets.userid = users.id WHERE raffle_tickets.raffleid = %s AND raffle_tickets.num_winners < raffle_tickets.num_tickets AND raffle_tickets.won_grand = 0 ORDER BY -LOG(1-RAND())/(num_tickets - num_winners) LIMIT 1",
                            [raffle_info[0]])
                        winner = cur.fetchone()
                        if winner is None:
                            # completely out of non-winning tickets
                            self.message(channel,
                                         "Could not draw a new grand prize winner as there are no applicable users left!",
                                         isWhisper)
                            return

                        # update their ticket entry
                        cur.execute(
                            "UPDATE raffle_tickets SET num_winners = num_winners + 1, won_grand = 1, updated = %s WHERE raffleid = %s AND userid = %s",
                            [current_milli_time(), raffle_info[0], winner[0]])

                        self.message(channel, "Drew a new grand prize winner: %s!" % winner[1])
                        return
            if command == "bounty":
                if len(args) == 0:
                    self.message(channel,
                                 "Usage: !bounty <ID> <amount> / !bounty list / !bounty check <ID> / !bounty cancel <ID>",
                                 isWhisper=isWhisper)
                    return
                subcmd = args[0].lower()

                # support !bounty ID amount to place an order
                if subcmd not in ['check', 'place', 'add', 'list', 'cancel']:
                    args = ['place'] + args
                    subcmd = 'place'

                if subcmd == "check":
                    if len(args) != 2:
                        self.message(channel, "Usage: !bounty check <ID>", isWhisper=isWhisper)
                        return

                    if infoCommandAvailable(tags['user-id'], sender, tags['display-name'], self, channel, isWhisper):
                        try:
                            waifu = getWaifuById(args[1])
                            assert waifu is not None
                            assert waifu['can_lookup'] == 1

                            if waifu['base_rarity'] >= int(config["numNormalRarities"]):
                                self.message(channel, "Bounties cannot be placed on special waifus.", isWhisper)
                                return

                            if sender not in superadmins:
                                useInfoCommand(tags['user-id'], sender, channel, isWhisper)

                            with db.cursor() as cur:
                                cur.execute(
                                    "SELECT COUNT(*), COALESCE(MAX(amount), 0) FROM bounties WHERE waifuid = %s AND status='open'",
                                    [waifu['id']])
                                allordersinfo = cur.fetchone()

                                if allordersinfo[0] == 0:
                                    self.message(channel,
                                                 "[{id}] {name} has no bounties right now.".format(id=waifu['id'],
                                                                                                   name=waifu['name']),
                                                 isWhisper)
                                    return

                                cur.execute(
                                    "SELECT amount FROM bounties WHERE userid = %s AND waifuid = %s AND status='open'",
                                    [tags['user-id'], waifu['id']])
                                myorderinfo = cur.fetchone()
                                minfo = {"count": allordersinfo[0], "id": waifu['id'], "name": waifu['name'],
                                         "highest": allordersinfo[1]}
                                if myorderinfo is not None:
                                    minfo["mine"] = myorderinfo[0]
                                    if myorderinfo[0] == allordersinfo[1]:
                                        self.message(channel,
                                                     "There are currently {count} bounties for [{id}] {name}. You are the highest bidder at {highest} points.".format(
                                                         **minfo), isWhisper)
                                    else:
                                        self.message(channel,
                                                     "There are currently {count} bounties for [{id}] {name}. Your bid of {mine} points is lower than the highest bid of {highest} points.".format(
                                                         **minfo), isWhisper)
                                else:
                                    self.message(channel,
                                                 "There are currently {count} bounties for [{id}] {name}. The highest bid is {highest} points. You don't have a bounty on this waifu right now.".format(
                                                     **minfo), isWhisper)

                        except Exception:
                            self.message(channel, "Invalid waifu ID.", isWhisper=isWhisper)

                    return
                if subcmd == "list":
                    cur = db.cursor()
                    cur.execute(
                        "SELECT waifuid, amount, waifus.name FROM bounties JOIN waifus ON bounties.waifuid = waifus.id WHERE userid = %s AND status='open'",
                        [tags['user-id']])
                    buyorders = cur.fetchall()
                    cur.close()

                    if len(buyorders) == 0:
                        self.message(channel,
                                     "%s, you don't have any bounties active right now!" % tags['display-name'],
                                     isWhisper)
                        return

                    messages = ["%s, you have %d active bounties: " % (tags['display-name'], len(buyorders))]
                    for order in buyorders:
                        message = "[%d] %s for %d points; " % (order[0], order[2], order[1])

                        if len(message) + len(messages[-1]) > 400:
                            messages.append(message)
                        else:
                            messages[-1] += message

                    for message in messages:
                        self.message(channel, message, isWhisper)

                    return

                if subcmd == "place" or subcmd == "add":
                    if len(args) < 3:
                        self.message(channel, "Usage: !bounty <ID> <amount>", isWhisper)
                        return

                    if not followsme(tags['user-id']):
                        self.message(channel,
                                     "%s, you must follow the bot to use bounties so you can be sent a whisper if your order is filled." %
                                     tags['display-name'], isWhisper)
                        return

                    try:
                        waifu = getWaifuById(args[1])
                        assert waifu is not None
                        assert waifu['can_lookup'] == 1

                        if waifu['base_rarity'] >= int(config["numNormalRarities"]):
                            self.message(channel, "Bounties cannot be placed on special waifus.", isWhisper)
                            return

                        amount = int(args[2])

                        # check for a current order
                        cur = db.cursor()
                        cur.execute(
                            "SELECT id, amount FROM bounties WHERE userid = %s AND waifuid = %s AND status='open'",
                            [tags['user-id'], waifu['id']])
                        myorderinfo = cur.fetchone()

                        if myorderinfo is not None and myorderinfo[1] == amount:
                            self.message(channel,
                                         "%s, you already have a bounty in place for that waifu for that exact amount." %
                                         tags['display-name'], isWhisper)
                            cur.close()
                            return

                        # check for affordability
                        old_bounty = 0 if myorderinfo is None else myorderinfo[1]
                        points_delta = amount if myorderinfo is None else amount - myorderinfo[1]

                        if points_delta > 0 and not hasPoints(tags['user-id'], points_delta):
                            if myorderinfo is None:
                                self.message(channel,
                                             "%s, you don't have enough points to place a bounty with that amount." %
                                             tags['display-name'], isWhisper)
                            else:
                                self.message(channel,
                                             "%s, you don't have enough points to increase your bounty to that amount." %
                                             tags['display-name'], isWhisper)
                            cur.close()
                            return

                        # check for hand space
                        if myorderinfo is None and currentCards(tags['user-id']) >= handLimit(tags['user-id']):
                            self.message(channel, "%s, you don't have a free hand space to make a new bounty!" % tags[
                                'display-name'], isWhisper)
                            cur.close()
                            return

                        # check the range
                        cur.execute(
                            "SELECT COALESCE(MAX(amount), 0) FROM bounties WHERE userid != %s AND waifuid = %s AND status = 'open'",
                            [tags['user-id'], waifu['id']])
                        highest_other_bid = cur.fetchone()[0]
                        de_value = int(config["rarity%dValue" % waifu['base_rarity']])
                        min_amount = de_value + 5
                        rarity_cap = int(config["rarity%dMaxBounty" % waifu['base_rarity']])
                        max_amount = max(rarity_cap, highest_other_bid * 6 // 5)
                        if amount < min_amount or amount > max_amount:
                            self.message(channel,
                                         "%s, your bounty for this waifu must fall between %d and %d points." % (
                                             tags['display-name'], min_amount, max_amount), isWhisper)
                            cur.close()
                            return

                        # outbidding?
                        outbidding = highest_other_bid != 0 and amount > highest_other_bid and old_bounty < highest_other_bid
                        minimum_outbid = max(highest_other_bid // 20, 5)
                        if outbidding:
                            if amount < highest_other_bid + minimum_outbid:
                                self.message(channel,
                                             "%s, you must place a bounty of at least %d points to outbid the current highest bid of %d points." % (
                                                 tags['display-name'], highest_other_bid + minimum_outbid,
                                                 highest_other_bid), isWhisper)
                                cur.close()
                                return
                        elif amount < old_bounty and highest_other_bid + minimum_outbid > amount and amount > highest_other_bid:
                            self.message(channel,
                                         "%s, the lowest you can reduce your bounty to is %d points due to the bid of %d points below it." % (
                                             tags['display-name'], highest_other_bid + minimum_outbid,
                                             highest_other_bid))
                            cur.close()
                            return

                        # check for duplicate amount
                        cur.execute(
                            "SELECT COUNT(*) FROM bounties WHERE waifuid = %s AND status = 'open' AND amount = %s",
                            [waifu['id'], amount])
                        dupe_amt = cur.fetchone()[0]
                        if dupe_amt > 0:
                            self.message(channel,
                                         "%s, someone else has already placed a bounty on that waifu for %d points. Choose another amount." % (
                                             tags['display-name'], amount), isWhisper)
                            cur.close()
                            return

                        # actions that require confirmation first
                        if len(args) < 4 or args[3].lower() != 'yes':
                            # check for placing a bounty that has already been outbid
                            if highest_other_bid > amount:
                                msgargs = (tags['display-name'], highest_other_bid, waifu['id'], amount)
                                if myorderinfo is None:
                                    self.message(channel,
                                                 '%s, are you sure you want to place a bounty for lower than the current highest bid (%d points)? Enter "!bounty %d %d yes" if you are sure.' % msgargs,
                                                 isWhisper)
                                else:
                                    self.message(channel,
                                                 '%s, are you sure you want to change your bounty to a lower amount than the current other highest bid (%d points)? Enter "!bounty %d %d yes" if you are sure.' % msgargs,
                                                 isWhisper)
                                cur.close()
                                return

                            # check for placing a bounty above regular cap
                            if amount > rarity_cap:
                                amount_refund = (amount - rarity_cap) // 2 + rarity_cap
                                msgargs = (tags['display-name'], amount_refund, waifu['id'], amount)
                                self.message(channel,
                                             '%s, are you sure you want to place a bounty above the normal cap for that waifu\'s rarity? If you cancel it, you will only receive %d points back unless a higher bounty than yours is filled. Enter "!bounty %d %d yes" if you are sure.' % msgargs,
                                             isWhisper)
                                cur.close()
                                return

                        # if it passed all of those checks it should be good to go.
                        # penalize them for reducing a bounty above regular cap?
                        if points_delta < 0 and old_bounty > rarity_cap:
                            change_above_cap = min(-points_delta, old_bounty - rarity_cap)
                            addPoints(tags['user-id'], change_above_cap // 2 + (-points_delta - change_above_cap))
                        else:
                            addPoints(tags['user-id'], -points_delta)

                        if myorderinfo is None:
                            cur.execute(
                                "INSERT INTO bounties (userid, waifuid, amount, status, created) VALUES(%s, %s, %s, 'open', %s)",
                                [tags['user-id'], waifu['id'], amount, current_milli_time()])
                            self.message(channel, "%s, you placed a new bounty on [%d] %s for %d points." % (
                                tags['display-name'], waifu['id'], waifu['name'], amount), isWhisper)
                        else:
                            cur.execute("UPDATE bounties SET amount = %s, updated = %s WHERE id = %s",
                                        [amount, current_milli_time(), myorderinfo[0]])
                            self.message(channel, "%s, you updated your bounty on [%d] %s to %d points." % (
                                tags['display-name'], waifu['id'], waifu['name'], amount), isWhisper)

                        # outbid message?
                        if outbidding:
                            # attempt to whisper for outbid
                            cur.execute(
                                "SELECT users.name FROM bounties JOIN users ON bounties.userid=users.id WHERE bounties.waifuid = %s AND bounties.amount = %s AND bounties.status = 'open' LIMIT 1",
                                [waifu['id'], highest_other_bid])

                            other_bidder = cur.fetchone()
                            if other_bidder is not None:
                                self.message('#%s' % other_bidder[0],
                                             "Your bounty on [%d] %s has been outbid. The new highest bounty is %d points." % (
                                                 waifu['id'], waifu['name'], amount), True)
                        cur.close()
                        return

                    except Exception as exc:
                        self.message(channel, "Usage: !bounty <ID> <amount>", isWhisper=isWhisper)
                        return

                if subcmd == "cancel":
                    if len(args) != 2:
                        self.message(channel, "Usage: !bounty cancel <ID>", isWhisper=isWhisper)
                        return

                    try:
                        waifu = getWaifuById(args[1])
                        assert waifu is not None
                        assert waifu['can_lookup'] == 1

                        # check for a current order
                        cur = db.cursor()
                        cur.execute(
                            "SELECT id, amount, created, updated FROM bounties WHERE userid = %s AND waifuid = %s AND status='open'",
                            [tags['user-id'], waifu['id']])
                        myorderinfo = cur.fetchone()
                        bounty_time = myorderinfo[3] if myorderinfo[3] is not None else myorderinfo[2]

                        if myorderinfo is not None:
                            cur.execute("UPDATE bounties SET status = 'cancelled', updated = %s WHERE id = %s",
                                        [current_milli_time(), myorderinfo[0]])
                            # penalise them?
                            rarity_cap = int(config["rarity%dMaxBounty" % waifu['base_rarity']])
                            # free cancel after direct outbid was met?
                            cur.execute(
                                "SELECT COUNT(*) FROM bounties WHERE waifuid = %s AND status='filled' AND updated > %s",
                                [waifu['id'], bounty_time])
                            free_cancel = cur.fetchone()[0] > 0
                            if myorderinfo[1] > rarity_cap and not free_cancel:
                                refund = (myorderinfo[1] - rarity_cap) // 2 + rarity_cap
                                addPoints(tags['user-id'], refund)
                                self.message(channel,
                                             "%s, you cancelled your bounty for [%d] %s and received only %d points back since it was above cap." % (
                                                 tags['display-name'], waifu['id'], waifu['name'], refund), isWhisper)
                            else:
                                addPoints(tags['user-id'], myorderinfo[1])
                                self.message(channel,
                                             "%s, you cancelled your bounty for [%d] %s and received your %d points back." % (
                                                 tags['display-name'], waifu['id'], waifu['name'], myorderinfo[1]),
                                             isWhisper)
                        else:
                            self.message(channel,
                                         "%s, you don't have an active bounty for that waifu!" % tags['display-name'],
                                         isWhisper)
                        cur.close()
                        return

                    except Exception:
                        self.message(channel, "Usage: !bounty cancel <ID>", isWhisper=isWhisper)
                        return
            if command == "raritychange" and sender in superadmins:
                if len(args) < 2:
                    self.message(channel, "Usage: !raritychange <ID> <rarity>", isWhisper)
                    return

                try:
                    waifu = getWaifuById(args[0])
                    assert waifu is not None
                    rarity = parseRarity(args[1])
                except Exception:
                    self.message(channel, "Usage: !raritychange <ID> <rarity>", isWhisper)
                    return

                if waifu['base_rarity'] >= int(config['numNormalRarities']):
                    self.message(channel, "You shouldn't be changing a special waifu into another rarity.", isWhisper)
                    return

                if rarity == waifu['base_rarity']:
                    self.message(channel, "[%d] %s is already %s base rarity!" % (
                        waifu['id'], waifu['name'], config['rarity%dName' % rarity]), isWhisper)
                    return

                # limit check
                oldRarityLimit = int(config['rarity%dMax' % waifu['base_rarity']])
                newRarityLimit = int(config['rarity%dMax' % rarity])
                if newRarityLimit != 0 and (oldRarityLimit == 0 or oldRarityLimit > newRarityLimit):
                    with db.cursor() as cur:
                        cur.execute(
                            "SELECT (SELECT COALESCE(SUM(amount), 0) FROM has_waifu WHERE waifuid = %s) + (SELECT COUNT(*) FROM boosters_cards JOIN boosters_opened ON boosters_cards.boosterid=boosters_opened.id WHERE boosters_cards.waifuid = %s AND boosters_opened.status = 'open')",
                            [waifu['id'], waifu['id']])
                        currentOwned = cur.fetchone()[0]

                    if currentOwned > newRarityLimit:
                        errorArgs = (
                            waifu['id'], waifu['name'], config['rarity%dName' % rarity], currentOwned, newRarityLimit)
                        self.message(channel,
                                     "[%d] %s cannot be changed to %s base rarity. There are %d copies of her already owned while the limit at the new rarity would be %d." % errorArgs,
                                     isWhisper)
                        return

                # okay, do it
                with db.cursor() as cur:
                    cur.execute("UPDATE waifus SET base_rarity = %s WHERE id = %s", [rarity, waifu['id']])
                    cur.execute("UPDATE has_waifu SET rarity = %s WHERE waifuid = %s AND rarity < %s",
                                [rarity, waifu['id'], rarity])

                    # cancel all bounties
                    cur.execute(
                        "SELECT bounties.userid, users.name, bounties.amount FROM bounties JOIN users ON bounties.userid = users.id WHERE bounties.waifuid = %s AND bounties.status = 'open'",
                        [waifu['id']])
                    bounties = cur.fetchall()
                    for bounty in bounties:
                        addPoints(bounty[0], bounty[2])
                        self.message('#%s' % bounty[1],
                                     "Your bounty for [%d] %s has been cancelled due to its rarity changing. Your %d points have been refunded." % (
                                         waifu['id'], waifu['name'], bounty[2]), True)
                    cur.execute(
                        "UPDATE bounties SET status='cancelled', updated=%s WHERE waifuid = %s AND status='open'",
                        [current_milli_time(), waifu['id']])

                # done
                self.message(channel, "Successfully changed [%d] %s's base rarity to %s." % (
                    waifu['id'], waifu['name'], config['rarity%dName' % rarity]), isWhisper)
                return
            if command == "profile":
                if len(args) == 0:
                    self.message(channel, tags["display-name"] + ", your profile: " + config[
                        "siteHost"] + "/profile?user=" + str(sender), isWhisper)
                    return
                elif args[0] == "favourite" or args[0] == "favorite":
                    newFav = 0
                    try:
                        newFav = int(args[1])
                    except ValueError:
                        self.message(channel, args[1] + " is not a number. Please try again.");
                        return
                    newFavW = getWaifuById(newFav)
                    if newFavW is None:
                        self.message(channel, "That Waifu doesn't exist! Try again!", isWhisper)
                        return
                    canLookup = newFavW["can_lookup"] == 1
                    hasOrIsLowRarity = False
                    if int(newFavW["base_rarity"]) > 7:
                        logger.debug(sender + " requested to set " + str(
                            newFav) + " as his new Favourite Waifu, which is promo or above. Checking if they have it...")
                        hand = getHand(tags["user-id"])
                        for w in hand:
                            if str(w["id"]) == str(newFav):
                                hasOrIsLowRarity = True
                                break
                    else:
                        hasOrIsLowRarity = True

                    if not canLookup and not hasOrIsLowRarity:
                        self.message(channel, tags[
                            "display-name"] + ", sorry, but that Waifu doesn't exist. Try a different one!",
                                     isWhisper)
                        return
                    elif hasOrIsLowRarity:
                        self.message(channel, "Updated your favourite Waifu to be " + newFavW["name"] + "! naroDesu",
                                     isWhisper)
                        setFavourite(tags["user-id"], newFav)
                        return
                    else:
                        self.message(channel, tags[
                            "display-name"] + ", sorry, but this Waifu is a Special or above, so you need to have it to set it as a favourite!",
                                     isWhisper)
                        return
                elif args[0] == "description":
                    newDesc = " ".join(args[1:])
                    logger.debug("New description: " + newDesc)
                    if len(newDesc) > 1023:
                        self.message(channel, "That description is too long. Please limit it to 1024 characters.",
                                     isWhisper)
                        return
                    setDescription(tags["user-id"], newDesc)
                    self.message(channel, tags["display-name"] + ", successfully updated your profile description!",
                                 isWhisper)
            if command == "fixwaifu":
                self.message(channel,
                             "To submit changes/fixes for any waifu, please go to %s/fixes" % config["siteHost"],
                             isWhisper)
                return
            if command == "packspending":
                    packstats = getPackStats(tags["user-id"])

                    if len(packstats) == 0:
                        self.message(channel,
                                     "%s, you haven't bought any boosters yet! Buy your first with !booster buy." %
                                     tags['display-name'], isWhisper)
                        return

                    totalspending = sum([row[2] for row in packstats])
                    packstr = ", ".join("%dx %s" % (row[1], row[0]) for row in packstats)
                    self.message(channel, "%s, you have spent %d total points on the following packs: %s." % (
                    tags['display-name'], totalspending, packstr), isWhisper)
                    if checkHandUpgrade(tags["user-id"]):
                        self.message(channel, "... and this was enough to upgrade your hand to a new slot! naroYay", isWhisper)
                    return


curg = db.cursor()
logger.info("Fetching channel list...")
curg.execute("SELECT * FROM channels")
channels = []
for row in curg.fetchall():
    channels.append("#" + row[0])
logger.debug("Channels: %s", str(channels))
curg.close()

loadConfig()

# twitch api init
checkAndRenewAppAccessToken()

# get user data for the bot itself
headers = {"Authorization": "Bearer %s" % config["appAccessToken"]}
r = requests.get("https://api.twitch.tv/helix/users", headers=headers,
                 params={"login": str(config["username"]).lower()})
j = r.json()
try:
    twitchid = j["data"][0]["id"]
except Exception:
    twitchid = 0
config["twitchid"] = str(twitchid)
b = NepBot(config, channels)
b.start(config["oauth"])

logger.debug("past start")

pool.handle_forever()

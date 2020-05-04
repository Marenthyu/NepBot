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
from collections import defaultdict, OrderedDict
from private_functions import validateWaifuURL, processWaifuURL, validateBadgeURL, processBadgeURL, tokenGachaRoll

import sys
import re
import logging

import websocket
import _thread as thread

formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s] %(message)s')
logger = logging.getLogger('nepbot')
logger.setLevel(logging.DEBUG)
logger.propagate = False
fh = logging.handlers.TimedRotatingFileHandler('debug.log', when='midnight', encoding='utf-8')
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)

logging.getLogger('tornado.application').addHandler(fh)
logging.getLogger('tornado.application').addHandler(ch)

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
streamlabsclient = None
twitchclientsecret = None
bannedWords = []
t = None
# read config values from file (db login etc)
try:
    f = open("nepbot.cfg", "r")
    lines = f.readlines()
    for line in lines:
        name, value = line.split("=", 1)
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
marathonActivityMap = {}
blacklist = []
config = {}
packAmountRewards = {}
emotewaremotes = []
revrarity = {}
visiblepacks = ""
validalertconfigvalues = []
discordhooks = []
cpuVoters = []

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
    global revrarity, blacklist, visiblepacks, admins, superadmins, validalertconfigvalues, waifu_regex, emotewaremotes, discordhooks, packAmountRewards
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
        validalertconfigvalues = ["color", "alertChannel", "defaultLength", "defaultSound", "setClaimSound",
                                  "setClaimLength"] \
                                 + ["rarity%dLength" % rarity for rarity in alertRarityRange] \
                                 + ["rarity%dSound" % rarity for rarity in alertRarityRange]
        waifu_regex = re.compile('(\[(?P<id>[0-9]+?)])?(?P<name>.+?) *- *(?P<series>.+) *- *(?P<rarity>[0-' + str(
            int(config["numNormalRarities"]) + int(config["numSpecialRarities"]) - 1) + ']) *- *(?P<link>.+?)$')
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
        curg.execute("SELECT id FROM banned_users WHERE banned = 1")
        blacklist = [int(row[0]) for row in curg.fetchall()]

        # visible packs
        curg.execute("SELECT name FROM boosters WHERE listed = 1 AND buyable = 1 ORDER BY sortIndex ASC")
        packrows = curg.fetchall()
        visiblepacks = "/".join(row[0] for row in packrows)

        # discord hooks
        with discordLock:
            curg.execute("SELECT url FROM discordHooks ORDER BY priority DESC")
            discrows = curg.fetchall()
            discordhooks = [row[0] for row in discrows]

        # pack amount rewards
        packAmountRewards = {}
        curg.execute("SELECT boostername, de_amount, reward_booster FROM pack_amount_rewards")
        rewardRows = curg.fetchall()
        for row in rewardRows:
            if row[0] not in packAmountRewards:
                packAmountRewards[row[0]] = {}
            packAmountRewards[row[0]][int(row[1])] = row[2]


def checkAndRenewAppAccessToken():
    global config, headers
    krakenHeaders = {"Authorization": "OAuth %s" % config["appAccessToken"], "Accept": "application/vnd.twitchtv.v5+json"}
    r = requests.get("https://api.twitch.tv/kraken", headers=krakenHeaders)
    resp = r.json()

    if "token" not in resp or "valid" not in resp["token"] or not resp["token"]["valid"]:
        # app access token has expired, get a new one
        logger.debug("Requesting new token")
        url = 'https://id.twitch.tv/oauth2/token?client_id=%s&client_secret=%s&grant_type=client_credentials' % (
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

def booleanConfig(name):
    return name in config and config[name].strip().lower() not in ["off", "no", "false"]


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
    
class NotEnoughBetsException(Exception):
    pass


class NoBetException(Exception):
    pass
    

class NotOpenLongEnoughException(Exception):
    pass


def startBet(channel, confirmed=False):
    with db.cursor() as cur:
        cur.execute("SELECT id, openedTime FROM bets WHERE channel = %s AND status = 'open' LIMIT 1", [channel])
        row = cur.fetchone()
        if row is not None:
            if not confirmed:
                cur.execute("SELECT COUNT(*) FROM placed_bets WHERE betid = %s", [row[0]])
                if cur.fetchone()[0] < int(config["betMinimumEntriesForPayout"]):
                    raise NotEnoughBetsException()
            if row[1] is not None and int(row[1]) + int(config["betMinimumMinutesOpen"])*60000 > current_milli_time():
                raise NotOpenLongEnoughException()
            cur.execute("UPDATE bets SET startTime = %s, status = 'started' WHERE id = %s", [current_milli_time(), row[0]])
        else:
            raise NoBetException()


def openBet(channel):
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) FROM bets WHERE channel = %s AND status IN('open', 'started')", [channel])
    result = cur.fetchone()[0] or 0
    if result > 0:
        cur.close()
        return False
    else:
        cur.execute("INSERT INTO bets(channel, status, openedTime) VALUES (%s, 'open', %s)", [channel, current_milli_time()])
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
    with db.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("SELECT cards.id AS cardid, waifus.name, waifus.id AS waifuid, cards.rarity, waifus.series, COALESCE(cards.customImage, waifus.image) AS image, waifus.base_rarity, cards.tradeableAt FROM cards JOIN waifus ON cards.waifuid = waifus.id WHERE cards.userid = %s AND cards.boosterid IS NULL ORDER BY COALESCE(cards.sortValue, 32000) ASC, (rarity < %s) DESC, waifus.id ASC, cards.id ASC",
        [tID, int(config["numNormalRarities"])])
        return cur.fetchall()
             
def getOpenBooster(twitchid):
    try:
        tID = int(twitchid)
    except Exception:
        logger.error("Got non-integer id for getBooster. Aborting.")
        return []
    with db.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("SELECT id, boostername, paid FROM boosters_opened WHERE userid = %s AND status = 'open' LIMIT 1", [tID])
        booster = cur.fetchone()
        if booster is None:
            return None
        cur.execute("SELECT cards.id AS cardid, waifus.name, waifus.id AS waifuid, cards.rarity, waifus.series, COALESCE(cards.customImage, waifus.image) AS image, waifus.base_rarity FROM cards JOIN waifus ON cards.waifuid = waifus.id WHERE cards.userid = %s AND cards.boosterid = %s ORDER BY waifus.id ASC", [tID, booster['id']])
        booster['cards'] = cur.fetchall()
        return booster
             
def getCard(cardID):
    with db.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("SELECT cards.*, waifus.base_rarity FROM cards JOIN waifus ON cards.waifuid=waifus.id WHERE cards.id = %s", [cardID])
        return cur.fetchone()
        
def addCard(userid, waifuid, source, boosterid=None, rarity=None):
    with db.cursor() as cur:
        if rarity is None:
            cur.execute("SELECT base_rarity FROM waifus WHERE id = %s", [waifuid])
            rarity = cur.fetchone()[0]
            
        waifuInfo = [userid, boosterid, waifuid, rarity, current_milli_time(), userid, boosterid, source]
        cur.execute("INSERT INTO cards (userid, boosterid, waifuid, rarity, created, originalOwner, originalBooster, source) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", waifuInfo)
        return cur.lastrowid
        
def updateCard(id, changes):
    changes["updated"] = current_milli_time()
    with db.cursor() as cur:
        cur.execute('UPDATE cards SET {} WHERE id = %s'.format(', '.join('{}=%s'.format(k) for k in changes)), list(changes.values()) + [id])

def search(query, series=None):
    cur = db.cursor()
    if series is None:
        cur.execute("SELECT id, name, series, base_rarity FROM waifus WHERE can_lookup = 1 AND name LIKE %s",
                    ["%" + query + "%"])
    else:
        cur.execute(
            "SELECT id, name, series, base_rarity FROM waifus WHERE can_lookup = 1 AND name LIKE %s AND series LIKE %s",
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
        "SELECT (SELECT COUNT(*) FROM cards WHERE userid = %s AND boosterid IS NULL AND rarity < %s), (SELECT COUNT(*) FROM bounties WHERE userid = %s AND status = 'open')",
        [userid, int(config["numNormalRarities"]), userid])
    result = cur.fetchone()
    cur.close()
    if verbose:
        return {"hand": result[0], "bounties": result[1], "total": result[0] + result[1]}
    else:
        return result[0] + result[1]


def upgradeHand(userid, gifted=False):
    cur = db.cursor()
    cur.execute(
        "UPDATE users SET paidHandUpgrades = paidHandUpgrades + %s, freeUpgrades = freeUpgrades + %s WHERE id = %s",
        [0 if gifted else 1, 1 if gifted else 0, userid])
    cur.close()
    
def disenchant(bot, cardid):
    # return amount of points gained
    with db.cursor() as cur:
        card = getCard(cardid)
        deValue = int(config["rarity" + str(card["rarity"]) + "Value"])
        updateCard(cardid, {"userid": None, "boosterid": None})
        # bounty to fill?
        cur.execute(
            "SELECT bounties.id, bounties.userid, users.name, bounties.amount, waifus.name, waifus.base_rarity FROM bounties JOIN users ON bounties.userid = users.id JOIN waifus ON bounties.waifuid = waifus.id WHERE bounties.waifuid = %s AND bounties.status = 'open' ORDER BY bounties.amount DESC LIMIT 1",
            [card["waifuid"]])
        order = cur.fetchone()

        if order is not None:
            # fill their order instead of actually disenchanting
            filledCardID = addCard(order[1], card['waifuid'], "bounty")
            # when we spawn a bounty card, carry over the old original owner id
            updateCard(filledCardID, {"originalOwner": card['originalOwner']})
            bot.message('#%s' % order[2],
                        "Your bounty for [%d] %s for %d points has been filled and they have been added to your hand." % (
                            card['waifuid'], order[4], order[3]), True)
            cur.execute("UPDATE bounties SET status = 'filled', oldCard = %s, newCard = %s, updated = %s WHERE id = %s",
                        [cardid, filledCardID, current_milli_time(), order[0]])
            # alert people with lower bounties but above the cap?
            base_value = int(config["rarity" + str(order[5]) + "Value"])
            min_bounty = int(config["rarity" + str(order[5]) + "MinBounty"])
            rarity_cap = int(config["rarity" + str(order[5]) + "MaxBounty"])
            cur.execute(
                "SELECT users.name FROM bounties JOIN users ON bounties.userid = users.id WHERE bounties.waifuid = %s AND bounties.status = 'open' AND bounties.amount > %s",
                [card['waifuid'], rarity_cap])
            for userrow in cur.fetchall():
                bot.message('#%s' % userrow[0],
                            "A higher bounty for [%d] %s than yours was filled, so you can now cancel yours and get full points back provided you don't change it." % (
                                card['waifuid'], order[4]), True)
            # give the disenchanter appropriate profit
            # everything up to the min bounty, 1/2 of any amount between the min and max bounties, 1/4 of anything above the max bounty.
            deValue += (min_bounty - base_value) + max(min(order[3] - min_bounty, rarity_cap - min_bounty) // 2, 0) + max((order[3] - rarity_cap) // 4, 0)
            
        return deValue


def setFavourite(userid, waifu):
    with db.cursor() as cur:
        cur.execute("UPDATE users SET favourite=%s WHERE id = %s", [waifu, userid])


def setDescription(userid, newDesc):
    with db.cursor() as cur:
        cur.execute("UPDATE users SET profileDescription=%s WHERE id = %s", [newDesc, userid])
        
def checkFavouriteValidity(userid):
    with db.cursor() as cur:
        cur.execute("SELECT favourite FROM users WHERE id = %s", [userid])
        favourite = getWaifuById(cur.fetchone()[0])
        valid = True
        if favourite["can_favourite"] == 0:
            valid = False
        elif favourite["base_rarity"] >= int(config["numNormalRarities"]):
            # must be owned
            cur.execute("SELECT COUNT(*) FROM cards WHERE waifuid = %s AND boosterid IS NULL AND userid = %s", [favourite["id"], userid])
            valid = cur.fetchone()[0] > 0
            
        if not valid:
            # reset favourite
            cur.execute("UPDATE users SET favourite = 1 WHERE id = %s", [userid])


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
    r = requests.get(
        "https://horaro.org/-/api/v1/schedules/{horaroid}/ticker".format(horaroid=config["horaroID"]))
    try:
        j = r.json()
        # ("got horaro ticker: " + str(j))
        return j
    except Exception:
        logger.error("Horaro Error:")
        logger.error(str(r.status_code))
        logger.error(r.text)

def getRawRunner(runner):
    if '[' not in runner:
        return runner
    return runner[runner.index('[') + 1 : runner.index(']')]


def updateBoth(game, title):
    if not booleanConfig("marathonBotFunctions"):
        return
    myheaders = headers.copy()
    myheaders["Authorization"] = "OAuth " + config["marathonOAuth"].replace("oauth:", "")
    myheaders["Content-Type"] = "application/json"
    myheaders["Accept"] = "application/vnd.twitchtv.v5+json"
    body = {"channel": {"status": str(title), "game": str(game)}}
    logger.debug(str(body))
    r = requests.put("https://api.twitch.tv/kraken/channels/"+config["marathonChannelID"], headers=myheaders, json=body)
    try:
        j = r.json()
        logger.debug("Response from twitch: "+str(j))
        # print("tried to update channel title, response: " + str(j))
    except Exception:
        logger.error(str(r.status_code))
        logger.error(r.text)


def updateTitle(title):
    if not booleanConfig("marathonBotFunctions"):
        return
    myheaders = headers.copy()
    myheaders["Authorization"] = "OAuth " + config["marathonOAuth"].replace("oauth:", "")
    myheaders["Content-Type"] = "application/json"
    myheaders["Accept"] = "application/vnd.twitchtv.v5+json"
    body = {"channel": {"status": str(title)}}
    r = requests.put("https://api.twitch.tv/kraken/channels/"+config["marathonChannelID"], headers=myheaders, json=body)
    try:
        j = r.json()
    except Exception:
        logger.error(str(r.status_code))
        logger.error(r.text)


def updateGame(game):
    if not booleanConfig("marathonBotFunctions"):
        return
    myheaders = headers.copy()
    myheaders["Authorization"] = "OAuth " + config["marathonOAuth"].replace("oauth:", "")
    myheaders["Content-Type"] = "application/json"
    myheaders["Accept"] = "application/vnd.twitchtv.v5+json"
    body = {"channel": {"game": str(game)}}
    r = requests.put("https://api.twitch.tv/kraken/channels/"+config["marathonChannelID"], headers=myheaders, json=body)
    try:
        j = r.json()
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


def sendAdminDiscordAlert(data):
    with discordLock:
        req2 = requests.post(config["adminDiscordHook"], json=data)
        while req2.status_code == 429:
            time.sleep((int(req2.headers["Retry-After"]) / 1000) + 1)
            req2 = requests.post(
                config["adminDiscordHook"],
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
                "title": "{user} dropped [{wid}] {name}!".format(user=str(user), wid=str(waifu["id"]),
                                                         name=str(waifu["name"])),
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
            "title": "[{wid}] {name} has been disenchanted! Press F to pay respects.".format(name=str(waifu["name"]),
                                                                                     wid=str(waifu["id"])),
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
            "title": "{user} promoted [{wid}] {name} to {rarity} rarity!".format(user=username, name=waifu["name"],
                                                                         wid=str(waifu["id"]),
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

def getWaifuRepresentationString(waifuid, baserarity=None, cardrarity=None, waifuname=None):
    if baserarity == None or cardrarity == None or waifuname == None:
        waifuData = getWaifuById(waifuid)
        if baserarity == None:
            baserarity = waifuData['base_rarity']
        if cardrarity == None:
            cardrarity = baserarity
        if waifuname == None:
            waifuname = waifuData['name']

    promoteDiff = cardrarity - baserarity
    promoteStars = (" (" + ("★" * (promoteDiff)) + ")") if promoteDiff > 0 else ""

    retStr = "[%d][%s%s] %s" % (
        waifuid, config["rarity" + str(cardrarity) + "Name"], promoteStars, waifuname)

    return retStr

def sendSetAlert(channel, user, name, waifus, reward, firstTime, discord=True):
    logger.info("Alerting for set claim %s", name)
    with busyLock:
        with db.cursor() as cur:
            chanOwner = str(channel).replace("#", "")
            cur.execute("SELECT config, val FROM alertConfig WHERE channelName = %s", [chanOwner])
            rows = cur.fetchall()

    alertconfig = {row[0]: row[1] for row in rows}
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
            "title": "A set has been completed%s!" % (" for the first time" if firstTime else ""),
            "color": int(config["rarity" + str(int(config["numNormalRarities"]) - 1) + "EmbedColor"])
        },
        {
            "type": "rich",
            "title": "{user} completed the set {name}!".format(user=str(user), name=name),
            "description": "They gathered {waifus} and received {reward} as their reward.".format(waifus=naturalJoinNames(waifus), reward=reward),
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
        if id < 1:
            return None
    except ValueError:
        return None
    with db.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("SELECT id, name, image, base_rarity, series, can_lookup, pulls, last_pull, can_favourite, can_purchase FROM waifus WHERE id=%s", [id])
        return cur.fetchone()
    
def getWaifuOwners(id, rarity):
    with db.cursor() as cur:
        baseRarityName = config["rarity%dName" % rarity]
        godRarity = int(config["numNormalRarities"]) - 1
        cur.execute(
            "SELECT users.name, c1.rarity, IF(c1.boosterid IS NOT NULL, 1, 0), IF(c1.rarity = %s AND NOT EXISTS(SELECT id FROM cards c2 WHERE c2.userid IS NOT NULL AND c2.rarity = %s AND c2.waifuid = c1.waifuid AND (c2.created < c1.created OR (c2.created=c1.created AND c2.id < c1.id))), 1, 0) AS firstGod FROM cards c1 JOIN users ON c1.userid = users.id WHERE c1.waifuid = %s AND c1.userid IS NOT NULL ORDER BY firstGod DESC, c1.rarity DESC, c1.created ASC, users.name ASC",
            [godRarity, godRarity, id])
        allOwners = cur.fetchall()

    # compile per-owner data grouped into not in booster / in booster
    ownerDescriptions = [[], []]
    for i in range(2):
        ownerData = OrderedDict()
        ownedByOwner = {}
        for row in allOwners:
            if row[2] != i:
                continue
            if row[0] not in ownerData:
                ownerData[row[0]] = OrderedDict()
                ownedByOwner[row[0]] = 0
            rarityName = ("α" if row[3] else "") + config["rarity%dName" % row[1]]
            if rarityName not in ownerData[row[0]]:
                ownerData[row[0]][rarityName] = 0
            ownerData[row[0]][rarityName] += 1
            ownedByOwner[row[0]] += 1

        for owner in ownerData:
            if len(ownerData[owner]) != 1 or baseRarityName not in ownerData[owner] or ownedByOwner[owner] > 1:
                # verbose
                ownerDescriptions[i].append(owner + " (" + ", ".join([("%d %s" % (ownerData[owner][k], k) if ownerData[owner][k] > 1 else k) for k in ownerData[owner]]) + ")")
            else:
                ownerDescriptions[i].append(owner)
            
    return ownerDescriptions


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


def getPuddingBalance(userid):
    with db.cursor() as cur:
        cur.execute("SELECT puddingCurrent, puddingPrevious, puddingExpiring FROM users WHERE id = %s", [userid])
        pinfo = cur.fetchone()
        return None if pinfo is None else [int(n) for n in pinfo]

def hasPudding(userid, amount):
    bal = getPuddingBalance(userid)
    return bal is not None and sum(bal) >= amount

def addPudding(userid, amount):
    with db.cursor() as cur:
        cur.execute("UPDATE users SET puddingCurrent = puddingCurrent + %s WHERE id = %s", [amount, userid])

def takePudding(userid, amount):
    pinfo = getPuddingBalance(userid)
    if pinfo is None or sum(pinfo) < amount:
        raise ValueError()
    # take from the pudding starting from the expiring amount first
    idx = 2
    while amount > 0:
        new_val = max(pinfo[idx] - amount, 0)
        amount -= pinfo[idx] - new_val
        pinfo[idx] = new_val
        idx -= 1
    # save the updated values
    with db.cursor() as cur:
        cur.execute("UPDATE users SET puddingCurrent = %s, puddingPrevious = %s, puddingExpiring = %s WHERE id = %s", pinfo + [userid])

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
                          int(config["rarity%dBuyPrice" % rarity]) <= 0]
        if len(uniqueRarities) == 0:
            return []
        else:
            inStr = ",".join(["%s"] * len(uniqueRarities))
            cur.execute("SELECT DISTINCT waifuid FROM cards WHERE userid = %s AND boosterid IS NULL AND rarity IN ({0})".format(inStr),
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
            weighting_column = "(event_weighting*normal_weighting)" if useEventWeightings else "normal_weighting"
            result = None
            if rarity >= int(config["strongerWeightingMinRarity"]):
                cur.execute("SELECT id FROM waifus WHERE base_rarity = %s{1} AND normal_weighting >= 1 ORDER BY -LOG(1-RAND())/{0} LIMIT 1".format(weighting_column, banClause), [rarity] + bannedCards)
                result = cur.fetchone()
            if result is None:
                cur.execute("SELECT id FROM waifus WHERE base_rarity = %s{1} ORDER BY -LOG(1-RAND())/{0} LIMIT 1".format(weighting_column, banClause), [rarity] + bannedCards)
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
                inString), [float(config["weighting_increase_amount"])**4, pullTime] + list(cards))
        cur.execute(
            "UPDATE waifus SET normal_weighting = 1, pulls = pulls + 1, last_pull = %s WHERE id IN({0}) AND normal_weighting > 1".format(
                inString), [pullTime] + list(cards))


def attemptPromotions(*cards):
    promosDone = {}
    with db.cursor() as cur:
        for waifuid in cards:
            while True:
                usersThisCycle = []
                cur.execute(
                    "SELECT userid, rarity, COUNT(*) AS amount FROM cards JOIN waifus ON cards.waifuid = waifus.id WHERE cards.waifuid = %s AND cards.boosterid IS NULL AND cards.userid IS NOT NULL AND waifus.can_promote = 1 GROUP BY cards.userid, cards.rarity HAVING COUNT(*) > 1 ORDER BY cards.rarity ASC, RAND() ASC",
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

                        if amountToMake != 0:
                            usersThisCycle.append(userid)
                            # fetch card ids to use - take the ones with the least trade restrictions
                            cur.execute("SELECT id, COALESCE(tradeableAt, 0) FROM cards WHERE waifuid = %s AND userid = %s AND rarity = %s AND boosterid IS NULL ORDER BY tradeableAt ASC", [waifuid, userid, rarity])
                            fodder = cur.fetchall()
                            for i in range(amountToMake):
                                material = fodder[i*promoteAmount:(i+1)*promoteAmount]
                                tradeTS = max([row[1] for row in material])
                                for row in material:
                                    updateCard(row[0], {"userid": None, "boosterid": None})
                                newID = addCard(userid, waifuid, 'promotion', None, rarity+1)
                                if tradeTS > 0:
                                    updateCard(newID, {"tradeableAt": tradeTS})

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


def formatTimeDelta(ms, showMS=True):
    output = str(datetime.timedelta(milliseconds=int(ms), microseconds=0))
    if "." in output:
        output = output[:-3] if showMS else output[:-7]
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


class CardIDNotInHandException(CardNotInHandException):
    pass
    
    
class AmbiguousWaifuException(Exception):
    pass


# given a string specifying a waifu id or card id, return card id of the hand card matching it
# throw various exceptions for invalid format / card not in hand / owns more than 1 copy of the waifu
def parseHandCardSpecifier(hand, specifier, rarity=None):
    id = int(specifier)
    
    if id < int(config["minimumCardID"]):
        # waifuid
        cardFound = None
        for card in hand:
            if card['waifuid'] == id and (rarity is None or rarity == card['rarity']):
                if cardFound is None:
                    cardFound = card
                else:
                    raise AmbiguousWaifuException()
        
        if cardFound is None:
            raise CardNotInHandException()
        
        return cardFound
    else:
        for card in hand:
            if card['cardid'] == id and (rarity is None or rarity == card['rarity']):
                return card
        raise CardIDNotInHandException()


class InvalidBoosterException(Exception):
    pass


class CantAffordBoosterException(Exception):
    def __init__(self, cost):
        super(CantAffordBoosterException, self).__init__()
        self.cost = cost


def getPackStats(userid):
    with db.cursor() as cur:
        cur.execute(
            "SELECT bo.boostername, COUNT(*) FROM (SELECT id, boostername FROM boosters_opened WHERE userid = %s UNION SELECT id, boostername FROM archive_boosters_opened WHERE userid = %s) AS bo JOIN boosters ON (bo.boostername IN(boosters.name, CONCAT('mega', boosters.name))) WHERE boosters.cost > 0 GROUP BY bo.boostername ORDER BY COUNT(*) DESC",
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

    nextSpendings += lut[currSlots + 1][1]
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
        
def addBooster(userid, boostername, paid, status, eventTokens, channel, isWhisper):
    with db.cursor() as cur:
        trueChannel = "$$whisper$$" if isWhisper else channel
        cur.execute(
            "INSERT INTO boosters_opened (userid, boostername, paid, created, status, eventTokens, channel) VALUES(%s, %s, %s, %s, %s, %s, %s)",
            [userid, boostername, paid, current_milli_time(), status, eventTokens, trueChannel])
        return cur.lastrowid


def openBooster(bot, userid, username, display_name, channel, isWhisper, packname, buying=True, mega=False):
    with db.cursor() as cur:
        rarityColumns = ", ".join(
            "rarity" + str(i) + "UpgradeChance" for i in range(int(config["numNormalRarities"]) - 1))

        if buying:
            cur.execute(
                "SELECT listed, buyable, cost, numCards, guaranteeRarity, guaranteeCount, useEventWeightings, maxEventTokens, eventTokenChance, canMega, " + rarityColumns + " FROM boosters WHERE name = %s AND buyable = 1",
                [packname])
        else:
            cur.execute(
                "SELECT listed, buyable, cost, numCards, guaranteeRarity, guaranteeCount, useEventWeightings, maxEventTokens, eventTokenChance, canMega, " + rarityColumns + " FROM boosters WHERE name = %s",
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
        numTokens = packinfo[7]
        tokenChance = packinfo[8]
        canMega = packinfo[9]
        normalChances = packinfo[10:]
        
        if numTokens >= numCards:
            raise InvalidBoosterException()
            
        iterations = 1
        if mega:
            if not canMega:
                raise InvalidBoosterException()
            iterations = 5

        if buying:
            if not hasPoints(userid, cost*iterations):
                raise CantAffordBoosterException(cost*iterations)

            addPoints(userid, -cost*iterations)

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
        
        totalTokensDropped = 0
        cards = []
        alertwaifus = []
        uniques = getUniqueCards(userid)
        totalDE = 0
        logPackName = "mega" + packname if mega else packname
        
        for iter in range(iterations):
            tokensDropped = 0
            for n in range(numTokens):
                if random.random() < tokenChance:
                    tokensDropped += 1
                    
            totalTokensDropped += tokensDropped
            iterDE = 0
            for i in range(numCards - tokensDropped):
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

                # actually drop the card
                logger.debug("using odds for card %d: %s", i, str(currentChances))
                card = int(dropCard(upgradeChances=currentChances, useEventWeightings=useEventWeightings,
                                    bannedCards=uniques + cards))
                cards.append(card)

                # check its rarity and adjust scaling data
                waifu = getWaifuById(card)
                iterDE += int(config["rarity%dValue" % waifu['base_rarity']])

                if waifu['base_rarity'] >= int(config["drawAlertMinimumRarity"]):
                    alertwaifus.append(waifu)

                if listed and buyable:
                    for r in range(numScalingRarities):
                        if r + minScalingRarity != waifu['base_rarity']:
                            scalingData[r] += cost / (numCards - tokensDropped)
                        else:
                            scalingData[r] = 0
                
            totalDE += iterDE
            # did they win a free amount-based reward pack?
            if packname in packAmountRewards and iterDE in packAmountRewards[packname]:
                reward = packAmountRewards[packname][iterDE]
                giveFreeBooster(userid, reward)
                msgArgs = (reward, packname, iterDE, reward)
                bot.message("#%s" % username, "You won a free %s pack due to getting a %s pack worth %d points. Open it with !freepacks open %s" % msgArgs, True)
        
        cards.sort()
        recordPullMetrics(*cards)
        addSpending(userid, cost*iterations)

        # pity pull data update
        cur.execute("UPDATE users SET pullScalingData = %s, eventTokens = eventTokens + %s WHERE id = %s",
                    [":".join(str(round(n)) for n in scalingData), totalTokensDropped, userid])

        # insert opened booster
        boosterid = addBooster(userid, logPackName, cost*iterations if buying else 0, 'open', totalTokensDropped, channel, isWhisper)
        for card in cards:
            addCard(userid, card, 'booster', boosterid)

        # alerts
        alertname = display_name if display_name.lower() == username.lower() else "%s (%s)" % (display_name, username)
        for w in alertwaifus:
            threading.Thread(target=sendDrawAlert, args=(channel, w, alertname)).start()

        return boosterid

def giveFreeBooster(userid, boostername, amount=1):
    with db.cursor() as cur:
        cur.execute("INSERT INTO freepacks (userid, boostername, remaining, total) VALUES(%s, %s, %s, %s)"
        + " ON DUPLICATE KEY UPDATE remaining = remaining + %s, total = total + %s",
        [userid, boostername, amount, amount, amount, amount])


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
            timeDiff = formatTimeDelta(timeUntilReset, False)
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
        
def generateRewardsSeed(cycleLength, numGoodRewards):
    # generate a reasonable rewards seed
    # "reasonable" is defined as the gap between successive good rewards
    # being between (CL/NumGood)/2 and (CL/NumGood)*2 every time
    # where gap is 1, not 0, for two consecutive good rewards
    # uses 0 to (numGoodRewards-1) to represent the good rewards
    # and other numbers to represent the bad
    hasSeed = False
    while not hasSeed:
        seed = random.randrange(0, 0x10000000000000000)
        if numGoodRewards == 0 or cycleLength == numGoodRewards:
            return seed
        generator = random.Random(seed)
        order = [x for x in range(cycleLength)]
        generator.shuffle(order)
        hasSeed = True
        lastPos = -1
        for i in range(int(numGoodRewards)):
            pos = lastPos + 1
            while order[pos] >= numGoodRewards:
                pos += 1
            if pos - lastPos <= (cycleLength/numGoodRewards)/2 or pos - lastPos >= (cycleLength/numGoodRewards)*2:
                hasSeed = False
                break
            lastPos = pos
        if cycleLength - lastPos >= (cycleLength/numGoodRewards)*2:
            hasSeed = False
    return seed
    
# returns (cycle length, number of good rewards) for use elsewhere
def getRewardsMetadata():
    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*), SUM(IF(is_good != 0, 1, 0)) FROM free_rewards")
        return cur.fetchone()
        


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

    def handleNameChange(self, oldName, newName):
        return self.handleNameChanges({oldName: newName})

    def handleNameChanges(self, nameMapping):
        with db.cursor() as cur:
            oldNames = list(nameMapping)
            cur.execute("SELECT name FROM channels WHERE name IN(%s)" % ",".join(["%s" * len(oldNames)]), oldNames)
            channelsFound = [row[0] for row in cur.fetchall()]
            for oldName in channelsFound:
                newName = nameMapping[oldName]
                hOldName = "#" + oldName
                hNewName = "#" + newName
                # channel name changed, attempt to deal with it
                try:
                    logger.debug("Attempting to handle channel name change %s -> %s..." % (oldName, newName))
                    cur.execute("UPDATE channels SET name = %s WHERE name = %s", [newName, oldName])
                    cur.execute("UPDATE bets SET channel = %s WHERE channel = %s", [hNewName, hOldName])
                    cur.execute("UPDATE boosters_opened SET channel = %s WHERE channel = %s", [hNewName, hOldName])
                    self.join(hNewName)
                    self.addchannels.append(hNewName)
                    self.leavechannels.append(hOldName)
                    self.part(hOldName)
                    logger.debug("OK, should be successful.")
                except Exception:
                    logger.debug("Could not handle name change properly, abandoning the attempt.")

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
                        cur.execute("SELECT id FROM cards WHERE boosterid = %s ORDER BY waifuid ASC",
                                    [pack[0]])
                        cardIDs = [row[0] for row in cur.fetchall()]
                        cards = [getCard(card) for card in cardIDs]
                        waifuIDs = [card['waifuid'] for card in cards]
                        # keep the best cards
                        cards.sort(key=lambda card: -card['base_rarity'])
                        # count any cards in the pack that don't take up hand space
                        # we can use this as a shortcut since these will always be the highest rarities too
                        nonSpaceCards = sum((1 if card['base_rarity'] >= int(config["numNormalRarities"]) else 0) for card in cards)
                        numKeep = int(min(max(handLimit(userid) - currentCards(userid), 0) + nonSpaceCards, len(cards)))
                        keeps = cards[:numKeep]
                        des = cards[numKeep:]
                        logger.info("Expired pack for user %s (%d): keeping %s, disenchanting %s", pack[2], userid,
                                    str(keeps), str(des))
                        for card in keeps:
                            updateCard(card['id'], {"boosterid": None})
                        numPoints = 0
                        for card in des:
                            numPoints += disenchant(self, card['id'])
                        addPoints(userid, numPoints)
                        attemptPromotions(*waifuIDs)
                        cur.execute("UPDATE boosters_opened SET status='closed', updated = %s WHERE id = %s",
                                    [current_milli_time(), pack[0]])

                    # inactivity timeout
                    # cur.execute("SELECT DISTINCT users.id, users.name FROM cards JOIN users ON cards.userid = users.id WHERE cards.rarity < %s AND users.inactivityImmunity = 0 AND (users.lastActiveTimestamp IS NULL OR users.lastActiveTimestamp < %s)", [config["numNormalRarities"], current_milli_time() - int(config["inactivityTimeoutDays"])*86400000])
                    # inactiveUsers = cur.fetchall()
                    # expireCount = len(inactiveUsers)
                    # expireCardCount = 0

                    # while len(inactiveUsers) > 0:
                    #     inactiveSlice = inactiveUsers[:100]
                    #     inactiveUsers = inactiveUsers[100:]
                    #     logger.debug("Expiring users: "+(", ".join(row[1] for row in inactiveSlice)))
                    #     expireCardCount += cur.execute("UPDATE cards SET userid = NULL, boosterid = NULL WHERE rarity < %s AND userid IN("+(",".join(["%s"] * len(inactiveSlice)))+")", [config["numNormalRarities"]] + [row[0] for row in inactiveSlice])
                    
                    # if expireCardCount > 0:
                    #     logger.debug("Expired %d cards from %d users" % (expireCardCount, expireCount))


                    # increase weightings
                    if int(config["last_weighting_update"]) < current_milli_time() - int(
                            config["weighting_increase_cycle"]):
                        logger.debug("Increasing card weightings...")
                        baseIncrease = float(config["weighting_increase_amount"])
                        cur.execute("UPDATE waifus SET normal_weighting = normal_weighting * %s WHERE base_rarity < %s",
                                    [baseIncrease, int(config["strongerWeightingMinRarity"])])
                        cur.execute("UPDATE waifus SET normal_weighting = normal_weighting * %s WHERE base_rarity BETWEEN %s AND %s",
                                    [baseIncrease**2, int(config["strongerWeightingMinRarity"]), int(config["numNormalRarities"])-1])
                        config["last_weighting_update"] = str(current_milli_time())
                        cur.execute("UPDATE config SET value = %s WHERE name = 'last_weighting_update'",
                                    [config["last_weighting_update"]])
                        # rounding edge case
                        cur.execute("UPDATE waifus SET normal_weighting=1 WHERE normal_weighting BETWEEN 0.999 AND 1")

                    # pudding expiry?
                    now = datetime.datetime.now()
                    ymdNow = now.strftime("%Y-%m-%d")
                    if ymdNow > config["last_pudding_check"]:
                        logger.debug("Processing pudding expiry...")
                        config["last_pudding_check"] = ymdNow
                        cur.execute("UPDATE config SET value = %s WHERE name = 'last_pudding_check'", [ymdNow])
                        
                        if now.day == 1:
                            # move pudding down a category, alert people with expiring pudding
                            cur.execute("UPDATE users SET puddingExpiring = puddingPrevious, puddingPrevious = puddingCurrent, puddingCurrent = 0")
                            cur.execute("SELECT name, puddingExpiring FROM users WHERE puddingExpiring > 0")
                            for userRow in cur.fetchall():
                                self.message("#%s" % userRow[0], "You have %d pudding expiring on the 8th of this month. !pudding to see your balance and to spend it." % userRow[1], True)
                        elif now.day == 8:
                            # actually expire pudding from 2 months ago
                            cur.execute("UPDATE users SET puddingExpiring = 0")

                    if booleanConfig("cpuWarActive") and now.minute < 5:
                        cpuVoters.clear()
                        self.message(config["marathonChannel"], "**You can now cast your votes for a CPU again!**", False)
            logger.debug("Checking live status of channels...")

            with busyLock:
                checkAndRenewAppAccessToken()
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
            
            marathonLive = config['marathonChannel'][1:] in viewerCount

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
                doneusers = set([])
                validactivity = set([])
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
                            logger.debug("Users in %s: %s", channel, self.channels[channel]['users'])
                            for viewer in self.channels[channel]['users']:
                                a.append(viewer)
                        doneusers.update(set(a))
                        if isLive[channelName]:
                            validactivity.update(set(a))
                            
                    except Exception:
                        logger.error("Error fetching chatters for %s, skipping their chat for this cycle" % channelName)
                        logger.error("Error: %s", str(sys.exc_info()))

                # process all users
                logger.debug("Caught users, giving points and creating accounts, amount to do = %d" % len(doneusers))
                newUsers = set([])

                maxPointsInactive = int(config["maxPointsInactive"])
                overflowPoints = 0
                passivePoints = int(config["passivePoints"])
                pointsMult = float(config["pointsMultiplier"])
                maraPointsMult = float(config["marathonPointsMultiplier"])

                doneusers = list(doneusers)
                with busyLock:
                    with db.cursor() as cur:
                        cur.execute('START TRANSACTION')
                        while len(doneusers) > 0:
                            logger.debug('Slicing... remaining length: %s' % len(doneusers))
                            currentSlice = doneusers[:100]
                            cur.execute("SELECT name, points, lastActiveTimestamp FROM users WHERE name IN(%s)" % ",".join(["%s"] * len(currentSlice)), currentSlice)
                            foundUsersData = cur.fetchall()
                            
                            foundUsers = set([row[0] for row in foundUsersData])
                            newUsers.update(set(currentSlice) - foundUsers)

                            if len(foundUsers) > 0:
                                updateData = []
                                for viewerInfo in foundUsersData:
                                    pointGain = passivePoints
                                    if viewerInfo[0] in activitymap and viewerInfo[0] in validactivity:
                                        pointGain += max(10 - int(activitymap[viewerInfo[0]]), 0)
                                    if viewerInfo[0] in marathonActivityMap and marathonActivityMap[viewerInfo[0]] < 10 and marathonLive:
                                        altPointGain = passivePoints + 10 - marathonActivityMap[viewerInfo[0]]
                                        altPointGain = round(altPointGain * maraPointsMult)
                                        pointGain = max(pointGain, altPointGain)
                                    pointGain = int(pointGain * pointsMult)
                                    if viewerInfo[2] is None:
                                        maxPointGain = max(maxPointsInactive - viewerInfo[1], 0)
                                        if pointGain > maxPointGain:
                                            overflowPoints += pointGain - maxPointGain
                                            pointGain = maxPointGain
                                    if pointGain > 0:
                                        updateData.append((pointGain, viewerInfo[0]))
                                cur.executemany("UPDATE users SET points = points + %s WHERE name = %s", updateData)
                            
                            doneusers = doneusers[100:]
                        cur.execute('COMMIT')

                if overflowPoints > 0:
                    logger.debug("Paying %d overflow points to the bot account" % overflowPoints)
                    with busyLock:
                        cur = db.cursor()
                        cur.execute("UPDATE users SET points = points + %s WHERE name = %s",
                                    [overflowPoints, config["username"]])
                        cur.close()

                # now deal with user names that aren't already in the DB
                newUsers = list(newUsers)
                if len(newUsers) > 10000:
                    logger.warning(
                        "DID YOU LET ME JOIN GDQ CHAT OR WHAT?!!? ... capping new user accounts at 10k. Sorry, bros!")
                    newUsers = newUsers[:10000]

                updateNames = []
                newAccounts = []
                updateData = []
                
                # don't hold the busyLock the whole time here - since we're dealing with twitch API
                while len(newUsers) > 0:
                    logger.debug("Adding new users...")
                    currentSlice = newUsers[:100]
                    logger.debug("New users to add: %s", str(currentSlice))
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
                    logger.debug("currentIdMapping: %s", currentIdMapping)
                    foundIdsData = []
                    if len(currentIdMapping) > 0:
                        with busyLock:
                            with db.cursor() as cur:
                                cur.execute("SELECT id, name FROM users WHERE id IN(%s)" % ",".join(["%s"] * len(currentIdMapping)),
                                            [id for id in currentIdMapping])
                                foundIdsData = cur.fetchall()
                    localIds = [row[0] for row in foundIdsData]
                    oldIdMapping = {row[0] : row[1] for row in foundIdsData}

                    # users to update the names for (id already exists)
                    updateNames.extend([(currentIdMapping[id], id) for id in currentIdMapping if id in localIds])
                    nameChanges = {oldIdMapping[id] : currentIdMapping[id] for id in currentIdMapping if id in localIds}
                    if len(nameChanges) > 0:
                        with busyLock:
                            self.handleNameChanges(nameChanges)

                    # new users (id does not exist)
                    newAccounts.extend([(id, currentIdMapping[id]) for id in currentIdMapping if id not in localIds])

                    # actually give points
                    for id in currentIdMapping:
                        viewer = currentIdMapping[id]
                        pointGain = passivePoints
                        if viewer in activitymap and viewer in validactivity:
                            pointGain += max(10 - int(activitymap[viewer]), 0)
                        if viewer in marathonActivityMap and marathonActivityMap[viewer] < 10 and marathonLive:
                            altPointGain = passivePoints + 10 - marathonActivityMap[viewer]
                            altPointGain = round(altPointGain * maraPointsMult)
                            pointGain = max(pointGain, altPointGain)
                        pointGain = int(pointGain * pointsMult)
                        updateData.append((pointGain, viewer))

                    # done with this slice
                    newUsers = newUsers[100:]

                # push new user data to database
                with busyLock:
                    with db.cursor() as cur:
                        cur.execute("START TRANSACTION")

                        if len(updateNames) > 0:
                            logger.debug("Updating names...")
                            cur.executemany("UPDATE users SET name = %s WHERE id = %s", updateNames)
                        
                        if len(newAccounts) > 0:
                            cur.executemany("INSERT INTO users (id, name, points, lastFree) VALUES(%s, %s, 0, 0)",
                                            newAccounts)
                        
                        if len(updateData) > 0:
                            cur.executemany("UPDATE users SET points = points + %s WHERE name = %s", updateData)

                        cur.execute("COMMIT")


                for user in activitymap:
                    activitymap[user] += 1
                for user in marathonActivityMap:
                    marathonActivityMap[user] += 1
            except Exception:
                logger.warning("We had an error during passive point gain. skipping this cycle.")
                logger.warning("Error: %s", str(sys.exc_info()))
                logger.warning("Last run query: %s", cur._last_executed)

            if self.autoupdate and booleanConfig("marathonBotFunctions"):
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
                    runners = [getRawRunner(runner) for runner in current[3:7] if runner is not None]
                    args = {"game": game}
                    args["category"] = " (%s)" % category if category is not None else ""
                    args["comingup"] = "COMING UP: " if wasNone else ""
                    args["runners"] = (" by " + ", ".join(runners)) if len(runners) > 0 else ""
                    args["title"] = config["marathonTitle"]
                    args["command"] = config["marathonHelpCommand"]
                    title = "{comingup}{title} - {game}{category}{runners} - !{command} in chat".format(**args)
                    twitchGame = game
                    if data["schedule"]["columns"][-1] == "TwitchGame" and current[-1] is not None:
                        twitchGame = current[-1]

                    updateBoth(twitchGame, title=title)
                    if len(runners) > 0:
                        thread.start_new_thread(MarathonBot.instance.updateFollowButtons, (runners,))
                except Exception:
                    logger.warning("Error updating from Horaro. Skipping this cycle.")
                    logger.warning("Error: %s", str(sys.exc_info()))

            if booleanConfig("marathonHelpAutopost"):
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

        activeCommands = ["checkhand", "points", "pudding", "freewaifu", "freebie", "disenchant",
         "de", "buy", "booster", "trade", "lookup", "owners", "alerts", "alert", "redeem", "wars",
          "war", "vote", "donate", "incentives", "upgrade", "search", "freepacks", "freepack", "bet",
           "sets", "set", "setbadge", "giveaway", "raffle", "bounty", "profile", "packspending", "godimage",
            "sendpoints", "sorthand"]
        
        userid = int(tags['user-id'])
        if userid in blacklist:
            if message.startswith("!") and message.split()[0][1:].lower() in activeCommands:
                self.message(source, "Account banned from playing TCG. If you're seeing this without knowing why, contact TCG staff @ %s/discord" % config["siteHost"], isWhisper)
            return
        elif "bot" in sender:
            with busyLock:
                with db.cursor() as cur:
                    cur.execute("SELECT id, banned FROM banned_users WHERE id = %s", [userid])
                    banrow = cur.fetchone()
                    if banrow[1] > 0:
                        # failsafe but shouldnt happen (blacklist updated in DB but not reloaded)
                        if message.startswith("!") and message.split()[0][1:].lower() in activeCommands:
                            self.message(source, "Account banned from playing TCG. If you're seeing this without knowing why, contact TCG staff @ %s/discord" % config["siteHost"], isWhisper)
                        return
                    elif banrow is None:
                        cur.execute("INSERT INTO banned_users (id, banned) VALUES(%s, 1)", [userid])
                        blacklist.append(userid)
                        if message.startswith("!") and message.split()[0][1:].lower() in activeCommands:
                            self.message(source, "This account appears to be a bot. If it is not, contact TCG staff @ %s/discord to have it unbanned." % config["siteHost"], isWhisper)
                        return
        
        activitymap[sender] = 0
        activitymap[channelowner] = 0
        isMarathonChannel = source == config['marathonChannel'] and not isWhisper
        if isMarathonChannel:
            marathonActivityMap[sender] = 0
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
                    self.handleNameChange(user[0], sender)

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
        # temp workaround to keep old syntax valid
        if command == "tokengacha":
            command = "tokenshop"
            args = ["gacha"] + args
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
            if command == "cpu" and isMarathonChannel and booleanConfig("cpuWarActive"):
                cpu = "" if len(args) < 1 else args[0].lower()
                if cpu.startswith("hdn"):
                    cpu = cpu[3:]
                if cpu in ["neptune", "noire", "blanc", "vert"]:
                    if tags['user-id'] in cpuVoters:
                        self.message(channel, "%s, you've already voted this hour!" % tags['display-name'], isWhisper)
                        return
                    with db.cursor() as cur:
                        cur.execute("UPDATE cpuwar SET votes = votes + 1 WHERE cpu = %s", [cpu])
                    formattedCpu = "HDN" + cpu[0].upper() + cpu[1:]
                    self.message(channel, "Registered %s's vote for %s" % (tags['display-name'], formattedCpu))
                    cpuVoters.append(tags['user-id'])
                elif cpu != "":
                    self.message(channel, "Invalid choice. Valid votes are Neptune, Noire, Blanc or Vert.", isWhisper)
                else:
                    with db.cursor() as cur:
                        cur.execute("SELECT cpu, votes FROM cpuwar ORDER BY votes DESC, cpu ASC")
                        currentVotes = cur.fetchall()
                    warStrings = ["%s - %d votes" % ("HDN" + row[0][0].upper() + row[0][1:], row[1]) for row in currentVotes]
                    self.message(channel, "Current results: %s" % ", ".join(warStrings), isWhisper)
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
                                 "%s, you don't have any waifus! Get your first with !freebie" % tags[
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
                                         "{user}, you have {curr} waifus, {bounties} bounties and {limit} total spaces. {link}".format(
                                             **msgArgs), True)
                        else:
                            self.message(whisperChannel,
                                         "{user}, you have {curr} waifus and {limit} total spaces. {link}".format(
                                             **msgArgs), True)
                        messages = ["Your current hand is: "]
                        for row in cards:
                            waifumsg = getWaifuRepresentationString(row['waifuid'], cardrarity=row[
                                'rarity']) + ' from {series} - {image} (Card ID {cardid}); '.format(**row)
                            if len(messages[-1]) + len(waifumsg) > 400:
                                messages.append(waifumsg)
                            else:
                                messages[-1] += waifumsg

                        for message in messages:
                            self.message(whisperChannel, message, True)
                    elif not isWhisper:
                        self.message(channel,
                                     "%s, to use verbose checkhand, follow the bot! Follow it and try again." %
                                     tags['display-name'])
                else:
                    if currentData['bounties'] > 0:
                        self.message(channel,
                                     "{user}, you have {curr} waifus, {bounties} bounties and {limit} total spaces. {link}".format(
                                         **msgArgs), isWhisper)
                    else:
                        self.message(channel,
                                     "{user}, you have {curr} waifus and {limit} total spaces. {link}".format(
                                         **msgArgs), isWhisper)
                return
            if command == "points":
                with db.cursor() as cur:
                    cur.execute("SELECT points FROM users WHERE id = %s", [tags['user-id']])
                    points = cur.fetchone()[0]
                    pudding = sum(getPuddingBalance(tags['user-id']))
                    self.message(channel, "%s, you have %d points and %d pudding!" % (tags['display-name'], points, pudding), isWhisper)
                    return
            if command == "pudding":
                subcmd = "" if len(args) < 1 else args[0].lower()
                if subcmd == "booster" or subcmd == "buy":
                    if len(args) < 2:
                        self.message(channel, "Usage: !pudding " + subcmd + " <name>", isWhisper)
                        return
                    # check that the pack is actually buyable
                    truename = boostername = args[1].lower()
                    mega = False
                    if boostername.startswith("mega"):
                        truename = boostername[4:]
                        mega = True
                    with db.cursor() as cur:
                        cur.execute("SELECT name, cost, canMega FROM boosters WHERE name = %s AND buyable = 1", [truename])
                        booster = cur.fetchone()
                        if booster is None or (mega and booster[2] == 0):
                            self.message(channel, "Invalid booster specified.", isWhisper)
                            return
                        # can they actually open it?
                        cur.execute("SELECT COUNT(*) FROM boosters_opened WHERE userid = %s AND status = 'open'",
                                [tags['user-id']])
                        boosteropen = cur.fetchone()[0] or 0

                        if boosteropen > 0:
                            self.message(channel,
                                        "%s, you have an open booster already! !booster show to check it." %
                                        tags['display-name'], isWhisper)
                            return
                        cost = math.ceil(int(booster[1])/int(config["puddingExchangeRate"]))*(5 if mega else 1)
                        if not hasPudding(tags['user-id'], cost):
                            self.message(channel, "%s, you can't afford a %s booster. They cost %d pudding." % (tags['display-name'], boostername, cost), isWhisper)
                            return
                        takePudding(tags['user-id'], cost)
                        try:
                            openBooster(self, tags['user-id'], sender, tags['display-name'], channel, isWhisper, truename, False, mega)
                            if checkHandUpgrade(tags['user-id']):
                                messageForHandUpgrade(tags['user-id'], tags['display-name'], self, channel, isWhisper)
                            self.message(channel, "%s, you open a %s booster for %d pudding: %s/booster?user=%s" % (tags['display-name'], boostername, cost, config["siteHost"], sender), isWhisper)
                        except InvalidBoosterException:
                            discordbody = {
                                "username": "WTCG Admin", 
                                "content" : "Booster type %s is broken, please fix it." % booster[0]
                            }
                            threading.Thread(target=sendAdminDiscordAlert, args=(discordbody,)).start()
                            self.message(channel,
                                        "There was an error processing your booster, please try again later.",
                                        isWhisper)
                            return
                elif subcmd == "list":
                    with db.cursor() as cur:
                        cur.execute("SELECT name, cost FROM boosters WHERE listed = 1 AND buyable = 1 ORDER BY sortIndex ASC")
                        boosters = cur.fetchall()
                        boosterInfo = ", ".join("%s / %d pudding" % (row[0], math.ceil(int(row[1])/int(config["puddingExchangeRate"]))) for row in boosters)
                        self.message(channel, "Current buyable packs: %s. !pudding booster <name> to buy a booster with pudding." % boosterInfo, isWhisper)
                elif subcmd == "topup":
                    if len(args) < 2:
                        self.message(channel, "Usage: !pudding topup <amount>", isWhisper)
                        return
                    try:
                        amount = int(args[1])
                    except ValueError:
                        self.message(channel, "Usage: !pudding topup <amount>", isWhisper)
                        return
                    if amount <= 0:
                        self.message(channel, "Invalid amount of pudding to buy.", isWhisper)
                        return

                    pointsCost = amount * int(config["puddingExchangeRate"])
                    if not hasPoints(tags['user-id'], pointsCost):
                        self.message(channel, "%s, you don't have enough points to buy %d pudding. You need %d." % (tags['display-name'], amount, pointsCost), isWhisper)
                        return
                    
                    addPoints(tags['user-id'], -pointsCost)
                    addPudding(tags['user-id'], amount)
                    newBalance = sum(getPuddingBalance(tags['user-id']))
                    msgArgs = (tags['display-name'], pointsCost, amount, newBalance)

                    self.message(channel, "%s, you spent %d points to buy %d extra pudding. You now have %d total pudding (!pudding for full details)" % msgArgs, isWhisper)
                else:
                    # base: show pudding balance broken down
                    pudding = getPuddingBalance(tags['user-id'])
                    if sum(pudding) == 0:
                        self.message(channel, "%s, you don't currently have any pudding. You can earn some by participating in bets or completing sets." % tags['display-name'], isWhisper)
                    else:
                        msgArgs = (tags['display-name'], sum(pudding), pudding[0], pudding[1], pudding[2], config["puddingExchangeRate"])
                        self.message(channel, "%s, you have %d total pudding: %d earned this month, %d earned last month, %d expiring soon. !pudding list to see what boosters you can buy, !pudding booster <name> to buy a booster with pudding. !pudding topup <amount> to buy extra pudding (%s points each)" % msgArgs, isWhisper)
                return
            if command == "freewaifu" or command == "freebie":
                # print("Checking free waifu egliability for " + str(sender))
                with db.cursor() as cur:
                    cur.execute("SELECT lastFree, rewardSeqSeed, rewardSeqIndex FROM users WHERE id = %s", [tags['user-id']])
                    res = cur.fetchone()
                    nextFree = 79200000 + int(res[0])
                    if nextFree > current_milli_time():
                        datestring = formatTimeDelta(nextFree - current_milli_time(), False)
                        self.message(channel,
                                     str(tags[
                                             'display-name']) + ", you need to wait {0} for your next free drop!".format(
                                         datestring), isWhisper=isWhisper)
                        return
                        
                    cur.execute("SELECT COUNT(*) FROM boosters_opened WHERE userid = %s AND status = 'open'", [tags['user-id']])
                    hasPack = cur.fetchone()[0] > 0
                    spaceInHand = currentCards(tags['user-id']) < handLimit(tags['user-id'])
                    
                    freeData = getRewardsMetadata()
                    
                    seed = res[1]
                    index = res[2]
                    
                    if seed is None or index >= freeData[0]:
                        seed = generateRewardsSeed(*freeData)
                        index = 0
                        
                    # retrieve their reward for this time
                    generator = random.Random(seed)
                    seq = [x for x in range(freeData[0])]
                    generator.shuffle(seq)
                    rewardNum = seq[index]
                    
                    if rewardNum >= freeData[1]:
                        # not good reward
                        lookup = [0, rewardNum - freeData[1]]
                    else:
                        # good
                        lookup = [1, rewardNum]
                        
                    cur.execute("SELECT points, waifuid, waifu_rarity, boostername FROM free_rewards WHERE `is_good` = %s AND `index` = %s", lookup)
                    rewardInfo = cur.fetchone()
                    
                    if rewardInfo is None:
                        discordbody = {
                            "username": "WTCG Admin", 
                            "content" : "The free reward database is misconfigured, please fix it."
                        }
                        threading.Thread(target=sendAdminDiscordAlert, args=(discordbody,)).start()
                        self.message(channel, "Could not retrieve your free reward, try again later.", isWhisper)
                        return
                        
                    # only one of the latter three rewards is allowed to be filled in, and there needs to be at least one reward.
                    cardRewardCount = sum([(1 if rewardInfo[n] is not None else 0) for n in range(1, 4)])
                    ovrRewardCount = sum([(1 if rewardInfo[n] is not None else 0) for n in range(4)])
                    if cardRewardCount > 1 or ovrRewardCount == 0:
                        discordbody = {
                            "username": "WTCG Admin", 
                            "content" : "The free reward database is misconfigured, please fix it."
                        }
                        threading.Thread(target=sendAdminDiscordAlert, args=(discordbody,)).start()
                        self.message(channel, "Could not retrieve your free reward, try again later.", isWhisper)
                        return
                        
                    # can they take the reward at the current time?
                    if (rewardInfo[1] is not None or rewardInfo[2] is not None) and hasPack and not spaceInHand:
                        # save seed in progress so it doesn't reset if this happens at the end of one
                        cur.execute("UPDATE users SET rewardSeqSeed = %s, rewardSeqIndex = %s WHERE id = %s", [seed, index, tags['user-id']])
                        self.message(channel, "%s, your hand is full and you have a booster open!" % tags['display-name'], isWhisper)
                        return
                        
                    # if we made it this far they can receive it. process it
                    if rewardInfo[0] is not None:
                        addPoints(tags['user-id'], rewardInfo[0])
                        if cardRewardCount == 0:
                            self.message(channel, "%s, you got your daily free reward: %d points!" % (tags['display-name'], rewardInfo[0]), isWhisper)
                        
                    pointsPrefix = ("%d points and " % rewardInfo[0]) if rewardInfo[0] is not None else ""
                    if rewardInfo[1] is not None or rewardInfo[2] is not None:
                        if rewardInfo[1] is not None:
                            wid = rewardInfo[1]
                        else:
                            wid = dropCard(rarity=rewardInfo[2], bannedCards=getUniqueCards(tags['user-id']))
                        
                        row = getWaifuById(wid)
                        recordPullMetrics(row['id'])
                        if row['base_rarity'] >= int(config["drawAlertMinimumRarity"]):
                            threading.Thread(target=sendDrawAlert, args=(channel, row, str(tags["display-name"]))).start()
                        
                        boosterid = None
                        if not spaceInHand:
                            boosterid = addBooster(tags['user-id'], 'freebie', 0, 'open', 0, channel, isWhisper)
                        addCard(tags['user-id'], row['id'], 'freebie', boosterid)
                        if spaceInHand:
                            attemptPromotions(row['id'])
                            
                        droplink = config["siteHost"] + "/booster?user=" + sender
                        msgArgs = {"username": tags['display-name'], "id": row['id'],
                                   "rarity": config["rarity%dName" % row['base_rarity']],
                                   "name": row['name'], "series": row['series'],
                                   "link": row['image'] if spaceInHand else "",
                                   "pack": " ( %s )" % droplink if not spaceInHand else "",
                                   "points": pointsPrefix}
                        
                        self.message(channel, "{username}, you got your daily free reward: {points}[{id}][{rarity}] {name} from {series} - {link}{pack}".format(**msgArgs), isWhisper)
                       
                            
                    if rewardInfo[3] is not None:
                        if hasPack:
                            # send the pack to freepacks
                            giveFreeBooster(tags['user-id'], rewardInfo[3])
                            self.message(channel, "%s, you got your daily free reward: %sa %s booster (sent to !freepacks)" % (tags['display-name'], pointsPrefix, rewardInfo[3]), isWhisper)
                        else:
                            try:
                                packid = openBooster(self, tags['user-id'], sender, tags['display-name'], channel, isWhisper, rewardInfo[3], False)
                                if checkHandUpgrade(tags['user-id']):
                                    messageForHandUpgrade(tags['user-id'], tags['display-name'], self, channel, isWhisper)
                                self.message(channel, "%s, you got your daily free reward: %sa %s booster - %s/booster?user=%s" % (tags['display-name'], pointsPrefix, rewardInfo[3], config['siteHost'], sender), isWhisper)
                            except InvalidBoosterException:
                                discordbody = {
                                    "username": "WTCG Admin", 
                                    "content" : "The free reward database is misconfigured, please fix it."
                                }
                                threading.Thread(target=sendAdminDiscordAlert, args=(discordbody,)).start()
                                self.message(channel, "Could not retrieve your free reward, try again later.", isWhisper)
                                return

                    cur.execute("UPDATE users SET lastFree = %s, rewardSeqSeed = %s, rewardSeqIndex = %s WHERE id = %s", [current_milli_time(), seed, index + 1, tags['user-id']])
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
                disenchantingSpecial = False
                godRarity = int(config["numNormalRarities"]) - 1

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
                    except AmbiguousWaifuException:
                        self.message(channel,
                                     "You have more than one copy of waifu %s in your hand. Please specify a card ID instead. You can find your card IDs using !checkhand" % (
                                         arg), isWhisper)
                        return
                    except ValueError:
                        self.message(channel, "Could not decipher one or more of the waifu/card IDs you provided.",
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
                    if row['waifuid'] not in checkPromos:
                        checkPromos.append(row['waifuid'])
                    baseValue = int(config["rarity" + str(row['rarity']) + "Value"])
                    gain = disenchant(self, row['cardid'])
                    
                    if row['base_rarity'] >= int(config["numNormalRarities"]):
                        disenchantingSpecial = True
                    
                    pointsGain += gain
                    if gain > baseValue:
                        ordersFilled += 1
                    elif row['rarity'] >= int(config["disenchantAlertMinimumRarity"]):
                        # valuable waifu disenchanted
                        waifuData = getWaifuById(row['waifuid'])
                        waifuData['base_rarity'] = row['rarity']  # cheat to make it show any promoted rarity override
                        threading.Thread(target=sendDisenchantAlert,
                                         args=(channel, waifuData, tags["display-name"])).start()

                    if row['rarity'] == godRarity:
                        # check image change
                        with db.cursor() as cur:
                            cur.execute("UPDATE godimage_requests SET state='cancelled' WHERE requesterid = %s AND cardid = %s AND state = 'pending'", [tags['user-id'], row['cardid']])
                            if cur.rowcount > 0:
                                # request was cancelled
                                waifuData = getWaifuById(row['waifuid'])
                                self.message("#%s" % sender, "Your image change request for [%d] %s was cancelled since you disenchanted it." % (row['waifuid'], waifuData['name']), True)
                
                attemptPromotions(*checkPromos)
                addPoints(tags['user-id'], pointsGain)
                
                if disenchantingSpecial:
                    checkFavouriteValidity(tags['user-id'])

                if len(disenchants) == 1:
                    buytext = " (bounty filled)" if ordersFilled > 0 else ""
                    self.message(channel, "Successfully disenchanted waifu %d%s. %s gained %d points" % (
                        disenchants[0]['waifuid'], buytext, str(tags['display-name']), pointsGain), isWhisper=isWhisper)
                else:
                    buytext = " (%d bounties filled)" % ordersFilled if ordersFilled > 0 else ""
                    self.message(channel,
                                 "Successfully disenchanted %d waifus%s. Added %d points to %s's account" % (
                                     len(disenchants), buytext, pointsGain, str(tags['display-name'])),
                                 isWhisper=isWhisper)

                return
            if command == "giveme":
                self.message(channel, "No.", isWhisper=isWhisper)
                return
            if command == "buy":
                if len(args) != 1:
                    if len(args) > 0 and args[0].lower() == "booster":
                        self.message(channel, "%s, did you mean !booster buy?" % tags['display-name'], isWhisper)
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
                price = int(config["rarity" + str(rarity) + "BuyPrice"])
                if rarity >= int(config["numNormalRarities"]) or price <= 0:
                    self.message(channel, "You can't buy that rarity of waifu.", isWhisper=isWhisper)
                    return
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
                    addCard(tags['user-id'], row['id'], 'buy')
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
                                 "Usage: !booster list OR !booster buy <%s> OR !booster select <take/disenchant> (for each waifu) OR !booster show" % visiblepacks,
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
                currBooster = getOpenBooster(tags['user-id'])

                if (cmd == "show" or cmd == "select") and currBooster is None:
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
                    # check for shorthand syntax
                    if len(args) == 2:
                        if args[1].lower() == 'deall' or args[1].lower() == 'disenchantall':
                            selectArgs = ["disenchant"] * len(currBooster['cards'])
                        elif args[1].lower() == 'keep' or args[1].lower() == 'disenchant':
                            selectArgs = args[1:]
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

                    if len(selectArgs) != len(currBooster['cards']):
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
                    for i in range(len(currBooster['cards'])):
                        card = currBooster['cards'][i]
                        if selectArgs[i].lower() == "keep":
                            keepCards.append(card)
                            if card['base_rarity'] < int(config["numNormalRarities"]):
                                keepingCount += 1
                        else:
                            # disenchant
                            if card['base_rarity'] >= int(
                                    config["disenchantRequireConfirmationRarity"]) and not hasConfirmed:
                                confirmRarityName = config[
                                    "rarity%sName" % config["disenchantRequireConfirmationRarity"]]
                                self.message(channel,
                                             "%s, you are trying to disenchant one or more waifus of %s rarity or higher! If you are sure you want to do this, append \" yes\" to the end of your command." % (
                                                 tags['display-name'], confirmRarityName), isWhisper)
                                return
                            deCards.append(card)

                    if keepingCount + currentCards(tags['user-id']) > handLimit(tags['user-id']) and keepingCount != 0:
                        self.message(channel, "You can't keep that many waifus! !disenchant some!", isWhisper=isWhisper)
                        cur.close()
                        return
                    trash = len(keepCards) == 0
                    # if we made it through the whole pack without tripping confirmation, we can actually do it now
                    for card in keepCards:
                        updateCard(card['cardid'], {"boosterid": None})
                    gottenpoints = 0
                    ordersFilled = 0
                    for card in deCards:
                        baseValue = int(config["rarity" + str(card['base_rarity']) + "Value"])
                        gain = disenchant(self, card['cardid'])
                        gottenpoints += gain
                        if gain > baseValue:
                            ordersFilled += 1
                        elif card['base_rarity'] >= int(config["disenchantAlertMinimumRarity"]):
                            # valuable waifu being disenchanted
                            threading.Thread(target=sendDisenchantAlert,
                                             args=(channel, getWaifuById(card['waifuid']), str(tags["display-name"]))).start()
                    cardIDs = [row['waifuid'] for row in currBooster['cards']]
                    attemptPromotions(*cardIDs)
                    addPoints(tags['user-id'], gottenpoints)

                    # compile the message to be sent in chat
                    response = "You %s your booster pack%s" % (("trash", "") if trash else ("take"," and: "))

                    if len(keepCards) > 0:
                        response += " keep " + ', '.join(str(x['waifuid']) for x in keepCards) + ";"
                    if len(deCards) > 0 and not trash:
                        response += " disenchant the rest"

                    if ordersFilled > 0:
                        response += " (filling %d bounties);" % ordersFilled
                    elif len(deCards) > 0:
                        response += ";"

                    self.message(channel, response + ((" netting " + str(gottenpoints) + " points.") if gottenpoints>0 else ""),
                                 isWhisper=isWhisper)
                    cur.execute("UPDATE boosters_opened SET status = 'closed', updated = %s WHERE id = %s",
                                [current_milli_time(), currBooster['id']])
                    cur.close()
                    return

                if cmd == "list":
                    with db.cursor() as cur:
                        cur.execute("SELECT name, cost FROM boosters WHERE listed = 1 AND buyable = 1 ORDER BY sortIndex ASC")
                        boosters = cur.fetchall()
                        boosterInfo = ", ".join("%s / %d points" % (row[0], row[1]) for row in boosters)
                        self.message(channel, "Current buyable packs: %s. !booster buy <name> to buy a booster with points." % boosterInfo, isWhisper)
                    return
                
                if cmd == "buy":
                    if currBooster is not None:
                        self.message(channel,
                                     "You already have an open booster. Close it first!",
                                     isWhisper=isWhisper)
                        cur.close()
                        return
                    if len(args) < 2:
                        self.message(channel, "Usage: !booster buy <%s>" % visiblepacks, isWhisper=isWhisper)
                        cur.close()
                        return

                    truepackname = packname = args[1].lower()
                    mega = False
                    if packname.startswith("mega"):
                        truepackname = packname[4:]
                        mega = True
                    try:
                        openBooster(self, tags['user-id'], sender, tags['display-name'], channel, isWhisper, truepackname, True, mega)
                        if checkHandUpgrade(tags['user-id']):
                            messageForHandUpgrade(tags['user-id'], tags['display-name'], self, channel, isWhisper)

                        droplink = config["siteHost"] + "/booster?user=" + sender
                        self.message(channel, "{user}, you open a {type} booster: {droplink}".format(
                            user=tags['display-name'], type=packname, droplink=droplink), isWhisper=isWhisper)
                    except InvalidBoosterException:
                        self.message(channel, "Invalid booster type. Packs available right now: %s." % visiblepacks,
                                     isWhisper=isWhisper)
                    except CantAffordBoosterException as exc:
                        self.message(channel,
                                     "{user}, you don't have enough points for a {name} pack. You need {points}.".format(
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
                    if len(args) < 1 or (len(args) < 2 and args[0].lower() != "list"):
                        self.message(channel,
                                     "Usage: !trade <check/accept/decline/cancel> <user> OR !trade <user> <have> <want> OR !trade list",
                                     isWhisper=isWhisper)
                        return
                    subarg = args[0].lower()
                    if subarg == "list":
                        cur.execute("SELECT users.name FROM trades JOIN users ON trades.toid = users.id WHERE trades.fromid = %s AND trades.status = 'open'", [ourid])
                        sentTradesTo = [row[0] for row in cur.fetchall()]

                        cur.execute("SELECT users.name FROM trades JOIN users ON trades.fromid = users.id WHERE trades.toid = %s AND trades.status = 'open'", [ourid])
                        recvdTradesFrom = [row[0] for row in cur.fetchall()]

                        if len(sentTradesTo) == 0 and len(recvdTradesFrom) == 0:
                            self.message(channel, "You have no unresolved trades right now.", isWhisper)
                        else:
                            numTrades = len(sentTradesTo) + len(recvdTradesFrom)
                            parts = []
                            if len(sentTradesTo) > 0:
                                parts.append("Sent trades to: %s." % (", ".join(sentTradesTo)))
                            if len(recvdTradesFrom) > 0:
                                parts.append("Received trades from: %s." % (", ".join(recvdTradesFrom)))
                            self.message(channel, "%s, you have %d pending trade%s. %s"%(tags['display-name'], numTrades, "s" if numTrades > 1 else "", " ".join(parts)), isWhisper)
                        return
                    if subarg in ["check", "accept", "decline", "cancel"]:
                        otherparty = args[1].lower()

                        cur.execute("SELECT id FROM users WHERE name = %s", [otherparty])
                        otheridrow = cur.fetchone()
                        if otheridrow is None:
                            self.message(channel, "I don't recognize that username.", isWhisper=isWhisper)
                            return
                        otherid = int(otheridrow[0])

                        if ourid == otherid:
                            self.message(channel, "You cannot trade with yourself.", isWhisper)
                            return

                        if subarg == "cancel":
                            # different case here, since the trade is FROM us
                            cur.execute("SELECT id FROM trades WHERE fromid = %s AND toid = %s AND status = 'open' LIMIT 1", [ourid, otherid])
                            trade = cur.fetchone()

                            if trade is None:
                                self.message(channel, "You do not have a pending trade with %s. Send one with !trade %s <have> <want>" % (otherparty, otherparty), isWhisper)
                            else:
                                cur.execute("UPDATE trades SET status = 'cancelled', updated = %s WHERE id = %s", [current_milli_time(), trade[0]])
                                self.message(channel, "You cancelled your pending trade with %s." % otherparty, isWhisper)
                            return

                        # look for trade row
                        cur.execute(
                            "SELECT id, want, have, points, payup FROM trades WHERE fromid = %s AND toid = %s AND status = 'open' LIMIT 1",
                            [otherid, ourid])
                        trade = cur.fetchone()

                        if trade is None:
                            self.message(channel,
                                         otherparty + " did not send you a trade. Send one with !trade " + otherparty + " <have> <want>",
                                         isWhisper=isWhisper)
                            return

                        want = getCard(trade[1])
                        have = getCard(trade[2])
                        tradepoints = trade[3]
                        payup = trade[4]
                        
                        # check that cards are still in place and owned by us
                        if want['userid'] is None or want['userid'] != ourid or have['userid'] is None or have['userid'] != otherid:
                            self.message(channel, "%s, one or more of the cards involved in the trade are no longer in their owners hands. Trade cancelled." % tags['display-name'], isWhisper)
                            cur.execute("UPDATE trades SET status = 'invalid', updated = %s WHERE id = %s", [current_milli_time(), trade[0]])
                            return

                        if (want['tradeableAt'] is not None and want['tradeableAt'] > current_milli_time()) or (have['tradeableAt'] is not None and have['tradeableAt'] > current_milli_time()):
                            self.message(channel, "%s, one or more of the cards involved in the trade are no longer able to be traded. Trade cancelled." % tags['display-name'], isWhisper)
                            cur.execute("UPDATE trades SET status = 'invalid', updated = %s WHERE id = %s", [current_milli_time(), trade[0]])
                            return

                        if subarg == "check":
                            wantdata = getWaifuById(want['waifuid'])
                            havedata = getWaifuById(have['waifuid'])

                            haveStr = getWaifuRepresentationString(havedata['id'], havedata['base_rarity'], have['rarity'], havedata['name'])
                            wantStr = getWaifuRepresentationString(wantdata['id'], wantdata['base_rarity'], want['rarity'], wantdata['name'])

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

                            nonpayer = ourid if payup == otherid else otherid

                            if tradepoints > 0 and not hasPoints(payup, tradepoints):
                                self.message(channel, "Sorry, but %s cannot cover the fair trading fee." % (
                                    "you" if payup == ourid else otherparty),
                                             isWhisper=isWhisper)
                                return

                            # move the cards
                            updateCard(want['id'], {"userid": otherid, "boosterid": None})
                            updateCard(have['id'], {"userid": ourid, "boosterid": None})
                            # cancel pending godimage requests for these cards
                            godRarity = int(config["numNormalRarities"]) - 1
                            if want['rarity'] == godRarity:
                                cur.execute("UPDATE godimage_requests SET state = 'cancelled' WHERE cardid = %s AND state = 'pending'", [want['id']])
                                if cur.rowcount > 0:
                                    # a request was actually cancelled
                                    wantdata = getWaifuById(want['waifuid'])
                                    self.message("#%s" % sender, "Your image change request for [%d] %s was cancelled since you traded it away." % (want['waifuid'], wantdata['name']), True)
                                
                            if have['rarity'] == godRarity:
                                cur.execute("UPDATE godimage_requests SET state = 'cancelled' WHERE cardid = %s AND state = 'pending'", [have['id']])
                                if cur.rowcount > 0:
                                    # a request was actually cancelled
                                    havedata = getWaifuById(have['waifuid'])
                                    self.message("#%s" % otherparty, "Your image change request for [%d] %s was cancelled since you traded it away." % (have['waifuid'], havedata['name']), True)

                            attemptPromotions(want['waifuid'], have['waifuid'])
                            if want['rarity'] >= int(config["numNormalRarities"]):
                                checkFavouriteValidity(ourid)
                            if have['rarity'] >= int(config["numNormalRarities"]):
                                checkFavouriteValidity(otherid)

                            # points
                            addPoints(payup, -tradepoints)
                            addPoints(nonpayer, tradepoints)

                            # done
                            cur.execute("UPDATE trades SET status = 'accepted', updated = %s WHERE id = %s",
                                        [current_milli_time(), trade[0]])

                            self.message(channel, "Trade executed!", isWhisper=isWhisper)
                            return

                    if len(args) < 3:
                        self.message(channel,
                                     "Usage: !trade <check/accept/decline/cancel> <user> OR !trade <user> <have> <want> OR !trade list",
                                     isWhisper=isWhisper)
                        return

                    other = args[0]

                    cur.execute("SELECT id FROM users WHERE name = %s", [other])
                    otheridrow = cur.fetchone()
                    if otheridrow is None:
                        self.message(channel, "I don't recognize that username.", isWhisper=isWhisper)
                        return

                    otherid = int(otheridrow[0])

                    if ourid == otherid:
                        self.message(channel, "You cannot trade with yourself.", isWhisper)
                        return
                    
                    ourhand = getHand(ourid)
                    otherhand = getHand(otherid)

                    try:
                        have = parseHandCardSpecifier(ourhand, args[1])
                    except CardNotInHandException:
                        self.message(channel, "%s, you don't own that waifu/card!" % tags['display-name'],
                                     isWhisper)
                        return
                    except AmbiguousWaifuException:
                        self.message(channel,
                                     "%s, you own more than one rarity of waifu %s! Please specify a card ID instead. You can find card IDs using !checkhand" % (
                                         tags['display-name'], args[1]), isWhisper)
                        return
                    except ValueError:
                        self.message(channel, "Only whole numbers/IDs please.", isWhisper)
                        return

                    if have["tradeableAt"] is not None and have["tradeableAt"] > current_milli_time():
                        self.message(channel, "%s, the card you are attempting to send in the trade is not tradeable right now!" % tags['display-name'], isWhisper)
                        return

                    try:
                        want = parseHandCardSpecifier(otherhand, args[2])
                    except CardNotInHandException:
                        self.message(channel,
                                     "%s, %s doesn't own that waifu/card!" % (tags['display-name'], other),
                                     isWhisper)
                        return
                    except AmbiguousWaifuException:
                        self.message(channel,
                                     "%s, %s owns more than one rarity of waifu %s! Please specify a card ID instead." % (
                                         tags['display-name'], other, args[2]), isWhisper)
                        return
                    except ValueError:
                        self.message(channel, "Only whole numbers/IDs please.", isWhisper)
                        return

                    if want["tradeableAt"] is not None and want["tradeableAt"] > current_milli_time():
                        self.message(channel, "%s, the card you are attempting to receive in the trade is not tradeable right now!" % tags['display-name'], isWhisper)
                        return
                            
                    # actual specials can't be traded
                    firstSpecialRarity = int(config["numNormalRarities"])
                    if have["rarity"] == firstSpecialRarity or want["rarity"] == firstSpecialRarity:
                        self.message(channel, "Sorry, cards of that rarity cannot be traded.", isWhisper)
                        return

                    payup = ourid
                    canTradeDirectly = (want["rarity"] == have["rarity"]) or (
                            want["rarity"] >= firstSpecialRarity and have["rarity"] >= firstSpecialRarity)
                    points = 0
                    if not canTradeDirectly:
                        if have["rarity"] >= firstSpecialRarity or want["rarity"] >= firstSpecialRarity:
                            self.message(channel,
                                         "Sorry, irregular rarity cards can only be traded for other irregular rarity cards.",
                                         isWhisper=isWhisper)
                            return
                        highercost = int(config["rarity" + str(max(have["rarity"], want["rarity"])) + "Value"])
                        lowercost = int(config["rarity" + str(min(have["rarity"], want["rarity"])) + "Value"])
                        points = highercost - lowercost
                        if want["rarity"] < have["rarity"]:
                            payup = otherid

                    # cancel any old trades with this pairing
                    cur.execute(
                        "UPDATE trades SET status = 'cancelled', updated = %s WHERE fromid = %s AND toid = %s AND status = 'open'",
                        [current_milli_time(), ourid, otherid])

                    # insert new trade
                    tradeData = [ourid, otherid, want['cardid'], have['cardid'], points, payup,
                                 current_milli_time(), "$$whisper$$" if isWhisper else channel]
                    cur.execute(
                        "INSERT INTO trades (fromid, toid, want, have, points, payup, status, created, originChannel) VALUES(%s, %s, %s, %s, %s, %s, 'open', %s, %s)",
                        tradeData)

                    havedata = getWaifuById(have['waifuid'])
                    wantdata = getWaifuById(want['waifuid'])

                    haveStr = getWaifuRepresentationString(have['waifuid'], havedata['base_rarity'], have['rarity'],
                                                           havedata['name'])
                    wantStr = getWaifuRepresentationString(want['waifuid'], wantdata['base_rarity'], want['rarity'],
                                                           wantdata['name'])

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
                        
                        ownerData = getWaifuOwners(waifu['id'], waifu['base_rarity'])
                        
                        ownerDescriptions = ownerData[0]
                        packholders = ownerData[1]
                        if len(ownerDescriptions) > 3:
                            ownerDescriptions = ownerDescriptions[0:2] + ["%d others" % (len(ownerDescriptions) - 2)]
                        waifu["rarity"] = config["rarity%dName" % waifu["base_rarity"]]

                        # check for packs

                        if len(ownerDescriptions) > 0:
                            waifu["owned"] = " - owned by " + ", ".join(ownerDescriptions)
                            if len(packholders) > 0:
                                waifu["owned"] += "; in a pack for: " + ", ".join(packholders)
                        elif len(packholders) > 0:
                            waifu["owned"] = " - in a pack for: " + ", ".join(packholders)
                        elif waifu["pulls"] > 0:
                            waifu["owned"] = " (not currently owned or in a pack)"
                        else:
                            waifu["owned"] = " (not dropped yet)"

                        # bounty info
                        if waifu["base_rarity"] >= int(config["numNormalRarities"]):
                            waifu["bountyinfo"] = ""
                            waifu["lp"] = ""
                        else:
                            with db.cursor() as cur:
                                cur.execute(
                                    "SELECT COUNT(*), COALESCE(MAX(amount), 0) FROM bounties WHERE waifuid = %s AND status='open'",
                                    [waifu['id']])
                                allordersinfo = cur.fetchone()

                                if allordersinfo[0] > 0:
                                    cur.execute(
                                        "SELECT amount FROM bounties WHERE userid = %s AND waifuid = %s AND status='open'",
                                        [tags['user-id'], waifu['id']])
                                    myorderinfo = cur.fetchone()
                                    minfo = {"count": allordersinfo[0], "highest": allordersinfo[1]}
                                    if myorderinfo is not None:
                                        minfo["mine"] = myorderinfo[0]
                                        if myorderinfo[0] == allordersinfo[1]:
                                            waifu[
                                                "bountyinfo"] = " {count} current bounties, your bid is highest at {highest} points.".format(
                                                **minfo)
                                        else:
                                            waifu[
                                                "bountyinfo"] = "{count} current bounties, your bid of {mine} points is lower than the highest at {highest} points.".format(
                                                **minfo)
                                    else:
                                        waifu[
                                            "bountyinfo"] = "{count} current bounties, the highest bid is {highest} points.".format(
                                            **minfo)
                                else:
                                    waifu["bountyinfo"] = "No current bounties on this waifu."

                            # last pull
                            if waifu["pulls"] == 0 or waifu["last_pull"] is None:
                                waifu["lp"] = ""
                            else:
                                lpdiff = (current_milli_time() - waifu["last_pull"]) // 86400000
                                if lpdiff == 0:
                                    waifu["lp"] = " Last pulled less than a day ago."
                                elif lpdiff == 1:
                                    waifu["lp"] = " Last pulled 1 day ago."
                                else:
                                    waifu["lp"] = " Last pulled %d days ago." % lpdiff

                        self.message(channel,
                                     '[{id}][{rarity}] {name} from {series} - {image}{owned}. {bountyinfo}{lp}'.format(
                                         **waifu),
                                     isWhisper=isWhisper)

                        if sender not in superadmins:
                            useInfoCommand(tags['user-id'], sender, channel, isWhisper)
                    except Exception as exc:
                        self.message(channel, "Invalid waifu ID.", isWhisper=isWhisper)

                return
            if command == "owners":
                if len(args) != 1:
                    self.message(channel, "Usage: !owners <id>", isWhisper=isWhisper)
                    return

                if infoCommandAvailable(tags['user-id'], sender, tags['display-name'], self, channel, isWhisper):
                    try:
                        waifu = getWaifuById(args[0])
                        assert waifu is not None
                        assert waifu['can_lookup'] == 1

                        ownerData = getWaifuOwners(waifu['id'], waifu['base_rarity'])
                        ownerDescriptions = ownerData[0]
                        packholders = ownerData[1]
                        waifu["rarity"] = config["rarity%dName" % waifu["base_rarity"]]

                        if len(ownerDescriptions) > 0:
                            waifu["owned"] = " is owned by " + ", ".join(ownerDescriptions)
                            if len(packholders) > 0:
                                waifu["owned"] += "; in a pack for: " + ", ".join(packholders)
                        elif len(packholders) > 0:
                            waifu["owned"] = " is in a pack for: " + ", ".join(packholders)
                        elif waifu["pulls"] > 0:
                            waifu["owned"] = " is not currently owned or in a pack"
                        else:
                            waifu["owned"] = " has not dropped yet"

                        self.message(channel,
                                     '[{id}][{rarity}] {name} from {series}{owned}.'.format(
                                         **waifu),
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
            if command == "nepdoc":
                self.message(channel, config["nepdocURL"], isWhisper=isWhisper)
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
                    row = cur.fetchone()
                    if row is None:
                        self.message(channel,
                                     "The bot is not in your channel, so alerts can't be set up for you. Ask an admin to let it join!",
                                     isWhisper=isWhisper)
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
                    if len(args) > 1 and args[1].lower() == "set":
                        rarity = int(config["numNormalRarities"]) - 1
                        isSet = True
                    else:
                        try:
                            rarity = parseRarity(args[1])
                        except Exception:
                            rarity = int(config["numNormalRarities"]) - 1
                    cur = db.cursor()
                    cur.execute("SELECT alertkey FROM channels WHERE name=%s", [sender])
                    row = cur.fetchone()
                    cur.close()
                    if row is None or row[0] is None:
                        self.message(channel,
                                     "Alerts do not seem to be set up for your channel, please set them up using !alerts setup",
                                     isWhisper=isWhisper)
                    else:
                        if isSet:
                            threading.Thread(target=sendSetAlert, args=(
                                sender, sender, "Test Set", ["Neptune", "Nepgear", "Some other test waifu"], "0 pudding",
                                False, False)).start()
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
            if command == "togglehoraro" and sender in admins and booleanConfig("marathonBotFunctions"):
                self.autoupdate = not self.autoupdate
                if self.autoupdate:
                    self.message(channel, "Enabled Horaro Auto-update.", isWhisper=isWhisper)
                else:
                    self.message(channel, "Disabled Horaro Auto-update.", isWhisper=isWhisper)
                return
            if sender in admins and command in ["status", "title"] and isMarathonChannel and booleanConfig("marathonBotFunctions"):
                updateTitle(" ".join(args))
                self.message(channel, "%s -> Title updated to %s." % (tags['display-name'], " ".join(args)))
                return
            if sender in admins and command == "game" and isMarathonChannel and booleanConfig("marathonBotFunctions"):
                updateGame(" ".join(args))
                self.message(channel, "%s -> Game updated to %s." % (tags['display-name'], " ".join(args)))
                return
            if sender in admins and booleanConfig("marathonBotFunctions") and command == "ffzfollowing":
                MarathonBot.instance.updateFollowButtons(args)
                self.message(channel, "%s -> Attempted to update follower buttons to %s." % (tags['display-name'], ", ".join(args)))
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
                cardid = None
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
                        packid = openBooster(self, tags['user-id'], sender, tags['display-name'], channel, isWhisper, redeemdata[3],
                                             False)
                        if checkHandUpgrade(tags['user-id']):
                            messageForHandUpgrade(tags['user-id'], tags['display-name'], self, channel, isWhisper)
                        received.append("a free booster: %s/booster?user=%s" % (config["siteHost"], sender))
                    except InvalidBoosterException:
                        discordbody = {
                            "username": "WTCG Admin", 
                            "content" : "Booster type %s is broken, please fix it." % redeemdata[3]
                        }
                        threading.Thread(target=sendAdminDiscordAlert, args=(discordbody,)).start()
                        self.message(channel,
                                    "There was an error processing your redeem, please try again later.",
                                    isWhisper)
                        cur.close()
                        return

                # waifu?
                if redeemdata[2] is not None:
                    waifuinfo = getWaifuById(redeemdata[2])
                    cardid = addCard(tags['user-id'], waifuinfo['id'], 'redeem')
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
                    "INSERT INTO tokens_claimed (tokenid, userid, points, waifuid, cardid, boostername, boosterid, timestamp, badgeID) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    [redeemdata[0], tags['user-id'], redeemdata[1], redeemdata[2], cardid, redeemdata[3], packid,
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
            if command in ["vote", "donate"] and isMarathonChannel:
                # pudding mode?
                puddingMode = False
                if len(args) > 0 and args[-1].lower() == "pudding":
                    puddingMode = True
                    args = args[:-1]
                    
                if len(args) == 1:
                    # special case: is there only 1 incentive and no bidwars?
                    with db.cursor() as cur:
                        cur.execute("SELECT COUNT(*) FROM bidWars WHERE `status` = 'open'")
                        warCount = cur.fetchone()[0] or 0
                        
                        cur.execute("SELECT COUNT(*) FROM incentives WHERE `status` = 'open'")
                        incCount = cur.fetchone()[0] or 0
                        
                        if warCount == 0 and incCount == 1:
                            # donate to that incentive
                            cur.execute("SELECT id FROM incentives WHERE `status` = 'open' LIMIT 1")
                            args = [cur.fetchone()[0]] + args
                
                if len(args) < 2:
                    if command == "vote":
                        self.message(channel, "Usage: !vote <warid> <choice> <amount>", isWhisper)
                    else:
                        self.message(channel,
                                 "Usage: !donate <id> <amount> (!incentives to see a list of incentives / IDs)",
                                 isWhisper)
                    return
                
                with db.cursor() as cur:
                    # find out if this is a bidwar, an incentive or nothing
                    cur.execute("SELECT id, title, status, openEntry, openEntryMinimum, openEntryMaxLength FROM bidWars WHERE id = %s", [args[0]])
                    war = cur.fetchone()

                    if war is not None:
                        if len(args) < 3:
                            self.message(channel, "Usage: !vote <warid> <choice> <amount>", isWhisper)
                            return

                        warid = war[0]
                        title = war[1]
                        status = war[2]
                        openEntry = war[3] != 0
                        openEntryMinimum = war[4]
                        openEntryMaxLength = war[5]

                        if status == 'closed':
                            self.message(channel, "%s -> That bidwar is currently closed." % tags['display-name'])
                            return

                        # pudding mode?
                        exchangeRate = int(config["puddingExchangeRateMarathon"])
                        currency = 'pudding' if puddingMode else 'points'

                        # check their points entry
                        try:
                            points = int(args[-1])
                            if points <= 0:
                                raise ValueError()
                        except ValueError:
                            self.message(channel, "%s -> Invalid amount of points/pudding entered." % tags['display-name'])
                            return

                        if puddingMode:
                            if not hasPudding(tags['user-id'], points):
                                self.message(channel, "%s -> You don't have that much pudding!" % tags['display-name'])
                                return

                            contribution = points * exchangeRate
                            contributionStr = "%d pudding (-> %d points)" % (points, contribution)
                        else:
                            if not hasPoints(tags['user-id'], points):
                                self.message(channel, "%s -> You don't have that many points!" % tags['display-name'])
                                return

                            contribution = points
                            contributionStr = "%d points" % points

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
                                    tags['display-name'], title))
                                return

                            for word in bannedWords:
                                if word in theirchoiceL:
                                    #self.message(channel, ".timeout %s 300" % sender, isWhisper)
                                    self.message(channel,
                                                "%s -> No vulgar choices allowed (warning)" % tags['display-name'])
                                    return

                            if contribution < openEntryMinimum:
                                self.message(channel,
                                            "%s -> You must contribute at least %d points or %d pudding to add a new choice to this bidwar!" % (
                                                tags['display-name'], openEntryMinimum, math.ceil(openEntryMinimum / exchangeRate)), isWhisper)
                                return

                            if len(theirchoice) > openEntryMaxLength:
                                self.message(channel,
                                            "%s -> The maximum length of a choice in the %s bidwar is %d characters." % (
                                                tags['display-name'], title, openEntryMaxLength), isWhisper)
                                return

                            # all clear, add it
                            if puddingMode:
                                takePudding(tags['user-id'], points)
                            else:
                                addPoints(tags['user-id'], -points)
                            actionTime = current_milli_time()
                            qargs = [warid, theirchoice, contribution, actionTime, tags['user-id'], actionTime, tags['user-id']]
                            cur.execute(
                                "INSERT INTO bidWarChoices (warID, choice, amount, created, creator, lastVote, lastVoter) VALUES(%s, %s, %s, %s, %s, %s, %s)",
                                qargs)

                            logargs = [tags['user-id'], warid, theirchoice, points, contribution, currency, current_milli_time()]
                        else:
                            # already existing choice, just vote for it
                            if puddingMode:
                                takePudding(tags['user-id'], points)
                            else:
                                addPoints(tags['user-id'], -points)
                            qargs = [contribution, current_milli_time(), tags['user-id'], warid, theirchoiceL]
                            cur.execute(
                                "UPDATE bidWarChoices SET amount = amount + %s, lastVote = %s, lastVoter = %s WHERE warID = %s AND choice = %s",
                                qargs)
                            logargs = [tags['user-id'], warid, theirchoiceL, points, contribution, currency, current_milli_time()]
                        
                        cur.execute("INSERT INTO `contributionLog` (`userid`, `to_id`, `to_choice`, `raw_amount`, `contribution`, `currency`, `timestamp`) " +
                            "VALUES(%s, %s, %s, %s, %s, %s, %s)", logargs)

                        self.message(channel, "%s -> Successfully added %s to %s in the %s bidwar." % (
                            tags['display-name'], contributionStr, theirchoice, title))
                        return
                    else:
                        cur.execute("SELECT id, title, amount, required FROM incentives WHERE id = %s", [args[0]])
                        incentive = cur.fetchone()

                        if incentive is None:
                            self.message(channel, "%s -> Invalid incentive/war ID." % tags['display-name'])
                            return

                        incid = incentive[0]
                        title = incentive[1]
                        currAmount = incentive[2]
                        required = incentive[3]

                        if currAmount >= required:
                            self.message(channel,
                                        "%s -> The %s incentive has already been met!" % (tags['display-name'], title))
                            return

                        try:
                            points = int(args[1])
                            if points <= 0:
                                raise ValueError()
                        except ValueError:
                            self.message(channel, "%s -> Invalid amount of points/pudding entered." % tags['display-name'])
                            return

                        if puddingMode:
                            exchangeRate = int(config["puddingExchangeRateMarathon"])
                            points = min(points, math.ceil((required - currAmount) / exchangeRate))

                            if not hasPudding(tags['user-id'], points):
                                self.message(channel, "%s -> You don't have that much pudding!" % tags['display-name'])
                                return

                            takePudding(tags['user-id'], points)

                            contribution = min(points * exchangeRate, required - currAmount)
                            contributionStr = "%d pudding (-> %d points)" % (points, contribution)
                            currency = 'pudding'
                        else:
                            points = min(points, required - currAmount)

                            if not hasPoints(tags['user-id'], points):
                                self.message(channel, "%s -> You don't have that many points!" % tags['display-name'])
                                return

                            addPoints(tags['user-id'], -points)
                            contribution = points
                            contributionStr = "%d points" % points
                            currency = 'points'
                        
                        cur.execute(
                            "UPDATE incentives SET amount = amount + %s, lastContribution = %s, lastContributor = %s WHERE id = %s",
                            [contribution, current_milli_time(), tags['user-id'], incid])

                        logargs = [tags['user-id'], incid, None, points, contribution, currency, current_milli_time()]
                        cur.execute("INSERT INTO `contributionLog` (`userid`, `to_id`, `to_choice`, `raw_amount`, `contribution`, `currency`, `timestamp`) " +
                            "VALUES(%s, %s, %s, %s, %s, %s, %s)", logargs)

                        if contribution + currAmount >= required:
                            self.message(channel, "%s -> You successfully donated %s and met the %s incentive!" % (
                                tags['display-name'], contributionStr, title), isWhisper)
                        else:
                            self.message(channel,
                                        "%s -> You successfully donated %s towards the %s incentive. It needs %d more points to be met." % (
                                            tags['display-name'], contributionStr, title, required - currAmount - contribution),
                                        isWhisper)

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
            if command == "upgrade":
                user = tags['user-id']

                if checkHandUpgrade(user):
                    messageForHandUpgrade(tags['user-id'], tags['display-name'], self, channel, isWhisper)
                    return

                spendingsToNext = getNextUpgradeSpendings(user) - getSpendings(user)
                multiplier = 0.5  # TODO: Make multiplier configurable
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
                                     "{user}, you do not have enough points to upgrade your hand for {price} points.".format(
                                         user=tags['display-name'], price=str(directPrice)), isWhisper=isWhisper)
                        return

                currLimit = handLimit(tags['user-id'])
                msgArgs = (tags['display-name'], currLimit, currLimit + 1, spendingsToNext, directPrice)
                self.message(channel, ("%s, you currently have %d slots from pack spending. " +
                                       "For space #%d, spend %d more points or use !upgrade buy for %d points.") % msgArgs,
                             isWhisper)

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
                    cur.execute("SELECT DISTINCT waifuid FROM cards WHERE userid IS NOT NULL AND boosterid IS NULL GROUP BY userid, waifuid, rarity HAVING COUNT(*) >= 2")
                    rows = cur.fetchall()
                    ids = [row[0] for row in rows]
                    attemptPromotions(*ids)
                    self.message(channel, "Rechecked promotions for %d waifus" % len(ids))
                return
            if command == "freepacks" or command == "freepack" or (command == "bet" and len(args) > 0 and args[0].lower() == "packs"):
                if len(args) > 0 and args[0].lower() in ["open", "claim", "redeem"]:
                    if len(args) < 2:
                        self.message(channel, "Usage: !freepacks open <booster name>", isWhisper)
                        return
                    with db.cursor() as cur:
                        cur.execute("SELECT remaining, boostername FROM freepacks WHERE userid = %s AND boostername = %s", [tags['user-id'], args[1]])
                        result = cur.fetchone()

                        if result is None or result[0] == 0:
                            self.message(channel, "You don't have any free packs of that type left to claim!", isWhisper)
                            return

                        # can they actually open it?
                        cur.execute("SELECT COUNT(*) FROM boosters_opened WHERE userid = %s AND status = 'open'",
                                [tags['user-id']])
                        boosteropen = cur.fetchone()[0] or 0

                        if boosteropen > 0:
                            self.message(channel,
                                        "%s, you can't open a free pack with an open booster! !booster show to check it." %
                                        tags['display-name'], isWhisper)
                            return

                        # all good
                        try:
                            packid = openBooster(self, tags['user-id'], sender, tags['display-name'], channel, isWhisper, args[1], False)
                            if checkHandUpgrade(tags['user-id']):
                                messageForHandUpgrade(tags['user-id'], tags['display-name'], self, channel, isWhisper)
                            cur.execute("UPDATE freepacks SET remaining = remaining - 1 WHERE userid = %s AND boostername = %s", [tags['user-id'], args[1]])
                            self.message(channel, "%s, you open a free %s booster: %s/booster?user=%s" % (tags['display-name'], result[1], config["siteHost"], sender), isWhisper)
                        except InvalidBoosterException:
                            discordbody = {
                                "username": "WTCG Admin", 
                                "content" : "Booster type %s is broken, please fix it." % args[1]
                            }
                            threading.Thread(target=sendAdminDiscordAlert, args=(discordbody,)).start()
                            self.message(channel,
                                        "There was an error opening your free pack, please try again later.",
                                        isWhisper)
                            return
                        return
                else:
                    with db.cursor() as cur:
                        cur.execute("SELECT boostername, remaining FROM freepacks WHERE userid = %s AND remaining > 0", [tags['user-id']])
                        freepacks = cur.fetchall()

                        if len(freepacks) == 0:
                            self.message(channel, "%s, you don't have any free pack entitlements right now." % tags['display-name'], isWhisper)
                        else:
                            freeStr = ", ".join("%s x%d" % (fp[0], fp[1]) for fp in freepacks)
                            self.message(channel, "%s, your current free packs: %s. !freepacks open <name> to open one." % (tags['display-name'], freeStr), isWhisper)
                        return
            if command == "bet":
                if isWhisper:
                    self.message(channel, "You can't use bet commands over whisper.", isWhisper)
                    return
                if len(args) < 1:
                    self.message(channel,
                                 "Usage: !bet <time> OR !bet status OR (as channel owner) !bet open OR !bet start OR !bet end OR !bet cancel OR !bet results OR !bet forcereset",
                                 isWhisper)
                    return
                # check restrictions
                with db.cursor() as cur:
                    cur.execute("SELECT betsBanned, forceresetsBanned FROM channels WHERE name = %s", [channel[1:]])
                    restrictions = cur.fetchone()
                    if restrictions is None:
                        # this shouldn't ever happen, but just in case...
                        self.message(channel, "This isn't a Waifu TCG channel. No can do.", isWhisper)
                        return
                
                if restrictions[0] != 0:
                    self.message(channel, "Bets are currently banned in this channel.")
                    return
                canAdminBets = sender in superadmins or (sender in admins and isMarathonChannel)
                isBroadcaster = str(tags["badges"]).find("broadcaster") > -1
                canManageBets = canAdminBets or isBroadcaster

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

                    if canManageBets and subcmd == "open":
                        if openBet(channel):
                            self.message(channel, "Bets are now open! Use !bet HH:MM:SS(.ms) to submit your bet!")
                        else:
                            self.message(channel,
                                         "There is already a prediction contest in progress in your channel! Use !bet status to check what to do next!")
                        return
                    elif canManageBets and subcmd == "start":
                        confirmed = args[-1].lower() == "yes"
                        try:
                            startBet(channel, confirmed)
                            self.message(channel, "Taking current time as start time! Good Luck! Bets are now closed.")
                        except NotEnoughBetsException:
                            self.message(channel, "WARNING: This bet does not currently have enough participants to be eligible for payout. To start anyway, use !bet start yes")
                        except NotOpenLongEnoughException:
                            self.message(channel, "You must wait at least %d minutes after opening a bet to start it." % int(config["betMinimumMinutesOpen"]))
                        except NoBetException:
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
                            if not canAdminBets and len(winners) >= int(config["betMinimumEntriesForPayout"]):
                                # notify the discordhook of the new bet completion
                                chanStr = channel[1:].lower()
                                discordArgs = {"channel": chanStr, "time": formattedTime, "link": "https://twitch.tv/" + chanStr}
                                discordbody = {
                                    "username": "WTCG Admin", 
                                    "content" : "A bet has just finished in {channel} with a time of {time}. Check results and consider payout at <{link}>.".format(**discordArgs)
                                }
                                threading.Thread(target=sendAdminDiscordAlert, args=(discordbody,)).start()
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
                            cur.execute("SELECT bet FROM placed_bets WHERE userid = %s AND betid = %s",
                                        [tags["user-id"], betRow[0]])
                            placedBets = cur.fetchall()
                            placedBet = None if len(placedBets) == 0 else placedBets[0][0]
                            hasBet = placedBet is not None
                            if betRow[1] == 'open':
                                if canManageBets:
                                    if hasBet:
                                        self.message(channel,
                                                     "Bets are currently open for a new contest. %d bets have been placed so far. !bet start to close bets and start the run timer. Your bet currently is %s" % (
                                                         numBets, formatTimeDelta(placedBet)))
                                    elif not isBroadcaster:
                                        self.message(channel,
                                                     "Bets are currently open for a new contest. %d bets have been placed so far. !bet start to close bets and start the run timer. You have not bet yet." % numBets)
                                    else:
                                        self.message(channel,
                                                     "Bets are currently open for a new contest. %d bets have been placed so far. !bet start to close bets and start the run timer." % numBets)
                                else:
                                    if hasBet:
                                        self.message(channel,
                                                     "Bets are currently open for a new contest. %d bets have been placed so far. Your bet currently is %s" % (
                                                         numBets, formatTimeDelta(placedBet)))
                                    else:
                                        self.message(channel,
                                                     "Bets are currently open for a new contest. %d bets have been placed so far. You have not bet yet." % numBets)



                            elif betRow[1] == 'started':
                                elapsed = current_milli_time() - betRow[2]
                                formattedTime = formatTimeDelta(elapsed)
                                if canManageBets:
                                    if hasBet:
                                        self.message(channel,
                                                     "Run in progress - elapsed time %s. %d bets were placed. !bet end to end the run timer and determine results. Your bet is %s" % (
                                                         formattedTime, numBets, formatTimeDelta(placedBet)))
                                    elif not isBroadcaster:
                                        self.message(channel,
                                                     "Run in progress - elapsed time %s. %d bets were placed. !bet end to end the run timer and determine results. You did not bet." % (
                                                         formattedTime, numBets))
                                    else:
                                        self.message(channel,
                                                     "Run in progress - elapsed time %s. %d bets were placed. !bet end to end the run timer and determine results." % (
                                                         formattedTime, numBets))
                                else:
                                    if hasBet:
                                        self.message(channel,
                                                     "Run in progress - elapsed time %s. %d bets were placed. Your bet is %s" % (
                                                         formattedTime, numBets, formatTimeDelta(placedBet)))
                                    else:
                                        self.message(channel,
                                                     "Run in progress - elapsed time %s. %d bets were placed. You did not bet." % (
                                                         formattedTime, numBets))
                            else:
                                formattedTime = formatTimeDelta(betRow[3] - betRow[2])
                                paidOut = " and has been paid out" if betRow[1] == 'paid' else ""
                                if canManageBets:
                                    self.message(channel,
                                                 "No time prediction contest in progress. The most recent contest ended in %s with %d bets placed%s. Use !bet results to see full results or !bet open to open a new one." % (
                                                     formattedTime, numBets, paidOut))
                                else:
                                    self.message(channel,
                                                 "No time prediction contest in progress. The most recent contest ended in %s with %d bets placed%s." % (
                                                     formattedTime, numBets, paidOut))
                        cur.close()
                        return
                    elif canManageBets and subcmd == "results":
                        cur = db.cursor()
                        cur.execute("SELECT id, status FROM bets WHERE channel = %s AND `status` != 'open' ORDER BY id DESC LIMIT 1",
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
                    elif subcmd == "forcereset" and canManageBets:
                        # change a started bet to open, preserving all current bets made
                        with db.cursor() as cur:
                            cur.execute("SELECT id, status FROM bets WHERE channel = %s ORDER BY id DESC LIMIT 1",
                                        [channel])
                            betRow = cur.fetchone()

                            if betRow is None or betRow[1] != 'started':
                                self.message(channel, "There is no bet in progress in this channel.", isWhisper)
                            else:
                                if '#' + sender == channel:
                                    # own channel, check limit and restriction
                                    if restrictions[1] != 0:
                                        self.message(channel, "This channel is banned from using self forcereset at the present time.")
                                        return
                                    invocation = current_milli_time()
                                    period = invocation - int(config["betForceResetPeriod"])
                                    cur.execute("SELECT COUNT(*), MIN(`timestamp`) FROM forceresets WHERE channel = %s AND `timestamp` > %s", [channel, period])
                                    frData = cur.fetchone()
                                    if frData[0] >= int(config["betForceResetLimit"]):
                                        nextUse = int(frData[1]) + int(config["betForceResetPeriod"]) - invocation
                                        datestring = formatTimeDelta(nextUse, False)
                                        self.message(channel, "You are currently out of self forceresets. Your next one will be available in %s." % datestring)
                                        return
                                    cur.execute("INSERT INTO forceresets (channel, user, `timestamp`) VALUES(%s, %s, %s)", [channel, tags['user-id'], invocation])
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
                            cur.execute("SELECT id, status FROM bets WHERE channel = %s AND `status` != 'open' ORDER BY id DESC LIMIT 1",
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
                        if isMarathonChannel:
                            self.message(channel, "No forceenters allowed in the marathon channel.", isWhisper)
                            return
                       
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
                            datestring = formatTimeDelta(lastPayout + 79200000 - currTime, False)
                            self.message(channel, "Bet payout may be used again in this channel in %s." % datestring,
                                         isWhisper)
                            cur.close()
                            return

                        cur.execute(
                            "SELECT id, status, endTime FROM bets WHERE channel = %s AND status IN('completed', 'paid', 'cancelled') ORDER BY id DESC LIMIT 1",
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

                            if numEntries < int(config["betMinimumEntriesForPayout"]):
                                self.message(channel, "This contest had less than %d entrants, no payout." % int(config["betMinimumEntriesForPayout"]), isWhisper)
                                cur.close()
                                return

                            # calculate first run of prizes
                            minPrize = int(config["betMinPrize"])
                            maxPrize = int(config["betMaxPrize"]) * min(1 + numEntries/10, 2)
                            bbReward = int(config["baseBroadcasterReward"])
                            canWinBigPrizes = resultData["result"] >= 1800000
                            whispers = []
                            prizeStrings = []
                            place = 0
                            for winner in resultData["winners"]:
                                place += 1

                                if abs(winner["timedelta"]) < 1000 and canWinBigPrizes:
                                    booster = config["sameSecondBooster"]
                                    if abs(winner["timedelta"]) < 10:
                                        booster = config["almostExactBooster"]
                                    giveFreeBooster(winner["id"], booster)
                                    msg = "You won a %s booster from the bet in %s's channel. Open it in any chat with !freepacks open %s" % (booster, channel[1:], booster)
                                    prizeStrings.append("%s - %s pack" % (winner["name"], booster))
                                    cur.execute("UPDATE placed_bets SET prizePack = %s WHERE betid = %s AND userid = %s",
                                            [booster, betRow[0], winner["id"]])
                                else:
                                    pudding = minPrize + (maxPrize - minPrize) * (numEntries - place) / (numEntries - 1) / (1.4 if place > numEntries / 2 else 1)
                                    if place == 1:
                                        pudding *= 1.3
                                    if isMarathonChannel and booleanConfig("marathonBetBoost"):
                                        pudding *= 1.5
                                    if canWinBigPrizes and abs(winner["timedelta"]) < resultData["result"] / 120:
                                        pudding *= 1.5
                                    if winner["bet"] < resultData["result"] / 2 or winner["bet"] > resultData["result"] * 2:
                                        pudding *= 0.5
                                    pudding = round(pudding)
                                    addPudding(winner["id"], pudding)
                                    msg = "You won %d pudding from the bet in %s's channel. Check and spend it with !pudding" % (pudding, channel[1:])
                                    prizeStrings.append("%s - %d pudding" % (winner["name"], pudding))
                                    cur.execute("UPDATE placed_bets SET prizePudding = %s WHERE betid = %s AND userid = %s",
                                            [pudding, betRow[0], winner["id"]])
                                whispers.append(('#' + winner["name"], msg))

                            # broadcaster prize
                            # run length in hours * 30, rounded to nearest whole pudding
                            runHours = resultData["result"] / 3600000.0
                            bcPrize = round(min(max(runHours, 1) * bbReward, int(config["maxBroadcasterReward"])))
                            capped = False
                            if not isMarathonChannel:
                                cur.execute("SELECT COALESCE(SUM(paidBroadcaster), 0) FROM bets WHERE status='paid' AND SUBSTRING(FROM_UNIXTIME(startTime/1000),1,7)=SUBSTRING(NOW(),1,7) AND channel = %s", [channel])
                                puddingMonth = cur.fetchone()[0] or 0
                                if puddingMonth + bcPrize > int(config["maxMonthlyBCReward"]):
                                    bcPrize = int(config["maxMonthlyBCReward"]) - puddingMonth
                                    capped = True
                            prizeStrings.append("%s (broadcaster) - %d pudding%s" % (channel[1:], bcPrize, " (monthly cap reached)" if capped else ""))
                            whispers.append((channel, "You were rewarded %d pudding%s for running your recent bet. Check and spend it with !pudding" % (bcPrize, " (monthly cap reached)" if capped else "")))
                            # skip using addPudding to save a database lookup
                            cur.execute("UPDATE users SET puddingCurrent = puddingCurrent + %s WHERE name = %s", [bcPrize, channel[1:]])
                            
                            # start cooldown for next bet payout at max(endTime, lastPayout + 22h)
                            payoutTime = min(max(betRow[2], lastPayout + 79200000), current_milli_time())
                            cur.execute(
                                "UPDATE bets SET status = 'paid', paidBroadcaster = %s, paidAt = %s WHERE id = %s",
                                [bcPrize, payoutTime, betRow[0]])

                            messages = ["Paid out the following prizes: "]
                            first = True
                            for prize in prizeStrings:
                                msg = prize if first else "; " + prize
                                if len(messages[-1] + msg) > 400:
                                    messages.append(prize)
                                else:
                                    messages[-1] += msg
                                first = False

                            for message in messages:
                                self.message(channel, message, isWhisper)

                            # alert each person individually as well
                            # sent after the messages to the channel itself deliberately
                            for whisper in whispers:
                                self.message(whisper[0], whisper[1], True)

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
                        cur.executemany("INSERT INTO waifus (name, image, base_rarity, series) VALUES(%s, %s, %s, %s)",
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
                                     config["siteHost"], sender.lower()), isWhisper)
                    return
                
                subcmd = args[0].lower()

                if subcmd == "claim":
                    with db.cursor(pymysql.cursors.DictCursor) as cur:
                        claimable = 0

                        cur.execute("SELECT sets.* FROM sets LEFT JOIN sets_claimed ON (sets.id=sets_claimed.setid AND sets_claimed.userid = %s) WHERE sets.claimable = 1 AND sets_claimed.userid IS NULL AND id NOT IN(SELECT DISTINCT setID FROM set_cards LEFT JOIN cards ON (set_cards.cardID=cards.waifuid AND cards.userid=%s) WHERE cards.userid IS NULL)", [tags['user-id']]*2)

                        for row in cur.fetchall():
                            claimable += 1
                            if row["lastClaimTime"] is not None:
                                cooldown = row["lastClaimTime"] + int(config["setCooldownDays"])*86400000 - current_milli_time()
                                if cooldown > 0:
                                    # on cooldown
                                    datestring = formatTimeDelta(cooldown, False)
                                    self.message(channel, "Could not claim the set %s as it is on cooldown, try again in %s" % (row["name"], datestring), isWhisper)
                                    continue
                            
                            # calculate rewards
                            rPts = row["rewardPoints"]
                            rPud = row["rewardPudding"]
                            first = False
                            badgeid = row["badgeid"]
                            
                            if row["firstClaimer"] is None:
                                first = True
                                rPts *= 2
                                rPud *= 2
                                badgeid = addBadge(row["name"], config["setBadgeDescription"], config["setBadgeDefaultImage"])
                            
                            # give rewards
                            addPoints(tags['user-id'], rPts)
                            addPudding(tags['user-id'], rPud)
                            giveBadge(tags['user-id'], badgeid)
                            claimTime = current_milli_time()

                            # record everything
                            scValues = [row["id"], tags['user-id'], rPts, rPud, claimTime]
                            cur.execute("INSERT INTO sets_claimed (setid, userid, rewardPoints, rewardPudding, timestamp) VALUES(%s, %s, %s, %s, %s)", scValues)

                            firstClaimer = tags['user-id'] if first else row["firstClaimer"]
                            cur.execute("UPDATE sets SET firstClaimer = %s, lastClaimTime = %s, badgeid = %s WHERE id = %s", [firstClaimer, claimTime, badgeid, row["id"]])

                            # let them know what happened
                            if rPts > 0 and rPud > 0:
                                reward = "%d points and %d pudding" % (rPts, rPud)
                            elif rPts > 0:
                                reward = "%d points" % rPts
                            else:
                                reward = "%d pudding" % rPud
                            
                            msgArgs = (row["name"], " (first claim!)" if first else "", tags['display-name'], reward)
                            self.message(channel, "Successfully claimed the set %s%s and rewarded %s with %s!" % msgArgs, isWhisper)

                            # send alert
                            cur.execute("SELECT waifus.name FROM set_cards INNER JOIN waifus ON set_cards.cardID = waifus.id WHERE setID = %s", [row["id"]])
                            cards = [sc["name"] for sc in cur.fetchall()]
                            threading.Thread(target=sendSetAlert,
                                         args=(channel, tags["display-name"], row["name"], cards, reward, first)).start()

                        if claimable == 0:
                            self.message(channel, "You do not have any completed sets that are available to be claimed. !sets to check progress.", isWhisper)
                            return
                elif subcmd == "checkid":
                    if len(args) == 1:
                        self.message(channel, "Usage: !sets checkid <set name>", isWhisper)
                        return
                    
                    checkname = " ".join(args[1:])
                    with db.cursor() as cur:
                        cur.execute("SELECT id, name FROM sets WHERE name = %s", checkname)
                        setData = cur.fetchone()

                        if setData is None:
                            self.message(channel, "Could not find that set name in the database.", isWhisper)
                        else:
                            self.message(channel, "Set %s's ID is %d." % (setData[1], setData[0]), isWhisper)
                        return
                else:
                    self.message(channel, "Usage: !sets OR !sets claim OR !sets checkid <set name>", isWhisper)
                    return
            if command == "setbadge":
                canManageImages = sender in superadmins
                if len(args) < 1:
                    if canManageImages:
                        self.message(channel, "Usage: !setbadge change / queue / check / accept / reject", isWhisper)
                    else:
                        self.message(channel, "Usage: !setbadge change / list / cancel", isWhisper)
                    return
                subcmd = args[0].lower()
                if subcmd in ["change", "request"]:
                    if len(args) < 3:
                        self.message(channel, "Usage: !setbadge change <id> <link>", isWhisper)
                        return

                    with db.cursor(pymysql.cursors.DictCursor) as cur:
                        cur.execute("SELECT id, name, firstClaimer, badgeid FROM sets WHERE id = %s", args[1])
                        setData = cur.fetchone()

                        if setData is None:
                            self.message(channel, "Invalid set ID.", isWhisper)
                            return

                        if setData["firstClaimer"] is None or int(setData["firstClaimer"]) != int(tags['user-id']):
                            self.message(channel, "You aren't the first claimer of %s!" % setData["name"], isWhisper)
                            return

                        if canManageImages:
                            # automatically do the change
                            try:
                                hostedURL = processBadgeURL(args[2])
                            except Exception as ex:
                                self.message(channel, "Could not process image. %s" % str(ex), isWhisper)
                                return
                                
                            cur.execute("UPDATE badges SET image = %s WHERE id = %s", [hostedURL, setData["badgeid"]])

                            # log the change for posterity
                            insertArgs = [tags['user-id'], setData["id"], args[2], tags['user-id'], current_milli_time()]
                            cur.execute("INSERT INTO setbadge_requests (requesterid, setid, image, state, moderatorid, created) VALUES(%s, %s, %s, 'auto_accepted', %s, %s)", insertArgs)

                            self.message(channel, "Set badge change processed successfully.", isWhisper)
                            return
                        else:
                            cur.execute("SELECT COALESCE(MAX(created), 0) AS lastReq FROM setbadge_requests WHERE state = 'accepted' AND setid = %s", [setData["id"]])
                            lastRequest = cur.fetchone()["lastReq"]
                            cooldown = lastRequest + int(config["imageChangeCooldownDays"])*86400000 - current_milli_time()

                            if cooldown > 0:
                                datestring = formatTimeDelta(cooldown, False)
                                self.message(channel, "Sorry, that set has had its badge changed too recently. Please try again in %s" % datestring, isWhisper)
                                return
                            
                            try:
                                validateBadgeURL(args[2])
                            except ValueError as ex:
                                self.message(channel, "Invalid link specified. %s" % str(ex), isWhisper)
                                return
                            except Exception:
                                self.message(channel, "There was an unknown problem with the link you specified. Please try again later.", isWhisper)
                                return
                            # cancel any old pending requests for this set
                            cur.execute("UPDATE setbadge_requests SET state = 'cancelled', updated = %s WHERE setid = %s AND state = 'pending'", [current_milli_time(), setData["id"]])

                            # record a new request
                            insertArgs = [tags['user-id'], setData["id"], args[2], current_milli_time()]
                            cur.execute("INSERT INTO setbadge_requests (requesterid, setid, image, state, created) VALUES(%s, %s, %s, 'pending', %s)", insertArgs)

                            # notify the discordhook of the new request
                            discordArgs = {"user": tags['display-name'], "setid": setData["id"], "name": setData["name"], "image": args[2]}
                            discordbody = {
                                "username": "WTCG Admin", 
                                "content" : "{user} requested a set badge change for {name} to <{image}>!\nUse `!setbadge check {setid}` in any chat to check it.".format(**discordArgs)
                            }
                            threading.Thread(target=sendAdminDiscordAlert, args=(discordbody,)).start()

                            self.message(channel, "Your request has been placed. You will be notified when bot staff accept or decline it.", isWhisper)
                            return
                elif subcmd == "list":
                    with db.cursor() as cur:
                        cur.execute("SELECT sets.id, sets.name FROM setbadge_requests sr JOIN sets ON sr.setid=sets.id WHERE sr.requesterid = %s AND sr.state = 'pending'", [tags['user-id']])
                        reqs = cur.fetchall()

                        if len(reqs) == 0:
                            self.message(channel, "You don't have any pending set badge change requests.", isWhisper)
                        else:
                            reqList = ", ".join(["[%d] %s" % (req[0], req[1]) for req in reqs])
                            self.message(channel, "%s, you have pending set badge change requests for: %s." % (tags['display-name'], reqList), isWhisper)
                        
                        return
                elif subcmd == "cancel":
                    if len(args) < 2:
                        self.message(channel, "Usage: !setbadge cancel <id>", isWhisper)
                        return
                    with db.cursor() as cur:
                        cur.execute("UPDATE setbadge_requests SET state = 'cancelled', updated = %s WHERE setid = %s AND state = 'pending'", [current_milli_time(), args[1]])
                        if cur.rowcount > 0:
                            # send discord notif
                            cur.execute("SELECT name FROM sets WHERE id = %s", [args[1]])
                            setName = cur.fetchone()[0]
                            discordArgs = {"user": tags['display-name'], "id": args[1], "name": setName}
                            discordbody = {
                                "username": "WTCG Admin", 
                                "content" : "{user} cancelled their badge change request for [{id}] {name}.".format(**discordArgs)
                            }
                            threading.Thread(target=sendAdminDiscordAlert, args=(discordbody,)).start()
                            self.message(channel, "You cancelled your badge change request for [%d] %s." % (args[1], setName), isWhisper)
                        else:
                            self.message(channel, "You didn't have a pending badge change request for that set.", isWhisper)
                        return
                elif subcmd == "queue" and canManageImages:
                    with db.cursor() as cur:
                        cur.execute("SELECT setid FROM setbadge_requests WHERE state = 'pending' ORDER BY created ASC")
                        queue = cur.fetchall()
                        if len(queue) == 0:
                            self.message(channel, "The request queue is currently empty.", isWhisper)
                        else:
                            queueStr = ", ".join(str(item[0]) for item in queue)
                            self.message(channel, "Current requested IDs for set badge changes: %s. !setbadge check <id> to see each request." % queueStr, isWhisper)
                        return
                elif canManageImages and subcmd in ["check", "accept", "reject"]:
                    if len(args) < 2:
                        self.message(channel, "Usage: !setbadge %s <set id>" % subcmd, isWhisper)
                        return
                    try:
                        setid = int(args[1])
                    except ValueError:
                        self.message(channel, "Usage: !setbadge %s <set id>" % subcmd, isWhisper)
                        return
                    with db.cursor() as cur:
                        cur.execute("SELECT sr.id, sr.image, users.id, users.name, sets.id, sets.name, sets.badgeid FROM setbadge_requests sr"
                        + " JOIN sets ON sr.setid = sets.id"
                        + " JOIN users ON sr.requesterid = users.id"
                        + " WHERE sr.setid = %s AND sr.state = 'pending'", [setid])
                        request = cur.fetchone()
                        if request is None:
                            self.message(channel, "There is no pending request for that set.", isWhisper)
                            return

                        if subcmd == "check":
                            msgArgs = {"user": request[3], "setid": request[4], "name": request[5], "image": request[1]}
                            self.message(channel, ("{user} requested {name}'s set badge image to be changed to {image} ." + 
                            " You can accept this request with !setbadge accept {setid}" +
                            " or deny it with !setbadge reject {setid} <reason>.").format(**msgArgs), isWhisper)
                        elif subcmd == "reject":
                            if len(args) < 3:
                                self.message(channel, "You must provide a reason to reject the request. If it is porn/illegal/etc, just ban the user.", isWhisper)
                                return
                            rejectionReason = " ".join(args[2:])
                            queryArgs = [tags['user-id'], current_milli_time(), rejectionReason, request[0]]
                            cur.execute("UPDATE setbadge_requests SET state = 'rejected', moderatorid = %s, updated = %s, rejection_reason = %s WHERE id = %s", queryArgs)

                            # notify them
                            self.message("#%s" % request[3], "Your badge change request for %s was rejected with the following reason: %s" % (request[5], rejectionReason), True)

                            self.message(channel, "Request rejected and user notified.", isWhisper)
                        else:
                            # update it
                            try:
                                hostedURL = processBadgeURL(request[1])
                            except Exception as ex:
                                self.message(channel, "Could not process image. %s. Check the URL yourself and if it is invalid reject their request." % str(ex), isWhisper)
                                return

                            cur.execute("UPDATE badges SET image = %s WHERE id = %s", [hostedURL, request[6]])

                            queryArgs = [tags['user-id'], current_milli_time(), request[0]]
                            cur.execute("UPDATE setbadge_requests SET state = 'accepted', moderatorid = %s, updated = %s WHERE id = %s", queryArgs)

                            # notify them
                            self.message("#%s" % request[3], "Your set badge change request for %s was accepted." % request[5], True)

                            self.message(channel, "Request accepted. The new set badge for %s is %s" % (request[5], hostedURL), isWhisper)
                        return
            if command == "debug" and sender in superadmins:
                if debugMode:
                    updateBoth("Hyperdimension Neptunia", "Testing title updates.")
                    self.message(channel, "Title and game updated for testing purposes")
                else:
                    self.message(channel, "Debug mode is off. Debug command disabled.")
                return
            if command == "givefreepack" and sender in superadmins:
                if len(args) < 2:
                    self.message(channel, "Usage: !givefreepack <username> <booster name> [<amount> (default 1)]", isWhisper)
                    return
                
                if len(args) >= 3:
                    try:
                        amount = int(args[2])
                    except ValueError:
                        self.message(channel, "Invalid amount specified.", isWhisper)
                        return
                else:
                    amount = 1

                with db.cursor() as cur:
                    cur.execute("SELECT id, name FROM users WHERE name = %s", [args[0]])
                    userData = cur.fetchone()
                    if userData is None:
                        self.message(channel, "Invalid username specified.", isWhisper)
                        return

                    cur.execute("SELECT COUNT(*) FROM boosters WHERE name = %s", [args[1]])
                    if cur.fetchone()[0] == 0:
                        self.message(channel, "Invalid booster name specified.", isWhisper)
                        return

                    giveFreeBooster(userData[0], args[1], amount)
                    if amount > 1:
                        self.message('#%s' % userData[1], "You were given %d free %s packs by an admin. Check them using !freepacks" % (amount, args[1]), True)
                    else:
                        self.message('#%s' % userData[1], "You were given a free %s pack by an admin. Open it using !freepacks open %s" % (args[1], args[1]), True)

                    self.message(channel, "Successfully gave %d %s packs to %s." % (amount, args[1], userData[1]), isWhisper)
                    return
                    
            if command == "nepcord":
                self.message(channel,
                             "To join the discussion in the official Waifu TCG Discord Channel, go to %s/discord" %
                             config["siteHost"], isWhisper=isWhisper)
                return
            if command == "giveaway":
                if booleanConfig("marathonOnlyGiveaway") and not isMarathonChannel:
                    return
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
                        min_amount = int(config["rarity%dMinBounty" % waifu['base_rarity']])
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
                hasConfirmed = False
                if len(args) > 0 and args[-1].lower() == "yes":
                    hasConfirmed = True
                    args = args[:-1]
                
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

                if waifu['base_rarity'] == int(config['numNormalRarities']):
                    self.message(channel, "You shouldn't be changing a special waifu into another rarity.", isWhisper)
                    return

                if rarity == waifu['base_rarity']:
                    self.message(channel, "[%d] %s is already %s base rarity!" % (
                        waifu['id'], waifu['name'], config['rarity%dName' % rarity]), isWhisper)
                    return
                    
                if not hasConfirmed and rarity > waifu['base_rarity'] and waifu['base_rarity'] < int(config["numNormalRarities"]) - 1:
                    # check for promoted copies existing
                    with db.cursor() as cur:
                        cur.execute("SELECT COUNT(*) FROM cards WHERE waifuid = %s AND rarity BETWEEN %s AND %s", [waifu['id'], waifu['base_rarity'] + 1, int(config["numNormalRarities"]) - 1])
                        if cur.fetchone()[0] > 0:
                            self.message(channel, "WARNING: You are trying to increase the rarity of a card which people have already promoted. This may cause undesirable results. Append ' yes' to your command if you want to do this anyway.", isWhisper)
                            return

                # okay, do it
                with db.cursor() as cur:
                    if rarity < int(config['numNormalRarities']):
                        cur.execute("UPDATE waifus SET normal_weighting = LEAST(GREATEST(normal_weighting, (SELECT MIN(w1.normal_weighting) FROM (SELECT * FROM waifus) w1 WHERE w1.base_rarity = %s)), (SELECT MAX(w2.normal_weighting) FROM (SELECT * FROM waifus) w2 WHERE w2.base_rarity = %s)), base_rarity = %s WHERE id = %s", [rarity, rarity, rarity, waifu['id']])
                    else:
                        cur.execute("UPDATE waifus SET base_rarity = %s WHERE id = %s", [rarity, waifu['id']])
                    
                    cur.execute("SELECT id, userid, boosterid FROM cards WHERE userid IS NOT NULL AND waifuid = %s AND rarity < %s", [waifu['id'], rarity])
                    for copy in cur.fetchall():
                        if rarity >= int(config['numNormalRarities']):
                            updateCard(copy[0], {"rarity": rarity})
                        else:
                            updateCard(copy[0], {"userid": None, "boosterid": None})
                            addCard(copy[1], waifu['id'], 'other', copy[2], rarity)
                    attemptPromotions(waifu['id'])

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
                        
                    if rarity >= int(config["numNormalRarities"]):
                        cur.execute("UPDATE users SET favourite = 1 WHERE favourite = %s AND (SELECT COUNT(*) FROM cards WHERE cards.userid = users.id AND cards.boosterid IS NULL AND cards.waifuid = %s) = 0", [waifu['id']] * 2)

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
                        self.message(channel, args[1] + " is not a number. Please try again.")
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
                            if str(w["waifuid"]) == str(newFav):
                                hasOrIsLowRarity = True
                                break
                    else:
                        hasOrIsLowRarity = True

                    if not canLookup and not hasOrIsLowRarity:
                        self.message(channel, tags[
                            "display-name"] + ", sorry, but that Waifu doesn't exist. Try a different one!",
                                     isWhisper)
                        return
                    elif newFavW["can_favourite"] == 0:
                        self.message(channel, "%s, sorry, but that Waifu can't be set as your favourite. Try a different one!" % tags['display-name'], isWhisper)
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

                totalspending = getSpendings(tags['user-id'])
                packstr = ", ".join("%dx %s" % (row[1], row[0]) for row in packstats)
                self.message(channel, "%s, you have spent %d total points on the following packs: %s." % (
                    tags['display-name'], totalspending, packstr), isWhisper)
                if checkHandUpgrade(tags["user-id"]):
                    self.message(channel, "... and this was enough to upgrade your hand to a new slot! naroYay",
                                 isWhisper)
                return
            if command == "godimage":
                canManageImages = sender in superadmins
                godRarity = int(config["numNormalRarities"]) - 1
                if len(args) < 1:
                    if canManageImages:
                        self.message(channel, "Usage: !godimage change / queue / check / accept / reject", isWhisper)
                    else:
                        self.message(channel, "Usage: !godimage change / list / cancel", isWhisper)
                    return
                subcmd = args[0].lower()
                if subcmd in ["change", "request"]:
                    if len(args) < 3:
                        self.message(channel, "Usage: !godimage change[global] <id> <link>", isWhisper)
                        return
                        
                    hand = getHand(tags['user-id'])
                    try:
                        card = parseHandCardSpecifier(hand, args[1], godRarity)
                    except CardNotInHandException:
                        self.message(channel, "%s, you don't own that waifu/card or it isn't god rarity!" % tags['display-name'],
                                     isWhisper)
                        return
                    except AmbiguousWaifuException:
                        self.message(channel,
                                     "%s, you own more than one god card of waifu %s! Please specify a card ID instead. You can find card IDs using !checkhand" % (
                                         tags['display-name'], args[1]), isWhisper)
                        return
                    except ValueError:
                        self.message(channel, "Only whole numbers/IDs please.", isWhisper)
                        return

                    with db.cursor() as cur:
                        if card["base_rarity"] == godRarity:
                            self.message(channel, "Base god rarity waifus cannot have their picture changed!", isWhisper)
                            return

                        if canManageImages:
                            # automatically do the change
                            try:
                                hostedURL = processWaifuURL(args[2])
                            except Exception as ex:
                                self.message(channel, "Could not process image. %s" % str(ex), isWhisper)
                                return
                                
                            updateCard(card['cardid'], {"customImage": hostedURL})

                            # log the change for posterity
                            insertArgs = [tags['user-id'], card['cardid'], args[2], tags['user-id'], current_milli_time()]
                            cur.execute("INSERT INTO godimage_requests (requesterid, cardid, image, state, moderatorid, created) VALUES(%s, %s, %s, 'auto_accepted', %s, %s)", insertArgs)

                            self.message(channel, "Image change processed successfully.", isWhisper)
                            return
                        else:
                            cur.execute("SELECT COALESCE(MAX(created), 0) FROM godimage_requests WHERE state = 'accepted' AND cardid = %s", [card['cardid']])
                            lastRequest = cur.fetchone()[0]
                            cooldown = lastRequest + int(config["imageChangeCooldownDays"])*86400000 - current_milli_time()

                            if cooldown > 0:
                                datestring = formatTimeDelta(cooldown, False)
                                self.message(channel, "Sorry, that card has had its image changed too recently. Please try again in %s" % datestring, isWhisper)
                                return
                            
                            try:
                                validateWaifuURL(args[2])
                            except ValueError as ex:
                                self.message(channel, "Invalid link specified. %s" % str(ex), isWhisper)
                                return
                            except Exception:
                                self.message(channel, "There was an unknown problem with the link you specified. Please try again later.", isWhisper)
                                return
                            # cancel any old pending requests for this card
                            cur.execute("UPDATE godimage_requests SET state = 'cancelled', updated = %s WHERE cardid = %s AND state = 'pending'", [current_milli_time(), card['cardid']])

                            # record a new request
                            insertArgs = [tags['user-id'], card['cardid'], args[2], current_milli_time()]
                            cur.execute("INSERT INTO godimage_requests (requesterid, cardid, image, state, created) VALUES(%s, %s, %s, 'pending', %s)", insertArgs)

                            # notify the discordhook of the new request
                            discordArgs = {"user": tags['display-name'], "waifuid": card['waifuid'], "cardid": card['cardid'], "name": card["name"], "image": args[2]}
                            discordbody = {
                                "username": "WTCG Admin", 
                                "content" : "{user} requested an image change for [{waifuid}] {name} to <{image}>!\nUse `!godimage check {cardid}` in any chat to check it.".format(**discordArgs)
                            }
                            threading.Thread(target=sendAdminDiscordAlert, args=(discordbody,)).start()

                            self.message(channel, "Your request has been placed. You will be notified when bot staff accept or decline it.", isWhisper)
                            return
                elif subcmd == "list":
                    with db.cursor() as cur:
                        cur.execute("SELECT waifus.id, waifus.name FROM godimage_requests gr JOIN cards ON gr.cardid=cards.id JOIN waifus ON cards.waifuid = waifus.id WHERE gr.requesterid = %s AND gr.state = 'pending'", [tags['user-id']])
                        reqs = cur.fetchall()

                        if len(reqs) == 0:
                            self.message(channel, "You don't have any pending god image change requests.", isWhisper)
                        else:
                            reqList = ", ".join(["[%d] %s" % (req[0], req[1]) for req in reqs])
                            self.message(channel, "%s, you have pending image change requests for: %s." % (tags['display-name'], reqList), isWhisper)
                        
                        return
                elif subcmd == "cancel":
                    if len(args) < 2:
                        self.message(channel, "Usage: !godimage cancel <id>", isWhisper)
                        return
                    hand = getHand(tags['user-id'])
                    try:
                        card = parseHandCardSpecifier(hand, args[1], godRarity)
                    except CardNotInHandException:
                        self.message(channel, "%s, you don't own that waifu/card or it isn't god rarity!" % tags['display-name'],
                                     isWhisper)
                        return
                    except AmbiguousWaifuException:
                        self.message(channel,
                                     "%s, you own more than one god card of waifu %s! Please specify a card ID instead. You can find card IDs using !checkhand" % (
                                         tags['display-name'], args[1]), isWhisper)
                        return
                    except ValueError:
                        self.message(channel, "Only whole numbers/IDs please.", isWhisper)
                        return
                    with db.cursor() as cur:
                        cur.execute("UPDATE godimage_requests SET state = 'cancelled', updated = %s WHERE cardid = %s AND state = 'pending'", [current_milli_time(), card['cardid']])
                        if cur.rowcount > 0:
                            # send discord notif
                            discordArgs = {"user": tags['display-name'], "id": card['waifuid'], "name": card["name"]}
                            discordbody = {
                                "username": "WTCG Admin", 
                                "content" : "{user} cancelled their image change request for [{id}] {name}.".format(**discordArgs)
                            }
                            threading.Thread(target=sendAdminDiscordAlert, args=(discordbody,)).start()
                            self.message(channel, "You cancelled your image change request for [%d] %s." % (card['waifuid'], card["name"]), isWhisper)
                        else:
                            self.message(channel, "You didn't have a pending image change request for that waifu.", isWhisper)
                        return
                elif subcmd == "queue" and canManageImages:
                    with db.cursor() as cur:
                        cur.execute("SELECT cardid FROM godimage_requests WHERE state = 'pending' ORDER BY created ASC")
                        queue = cur.fetchall()
                        if len(queue) == 0:
                            self.message(channel, "The request queue is currently empty.", isWhisper)
                        else:
                            queueStr = ", ".join(str(item[0]) for item in queue)
                            self.message(channel, "Current requested IDs for image changes: %s. !godimage check <id> to see each request." % queueStr, isWhisper)
                        return
                elif canManageImages and subcmd in ["check", "acceptsingle", "accept", "reject"]:
                    if len(args) < 2:
                        self.message(channel, "Usage: !godimage %s <card id>" % subcmd, isWhisper)
                        return
                    try:
                        cardid = int(args[1])
                    except ValueError:
                        self.message(channel, "Usage: !godimage %s <card id>" % subcmd, isWhisper)
                        return
                    with db.cursor() as cur:
                        cur.execute("SELECT gr.id, gr.image, users.id, users.name, waifus.id, waifus.name, cards.id FROM godimage_requests gr"
                        + " JOIN cards ON gr.cardid = cards.id"
                        + " JOIN users ON gr.requesterid = users.id"
                        + " JOIN waifus ON cards.waifuid = waifus.id"
                        + " WHERE gr.cardid = %s AND gr.state = 'pending'", [cardid])
                        request = cur.fetchone()
                        if request is None:
                            self.message(channel, "There is no pending request for that card.", isWhisper)
                            return

                        if subcmd == "check":
                            msgArgs = {"user": request[3], "waifuid": request[4], "name": request[5], "image": request[1], "cardid": request[6]}
                            self.message(channel, ("{user} requested their copy of [{waifuid}] {name}'s image to be changed to {image} ." + 
                            " You can accept this request with !godimage accept {cardid}" +
                            " or deny it with !godimage reject {cardid} <reason>.").format(**msgArgs), isWhisper)
                        elif subcmd == "reject":
                            if len(args) < 3:
                                self.message(channel, "You must provide a reason to reject the request. If it is porn/illegal/etc, just ban the user.", isWhisper)
                                return
                            rejectionReason = " ".join(args[2:])
                            queryArgs = [tags['user-id'], current_milli_time(), rejectionReason, request[0]]
                            cur.execute("UPDATE godimage_requests SET state = 'rejected', moderatorid = %s, updated = %s, rejection_reason = %s WHERE id = %s", queryArgs)

                            # notify them
                            self.message("#%s" % request[3], "Your image change request for [%d] %s was rejected with the following reason: %s" % (request[4], request[5], rejectionReason), True)

                            self.message(channel, "Request rejected and user notified.", isWhisper)
                        else:
                            # update it
                            try:
                                hostedURL = processWaifuURL(request[1])
                            except Exception as ex:
                                self.message(channel, "Could not process image. %s. Check the URL yourself and if it is invalid reject their request." % str(ex), isWhisper)
                                return
                            updateCard(request[6], {"customImage": hostedURL})

                            queryArgs = [tags['user-id'], current_milli_time(), request[0]]
                            cur.execute("UPDATE godimage_requests SET state = 'accepted', moderatorid = %s, updated = %s WHERE id = %s", queryArgs)

                            # notify them
                            self.message("#%s" % request[3], "Your image change request for your copy of [%d] %s was accepted." % (request[4], request[5]), True)

                            self.message(channel, "Request accepted. The new image for %s's copy of [%d] %s is %s" % (request[3], request[4], request[5], hostedURL), isWhisper)
                        return
            if command == "sendpoints":
                # expire old points transfers
                with db.cursor(pymysql.cursors.DictCursor) as cur:
                    cur.execute("UPDATE points_transfers SET status = 'expired' WHERE status = 'pending' AND created <= %s", [current_milli_time() - int(config["pointsTransferExpiryMinutes"])*60000])
                    if len(args) < 2:
                        self.message(channel, "Usage: !sendpoints <user> <amount> <reason> OR !sendpoints confirm <code>", isWhisper)
                        return
                    
                    if args[0].lower() == "confirm":
                        code = args[1]
                        
                        cur.execute("SELECT pt.*, users.name AS toName FROM points_transfers AS pt JOIN users ON pt.toid = users.id WHERE pt.fromid = %s ORDER BY pt.id DESC LIMIT 1", [tags['user-id']])
                        transfer = cur.fetchone()

                        if transfer is None or transfer["status"] in ["confirmed", "confirm_failed"]:
                            self.message(channel, "%s, you have no pending points transfer." % tags['display-name'], isWhisper)
                            return
                        
                        if transfer["status"] == "expired":
                            self.message(channel, "%s, your points transfer has expired. Please try sending a new one." % tags['display-name'], isWhisper)
                            return

                        if "%s-%d" % (transfer["toName"], transfer["paid"]) != code:
                            cur.execute("UPDATE points_transfers SET status='confirm_failed' WHERE id = %s", [transfer["id"]])
                            self.message(channel, "%s, the confirmation code you entered was wrong. Please try sending a new points transfer." % tags['display-name'], isWhisper)
                            return
                        
                        if not hasPoints(tags['user-id'], transfer["paid"]):
                            cur.execute("UPDATE points_transfers SET status='confirm_failed' WHERE id = %s", [transfer["id"]])
                            self.message(channel, "%s, you no longer have enough points to complete this transfer. Please try sending a new points transfer." % tags['display-name'], isWhisper)
                            return

                        addPoints(tags['user-id'], -transfer["paid"])
                        addPoints(transfer["toid"], transfer["sent"])

                        if int(transfer["sent"]) >= int(config["pointsTransferMinWhisperAmount"]):
                            self.message("#%s" % transfer["toName"], "%s sent you %d points with the reason: %s" % (tags["display-name"], transfer["sent"], transfer["reason"]), True)
                        
                        cur.execute("UPDATE points_transfers SET status='confirmed', confirmed=%s WHERE id = %s", [current_milli_time(), transfer["id"]])

                        self.message(channel, "%s, you successfully paid %d points to send %d points to %s." % (tags["display-name"], transfer["paid"], transfer["sent"], transfer["toName"]))
                    else:
                        if len(args) < 3:
                            self.message(channel, "Usage: !sendpoints <user> <amount> <reason>", isWhisper)
                            return

                        otherparty = args[0].lower()

                        cur.execute("SELECT id FROM users WHERE name = %s", [otherparty])
                        otheridrow = cur.fetchone()
                        if otheridrow is None:
                            self.message(channel, "I don't recognize that username.", isWhisper)
                            return
                        otherid = int(otheridrow["id"])

                        if otherid == int(tags['user-id']):
                            self.message(channel, "You can't send points to yourself.", isWhisper)
                            return

                        try:
                            amount = int(args[1])
                        except ValueError:
                            self.message(channel, "Invalid amount of points entered.", isWhisper)
                            return

                        if amount <= 0 or amount > int(config["pointsTransferMaxAmount"]):
                            self.message(channel, "Invalid amount of points entered.", isWhisper)
                            return

                        reason = " ".join(args[2:])

                        if len(reason) < 10:
                            self.message(channel, "Your reason for the transfer must be at least 10 characters long.")
                            return

                        toPay = amount * 2

                        if not hasPoints(tags['user-id'], toPay):
                            self.message(channel, "You don't have enough points to send %d points. You need %d." % (amount, toPay), isWhisper)
                            return

                        insertArgs = [tags['user-id'], otherid, amount, toPay, current_milli_time(), reason]
                        cur.execute("INSERT INTO points_transfers (fromid, toid, sent, paid, status, created, reason) VALUES(%s, %s, %s, %s, 'pending', %s, %s)", insertArgs)

                        msgArgs = (tags["display-name"], amount, otherparty, toPay, otherparty, toPay)
                        self.message(channel, "%s, you want to send %d points to %s. This will cost you %d points. To confirm this action, enter !sendpoints confirm %s-%d" % msgArgs, isWhisper)
                    return
            if command == "sorthand":
                if len(args) == 0 or (args[0].lower() != "reset" and len(args) < 2):
                    self.message(channel, "Usage: !sorthand <comma-delimited card ids> <comma-delimited positions> OR !sorthand reset", isWhisper)
                    return
                with db.cursor() as cur:
                    if args[0].lower() == "reset":
                        cur.execute("UPDATE cards SET sortValue = NULL WHERE userid = %s", [tags['user-id']])
                        self.message(channel, "Successfully reset %s's hand to default sort order." % tags['display-name'], isWhisper)
                    else:
                        if args[0].count(",") != args[1].count(","):
                            self.message(channel, "You must provide an equal amount of waifu/card IDs and sort values.")
                            return
                        
                        cardSpecifiers = args[0].split(",")
                        sortValueStrings = args[1].split(",")
                        seenIDs = []
                        updatePairs = []
                        hand = getHand(tags['user-id'])

                        for i in range(len(cardSpecifiers)):
                            try:
                                card = parseHandCardSpecifier(hand, cardSpecifiers[i])
                                if card['cardid'] in seenIDs:
                                    self.message(channel, "You entered the same card twice!", isWhisper)
                                    return
                                seenIDs.append(card['cardid'])
                                sortValue = int(sortValueStrings[i])
                                if sortValue < -32768 or sortValue > 32767:
                                    self.message(channel, "Invalid sort value entered. Valid values are -32768 to 32767 inclusive.", isWhisper)
                                    return
                                updatePairs.append([sortValue, card['cardid']])
                            except CardNotInHandException:
                                self.message(channel, "%s, you don't own that waifu/card!" % tags['display-name'],
                                            isWhisper)
                                return
                            except AmbiguousWaifuException:
                                self.message(channel,
                                            "%s, you own more than one rarity of waifu %s! Please specify a card ID instead. You can find card IDs using !checkhand" % (
                                                tags['display-name'], cardSpecifiers[i]), isWhisper)
                                return
                            except ValueError:
                                self.message(channel, "Only whole numbers/IDs please.", isWhisper)
                                return
                        cur.executemany("UPDATE cards SET sortValue = %s WHERE id = %s", updatePairs)
                        self.message(channel, "Updated sort values for %d cards in %s's hand." % (len(updatePairs), tags['display-name']), isWhisper)
                    return
            if command == "autogacha" and sender in superadmins:
                tokenName = config["eventTokenName"]
                with db.cursor() as cur:
                    cur.execute("SELECT id, name, eventTokens FROM users WHERE eventTokens > 0 ORDER BY eventTokens DESC")
                    holders = cur.fetchall()

                    for holder in holders:
                        fullPrizes = []
                        userid = int(holder[0])
                        for i in range(int(holder[2])):
                            roll = tokenGachaRoll()
                            prizes = []

                            if "pack" in roll["prize"]:
                                giveFreeBooster(userid, roll["prize"]["pack"], roll["prize"]["amount"])
                                prizes.append("%dx %s pack (!freepacks open %s)" % (roll["prize"]["amount"], roll["prize"]["pack"], roll["prize"]["pack"]))

                            if "points" in roll["prize"]:
                                addPoints(userid, roll["prize"]["points"])
                                prizes.append("%d points" % roll["prize"]["points"])
                            
                            if "pudding" in roll["prize"]:
                                addPudding(userid, roll["prize"]["pudding"])
                                prizes.append("%d pudding" % roll["prize"]["pudding"])

                            fullPrizes.append("[%d◆] %s" % (roll["tier"], " and ".join(prizes)))

                        messages = ["Your %d leftover %s(s) were fed into the Token Gacha and you got: " % (holder[2], tokenName)]
                        first = True
                        for prizeStr in fullPrizes:
                            if len(messages[-1]) + len(prizeStr) > 398:
                                messages.append(prizeStr)
                            elif first:
                                messages[-1] += prizeStr
                            else:
                                messages[-1] += ", " + prizeStr
                            first = False

                        for message in messages:
                            self.message('#' + holder[1], message, True)

                    cur.execute("UPDATE users SET eventTokens = 0")
                    self.message(channel, "Done.", isWhisper)
                    return
            if command == "tokenshop":
                if not booleanConfig("annivShopOpen"):
                    return

                promoCost = int(config["annivPromoBaseCost"])
                maxPromos = int(config["annivMaxPromos"])
                specialCost = int(config["annivSpecialCost"])
                huCost = int(config["annivHandUpgradeCost"])
                
                with db.cursor() as cur:
                    cur.execute("SELECT annivPromosBought, annivSpecialBought, annivHandUpgradeBought, eventTokens FROM users WHERE id = %s", [tags['user-id']])
                    purchaseData = cur.fetchone()
                    subcmd = "" if not len(args) else args[0].lower()
                    if booleanConfig("annivShopSpecialOnly") and subcmd not in ["special", "specialadmin"]:
                        self.message(channel, "Token Shop purchases are closed for this year, thanks for playing! If you've already purchased a Special and don't have it in your hand yet, you still have access to !tokenshop special commands.", isWhisper)
                        return
                    if subcmd == "gacha":
                        tokenName = config["eventTokenName"]
                        if len(args) == 1 or args[1].lower() != "roll":
                            self.message(channel, "!tokenshop gacha roll to try your luck on the %s Gacha. 1 %s per go." % (tokenName, tokenName), isWhisper)
                            return
                        # check the user's tokens
                        cur.execute("SELECT eventTokens FROM users WHERE id = %s", [tags['user-id']])
                        tokens = cur.fetchone()[0] or 0

                        if tokens < 1:
                            self.message(channel, "You don't have any %ss to roll the Gacha with." % (tokenName), isWhisper)
                            return
                        cur.execute("UPDATE users SET eventTokens = eventTokens - 1 WHERE id = %s", [tags['user-id']])
                        roll = tokenGachaRoll()
                        prizes = []

                        if "pack" in roll["prize"]:
                            giveFreeBooster(tags['user-id'], roll["prize"]["pack"], roll["prize"]["amount"])
                            prizes.append("%dx %s pack (!freepacks open %s)" % (roll["prize"]["amount"], roll["prize"]["pack"], roll["prize"]["pack"]))

                        if "points" in roll["prize"]:
                            addPoints(tags['user-id'], roll["prize"]["points"])
                            prizes.append("%d points" % roll["prize"]["points"])
                        
                        if "pudding" in roll["prize"]:
                            addPudding(tags['user-id'], roll["prize"]["pudding"])
                            prizes.append("%d pudding" % roll["prize"]["pudding"])

                        prizeStr = " and ".join(prizes)

                        self.message(channel, "%s, you roll the %s Gacha and you get: [%d◆] %s" % (tags['display-name'], tokenName, roll["tier"], prizeStr), isWhisper)
                        return
                    if subcmd == "buy":
                        if len(args) < 2:
                            self.message(channel, "Usage: !tokenshop buy special | !tokenshop buy handupgrade | !tokenshop buy <promo waifu id>", isWhisper)
                            return
                        
                        target = args[1].lower()

                        if target == "special":
                            if purchaseData[1]:
                                self.message(channel, "%s, you've already bought a special card! Pick another reward." % tags['display-name'], isWhisper)
                                return
                            
                            if purchaseData[3] < specialCost:
                                msgArgs = (tags['display-name'], specialCost, purchaseData[3])
                                self.message(channel, "%s, you can't afford a special card! They cost %d tokens and you only have %d." % msgArgs, isWhisper)
                                return
                            
                            # ok, make the purchase

                            cur.execute("UPDATE users SET eventTokens = eventTokens - %s, annivSpecialBought = 1 WHERE id = %s", [specialCost, tags['user-id']])
                            cur.execute("INSERT INTO special_requests (requesterid, state, created) VALUES(%s, 'notsent', %s)", [tags['user-id'], current_milli_time()])
                            self.message(channel, "%s, you bought a special card for %d tokens! You now have access to !tokenshop special which you can use to set the name/series/image of your special before submitting it for review." % (tags['display-name'], specialCost), isWhisper)

                            discordbody = {
                                "username": "WTCG Admin", 
                                "content" : "%s just bought a Special from the token shop." % tags['display-name']
                            }
                            threading.Thread(target=sendAdminDiscordAlert, args=(discordbody,)).start()
                        elif target == "handupgrade":
                            if purchaseData[2]:
                                self.message(channel, "%s, you've already bought a hand upgrade! Pick another reward." % tags['display-name'], isWhisper)
                                return
                            
                            if purchaseData[3] < huCost:
                                msgArgs = (tags['display-name'], huCost, purchaseData[3])
                                self.message(channel, "%s, you can't afford a hand upgrade! They cost %d tokens and you only have %d." % msgArgs, isWhisper)
                                return
                            
                            # ok, make the purchase
                            cur.execute("UPDATE users SET eventTokens = eventTokens - %s, annivHandUpgradeBought = 1, freeUpgrades = freeUpgrades + 1 WHERE id = %s", [huCost, tags['user-id']])
                            self.message(channel, "%s, you bought a hand upgrade for %d tokens! You can begin using it immediately." % (tags['display-name'], huCost), isWhisper)
                        else:
                            # promo
                            if purchaseData[0] >= maxPromos:
                                self.message(channel, "%s, you have already bought the maximum of %d promos. Pick another reward." % (tags['display-name'], maxPromos), isWhisper)
                                return
                            
                            promoPrice = promoCost * (purchaseData[0] + 1)

                            if purchaseData[3] < promoPrice:
                                msgArgs = (tags['display-name'], promoPrice, purchaseData[3])
                                self.message(channel, "%s, you can't afford a promo! Your current cost for one is %d tokens and you only have %d." % msgArgs, isWhisper)
                                return

                            promoCard = getWaifuById(target)

                            if promoCard is None:
                                self.message(channel, "%s, you entered an invalid waifu ID." % tags['display-name'], isWhisper)
                                return
                            
                            if not promoCard["can_purchase"]:
                                self.message(channel, "%s, that waifu cannot be bought. !tokenshop listpromos to get a list of buyable promos." % tags['display-name'], isWhisper)
                                return
                            
                            addCard(tags['user-id'], promoCard['id'], 'other', None, int(config["numNormalRarities"]) + int(config["numSpecialRarities"]) - 1)
                            cur.execute("UPDATE users SET eventTokens = eventTokens - %s, annivPromosBought = annivPromosBought + 1 WHERE id = %s", [promoPrice, tags['user-id']])
                            if purchaseData[0] + 1 < maxPromos:
                                msgArgs = (tags['display-name'], promoCard['id'], promoCard['name'], promoPrice, purchaseData[0] + 1, promoPrice + promoCost)
                                self.message(channel, "%s, you bought a copy of [%d] %s for %d tokens. You have now bought %d promos so your next one will cost %d tokens." % msgArgs, isWhisper)
                            else:
                                msgArgs = (tags['display-name'], promoCard['id'], promoCard['name'], promoPrice, purchaseData[0] + 1)
                                self.message(channel, "%s, you bought a copy of [%d] %s for %d tokens. You have now bought %d promos so you cannot buy any more." % msgArgs, isWhisper)
                    elif subcmd == "listpromos":
                        self.message(channel, "%s, you can view the list of promos buyable with tokens here - %s" % (tags['display-name'], config["tokenPromoList"]), isWhisper)
                    elif subcmd == "specialadmin" and sender in superadmins:
                        spclcmd = "" if len(args) < 2 else args[1].lower()
                        if spclcmd in ["check", "accept", "reject"]:
                            if len(args) < 3:
                                self.message(channel, "Usage: !tokenshop specialadmin check/accept/reject <username>")
                                return
                            cur.execute("SELECT sr.requesterid, sr.name, sr.series, sr.image FROM special_requests sr JOIN users ON sr.requesterid=users.id WHERE users.name = %s AND sr.state = 'pending'", [args[2]])
                            requestInfo = cur.fetchone()

                            if requestInfo is None:
                                self.message(channel, "That user doesn't have a pending Special card request.", isWhisper)
                                return
                            
                            if spclcmd == "check":
                                msgArgs = (args[2], requestInfo[1], requestInfo[2], requestInfo[3], args[2], args[2])
                                self.message(channel, "%s requested a Special card of %s from %s with image %s . Accept it with !tokenshop specialadmin accept %s or reject it with !tokenshop specialadmin reject %s <reason>." % msgArgs, isWhisper)
                            elif spclcmd == "accept":
                                # create waifu
                                try:
                                    hostedURL = processWaifuURL(requestInfo[3])
                                except Exception as ex:
                                    self.message(channel, "Could not process image. %s. Check the URL yourself and if it is invalid reject their request." % str(ex), isWhisper)
                                    return
                                queryArgs = [requestInfo[1], hostedURL, int(config["numNormalRarities"]), requestInfo[2]]
                                cur.execute("INSERT INTO waifus (name, image, base_rarity, series) VALUES(%s, %s, %s, %s)", queryArgs)
                                waifuid = cur.lastrowid

                                addCard(requestInfo[0], waifuid, 'other')
                                cur.execute("UPDATE special_requests SET state = 'accepted', moderatorid = %s, updated = %s WHERE requesterid = %s", [tags['user-id'], current_milli_time(), requestInfo[0]])
                                self.message(channel, "Request accepted. Created Special card ID %d and gave it to %s." % (waifuid, args[2]), isWhisper)
                                self.message('#%s' % args[2].lower(), "Your Special card request for %s from %s was accepted." % (requestInfo[1], requestInfo[2]), True)
                            else:
                                # reject
                                if len(args) < 4:
                                    self.message(channel, "Rejection reason is required.", isWhisper)
                                    return
                                
                                rejectionReason = " ".join(args[3:])
                                cur.execute("UPDATE special_requests SET state = 'rejected', moderatorid = %s, updated = %s, rejection_reason = %s, changed_since_rejection = 0 WHERE requesterid = %s", [tags['user-id'], current_milli_time(), rejectionReason, requestInfo[0]])
                                self.message('#%s' % args[2].lower(), "Your Special card request for %s from %s was rejected for the following reason: %s. Please adjust it with !tokenshop special and then resubmit." % (requestInfo[1], requestInfo[2], rejectionReason), True)
                                self.message(channel, "Request rejected and user notified.", isWhisper)
                        else:
                            # list pending
                            cur.execute("SELECT users.name FROM special_requests sr JOIN users ON sr.requesterid=users.id WHERE sr.state='pending' ORDER BY sr.updated ASC")
                            results = cur.fetchall()

                            if not results:
                                self.message(channel, "There are no pending Special card requests right now.", isWhisper)
                            else:
                                self.message(channel, "Special card requests have been received from: %s. Use !tokenshop specialadmin check <username> for details." % (", ".join([row[0] for row in results])), isWhisper)
                    elif subcmd == "special":
                        if not purchaseData[1]:
                            self.message(channel, "%s, you haven't bought a Special card with tokens yet!" % tags['display-name'], isWhisper)
                            return
                        
                        spclcmd = "" if len(args) < 2 else args[1].lower()

                        cur.execute("SELECT name, series, image, state, updated, rejection_reason, changed_since_rejection FROM special_requests WHERE requesterid = %s", [tags['user-id']])
                        specialData = cur.fetchone()

                        if specialData is None:
                            cur.execute("INSERT INTO special_requests (requesterid, state, created) VALUES(%s, 'notsent', %s)", [tags['user-id'], current_milli_time()])
                            specialData = [None, None, None, 'notsent', None, None, 0]
                        
                        if spclcmd in ["name", "series", "image", "submit", "confirm"] and specialData[3] not in["notsent", "rejected"]:
                            if specialData[3] == "pending":
                                self.message(channel, "%s, you have already sent in your Special card request! Please wait for a response." % tags['display-name'], isWhisper)
                            else:
                                self.message(channel, "%s, your Special card request has already been accepted!" % tags['display-name'], isWhisper)
                            return
                        
                        if spclcmd in ["submit", "confirm"] and specialData[3] == "rejected" and not specialData[6]:
                            self.message(channel, "%s, you haven't changed your request at all since its rejection! Please review the rejection reason (%s) and make adjustments before resubmitting." % (tags['display-name'], specialData[5]), isWhisper)
                            return

                        if spclcmd in ["submit", "confirm"] and (specialData[0] is None or specialData[1] is None or specialData[2] is None):
                            self.message(channel, "%s, you haven't finished filling in the info for your Special card request yet! Please specify all 3 of name/series/image before continuing." % tags['display-name'], isWhisper)
                            return

                        if spclcmd in ["name", "series", "image"] and len(args) < 3:
                            self.message(channel, "Usage: !tokenshop special name <name> / series <series> / image <image URL> / submit")
                            return
                        
                        if spclcmd == "name":
                            name = (" ".join(args[2:])).strip()
                            if not len(name) or len(name) > 50:
                                self.message(channel, "Please enter a waifu name of between 1 and 50 characters.", isWhisper)
                                return
                            
                            queryArgs = [name, current_milli_time(), tags['user-id']]
                            cur.execute("UPDATE special_requests SET name = %s, changed_since_rejection = 1, updated = %s WHERE requesterid = %s", queryArgs)
                            self.message(channel, "%s -> Set the name for your Special card request to %s." % (tags['display-name'], name), isWhisper)
                        elif spclcmd == "series":
                            series = (" ".join(args[2:])).strip()
                            if not len(series) or len(series) > 50:
                                self.message(channel, "Please enter a waifu series of between 1 and 50 characters.", isWhisper)
                                return
                            
                            queryArgs = [series, current_milli_time(), tags['user-id']]
                            cur.execute("UPDATE special_requests SET series = %s, changed_since_rejection = 1, updated = %s WHERE requesterid = %s", queryArgs)
                            self.message(channel, "%s -> Set the series for your Special card request to %s." % (tags['display-name'], series), isWhisper)
                        elif spclcmd == "image":
                            try:
                                validateWaifuURL(args[2])
                            except ValueError as ex:
                                self.message(channel, "Invalid image link specified. %s" % str(ex), isWhisper)
                                return
                            except Exception:
                                self.message(channel, "There was an unknown problem with the image link you specified. Please try again later.", isWhisper)
                                return
                            
                            queryArgs = [args[2], current_milli_time(), tags['user-id']]
                            cur.execute("UPDATE special_requests SET image = %s, changed_since_rejection = 1, updated = %s WHERE requesterid = %s", queryArgs)
                            self.message(channel, "%s -> Set the image for your Special card request to %s" % (tags['display-name'], args[2]), isWhisper)
                        elif spclcmd == "submit":
                            self.message(channel, "%s, please confirm that you want a Special card of %s from %s with image %s by entering !tokenshop special confirm. You cannot change your submission after it is confirmed so please be careful." % (tags['display-name'], specialData[0], specialData[1], specialData[2]), isWhisper)
                        elif spclcmd == "confirm":
                            # TODO submit to admin discordhook
                            discordargs = (tags['display-name'], specialData[0], specialData[1], specialData[2], sender)
                            discordbody = {
                                "username": "WTCG Admin", 
                                "content" : "%s submitted their request for a Special Card:\nName: %s\nSeries: %s\nImage: %s\nUse `!tokenshop specialadmin check %s` in any chat to check it." % discordargs
                            }
                            threading.Thread(target=sendAdminDiscordAlert, args=(discordbody,)).start()
                            cur.execute("UPDATE special_requests SET state = 'pending' WHERE requesterid = %s", [tags['user-id']])
                            self.message(channel, "%s, your Special card request has been submitted for approval. Keep an eye on your whispers to see if it is accepted or rejected. If it is rejected you'll be able to resubmit after you fix the problems." % tags['display-name'], isWhisper)
                        else:
                            # show status
                            if specialData[3] == "accepted":
                                self.message(channel, "%s, your Special card request of %s from %s was accepted." % (tags['display-name'], specialData[0], specialData[1]), isWhisper)
                            elif specialData[3] == "rejected":
                                if specialData[6]:
                                    self.message(channel, "%s, your previous submission was rejected for the following reason: %s. Your current submission is %s from %s with image %s . !tokenshop special name/image/series to change details or !tokenshop special submit to submit it for approval." % (tags['display-name'], specialData[5], specialData[0], specialData[1], specialData[2]), isWhisper)
                                else:
                                    self.message(channel, "%s, your submission of %s from %s with image %s was rejected for the following reason: %s. Please use !tokenshop special name/image/series to change details to solve the problems and then !tokenshop special submit to resubmit it for approval." % (tags['display-name'], specialData[0], specialData[1], specialData[2], specialData[5]), isWhisper)
                            elif specialData[3] == "pending":
                                self.message(channel, "%s, your Special card request of %s from %s is still pending approval. Please wait patiently." % (tags['display-name'], specialData[0], specialData[1]), isWhisper)
                            else:
                                name = specialData[0] or "<no name yet>"
                                series = specialData[1] or "<no series yet>"
                                image = specialData[2] or "<no image yet>"
                                if specialData[0] and specialData[1] and specialData[2]:
                                    self.message(channel, "%s, your current Special request is %s from %s with image %s . Use !tokenshop special name/image/series to change details or !tokenshop special submit to submit it for approval." % (tags['display-name'], name, series, image), isWhisper)
                                elif specialData[0] or specialData[1] or specialData[2]:
                                    self.message(channel, "%s, your current Special request is %s from %s with image %s . Use !tokenshop special name/image/series to add the missing details and then !tokenshop special submit to submit it for approval." % (tags['display-name'], name, series, image), isWhisper)
                                else:
                                    self.message(channel, "%s, you haven't started on your Special request yet. Use !tokenshop special name/image/series to add the required details and then !tokenshop special submit to submit it for approval." % tags['display-name'], isWhisper)
                    else:
                        # list items
                        purchasable = []

                        if purchaseData[0] < maxPromos:
                            promoPrice = promoCost * (purchaseData[0] + 1)
                            purchasable.append("Promo card - current cost %d tokens (%d already bought, %d max) - !tokenshop buy <waifu id> (!tokenshop listpromos for a list)" % (promoPrice, purchaseData[0], maxPromos))
                        
                        if not purchaseData[1]:
                            purchasable.append("Special card - cost %d tokens - !tokenshop buy special" % specialCost)
                        
                        if not purchaseData[2]:
                            purchasable.append("Hand upgrade - cost %d tokens - !tokenshop buy handupgrade" % huCost)

                        purchasable.append("Gacha roll - costs 1 token - !tokenshop gacha roll")

                        self.message(channel, "%s, you have %d Anniversary Tokens. Items currently available to you: %s" % (tags['display-name'], purchaseData[3], " / ".join(purchasable)), isWhisper)
                return



                            

            
                    
class MarathonBot(pydle.Client):
    instance = None
    pw=None

    def __init__(self):
        super().__init__(config["marathonChannel"][1:])
        MarathonBot.instance = self
        self.ffz = MarathonFFZWebsocket(config["marathonChannel"][1:])

    def start(self, password):
        pool.connect(self, "irc.twitch.tv", 6667, tls=False, password=password)
        self.pw = password
        logger.info("Connecting MarathonBot...")

    def on_disconnect(self, expected):
        logger.warning("MarathonBot Disconnected, reconnecting....")
        pool.connect(self, "irc.twitch.tv", 6667, tls=False, password=self.pw, reconnect=True)

    def on_connect(self):
        super().on_connect()
        logger.info("MarathonBot Joining")

    def on_message(self, source, target, message):
        logger.debug("message on MarathonBot: %s, %s, %s", str(source), str(target), message)
        
    def updateFollowButtons(self, channels):
        if self.ffz is None:
            self.ffz = MarathonFFZWebsocket(config["marathonChannel"][1:], channels)
        else:
            self.ffz.updateFollowButtons(channels)
        
    def message(self, *args):
        logger.info("MarathonBot Sending "+str(args))
        super().message(*args)
        
class MarathonFFZWebsocket:
    def __init__(self, channelName, newFollowButtons=None):
        self.channelName = channelName
        self.messageNumber = 0
        self.queuedChanges = []
        self.initDone = False
        if newFollowButtons is not None:
            self.queuedChanges.append(newFollowButtons)
        self.ws = websocket.WebSocketApp(ffzws, on_message = self.on_message, on_error = self.on_error, on_close = self.on_close)
        self.ws.on_open = self.on_open
        thread.start_new_thread(self.ws.run_forever, (), {"origin": ""})
        
        
    def sendMessage(self, message):
        self.messageNumber += 1
        self.ws.send("%d %s" % (self.messageNumber, message))
        
    def on_open(self):
        self.sendMessage('hello ["waifutcg-ffzclient",false]')
    
    def on_message(self, message):
        logger.debug("Websocket recv: "+message)
        code, msg = message.split(" ", 1)
        code = int(code)
        if code == -1:
            # probably authorize
            if msg.startswith("do_authorize"):
                # must send auth code
                authCode = json.loads(msg[13:])
                logger.debug("trying to authenticate with FFZ "+authCode)
                MarathonBot.instance.message("#frankerfacezauthorizer", "AUTH "+authCode)
        elif code == self.messageNumber and self.messageNumber < 5 and msg.split(" ")[0] == "ok":
            # send the rest of the intro
            if self.messageNumber == 1:
                self.sendMessage('setuser %s' % json.dumps(self.channelName))
            elif self.messageNumber == 2:
                self.sendMessage('sub %s' % json.dumps('room.'+self.channelName))
            elif self.messageNumber == 3:
                self.sendMessage('sub %s' % json.dumps('channel.'+self.channelName))
            else:
                self.sendMessage('ready 0')
        elif code >= 5 and self.messageNumber >= 5 and len(self.queuedChanges) > 0:
            self.initDone = True
            self.updateFollowButtons(self.queuedChanges[0])
            self.queuedChanges = self.queuedChanges[1:]
        elif code >= 5 and self.messageNumber >= 5 and msg.split(" ")[0] == "ok":
            self.initDone = True
        else:
            # don't do anything immediately
            pass
            
    def on_error(self, error):
        logger.debug("WS Error: "+error)
        self.ws.close()
        
    def on_close(self):
        logger.debug("Websocket closed")
        MarathonBot.instance.ffz = None
        
    def updateFollowButtons(self, channels):
        if not self.initDone:
            self.queuedChanges.append(channels)
        else:
            self.sendMessage("update_follow_buttons %s" % json.dumps([self.channelName, channels]))   


curg = db.cursor()
logger.info("Fetching channel list...")
curg.execute("SELECT name FROM channels")
channels = []
for row in curg.fetchall():
    channels.append("#" + row[0])
logger.debug("Channels: %s", str(channels))
curg.close()

loadConfig()

# twitch api init
checkAndRenewAppAccessToken()

# get user data for the bot itself
headers = {"Authorization": "Bearer %s" % config["appAccessToken"], "Client-ID": config["clientID"]}
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

# marathon bot?
if booleanConfig("marathonBotFunctions"):
    maraBot = MarathonBot()
    maraBot.start(config["marathonOAuth"])

logger.debug("past start")

pool.handle_forever()

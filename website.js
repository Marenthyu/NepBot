'use strict';

let http = require('http');
let mysql = require('mysql');
let url = require('url');
let fs = require('fs');
let request = require('request');
let async = require('async');
let ejs = require('ejs');
let moment = require('moment');

let download = function (uri, filename, callback) {
    request.head(uri, function (err, res, body) {
        console.log('content-type:', res.headers['content-type']);
        console.log('content-length:', res.headers['content-length']);

        request(uri).pipe(fs.createWriteStream(filename)).on('close', callback);
    });
};

let cfgfile = fs.readFileSync('nepbot.cfg', 'utf8');
let cfglines = cfgfile.match(/[^\r\n]+/g);
let dbpw = null;
let dbname = null;
let dbuser = null;
let dbhost = null;
let isLocalMode = false;
let config = {};
for (let line of cfglines) {
    let lineparts = line.split("=");
    if (lineparts[0] === "dbpassword") {
        dbpw = lineparts[1];
    } else if (lineparts[0] === "database") {
        dbname = lineparts[1];
    } else if (lineparts[0] === "dbuser") {
        dbuser = lineparts[1];
    } else if (lineparts[0] === "dbhost") {
        dbhost = lineparts[1];
    } else if (lineparts[0] === "local") {
        isLocalMode = true;
    }
}

if (!isLocalMode) {
    if (dbpw === null || dbname === null || dbuser === null || dbhost === null) {
        process.exit(1);
        return;
    }
}

let con;

if (!isLocalMode) {
    con = mysql.createConnection({
        host: dbhost,
        user: dbuser,
        password: dbpw,
        database: dbname,
        charset: "utf8mb4",
    });

    con.connect(function (err) {
        if (err) throw err;
        console.log("Connected!");
    });
}
let bootstrapwaifucss = fs.readFileSync('waifus-bootstrap.css', 'utf8');
let jsdata = {};
let jsfiles = fs.readdirSync("js/");
jsfiles.forEach(function (filename) {
    jsdata[filename] = fs.readFileSync('js/' + filename, 'utf8');
});

function booleanConfig(key) {
    return key in config && !["off", "no", "false"].includes(config[key].trim().toLowerCase());
}

function renderTemplateAndEnd(filename, vars, res) {
    vars["moment"] = moment;
    vars["config"] = config;
    vars["booleanConfig"] = booleanConfig;
    ejs.renderFile(filename, vars, {}, function (err, str) {
        if (err) { throw err; }
        res.write(str);
        res.end();
    })
}

function httpError(res, code, status, body) {
    res.writeHead(code, status);
    if (body) {
        res.write(body);
    }
    res.end();
}

let rarities = {
    0: "common",
    1: "uncommon",
    2: "rare",
    3: "super",
    4: "ultra",
    5: "legendary",
    6: "mythical",
    7: "god",
    8: "special",
    9: "promo"
};

function getRarityName(number) {
    if (number in rarities) {
        return rarities[number];
    } else {
        return "unknown";
    }
}

function hand(req, res, query) {
    if (!('user' in query)) {
        httpError(res, 400, "Missing Parameter");
        return;
    }

    con.query("SELECT waifus.*, c1.rarity, c1.customImage, c1.id as cardid, c1.tradeableAt, c1.created, " +
        "IF(c1.rarity = 7 AND NOT EXISTS(SELECT id FROM cards c2 WHERE c2.userid IS NOT NULL AND c2.rarity = 7 AND c2.waifuid = c1.waifuid AND (c2.created < c1.created OR (c2.created=c1.created AND c2.id < c1.id))), 1, 0) AS firstGod FROM waifus JOIN cards c1 ON waifus.id = c1.waifuid JOIN users ON " +
        "c1.userid = users.id WHERE users.name = ? AND c1.boosterid IS NULL ORDER BY COALESCE(c1.sortValue, 32000) ASC, (c1.rarity < 8) DESC, waifus.id ASC, c1.rarity ASC, c1.id ASC", query.user, function (err, result) {
            if (err) throw err;
            let wantJSON = false;
            if ("accept" in req.headers && req.headers["accept"] === "application/json") {
                wantJSON = true;
            }
            if (result.length === 0) {
                if (wantJSON) {
                    res.writeHead(404, "Not Found", { 'Content-Type': 'application/json; charset=utf-8' });
                    res.write(JSON.stringify({ "error": { "status": 404, "explanation": "User not found" } }));
                    res.end();
                } else {
                    res.writeHead(404, "Not Found", { 'Content-Type': 'text/html' });
                    renderTemplateAndEnd("templates/hand.ejs", { user: query.user, cards: [], error: "404 - This user doesn't exist.", eventTokens: 0 }, res);
                }
                return;
            }
            con.query("SELECT eventTokens FROM users WHERE users.name = ?", query.user, function (err, resultTokens) {
                if (resultTokens.length === 0) {
                    if (wantJSON) {
                        res.writeHead(404, "Not Found", { 'Content-Type': 'application/json; charset=utf-8' });
                        res.write(JSON.stringify({ "error": { "status": 404, "explanation": "User not found" } }))
                        res.end();
                    } else {
                        res.writeHead(404, "Not Found", { 'Content-Type': 'text/html' });
                        renderTemplateAndEnd("templates/hand.ejs", { user: query.user, cards: [], error: "404 - This user doesn't exist.", eventTokens: 0 }, res);
                    }
                    return;
                }
                if (wantJSON) {
                    let sanitizedResult = [];
                    for (let row of result) {
                        let obj = {
                            "id": row.id,
                            "Name": row.name,
                            "series": row.series,
                            "image": row.customImage || row.image,
                            "base_rarity": row.base_rarity,
                            "rarity": row.rarity,
                            "amount": 1,
                            "cardid": row.cardid,
                            "firstGod": row.firstGod,
                        };
                        sanitizedResult.push(obj);
                    }
                    res.writeHead(200, { 'Content-Type': 'application/json' });
                    res.write(JSON.stringify({
                        'user': query.user,
                        "cards": sanitizedResult,
                        "eventTokens": resultTokens[0].eventTokens
                    }));
                    res.end();
                } else {
                    res.writeHead(200, { 'Content-Type': 'text/html' });
                    renderTemplateAndEnd("templates/hand.ejs", { user: query.user, cards: result, error: "", eventTokens: resultTokens[0].eventTokens }, res);
                }
            });
        });
}

function booster(req, res, query) {
    if (!('user' in query)) {
        httpError(res, 400, "Missing Parameter");
        return;
    }
    let start = Date.now();
    con.query("SELECT waifus.* FROM boosters_opened JOIN users ON boosters_opened.userid = users.id LEFT JOIN cards ON boosters_opened.id = cards.boosterid LEFT JOIN waifus ON cards.waifuid = waifus.id WHERE users.name = ? AND boosters_opened.status = 'open' ORDER BY waifus.id ASC", query.user, function (err, result) {
        if (err) throw err;
        let wantJSON = false;
        if ("accept" in req.headers && req.headers["accept"] === "application/json") {
            wantJSON = true;
        }
        if (result.length === 0) {
            if (wantJSON) {
                res.writeHead(404, "Not Found", { 'Content-Type': 'application/json; charset=utf-8' });
                res.write(JSON.stringify({
                    "error": {
                        "status": 404,
                        "explanation": "User not found or does not have an open booster."
                    }
                }));
                res.end();
            } else {
                res.writeHead(404, "Not Found", { 'Content-Type': 'text/html' });
                renderTemplateAndEnd("templates/booster.ejs", { user: query.user, cards: [], error: "404 - This user doesn't exist or has no open booster.", eventTokens: 0 }, res);
            }
            return;
        }
        con.query("SELECT boosters_opened.eventTokens FROM boosters_opened JOIN users ON boosters_opened.userid = users.id WHERE users.name = ? AND boosters_opened.status = 'open'", query.user, function (err, resultTokens) {
            if (resultTokens.length === 0) {
                if (wantJSON) {
                    res.writeHead(404, "Not Found", { 'Content-Type': 'application/json; charset=utf-8' });
                    res.write(JSON.stringify({
                        "error": {
                            "status": 404,
                            "explanation": "User not found or does not have an open booster."
                        }
                    }));
                    res.end();
                } else {
                    res.writeHead(404, "Not Found", { 'Content-Type': 'text/html' });
                    renderTemplateAndEnd("templates/booster.ejs", { user: query.user, cards: [], error: "404 - This user doesn't exist or has no open booster.", eventTokens: 0 }, res);
                }
                return;
            }
            if (wantJSON) {
                res.writeHead(200, { 'Content-Type': 'application/json' });
                let sanitizedResult = [];
                for (let row of result) {
                    let obj = {};
                    obj.id = row.id;
                    obj.name = row.name;
                    obj.image = row.image;
                    obj.rarity = row.base_rarity;
                    obj.series = row.series;
                    sanitizedResult.push(obj);
                }
                res.write(JSON.stringify({
                    "user": query.user,
                    "cards": sanitizedResult,
                    "eventTokens": resultTokens[0].eventTokens
                }));
                res.end();
            } else {
                res.writeHead(200, { 'Content-Type': 'text/html' });
                renderTemplateAndEnd("templates/booster.ejs", { user: query.user, cards: result, error: "", eventTokens: resultTokens[0].eventTokens }, res);
            }
        });
    });
}

function pullfeed(req, res, query) {
    con.query("SELECT cards.rarity, cards.source, boosters_opened.boostername, boosters_opened.channel, cards.created, waifus.id AS waifuID, waifus.name as waifuName, waifus.series AS waifuSeries, waifus.image AS waifuImage, users.name AS username " +
        "FROM cards JOIN waifus ON cards.waifuid = waifus.id JOIN users ON cards.originalOwner = users.id LEFT JOIN boosters_opened ON cards.originalBooster=boosters_opened.id WHERE cards.rarity >= 4 AND cards.source IN('booster', 'freebie', 'buy') ORDER BY cards.id DESC LIMIT 100", function (err, result) {
            if (err) throw err;
            if ("accept" in req.headers && req.headers["accept"] === "application/json") {
                let jsonresp = [];
                res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
                for (let row of result) {
                    let obj = {};
                    obj["timestamp"] = row.created;
                    obj["user"] = row.username;
                    obj["waifu"] = {
                        "name": row.waifuName,
                        "id": row.waifuID,
                        "series": row.waifuSeries,
                        "image": row.waifuImage,
                        "rarity": row.rarity
                    };
                    obj["source"] = row.source;
                    obj["channel"] = row.channel;
                    jsonresp.push(obj);
                }
                res.write(JSON.stringify(jsonresp));
                res.end();
            } else {
                res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
                renderTemplateAndEnd("templates/pullfeed.ejs", { items: result }, res);
            }
        });

}

function teaser(req, res, query) {
    res.writeHead(200, "naroHodo");
    renderTemplateAndEnd("templates/teaser.ejs", {}, res);
}

function api(req, res, query) {
    // Authentication
    let key = req.headers["x-waifus-api-key"];
    if (!key) {
        httpError(res, 401, "Unauthorized");
        return
    }
    con.query("SELECT * FROM api_keys WHERE `value` = ?", key, function (err, result) {
        if (err) throw err;
        if (result.length === 0) {
            httpError(res, 401, "Unauthorized");
            return
        }
        if (!('type' in query)) {
            httpError(res, 400, "Missing Parameter");
            return;
        }
        if (query.type === 'wars') {
            con.query("SELECT * FROM bidWars JOIN bidWarChoices ON bidWars.id = bidWarChoices.warID WHERE bidWars.status = 'open' ORDER BY bidWars.id ASC, bidWarChoices.amount DESC, RAND() ASC", function (err, result) {
                if (err) throw err;
                let wars = [];
                let lastwarid = '';
                let lastwar = null;
                for (let row of result) {
                    if (row.id !== lastwarid) {
                        lastwarid = row.id;
                        lastwar = {
                            "id": row.id,
                            "title": row.title,
                            "status": row.status,
                            "openEntry": row.openEntry !== 0,
                            "openEntryMinimum": row.openEntryMinimum,
                            "openEntryMaxLength": row.openEntryMaxLength,
                            "choices": []
                        };
                        wars.push(lastwar);
                    }
                    lastwar.choices.push({
                        "choice": row.choice,
                        "amount": row.amount,
                        "created": row.created,
                        "creator": row.creator,
                        "lastVote": row.lastVote,
                        "lastVoter": row.lastVoter
                    })
                }
                res.writeHead(200, { 'Content-Type': 'text/json' });
                res.write(JSON.stringify(wars));
                res.end();
            });
        } else if (query.type === 'incentives') {
            con.query("SELECT id, title, status, IF(id='BonusGame', amount * 10000000, amount) AS amount, IF(id='BonusGame', required*10000000, required) AS required FROM incentives WHERE incentives.status = 'open' AND incentives.amount < incentives.required ORDER BY incentives.id ASC", function (err, result) {
                if (err) throw err;
                res.writeHead(200, { 'Content-Type': 'text/json' });
                res.write(JSON.stringify(result));
                res.end();
            });
        } else if (query.type === 'emotewar') {
            con.query("SELECT * FROM emoteWar ORDER BY count DESC", function (err, result) {
                if (err) throw err;
                res.writeHead(200, { 'Content-Type': 'text/json' });
                res.write(JSON.stringify(result));
                res.end();
            });
        } else if (query.type === 'tracker') {
            // All-in-one endpoint for the tracker
            con.query("SELECT * FROM bidWars LEFT JOIN bidWarChoices ON bidWars.id = bidWarChoices.warID WHERE bidWars.status = 'open' ORDER BY bidWars.id ASC, bidWarChoices.amount DESC, RAND() ASC", function (err, result) {
                if (err) throw err;
                let wars = [];
                let lastwarid = '';
                let lastwar = null;
                for (let row of result) {
                    if (row.id !== lastwarid) {
                        lastwarid = row.id;
                        lastwar = {
                            "id": row.id,
                            "title": row.title,
                            "status": row.status,
                            "openEntry": row.openEntry !== 0,
                            "openEntryMinimum": row.openEntryMinimum,
                            "openEntryMaxLength": row.openEntryMaxLength,
                            "choices": []
                        };
                        wars.push(lastwar);
                    }
                    if (row.choice !== null) {
                        lastwar.choices.push({
                            "choice": row.choice,
                            "amount": row.amount,
                            "created": row.created,
                            "creator": row.creator,
                            "lastVote": row.lastVote,
                            "lastVoter": row.lastVoter
                        })
                    }
                }
                con.query("SELECT id, title, status, IF(id='BonusGame', amount * 10000000, amount) AS amount, IF(id='BonusGame', required*10000000, required) AS required FROM incentives WHERE incentives.status = 'open' AND incentives.amount < incentives.required ORDER BY incentives.id ASC", function (err, result2) {
                    if (err) throw err;
                    con.query("SELECT * FROM cpuwar ORDER BY votes DESC", function (err, result3) {
                        if (err) throw err;
                        res.writeHead(200, { 'Content-Type': 'text/json' });
                        res.write(JSON.stringify({ "wars": wars, "incentives": result2, "cpuwar": result3 }));
                        res.end();
                    });
                });
            });
        } else {
            res.writeHead(400, "Bad Request");
            res.end();

        }
    });
}

function profile(req, res, query) {
    if (!('user' in query)) {
        httpError(res, 400, "Missing Parameter");
        return;
    }
    con.query("SELECT profileDescription, favourite, id, spending, paidHandUpgrades FROM users WHERE users.name = ?", query.user, function (err, resultOuter) {
        if (err) throw err;
        if (resultOuter.length === 0) {
            res.writeHead(404, "User Not Found", { 'Content-Type': 'text/html' });
            renderTemplateAndEnd("templates/profile.ejs", { user: query.user, error: "404 - User not found." }, res);
            return;
        }
        let userID = resultOuter[0].id;
        let spending = resultOuter[0].spending;
        let paidSlots = resultOuter[0].paidHandUpgrades;
        con.query("SELECT badges.name, badges.description, badges.image FROM has_badges JOIN badges ON has_badges.badgeID =" +
            " badges.id JOIN users ON has_badges.userID = users.id WHERE users.id = ?", userID, function (err, result) {
                if (err) throw err;
                res.writeHead(200, { 'Content-Type': 'text/html' });
                con.query("SELECT waifus.id, waifus.name, waifus.image, waifus.base_rarity, waifus.series, c1.rarity, c1.customImage, IF(c1.rarity = 7 AND NOT EXISTS(SELECT id FROM cards c2 WHERE c2.userid IS NOT NULL AND c2.rarity = 7 AND c2.waifuid = c1.waifuid AND (c2.created < c1.created OR (c2.created=c1.created AND c2.id < c1.id))), 1, 0) AS firstGod FROM waifus LEFT JOIN cards c1 ON (c1.waifuid = waifus.id AND c1.userid = ? AND c1.boosterid IS NULL) WHERE waifus.id = ? ORDER BY c1.rarity DESC LIMIT 1", [userID, resultOuter[0].favourite], function (err, resultInner) {
                    if (err) throw err;

                    let row = resultInner[0];

                    row.amount = 1;
                    row.rarity = (row.rarity && row.rarity > row.base_rarity) ? row.rarity : row.base_rarity;
                    con.query("SELECT slot, spendings FROM handupgrades", function (err, huLUTresult) {
                        if (err) throw err;

                        let nextspendings = 0;
                        let lastspendings = 0;

                        if (paidSlots + 1 < huLUTresult.length) {
                            nextspendings = huLUTresult[paidSlots + 1].spendings;
                            lastspendings = huLUTresult[paidSlots].spendings;
                        } else {
                            nextspendings = huLUTresult[huLUTresult.length - 1].spendings + (1000000 * (paidSlots - (huLUTresult.length - 1) + 1));
                            lastspendings = huLUTresult[huLUTresult.length - 1].spendings + (1000000 * (paidSlots - (huLUTresult.length - 1)));
                        }
                        let percentspendings = Math.max(((spending - lastspendings) / (nextspendings - lastspendings)) * 100, 0);

                        let vars = {
                            badges: result,
                            description: resultOuter[0].profileDescription,
                            favourite: row,
                            nextspendings: nextspendings,
                            lastspendings: lastspendings,
                            currentspendings: spending,
                            percentspendings: percentspendings,
                            user: query.user,
                            error: "",
                        };

                        renderTemplateAndEnd("templates/profile.ejs", vars, res);
                    });
                });
            });
    });

}

function sets(req, res, query) {
    let user = "";
    if ('user' in query) {
        user = query.user;
    }
    res.writeHead(200, { 'Content-Type': 'text/html' });
    renderTemplateAndEnd("templates/sets.ejs", { user: user }, res);
}

function setsdata(req, res, query) {
    let hasUser = 'user' in query && query.user;

    if (!('type' in query)) {
        httpError(res, 400, "Missing Parameter", "Missing Parameter");
        return;
    }

    if (query.type === 'progress' || query.type === 'claimed') {
        if (!hasUser) {
            httpError(res, 400, "Missing Parameter", "Missing Parameter");
            return;
        }
    }
    else if (query.type !== 'allsets') {
        if (!('q' in query)) {
            httpError(res, 400, "Missing Parameter", "Missing Parameter");
            return;
        }
        if (query.q.length < 3) {
            httpError(res, 400, "Bad Request", "Query string too short");
            return;
        }
    }

    let sets_query = "";
    let parameters = "";

    if (query.type === 'claimed') {
        sets_query = "SELECT s.id, s.name, s.claimable, s.firstClaimer, s.lastClaimTime, uf.name AS firstClaimerName, s.rewardPoints, s.rewardPudding, scl.rewardPoints as pointsRecvd, scl.rewardPudding as puddingRecvd, scl.timestamp, b.image, (SELECT COUNT(*) FROM sets_claimed scc WHERE s.id=scc.setid) AS numClaims";
        sets_query += " FROM sets AS s JOIN sets_claimed AS scl ON s.id = scl.setid JOIN users AS uc ON scl.userid=uc.id LEFT JOIN users AS uf ON s.firstClaimer = uf.id"
        sets_query += " LEFT JOIN badges AS b ON s.badgeid=b.id";
        sets_query += " WHERE uc.name = ?";
        parameters = query.user;
    }
    else if (['waifuname', 'waifuseries', 'progress', 'allsets'].includes(query.type)) {
        sets_query = "SELECT DISTINCT s.id, s.name, s.claimable, s.firstClaimer, s.lastClaimTime, uf.name AS firstClaimerName, s.rewardPoints, s.rewardPudding, b.image, (SELECT COUNT(*) FROM sets_claimed scc WHERE s.id=scc.setid) AS numClaims";
        sets_query += " FROM sets AS s";
        if (query.type === 'waifuname' || query.type === 'waifuseries') {
            sets_query += " JOIN set_cards AS sca ON s.id = sca.setID JOIN waifus AS w ON sca.cardID=w.id";
        }
        if (query.type === 'progress') {
            sets_query += " JOIN set_cards AS sca ON s.id = sca.setID JOIN cards AS c ON (sca.cardID = c.waifuid AND c.boosterid IS NULL) JOIN users AS up ON c.userid = up.id";
        }
        sets_query += " LEFT JOIN users AS uf ON s.firstClaimer = uf.id";
        sets_query += " LEFT JOIN badges AS b ON s.badgeid=b.id";
        sets_query += " WHERE s.claimable = 1";
        parameters = [];
        if (query.type === 'waifuname') {
            sets_query += " AND w.name LIKE ?";
            parameters.push("%" + query.q + "%");
        }
        if (query.type === 'waifuseries') {
            sets_query += " AND w.series LIKE ?";
            parameters.push("%" + query.q + "%");
        }
        if (query.type === 'progress') {
            sets_query += " AND up.name = ?";
            parameters.push(query.user);
        }
        if (hasUser) {
            sets_query += " AND NOT EXISTS(SELECT * FROM sets_claimed AS scl JOIN users AS u ON scl.userid = u.id WHERE u.name = ? AND scl.setid = s.id)";
            parameters.push(query.user);
        }
    }
    else {
        httpError(res, 400, "Bad Request", "Bad Request");
        return;
    }

    con.query(sets_query, parameters, function (err, result) {
        if (err) throw err;
        let setsById = {};
        let setIDs = [];
        let checkOwnership = hasUser && query.type !== 'claimed';

        for (let row of result) {
            let set = {};
            set["image"] = row.image || null;
            set["id"] = row.id;
            set["name"] = row.name;
            set["rewardPoints"] = row.rewardPoints;
            set["rewardPudding"] = row.rewardPudding;
            set["totalCards"] = 0;
            set["cardsOwned"] = 0;
            set["cards"] = [];
            set["row"] = row;
            set["claimable"] = row.claimable;
            setsById[row.id] = set;
            setIDs.push(row.id);
        }

        if (setIDs.length > 0) {
            let baseQuery = "";
            let params2 = [];
            if (checkOwnership) {
                baseQuery = "SELECT setID, a.name AS userName, waifus.id AS waifuID, waifus.name AS waifuName, waifus.base_rarity AS waifuRarity, waifus.image AS waifuImage, waifus.series AS waifuSeries FROM set_cards LEFT JOIN (SELECT DISTINCT cards.waifuid, users.name FROM cards JOIN users ON cards.userid = users.id WHERE users.name = ? AND cards.boosterid IS NULL) AS a ON set_cards.cardID = a.waifuid JOIN waifus ON set_cards.cardID = waifus.id";
                params2 = [query.user].concat(setIDs);
            }
            else {
                baseQuery = "SELECT setID, NULL as userName, waifus.id AS waifuID, waifus.name AS waifuName, waifus.base_rarity AS waifuRarity, waifus.image AS waifuImage, waifus.series AS waifuSeries FROM set_cards JOIN waifus ON set_cards.cardID = waifus.id";
                params2 = setIDs;
            }
            let inBinds = "?, ".repeat(setIDs.length).substring(0, setIDs.length * 3 - 2);

            con.query(baseQuery + " WHERE set_cards.setID IN(" + inBinds + ") ORDER BY waifuID", params2,
                function (err, result2) {
                    if (err) throw err;
                    for (let row of result2) {
                        let waifu = {
                            id: row.waifuID,
                            name: row.waifuName,
                            rarity: getRarityName(row.waifuRarity),
                            image: row.waifuImage,
                            series: row.waifuSeries,
                            owned: row.userName !== null
                        };
                        setsById[row.setID].cards.push(waifu);
                        setsById[row.setID].totalCards += 1;
                        if (waifu.owned) {
                            setsById[row.setID].cardsOwned += 1;
                        }
                    }
                    // Build actual response
                    res.writeHead(200, { 'Content-Type': 'text/json' });
                    let response = { count: 0, sets: [] };
                    for (let setID in setsById) {
                        let set = setsById[setID];
                        let row = set.row;
                        delete set.row;
                        // Populate display strings in the data
                        if (query.type === 'claimed') {
                            set["claimedText"] = "Claimed " + moment(new Date(row.timestamp)).fromNow();
                            if (row.pointsRecvd || row.puddingRecvd) {
                                set["claimedText"] += " and received ";
                                if (row.pointsRecvd && row.puddingRecvd) {
                                    set["claimedText"] += row.pointsRecvd + " points and " + row.puddingRecvd + " pudding";
                                }
                                else if (row.pointsRecvd) {
                                    set["claimedText"] += row.pointsRecvd + " points";
                                }
                                else {
                                    set["claimedText"] += row.puddingRecvd + " pudding";
                                }
                            }
                        }
                        if (!row.claimable) {
                            set["claimableIcon"] = "cancel";
                            set["claimableText"] = "Not currently claimable";
                        }
                        else if (row.lastClaimTime && new Date(row.lastClaimTime + config.setCooldownDays * 86400000) > Date.now()) {
                            set["claimableIcon"] = "watch_later";
                            set["claimableText"] = "On cooldown, claimable " + moment(new Date(row.lastClaimTime + config.setCooldownDays * 86400000)).fromNow();
                        }
                        else {
                            set["claimableIcon"] = (checkOwnership && set.cardsOwned == set.totalCards) ? "done_all" : "done";
                            set["claimableText"] = "Currently claimable";
                        }
                        if (row.claimable && checkOwnership) {
                            if (set.cardsOwned == set.totalCards) {
                                set["claimableText"] += " (all cards obtained)";
                            }
                            else {
                                set["claimableText"] += " (" + (set.totalCards - set.cardsOwned) + " cards missing)";
                            }
                        }
                        if (row.numClaims == 0) {
                            set["numClaimsText"] = "No claims yet";
                        }
                        else if (row.numClaims == 1) {
                            set["numClaimsText"] = "One claim by " + row.firstClaimerName;
                        }
                        else {
                            set["numClaimsText"] = row.numClaims + " claims, first claimer was " + row.firstClaimerName;
                        }
                        response.count += 1;
                        response.sets.push(set);
                    }
                    response.sets.sort(
                        function (a, b) {
                            let aValue = a.cardsOwned === 0 ? 9999999 : a.totalCards - a.cardsOwned;
                            let bValue = b.cardsOwned === 0 ? 9999999 : b.totalCards - b.cardsOwned;
                            return (aValue !== bValue) ? aValue - bValue : a.name.localeCompare(b.name);
                        });
                    res.write(JSON.stringify(response, null, 2));
                    res.end();

                })
        } else {
            res.writeHead(200, { 'Content-Type': 'text/json' });
            res.write(JSON.stringify({ count: 0, sets: [] }));
            res.end();

        }
    });
}

function packTracker(req, res, query) {
    let user = "";
    if ('user' in query) {
        user = query.user;
    }

    con.query("SELECT (SELECT COUNT(*) FROM boosters_opened WHERE boostername = ?)+(SELECT COUNT(*) FROM boosters_opened WHERE boostername = ?)*5 AS cnt", [config.packTrackerPack, "mega" + config.packTrackerPack], function (err, result) {
        if (err) throw err;
        let goals = JSON.parse(config.packTrackerGoals);
        let packCount = result[0].cnt;
        let highestGoal = goals[goals.length - 1];
        let vars = {
            user: user,
            goals: goals,
            packCount: packCount,
            highestGoal: highestGoal,
            currentAmountCapped: Math.min(highestGoal, packCount),
            currentWidth: 100 * Math.min(highestGoal, packCount) / highestGoal
        };
        res.writeHead(200, { 'Content-Type': 'text/html' });
        renderTemplateAndEnd("templates/packtracker.ejs", vars, res);
    });
}

function tracker(req, res, query) {
    let user = "";
    if ('user' in query) {
        user = query.user;
    }
    con.query("SELECT * FROM bidWars LEFT JOIN bidWarChoices ON bidWars.id = bidWarChoices.warID WHERE bidWars.status != 'hidden' ORDER BY bidWars.id ASC, bidWarChoices.amount DESC, RAND() ASC", function (err, result) {
        if (err) throw err;
        let wars = [];
        let lastwarid = '';
        let lastwar = null;
        for (let row of result) {
            if (row.id !== lastwarid) {
                lastwarid = row.id;
                lastwar = {
                    "id": row.id,
                    "title": row.title,
                    "status": row.status,
                    "openEntry": row.openEntry !== 0,
                    "openEntryMinimum": row.openEntryMinimum,
                    "openEntryMaxLength": row.openEntryMaxLength,
                    "choices": []
                };
                wars.push(lastwar);
            }
            if (row.choice !== null) {
                lastwar.choices.push({
                    "choice": row.choice,
                    "amount": row.amount,
                    "created": row.created,
                    "creator": row.creator,
                    "lastVote": row.lastVote,
                    "lastVoter": row.lastVoter
                })
            }
        }
        for (let war of wars) {
            let warTotal = 0;
            for (let choice of war['choices']) {
                warTotal += choice['amount'];
            }
            war.total = warTotal;
        }
        con.query("SELECT id, title, status, IF(id='BonusGame', amount * 10000000, amount) AS amount, IF(id='BonusGame', required*10000000, required) AS required FROM incentives WHERE incentives.status != 'hidden' ORDER BY incentives.id ASC", function (err, result2) {
            let incentives = result2;
            if (err) {
                res.writeHead(500, 'Internal Server Error');
                res.end("Something went wrong. Blame maren.");
                throw err;
            }

            res.writeHead(200, { 'Content-Type': 'text/html' });
            renderTemplateAndEnd("templates/tracker.ejs", { user: user, wars: wars, incentives: incentives }, res);
        });
    });
}

function browser(res, req, query) {
    let page = 0;
    if ("page" in query) {
        page = query.page;
    }
    let auth = req.headers["authorization"];
    if (!auth) {
        res.setHeader("WWW-Authenticate", "Basic realm=\"Waifu TCG Admin\", charset=\"UTF-8\"");
        httpError(res, 401, "Unauthorized");
        return
    }
    let buff = new Buffer(auth, 'base64');
    let authText = buff.toString('UTF-8');
    let user, pass = authText.split(':');
    if (config['adminPass'] === pass) {
        con.query("SELECT waifus.* FROM waifus LIMIT ?, 100", [(page * 100)], function (error, result) {
            if (err) throw err;
            renderTemplateAndEnd("templates/image-browser.ejs", {user: user, page: page, cards: result})
        });

    } else {
        res.setHeader("WWW-Authenticate", "Basic realm=\"Waifu TCG Admin\", charset=\"UTF-8\"");
        httpError(res, 401, "Unauthorized");
        return
    }

}

function readConfig(callback) {
    config = {};
    if (!isLocalMode) {
        con.query("SELECT * FROM config", function (err, result) {
            for (let row of result) {
                config[row.name] = row.value;
            }
            callback();
        });
    } else {
        callback();
    }
}

function bootServer(callback) {
    http.createServer(function (req, res) {
        let q = url.parse(req.url, true);
        try {
            if (q.pathname.startsWith("/js/") && jsdata[q.pathname.substring(4)]) {
                res.writeHead(200, { 'Content-Type': 'text/javascript' });
                res.write(jsdata[q.pathname.substring(4)]);
                res.end();
                return;
            }
            switch (q.pathname.substring(1)) {
                case "hand": {
                    hand(req, res, q.query);
                    break;
                }
                case "booster": {
                    booster(req, res, q.query);
                    break;
                }
                case "sets": {
                    sets(req, res, q.query);
                    break;
                }
                case "setsdata": {
                    setsdata(req, res, q.query);
                    break;
                }
                case "help": {
                    res.writeHead(302, { 'Location': config.helpDocURL });
                    res.end();
                    break;
                }
                case "fixes": {
                    res.writeHead(302, { 'Location': 'https://goo.gl/forms/ymaZWjM6ZyGl2DXj2' });
                    res.end();
                    break;
                }
                case "waifus-bootstrap.css": {
                    res.writeHead(200, { 'Content-Type': 'text/css' });
                    res.write(bootstrapwaifucss);
                    res.end();
                    break;
                }
                case "discord": {
                    res.writeHead(302, { 'Location': 'https://discord.gg/4KVy32j' });
                    res.end();
                    break;
                }
                case "teasing": {
                    teaser(req, res, q.query);
                    break;
                }
                case "api": {
                    api(req, res, q.query);
                    break;
                }
                case "profile": {
                    profile(req, res, q.query);
                    break;
                }
                case "pullfeed": {
                    pullfeed(req, res, q.query);
                    break;
                }
                case "tracker": {
                    if (booleanConfig("marathonTrackerEnabled")) {
                        tracker(req, res, q.query);
                        break;
                    }
                    else {
                        res.writeHead(404, { 'Content-Type': 'text/html' });
                        res.write("The Specified content could not be found on this Server. If you want to know more about the Waifu TCG Bot, head over to https://waifus.de/help");
                        res.end();
                    }
                    break;
                }
                case "packs": {
                    if (booleanConfig("packTrackerEnabled")) {
                        packTracker(req, res, q.query);
                    }
                    else {
                        res.writeHead(404, { 'Content-Type': 'text/html' });
                        res.write("The Specified content could not be found on this Server. If you want to know more about the Waifu TCG Bot, head over to https://waifus.de/help");
                        res.end();
                    }
                    break;
                }
                case "rules": {
                    res.writeHead(200, "OK", { 'Content-Type': 'text/html' });
                    renderTemplateAndEnd("templates/rules.ejs", { nepdoc: config['nepdocURL'], currentPage: "rules", user: q.query.user }, res);
                    break;
                }
                case "browser": {
                    browser(req, res, q.query);
                    break;
                }
                default: {
                    res.writeHead(404, { 'Content-Type': 'text/html' });
                    res.write("The Specified content could not be found on this Server. If you want to know more about the Waifu TCG Bot, head over to https://waifus.de/help");
                    res.end();
                }
            }
        } catch (err) {
            res.writeHead(500, "Internal Server Error", { "Content-Type": "text/html" });
            res.write("Something went wrong. Sorry. Blame Marenthyu! Tell him this: " + err.toString());
            res.end();
            console.log(err.toString());
        }


    }).listen(8088);
}

async.series([readConfig, bootServer]);

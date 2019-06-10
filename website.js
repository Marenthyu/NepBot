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
    }
}

if (dbpw === null || dbname === null || dbuser === null || dbhost === null) {
    process.exit(1);
    return;
}

let con = mysql.createConnection({
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

let bootstrapwaifucss = fs.readFileSync('waifus-bootstrap.css', 'utf8');
let jsdata = {};
let jsfiles = fs.readdirSync("js/");
jsfiles.forEach(function(filename) {
    jsdata[filename] = fs.readFileSync('js/'+filename, 'utf8');
});

function renderTemplateAndEnd(filename, vars, res) {
    vars["moment"] = moment;
    ejs.renderFile(filename, vars, {}, function(err, str) {
        if(err) { throw err; }
        res.write(str);
        res.end();
    })
}

function hand(req, res, query) {
    if (!('user' in query)) {
        res.writeHead(400, "Missing Parameter");
        res.end();
        return;
    }

    con.query("SELECT waifus.*, cards.rarity, cards.customImage, cards.id as cardid, cards.tradeableAt FROM waifus JOIN cards ON waifus.id = cards.waifuid JOIN users ON " +
        "cards.userid = users.id WHERE users.name = ? AND cards.boosterid IS NULL ORDER BY (cards.rarity < 8) DESC, waifus.id ASC, cards.rarity ASC", query.user, function (err, result) {
        if (err) throw err;
        let wantJSON = false;
        if ("accept" in req.headers && req.headers["accept"] === "application/json") {
            wantJSON = true;
        }
        if (result.length === 0) {
            if (wantJSON) {
                res.writeHead(404, "Not Found", {'Content-Type': 'application/json; charset=utf-8'});
                res.write(JSON.stringify({"error": {"status": 404, "explanation": "User not found"}}));
                res.end();
            } else {
                res.writeHead(404, "Not Found", {'Content-Type': 'text/html'});
                renderTemplateAndEnd("templates/hand.ejs", {user: query.user, cards: [], error: "404 - This user doesn't exist."}, res);
            }
            return;
        }
        con.query("SELECT eventTokens FROM users WHERE users.name = ?", query.user, function (err, resultTokens) {
            if (resultTokens.length === 0) {
                if (wantJSON) {
                    res.writeHead(404, "Not Found", {'Content-Type': 'application/json; charset=utf-8'});
                    res.write(JSON.stringify({"error": {"status": 404, "explanation": "User not found"}}))
                    res.end();
                } else {
                    res.writeHead(404, "Not Found", {'Content-Type': 'text/html'});
                    renderTemplateAndEnd("templates/hand.ejs", {user: query.user, cards: [], error: "404 - This user doesn't exist."}, res);
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
                    };
                    sanitizedResult.push(obj);
                }
                res.writeHead(200, {'Content-Type': 'application/json'});
                res.write(JSON.stringify({
                    'user': query.user,
                    "cards": sanitizedResult,
                    "eventTokens": resultTokens[0].eventTokens
                }));
                res.end();
            } else {
                // @TODO event tokens
                res.writeHead(200, {'Content-Type': 'text/html'});
                renderTemplateAndEnd("templates/hand.ejs", {user: query.user, cards: result, error: ""}, res);
            }
        });
    });
}

function booster(req, res, query) {
    if (!('user' in query)) {
        res.writeHead(400, "Missing Parameter");
        res.end();
        return;
    }

    con.query("SELECT waifus.* FROM boosters_opened JOIN users ON boosters_opened.userid = users.id LEFT JOIN cards ON boosters_opened.id = cards.boosterid JOIN waifus ON cards.waifuid = waifus.id WHERE users.name = ? AND boosters_opened.status = 'open' ORDER BY waifus.id ASC", query.user, function (err, result) {
        if (err) throw err;
        let wantJSON = false;
        if ("accept" in req.headers && req.headers["accept"] === "application/json") {
            wantJSON = true;
        }
        if (result.length === 0) {
            if (wantJSON) {
                res.writeHead(404, "Not Found", {'Content-Type': 'application/json; charset=utf-8'});
                res.write(JSON.stringify({
                    "error": {
                        "status": 404,
                        "explanation": "User not found or does not have an open booster."
                    }
                }));
                res.end();
            } else {
                res.writeHead(404, "Not Found", {'Content-Type': 'text/html'});
                renderTemplateAndEnd("templates/booster.ejs", {user: query.user, cards: [], error: "404 - This user doesn't exist or has no open booster."}, res);
            }            
            return;
        }
        con.query("SELECT boosters_opened.eventTokens FROM boosters_opened JOIN users ON boosters_opened.userid = users.id WHERE users.name = ? AND boosters_opened.status = 'open'", query.user, function (err, resultTokens) {
            if (resultTokens.length === 0) {
                if (wantJSON) {
                    res.writeHead(404, "Not Found", {'Content-Type': 'application/json; charset=utf-8'});
                    res.write(JSON.stringify({
                        "error": {
                            "status": 404,
                            "explanation": "User not found or does not have an open booster."
                        }
                    }));
                    res.end();
                } else {
                    res.writeHead(404, "Not Found", {'Content-Type': 'text/html'});
                    renderTemplateAndEnd("templates/booster.ejs", {user: query.user, cards: [], error: "404 - This user doesn't exist or has no open booster."}, res);
                }            
                return;
            }
            if (wantJSON) {
                res.writeHead(200, {'Content-Type': 'application/json'});
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
                res.writeHead(200, {'Content-Type': 'text/html'});
                // @TODO event tokens
                renderTemplateAndEnd("templates/booster.ejs", {user: query.user, cards: result, error: ""}, res);
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
            res.writeHead(200, {'Content-Type': 'application/json; charset=utf-8'});
            for(let row of result) {
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
            res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'});
            renderTemplateAndEnd("templates/pullfeed.ejs", {items: result}, res);
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
        res.writeHead(401, "Unauthorized");
        res.end();
        return
    }
    con.query("SELECT * FROM api_keys WHERE `value` = ?", key, function (err, result) {
        if (err) throw err;
        if (result.length === 0) {
            res.writeHead(401, "Unauthorized");
            res.end();
            return
        }
        if (!('type' in query)) {
            res.writeHead(400, "Missing Parameter");
            res.end();
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
                res.writeHead(200, {'Content-Type': 'text/json'});
                res.write(JSON.stringify(wars));
                res.end();
            });
        } else if (query.type === 'incentives') {
            con.query("SELECT * FROM incentives WHERE incentives.status = 'open' AND incentives.amount < incentives.required ORDER BY incentives.id ASC", function (err, result) {
                if (err) throw err;
                res.writeHead(200, {'Content-Type': 'text/json'});
                res.write(JSON.stringify(result));
                res.end();
            });
        } else if (query.type === 'emotewar') {
            con.query("SELECT * FROM emoteWar ORDER BY count DESC", function (err, result) {
                if (err) throw err;
                res.writeHead(200, {'Content-Type': 'text/json'});
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
                con.query("SELECT * FROM incentives WHERE incentives.status = 'open' AND incentives.amount < incentives.required ORDER BY incentives.id ASC", function (err, result2) {
                    if (err) throw err;
                    con.query("SELECT * FROM emoteWar ORDER BY count DESC", function (err, result3) {
                        if (err) throw err;
                        res.writeHead(200, {'Content-Type': 'text/json'});
                        res.write(JSON.stringify({"wars": wars, "incentives": result2, "emotewar": result3}));
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
        res.writeHead(400, "Missing Parameter");
        res.end();
        return;
    }
    con.query("SELECT profileDescription, favourite, id, spending, paidHandUpgrades FROM users WHERE users.name = ?", query.user, function (err, resultOuter) {
        if (err) throw err;
        if (resultOuter.length === 0) {
            res.writeHead(404, "User Not Found", {'Content-Type': 'text/html'});
            renderTemplateAndEnd("templates/profile.ejs", {user: query.user, error: "404 - User not found."}, res);
            return;
        }
        let userID = resultOuter[0].id;
        let spending = resultOuter[0].spending;
        let paidSlots = resultOuter[0].paidHandUpgrades;
        con.query("SELECT badges.name, badges.description, badges.image FROM has_badges JOIN badges ON has_badges.badgeID =" +
            " badges.id JOIN users ON has_badges.userID = users.id WHERE users.id = ?", userID, function (err, result) {
            if (err) throw err;
            res.writeHead(200, {'Content-Type': 'text/html'});
            con.query("SELECT waifus.id, waifus.name, waifus.image, waifus.base_rarity, waifus.series, cards.rarity, cards.customImage FROM waifus LEFT JOIN cards ON (cards.waifuid = waifus.id AND cards.userid = ? AND cards.boosterid IS NULL) WHERE waifus.id = ? ORDER BY cards.rarity DESC LIMIT 1", [userID, resultOuter[0].favourite], function (err, resultInner) {
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

function readConfig(callback) {
    config = {};
    con.query("SELECT * FROM config", function (err, result) {
        for (let row of result) {
            config[row.name] = row.value;
        }
        callback();
    });
}

function bootServer(callback) {
    http.createServer(function (req, res) {
        let q = url.parse(req.url, true);
        try {
            if(q.pathname.startsWith("/js/") && jsdata[q.pathname.substring(4)]) {
                res.writeHead(200, {'Content-Type': 'text/javascript'});
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
                case "help": {
                    res.writeHead(302, {'Location': 'http://t.fuelr.at/heq'});
                    res.end();
                    break;
                }
                case "fixes": {
                    res.writeHead(302, {'Location': 'https://goo.gl/forms/ymaZWjM6ZyGl2DXj2'});
                    res.end();
                    break;
                }
                case "waifus-bootstrap.css": {
                    res.writeHead(200, {'Content-Type': 'text/css'});
                    res.write(bootstrapwaifucss);
                    res.end();
                    break;
                }
                case "discord": {
                    res.writeHead(302, {'Location': 'https://discord.gg/4KVy32j'});
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
                default: {
                    res.writeHead(404, {'Content-Type': 'text/html'});
                    res.write("The Specified content could not be found on this Server. If you want to know more about the Waifu TCG Bot, head over to https://waifus.de/help");
                    res.end();
                }
            }
        } catch (err) {
            res.writeHead(500, "Internal Server Error", {"Content-Type": "text/html"});
            res.write("Something went wrong. Sorry. Blame Marenthyu! Tell him this: " + err.toString());
            res.end();
            console.log(err.toString());
        }


    }).listen(8088);
}

async.series([readConfig, bootServer]);

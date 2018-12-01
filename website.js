'use strict';

let http = require('http');
let mysql = require('mysql');
let url = require('url');
let fs = require('fs');
let request = require('request');
let async = require('async');

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
    }
    else if (lineparts[0] === "database") {
        dbname = lineparts[1];
    }
    else if (lineparts[0] === "dbuser") {
        dbuser = lineparts[1];
    }
    else if (lineparts[0] === "dbhost") {
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
let pfhead = "<!DOCTYPE html>\n" +
    "<html lang=\"en\">\n" +
    "<head>\n" +
    "    <meta charset=\"UTF-8\">\n" +
    "    <title>Waifu TCG Pull Feed</title>\n" +
    "</head>\n" +
    "<body>\n";
let pffoot = "</body>\n" +
    "</html>";
let sethead = "<!DOCTYPE html>\n" +
    "<html lang=\"en\">\n" +
    "<head>\n" +
    "    <meta charset=\"UTF-8\">\n" +
    "    <title>Waifu TCG Sets</title>\n" +
    "    <script src=\"https://code.jquery.com/jquery-3.2.1.min.js\"></script>\n" +
    "    <script src=\"https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-beta.2/js/bootstrap.bundle.min.js\" crossorigin=\"anonymous\"></script>\n" +
    "    <script src=\"https://cdn.jsdelivr.net/npm/lazyload@2.0.0-beta.2/lazyload.js\"></script>\n" +
    "    <link rel=\"stylesheet\" type=\"text/css\" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-beta.2/css/bootstrap.min.css\"/>\n" +
    "</head>\n" +
    "<body>\n";
let availablehead = "<h1>Available Sets</h1>\n" +
    "<p>";
let claimedhead = "<h1>Claimed Sets</h1>\n" +
    "<p>";
let setbetween = "</p>";
let setfoot = "</body><script type=\"text/javascript\">lazyload()</script>\n" +
    "</html>";
let setbuttonpreids = "<button class=\"btn btn-primary\" type=\"button\" data-toggle=\"collapse\" data-target=\"#set";
let setbuttonbetweenids = "\" aria-expanded=\"false\" aria-controls=\"set";
let setbuttonbetweenidsandname = "\">";
let setbuttonend = "</button> ";
let setsethead = "<div class=\"collapse\" id=\"set";
let setsetbetween = "\">\n" +
    "    <div class=\"card card-body\" style=\"display: inline-block\">";
let setimagehead = "<div class=\"card card-body\" style=\"display: inline-block; background-color: ";
let setimagebetween = "\"><img class=\"lazyload\" data-src=\"";
let setimageafterlink = "\" height=\"300\" /><br/>";
let setimagefoot = "</div>" +
    "";
let setsetfoot = "    </div>\n" +
    "</div>";
let white = "#FFFFFF";

let bootstraphandtpl = fs.readFileSync('bootstraphandtemplate.htm', 'utf8');
let bootstraphandcard = '<div class="card card-tcg card-{RARITY}">' +
    '<div class="card-body card-body-tcg">' +
    '<img src="{IMAGE}" alt="{CARDNAME}" title="{CARDNAME}" class="card-image" />' +
    '<div class="id-holder rarity-{RARITY}">{ID}</div>' +
    '<div class="invisible-space-holder">&nbsp;</div>' +
    '<div class="rarity-holder rarity-{RARITY}">{RARITY}</div>' +
    '{AMOUNTHOLDER}' +
    '{PROMOTEDHOLDER}' +
    '</div>' +
    '<div class="card-footer text-center">' +
    '{CARDNAME}<br />' +
    '{SERIES}' +
    '</div>' +
    '</div>';
let bootstraphandeventtoken = '<div class="card card-tcg">' +
    '<div class="card-body card-body-tcg">' +
    '<img src="{IMAGE}" alt="{CARDNAME}" title="{CARDNAME}" class="card-image" />' +
    '{AMOUNTHOLDER}' +
    '{PROMOTEDHOLDER}' +
    '</div>' +
    '<div class="card-footer text-center">' +
    '<b>{CARDNAME}</b><br />' +
    '</div>' +
    '</div>';
let bootstraphandamtholder = '<div class="amount-holder rarity-{RARITY}">x{AMOUNT}</div>';
let bootstraphandpromoholder = '<div class="promotion-holder rarity-{RARITY}">{STARS}</div>';
let bootstrapboostertpl = fs.readFileSync('bootstrapboostertemplate.htm', 'utf8');
let bootstrapboostercard = '<div class="card card-tcg card-{RARITY}">' +
    '<div class="card-body card-body-tcg">' +
    '<img src="{IMAGE}" alt="{CARDNAME}" title="{CARDNAME}" class="card-image" />' +
    '<div class="id-holder rarity-{RARITY}">{ID}</div>' +
    '<div class="invisible-space-holder">&nbsp;</div>' +
    '<div class="rarity-holder rarity-{RARITY}">{RARITY}</div>' +
    '</div>' +
    '<div class="card-footer text-center">' +
    '<div class="keep-box">' +
    '<input type="checkbox" onchange="update()" /><br />Keep?' +
    '</div>' +
    '<div class="card-info">' +
    '{CARDNAME}<br />' +
    '{SERIES}' +
    '</div>' +
    '</div>' +
    '</div>';
let bootstrapboostereventtoken = '<div class="card card-tcg card-{RARITY}">' +
    '<div class="card-body card-body-tcg">' +
    '<img src="{IMAGE}" alt="{CARDNAME}" title="{CARDNAME}" class="card-image" />' +
    '</div>' +
    '<div class="card-footer text-center">' +
    '<div class="card-info">' +
    '<b>{CARDNAME}</b><br />' +
    '<small>(sent directly to account)</small>' + 
    '</div>' +
    '</div>' +
    '</div>';
let badgetemplate = '' +
    '<div class="badge">' +
    '<img class="badge-img rounded-circle" src="{IMAGE}" title="{CARDNAME}"/><br/>' +
    '<div class="badge-inf">' +
    '{CARDNAME}<br/>' +
    '{DESCRIPTION}' +
    '</div>' +
    '</div>';
let smartsetstpl = fs.readFileSync('smartsets.htm', 'utf8');
let bootstrapwaifucss = fs.readFileSync('waifus-bootstrap.css', 'utf8');
let profiletpl = fs.readFileSync('profiletemplate.html', 'utf8');
let holidaytpl = fs.readFileSync('holidaytemplate.htm', 'utf8');

let entityMap = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
    '/': '&#x2F;',
    '`': '&#x60;',
    '=': '&#x3D;'
};

function escapeHtml(string) {
    return String(string).replace(/[&<>"'`=\/]/g, function (s) {
        return entityMap[s];
    });
}

function getRarityName(number) {
    if (number in rarities) {
        return rarities[number];
    } else {
        return "unknown";
    }
}

function smartsets(req, res, query) {
    let user = "null";
    if ('user' in query) {
        user = query.user;
    }
    let response = smartsetstpl.replace(/{NAME}/g, escapeHtml(user));
    res.writeHead(200, {'Content-Type': 'text/html'});
    res.write(response);
    res.end();

}

function smartsetsdata(req, res, query) {
    let user = "null";
    if ('user' in query) {
        user = query.user;
    }
    if (!('type' in query)) {
        res.writeHead(400, "Missing Parameter");
        res.write("Missing Parameter");
        res.end();
        return;
    }

    if (query.type === 'progress') {
        if (!('user' in query)) {
            res.writeHead(400, "Missing Parameter");
            res.write("Missing Parameter");
            res.end();
            return;
        }
    }
    else if (query.type !== 'allsets') {
        if (!('q' in query)) {
            res.writeHead(400, "Missing Parameter");
            res.write("Missing Parameter");
            res.end();
            return;
        }
        if (query.q.length < 3) {
            res.writeHead(400, "Bad Request");
            res.write("Query string too short");
            res.end();
            return;
        }
    }

    let sets_query = "";
    let parameter = "";
    if (query.type === 'setname') {
        sets_query = "SELECT id, name, reward, rewardPudding FROM sets WHERE name LIKE (?) AND claimed_by IS NULL";
        parameter = "%" + query.q + "%";
    }
    else if (query.type === 'waifuname') {
        sets_query = "SELECT DISTINCT sets.id, sets.name, sets.reward, sets.rewardPudding FROM sets LEFT JOIN set_cards ON sets.id=set_cards.setID JOIN waifus ON set_cards.cardID=waifus.id WHERE waifus.name LIKE(?) AND claimed_by IS NULL";
        parameter = "%" + query.q + "%";
    }
    else if (query.type === 'waifuseries') {
        sets_query = "SELECT DISTINCT sets.id, sets.name, sets.reward, sets.rewardPudding FROM sets LEFT JOIN set_cards ON sets.id=set_cards.setID JOIN waifus ON set_cards.cardID=waifus.id WHERE waifus.series LIKE(?) AND claimed_by IS NULL";
        parameter = "%" + query.q + "%";
    }
    else if (query.type === 'progress') {
        sets_query = "SELECT DISTINCT sets.id, sets.name, sets.reward, sets.rewardPudding FROM set_cards JOIN sets ON set_cards.setID = sets.id LEFT JOIN has_waifu ON set_cards.cardID = has_waifu.waifuid JOIN users ON has_waifu.userid = users.id WHERE users.name = ? AND claimed_by IS NULL";
        parameter = query.user;
    }
    else if (query.type === 'allsets') {
        sets_query = "SELECT id, name, reward, rewardPudding FROM sets WHERE claimed_by IS NULL";
        parameter = null;
    }
    else {
        res.writeHead(400, "Bad Request");
        res.write("Bad Request");
        res.end();
        return;
    }
    con.query(sets_query, parameter,
        function (err, result) {
            if (err) throw err;
            let setsById = {};
            let setIDs = [];
            for (let row of result) {
                setsById[row.id] = {
                    id: row.id,
                    name: row.name,
                    reward: row.reward,
                    rewardPudding: row.rewardPudding,
                    totalCards: 0,
                    cardsOwned: 0,
                    cards: []
                };
                setIDs.push(row.id);
            }
            if (setIDs.length > 0) {
                let bindArray = [user].concat(setIDs);
                let inBinds = "?, ".repeat(setIDs.length).substring(0, setIDs.length * 3 - 2);
                con.query("SELECT setID, a.name AS userName, waifus.id AS waifuID, waifus.Name AS waifuName, waifus.base_rarity AS waifuRarity, waifus.image AS waifuImage, waifus.series AS waifuSeries FROM set_cards LEFT JOIN (SELECT DISTINCT has_waifu.waifuid, users.name FROM has_waifu JOIN users ON has_waifu.userid = users.id WHERE users.name = ?) AS a ON set_cards.cardID = a.waifuid JOIN waifus ON set_cards.cardID = waifus.id WHERE set_cards.setID IN(" + inBinds + ") ORDER BY waifuID", bindArray,
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
                        res.writeHead(200, {'Content-Type': 'text/json'});
                        let response = {count: 0, sets: []};
                        for (let setID in setsById) {
                            response.count += 1;
                            response.sets.push(setsById[setID]);
                        }
                        response.sets.sort(
                            function (a, b) {
                                let aValue = a.cardsOwned === 0 ? 9999999 : a.totalCards - a.cardsOwned;
                                let bValue = b.cardsOwned === 0 ? 9999999 : b.totalCards - b.cardsOwned;
                                return (aValue !== bValue) ? aValue - bValue : a.name.localeCompare(b.name);
                            });
                        res.write(JSON.stringify(response));
                        res.end();

                    })
            }
            else {
                res.writeHead(200, {'Content-Type': 'text/json'});
                res.write(JSON.stringify({count: 0, sets: []}));
                res.end();

            }
        })
}

function claimedsets(req, res, query) {
    con.query("SELECT setID, sets.name as setNam, waifus.id as waifuID, " +
        "waifus.Name as waifuName, waifus.base_rarity as waifuRarity, waifus.image as image, waifus.series as waifuSeries, " +
        "sort_index, sets.reward as setReward, sets.rewardPudding as setRewardPudding, users.name as userName " +
        "FROM set_cards " +
        "JOIN sets ON set_cards.setID = sets.id " +
        "JOIN waifus ON cardID = waifus.id " +
        "JOIN users ON sets.claimed_by = users.id " +
        "WHERE sets.claimed_by IS NOT NULL " +
        "ORDER BY sort_index, setID, waifus.id",
        function (err, result) {
            if (err) throw err;
            con.query("SELECT setID, rarity_sets.name as setNam, waifus.id as waifuID, waifus.Name as waifuName, waifus.base_rarity as waifuRarity, waifus.image as image, waifus.series as waifuSeries," +
                "rarity_sets.grouping as sort_index, rarity_sets.reward as setReward, 0 as setRewardPudding, users.name as userName " +
                "FROM rarity_sets_cards JOIN rarity_sets ON rarity_sets_cards.setID = rarity_sets.id " +
                "JOIN waifus ON cardID = waifus.id " +
                "JOIN users ON rarity_sets.claimed_by = users.id " +
                "WHERE rarity_sets.claimed_by IS NOT NULL " +
                "ORDER BY sort_index, setID, waifus.id", function (rerr, rresult) {
                if (rerr) throw rerr;
                res.writeHead(200, {'Content-Type': 'text/html'});
                res.write(sethead);
                res.write(claimedhead);
                let sets = [];
                let lastsortindex = null;
                let maxSetID = 0;
                let maxSortIndex = 0;
                for (let row of result) {
                    if (row.setID > maxSetID) {
                        maxSetID = row.setID;
                    }
                    if (row.sort_index > maxSortIndex) {
                        maxSortIndex = row.sort_index;
                    }
                }
                for (let row of rresult) {
                    row.setID += maxSetID;
                    row.sort_index += maxSortIndex;
                    result.push(row);
                }
                for (let row of result) {
                    if (sets.indexOf(row.setID) < 0) {
                        if (lastsortindex === null) {
                            lastsortindex = row.sort_index;
                        }
                        if (row.sort_index !== lastsortindex) {
                            res.write("<br/><br/>");
                            lastsortindex = row.sort_index;
                        }
                        sets.push(row.setID);
                        res.write(setbuttonpreids);
                        res.write(row.setID + "");
                        res.write(setbuttonbetweenids);
                        res.write(row.setID + "");
                        res.write(setbuttonbetweenidsandname);
                        res.write(row.setNam);
                        res.write(setbuttonend);
                    }
                }
                res.write(setbetween);
                let lastset = null;
                for (let row of result) {
                    if (lastset !== row.setID) {
                        if (lastset !== null) {
                            res.write(setsetfoot);
                        }
                        lastset = row.setID;
                        res.write(setsethead);
                        res.write(row.setID + "");
                        res.write(setsetbetween);
                        if(row.setReward > 0) {
                            res.write("Reward: " + row.setReward + " points");
                        }
                        else {
                            res.write("Reward: " + row.setRewardPudding + " pudding");
                        }
                        res.write(" - Claimed by: " + row.userName + "<br/>");
                    }
                    res.write(setimagehead);
                    res.write(white);
                    res.write(setimagebetween);
                    res.write(row.image);
                    res.write(setimageafterlink);
                    res.write("[" + row.waifuID + "][" + getRarityName(row.waifuRarity) + "] " + row.waifuName + " from " + row.waifuSeries);
                    res.write(setimagefoot);
                }
                res.write(setsetfoot);
                res.write(setfoot);
                res.end();
            })

        })
}

function getCardHtml(template, row) {
    template = template.replace(/{AMOUNTHOLDER}/g, row.amount > 1 ? bootstraphandamtholder : '');
    template = template.replace(/{PROMOTEDHOLDER}/g, row.rarity > row.base_rarity ? bootstraphandpromoholder : '');
    if (row.rarity > row.base_rarity && row.rarity < 8) {
        template = template.replace(/{STARS}/g, "★".repeat(row.rarity - row.base_rarity));
    }
    else {
        template = template.replace(/{STARS}/g, "");
    }
    template = template.replace(/{ID}/g, row.id.toString());
    template = template.replace(/{IMAGE}/g, (row.custom_image || row.image).toString());
    template = template.replace(/{CARDNAME}/g, row.Name.toString());
    template = template.replace(/{SERIES}/g, row.series.toString());
    template = template.replace(/{RARITY}/g, getRarityName(row.rarity));
    template = template.replace(/{AMOUNT}/g, row.amount.toString());
    return template;
}

function bootstraphand(req, res, query) {
    if (!('user' in query)) {
        res.writeHead(400, "Missing Parameter");
        res.end();
        return;
    }

    con.query("SELECT waifus.*, rarity, amount, custom_image FROM waifus JOIN has_waifu ON waifus.id = has_waifu.waifuid JOIN users ON " +
        "has_waifu.userid = users.id WHERE users.name = ? ORDER BY (has_waifu.rarity < 8) DESC, waifus.id ASC, has_waifu.rarity ASC", query.user, function (err, result) {
        if (err) throw err;
        let wantJSON = false;
        if ("accept" in req.headers && req.headers["accept"] === "application/json") {
            wantJSON = true;
        }
        if (result.length === 0) {
            if (wantJSON) {
                res.writeHead(404, "Not Found", {'Content-Type': 'application/json; charset=utf-8'});
                res.write(JSON.stringify({"error": {"status": 404, "explanation": "User not found"}}))
            } else {
                res.writeHead(404, "Not Found", {'Content-Type': 'text/html'});
                res.write(bootstraphandtpl.replace(/{CARDS}/g, "404 - This user doesn't exist.").replace(/{NAME}/g, escapeHtml(query.user)));

            }
            res.end();
            return;
        }
        con.query("SELECT eventTokens FROM users WHERE users.name = ?", query.user, function(err, resultTokens) {
            if (resultTokens.length === 0) {
                if (wantJSON) {
                    res.writeHead(404, "Not Found", {'Content-Type': 'application/json; charset=utf-8'});
                    res.write(JSON.stringify({"error": {"status": 404, "explanation": "User not found"}}))
                } else {
                    res.writeHead(404, "Not Found", {'Content-Type': 'text/html'});
                    res.write(bootstraphandtpl.replace(/{CARDS}/g, "404 - This user doesn't exist.").replace(/{NAME}/g, escapeHtml(query.user)));

                }
                res.end();
                return;
            }
            if (wantJSON) {
                let sanitizedResult = [];
                for (let row of result) {
                    let obj = {
                        "id": row.id,
                        "Name": row.Name,
                        "series": row.series,
                        "image": row.custom_image || row.image,
                        "base_rarity": row.base_rarity,
                        "rarity": row.rarity,
                        "amount": row.amount
                    };
                    sanitizedResult.push(obj);
                }
                res.writeHead(200, {'Content-Type': 'application/json'});
                res.write(JSON.stringify({'user': query.user, "cards": sanitizedResult, "eventTokens": resultTokens[0].eventTokens}))
            } else {
                res.writeHead(200, {'Content-Type': 'text/html'});
                let cards = '';
                for (let row of result) {
                    let card = bootstraphandcard;
                    card = getCardHtml(card, row);
                    cards += card;
                }
                if(resultTokens[0].eventTokens > 0) {
                    let card = bootstraphandeventtoken;
                    card = getCardHtml(card, { "id": "", "Name": config.eventTokenName, "series": "", "image": config.eventTokenImage, "base_rarity": 9, "rarity": 9, "amount": resultTokens[0].eventTokens});
                    cards += card;
                }
                res.write(bootstraphandtpl.replace(/{CARDS}/g, cards).replace(/{NAME}/g, query.user));
            }
            res.end();
        });
    });
}

function bootstrapbooster(req, res, query) {
    if (!('user' in query)) {
        res.writeHead(400, "Missing Parameter");
        res.end();
        return;
    }

    con.query("SELECT waifus.* FROM boosters_opened JOIN users ON boosters_opened.userid = users.id LEFT JOIN boosters_cards ON boosters_opened.id = boosters_cards.boosterid JOIN waifus ON boosters_cards.waifuid = waifus.id WHERE users.name = ? AND boosters_opened.status = 'open' ORDER BY waifus.id ASC", query.user, function (err, result) {
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
                }))
            } else {
                res.writeHead(404, "Not Found", {'Content-Type': 'text/html'});
                res.write(bootstrapboostertpl.replace(/{CARDS}/g, "404 - This user doesn't exist or has no open booster.").replace(/{NAME}/g, escapeHtml(query.user)));

            }
            res.end();
            return;
        }
        con.query("SELECT boosters_opened.eventTokens FROM boosters_opened JOIN users ON boosters_opened.userid = users.id WHERE users.name = ? AND boosters_opened.status = 'open'", query.user, function(err, resultTokens) {
            if(resultTokens.length === 0) {
                if (wantJSON) {
                    res.writeHead(404, "Not Found", {'Content-Type': 'application/json; charset=utf-8'});
                    res.write(JSON.stringify({
                        "error": {
                            "status": 404,
                            "explanation": "User not found or does not have an open booster."
                        }
                    }))
                } else {
                    res.writeHead(404, "Not Found", {'Content-Type': 'text/html'});
                    res.write(bootstrapboostertpl.replace(/{CARDS}/g, "404 - This user doesn't exist or has no open booster.").replace(/{NAME}/g, escapeHtml(query.user)));

                }
                res.end();
                return;
            }
            if (wantJSON) {
                res.writeHead(200, {'Content-Type': 'application/json'});
                let sanitizedResult = [];
                for (let row of result) {
                    let obj = {};
                    obj.id = row.id;
                    obj.Name = row.Name;
                    obj.image = row.image;
                    obj.rarity = row.base_rarity;
                    obj.series = row.series;
                    sanitizedResult.push(obj);
                }
                res.write(JSON.stringify({"user": query.user, "cards": sanitizedResult, "eventTokens": resultTokens[0].eventTokens}));
            } else {
                res.writeHead(200, {'Content-Type': 'text/html'});
                let cards = '';
                for (let row of result) {
                    let card = bootstrapboostercard;
                    card = card.replace(/{ID}/g, row.id.toString());
                    card = card.replace(/{IMAGE}/g, row.image.toString());
                    card = card.replace(/{CARDNAME}/g, row.Name.toString());
                    card = card.replace(/{SERIES}/g, row.series.toString());
                    card = card.replace(/{RARITY}/g, getRarityName(row.base_rarity));
                    cards += card;
                }
                for(let i = 0; i < resultTokens[0].eventTokens; i++) {
                    let card = bootstrapboostereventtoken;
                    card = card.replace(/{ID}/g, "");
                    card = card.replace(/{IMAGE}/g, config.eventTokenImage);
                    card = card.replace(/{CARDNAME}/g, config.eventTokenName);
                    card = card.replace(/{RARITY}/g, "promo");
                    cards += card;
                }
                let responsestr = bootstrapboostertpl.replace(/{CARDS}/g, cards).replace(/{NAME}/g, escapeHtml(query.user));
                res.write(responsestr);

            }

            res.end();
        });
    });
}

function pullfeed(req, res, query) {

    con.query("SELECT drops.rarity, drops.source, drops.channel, drops.timestamp, waifus.id AS waifuID, waifus.Name as waifuName, waifus.series AS waifuSeries, waifus.image AS waifuImage, users.name AS username " +
        "FROM drops JOIN waifus ON drops.waifuid = waifus.id JOIN users ON drops.userid = users.id WHERE drops.rarity >= 4 ORDER BY drops.id DESC LIMIT 100", function (err, result) {
        if (err) throw err;
        let wantJSON = false;
        let jsonresp = [];
        if ("accept" in req.headers && req.headers["accept"] === "application/json") {
            wantJSON = true;
            res.writeHead(200, {'Content-Type': 'application/json; charset=utf-8'});
        } else {
            res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'});
            res.write(pfhead);
        }
        for (let row of result) {
            let obj = {};
            if (wantJSON) {
                obj["timestamp"] = row.timestamp;
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
            } else {
                res.write("[" + new Date(row.timestamp).toISOString() + "] ");
                res.write(row.username + " pulled <code>[" + row.waifuID + "][" + getRarityName(row.rarity) + "] " + row.waifuName + " from " + row.waifuSeries + "</code>");
                if (row.source === 'freewaifu') {
                    res.write(" as a free waifu");
                }
                else if (row.source === 'buy') {
                    res.write(" using <code>!buy</code>");
                }
                else if (row.source.toString().startsWith("boosters.")) {
                    res.write(" from a " + row.source.substring(9) + " booster");
                }
                else {
                    res.write(" from a mysterious unknown source");
                }

                if (row.channel === '$$whisper$$') {
                    res.write(" via whisper.");
                }
                else {
                    res.write(" in " + row.channel.substring(1) + "&#39;s channel.");
                }
                res.write("<br />");
            }

        }
        if (wantJSON) {
            res.write(JSON.stringify(jsonresp));
        } else {
            res.write(pffoot);
        }
        res.end();
    });

}

function teaser(req, res, query) {
    res.writeHead(200, "naroHodo");
    res.write("<html><head><title>A Secret</title></head><body><small>Your journey isn't yet over.</small><br /><img src='https://share.marenthyu.de/B0kVw3MU.gif' width='100%' height='99%' alt='SGV5LCB5b3UgZm91bmQgbWUhIENvbmdyYXR1bGF0aW9ucyENCklmIHlvdSdyZSB0aGUgZmlyc3QsIHRha2UgdGhpcyBwcmVzZW50LCB5b3Uga25vdyB3aGVyZSB0byByZWRlZW0gaXQ6IFBhcnROdW1lcm9Vbm8NClNvLCBzb21lb25lIHdhcyBoZXJlIGJlZm9yZSB5b3UuIERvIHRha2UgdGhlIHNlY29uZGFyeSBwcmljZSwgYnV0IGRvbid0IHRlbGwgdGhlIG90aGVycyA7KTogUGFydE51bWVyb1Vub1lPVVJFVE9PU0xPVw0KDQpCdXQgc2luY2Ugd2UncmUgdGFsa2luZyB0ZWFzaW5nLCBoZXJlJ3MgdGhlIGdlbmVyYWwgZ2lzdDoNCkxhdGVyIHRvZGF5LCB3ZSB3aWxsIGFubm91bmNlIG91ciBuZXcgcmFyaXR5LCBteXRoaWNhbCwgb2ZmaWNpYWxseSEgVGhlcmUncyBhIGxvdCBtb3JlIGNvbWluZywgYnV0IHN0YXJ0aW5nIHdpdGggdGhlIG9mZmljaWFsIGFubm91bmNlbWVudCwgIXByb21vdGUgd2lsbCBiZSBsb2NrZWQuDQpBcyB3ZSBpbnRyb2R1Y2UgYSBuZXcgd2F5IHRvIHByb21vdGUgYW55IHdhaWZ1IHRvIHRoZSBuZXh0IHJhcml0eSwgeW91J2xsIGJlIGFibGUgdG8gaGF2ZSBhIHdhaWZ1IG5vdCBvbmx5IG9uIHRoZWlyIGJhc2UgcmFyaXR5LCBidXQgYWxzbyBwcm9tb3RlIGl0IHVwIHRoZSByYXJpdHkgbGFkZGVyLg0KDQpJdCdsbCBiZSBmdW4hDQoNCk9oIGFuZCByZW1lbWJlcjogSXQgYWluJ3Qgb3ZlciB1bnRpbCB0aGUgQ3JlZGl0cyByb2xsLg0KDQpZb3UncmUgaGVyZSBhZ2Fpbj8gR29vZCEgVGhhdCdzIGdvb2QhIEJlIHNuZWFreSBhbmQgeW91IHdpbGwgbm90IGxldCB0aGUgb3RoZXJzIGtub3d+DQpDaGVjayB0aG9zZSBtZXNzYWdlcyBpIHNlbnQuDQoNCkFuZCByZW1lbWJlcjogTm90IGV2ZXJ5dGhpbmcgbWF5IGJlIGNsZWFyIGZyb20gdGhlIGJlZ2lubmluZywgYnV0IHdpdGggdGltZSwgeW91IHdpbGwgaGF2ZSBhbGwgdGhlIGNsdWVzLg=='/></body></head></html>");
    res.end();
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
        }
        else if (query.type === 'incentives') {
            con.query("SELECT * FROM incentives WHERE incentives.status = 'open' AND incentives.amount < incentives.required ORDER BY incentives.id ASC", function (err, result) {
                if (err) throw err;
                res.writeHead(200, {'Content-Type': 'text/json'});
                res.write(JSON.stringify(result));
                res.end();
            });
        }
        else if (query.type === 'emotewar') {
            con.query("SELECT * FROM emoteWar ORDER BY count DESC", function (err, result) {
                if (err) throw err;
                res.writeHead(200, {'Content-Type': 'text/json'});
                res.write(JSON.stringify(result));
                res.end();
            });
        }
        else if (query.type === 'tracker') {
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
        }
        else {
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
    con.query("SELECT profileDescription, favourite, id FROM users WHERE users.name = ?", query.user, function (err, resultOuter) {
        if (err) throw err;
        if (resultOuter.length === 0) {
            res.writeHead(404, "User Not Found", {'Content-Type': 'text/html'});
            res.write(profiletpl.replace(/{NAME}/g, escapeHtml(query.user)).replace(/{BADGES}/g, "404 - User not found.").replace(/{DESCRIPTION}/g, "404 - User not found.").replace(/{FAVOURITE}/g, "404 - User not found.").replace(/{CURRENTSPENDINGS}/g, "0").replace(/{NEXTSPENDINGS}/g, "0"));

            res.end();
            return;
        }
        let userID = resultOuter[0].id;
        con.query("SELECT badges.name, badges.description, badges.image FROM has_badges JOIN badges ON has_badges.badgeID =" +
            " badges.id JOIN users ON has_badges.userID = users.id WHERE users.id = ?", userID, function (err, result) {
            if (err) throw err;
            res.writeHead(200, {'Content-Type': 'text/html'});
            let badges = '';
            for (let row of result) {
                let badge = badgetemplate;
                badge = badge.replace(/{DESCRIPTION}/g, row.description);
                badge = badge.replace(/{IMAGE}/g, row.image);
                badge = badge.replace(/{CARDNAME}/g, row.name);
                badges += badge;
            }
            con.query("SELECT waifus.id, waifus.Name, waifus.image, waifus.base_rarity, waifus.series, has_waifu.rarity, has_waifu.custom_image FROM waifus LEFT JOIN has_waifu ON (has_waifu.waifuid = waifus.id AND has_waifu.userid = ?) WHERE id = ? ORDER BY has_waifu.rarity DESC LIMIT 1", [userID, resultOuter[0].favourite], function (err, resultInner) {
                if (err) throw err;

                let row = resultInner[0];

                row.amount = 1;
                row.rarity = (row.rarity && row.rarity > row.base_rarity) ? row.rarity : row.base_rarity;

                let card = bootstraphandcard;

                card = getCardHtml(card, row);

                con.query("SELECT spending FROM users WHERE id = ?", userID, function (err, resultInnermost) {
                    if (err) throw err;
                    let spending = 0;
                    spending = resultInnermost[0].spending;
                    con.query("SELECT slot, spendings, (SELECT paidHandUpgrades FROM users WHERE id = ?) as paidSlots FROM handupgrades", userID, function (err, huLUTresult) {
                        if (err) throw err;

                        let nextspendings = 0;
                        let lastspendings = 0;

                        let paidSlots = huLUTresult[0].paidSlots;

                        if (paidSlots + 1 < huLUTresult.length) {
                            nextspendings = huLUTresult[paidSlots + 1].spendings;
                            lastspendings = huLUTresult[paidSlots].spendings;
                        } else {
                            nextspendings = huLUTresult[huLUTresult.length - 1].spendings + (1000000 * (paidSlots - (huLUTresult.length - 1) + 1));
                            lastspendings = huLUTresult[huLUTresult.length - 1].spendings + (1000000 * (paidSlots - (huLUTresult.length - 1)));
                        }

                        let percentspendings = Math.max(((spending - lastspendings) / (nextspendings - lastspendings)) * 100, 0);
                        res.write(profiletpl.replace(/{BADGES}/g, badges).replace(/{NAME}/g, escapeHtml(query.user)).replace(/{DESCRIPTION}/g, escapeHtml(resultOuter[0].profileDescription)).replace(/{FAVOURITE}/g, card).replace(/{LASTSPENDINGS}/g, "" + lastspendings).replace(/{CURRENTSPENDINGS}/g, "" + spending).replace(/{NEXTSPENDINGS}/g, "" + nextspendings).replace(/{PERCENTSPENDINGS}/g, "" + percentspendings));
                        res.end();

                    })
                })

            });

        });
    });

}

function holiday(req, res, query) {
    let user = "null";
    if ('user' in query) {
        user = query.user;
    }
    
    con.query("SELECT COUNT(*) AS cnt FROM boosters_opened WHERE boostername = ?", "holiday", function (err, result) {
        if (err) throw err;
        let goals = [2500, 5000, 10000];
        let holidayCount = result[0].cnt;
        let highestGoal = goals[goals.length-1];
        let currentAmountCapped = Math.min(highestGoal, holidayCount);
        let currentWidth = 100*Math.min(highestGoal, holidayCount)/highestGoal;
        let response = holidaytpl.replace(/{NAME}/g, escapeHtml(user));
        response = response.replace(/{CURRENT_WIDTH}/g, currentWidth).replace(/{CURRENT_AMOUNT_CAPPED}/g, currentAmountCapped).replace(/{HIGHEST_GOAL_AMOUNT}/g, highestGoal).replace(/{CURRENT_AMOUNT}/g, holidayCount);
        // individual goals
        let goalsOutput = "";
        for(let i=0;i<goals.length;i++) {
            goalsOutput += "<h3>Goal "+(i+1)+"</h3>";
            goalsOutput += "<div class='progress' style='height: 2rem; font-size: 1rem;'><div class='progress-bar progress-bar-striped"+(holidayCount >= goals[i] ? " bg-success" : "")+"' style='width: "+(100*Math.min(goals[i], holidayCount)/goals[i])+"%;' aria-valuenow='"+Math.min(goals[i], holidayCount)+"' aria-valuemin='0' aria-valuemax='"+goals[i]+"'>"+Math.min(goals[i], holidayCount)+"/"+goals[i]+(holidayCount >= goals[i] ? " (MET!)" : "")+"</div></div>";
        }
        response = response.replace(/{GOALS}/g, goalsOutput);
        res.write(response);
        res.end();
        
        
    });
}

function readConfig(callback) {
    config = {};
    con.query("SELECT * FROM config", function(err, result) {
        for(let row of result) {
            config[row.name] = row.value;
        }
        callback();
    });
}

function bootServer(callback) {
    http.createServer(function (req, res) {
        let q = url.parse(req.url, true);
        try {
            switch (q.pathname.replace("/", "")) {
                case "hand": {
                    bootstraphand(req, res, q.query);
                    break;
                }
                // case "sets": {
                    // smartsets(req, res, q.query);
                    // break;
                // }
                // case "smartsets": {
                    // smartsets(req, res, q.query);
                    // break;
                // }
                // case "smartsetsdata": {
                    // smartsetsdata(req, res, q.query);
                    // break;
                // }

                case "claimedsets": {
                    claimedsets(req, res, q.query);
                    break;
                }
                case "booster": {
                    bootstrapbooster(req, res, q.query);
                    break;
                }
                case "holiday": {
                    holiday(req, res, q.query);
                    break;
                }
                case "sets":
                case "smartsets":
                case "smartsetsdata":
                case "fancybooster": {
                    res.writeHead(410, 'Gone');
                    res.end();
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
                    res.writeHead(302, {'Location': 'https://discord.gg/qCtqzyF'});
                    res.end();
                    break;
                }
                case "live": {
                    res.writeHead(410, 'Gone');
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

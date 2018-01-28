'use strict';

let http = require('http');
let mysql = require('mysql');
let url = require('url');
let fs = require('fs');
let request = require('request');

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
for(let line of cfglines) {
    let lineparts = line.split("=");
    if(lineparts[0] == "dbpassword") {
        dbpw = lineparts[1];
    }
    else if(lineparts[0] == "database") {
        dbname = lineparts[1];
    }
    else if(lineparts[0] == "dbuser") {
        dbuser = lineparts[1];
    }
    else if(lineparts[0] == "dbhost") {
        dbhost = lineparts[1];
    }
}

if(dbpw === null || dbname === null || dbuser === null || dbhost === null) {
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

let rarities = {0: "common", 1: "uncommon", 2: "rare", 3: "super", 4: "ultra", 5: "legendary", 6: "mythical", 7: "god", 8: "special", 9: "promo"};
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
let red = "#ffabb2";
let green = "#96ff95";
let downloading = 0;

let bootstraphandtpl = fs.readFileSync('bootstraphandtemplate.htm', 'utf8');
let bootstraphandcard = '<div class="card card-tcg">' +
    '<div class="card-body card-body-tcg">' +
    '<img src="{IMAGE}" alt="{NAME}" title="{NAME}" class="card-image" />'+
    '<div class="id-holder rarity-{RARITY}">{ID}</div>'+
    '<div class="invisible-space-holder">&nbsp;</div>'+
    '<div class="rarity-holder rarity-{RARITY}">{RARITY}</div>'+
    '{AMOUNTHOLDER}'+
    '{PROMOTEDHOLDER}'+
    '</div>'+
    '<div class="card-footer text-center">'+
    '{NAME}<br />'+
    '{SERIES}'+
    '</div>'+
    '</div>'
let bootstraphandamtholder = '<div class="amount-holder rarity-{RARITY}">x{AMOUNT}</div>'
let bootstraphandpromoholder = '<div class="promotion-holder rarity-{RARITY}">{STARS}</div>'
let bootstrapboostertpl = fs.readFileSync('bootstrapboostertemplate.htm', 'utf8');
let bootstrapboostercard = '<div class="card card-tcg">' +
    '<div class="card-body card-body-tcg">' +
    '<img src="{IMAGE}" alt="{NAME}" title="{NAME}" class="card-image" />'+
    '<div class="id-holder rarity-{RARITY}">{ID}</div>'+
    '<div class="invisible-space-holder">&nbsp;</div>'+
    '<div class="rarity-holder rarity-{RARITY}">{RARITY}</div>'+
    '</div>'+
    '<div class="card-footer text-center">'+
    '<div class="keep-box">'+
    '<input type="checkbox" onchange="update()" /><br />Keep?'+
    '</div>'+
    '<div class="card-info">'+
    '{NAME}<br />'+
    '{SERIES}'+
    '</div>'+
    '</div>'+
    '</div>'
let smartsetstpl = fs.readFileSync('smartsets.htm', 'utf8');
let bootstrapwaifucss = fs.readFileSync('waifus-bootstrap.css', 'utf8');

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
    let response = smartsetstpl.replace(/{HTMLSTRINGUSER}/g, escapeHtml(user));
    res.writeHead(200, {'Content-Type': 'text/html'});
    res.write(response);
    res.end();
    return;
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
    
    if(query.type == 'progress') {
        if(!('user' in query)) {
            res.writeHead(400, "Missing Parameter");
            res.write("Missing Parameter");
            res.end();
            return;
        }
    }
    else if(query.type != 'allsets') {
        if(!('q' in query)) {
            res.writeHead(400, "Missing Parameter");
            res.write("Missing Parameter");
            res.end();
            return;
        }
        if(query.q.length < 3) {
            res.writeHead(400, "Bad Request");
            res.write("Query string too short");
            res.end();
            return;
        }
    }
    
    let sets_query = "";
    let parameter = "";
    if(query.type == 'setname') {
        sets_query = "SELECT id, name, reward FROM sets WHERE name LIKE (?) AND claimed_by IS NULL";
        parameter = "%"+query.q+"%";
    }
    else if(query.type == 'waifuname') {
        sets_query = "SELECT DISTINCT sets.id, sets.name, sets.reward FROM sets LEFT JOIN set_cards ON sets.id=set_cards.setID JOIN waifus ON set_cards.cardID=waifus.id WHERE waifus.name LIKE(?) AND claimed_by IS NULL";
        parameter = "%"+query.q+"%";
    }
    else if(query.type == 'waifuseries') {
        sets_query = "SELECT DISTINCT sets.id, sets.name, sets.reward FROM sets LEFT JOIN set_cards ON sets.id=set_cards.setID JOIN waifus ON set_cards.cardID=waifus.id WHERE waifus.series LIKE(?) AND claimed_by IS NULL";
        parameter = "%"+query.q+"%";
    }
    else if(query.type == 'progress') {
        sets_query = "SELECT DISTINCT sets.id, sets.name, sets.reward FROM set_cards JOIN sets ON set_cards.setID = sets.id LEFT JOIN has_waifu ON set_cards.cardID = has_waifu.waifuid JOIN users ON has_waifu.userid = users.id WHERE users.name = ? AND claimed_by IS NULL";
        parameter = query.user;
    }
    else if(query.type == 'allsets') {
        sets_query = "SELECT id, name, reward FROM sets WHERE claimed_by IS NULL";
        parameter = null;
    }
    else {
        res.writeHead(400, "Bad Request");
        res.write("Bad Request");
        res.end();
        return;
    }
    con.query(sets_query, parameter,
    function(err, result) {
        if(err) throw err;
        let setsById = {};
        let setIDs = [];
        for(let row of result) {
            setsById[row.id] = {id: row.id, name: row.name, reward: row.reward, totalCards: 0, cardsOwned: 0, cards: []};
            setIDs.push(row.id);
        }
        if(setIDs.length > 0) {
            let bindArray = [user].concat(setIDs);
            let inBinds = "?, ".repeat(setIDs.length).substring(0, setIDs.length*3 - 2);
            con.query("SELECT setID, a.name AS userName, waifus.id AS waifuID, waifus.Name AS waifuName, waifus.base_rarity AS waifuRarity, waifus.image AS waifuImage, waifus.series AS waifuSeries FROM set_cards LEFT JOIN (SELECT * FROM has_waifu JOIN users ON has_waifu.userid = users.id WHERE users.name = ?) AS a ON set_cards.cardID = a.waifuid JOIN waifus ON set_cards.cardID = waifus.id WHERE set_cards.setID IN("+inBinds+") ORDER BY waifuID", bindArray,
            function(err, result2) {
                if(err) throw err;
                for(let row of result2) {
                    let waifu = {id: row.waifuID, name: row.waifuName, rarity: getRarityName(row.waifuRarity), image: row.waifuImage, series: row.waifuSeries, owned: row.userName !== null}
                    setsById[row.setID].cards.push(waifu);
                    setsById[row.setID].totalCards += 1;
                    if(waifu.owned) {
                        setsById[row.setID].cardsOwned += 1;
                    }
                }
                // Build actual response
                res.writeHead(200, {'Content-Type': 'text/json'});
                let response = {count: 0, sets: []};
                for(let setID in setsById) {
                    response.count += 1;
                    response.sets.push(setsById[setID]);
                }
                response.sets.sort(
                function(a, b) {
                    let aValue = a.cardsOwned == 0 ? 9999999 : a.totalCards - a.cardsOwned;
                    let bValue = b.cardsOwned == 0 ? 9999999 : b.totalCards - b.cardsOwned;
                    return (aValue != bValue) ? aValue - bValue : a.name.localeCompare(b.name);
                });
                res.write(JSON.stringify(response));
                res.end();
                return;
            })
        }
        else {
            res.writeHead(200, {'Content-Type': 'text/json'});
            res.write(JSON.stringify({count: 0, sets: []}));
            res.end();
            return;
        }
    })
}

function claimedsets(req, res, query) {
    con.query("SELECT setID, sets.name as setNam, waifus.id as waifuID, " +
        "waifus.Name as waifuName, waifus.base_rarity as waifuRarity, waifus.image as image, waifus.series as waifuSeries, " +
        "sort_index, sets.reward as setReward, users.name as userName " +
        "FROM set_cards " +
        "JOIN sets ON set_cards.setID = sets.id " +
        "JOIN waifus ON cardID = waifus.id " +
        "JOIN users ON sets.claimed_by = users.id " +
        "WHERE sets.claimed_by IS NOT NULL " +
        "ORDER BY sort_index, setID, waifus.id",
        function (err, result) {
            if (err) throw err;
            con.query("SELECT setID, rarity_sets.name as setNam, waifus.id as waifuID, waifus.Name as waifuName, waifus.base_rarity as waifuRarity, waifus.image as image, waifus.series as waifuSeries," +
                "grouping as sort_index, rarity_sets.reward as setReward, users.name as userName " +
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
                        res.write("Reward: " + row.setReward + " - Claimed by: " + row.userName + "<br/>");
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

function bootstraphand(req, res, query) {
    if (!('user' in query)) {
        res.writeHead(400, "Missing Parameter");
        res.end();
        return;
    }

    con.query("SELECT waifus.*, rarity, amount FROM waifus JOIN has_waifu ON waifus.id = has_waifu.waifuid JOIN users ON " +
        "has_waifu.userid = users.id WHERE users.name = ? ORDER BY (has_waifu.rarity < 8) DESC, waifus.id ASC, has_waifu.rarity ASC", query.user, function (err, result) {
        if (err) throw err;
        if (result.length === 0) {
            res.writeHead(404, "User Not Found", {'Content-Type': 'text/html'});
            res.write("404 - User not found or Empty Hand.");
            res.end();
            return;
        }
        res.writeHead(200, {'Content-Type': 'text/html'});
        let cards = '';
        for (let row of result) {
            let card = bootstraphandcard;
            card = card.replace(/{AMOUNTHOLDER}/g, row.amount > 1 ? bootstraphandamtholder : '')
            card = card.replace(/{PROMOTEDHOLDER}/g, row.rarity > row.base_rarity ? bootstraphandpromoholder : '');
            if(row.rarity > row.base_rarity) {
                card = card.replace(/{STARS}/g, "★".repeat(row.rarity - row.base_rarity));
            }
            else {
                card = card.replace(/{STARS}/g, "");
            }
            card = card.replace(/{ID}/g, row.id.toString());
            card = card.replace(/{IMAGE}/g, row.image.toString());
            card = card.replace(/{NAME}/g, row.Name.toString());
            card = card.replace(/{SERIES}/g, row.series.toString());
            card = card.replace(/{RARITY}/g, getRarityName(row.rarity));
            card = card.replace(/{AMOUNT}/g, row.amount.toString());
            cards += card;
        }
        res.write(bootstraphandtpl.replace(/{CARDS}/g, cards).replace(/{NAME}/g, query.user));
        res.end();
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
        if (result.length === 0) {
            res.writeHead(404, "Not Found", {'Content-Type': 'text/html'});
            res.write("404 - This user doesn't exist or has no open booster.");
            res.end();
            return;
        }
        res.writeHead(200, {'Content-Type': 'text/html'});
        let cards = '';
        for (let row of result) {
            let card = bootstrapboostercard;
            card = card.replace(/{ID}/g, row.id.toString());
            card = card.replace(/{IMAGE}/g, row.image.toString());
            card = card.replace(/{NAME}/g, row.Name.toString());
            card = card.replace(/{SERIES}/g, row.series.toString());
            card = card.replace(/{RARITY}/g, getRarityName(row.base_rarity));
            cards += card;
        }
        res.write(bootstrapboostertpl.replace(/{CARDS}/g, cards));
        res.end();
    });
}

http.createServer(function (req, res) {
    let q = url.parse(req.url, true);
    try {
        switch (q.pathname.replace("/", "")) {
            case "hand": {
                bootstraphand(req, res, q.query);
                break;
            }
            case "sets": {
                smartsets(req, res, q.query);
                break;
            }
            case "smartsets": {
                smartsets(req, res, q.query);
                break;
            }
            case "smartsetsdata": {
                smartsetsdata(req, res, q.query);
                break;
            }

            case "claimedsets": {
                claimedsets(req, res, q.query);
                break;
            }
            case "booster": {
                bootstrapbooster(req, res, q.query);
                break;
            }
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
            case "take.png": {
                res.writeHead(302, {'Location': 'https://share.marenthyu.de/take.png'});
                res.end();
                break;
            }
            case "discard.png": {
                res.writeHead(302, {'Location': 'https://share.marenthyu.de/discard.png'});
                res.end();
                break;
            }
            case "particle2.png": {
                res.writeHead(302, {'Location': 'https://share.marenthyu.de/particle2.png'});
                res.end();
                break;
            }
            case "copy.png": {
                res.writeHead(302, {'Location': 'https://share.marenthyu.de/copy.png'});
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
            default: {
                res.writeHead(404, {'Content-Type': 'text/html'});
                res.write("The Specified content could not be found on this Server. If you want to know more about the Waifu TCG Bot, head over to https://waifus.de/help");
                res.end();
            }
        }
    } catch (err) {
        res.writeHead(500, "Internal Server Error", {"Content-Type":"text/html"});
        res.write("Something went wrong. Sorry. Blame Marenthyu! Tell him this: " + err.toString());
        res.end();
        console.log(err.toString());
    }


}).listen(8088);

'use strict';

let http = require('http');
let mysql = require('mysql');
let url = require('url');
let fs = require('fs');
let request = require('request');
let async = require('async');
let ejs = require('ejs');
let moment = require('moment');
let webpush = require('web-push');
let got = require('got');
let querystring = require('querystring');
let childProcess = require('child_process');
// From BarryCarlyon, thanks! https://github.com/BarryCarlyon/twitch_misc/blob/master/authentication/oidc_authentication/server.js
const jwt = require('jsonwebtoken');

let oidc_data = {};
let verifier_options;
let verifier_keys;
let verifier_client;
let jwksClient = require('jwks-rsa');

// Fetch OpenID data

// Twitch provides a endpoint that contains information about openID
// This includes the relevant endpoitns for authentatication
// And the available scopes
// And the keys for validation JWT's
got({
    url: 'https://id.twitch.tv/oauth2/.well-known/openid-configuration',
    method: 'GET',
    responseType: 'json'
})
    .then(resp => {
        console.log('BOOT: Got openID config');
        oidc_data = resp.body;

        verifier_options = {
            algorithms: oidc_data.id_token_signing_alg_values_supported,
            audience: config['clientID'],
            issuer: oidc_data.issuer
        }

        verifier_client = jwksClient({
            jwksUri: oidc_data.jwks_uri
        });
    })
    .catch(err => {
        console.error('OIDC Got a', err);
    });

// https://github.com/auth0/node-jsonwebtoken
function getKey(header, callback) {
    verifier_client.getSigningKey(header.kid, function (err, key) {
        var signingKey = key.publicKey || key.rsaPublicKey;
        callback(null, signingKey);
    });
}

// END Barry Code


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
let cfgConfig = {};
for (let line of cfglines) {
    let lineparts = line.split("=");
    let name = lineparts[0];
    let value = lineparts.slice(1).join("=");
    if (name) {
        cfgConfig[name] = value;
    }
    if (name === "dbpassword") {
        dbpw = value;
    } else if (name === "database") {
        dbname = value;
    } else if (name === "dbuser") {
        dbuser = value;
    } else if (name === "dbhost") {
        dbhost = value;
    } else if (name === "local") {
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
        charset: "utf8mb4"
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
jsdata['sw.js'] = fs.readFileSync('sw.js', 'utf-8'); //needs to be explicitly on the root

function booleanConfig(key) {
    return key in config && !["off", "no", "false"].includes(config[key].trim().toLowerCase());
}

function renderTemplateAndEnd(filename, vars, res) {
    vars["moment"] = moment;
    vars["config"] = config;
    vars["booleanConfig"] = booleanConfig;
    ejs.renderFile(filename, vars, {}, function (err, str) {
        if (err) {
            throw err;
        }
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

function parseCookies(req) {
    let header = req.headers.cookie;
    let cookies = {};
    if (!header) {
        return cookies;
    }
    let split = header.split(';');
    for (let item of split) {
        let parts = item.split('=');
        if (parts.length < 2) {
            continue;
        }
        cookies[parts[0].trim()] = decodeURIComponent(parts.slice(1).join('='));
    }
    return cookies;
}

function parseRequestBody(req, callback) {
    let body = '';
    req.on('data', (data) => {
        body += data;
        if (body.length > 1e6) {
            req.connection.destroy();
        }
    });
    req.on('end', () => {
        callback(body);
    });
}

function isAdminIdentity(identity, callback) {
    con.query('SELECT 1 FROM admins WHERE LOWER(name) = ? LIMIT 1', [identity.login.toLowerCase()], function (err, result) {
        if (err) {
            callback(err, false);
            return;
        }
        callback(null, result.length > 0);
    });
}

function getAdminJWTSecret() {
    return config['adminJwtSecret'] || config['adminjwtsecret'] || config['jwtSecret'] || config['jwtsecret'] || config['admin_jwt_secret'];
}

function issueAdminJWT(identity) {
    let secret = getAdminJWTSecret();
    if (!secret) {
        throw new Error('Missing adminJwtSecret configuration');
    }
    return jwt.sign({
        display_name: identity.display_name,
        login: identity.login,
        user_id: identity.user_id,
        is_admin: true
    }, secret, {
        algorithm: 'HS256',
        expiresIn: config['adminJwtExpirySeconds'] || '12h',
        issuer: 'waifus-admin',
        audience: 'waifus-admin-panel'
    });
}

function readAdminJWT(req) {
    let cookies = parseCookies(req);
    if (!cookies.admin_token) {
        return null;
    }
    let secret = getAdminJWTSecret();
    if (!secret) {
        return null;
    }
    try {
        return jwt.verify(cookies.admin_token, secret, {
            algorithms: ['HS256'],
            issuer: 'waifus-admin',
            audience: 'waifus-admin-panel'
        });
    } catch (e) {
        return null;
    }
}

function requireAdminJWT(req, res, callback) {
    let payload = readAdminJWT(req);
    if (!payload || payload.is_admin !== true) {
        httpError(res, 403, 'Forbidden', 'Admin access required.');
        return;
    }
    callback(payload);
}

function twitchOAuthStart(res) {
    let clientID = config['clientID'];
    let redirectUri = (config['siteHost'] || '').replace(/\/$/, '') + '/admin/twitch/callback';
    if (!clientID || !config['siteHost']) {
        httpError(res, 500, 'Server Error', 'Missing Twitch OAuth configuration.');
        return;
    }
    let state = Math.random().toString(36).slice(2) + Date.now().toString(36);
    let location = 'https://id.twitch.tv/oauth2/authorize?response_type=code&client_id=' + encodeURIComponent(clientID) +
        '&redirect_uri=' + encodeURIComponent(redirectUri) + '&scope=openid&state=' + encodeURIComponent(state);
    res.writeHead(302, {
        'Location': location,
        'Set-Cookie': 'twitch_admin_oauth_state=' + encodeURIComponent(state) + '; Path=/; HttpOnly; SameSite=Lax; Max-Age=600'
    });
    res.end();
}

function exchangeTwitchCode(code, callback) {
    let clientID = config['clientID'];
    let clientSecret = config['clientSecret'] || config['twitchclientsecret'] || config['twitchClientSecret'] || config['client_secret'];
    let redirectUri = (config['siteHost'] || '').replace(/\/$/, '') + '/admin/twitch/callback';
    if (!clientID || !clientSecret || !config['siteHost']) {
        console.error('[admin-oauth] Missing Twitch OAuth config values. clientID?', !!clientID, 'clientSecret?', !!clientSecret, 'siteHost?', !!config['siteHost']);
        callback(new Error('Missing Twitch OAuth config values clientID/clientSecret/siteHost'));
        return;
    }
    console.log('[admin-oauth] Exchanging Twitch code for token. redirectUri=', redirectUri, 'codeLength=', String(code || '').length);
    request.post({
        url: 'https://id.twitch.tv/oauth2/token',
        form: {
            client_id: clientID,
            client_secret: clientSecret,
            code: code,
            grant_type: 'authorization_code',
            redirect_uri: redirectUri
        },
        json: true
    }, function (err, response, body) {
        if (err) {
            console.error('[admin-oauth] Twitch token exchange request error:', err);
        }
        if (response) {
            console.log('[admin-oauth] Twitch token exchange response status:', response.statusCode);
        }
        if (err || !body || !body.access_token) {
            console.error('[admin-oauth] Twitch token exchange failed. Response body:', body);
            callback(err || new Error('Missing access token from Twitch'));
            return;
        }
        console.log('[admin-oauth] Twitch token exchange succeeded. accessTokenLength=', String(body.access_token || '').length);
        callback(null, body.access_token);
    });
}

function fetchTwitchUser(accessToken, callback) {
    request.get({
        url: 'https://api.twitch.tv/helix/users',
        headers: {
            'Authorization': 'Bearer ' + accessToken,
            'Client-ID': config['clientID']
        },
        json: true
    }, function (err, response, body) {
        if (err || !body || !body.data || body.data.length === 0) {
            callback(err || new Error('Unable to fetch Twitch user profile'));
            return;
        }
        let user = body.data[0];
        callback(null, {
            display_name: user.display_name,
            login: user.login,
            user_id: user.id
        });
    });
}

function getBoosterUpgradeColumns() {
    let normalRarities = parseInt(config['numNormalRarities'] || '0', 10);
    if (!normalRarities || normalRarities < 2) {
        normalRarities = 6;
    }
    let columns = [];
    for (let i = 0; i < normalRarities - 1; i++) {
        columns.push('rarity' + i + 'UpgradeChance');
    }
    return columns;
}

function defaultBoosterFormData() {
    let rarityChances = {};
    for (let column of getBoosterUpgradeColumns()) {
        rarityChances[column] = 1;
    }
    return {
        name: '',
        sortIndex: 0,
        listed: 0,
        buyable: 0,
        cost: 0,
        numCards: 1,
        guaranteeRarity: 0,
        guaranteeCount: 0,
        useEventWeightings: 0,
        maxEventTokens: 0,
        eventTokenChance: 0,
        canMega: 0,
        applyScaling: 1,
        rarityChances: rarityChances
    };
}

function parseBoosterForm(form) {
    let booster = {
        name: String(form.name || '').trim(),
        sortIndex: parseInt(form.sortIndex || '0', 10),
        listed: form.listed === '1' ? 1 : 0,
        buyable: form.buyable === '1' ? 1 : 0,
        cost: parseInt(form.cost || '0', 10),
        numCards: parseInt(form.numCards || '0', 10),
        guaranteeRarity: parseInt(form.guaranteeRarity || '0', 10),
        guaranteeCount: parseInt(form.guaranteeCount || '0', 10),
        useEventWeightings: form.useEventWeightings === '1' ? 1 : 0,
        maxEventTokens: parseInt(form.maxEventTokens || '0', 10),
        eventTokenChance: parseFloat(form.eventTokenChance || '0'),
        canMega: form.canMega === '1' ? 1 : 0,
        applyScaling: form.applyScaling === '1' ? 1 : 0,
        rarityChances: {}
    };
    for (let column of getBoosterUpgradeColumns()) {
        booster.rarityChances[column] = parseFloat(form[column] || '1');
    }
    let invalidFields = [];
    for (let field of ['sortIndex', 'cost', 'numCards', 'guaranteeRarity', 'guaranteeCount', 'maxEventTokens', 'eventTokenChance']) {
        if (Number.isNaN(booster[field])) {
            invalidFields.push(field);
        }
    }
    for (let column of Object.keys(booster.rarityChances)) {
        if (Number.isNaN(booster.rarityChances[column])) {
            invalidFields.push(column);
        }
    }
    if (invalidFields.length) {
        console.warn('[admin-booster] Parsed booster form contains invalid numeric fields.', {
            name: booster.name,
            invalidFields: invalidFields
        });
    }
    return booster;
}

function renderAdminPanel(req, res, adminUser, message, editWaifu, boosterForm) {
    res.writeHead(200, {'Content-Type': 'text/html'});
    renderTemplateAndEnd('templates/admin.ejs', {
        title: 'Admin Panel',
        currentPage: 'admin',
        user: adminUser.login,
        isAdmin: true,
        adminUser: adminUser,
        message: message || '',
        editWaifu: editWaifu || {
            id: '',
            name: '',
            series: '',
            image: '',
            base_rarity: 0,
            normal_weighting: 1,
            event_weighting: 1
        },
        boosterForm: boosterForm || defaultBoosterFormData(),
        boosterUpgradeColumns: getBoosterUpgradeColumns()
    }, res);
}

function adminPanel(req, res) {
    let adminUser = readAdminJWT(req);
    if (!adminUser || adminUser.is_admin !== true) {
        res.writeHead(302, {'Location': '/admin-login'});
        res.end();
        return;
    }
    renderAdminPanel(req, res, adminUser, '', null, null);
}

function adminLogout(res) {
    res.writeHead(302, {
        'Location': '/',
        'Set-Cookie': 'admin_token=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0'
    });
    res.end();
}

function adminLoadWaifu(req, res, query) {
    requireAdminJWT(req, res, (adminUser) => {
        let waifuID = parseInt(query.waifuId || '0', 10);
        if (!waifuID) {
            renderAdminPanel(req, res, adminUser, 'Please provide a valid Waifu ID to load.', null, null);
            return;
        }
        con.query('SELECT id, name, series, image, base_rarity, normal_weighting, event_weighting FROM waifus WHERE id = ? LIMIT 1', [waifuID], (err, rows) => {
            if (err) {
                httpError(res, 500, 'Server Error', 'Could not load waifu.');
                return;
            }
            if (!rows || rows.length === 0) {
                renderAdminPanel(req, res, adminUser, 'No waifu found for ID ' + waifuID + '.', null, null);
                return;
            }
            renderAdminPanel(req, res, adminUser, 'Loaded waifu #' + waifuID + ' for editing.', rows[0], null);
        });
    });
}

function adminUpdateWaifu(req, res) {
    requireAdminJWT(req, res, () => {
        parseRequestBody(req, (body) => {
            let form = querystring.parse(body);
            let waifuID = parseInt(form.waifuId, 10);
            if (!waifuID || !form.name || !form.series || !form.image) {
                httpError(res, 400, 'Bad Request', 'Missing required waifu fields');
                return;
            }
            con.query('UPDATE waifus SET name = ?, series = ?, image = ?, base_rarity = ?, normal_weighting = ?, event_weighting = ? WHERE id = ?', [form.name, form.series, form.image, parseInt(form.baseRarity || 0, 10), parseFloat(form.normalWeighting || 1), parseFloat(form.eventWeighting || 1), waifuID], (err) => {
                if (err) {
                    httpError(res, 500, 'Server Error', 'Could not update waifu.');
                    return;
                }
                res.writeHead(302, {'Location': '/admin'});
                res.end();
            });
        });
    });
}

function adminAddWaifu(req, res) {
    requireAdminJWT(req, res, () => {
        parseRequestBody(req, (body) => {
            let form = querystring.parse(body);
            if (!form.name || !form.series || !form.image) {
                httpError(res, 400, 'Bad Request', 'Missing required waifu fields');
                return;
            }
            con.query('INSERT INTO waifus(name, series, image, base_rarity, normal_weighting, event_weighting) VALUES (?, ?, ?, ?, ?, ?)', [form.name, form.series, form.image, parseInt(form.baseRarity || 0, 10), parseFloat(form.normalWeighting || 1), parseFloat(form.eventWeighting || 1)], (err) => {
                if (err) {
                    httpError(res, 500, 'Server Error', 'Could not add waifu.');
                    return;
                }
                res.writeHead(302, {'Location': '/admin'});
                res.end();
            });
        });
    });
}

function adminUpdateBooster(req, res) {
    requireAdminJWT(req, res, (adminUser) => {
        parseRequestBody(req, (body) => {
            let form = querystring.parse(body);
            let booster = parseBoosterForm(form);
            console.log('[admin-booster] Save request received.', {
                admin: adminUser && adminUser.login ? adminUser.login : 'unknown',
                boosterName: booster.name,
                listed: booster.listed,
                buyable: booster.buyable,
                numCards: booster.numCards,
                rarityColumnsConfigured: getBoosterUpgradeColumns().length
            });
            if (!booster.name) {
                console.warn('[admin-booster] Save aborted due to missing booster name.');
                httpError(res, 400, 'Bad Request', 'Missing booster name');
                return;
            }
            let rarityColumns = getBoosterUpgradeColumns();
            let fields = ['name', 'sortIndex', 'listed', 'buyable', 'cost', 'numCards', 'guaranteeRarity', 'guaranteeCount', 'useEventWeightings', 'maxEventTokens', 'eventTokenChance', 'canMega', 'applyScaling'].concat(rarityColumns);
            let placeholders = fields.map(() => '?').join(', ');
            let values = [booster.name, booster.sortIndex, booster.listed, booster.buyable, booster.cost, booster.numCards, booster.guaranteeRarity, booster.guaranteeCount, booster.useEventWeightings, booster.maxEventTokens, booster.eventTokenChance, booster.canMega, booster.applyScaling];
            for (let column of rarityColumns) {
                values.push(booster.rarityChances[column]);
            }
            let updateParts = fields.filter((field) => field !== 'name').map((field) => field + ' = VALUES(' + field + ')').join(', ');
            let sql = 'INSERT INTO boosters(' + fields.join(', ') + ') VALUES (' + placeholders + ') ON DUPLICATE KEY UPDATE ' + updateParts;
            con.query(sql, values, (err) => {
                if (err) {
                    console.error('[admin-booster] Save failed.', {
                        boosterName: booster.name,
                        fieldCount: fields.length,
                        rarityColumnCount: rarityColumns.length,
                        errorCode: err.code,
                        errorNumber: err.errno,
                        sqlState: err.sqlState,
                        sqlMessage: err.sqlMessage || err.message
                    });
                    httpError(res, 500, 'Server Error', 'Could not save booster settings.');
                    return;
                }
                console.log('[admin-booster] Save successful.', {
                    boosterName: booster.name,
                    admin: adminUser && adminUser.login ? adminUser.login : 'unknown'
                });
                res.writeHead(302, {'Location': '/admin'});
                res.end();
            });
        });
    });
}

function adminLoadBooster(req, res, query) {
    requireAdminJWT(req, res, (adminUser) => {
        let boosterName = String(query.name || '').trim();
        if (!boosterName) {
            console.warn('[admin-booster] Load aborted due to missing booster name.', {
                admin: adminUser && adminUser.login ? adminUser.login : 'unknown'
            });
            renderAdminPanel(req, res, adminUser, 'Please provide a booster name to load.', null, null);
            return;
        }
        console.log('[admin-booster] Load request received.', {
            admin: adminUser && adminUser.login ? adminUser.login : 'unknown',
            boosterName: boosterName
        });
        let rarityColumns = getBoosterUpgradeColumns();
        let sql = 'SELECT name, sortIndex, listed, buyable, cost, numCards, guaranteeRarity, guaranteeCount, useEventWeightings, maxEventTokens, eventTokenChance, canMega, applyScaling' + (rarityColumns.length ? ', ' + rarityColumns.join(', ') : '') + ' FROM boosters WHERE name = ? LIMIT 1';
        con.query(sql, [boosterName], (err, rows) => {
            if (err) {
                console.error('[admin-booster] Load failed.', {
                    boosterName: boosterName,
                    errorCode: err.code,
                    errorNumber: err.errno,
                    sqlState: err.sqlState,
                    sqlMessage: err.sqlMessage || err.message
                });
                httpError(res, 500, 'Server Error', 'Could not load booster.');
                return;
            }
            if (!rows || rows.length === 0) {
                console.warn('[admin-booster] Load found no booster.', {
                    boosterName: boosterName
                });
                renderAdminPanel(req, res, adminUser, 'No booster found for name "' + boosterName + '".', null, null);
                return;
            }
            let row = rows[0];
            let boosterForm = {
                name: row.name,
                sortIndex: row.sortIndex,
                listed: row.listed,
                buyable: row.buyable,
                cost: row.cost,
                numCards: row.numCards,
                guaranteeRarity: row.guaranteeRarity,
                guaranteeCount: row.guaranteeCount,
                useEventWeightings: row.useEventWeightings,
                maxEventTokens: row.maxEventTokens,
                eventTokenChance: row.eventTokenChance,
                canMega: row.canMega,
                applyScaling: row.applyScaling,
                rarityChances: {}
            };
            for (let column of rarityColumns) {
                boosterForm.rarityChances[column] = row[column];
            }
            console.log('[admin-booster] Load successful.', {
                boosterName: boosterName,
                rarityColumnCount: rarityColumns.length
            });
            renderAdminPanel(req, res, adminUser, 'Loaded booster "' + boosterName + '" for editing.', null, boosterForm);
        });
    });
}

function adminGetListedBoosters(req, res) {
    requireAdminJWT(req, res, () => {
        con.query('SELECT name FROM boosters WHERE listed = 1 ORDER BY sortIndex ASC, name ASC', (err, rows) => {
            if (err) {
                console.error('[admin-event-close] Could not load listed boosters:', err);
                httpError(res, 500, 'Server Error', 'Could not load listed boosters.');
                return;
            }
            res.writeHead(200, {'Content-Type': 'application/json'});
            res.end(JSON.stringify({boosters: rows.map((row) => row.name)}));
        });
    });
}

function triggerPythonBotReload(callback) {
    childProcess.execFile('pkill', ['-USR1', '-f', 'main.py'], (err, stdout, stderr) => {
        if (err) {
            callback(err);
            return;
        }
        callback(null, {
            stdout: stdout,
            stderr: stderr
        });
    });
}

function adminCloseEvent(req, res) {
    requireAdminJWT(req, res, (adminUser) => {
        parseRequestBody(req, (body) => {
            let form = querystring.parse(body);
            let boosterName = String(form.boosterName || '').trim();
            let allowPromotedCopies = form.allowPromotedCopies === '1';
            if (!boosterName) {
                httpError(res, 400, 'Bad Request', 'Missing boosterName.');
                return;
            }
            console.log('[admin-event-close] Request received.', {
                admin: adminUser && adminUser.login ? adminUser.login : 'unknown',
                boosterName: boosterName,
                allowPromotedCopies: allowPromotedCopies
            });
            con.query('SELECT id, name FROM waifus WHERE is_event = 1 AND rarity = ? LIMIT 1', ['promoted'], (promotedErr, promotedRows) => {
                if (promotedErr) {
                    console.error('[admin-event-close] Could not inspect promoted event waifus:', promotedErr);
                    httpError(res, 500, 'Server Error', 'Could not inspect event waifu rarity state.');
                    return;
                }
                if (promotedRows && promotedRows.length > 0 && !allowPromotedCopies) {
                    res.writeHead(409, {'Content-Type': 'application/json'});
                    res.end(JSON.stringify({
                        requiresConfirmation: true,
                        message: 'Some event waifus are already promoted. Confirm to continue anyway.',
                        sampleWaifuId: promotedRows[0].id,
                        sampleWaifuName: promotedRows[0].name
                    }));
                    return;
                }
                con.beginTransaction((txErr) => {
                    if (txErr) {
                        console.error('[admin-event-close] Could not start transaction:', txErr);
                        httpError(res, 500, 'Server Error', 'Could not start update transaction.');
                        return;
                    }
                    con.query('UPDATE waifus SET rarity = ? WHERE is_event = 1', ['promo'], (waifuErr, waifuResult) => {
                        if (waifuErr) {
                            return con.rollback(() => {
                                console.error('[admin-event-close] Could not update event waifus:', waifuErr);
                                httpError(res, 500, 'Server Error', 'Could not update event waifus.');
                            });
                        }
                        con.query('UPDATE boosters SET listed = 0, buyable = 0 WHERE name = ? LIMIT 1', [boosterName], (boosterErr, boosterResult) => {
                            if (boosterErr) {
                                return con.rollback(() => {
                                    console.error('[admin-event-close] Could not update booster listing state:', boosterErr);
                                    httpError(res, 500, 'Server Error', 'Could not update booster listing state.');
                                });
                            }
                            if (!boosterResult || boosterResult.affectedRows < 1) {
                                return con.rollback(() => {
                                    httpError(res, 404, 'Not Found', 'Selected booster not found.');
                                });
                            }
                            con.commit((commitErr) => {
                                if (commitErr) {
                                    return con.rollback(() => {
                                        console.error('[admin-event-close] Could not commit updates:', commitErr);
                                        httpError(res, 500, 'Server Error', 'Could not commit event close updates.');
                                    });
                                }
                                triggerPythonBotReload((reloadErr) => {
                                    if (reloadErr) {
                                        console.error('[admin-event-close] Data updates succeeded, but bot reload trigger failed:', reloadErr);
                                        httpError(res, 500, 'Server Error', 'Event updates were applied, but bot reload trigger failed.');
                                        return;
                                    }
                                    console.log('[admin-event-close] Completed successfully.', {
                                        admin: adminUser && adminUser.login ? adminUser.login : 'unknown',
                                        boosterName: boosterName,
                                        waifusUpdated: waifuResult && typeof waifuResult.affectedRows === 'number' ? waifuResult.affectedRows : null
                                    });
                                    res.writeHead(200, {'Content-Type': 'application/json'});
                                    res.end(JSON.stringify({
                                        ok: true,
                                        message: 'Event waifus switched to promo, booster hidden/unbuyable, and bot reload triggered.'
                                    }));
                                });
                            });
                        });
                    });
                });
            });
        });
    });
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
                res.writeHead(404, "Not Found", {'Content-Type': 'application/json; charset=utf-8'});
                res.write(JSON.stringify({"error": {"status": 404, "explanation": "User not found"}}));
                res.end();
            } else {
                res.writeHead(404, "Not Found", {'Content-Type': 'text/html'});
                renderTemplateAndEnd("templates/hand.ejs", {
                    user: query.user,
                    cards: [],
                    error: "404 - This user doesn't exist.",
                    eventTokens: 0
                }, res);
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
                    renderTemplateAndEnd("templates/hand.ejs", {
                        user: query.user,
                        cards: [],
                        error: "404 - This user doesn't exist.",
                        eventTokens: 0
                    }, res);
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
                res.writeHead(200, {'Content-Type': 'application/json'});
                res.write(JSON.stringify({
                    'user': query.user,
                    "cards": sanitizedResult,
                    "eventTokens": resultTokens[0].eventTokens
                }));
                res.end();
            } else {
                res.writeHead(200, {'Content-Type': 'text/html'});
                renderTemplateAndEnd("templates/hand.ejs", {
                    user: query.user,
                    cards: result,
                    error: "",
                    eventTokens: resultTokens[0].eventTokens
                }, res);
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
                renderTemplateAndEnd("templates/booster.ejs", {
                    user: query.user,
                    cards: [],
                    error: "404 - This user doesn't exist or has no open booster.",
                    eventTokens: 0
                }, res);
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
                    renderTemplateAndEnd("templates/booster.ejs", {
                        user: query.user,
                        cards: [],
                        error: "404 - This user doesn't exist or has no open booster.",
                        eventTokens: 0
                    }, res);
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
                renderTemplateAndEnd("templates/booster.ejs", {
                    user: query.user,
                    cards: result,
                    error: "",
                    eventTokens: resultTokens[0].eventTokens
                }, res);
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
                res.writeHead(200, {'Content-Type': 'text/json'});
                res.write(JSON.stringify(wars));
                res.end();
            });
        } else if (query.type === 'incentives') {
            con.query("SELECT id, title, status, IF(id='BonusGame', amount * 10000000, amount) AS amount, IF(id='BonusGame', required*10000000, required) AS required FROM incentives WHERE incentives.status = 'open' AND incentives.amount < incentives.required ORDER BY incentives.id ASC", function (err, result) {
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
                con.query("SELECT id, title, status, IF(id='BonusGame', amount * 10000000, amount) AS amount, IF(id='BonusGame', required*10000000, required) AS required FROM incentives WHERE incentives.status = 'open' AND incentives.amount < incentives.required ORDER BY incentives.id ASC", function (err, result2) {
                    if (err) throw err;
                    con.query("SELECT * FROM cpuwar ORDER BY votes DESC", function (err, result3) {
                        if (err) throw err;
                        res.writeHead(200, {'Content-Type': 'text/json'});
                        res.write(JSON.stringify({"wars": wars, "incentives": result2, "cpuwar": result3}));
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
    res.writeHead(200, {'Content-Type': 'text/html'});
    renderTemplateAndEnd("templates/sets.ejs", {user: user}, res);
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
    } else if (query.type !== 'allsets') {
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
    } else if (['waifuname', 'waifuseries', 'progress', 'allsets'].includes(query.type)) {
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
    } else {
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
            } else {
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
                    res.writeHead(200, {'Content-Type': 'text/json'});
                    let response = {count: 0, sets: []};
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
                                } else if (row.pointsRecvd) {
                                    set["claimedText"] += row.pointsRecvd + " points";
                                } else {
                                    set["claimedText"] += row.puddingRecvd + " pudding";
                                }
                            }
                        }
                        if (!row.claimable) {
                            set["claimableIcon"] = "cancel";
                            set["claimableText"] = "Not currently claimable";
                        } else if (row.lastClaimTime && new Date(row.lastClaimTime + config.setCooldownDays * 86400000) > Date.now()) {
                            set["claimableIcon"] = "watch_later";
                            set["claimableText"] = "On cooldown, claimable " + moment(new Date(row.lastClaimTime + config.setCooldownDays * 86400000)).fromNow();
                        } else {
                            set["claimableIcon"] = (checkOwnership && set.cardsOwned == set.totalCards) ? "done_all" : "done";
                            set["claimableText"] = "Currently claimable";
                        }
                        if (row.claimable && checkOwnership) {
                            if (set.cardsOwned == set.totalCards) {
                                set["claimableText"] += " (all cards obtained)";
                            } else {
                                set["claimableText"] += " (" + (set.totalCards - set.cardsOwned) + " cards missing)";
                            }
                        }
                        if (row.numClaims == 0) {
                            set["numClaimsText"] = "No claims yet";
                        } else if (row.numClaims == 1) {
                            set["numClaimsText"] = "One claim by " + row.firstClaimerName;
                        } else {
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
            res.writeHead(200, {'Content-Type': 'text/json'});
            res.write(JSON.stringify({count: 0, sets: []}));
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
        res.writeHead(200, {'Content-Type': 'text/html'});
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

            res.writeHead(200, {'Content-Type': 'text/html'});
            renderTemplateAndEnd("templates/tracker.ejs", {user: user, wars: wars, incentives: incentives}, res);
        });
    });
}

function browser(req, res, query) {
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
    let buff = Buffer.from(auth.replace('Basic ', ''), 'base64');
    let authText = buff.toString('utf-8');
    let parts = authText.split(':');
    let user = parts[0];
    let pass = parts[1];
    if (config['adminPass'] === pass) {
        con.query("SELECT waifus.* FROM waifus LIMIT ?, 100", [(page * 100)], function (err, result) {
            if (err) throw err;
            renderTemplateAndEnd("templates/image-browser.ejs", {user: user, page: page, cards: result}, res);
        });

    } else {
        res.setHeader("WWW-Authenticate", "Basic realm=\"Waifu TCG Admin\", charset=\"UTF-8\"");
        httpError(res, 401, "Unauthorized wrong password");
        return
    }

}

function requiredAuthWithOIDC(req, res, callback) {
    let auth = req.headers['authorization'];
    console.log(JSON.stringify(auth));
    if (!auth) {
        httpError(res, 401, 'Unauthorized', 'You did not send any authentication, smh');
        return
    }
    console.log("Found auth header: " + JSON.stringify(auth));
    let authParts = auth.split(" ");
    if (authParts.length !== 2 || authParts[0] !== 'Bearer') {
        httpError(res, 401, 'Unauthorized', 'Invalid Auth Header');
        return;
    }
    jwt.verify(authParts[1], getKey, verifier_options, function (err, payload) {
        if (err) {
            console.error("Error authenticating user JWT.");
            console.error(err);
            httpError(res, 401, 'Unauthorized', 'Error during authentication verification.');
        } else {
            callback(payload);
        }
    });
}

function pushRegistration(req, res, query) {
    if (req.method === 'OPTIONS') {
        res.writeHead(200, 'BUGGER OFF CORS', {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': 'https://waifus.de',
            'Access-Control-Allow-Headers': 'Authorization',
            'Access-Control-Allow-Methods': 'OPTIONS, POST'
        });
        res.end(JSON.stringify({message: 'SERIOUSLY; I HATE THIS!'}));
        return
    } else if (req.method === 'POST') {
        requiredAuthWithOIDC(req, res, (payload) => {
            console.log("Successful login for " + payload.sub + ", waiting for post data...");
            let body = '';
            req.on('data', (data) => {
                body += data;

                // Too much POST data, kill the connection!
                // 1e6 === 1 * Math.pow(10, 6) === 1 * 1000000 ~~~ 1MB
                if (body.length > 1e6) {
                    req.connection.destroy();
                    httpError(res, 413, 'Entity Too Large', "THAT'S TOO MUCH DATA!");
                }
            });

            req.on('end', () => {
                let sub = {};
                try {
                    sub = JSON.parse(body);
                    if (!sub.hasOwnProperty('endpoint') || !sub.hasOwnProperty('keys')) {
                        throw new Error("Missing Properties");
                    }
                    new url.URL(sub.endpoint);
                    if (sub.hasOwnProperty('expirationTime') && sub.expirationTime !== null && Date.now() > sub.expirationTime) {
                        throw new Error("Already Expired");
                    }
                } catch (e) {
                    console.error("Error during post data processing:");
                    console.error(e);
                    httpError(res, 422, 'Unprocessable Entity', "That's not a valid subscription!");
                    return
                }
                webpush.sendNotification(sub, JSON.stringify({
                    type: 'subSuccess',
                    message: 'Successfully Subscribed to TCG Push Notifications!'
                })).then(() => {
                    con.query("INSERT INTO push_subscriptions(subscription, userid) VALUES (?, ?)", [JSON.stringify(sub), payload.sub], (err, result) => {
                        if (err) {
                            httpError(res, 500, 'Server Error', 'Server Error adding subscription');
                            console.error(err);
                        } else {
                            res.writeHead(200, 'OK', {
                                'Content-Type': 'application/json',
                                'Access-Control-Allow-Origin': 'https://waifus.de',
                                'Access-Control-Allow-Headers': 'Authorization',
                                'Access-Control-Allow-Methods': 'OPTIONS, POST'
                            });
                            res.end(JSON.stringify({message: 'Subscription added successfully!'}));
                            console.log("Subscription added to database!");
                        }
                    });
                }).catch((err) => {
                    console.error(err);
                    httpError(res, 500, 'Server Error', 'Could not send test notification.');
                })


            })
        });
    } else {
        res.writeHead(405, 'Method Not Allowed', {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': 'https://waifus.de',
            'Access-Control-Allow-Headers': 'Authorization',
            'Access-Control-Allow-Methods': 'OPTIONS, POST',
            'Allow': 'OPTIONS, POST'
        });
        res.end(JSON.stringify({message: 'The hell are you trying to do?'}))
    }
}

function sendPush(req, res, query) {
    if (req.method !== 'POST') {
        httpError(res, 405, 'Method Not Allowed', "GO AWAY");
        return
    }
    let auth = req.headers["authorization"];
    if (!auth) {
        res.setHeader("WWW-Authenticate", "Basic realm=\"Waifu TCG Admin\", charset=\"UTF-8\"");
        httpError(res, 401, "Unauthorized");
        return
    }
    let buff = Buffer.from(auth.replace('Basic ', ''), 'base64');
    let authText = buff.toString('utf-8');
    let parts = authText.split(':');
    let user = parts[0];
    let pass = parts[1];
    if (config['adminPass'] === pass && user === 'internal') {
        let body = '';
        req.on('data', (data) => {
            body += data;

            // Too much POST data, kill the connection!
            // 1e6 === 1 * Math.pow(10, 6) === 1 * 1000000 ~~~ 1MB
            if (body.length > 1e6) {
                req.connection.destroy();
                httpError(res, 413, 'Entity Too Large', "THAT'S TOO MUCH DATA!");
            }
        });

        req.on('end', () => {
            let obj = JSON.parse(body);
            if (obj.ids === 'all') {
                con.query("SELECT subscription FROM push_subscriptions", (err, result) => {
                    if (err) {
                        httpError(res, 500, 'Server Error', JSON.stringify({message: 'Error during query of subscriptions'}));
                    } else {
                        sendPushNotification(result, obj.data);
                        res.writeHead(200, 'OK', {
                            'Content-Type': 'application/json'
                        });
                        res.end(JSON.stringify({message: 'Notifications triggered'}));
                    }
                });
            } else {
                con.query("SELECT subscription, id FROM push_subscriptions WHERE userid IN (?)", [obj.ids], (err, result) => {
                    if (err) {
                        httpError(res, 500, 'Server Error', JSON.stringify({message: 'Error during query of subscriptions'}));
                    } else {

                        sendPushNotification(result, obj.data);
                        res.writeHead(200, 'OK', {
                            'Content-Type': 'application/json'
                        });
                        res.end(JSON.stringify({message: 'Notifications triggered'}));
                    }
                });
            }
        });
    } else {
        res.setHeader("WWW-Authenticate", "Basic realm=\"Waifu TCG Admin\", charset=\"UTF-8\"");
        httpError(res, 401, "Unauthorized", "You really shouldn't be here.");
        return
    }
}

function sendPushNotification(subscriptions, data) {
    let counter = 0;
    for (let sub of subscriptions) {
        // console.log("Subscription: " + JSON.stringify(sub));
        webpush.sendNotification(JSON.parse(sub['subscription']), JSON.stringify(data)).then(() => {
            console.log("Sent notification " + (++counter).toString() + "/" + subscriptions.length.toString());
        }).catch((err) => {
            console.log("Error sending notification " + (++counter).toString() + "/" + subscriptions.length.toString() + " - deleting");
            console.log("Subscription: " + JSON.stringify(sub));
            console.error(err);
            removeSubscription(sub['id']);
        });
    }
}

function removeSubscription(subID) {
    con.query("DELETE FROM push_subscriptions WHERE id = ?", [subID], (err, result) => {
        if (err) console.error(err);
        else console.log("Removed subscription " + JSON.stringify(subID));
    });
}

function readConfig(callback) {
    config = Object.assign({}, cfgConfig);
    if (!isLocalMode) {
        con.query("SELECT * FROM config", function (err, result) {
            for (let row of result) {
                config[row.name] = row.value;
            }
            webpush.setVapidDetails('mailto:' + config["vapidContactEmail"], config['vapidPublicKey'], config['vapidPrivateKey']);
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
                res.writeHead(200, {'Content-Type': 'text/javascript'});
                res.write(jsdata[q.pathname.substring(4)]);
                res.end();
                return;
            }
            if (q.pathname === "/sw.js") {
                res.writeHead(200, {'Content-Type': 'text/javascript'});
                res.write(jsdata['sw.js']);
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
                    res.writeHead(302, {'Location': config.helpDocURL});
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
                case "tracker": {
                    if (booleanConfig("marathonTrackerEnabled")) {
                        tracker(req, res, q.query);
                        break;
                    } else {
                        res.writeHead(404, {'Content-Type': 'text/html'});
                        res.write("The Specified content could not be found on this Server. If you want to know more about the Waifu TCG Bot, head over to https://waifus.de/help");
                        res.end();
                    }
                    break;
                }
                case "packs": {
                    if (booleanConfig("packTrackerEnabled")) {
                        packTracker(req, res, q.query);
                    } else {
                        res.writeHead(404, {'Content-Type': 'text/html'});
                        res.write("The Specified content could not be found on this Server. If you want to know more about the Waifu TCG Bot, head over to https://waifus.de/help");
                        res.end();
                    }
                    break;
                }
                case "rules": {
                    res.writeHead(200, "OK", {'Content-Type': 'text/html'});
                    renderTemplateAndEnd("templates/rules.ejs", {
                        nepdoc: config['nepdocURL'],
                        currentPage: "rules",
                        user: q.query.user
                    }, res);
                    break;
                }
                case "browser": {
                    browser(req, res, q.query);
                    break;
                }
                case "admin": {
                    adminPanel(req, res);
                    break;
                }
                case "admin-login": {
                    twitchOAuthStart(res);
                    break;
                }
                case "admin-logout": {
                    adminLogout(res);
                    break;
                }
                case "admin-waifu-add": {
                    adminAddWaifu(req, res);
                    break;
                }
                case "admin-waifu-load": {
                    adminLoadWaifu(req, res, q.query);
                    break;
                }
                case "admin-waifu-update": {
                    adminUpdateWaifu(req, res);
                    break;
                }
                case "admin-booster-load": {
                    adminLoadBooster(req, res, q.query);
                    break;
                }
                case "admin-listed-boosters": {
                    adminGetListedBoosters(req, res);
                    break;
                }
                case "admin-event-close": {
                    adminCloseEvent(req, res);
                    break;
                }
                case "admin-booster-update": {
                    adminUpdateBooster(req, res);
                    break;
                }
                case "pushregistration": {
                    pushRegistration(req, res, q.query);
                    break;
                }
                case "twitchauth": {
                    res.writeHead(200, "OK", {'Content-Type': 'text/html'});
                    renderTemplateAndEnd("templates/twitchauth.ejs", {
                        currentPage: "twitchauth",
                        publicKey: config["vapidPublicKey"],
                        user: "user" in q.query ? q.query['user'] : ''
                    }, res);
                    break;
                }
                case "push": {
                    res.writeHead(302, {'Location': "https://id.twitch.tv/oauth2/authorize?response_type=id_token&client_id=6rm9gnxqvo42oprfnqx8b7hptqkfn9&redirect_uri=" + config['siteHost'] + "/twitchauth&scope=openid"});
                    res.end();
                    break;
                }
                case "sendpush": {
                    sendPush(req, res, q.query);
                    break;
                }
                case "admin/twitch/callback": {
                    let cookies = parseCookies(req);
                    if (!q.query.code || !q.query.state || cookies.twitch_admin_oauth_state !== q.query.state) {
                        console.error('[admin-oauth] Invalid callback state.', {
                            hasCode: !!q.query.code,
                            hasState: !!q.query.state,
                            hasCookieState: !!cookies.twitch_admin_oauth_state,
                            stateMatches: cookies.twitch_admin_oauth_state === q.query.state
                        });
                        httpError(res, 400, 'Bad Request', 'Invalid Twitch OAuth callback state.');
                        break;
                    }
                    console.log('[admin-oauth] Received valid callback. stateLength=', String(q.query.state || '').length, 'codeLength=', String(q.query.code || '').length);
                    exchangeTwitchCode(q.query.code, (err, accessToken) => {
                        if (err) {
                            console.error('[admin-oauth] Could not exchange Twitch OAuth code:', err);
                            httpError(res, 500, 'Server Error', 'Could not exchange Twitch OAuth code.');
                            return;
                        }
                        fetchTwitchUser(accessToken, (err2, identity) => {
                            if (err2) {
                                httpError(res, 500, 'Server Error', 'Could not load Twitch profile.');
                                return;
                            }
                            isAdminIdentity(identity, (err3, isAdmin) => {
                                if (err3) {
                                    httpError(res, 500, 'Server Error', 'Could not verify admin permissions.');
                                    return;
                                }
                                if (!isAdmin) {
                                    httpError(res, 403, 'Forbidden', 'This Twitch account is not an admin.');
                                    return;
                                }
                                    let token;
                                    try {
                                        token = issueAdminJWT(identity);
                                    } catch (jwtErr) {
                                        console.error('[admin-oauth] Could not issue admin JWT:', jwtErr);
                                        httpError(res, 500, 'Server Error', 'Could not issue admin JWT.');
                                        return;
                                    }
                                    res.writeHead(302, {
                                        'Location': '/admin',
                                        'Set-Cookie': 'admin_token=' + encodeURIComponent(token) + '; Path=/; HttpOnly; SameSite=Lax; Max-Age=43200'
                                    });
                                res.end();
                            });
                        });
                    });
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

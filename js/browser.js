let cardTemplate = '<div class="card card-tcg card-{rarityName}">\n' +
    '<label>\n' +
    '<div class="card-bg-image" style="background-image: url({image});"></div>\n' +
    '<div class="card-body card-body-tcg">\n' +
    '<img src="{image}" alt="{name}" title="{name}" class="card-image" />\n' +
    '<div class="id-holder rarity-{rarityName}">{id}</div>\n' +
    '<div class="invisible-space-holder">&nbsp;</div>\n' +
    '<div class="rarity-holder rarity-{rarityName}">{rarityName}</div>\n' +
    '</div>\n' +
    '<div class="card-footer text-center">\n' +
    '<div class="card-info">\n' +
    '{name}<br />\n' +
    '{series}\n' +
    '<i class="material-icons copyTextLine" title="Click me to get a text copy for Discord/IRC" data-id="{id}" data-name="{escapedName}" data-series="{escapedSeries}" data-rarity="{base_rarity}" data-image="{escapedImage}">file_copy</i>\n' +
    '</div>\n' +
    '</div>\n' +
    '</label>\n' +
    '</div>';

// TODO: Check if twitchToken in localStorage

let token = localStorage.getItem("twitchToken");
if (token === null) {
    let twitchURL = "https://id.twitch.tv/oauth2/authorize" +
        "?client_id=" + clientID +
        "&redirect_uri=" + encodeURI(window.location.protocol + "//" + window.location.host + "/browsertwitchauth") +
        "&response_type=token"
    ;
    $('#cards-grid').append('You need to <a href="' + twitchURL + '">Log In</a> to use this page!');
}

// TODO: Get and parse cards from /browserdata; Then add all the cards based on cardTemplate.

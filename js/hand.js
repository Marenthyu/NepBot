// from stackoverflow https://stackoverflow.com/questions/31626852/how-to-add-konami-code-in-a-website-based-on-html
// memes.
// a key map of allowed keys
let allowedKeys = {
    37: 'left',
    38: 'up',
    39: 'right',
    40: 'down',
    65: 'a',
    66: 'b'
};

// the 'official' Konami Code sequence
let konamiCode = ['up', 'up', 'down', 'down', 'left', 'right', 'left', 'right', 'b', 'a'];

// a variable to remember the 'position' the user has reached so far.
let konamiCodePosition = 0;

// add keydown event listener
document.addEventListener('keydown', function (e) {
    // get the value of the key code from the key map
    let key = allowedKeys[e.keyCode];
    // get the value of the required key from the konami code
    let requiredKey = konamiCode[konamiCodePosition];

    // compare the key with the required key
    if (key === requiredKey) {

        // move to the next key in the konami code sequence
        konamiCodePosition++;

        // if the last key is reached, activate cheats
        if (konamiCodePosition === konamiCode.length) {
            activateCheats();
            konamiCodePosition = 0;
        }
    } else {
        konamiCodePosition = 0;
    }
});

function activateCheats() {
    alert("memes");
    $("#controlpanel").removeClass("disabled").attr("href", "/teasing");
}

// Hand sorting code by xorhash
/*

 Copyright (c) 2019 xorhash

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.

 THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

*/
let inSortMode = false;

function updateOutput(e, ui) {
    // this part is rewritten from hash's original code to allow splitting
    // messages for extremely big hands (40+ cards)
    var cardIDStrings = [""];
    var sortNumStrings = [""];
    var first = true;
    var i = 0;
    var currStr = 0;
    $('#cards-grid').children('.hand-card').each(function() {
        var cardid = $(this).data('cardid');
        var sortval = i++;
        // split into new message?
        if(13 + cardIDStrings[currStr].length + sortNumStrings[currStr].length + cardid.toString().length + sortval.toString().length > 450) {
            currStr++;
            cardIDStrings[currStr] = cardid.toString();
            sortNumStrings[currStr] = sortval.toString();
        }
        else {
            if(!first) {
                cardIDStrings[currStr] += ",";
                sortNumStrings[currStr] += ",";
            }
            cardIDStrings[currStr] += cardid;
            sortNumStrings[currStr] += sortval;
        }
        first = false;
    });

    if (currStr > 0) {
        $("#sorthand-single").hide();
        var commands = [];
        for(var idx=0; idx<=currStr; idx++) {
            commands.push("!sorthand "+cardIDStrings[idx]+" "+sortNumStrings[idx]);
        }
        $("#sorthand-multiple").show().text(commands.join("\r\n")).attr("rows", commands.length + 1);
    }
    else {
        $("#sorthand-multiple").hide();
        $("#sorthand-single").show().val("!sorthand "+cardIDStrings[0]+" "+sortNumStrings[0]);
    }
}

$(document).ready( function() {
    $(".creation-date").each(function() {
        let ts = $(this).attr("data-timestamp");
        $(this).text(moment(new Date(parseInt(ts))).format('MMMM Do YYYY, h:mm:ss a'));
    });
    $("#date-display-text").text("Card creation dates are displayed in your device timezone (detected as "+moment.tz(moment.tz.guess()).zoneAbbr()+").");

    $("#sort-mode").click(function() {
        if(inSortMode) {
            $('#cards-grid').sortable('destroy');
            $('#cards-grid').unbind();
            $('#cards-grid').enableSelection();
            $('.sorthand-output').hide();
            $(this).text("Sorthand command generator");
            $("#sorthand-info").hide();
        }
        else {
            $('#cards-grid').sortable({
                placeholder: 'card-placeholder card-tcg card',
                update: updateOutput,
                items: "> .hand-card"
            });
            $('#cards-grid').disableSelection();
            updateOutput();
            $(this).text("Done");
            $("#sorthand-info").show();
        }
        inSortMode = !inSortMode;
    });
});

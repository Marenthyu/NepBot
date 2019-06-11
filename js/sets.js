let uniqid = 0;

function populateSets(destination, data, emptyString, type) {
    if ('error' in data || !('count' in data && 'sets' in data)) {
        $(destination).html("<i>Data loading error. Please try again.</i>");
        return;
    }
    if (data.count === 0) {
        $(destination).html("<i>" + emptyString + "</i>");
        return;
    }
    uniqid += 1;
    let buttons = "";
    let content = "";
    buttonTemplate='<li class="nav-item"><a class="nav-link{ACTIVE}" id="tab-{SRID}" data-toggle="tab" href="#{SRID}" role="tab" aria-controls="{SRID}" aria-selected="{FIRST}">{TITLE}</a></li>';
    let setRows = [];
    let first = true;
    for (set of data.sets) {
        let setRow = $("#set-template").clone();
        let setRowID = "set"+set.id+"-"+uniqid;
        setRow.attr("id", setRowID);
        if(first) {
            setRow.addClass("show active");
        }
        if(set.image) {
            let setImageHolder = setRow.find(".set-image");
            setImageHolder.html("<img class='lazyload rounded-circle' />");
            setImageHolder.find("img").attr("data-src", set.image).attr("alt", set.title).attr("title", set.title);
        }
        setRow.find(".set-title").text(set.name);
        setRow.find(".claim-info").text(set.numClaimsText);
        if(set.claimable) {
            let rewards = [];
            if(set.rewardPoints > 0) {
                rewards.push(set.rewardPoints+" points");
            }
            if(set.rewardPudding > 0) {
                rewards.push(set.rewardPudding+" pudding");
            }
            setRow.find(".claim-reward").text(rewards.join(", "));
        }
        else {
            setRow.find(".claim-reward-row").hide();
        }
        if(set.claimedText) {
            setRow.find(".claim-time").text(set.claimedText);
        }
        else {
            setRow.find(".claim-time-row").hide();
            setRow.find(".claim-time-linebreak").hide();
        }
        setRow.find(".claimable-status-icon").text(set.claimableIcon);
        setRow.find(".claimable-status").text(set.claimableText);
        title = (user !== "" && type !== "claimed") ? set.name + " (" + set.cardsOwned + "/" + set.totalCards + ")" : set.name;
        buttons += buttonTemplate.replace(/{SRID}/g, setRowID).replace(/{ACTIVE}/g, first ? " active" : "").replace(/{FIRST}/g, ""+first).replace(/{TITLE}/g, title);
        for (row of set.cards) {
            let cardRow = $("#card-template").clone();
            cardRow.attr("id", "card"+row.id+"-"+set.id+"-"+uniqid);
            let rarityClass = "rarity-"+row.rarity;
            cardRow.find(".card-image").attr("data-src", row.image).attr("alt", row.name).attr("title", row.name).addClass("lazyload");
            cardRow.find(".id-holder").addClass(rarityClass).text(row.id.toString());
            cardRow.find(".rarity-holder").addClass(rarityClass).text(row.rarity);
            cardRow.find(".card-name").text(row.name);
            cardRow.find(".card-series").text(row.series);
            if(user != "" && type !== "claimed") {
                // owned data
                cardRow.find(".card-footer").addClass(row.owned ? " owned" : " not-owned").prepend($('<div class="owned-icon">' + (row.owned ? "✔" : "❌") + "</div>"));
            }
            setRow.find(".set-cards").append(cardRow);
        }
        setRows.push(setRow);
        first = false;
    }
    $(destination).html("<p>" + data.count + " set(s) found.</p><ul class='nav nav-tabs' id='sets-"+uniqid+"' role='tablist'>" + buttons + "</ul><div class='tab-content' id='sets-"+uniqid+"Content'></div>");
    for(let setRow of setRows) {
        setRow.appendTo("#sets-"+uniqid+"Content");
    }
}

let currentRequest = null;

function search() {
    $("#search-results-holder").show();
    if ($("#query").val().length >= 3) {
        $("#search-results-holder").html("<i>Loading...</i>");
        currentRequest = $.get({
            url: "/setsdata",
            data: {type: $("input[name='type']:checked").val(), q: $("#query").val(), user: user},
            success: function (data) {
                currentRequest = null;
                populateSets("#search-results-holder", data, "No results found for those search terms.", "search");
            },
            beforeSend: function () {
                if (currentRequest != null) {
                    currentRequest.abort();
                }
            },
            dataType: 'json'
        });
    }
    else {
        $("#search-results-holder").html("<b>Please enter a search query of 3 characters or more.</b>");
    }
}

if (user !== "") {
    $("#your-set-progress").show();
    $.get("/setsdata", {type: "progress", user: user}, function (data) {
        populateSets("#set-progress-holder", data, "This user doesn't own any cards in any unclaimed sets right now.", "progress");
    }, 'json');
    $.get("/setsdata", {type: "claimed", user: user}, function (data) {
        populateSets("#claimed-sets-holder", data, "This user hasn't claimed any sets yet.", "claimed");
    }, 'json');
}
$("#load-allsets-button").click(function () {
    $("#all-sets-holder").html("<i>Loading...</i>");
    $.get("/setsdata", {type: "allsets", user: user}, function (data) {
        populateSets("#all-sets-holder", data, "There are no unclaimed sets in the system right now.", "all");
    }, 'json');
});
$("#query").on('input propertychange paste', search);
$("input[name='type']").on('change', search);
$(document).ajaxStop(function () {
    $("img.lazyload").lazyload({
        effect: "fadeIn"
    }).removeClass("lazyload");
});
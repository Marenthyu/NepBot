if (user === "") {
    $("#navbartitle").html("Waifu TCG");
    $("#main-nav .nav-link.user-required").addClass("disabled").attr('href', '').css('cursor', 'default');
    $("#inputname").val("");
}

$(".cardExtraInfo").on('show.bs.collapse', function() {
    $("#card"+($(this).attr("data-cardid"))+"Toggler").text("expand_less");
});

$(".cardExtraInfo").on('hide.bs.collapse', function() {
    $("#card"+($(this).attr("data-cardid"))+"Toggler").text("expand_more");
});

let settingStored = false;
if (typeof(Storage) !== "undefined") {
    // enable storing the setting
    if('darkMode' in window.localStorage) {
        settingStored = true;
        if(window.localStorage.darkMode === 'yes') {
            $("html").addClass("dark-mode");
        }
    }
    else {
        // check if they want dark mode explicitly, else set light mode for now
        if(window.matchMedia("(prefers-color-scheme: dark)").matches) {
            $("html").addClass("dark-mode");
        }
    }
} else {
    // query preference and only set dark mode if yes.
    if(window.matchMedia("(prefers-color-scheme: dark)").matches) {
        $("html").addClass("dark-mode");
    }
}

$(document).ready( function() {
    if (typeof(Storage) !== "undefined") {
        // enable toggle
        $("#darkmode-toggle").click(function() {
            if(settingStored) {
                if(window.localStorage.darkMode === 'yes') {
                    $("#darkmode-preference").html("Your current preference is set to: <b>dark mode</b>");
                    $("#darkmode-yes").prop("checked", true);
                }
                else {
                    $("#darkmode-preference").html("Your current preference is set to: <b>light mode</b>");
                    $("#darkmode-no").prop("checked", true);
                }
            }
            else {
                if(window.matchMedia("(prefers-color-scheme: dark)").matches) {
                    $("#darkmode-preference").html("Your preference has been autodetected as <b>dark mode</b> based on system settings, you can override it here.");
                    $("#darkmode-yes").prop("checked", true);
                }
                else if(window.matchMedia("(prefers-color-scheme: light)").matches) {
                    $("#darkmode-preference").html("Your preference has been autodetected as <b>light mode</b> based on system settings, you can override it here.");
                    $("#darkmode-no").prop("checked", true);
                }
                else {
                    $("#darkmode-preference").html("You are currently being shown <b>light mode</b> as we can't detect a preference from your browser, you can make a choice here.");
                    $("#darkmode-no").prop("checked", true);
                }
            }

            $("#darkmode-modal").modal();
        });

        $("#darkmode-save-button").click(function() {
            if($("#darkmode-yes").prop("checked")) {
                window.localStorage.setItem("darkMode", "yes");
                $("html").addClass("dark-mode");
            }
            else {
                window.localStorage.setItem("darkMode", "no");
                $("html").removeClass("dark-mode");
            }
            settingStored = true;
            $("#darkmode-modal").modal('hide');
        });
    } else {
        // make the toggle just an actual toggle that doesn't save anything
        $("#darkmode-toggle").click(function() {
            $("html").toggleClass("dark-mode");
        });
    }
});

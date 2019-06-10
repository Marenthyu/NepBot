if (user === "null") {
    $("#navbartitle").html("Waifu TCG");
    $(".nav-link").addClass("disabled").attr('href', '').css('cursor', 'default');
    $("#inputname").val("");
}

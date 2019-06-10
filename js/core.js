if (user === "null") {
    $("#navbartitle").html("Waifu TCG");
    $(".nav-link").addClass("disabled").attr('href', '').css('cursor', 'default');
    $("#inputname").val("");
}

$(".cardExtraInfo").on('show.bs.collapse', function() {
    $("#card"+($(this).attr("data-cardid"))+"Toggler").text("expand_less");
});

$(".cardExtraInfo").on('hide.bs.collapse', function() {
    $("#card"+($(this).attr("data-cardid"))+"Toggler").text("expand_more");
});

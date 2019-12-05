(function () {
    var fullHash = window.location.hash;
    if (fullHash == null) return;
    var locationParts = fullHash.split('_');
    window.location.hash = '';
    if (locationParts.length > 1 && locationParts[0] !== "") {
        $(locationParts[0]).one('shown.bs.collapse', function () {
            window.location.hash = fullHash;
            $(document.body).scrollTop($(fullHash).offset().top + 60);
        });
        $(locationParts[0]).collapse('show');
    }
})();

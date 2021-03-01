$(window).on('load', () => {
    if (document.location.hash) {
        console.log("Has hash");
        let token = '';
        let parts = document.location.hash.substring(1).split("&");
        for (let part of parts) {
            if (part.startsWith('access_token=')) {
                token = part.split('=')[1];
                break;
            }
        }
        if (token === '') {
            console.log("No token");
            $('#authcallbackoutput').append('<br>No token found in URL. Please go back and try again.');
        } else {
            console.log("Token");
            $('#authcallbackoutput').append('<br>Token found!<br>Redirecting you back....');
            localStorage.setItem("twitchToken", token);
            window.location.href = "/browser";
        }
    } else {
        $('#authcallbackoutput').text('No token found in URL. Please go back and try again.');
    }

});

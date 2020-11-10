$('#authcallbackoutput').text('Checking for Token...');
let token = '';
function urlBase64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - base64String.length % 4) % 4);
    const base64 = (base64String + padding)
        .replace(/\-/g, '+')
        .replace(/_/g, '/');

    const rawData = window.atob(base64);
    const outputArray = new Uint8Array(rawData.length);

    for (let i = 0; i < rawData.length; ++i) {
        outputArray[i] = rawData.charCodeAt(i);
    }
    return outputArray;
}
async function signup() {
    console.log("Button clicked!");
    if ('serviceWorker' in navigator) {
        let registration;
        try {
            registration = await navigator.serviceWorker.register('/sw.js', {
                scope: '/'
            });
            console.log('ServiceWorker registration successful with scope: ', registration.scope);
            $('#authcallbackoutput').text('Please allow Notifications now!');
        } catch (e) {
            console.error(e);
            $('#authcallbackoutput').text("There was an error installing the Service Worker - Are you blocking scripts or something?");
            return
        }
        if (!Reflect.has(window, "Notification")) {
            $('#authcallbackoutput').text('Sorry! Your Browser seems to not support Notifications. :/ - Please use a different browser, if possible!');
            return
        }
        let perm = await Notification.requestPermission();
        if (perm) {
            $('#authcallbackoutput').text('Thank you! Please be patient while we do some magic...');
        } else {
            $('#authcallbackoutput').text('You denied Notification Permissions. Without them, this can\'t work! Please change your settings to allow them and reload this page.');
            return
        }
        const subscribeOptions = {
            userVisibleOnly: true,
            applicationServerKey: urlBase64ToUint8Array(
                publicKey
            )
        };
        let sub = await registration.pushManager.subscribe(subscribeOptions);
        console.log("Registration info:");
        console.log(JSON.stringify(sub));
        let response = await fetch("pushregistration", {
            method: "POST",
            body: JSON.stringify(sub),
            headers: {
                Authorization: "Bearer " + token
            }
        })
        let resptext;
        try {
            resptext = await response.text();
        } catch (e) {
            $('#authcallbackoutput').text("Something went wrong. Please tell marenthyu: " + JSON.stringify(e));
            console.error(e);
            return
        }
        console.log(JSON.stringify(response.status));
        if (response.status !== 200) {
            $('#authcallbackoutput').text("This went wrong: " + JSON.stringify(resptext) + " - Please try again.");
        } else {
            $('#authcallbackoutput').text("Success! You should get a test notification momentarily. Feel free to close this window afterwards.");
        }
    } else {
        console.log("No ServiceWorker capability.");
    }
}

console.log("Loading");
$(window).on('load', () => {
    if (document.location.hash) {
        console.log("Has hash");
        let parts = document.location.hash.substring(1).split("&");
        for (let part of parts) {
            if (part.startsWith('id_token=')) {
                token = part.split('=')[1];
                break;
            }
        }
        if (token === '') {
            console.log("No token");
            $('#authcallbackoutput').append('<br>No token found in URL. Please go back and try again.');
        } else {
            console.log("Token");
            $('#authcallbackoutput').append('<br>Token found!<br>Please click here to now register for Push Notifications: <input type="button" onclick="signup()" value="Subscribe!">');
        }
    } else {
        $('#authcallbackoutput').text('No token found in URL. Please go back and try again.');
    }

});

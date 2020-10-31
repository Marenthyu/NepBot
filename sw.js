self.addEventListener('install', function (event) {
    // Perform install steps
    console.log("I have been installed!");
});

self.addEventListener('activate', event => {
    console.log('Have been activated now!');
});

self.addEventListener('fetch', function (event) {
    // we don't want to do any caching, so this can be empty
});
self.addEventListener('push', event => {
    console.log("Push Event received!");
    let data = event.data.json();
    let actions = [];
    event.waitUntil(self.registration.showNotification("Waifu TCG", {
        image: data['image'],
        // icon: "https://marenthyu.de/danboorutool/img/nep.png",
        body: data['message'],
        actions: actions,
        data: data
    }).then(() => {
    }));
})
self.addEventListener('notificationclick', function(event) {
    let data = event.notification.data;
    event.waitUntil(clients.openWindow(data['openurl']));
    event.notification.close();
});

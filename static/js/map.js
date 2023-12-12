var map;

function removeMap(){
    // remove the map if one exists already
    if (map != undefined) {
        map.remove();
    }
}

// Function to create a marker and set up the hover event
function createHoverMarker(lat, lng, popupText) {
    var marker = L.marker([lat, lng]).addTo(map);
    marker.bindPopup(popupText);

    marker.on('mouseover', function(e) {
        this.openPopup();
    });
    marker.on('mouseout', function(e) {
        this.closePopup();
    });
}

function initializeMap(data) {
    map = L.map('map').setView([40.7128, -74.0060], 13);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap contributors'
    }).addTo(map);

    // a loop that creates a marker for each data point
    for (var i = 0; i < data.length; i++) {
        var lng = data[i][0];
        var lat = data[i][1];
        var revenue = data[i][2];
        var tip = data[i][3];
        var price = Math.round(revenue + tip);
        var popupText = "$" + price;
        createHoverMarker(lat, lng, popupText);
    }

    
    // createHoverMarker(40.785091, -73.968285, "$15");
    // createHoverMarker(40.712776, -74.005974, "$20");
    // createHoverMarker(40.706001, -73.996563, "$30");
}
var map;

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

function initializeMap() {
    map = L.map('map').setView([40.7128, -74.0060], 13);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap contributors'
    }).addTo(map);

    // // Adding markers
    // var marker1 = L.marker([40.785091, -73.968285]).addTo(map);
    // var marker2 = L.marker([40.712776, -74.005974]).addTo(map);
    // var marker3 = L.marker([40.706001, -73.996563]).addTo(map);

    // Optionally, you can add popups to these markers
    createHoverMarker(40.785091, -73.968285, "$15");
    createHoverMarker(40.712776, -74.005974, "$20");
    createHoverMarker(40.706001, -73.996563, "$30");
}
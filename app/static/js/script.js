var cartodb = L.tileLayer(
  "http://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png"
);
var osm = L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png");
var satellite = L.tileLayer(
  "http://{s}.google.com/vt/lyrs=s&x={x}&y={y}&z={z}",
  { subdomains: ["mt0", "mt1", "mt2", "mt3"] }
);

var busIcon = L.icon({
  iconUrl: "static/images/bus.jpg",
  iconSize: [25, 25],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
});

var stopIcon = L.icon({
  iconUrl: "static/images/stop.png",
  iconSize: [25, 25],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
});

var baseMaps = {
  Cartodbpositron: cartodb,
  OpenStreetMap: osm,
  "Google Satellite": satellite,
};

var map = L.map("map", {
  center: [44.731808, -93.238322],
  zoom: 14,
  layers: [cartodb],
});

function add_geojson(group, feature) {
  L.geoJSON(feature, {
    style: feature["style"],
  }).addTo(group);
}

function check_congestion_level(level) {
  if (level < 2) {
    return "green";
  } else if (level == 2) {
    return "yellow";
  } else {
    return "red";
  }
}

var layerControl;

var source = new EventSource("stream");
source.addEventListener(
  "message",
  function (e) {
    collection_list = JSON.parse(e.data);
    console.log(collection_list);

    if (layerControl != undefined) {
      map.removeControl(layerControl);
      map.eachLayer(function (layer) {
        if (layer != cartodb && layer != osm && layer != satellite) {
          map.removeLayer(layer);
        }
      });
    }

    let overlays = {};

    collection_list.forEach((element) => {
      if (element != null) {
        var gj = L.geoJSON(element, {
          pointToLayer: function (feature, latlng) {
            switch (feature.object) {
              case "Bus":
                return new L.Marker(latlng, { icon: busIcon });
              case "Stop":
                return new L.Marker(latlng, { icon: stopIcon });
            }
          },
          style: function (feature) {
            switch (feature.object) {
              case "Path":
                return {
                  color:
                    "#" + Math.floor(Math.random() * 16777215).toString(16),
                };
              case "Segment":
                return {
                  color: check_congestion_level(
                    feature.properties.congestion_level
                  ),
                };
            }
          },
          onEachFeature: function (feature, layer) {
            layer.bindPopup(feature.Popup);
          },
        }).addTo(map);

        overlays[element.properties.trip_id] = gj;
      }
    });

    layerControl = L.control
      .layers(baseMaps, overlays, { position: "topleft" })
      .addTo(map);
  },
  false
);

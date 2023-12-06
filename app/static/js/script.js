var cartodb = L.tileLayer(
  "http://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png"
);
var osm = L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png");
var satellite = L.tileLayer(
  "http://{s}.google.com/vt/lyrs=s&x={x}&y={y}&z={z}",
  { subdomains: ["mt0", "mt1", "mt2", "mt3"] }
);

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
      var gj = L.geoJSON(element, {
        pointToLayer: function (feature, latlng) {
          switch (feature.object) {
            case "Bus":
              return new L.CircleMarker(latlng, { radius: 4, color: "red" });
            case "Stop":
              return new L.CircleMarker(latlng, { radius: 6, color: "blue" });
          }
        },
        style: function (feature) {
          switch (feature.object) {
            case "Path":
              return {
                color: "#" + Math.floor(Math.random() * 16777215).toString(16),
              };
            case "Segment":
              return {
                color: check_congestion_level(
                  feature.properties.congestion_level
                ),
              };
          }
        },
      }).addTo(map);

      overlays[element.properties.trip_id] = gj;
    });

    layerControl = L.control
      .layers(baseMaps, overlays, { position: "topleft" })
      .addTo(map);
  },
  false
);

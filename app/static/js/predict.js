var source = new EventSource("predict");
source.addEventListener(
  "message",
  function (e) {
    arr = JSON.parse(e.data);
    table = document.getElementById("tb");
    arr.forEach((element) => {
      let row = document.createElement("tr");
      let html =
        "<td>" +
        element.timestamp +
        "</td><td>" +
        element.trip_id +
        "</td><td>" +
        element.route_id +
        "</td><td>" +
        element.label +
        "</td><td>" +
        element.congestion_level +
        "</td><td>" +
        element.predict_time +
        "</td><td>" +
        element.prediction +
        "</td>";
      row.innerHTML = html;
      table.prepend(row);
    });
  },
  false
);

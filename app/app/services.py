from .db import connect_db
from .models import Network
from .kit import *
import folium
from folium import plugins
import pandas as pd
from .gtfsrt import parser


COLORS_SET2 = [
    "#66c2a5",
    "#fc8d62",
    "#8da0cb",
    "#e78ac3",
    "#a6d854",
    "#ffd92f",
    "#e5c494",
    "#b3b3b3",
]


def initMap():
    m = folium.Map(location=[39.749563, -104.97364], zoom_start=13, tiles="openstreetmap")

    return m


def mapHTML():    
    html = """
        {% extends "base.html" %} 
        {% block title %}
            Map
        {% endblock %}

        {% block content %}
            <div id='map'> {{ map | safe }} </div>      
        {% endblock %}
    """
    return html


def mapTag(map):
    plugins.Fullscreen(                                                         
        position                = "topleft",                                   
        title                   = "Open full-screen map",                       
        title_cancel            = "Close full-screen map",                      
        # force_separate_button   = True,                                         
    ).add_to(map) 

    return map.get_root()._repr_html_()


def getData() -> Network:
    """
    Fetch data from Mongo database and assign to Network model
    Return a Network model object
    """
    db = connect_db()

    network = Network(
        stop_times=pd.DataFrame(db["stop_times"].find({}, { "_id": 0}).sort({ "$natural": 1 })),
        routes=pd.DataFrame(db['routes'].find({}, {"_id": 0}).sort({ "$natural": 1 })),
        shapes=pd.DataFrame(db["shapes"].find({}, {"_id": 0}).sort({ "$natural": 1 })),
        stops=pd.DataFrame(db["stops"].find({}, { "_id": 0}).sort({ "$natural": 1 })),
        trips=pd.DataFrame(db["trips"].find({}, { "_id": 0}).sort({ "$natural": 1 })),
    )
    network.vehicle_positions = parser.updateVehiclePositions()

    return network


def map_routes(map: folium.Map):
    """
    Showing the given routes and (optionally) their stops to the folium map.
    If any of the given route IDs are not found in the network, then raise a ValueError.
    """
    # Initialize Network model
    network = getData()

    route_ids = network.routes.route_id.iloc[:]

    # Create route colors
    n = len(route_ids)
    colors = [COLORS_SET2[i % len(COLORS_SET2)] for i in range(n)]

    # Collect route bounding boxes to set map zoom later
    bboxes = []

    # Create a feature group for each route and add it to the map
    for i, route_id in enumerate(route_ids):
        collection = routes_to_geojson(network=network, route_ids=[route_id])

        # Use route short name for group name if possible; otherwise use route ID
        route_name = route_id
        for f in collection["features"]:
            if "route_short_name" in f["properties"]:
                route_name = f["properties"]["route_short_name"]
                break

        group = folium.FeatureGroup(name=f"Route {route_name}")
        color = colors[i]

        for f in collection["features"]:
            prop = f["properties"]

            # Add stop
            if f["geometry"]["type"] == "Point":
                lon, lat = f["geometry"]["coordinates"]
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=8,
                    fill=True,
                    color=color,
                    weight=1,
                    popup=folium.Popup(make_html(prop)),
                ).add_to(group)

            # Add path
            else:
                prop["color"] = color
                path = folium.GeoJson(
                    f,
                    name=prop["route_short_name"],
                    style_function=lambda x: {"color": x["properties"]["color"]},
                )
                path.add_child(folium.Popup(make_html(prop)))
                path.add_to(group)
                bboxes.append(sg.box(*sg.shape(f["geometry"]).bounds))

        group.add_to(map)

    folium.LayerControl().add_to(map)

    # Fit map to bounds
    bounds = so.unary_union(bboxes).bounds
    bounds2 = [bounds[1::-1], bounds[3:1:-1]]  # Folium expects this ordering
    map.fit_bounds(bounds2)


def map_realtime(network: Network, map: folium.Map):
    route_ids = network.vehicle_positions["route_id"].iloc[:]

    # Create route colors
    n = len(route_ids)
    colors = [COLORS_SET2[i % len(COLORS_SET2)] for i in range(n)]

    # Collect route bounding boxes to set map zoom later
    bboxes = []

    # Create a feature group for each route and add it to the map
    for i, route_id in enumerate(route_ids):
        if route_id != None:
            collection = routes_to_geojson(network=network, route_ids=[route_id])

            # Use route short name for group name if possible; otherwise use route ID
            route_name = route_id
            for f in collection["features"]:
                if "route_short_name" in f["properties"]:
                    route_name = f["properties"]["route_short_name"]
                    break

            group = folium.FeatureGroup(name=f"Route {route_name}")
            color = colors[i]

            for f in collection["features"]:
                prop = f["properties"]

                # Add stop
                if f["geometry"]["type"] == "Point":
                    lon, lat = f["geometry"]["coordinates"]
                    folium.CircleMarker(
                        location=[lat, lon],
                        radius=3,
                        fill=True,
                        color=color,
                        weight=1,
                        popup=folium.Popup(make_html(prop)),
                    ).add_to(group)

            for _, v in network.vehicle_positions.iterrows():
                attr = {k:v for k,v in dict(v).items() if k in ["route_id", "latitude", "longitude"]}
                folium.CircleMarker(location=[v["latitude"], v["longitude"]], 
                                    popup=make_html(attr),
                                    radius=8, color='red').add_to(group)
            group.add_to(map)

    folium.LayerControl().add_to(map)

    # Fit map to bounds
    bounds = so.unary_union(bboxes).bounds
    bounds2 = [bounds[1::-1], bounds[3:1:-1]]  # Folium expects this ordering
    map.fit_bounds(bounds2)
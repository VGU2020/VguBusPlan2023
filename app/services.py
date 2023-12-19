import datetime, time
import gtfs_realtime_pb2
import urllib.request
import pandas as pd
import json2html
import zipfile
import requests
import tempfile
import json
import os

from db import connect_db, insert
from models import Bus, Network, Stop, Path, Segment
from config import CONSUMER_CONFIG, PRODUCER_CONFIG
from confluent_kafka import Consumer, Producer
from predict import *


def consumer_client():
    return Consumer(CONSUMER_CONFIG)


def producer_client():
    return Producer(PRODUCER_CONFIG)


def fetch_train_data():
    mongodb = connect_db()
    data = pd.DataFrame(mongodb['vehicle_position'].find({}, {"_id": 0}))
    data = data.astype({'label':'int'})
    return data


def fetch_realtime_data(url) -> pd.DataFrame:
    """
    Get realtime data
    Return as pandas DataFrame
    """
    try:
        response = urllib.request.urlopen(url)
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.read())
        vehiclePositions = feed.entity

        trip_ids = []
        route_ids = []
        labels = []
        latitudes = []
        longitudes = []
        speeds = []
        stop_ids = []
        current_stop_seqs = []
        congestion_levels = []
        current_status = []
        timestamps = []

        for vp in vehiclePositions:
            if vp.vehicle.stop_id:
                trip_ids.append(str(vp.vehicle.trip.trip_id))
                route_ids.append(int(vp.vehicle.trip.route_id))
                labels.append(str(vp.vehicle.vehicle.label))
                latitudes.append(float(vp.vehicle.position.latitude))
                longitudes.append(float(vp.vehicle.position.longitude))
                current_stop_seqs.append(int(vp.vehicle.current_stop_sequence))
                stop_ids.append(int(vp.vehicle.stop_id))
                current_status.append(int(vp.vehicle.current_status))
                speeds.append(float(vp.vehicle.position.speed))
                congestion_levels.append(int(vp.vehicle.congestion_level)) if not vp.vehicle.congestion_level else None
                timestamps.append(str(datetime.datetime.fromtimestamp(vp.vehicle.timestamp)))

        df_vehicle_positions = pd.DataFrame({'trip_id': trip_ids, 'route_id': route_ids, 'label': labels, "stop_id": stop_ids, "speed": speeds,
                                            'latitude': latitudes, 'longitude': longitudes, 'congestion_level': congestion_levels, 
                                            'current_stop_sequence': current_stop_seqs, 'current_status': current_status, 'timestamp': timestamps})

        return df_vehicle_positions
    except Exception:
        raise ValueError("Unknown address or invalid url")


def fetch_static_data(url) -> Network:
    """
    Download data and assign to Network model
    Return a Network model object
    """
    try:
        response = requests.get(url)

        if response.headers["Content-Type"] != "application/zip":
            raise ValueError("Not a zip file url")
    except Exception:
        raise ValueError("Unknown address or invalid url")

    temp = tempfile.NamedTemporaryFile(delete=False)
    temp.write(response.content)
    temp.close()

    zfile = zipfile.ZipFile(temp.name)

    network = Network(
        stop_times=pd.read_csv(zfile.open("stop_times.txt")),
        shapes=pd.read_csv(zfile.open("shapes.txt")),
        stops=pd.read_csv(zfile.open("stops.txt")),
        trips=pd.read_csv(zfile.open("trips.txt")),
    )

    zfile.close()
    os.unlink(temp.name)

    return network


def make_html(d: dict) -> str:
    """
    Convert the given dictionary into an HTML table (string) with
    two columns: keys of dictionary, values of dictionary.
    """
    return json2html.json2html.convert(
        json=d, table_attributes="class='table table-condensed table-hover'"
    )


def obj_to_geojson(network, vehicle_positions):
    coll_list = list()
    
    for _, v in vehicle_positions.iterrows():
        # Bus feature
        bus = Bus(
            trip_id=v.trip_id,
            route_id=v.route_id,
            label=v.label,
            latitude=v.latitude,
            longitude=v.longitude,
            speed=v.speed,
            current_stop_sequence=v.current_stop_sequence,
            stop_id=v.stop_id,
            current_status=v.current_status,
            timestamp=v.timestamp
        )
        bus_feature = bus.to_geojson()
        bus_feature["Popup"] = make_html(bus_feature["properties"])

        # Stop feature
        station = network.stops.loc[network.stops.stop_id == v.stop_id]
        stop = Stop(
            stop_id=int(station.stop_id.to_string(index=False)),
            stop_name=station.stop_name.to_string(index=False),
            stop_lat=float(station.stop_lat.to_string(index=False)),
            stop_lon=float(station.stop_lon.to_string(index=False))
        )
        stop_feature = stop.to_geojson()
        stop_feature["Popup"] = make_html(stop_feature["properties"])

        # Path
        shape_id = network.trips.loc[network.trips.trip_id == v.trip_id, ["shape_id"]].shape_id.to_string(index=False)
        shape_coords = network.shapes.sort_values(["shape_pt_sequence", "shape_dist_traveled"]).loc[network.shapes.shape_id == shape_id, ["shape_pt_lon", "shape_pt_lat"]].values.tolist()

        path = Path(
            trip_id=v.trip_id,
            route_id=v.route_id,
            pathway=shape_coords
        )
        path_feature = path.to_geojson()
        path_feature["Popup"] = make_html(path_feature["properties"])


        # Segment
        dist_travelled = network.stop_times.loc[(network.stop_times.stop_sequence == v.current_stop_sequence) & (network.stop_times.trip_id == v.trip_id), ["shape_dist_traveled"]].shape_dist_traveled
        segment_feature = None
        if not dist_travelled.empty:
            segment_coords = network.shapes.sort_values(["shape_pt_sequence", "shape_dist_traveled"]).loc[(network.shapes.shape_id == shape_id) & (network.shapes.shape_dist_traveled >= float(dist_travelled.to_string(index=False))), ["shape_pt_lon", "shape_pt_lat"]].head(10).values.tolist()
            segment = Segment(
                trip_id=v.trip_id,
                congestion_lv=v.congestion_level,
                seg_coords=segment_coords
            )
            segment_feature = segment.to_geojson()
            segment_feature["Popup"] = make_html(segment_feature["properties"])


        # Collection
        collection = {
            "type": "FeatureCollection",
            "properties": {
                "trip_id": v.trip_id,
                "route_id": v.route_id,
            },
            "features": [],
        }

        collection["features"].append(bus_feature)
        collection["features"].append(stop_feature)
        collection["features"].append(path_feature)

        if not dist_travelled.empty:
            segment_coords = network.shapes.sort_values(["shape_pt_sequence", "shape_dist_traveled"]).loc[(network.shapes.shape_id == shape_id) & (network.shapes.shape_dist_traveled >= float(dist_travelled.to_string(index=False))), ["shape_pt_lon", "shape_pt_lat"]].head(10).values.tolist()
            segment = Segment(
                trip_id=v.trip_id,
                congestion_lv=v.congestion_level,
                seg_coords=segment_coords
            )
            segment_feature = segment.to_geojson()
            segment_feature["Popup"] = make_html(segment_feature["properties"])
            collection["features"].append(segment_feature) 

        coll_list.append(collection)
    return coll_list


def publisher(static_url: str, realtime_url: str):
    producer = producer_client()
    network = fetch_static_data(static_url)

    while 1:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            data = fetch_realtime_data(realtime_url)
            sample = fetch_train_data()
            model = train(sample)
            forecast = predict(model, data)
            insert(data)
            collections_list = obj_to_geojson(network=network, vehicle_positions=data)
            message = json.dumps(collections_list)
            prediction = json.dumps(forecast.to_dict("records"))
            producer.produce("Bus", message.encode("utf_8"))
            producer.produce("RealTime", prediction.encode("utf_8"))
            print(len(collections_list))

        except KeyboardInterrupt:
            break

        time.sleep(60 - time.time() % 60)


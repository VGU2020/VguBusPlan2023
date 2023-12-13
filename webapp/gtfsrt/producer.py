#!/usr/bin/env python

import datetime, time, json
import gtfs_realtime_pb2 
import urllib.request
import pandas as pd

from webapp.models import Bus, Network, Stop, Path, Segment
from webapp.db import connect_db

from confluent_kafka import Producer


def fetch_realtime_data(url) -> pd.DataFrame:
    """
    Get realtime data
    Return as pandas DataFrame
    """
    feed = gtfs_realtime_pb2.FeedMessage()

    response = urllib.request.urlopen(url) 
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


def fetch_static_data() -> Network:
    """
    Fetch data from Mongo database and assign to Network model
    Return a Network model object
    """
    db = connect_db()

    network = Network(
        stop_times=pd.DataFrame(db["stop_times"].find({}, { "_id": 0}).sort({ "$natural": 1 })),
        shapes=pd.DataFrame(db["shapes"].find({}, {"_id": 0}).sort({ "$natural": 1 })),
        stops=pd.DataFrame(db["stops"].find({}, { "_id": 0}).sort({ "$natural": 1 })),
        trips=pd.DataFrame(db["trips"].find({}, { "_id": 0}).sort({ "$natural": 1 })),
    )

    return network


def obj_to_json(network, vehicle_positions):
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


        # Stop feature
        station = network.stops.loc[network.stops.stop_id == v.stop_id]
        stop = Stop(
            stop_id=int(station.stop_id.to_string(index=False)),
            stop_name=station.stop_name.to_string(index=False),
            stop_lat=float(station.stop_lat.to_string(index=False)),
            stop_lon=float(station.stop_lon.to_string(index=False))
        )
        stop_feature = stop.to_geojson()


        # Path
        shape_id = network.trips.loc[network.trips.trip_id == v.trip_id, ["shape_id"]].shape_id.to_string(index=False)
        shape_coords = network.shapes.sort_values(["shape_pt_sequence", "shape_dist_traveled"]).loc[network.shapes.shape_id == shape_id, ["shape_pt_lon", "shape_pt_lat"]].values.tolist()

        path = Path(
            trip_id=v.trip_id,
            route_id=v.route_id,
            pathway=shape_coords
        )
        path_feature = path.to_geojson()

        # Segment
        segment_coords = network.shapes.sort_values(["shape_pt_sequence", "shape_dist_traveled"]).loc[network.shapes.shape_id == shape_id, ["shape_pt_lon", "shape_pt_lat"]].head(10).values.tolist()
        segment = Segment(
            trip_id=v.trip_id,
            congestion_lv=v.congestion_level,
            seg_coords=segment_coords
        )
        segment_feature = segment.to_geojson()

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
        collection["features"].append(segment_feature)

        coll_list.append(collection)
    return coll_list


def producer_run():
    # Create Producer instance
    producer = Producer({
        "bootstrap.servers": "pkc-312o0.ap-southeast-1.aws.confluent.cloud:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "WJJNVHLNUDCYW3O6",
        "sasl.password": "WDC0hL9zM7IVxAFMvLftqcMuIFppxGbEFNbbukFqfT8Y3jmZLjl0iLXuzp7dWVzO",
    })

    network = fetch_static_data()

    while 1:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            data = fetch_realtime_data('https://srv.mvta.com/infoPoint/GTFS-realtime.ashx?Type=VehiclePosition')
            collections_list = obj_to_json(network=network, vehicle_positions=data)
            print(len(collections_list))
            message = json.dumps(collections_list)
            producer.produce("bus_locations", message.encode("utf_8"))
        except KeyboardInterrupt:
            break

        time.sleep(60 - time.time() % 60)


producer_run()
import datetime, json
import gtfs_realtime_pb2
import urllib.request
import pandas as pd
from predict import *
from services import fetch_train_data

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

data = fetch_train_data()
dt = fetch_realtime_data("https://srv.mvta.com/infoPoint/GTFS-realtime.ashx?Type=VehiclePosition")

module = train(data)
z = predict(model=module, realtime_df=dt)
print(z.to_string(index=False))
# print(z.dtypes)
# a = json.dumps(z.to_dict("records"))
# print(z.to_dict("records"))
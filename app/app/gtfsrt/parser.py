import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta
from app.gtfsrt import gtfs_realtime_pb2
from protobuf_to_dict import protobuf_to_dict
import urllib.request


def businessday_to_datetime(date=None, time=None):
        try:
            res = datetime.strptime(date, '%Y%m%d') if date else date
            hr = int(time[:2])
            if hr >= 24:
                res = res + timedelta(days=1)
                hr -= 24
            res = res + timedelta(hours=hr, minutes=int(time[3:5]), seconds=int(time[6:8]))
            return res
        except:
            return None
        

def updateTrips() -> pd.DataFrame:
    """
    Trip updates
    Return as DataFrame
    """

    feed = gtfs_realtime_pb2.FeedMessage()

    # Trip
    response = urllib.request.urlopen('https://srv.mvta.com/infoPoint/GTFS-realtime.ashx?Type=TripUpdate') 
    feed.ParseFromString(response.read())
    tripUpdates = protobuf_to_dict(feed)
    print("Trip updates      : {}".format(len(tripUpdates['entity'])))

    trip_ids = []
    route_ids = []
    direction_ids = []
    vehicles = []
    stop_sequences = []
    arrival_times = []
    arrival_delays = []
    departure_times = []
    departure_delays = []
    timestamps = []

    timestamp = datetime.fromtimestamp(tripUpdates['header']['timestamp'])

    for tu in tripUpdates['entity']:
        trip_update = tu['trip_update']
        vehicle = trip_update['vehicle']['label'] if 'vehicle' in trip_update else None
        trip = trip_update['trip']
        trip_id = trip['trip_id']
        route_id = trip['route_id']
        direction_id = int(trip['direction_id']) if 'direction_id' in trip else None

        for stu in trip_update['stop_time_update'] if 'stop_time_update' in trip_update else []:
            stop_sequence = stu['stop_sequence']

            if 'arrival' in stu:
                arr = stu['arrival']
                arrival_time = arr['time'] if 'time' in arr else None
                arrival_time = datetime.fromtimestamp(arrival_time)
                arrival_delay = arr['delay'] if 'delay' in arr else None
            else:
                arrival_time = None
                arrival_delay = None

            if 'departure' in stu:
                dep = stu['departure']
                departure_time = dep['time'] if 'time' in dep else None
                departure_time = datetime.fromtimestamp(departure_time)
                departure_delay = dep['delay'] if 'delay' in dep else None
            else:
                departure_time = None
                departure_delay = None

            ###
            trip_ids.append(trip_id)
            route_ids.append(route_id)
            direction_ids.append(direction_id)
            vehicles.append(vehicle)
            stop_sequences.append(stop_sequence)
            arrival_times.append(arrival_time)
            arrival_delays.append(arrival_delay)
            departure_times.append(departure_time)
            departure_delays.append(departure_delay)
            timestamps.append(timestamp)
            ###

    df_trip_updates = pd.DataFrame({'trip_id': trip_ids, 'route_id': route_ids, 
                                    'direction_id': direction_ids, 'vehicle': vehicles,
                                    'stop_sequence': stop_sequences, 'arrival_time': arrival_times,
                                    'arrival_delay': arrival_delays, 'departure_time': departure_times,
                                    'departure_delay': departure_delays, 'timestamp': timestamps})
    
    return df_trip_updates


def updateVehiclePositions() -> gpd.GeoDataFrame:
    """
    Vehicle Positions updates
    Return as DataFrame
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    
    # Vehicle Position
    response = urllib.request.urlopen('https://srv.mvta.com/infoPoint/GTFS-realtime.ashx?Type=VehiclePosition') 
    feed.ParseFromString(response.read())
    vehiclePositions = protobuf_to_dict(feed)
    print("Vehicle positions : {}".format(len(vehiclePositions['entity'])))

    trip_ids = []
    route_ids = []
    direction_ids = []
    latitudes = []
    longitudes = []
    current_stop_seqs = []
    timestamps = []
    labels = []

    for vp in vehiclePositions['entity']:
        vehicle = vp['vehicle']
        trip = vehicle['trip'] if "trip" in vehicle else None
        trip_id = trip['trip_id'] if trip else None
        route_id = trip['route_id'] if trip else None
        direction_id = int(trip['direction_id']) if trip and 'direction_id' in trip else None
    
        latitude = vehicle['position']['latitude'] if 'position' in vehicle else None
        longitude = vehicle['position']['longitude'] if 'position' in vehicle else None
        current_stop_seq = vehicle['current_stop_sequence'] if 'current_stop_sequence' in vehicle else None
        timestamp = vehicle['timestamp']
        timestamp = datetime.fromtimestamp(timestamp)
        label = vehicle['vehicle']['label'] if 'vehicle' in vehicle else None
        
        ###
        trip_ids.append(trip_id)
        route_ids.append(route_id)
        direction_ids.append(direction_id)
        latitudes.append(latitude)
        longitudes.append(longitude)
        current_stop_seqs.append(current_stop_seq)
        timestamps.append(timestamp)
        labels.append(label)
        ###

    df_vehicle_positions = pd.DataFrame({'trip_id': trip_ids, 'route_id': route_ids, 
                                         'direction_id': direction_ids, 'latitude': latitudes,
                                         'longitude': longitudes, 'timestamp': timestamps, 'label': labels})
    
    df_vehicle_positions = gpd.GeoDataFrame(df_vehicle_positions,
                                            geometry=gpd.points_from_xy(df_vehicle_positions.longitude,
                                                                        df_vehicle_positions.latitude))

    return df_vehicle_positions

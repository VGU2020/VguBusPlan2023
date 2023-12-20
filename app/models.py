class Network(object):
    def __init__(self, shapes, stops, stop_times, trips):
        self.trips = trips
        self.shapes = shapes
        self.stops = stops
        self.stop_times = stop_times


class Bus(object):
    def __init__(self, trip_id, route_id, label, latitude, longitude, speed, current_stop_sequence, stop_id, current_status, timestamp):
        self.trip_id = trip_id
        self.route_id = route_id
        self.label = label
        self.latitude = latitude
        self.longitude = longitude
        self.speed = speed
        self.current_stop_sequence = current_stop_sequence
        self.stop_id = stop_id
        self.current_status = current_status
        self.timestamp = timestamp

    
    def to_geojson(self):
        feature = {
            "type": "Feature",
            "object": "Bus",
            "properties": {
                "trip_id": self.trip_id,
                "route_id": self.route_id,
                "label": self.label,
                "latitude": self.latitude,
                "longitude": self.longitude,
                "speed": self.speed,
            },
            "geometry": {
                "type": "Point",
                "coordinates": [self.longitude, self.latitude],
            }
        }

        return feature


class Stop(object):
    def __init__(self, stop_id, stop_name, stop_lat, stop_lon):
        self.stop_id = stop_id
        self.stop_name = stop_name
        self.stop_lat = stop_lat
        self.stop_lon = stop_lon


    def to_geojson(self):
        feature = {
            "type": "Feature",
            "object": "Stop",
            "properties": {
                "stop_id": self.stop_id,
                "stop_name": self.stop_name,
                "stop_lat": self.stop_lat,
                "stop_lon": self.stop_lon,
            },
            "geometry": {
                "type": "Point",
                "coordinates": [self.stop_lon, self.stop_lat],
            }
        }

        return feature
        

class Path(object):
    def __init__(self, trip_id, route_id, pathway):
        self.trip_id = trip_id
        self.route_id = route_id
        self.pathway = pathway


    def to_geojson(self):
        feature = {
            "type": "Feature",
            "object": "Path",
            "properties": {
                "trip_id": str(self.trip_id),
                "route_id": int(self.route_id),
            },
            "geometry": {
                "type": "LineString",
                "coordinates": self.pathway
            }
        }

        return feature
        

class Segment(object):
    def __init__(self, trip_id, congestion_lv, seg_coords):
        self.trip_id = trip_id
        self.congestion_lv = congestion_lv
        self.seg_coords = seg_coords


    def to_geojson(self):
        feature = {
            "type": "Feature",
            "object": "Segment",
            "properties": {
                "congestion_level": self.congestion_lv,
            },
            "geometry": {
                "type": "LineString",
                "coordinates": self.seg_coords
            }
        }

        return feature
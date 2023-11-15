class Network:
    def __init__(
        self,
        routes = None,
        shapes = None,
        stops = None,
        stop_times = None,
        trips = None,
        vehicle_positions = None
    ):
        self.routes = routes
        self.trips = trips
        self.shapes = shapes
        self.stops = stops
        self.stop_times = stop_times
        self.vehicle_positions = vehicle_positions

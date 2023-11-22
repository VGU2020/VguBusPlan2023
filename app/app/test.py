import geopandas as gp
import pandas as pd
import pytest

from .models import Network
from .services import getData
from .kit import *


network = getData()
network_copy = Network()


def test_geometrize_routes():
    route_ids = network.routes.route_id.iloc[:]
    g = geometrize_routes(network, route_ids)
    assert isinstance(g, gp.GeoDataFrame)
    assert g.shape[0] == len(route_ids)
    assert g.crs != "epsg:4326"


    nw = Network()
    nw.agency = pd.DataFrame(
        {"agency_id": ["agency_id_0"], "agency_name": ["agency_name_0"]}
    )
    nw.routes = pd.DataFrame(
        {
            "route_id": ["route_id_0"],
            "agency_id": ["agency_id_0"],
            "route_short_name": [None],
            "route_long_name": ["route_long_name_0"],
            "route_desc": [None],
            "route_type": [1],
            "route_url": [None],
            "route_color": [None],
            "route_text_color": [None],
        }
    )
    nw.trips = pd.DataFrame(
        {
            "route_id": ["route_id_0"],
            "service_id": ["service_id_0"],
            "trip_id": ["trip_id_0"],
            "trip_headsign": [None],
            "trip_short_name": [None],
            "direction_id": [None],
            "block_id": [None],
            "wheelchair_accessible": [None],
            "bikes_allowed": [None],
            "trip_desc": [None],
            "shape_id": ["shape_id_0"],
        }
    )
    nw.shapes = pd.DataFrame(
        {
            "shape_id": ["shape_id_0", "shape_id_0"],
            "shape_pt_lon": [2.36, 2.37],
            "shape_pt_lat": [48.82, 48.82],
            "shape_pt_sequence": [0, 1],
        }
    )
    nw.stops = pd.DataFrame(
        {
            "stop_id": ["stop_id_0", "stop_id_1"],
            "stop_name": ["stop_name_0", "stop_name_1"],
            "stop_desc": [None, None],
            "stop_lat": [48.82, 48.82],
            "stop_lon": [2.36, 2.37],
            "zone_id": [None, None],
            "stop_url": [None, None],
            "location_type": [0, 0],
            "parent_station": [None, None],
            "wheelchair_boarding": [None, None],
        }
    )
    nw.stop_times = pd.DataFrame(
        {
            "trip_id": ["trip_id_0", "trip_id_0"],
            "arrival_time": ["11:40:00", "11:45:00"],
            "departure_time": ["11:40:00", "11:45:00"],
            "stop_id": ["stop_id_0", "stop_id_1"],
            "stop_sequence": [0, 1],
            "stop_time_desc": [None, None],
            "pickup_type": [None, None],
            "drop_off_type": [None, None],
        }
    )

    gr = geometrize_routes(network=nw, route_ids=nw.routes.route_id.loc[:])
    assert isinstance(gr, gp.GeoDataFrame)


def test_geometrize_trips():
    trip_ids = network.trips.trip_id.iloc[:]
    g = geometrize_trips(network, trip_ids)
    assert isinstance(g, gp.GeoDataFrame)
    assert g.shape[0] == len(trip_ids)
    assert g.crs == "epsg:4326"

    with pytest.raises(ValueError):
        geometrize_trips(network_copy)


def test_geometrize_shapes():
    g_1 = geometrize_shapes(network)
    g_2 = geometrize_shapes_0(network.shapes)
    assert g_1.equals(g_2)

    with pytest.raises(ValueError):
        geometrize_shapes(network_copy)


def test_geometrize_stops():
    g_1 = geometrize_stops(network)
    g_2 = geometrize_stops_0(network.stops)
    assert g_1.equals(g_2)


def test_geometrize_shapes_0():
    shapes = network.shapes
    geo_shapes = geometrize_shapes_0(shapes)
    # Should be a GeoDataFrame
    assert isinstance(geo_shapes, gp.GeoDataFrame)
    # Should have the correct shape
    assert geo_shapes.shape[0] == shapes["shape_id"].nunique()
    assert geo_shapes.shape[1] == shapes.shape[1] - 3
    # Should have the correct columns
    expect_cols = set(list(shapes.columns) + ["geometry"]) - set(
        [
            "shape_pt_lon",
            "shape_pt_lat",
            "shape_pt_sequence",
            "shape_dist_traveled",
        ]
    )
    assert set(geo_shapes.columns) == expect_cols


def test_geometrize_stops_0():
    stops = network.stops
    geo_stops = geometrize_stops_0(stops)
    # Should be a GeoDataFrame
    assert isinstance(geo_stops, gp.GeoDataFrame)
    # Should have the correct shape
    assert geo_stops.shape[0] == stops.shape[0]
    assert geo_stops.shape[1] == stops.shape[1] - 1
    # Should have the correct columns
    expect_cols = set(list(stops.columns) + ["geometry"]) - set(
        ["stop_lon", "stop_lat"]
    )
    assert set(geo_stops.columns) == expect_cols


def test_routes_to_geojson():
    route_ids = network.routes.route_id.iloc[:]
    n = len(route_ids)

    gj = routes_to_geojson(network, route_ids)
    k = (
        network.stop_times.merge(network.trips.filter(["trip_id", "route_id"]))
        .loc[lambda x: x.route_id.isin(route_ids), "stop_id"]
        .nunique()
    )
    assert len(gj["features"]) == n + k

    with pytest.raises(ValueError):
        routes_to_geojson(network, route_ids=["ID_nay_khong_co_dau_^_^"])


def test_stops_to_geojson():
    stop_ids = network.stops.stop_id.unique()[:2]
    collection = stops_to_geojson(network, stop_ids)
    assert isinstance(collection, dict)
    assert len(collection["features"]) == len(stop_ids)

    with pytest.raises(ValueError):
        stops_to_geojson(network, ["Gia_tri_ao_nhung_code la_that_^_^"])


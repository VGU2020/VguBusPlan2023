import json
import copy
import geopandas as gp

import pandas as pd
from typing import Optional, Iterable
from json2html import json2html
import shapely.geometry as sg
import shapely.ops as so

from .models import Network


def geometrize_routes(network: "Network", route_ids: Optional[Iterable[str]] = None) -> gp.GeoDataFrame:
    """
    Return a GeoDataFrame with all the columns of `network.routes`
    plus a geometry column of (Multi)LineStrings, each of which represents the
    corresponding routes's shape.

    If an iterable of route IDs is given, then subset to those routes.
    """
    # Subset trips
    trip_ids = (
        network.trips.loc[lambda x: x.route_id.isin(route_ids)]
        # Drop unnecessary duplicate shapes
        .drop_duplicates(subset="shape_id").loc[:, "trip_id"]
    )


    def merge_lines(group):
        d = {}
        d["geometry"] = so.linemerge(group["geometry"].tolist())
        return pd.Series(d)

    return (
        geometrize_trips(network, trip_ids)
        .filter(["route_id", "direction_id", "geometry"])
        .groupby(["route_id"])
        .apply(merge_lines)
        .reset_index()
        .merge(network.routes)
    )


def geometrize_trips(network: "Network", trip_ids: Optional[Iterable[str]] = None) -> gp.GeoDataFrame:
    """
    Return a GeoDataFrame with the columns in ``network.trips`` and a geometry column
    of LineStrings, each of which represents the shape of the corresponding trip.

    Raise a ValueError if the network has no shapes.
    """
    if network.shapes is None:
        raise ValueError("This network has no shapes.")

    if trip_ids is not None:
        trips = network.trips.loc[lambda x: x.trip_id.isin(trip_ids)].copy()
    else:
        trips = network.trips.copy()

    return (
        geometrize_shapes(network, shape_ids=trips.shape_id.tolist())
        .filter(["shape_id", "geometry"])
        .merge(trips, how="left")
    )


def geometrize_shapes_0(shapes: pd.DataFrame) -> gp.GeoDataFrame:
    """
    Given a GTFS shapes DataFrame, convert it to a GeoDataFrame of LineStrings
    and return the result, which will no longer have the columns
    ``'shape_pt_sequence'``, ``'shape_pt_lon'``,
    ``'shape_pt_lat'``, and ``'shape_dist_traveled'``.
    """

    def my_agg(group):
        d = {}
        d["geometry"] = sg.LineString(group[["shape_pt_lon", "shape_pt_lat"]].values)
        return pd.Series(d)

    g = (
        shapes.sort_values(["shape_id", "shape_pt_sequence"])
        .groupby("shape_id", sort=False)
        .apply(my_agg)
        .reset_index()
        .pipe(gp.GeoDataFrame, crs="EPSG:4326")
    )

    return g


def geometrize_shapes(network: "Network", shape_ids: Optional[Iterable[str]] = None) -> gp.GeoDataFrame:
    """
    Given a network instance, convert its shapes DataFrame to a GeoDataFrame of
    LineStrings and return the result, which will no longer have the columns
    ``'shape_pt_sequence'``, ``'shape_pt_lon'``, ``'shape_pt_lat'``, and
    ``'shape_dist_traveled'``.

    If the network has no shapes, then raise a ValueError.
    """
    if network.shapes is None:
        raise ValueError("This network has no shapes.")

    if shape_ids is not None:
        shapes = network.shapes.loc[lambda x: x.shape_id.isin(shape_ids)]
    else:
        shapes = network.shapes

    return geometrize_shapes_0(shapes)


def geometrize_stops_0(stops: pd.DataFrame) -> gp.GeoDataFrame:
    """
    Given a stops DataFrame, convert it to a GeoPandas GeoDataFrame of Points
    and return the result, which will no longer have the columns ``'stop_lon'`` and
    ``'stop_lat'``.
    """
    g = (
        stops.assign(geometry=gp.points_from_xy(x=stops.stop_lon, y=stops.stop_lat))
        .drop(["stop_lon", "stop_lat"], axis=1)
        .pipe(gp.GeoDataFrame, crs="EPSG:4326")
    )

    return g


def geometrize_stops(network: "Network", stop_ids: Optional[Iterable[str]] = None) -> gp.GeoDataFrame:
    """
    Given a network instance, convert its stops DataFrame to a GeoDataFrame of
    Points and return the result, which will no longer have the columns
    ``'stop_lon'`` and ``'stop_lat'``.
    """
    if stop_ids is not None:
        stops = network.stops.loc[lambda x: x.stop_id.isin(stop_ids)]
    else:
        stops = network.stops

    return geometrize_stops_0(stops)


def stops_to_geojson(network: "Network", stop_ids: Optional[Iterable[str]] = None) -> dict:
    """
    Return a GeoJSON FeatureCollection of Point features 
    representing all the stops in ``network.stops``.
    The coordinates reference system is the default one for GeoJSON, namely WGS84.

    If some of the given stop IDs are not found in the network, then raise a ValueError.
    """
    if stop_ids is None or not list(stop_ids):
        stop_ids = network.stops.stop_id

    D = set(stop_ids) - set(network.stops.stop_id)
    if D:
        raise ValueError(f"Stops {D} are not found in network.")

    g = geometrize_stops(network, stop_ids=stop_ids)

    return drop_feature_ids(json.loads(g.to_json()))


def routes_to_geojson(
    network: "Network",
    route_ids: Optional[Iterable[str]] = None,
) -> dict:
    """
    Return a GeoJSON FeatureCollection of MultiLineString features representing this network's routes.
    The coordinates reference system is the default one for GeoJSON, namely WGS84.

    If the network has no shapes, then raise a ValueError.
    If any of the given route IDs are not found in the network, then raise a ValueError.
    """
    if route_ids is None or not list(route_ids):
        route_ids = network.routes.route_id

    D = set(route_ids) - set(network.routes.route_id)

    if D:
        raise ValueError(f"Route IDs {D} not found in network.")

    # Get routes
    g = geometrize_routes(network, route_ids=route_ids)
    collection = json.loads(g.to_json())

    # Get stops
    if route_ids is not None:
        stop_ids = (
            network.stop_times.merge(network.trips.filter(["trip_id", "route_id"]))
            .loc[lambda x: x.route_id.isin(route_ids), "stop_id"]
            .unique()
        )
    else:
        stop_ids = None

    stops_gj = stops_to_geojson(network, stop_ids=stop_ids)
    collection["features"].extend(stops_gj["features"])

    return drop_feature_ids(collection)


def make_html(d: dict) -> str:
    """
    Convert the given dictionary into an HTML table (string) with
    two columns: keys of dictionary, values of dictionary.
    """
    return json2html.convert(
        json=d, table_attributes="class='table table-condensed table-hover'"
    )


def drop_feature_ids(collection: dict) -> dict:
    """
    Given a GeoJSON FeatureCollection, remove the ``'id'`` attribute of each
    Feature, if it exists.
    """
    new_features = []
    for f in collection["features"]:
        new_f = copy.deepcopy(f)
        if "id" in new_f:
            del new_f["id"]
        new_features.append(new_f)

    collection["features"] = new_features
    return collection


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strip the whitespace from all column names in the given DataFrame
    and return the result.
    """
    f = df.copy()
    f.columns = [col.strip() for col in f.columns]
    return f


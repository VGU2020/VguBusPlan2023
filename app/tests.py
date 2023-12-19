from services import *
from predict import *

import pytest
import pandas as pd


def test_fetch_train_data():
    data_df = fetch_train_data()
    assert data_df["label"].dtype == int
    assert isinstance(data_df, pd.DataFrame)


def test_fetch_realtime_data():
    valid_url = "https://srv.mvta.com/infoPoint/GTFS-realtime.ashx?Type=VehiclePosition"
    df = fetch_realtime_data(valid_url)
    
    assert isinstance(df, pd.DataFrame)

    # Test Failure
    with pytest.raises(ValueError):
        fetch_realtime_data("invalid url")

    with pytest.raises(ValueError):
        fetch_realtime_data("https://www.google.com")


def test_fetch_static_data():
    nw = fetch_static_data("https://stanrta.rideralerts.com/InfoPoint/gtfs-zip.ashx")
    assert isinstance(nw.trips, pd.DataFrame)
    assert isinstance(nw.stops, pd.DataFrame)

    # Test failure
    with pytest.raises(ValueError):
        fetch_static_data("invalid url")

    with pytest.raises(ValueError):
        fetch_static_data("https://www.youtube.com/")


def test_obj_to_geojson():
    nw = fetch_static_data("http://srv.mvta.com/InfoPoint/GTFS-Zip.ashx")
    rt = fetch_realtime_data("https://srv.mvta.com/infoPoint/GTFS-realtime.ashx?Type=VehiclePosition")
    coll_lst = obj_to_geojson(network=nw, vehicle_positions=rt)

    assert isinstance(coll_lst, list)
    assert isinstance(coll_lst[0], dict)
    assert len(coll_lst[0]["features"]) >= 3
    assert isinstance(coll_lst[0]["features"][0], dict)
    assert isinstance(coll_lst[0]["features"][1], dict)
    assert isinstance(coll_lst[0]["features"][2], dict)


def test_data_wrangling():
    rt_data = fetch_realtime_data("https://srv.mvta.com/infoPoint/GTFS-realtime.ashx?Type=VehiclePosition")
    rt_df = data_wrangling(rt_data)

    assert isinstance(rt_df, pd.DataFrame)
    assert rt_df['timestamp'].dtypes  == "object"
    assert rt_df['predict_time'].dtypes  == "object"

    # Test Failure
    empty_df = pd.DataFrame({})
    assert data_wrangling(empty_df).empty
 

def test_predict():
    sample = fetch_train_data()
    rt_data = fetch_realtime_data("https://srv.mvta.com/infoPoint/GTFS-realtime.ashx?Type=VehiclePosition")
    model = train(sample)
    pred_df = predict(model, rt_data)

    assert isinstance(pred_df, pd.DataFrame)
    assert pred_df['label'].dtypes  == "int"

    # Test Failure
    empty_df = pd.DataFrame({})
    pred_empty = predict(model, empty_df)
    assert pred_empty.empty
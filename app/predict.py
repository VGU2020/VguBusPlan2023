import pandas as pd
import xgboost as xgb
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit


FEATURES = ["speed", "route_id", "label"]
TARGET = 'congestion_level'


def data_wrangling(df: pd.DataFrame):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df["predict_time"] = (df["timestamp"] + pd.Timedelta(value='1 minutes')).astype(str)
    df = df.astype({'timestamp': 'str'})
    return df


def train(sample: pd.DataFrame):
    test_sz = sample.shape[0] // 6
    remainder = sample.shape[0] - test_sz * 6
    tss = TimeSeriesSplit(n_splits=5, test_size=test_sz, gap=remainder)

    for train_idx, val_idx in tss.split(sample):
        train = sample.iloc[train_idx]
        test = sample.iloc[val_idx]

        X_train = train[FEATURES]
        y_train = train[TARGET]

        X_test = test[FEATURES]
        y_test = test[TARGET]

        reg = xgb.XGBRegressor(
            base_score=0.5, 
            booster='gbtree',    
            n_estimators=1000,
            early_stopping_rounds=50,
            objective='reg:squarederror',
            max_depth=3,
            learning_rate=0.01,
        )

        reg.fit(
            X_train, y_train,
            eval_set=[(X_train, y_train), (X_test, y_test)],
            verbose=0)
        
        return reg


def predict(model, realtime_df):
    realtime_df = realtime_df.astype({'label': 'int'})
    model.fit(
        realtime_df[FEATURES], realtime_df[TARGET],
        eval_set=[(realtime_df[FEATURES], realtime_df[TARGET])],
        verbose=0
    )
    
    result_df = data_wrangling(realtime_df)
    result_df = result_df[["timestamp", "trip_id", "route_id", "label", "congestion_level", "speed", "predict_time"]]
    result_df['prediction'] = model.predict(realtime_df[FEATURES])
    result_df['prediction'] = result_df['prediction'].abs()
    
    return result_df


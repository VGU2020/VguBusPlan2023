from pymongo.mongo_client import MongoClient
import pandas as pd
import zipfile


uri = "mongodb+srv://kafka:1a2b3c4d@us.l7mluba.mongodb.net/?retryWrites=true&w=majority"


def connect_db():
    try:
        client = MongoClient(uri)
        mongodb = client['minnesota']

        return mongodb
    except Exception as e:
        print(e)


def insert() -> None:
    mongodb = connect_db()
    csv_file = zipfile.ZipFile("C:/Users/vensy/OneDrive/Máy tính/final/app/votran.zip")
    
    for csv in csv_file.namelist():
        if not csv.replace(".txt", '') in mongodb.list_collection_names():
            collection = mongodb.create_collection(csv.replace(".txt", ''))
        else:
            collection = mongodb[csv.replace(".txt", '')]
        reader = pd.read_csv(csv_file.open(csv))
        if reader.empty:
            header = dict()
            for col in reader.columns.values:
                header[col] = None
            collection.insert_one(header)
        else:
            df = reader.fillna('null').to_dict("records")
        
            collection.insert_many(df)


import json
import os
import pandas as pd
from pandas import DataFrame

from spotify_request_data import CustomError

class Transform():

    def __init__(self) -> None:
        self._song_names = []
        self._artist_names = []
        self._played_at_list = []
        self._timestamps = []


    def __run__(self):
        print("Started Transform")

        data = self.read_raw_data()
        data = self.create_dict(data=data)
        dataframe = self.create_dataframe(data=data)
        self.check_if_valid_data(dataframe=dataframe)
        self.write_dataframe(dataframe=dataframe)

        print("Finish Transform \n")

    def read_raw_data(self) -> dict:
        with open('extract/json_data.json') as json_file:
            data = json.load(json_file)
            return data
    

    def create_dict(self, data: dict) -> dict:
        for song in data["items"]:
            self._song_names.append(song["track"]["name"])
            self._artist_names.append(song["track"]["album"]["artists"][0]["name"])
            self._played_at_list.append(song["played_at"])
            self._timestamps.append(song["played_at"][0:10])
            
        return {
            "song_name": self._song_names,
            "artist_name": self._artist_names,
            "played_at": self._played_at_list,
            "timestamp": self._timestamps
        }
            

    def create_dataframe(self, data: dict) -> DataFrame:
        columns = ["song_name", "artist_name", "played_at" , "timestamp"]
        return pd.DataFrame(data, columns=columns )

    
    def check_if_valid_data(self, dataframe: pd.DataFrame) -> bool:
        self.valid_dataframe_empty(dataframe=dataframe)
        self.valid_primary_key(dataframe=dataframe)
        self.valid_is_null(dataframe=dataframe)

        # yesterday = datetime.now() - timedelta(days=1)
        # timestamps = dataframe["timestamp"].tolist()
        # for timestamp in timestamps:
        #     print(timestamp)
        #     print(yesterday)
        #     if datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
        #         print("at least one of the return songs does not come from within the five days ago")
        #         raise CustomError

        return True

    def valid_dataframe_empty(self, dataframe: DataFrame) -> None:
        if dataframe.empty:
            print("No Data")
            raise CustomError

    def valid_primary_key(self, dataframe: DataFrame) -> None:
         if not pd.Series(dataframe["played_at"]).is_unique:
            print(("Primary Key Check is violeted"))
            raise CustomError 

    def valid_is_null(self, dataframe: DataFrame) -> None:
        if dataframe.isnull().values.any():
            print("Null value found")
            raise CustomError

    def write_dataframe(self, dataframe: DataFrame) -> None:
        os.system('rm -rf transform/silver_data/')
        dataframe.to_parquet(path="transform/silver_data/", partition_cols="artist_name", index=True)
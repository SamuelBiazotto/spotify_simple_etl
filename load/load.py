import sqlite3
import sqlalchemy
import pandas as pd
from pandas import DataFrame

from spotify_request_data import CustomError


class Load():

    def __init__(self) -> None:
        __DATABASE_LOCATION = "sqlite:///load/my_played_songs.sqlite"
        self._engine = sqlalchemy.create_engine(__DATABASE_LOCATION)
        self._conn = sqlite3.connect("load/my_played_songs.sqlite")
        self._cursor = self._conn.cursor()


    def __run__(self):
        print("Started Load")

        dataframe = self.read_dataframe()
        self.create_table()
        self.insert_data_into_database(dataframe=dataframe)

        print("Finished Load")

    def read_dataframe(self) -> DataFrame:
        return pd.read_parquet("transform/silver_data/")

    def create_table(self) -> None:
        sql_query = """
            CREATE TABLE IF NOT EXISTS my_played_songs(
                    song_name VARCHAR(200),
                    artist_name VARCHAR(200),
                    played_at VARCHAR(200),
                    timestamp VARCHAR(200),
                    CONSTRAINT primary_key_constraint PRIMARY KEY(played_at)
            )
        """

        self._cursor.execute(sql_query)
        print("Opened database successfully")
    

    def insert_data_into_database(self, dataframe: DataFrame) -> None:
        try:
            dataframe.to_sql(
                "my_played_songs", 
                self._engine, 
                index=False, 
                if_exists="append"
            )
        except CustomError:
            print("Data already exists in the database")
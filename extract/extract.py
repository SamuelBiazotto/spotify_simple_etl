from datetime import datetime, timedelta
import json
import requests

class Extract():

    def __init__(self) -> None:
        self._DATABASE_LOCATION = "sqlite:///my_played_songs.sqlite"
        self._USER_ID = "<USER_ID_FROM_SPOTIFY>"
        self._TOKEN = "<SPOTIFY_TOKEN>"
        self._today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self._yesterday = self._today - timedelta(days=3)
        self._yester_unix_timestamp = int(self._yesterday.timestamp()) * 1000
        self._header = {
            "Accept": "application/json",
            "Content-type": "application/json",
            "Authorization": "Bearer {token}".format(token=self._TOKEN)
        }

    def __run__(self):
        print("Started Extraction")

        data = self.get_spotify_data()
        data = self.valid_data_request(data=data)
        self.save_data_extraction(json_data=data)

        print("Finished Extract \n")
    

    def get_spotify_data(self) -> requests.models.Response:
        url = "https://api.spotify.com/v1/me/player/recently-played?after="
        return requests.get(url + str(self._yester_unix_timestamp), headers=self._header)


    def valid_data_request(self, data: requests) -> dict:
        if data.status_code == 200:
            return data.json()
        else:
            print(data.status_code)
            print(data.json())

    def save_data_extraction(self, json_data: dict):
        json_data = json.dumps(json_data)
        with open('extract/json_data.json', 'w') as outfile:
            outfile.write(json_data)

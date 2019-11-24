from typing import Dict, Callable

import requests
from airflow.sensors.http_sensor import HttpSensor


class OAuthHttpSensor(HttpSensor):
    def __init__(self, oauth_endpoint: str = None, oauth_body: Dict[str, str] = None,
                 oauth_response: Callable = None, *args, **kwargs):
        super(OAuthHttpSensor, self).__init__(*args, **kwargs)
        self.oauth_endpoint = oauth_endpoint
        self.oauth_body = oauth_body
        self.oauth_response = oauth_response

    def poke(self, context):
        oauth_response = requests.post(self.oauth_endpoint, data=self.oauth_body)
        access_token = self.oauth_response(oauth_response)

        self.headers = self.headers or {}
        self.headers['authorization'] = f'Bearer {access_token}'

        return super().poke(context)

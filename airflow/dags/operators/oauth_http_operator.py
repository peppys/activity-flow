from airflow.operators.http_operator import SimpleHttpOperator

import requests


class OAuthHttpOperator(SimpleHttpOperator):
    def __init__(self, oauth_endpoint, oauth_body, oauth_response, *args, **kwargs):
        super(OAuthHttpOperator, self).__init__(*args, **kwargs)
        self.oauth_endpoint = oauth_endpoint
        self.oauth_body = oauth_body
        self.oauth_response = oauth_response

    def execute(self, context):
        response = requests.post(self.oauth_endpoint, data=self.oauth_body)
        access_token = self.oauth_response(response)

        self.headers = self.headers or {}
        self.headers['authorization'] = f'Bearer {access_token}'

        return super().execute(context)

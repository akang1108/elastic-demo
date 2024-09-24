import json
from typing import Any
from urllib.parse import urlencode
from requests.auth import HTTPBasicAuth

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ES:
    def __init__(self, baseURL: str, user: str, password: str):
        self.baseURL = baseURL
        self.user = user
        self.password = password
        self.basic = HTTPBasicAuth(self.user, self.password)

    def upsert_index(self, name: str) -> Any:
        try:
            return self.send_request("GET", f"/{name}")
        except:
            return self.create_index(name)

    def create_index(self, name: str) -> Any:
        return self.send_request("PUT", f"/{name}", body={
            "settings": {
                "index": {
                    "number_of_shards": 3,
                    "number_of_replicas": 2
                }
            }
        })

    def index(self, index_name: str, doc: Any) -> Any:
        return self.send_request("POST", f"/{index_name}/_doc/", body=doc)

    def delete_index(self, name: str) -> Any:
        try:
            return self.send_request("DELETE", f"/{name}")
        except:
            return {}

    def status(self) -> Any:
        return self.send_request("GET", "/")

    def send_request(self, http_method: str, path: str, body: Any = {}, params: Any = {}) -> Any:
        url = f"{self.baseURL}{path}"
        if params:
            url += urlencode(params)

        print(f"--> {http_method} {url} {body}")

        headers = {
            "Content-Type": "application/json",
        }

        if body:
            response = requests.request(http_method, url, json=body, headers=headers, verify=False, timeout=(30, 120), auth=self.basic)
        else:
            response = requests.request(http_method, url, headers=headers, verify=False, timeout=(30, 120), auth=self.basic)

        print(f"<-- {response.status_code} {response.text}")

        if response.status_code >= 300:
            raise Exception(f"status code: {response.status_code}\nresponse body:\n{response.text}")
        return response.json()

import requests

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)


class AirtableActions:
    def __init__(self, api_key: str, table_key: str, table_name: str, body: str):
        self.api_key = api_key
        self.table_key = table_key
        self.table_name = table_name
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        self.body = body

    @property
    def post_data(self):
        airtable_url = f"https://api.airtable.com/v0/{self.table_key}/{self.table_name}"
        response = requests.post(
            airtable_url, headers=self.headers, json=self.body, timeout=10, verify=False,
        )
        return response

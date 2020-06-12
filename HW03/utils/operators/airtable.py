import csv

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from utils.helpers.airtable import AirtableActions


class PushDataToAirtableOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self, home_path: str, api_key: str, table_key: str, table_name: str, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.home_path = home_path
        self.api_key = api_key
        self.table_key = table_key
        self.table_name = table_name
        with open(f"{self.home_path}/temp_data_info.csv", "r") as temp_data_info:
            reader = csv.reader(temp_data_info)
            for info in reader:
                self.body = {
                    "records": [
                        {
                            "fields": {
                                "chat_id": info[0],
                                "username": info[1],
                                "triggered_at": info[2],
                                "event_type": info[3],
                                "reporter_name": info[4],
                            }
                        },
                    ]
                }
        self.airtable_actions = AirtableActions(
            self.api_key, self.table_key, self.table_name, self.body
        )

    def execute(self, *args, **kwargs):
        response = self.airtable_actions.post_data
        print(response.text)

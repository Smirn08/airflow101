import json

import requests

from airflow import AirflowException

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)


class BotMessageActions:
    def __init__(self, home_path: str, bot_token: str, body: str):
        self.home_path = home_path
        self.bot_token = bot_token
        self.headers = {"Content-Type": "application/json"}
        self.body = body

    @property
    def send_message(self):
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        response = requests.post(
            url, headers=self.headers, json=self.body, timeout=10, verify=False,
        )
        with open(f"{self.home_path}/last_message_id.txt", "w") as last_message_id_file:
            last_message_id_file.write(str(json.loads(response.text)["result"]["message_id"]))
            print("message_id saved to txt")
        return response

    @property
    def del_message(self):
        url = f"https://api.telegram.org/bot{self.bot_token}/deleteMessage"
        response = requests.post(
            url, headers=self.headers, json=self.body, timeout=10, verify=False,
        )
        return response


class GetBotUpdatesInfo:
    def __init__(self, home_path: str, bot_token: str):
        self.home_path = home_path
        self.url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
        self.message_id = None

    def get_updates(self):
        response = requests.get(self.url, timeout=10)
        self.info = json.loads(response.text)

    @property
    def callback_query(self):

        self.get_updates()

        if self.message_id is None:
            with open(f"{self.home_path}/last_message_id.txt", "r") as last_message_id_file:
                self.message_id = int(last_message_id_file.read())

        if self.info["ok"]:
            for update_data_index, update_data in enumerate(self.info["result"]):
                result_data = self.info["result"][update_data_index]
                if (
                    "callback_query" in result_data
                    and result_data["callback_query"]["message"]["message_id"] == self.message_id
                ):
                    print(result_data)
                    return result_data["callback_query"]
        else:
            print(self.info)
            raise AirflowException("Не удалось обработать ответ")

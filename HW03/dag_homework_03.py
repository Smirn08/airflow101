import csv
import json
from datetime import datetime

import requests

from airflow import DAG, AirflowException
from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)


HOME_PATH = "/home/smirn08m/WORKSPACE/HW03"

BOT_TOKEN = Variable.get("BOT_TOKEN")
CHAT_ID = Variable.get("CHAT_ID")

AIRTABLE_API_KEY = Variable.get("AIRTABLE_API_KEY")
AIRTABLE_TABLE_KEY = Variable.get("AIRTABLE_TABLE_KEY")
AIRTABLE_TABLE_NAME = Variable.get("AIRTABLE_TABLE_NAME")


default_args = {
    "owner": "Smirnov Maksim",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 6),
    "catchup": False,
}


dag_hw03_telegram_button = DAG(
    "hw03_telegram_button", default_args=default_args, schedule_interval=None
)


class BotMessageActions:
    def __init__(self, bot_token: str, body: str):
        self.bot_token = bot_token
        self.headers = {"Content-Type": "application/json"}
        self.body = body

    @property
    def send_message(self):
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        response = requests.post(
            url, headers=self.headers, json=self.body, timeout=10, verify=False,
        )
        with open(f"{HOME_PATH}/last_message_id.txt", "w") as last_message_id_file:
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
    def __init__(self, bot_token: str):
        self.url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
        self.message_id = None

    def get_updates(self):
        response = requests.get(self.url, timeout=10)
        self.info = json.loads(response.text)

    @property
    def callback_query(self):

        self.get_updates()

        if self.message_id is None:
            with open(f"{HOME_PATH}/last_message_id.txt", "r") as last_message_id_file:
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


class TelegramButtonSenderOperator(BaseOperator):
    @apply_defaults
    def __init__(self, bot_token: str, chat_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bot_token = bot_token
        self.body = {
            "chat_id": chat_id,
            "text": "Ну что, поехали?",
            "reply_markup": {
                "inline_keyboard": [[{"text": "Поехали", "callback_data": "Smirn08_airflow"}]]
            },
        }
        self.bot_actions = BotMessageActions(self.bot_token, self.body)

    def execute(self, *args, **kwargs):
        response = self.bot_actions.send_message
        print(response.text)


class CatchWhoPressTheButtonSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, bot_token: str, chat_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bot_token = bot_token
        self.users_info = GetBotUpdatesInfo(self.bot_token)
        with open(f"{HOME_PATH}/last_message_id.txt", "r") as last_message_id_file:
            self.message_id = int(last_message_id_file.read())
        self.body = {
            "chat_id": chat_id,
            "message_id": self.message_id,
        }
        self.bot_actions = BotMessageActions(self.bot_token, self.body)

    def poke(self, *args, **kwargs):
        print("Ожидание нажатия кнопки...")

        callback_query = self.users_info.callback_query

        if callback_query:
            chat_id = callback_query["message"]["chat"]["id"]
            username = callback_query["from"]["username"]
            triggered_at = datetime.utcnow().isoformat()[:-3] + "Z"
            event_type = "trigger_button"
            reporter_name = "Smirn08_airflow"

            triggered_button_user_data = [
                chat_id,
                username,
                triggered_at,
                event_type,
                reporter_name,
            ]
            print(chat_id, username, triggered_at, event_type, reporter_name)

            self.bot_actions.del_message

            with open(f"{HOME_PATH}/temp_data_info.csv", "w") as temp_csv:
                wr = csv.writer(temp_csv, quoting=csv.QUOTE_ALL)
                wr.writerow(triggered_button_user_data)

            return True


class PushDataToAirtableOperator(BaseOperator):
    @apply_defaults
    def __init__(self, api_key: str, table_key: str, table_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_key = api_key
        self.table_key = table_key
        self.table_name = table_name
        with open(f"{HOME_PATH}/temp_data_info.csv", "r") as temp_data_info:
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


task_send_button = TelegramButtonSenderOperator(
    task_id="send_button", bot_token=BOT_TOKEN, chat_id=CHAT_ID, dag=dag_hw03_telegram_button
)

task_waiting_for_button_press = CatchWhoPressTheButtonSensor(
    task_id="waiting_for_button_press",
    bot_token=BOT_TOKEN,
    chat_id=CHAT_ID,
    poke_interval=1,
    dag=dag_hw03_telegram_button,
)

task_send_data_to_airtable = PushDataToAirtableOperator(
    task_id="send_data_to_airtable",
    api_key=AIRTABLE_API_KEY,
    table_key=AIRTABLE_TABLE_KEY,
    table_name=AIRTABLE_TABLE_NAME,
    dag=dag_hw03_telegram_button,
)

task_send_button >> task_waiting_for_button_press >> task_send_data_to_airtable

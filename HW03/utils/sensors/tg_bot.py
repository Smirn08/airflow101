import csv
from datetime import datetime

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from utils.helpers.tg_bot import BotMessageActions, GetBotUpdatesInfo


class CatchWhoPressTheButtonSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, home_path: str, bot_token: str, chat_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.home_path = home_path
        self.bot_token = bot_token
        self.users_info = GetBotUpdatesInfo(self.bot_token)
        with open(f"{self.home_path}/last_message_id.txt", "r") as last_message_id_file:
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

            with open(f"{self.home_path}/temp_data_info.csv", "w") as temp_csv:
                wr = csv.writer(temp_csv, quoting=csv.QUOTE_ALL)
                wr.writerow(triggered_button_user_data)

            return True

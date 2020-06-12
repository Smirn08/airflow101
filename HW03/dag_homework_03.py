from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from utils.operators.airtable import PushDataToAirtableOperator
from utils.operators.tg_bot import TelegramButtonSenderOperator
from utils.sensors.tg_bot import CatchWhoPressTheButtonSensor

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


task_send_button = TelegramButtonSenderOperator(
    task_id="send_button",
    home_path=HOME_PATH,
    bot_token=BOT_TOKEN,
    chat_id=CHAT_ID,
    dag=dag_hw03_telegram_button,
)

task_waiting_for_button_press = CatchWhoPressTheButtonSensor(
    task_id="waiting_for_button_press",
    home_path=HOME_PATH,
    bot_token=BOT_TOKEN,
    chat_id=CHAT_ID,
    poke_interval=1,
    dag=dag_hw03_telegram_button,
)

task_send_data_to_airtable = PushDataToAirtableOperator(
    task_id="send_data_to_airtable",
    home_path=HOME_PATH,
    api_key=AIRTABLE_API_KEY,
    table_key=AIRTABLE_TABLE_KEY,
    table_name=AIRTABLE_TABLE_NAME,
    dag=dag_hw03_telegram_button,
)

task_send_button >> task_waiting_for_button_press >> task_send_data_to_airtable


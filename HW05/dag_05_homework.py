import json
from datetime import datetime, timedelta
import random
import time

import requests

from airflow import DAG, AirflowException
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)


HEALTH_URL = "https://www.google.com/"

BOT_TOKEN = Variable.get("BOT_TOKEN")
CHAT_ID = Variable.get("MY_CHAT_ID")


class TelegramMessageSender:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.headers = {"Content-Type": "application/json"}

    def messege_creator(self, text):
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        body = {
            "chat_id": self.chat_id,
            "text": text,
        }
        response = requests.post(url, headers=self.headers, json=body, timeout=10, verify=False)

        print(response.text)

        if not json.loads(response.text)["ok"]:
            raise AirflowException("Не удалось отправить сообщение")


def dag_failed(context):
    text = (
        f"DAG упал с ошибкой \nDAG ID:{context['dag'].dag_id} \nTASK ID: {context['task'].task_id}"
    )
    print(context["dag"].dag_id)
    print(context)
    tg_sender = TelegramMessageSender(bot_token=BOT_TOKEN, chat_id=CHAT_ID)
    tg_sender.messege_creator(text=text)


def sla_missed(dag, task_list, blocking_task_list, slas, blocking_tis):
    if blocking_tis[0].state == "running":
        text = f"Задача выполняется дольше положенного \nDAG ID: {dag.dag_id} \nTASK ID: {blocking_tis[0].task_id}"
        tg_sender = TelegramMessageSender(bot_token=BOT_TOKEN, chat_id=CHAT_ID)
        tg_sender.messege_creator(text=text)


def google_health_check(*args, **kwargs):
    number = random.choice([0, 1, 0, 0, 0, 2])
    if number == 1:
        print("Заснули")
        time.sleep(21)
        url = HEALTH_URL
    elif number == 2:
        raise AirflowException("Упали")
    else:
        url = HEALTH_URL
    response = requests.get(url, verify=False, timeout=10)
    print(response.status_code)


default_args = {
    "owner": "Smirnov Maksim",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 17, 9, 50),
    "catchup": False,
}


with DAG(
    dag_id="hw05_canary",
    default_args=default_args,
    sla_miss_callback=sla_missed,
    schedule_interval="*/10 * * * *",
) as dag:

    task_start_dag = DummyOperator(task_id="start_dag", dag=dag)

    task_end_dag = DummyOperator(task_id="end_dag", dag=dag)

    task_health_check = PythonOperator(
        task_id="health_check",
        python_callable=google_health_check,
        provide_context=True,
        on_failure_callback=dag_failed,
        sla=timedelta(seconds=20),
        dag=dag,
    )

task_start_dag >> task_health_check >> task_end_dag

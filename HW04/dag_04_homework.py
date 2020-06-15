import csv
import json
import logging
from datetime import datetime

import pandas as pd
import psycopg2
import requests
from pandas import read_csv

from airflow import DAG, AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators import BaseOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.utils.decorators import apply_defaults

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)


CSV_URL = "https://airflow101.python-jitsu.club/orders.csv"
JSON_URL = "https://api.jsonbin.io/b/5ed7391379382f568bd22822"
HOME_PATH = "/home/smirn08m/WORKSPACE/HW04"

ORDERS_FILENAME = "orders.csv"
STATUS_FILENAME = "status.csv"
CUSTOMERS_FILENAME = "customers.csv"
GOODS_FILENAME = "goods.csv"
FINAL_DATASET_FILENAME = "final_dataset.csv"

BOT_TOKEN = Variable.get("BOT_TOKEN")
CHAT_ID = Variable.get("MY_CHAT_ID")


default_args = {
    "owner": "Smirnov Maksim",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 13),
    "catchup": False,
}

dag_hw04_branch_health_check = DAG(
    "hw04_branch_health_check", default_args=default_args, schedule_interval=None
)


class TelegramMessageSenderOperator(BaseOperator):

    template_fields = ["error_message"]

    @apply_defaults
    def __init__(self, bot_token: str, chat_id: str, error_message: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.headers = {"Content-Type": "application/json"}
        self.error_message = error_message

    def execute(self, context, *args, **kwargs):
        upstream_tasks = self.get_flat_relatives(upstream=True)
        upstream_task_ids = [task.task_id for task in upstream_tasks]
        print(upstream_task_ids)
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        text = (
            "Пайплайн не завершился\n"
            f"DAG ID: {context['dag'].dag_id}\n"
            f"Task ID: {upstream_task_ids[0]}\n"
            f"Ошибка: {str(self.error_message)}"
        )
        body = {
            "chat_id": self.chat_id,
            "text": text,
        }
        response = requests.post(url, headers=self.headers, json=body, timeout=10, verify=False)

        print(response.text)

        if not json.loads(response.text)["ok"]:
            raise AirflowException("Не удалось отправить сообщение")


def csv_null_data_sanity_check(xcom_task_id: str, true_task_id: str, false_task_id: str, **context):
    dag_ti = context["task_instance"]
    file_names = dag_ti.xcom_pull(task_ids=xcom_task_id, key="file_name")

    report_list = []
    error_message = ""
    for file_name in file_names:
        try:
            csv_series = pd.read_csv(f"{HOME_PATH}/{file_name}", na_filter=True)
        except FileNotFoundError as no_file_in_path:
            error_message = f"Нет файла: {no_file_in_path}"
        else:
            check_null = csv_series.isnull().sum().to_dict()

            for column, value in check_null.items():
                report_list.append(column) if check_null[column] != 0 and column != "age" else True
            if report_list:
                error_message += f"В колонках: {'; '.join(report_list)}, файла {file_name} есть пустые значения\n"
    if error_message:
        context["task_instance"].xcom_push(key="error_message", value=error_message)
        return [false_task_id]
    else:
        return [true_task_id]


def posgres_database_check(**context):
    try:
        pg_hook = PostgresHook(postgres_conn_id="smirn08_database")
        pg_hook.get_conn()
    except psycopg2.OperationalError as connection_error:
        context["task_instance"].xcom_push(key="error_message", value=connection_error)
        logging.warn("Ошибка подключения к postgres", exc_info=True)
        return ["telegram_pg_allert"]
    else:
        return ["download_and_preprocessing_orders"]


def download_and_preprocessing_orders(**context):
    with open(f"{HOME_PATH}/temp_orders.csv", "wb") as csvfile:
        orders_csv = requests.get(CSV_URL, stream=True)
        csvfile.write(orders_csv.content)

    orders_df = read_csv(f"{HOME_PATH}/temp_orders.csv", sep=",", keep_default_na=False)
    orders_df.sort_values(by=["дата заказа"], inplace=True, ascending=False)
    orders_df["order_id"] = orders_df["id заказа"]
    orders_df["uuid"] = orders_df["uuid заказа"]
    orders_df["product"] = orders_df["название товара"]
    orders_df["order_date"] = orders_df["дата заказа"]
    orders_df["num"] = orders_df["количество"]

    def del_words(line):
        return line.replace("тов. ", "").replace("г-н ", "").replace("г-жа ", "")

    orders_df["fio"] = orders_df["ФИО"].map(lambda x: del_words(x))
    orders_df = orders_df[["order_id", "uuid", "product", "order_date", "num", "fio", "email"]]
    orders_df.to_csv(f"{HOME_PATH}/{ORDERS_FILENAME}", index=None)

    context["task_instance"].xcom_push(key="file_name", value=[ORDERS_FILENAME])


def download_payment_status_info(**context):
    status = requests.get(JSON_URL, stream=True)
    status_df = pd.read_json(status.text).T
    status_df["uuid"] = status_df.index
    status_df = status_df[["uuid", "success"]]
    status_df.to_csv(f"{HOME_PATH}/{STATUS_FILENAME}", index=None)

    context["task_instance"].xcom_push(key="file_name", value=[STATUS_FILENAME])


def download_customers_and_goods_data(**context):
    pg_hook = PostgresHook(postgres_conn_id="shop_database")
    hook_conn_obj = pg_hook.get_conn()
    hook_conn_obj.autocommit = True

    customers_df = pd.read_sql_query("SELECT * FROM public.customers", con=hook_conn_obj)
    goods_df = pd.read_sql_query("SELECT * FROM public.goods", con=hook_conn_obj)

    customers_df.to_csv(f"{HOME_PATH}/{CUSTOMERS_FILENAME}", index=None)
    goods_df.to_csv(f"{HOME_PATH}/{GOODS_FILENAME}", index=None)

    context["task_instance"].xcom_push(key="file_name", value=[CUSTOMERS_FILENAME, GOODS_FILENAME])


def create_dataframe(**context):
    orders_df = read_csv(f"{HOME_PATH}/{ORDERS_FILENAME}", sep=",", keep_default_na=False)
    status_df = read_csv(f"{HOME_PATH}/{STATUS_FILENAME}", sep=",", keep_default_na=False)
    customers_df = read_csv(f"{HOME_PATH}/{CUSTOMERS_FILENAME}", sep=",", keep_default_na=False)
    goods_df = read_csv(f"{HOME_PATH}/{GOODS_FILENAME}", sep=",", keep_default_na=False)

    orders_and_status_df = pd.merge(orders_df, status_df, on=["uuid"])
    orders_with_price_and_status_df = pd.merge(
        orders_and_status_df, goods_df, how="left", left_on="product", right_on="name"
    )
    full_dataframe = pd.merge(
        orders_with_price_and_status_df,
        customers_df,
        how="left",
        left_on="email",
        right_on="email",
    )
    today_timestamp = pd.to_datetime("now")
    full_dataframe["name"] = full_dataframe["fio"].str.strip("\n\t")
    full_dataframe["age"] = (
        (today_timestamp - pd.to_datetime(full_dataframe["birth_date"]))
        .astype("<m8[Y]")
        .astype("Int64")
    )
    full_dataframe["good_title"] = full_dataframe["product"].str.strip("\n\t")
    full_dataframe["date"] = pd.to_datetime(full_dataframe["order_date"])
    full_dataframe["payment_status"] = full_dataframe["success"]
    full_dataframe["total_price"] = full_dataframe["price"] * full_dataframe["num"]
    full_dataframe["amount"] = full_dataframe["num"]
    full_dataframe["last_modified_at"] = today_timestamp

    final_dataset = full_dataframe[
        [
            "name",
            "age",
            "good_title",
            "date",
            "payment_status",
            "total_price",
            "amount",
            "last_modified_at",
        ]
    ]
    final_dataset.to_csv(f"{HOME_PATH}/{FINAL_DATASET_FILENAME}", index=False, na_rep=None)

    context["task_instance"].xcom_push(key="file_name", value=[FINAL_DATASET_FILENAME])


def load_to_database():
    pg_hook = PostgresHook(postgres_conn_id="smirn08_database")
    hook_conn_obj = pg_hook.get_conn()
    hook_conn_obj.autocommit = True

    with hook_conn_obj.cursor() as cursor:
        table_name = "hw04_dataset"
        cursor.execute(
            f"""
            SELECT *
            FROM information_schema.tables
            WHERE table_name='{table_name}'"""
        )
        if not bool(cursor.rowcount):
            cursor.execute(
                f"""
                CREATE TABLE public.{table_name} (
                    name varchar(255),
                    age int,
                    good_title varchar(255),
                    date timestamp,
                    payment_status boolean,
                    total_price numeric(10,2),
                    amount int,
                    last_modified_at timestamp)"""
            )
        with open(f"{HOME_PATH}/final_dataset.csv", "r") as f:
            reader = csv.reader(f)
            next(reader)
            columns = (
                "name",
                "age",
                "good_title",
                "date",
                "payment_status",
                "total_price",
                "amount",
                "last_modified_at",
            )
            cursor.execute(f"TRUNCATE public.{table_name}")
            cursor.copy_from(f, table_name, columns=columns, sep=",", null="")


task_postgres_health_check = BranchPythonOperator(
    task_id="postgres_health_check",
    python_callable=posgres_database_check,
    provide_context=True,
    dag=dag_hw04_branch_health_check,
)

task_orders_sanity_check = BranchPythonOperator(
    task_id="orders_sanity_check",
    python_callable=csv_null_data_sanity_check,
    op_kwargs={
        "xcom_task_id": "download_and_preprocessing_orders",
        "true_task_id": "download_payment_status_info",
        "false_task_id": "telegram_csv_allert",
    },
    provide_context=True,
    dag=dag_hw04_branch_health_check,
)

task_payment_status_info_sanity_check = BranchPythonOperator(
    task_id="payment_status_info_sanity_check",
    python_callable=csv_null_data_sanity_check,
    op_kwargs={
        "xcom_task_id": "download_payment_status_info",
        "true_task_id": "download_customers_and_goods_data",
        "false_task_id": "telegram_json_allert",
    },
    provide_context=True,
    dag=dag_hw04_branch_health_check,
)

task_customers_and_goods_data_sanity_check = BranchPythonOperator(
    task_id="customers_and_goods_data_sanity_check",
    python_callable=csv_null_data_sanity_check,
    op_kwargs={
        "xcom_task_id": "download_customers_and_goods_data",
        "true_task_id": "create_dataframe",
        "false_task_id": "telegram_data_pg_allert",
    },
    provide_context=True,
    dag=dag_hw04_branch_health_check,
)

task_final_dataframe_sanity_check = BranchPythonOperator(
    task_id="final_dataframe_sanity_check",
    python_callable=csv_null_data_sanity_check,
    op_kwargs={
        "xcom_task_id": "create_dataframe",
        "true_task_id": "load_to_database",
        "false_task_id": "telegram_final_allert",
    },
    provide_context=True,
    dag=dag_hw04_branch_health_check,
)

task_telegram_pg_allert = TelegramMessageSenderOperator(
    task_id="telegram_pg_allert",
    bot_token=BOT_TOKEN,
    chat_id=CHAT_ID,
    error_message="{{ ti.xcom_pull(task_ids='postgres_health_check', key='error_message') }}",
    provide_context=True,
    dag=dag_hw04_branch_health_check,
)

task_telegram_csv_allert = TelegramMessageSenderOperator(
    task_id="telegram_csv_allert",
    bot_token=BOT_TOKEN,
    chat_id=CHAT_ID,
    error_message="{{ ti.xcom_pull(task_ids='orders_sanity_check', key='error_message') }}",
    provide_context=True,
    dag=dag_hw04_branch_health_check,
)

task_telegram_json_allert = TelegramMessageSenderOperator(
    task_id="telegram_json_allert",
    bot_token=BOT_TOKEN,
    chat_id=CHAT_ID,
    error_message="{{ ti.xcom_pull(task_ids='payment_status_info_sanity_check', key='error_message') }}",
    provide_context=True,
    dag=dag_hw04_branch_health_check,
)

task_telegram_data_pg_allert = TelegramMessageSenderOperator(
    task_id="telegram_data_pg_allert",
    bot_token=BOT_TOKEN,
    chat_id=CHAT_ID,
    error_message="{{ ti.xcom_pull(task_ids='customers_and_goods_data_sanity_check', key='error_message') }}",
    provide_context=True,
    dag=dag_hw04_branch_health_check,
)

task_telegram_final_allert = TelegramMessageSenderOperator(
    task_id="telegram_final_allert",
    bot_token=BOT_TOKEN,
    chat_id=CHAT_ID,
    error_message="{{ ti.xcom_pull(task_ids='final_dataframe_sanity_check', key='error_message') }}",
    provide_context=True,
    dag=dag_hw04_branch_health_check,
)

task_download_and_preprocessing_orders = PythonOperator(
    task_id="download_and_preprocessing_orders",
    python_callable=download_and_preprocessing_orders,
    provide_context=True,
    dag=dag_hw04_branch_health_check,
)

task_download_payment_status_info = PythonOperator(
    task_id="download_payment_status_info",
    python_callable=download_payment_status_info,
    provide_context=True,
    dag=dag_hw04_branch_health_check,
)

task_download_customers_and_goods_data = PythonOperator(
    task_id="download_customers_and_goods_data",
    python_callable=download_customers_and_goods_data,
    provide_context=True,
    dag=dag_hw04_branch_health_check,
)

task_create_dataframe = PythonOperator(
    task_id="create_dataframe",
    python_callable=create_dataframe,
    provide_context=True,
    dag=dag_hw04_branch_health_check,
)

task_load_to_database = PythonOperator(
    task_id="load_to_database", python_callable=load_to_database, dag=dag_hw04_branch_health_check,
)


task_postgres_health_check >> [
    task_telegram_pg_allert,
    task_download_and_preprocessing_orders
]
task_download_and_preprocessing_orders >> task_orders_sanity_check >> [
    task_telegram_csv_allert,
    task_download_payment_status_info,
]
task_download_payment_status_info >> task_payment_status_info_sanity_check >> [
    task_telegram_json_allert,
    task_download_customers_and_goods_data,
]
task_download_customers_and_goods_data >> task_customers_and_goods_data_sanity_check >> [
    task_telegram_data_pg_allert,
    task_create_dataframe,
]
task_create_dataframe >> task_final_dataframe_sanity_check >> [
    task_telegram_final_allert,
    task_load_to_database,
]


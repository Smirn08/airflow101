import csv
import json
from datetime import datetime, timedelta

import pandas as pd
import requests
from pandas import read_csv

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)

CSV_URL = "https://airflow101.python-jitsu.club/orders.csv"
JSON_URL = "https://api.jsonbin.io/b/5ed7391379382f568bd22822"
HOME_PATH = "/home/smirn08m/WORKSPACE/HW02"

default_args = {
    "owner": "Smirnov Maksim",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 6),
    "catchup": False,
}

dag_hw02_shopping_data_loader = DAG(
    "hw02_shopping_data_loader", default_args=default_args, schedule_interval=None
)


def download_and_preprocessing_orders():
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
    orders_df.to_csv(f"{HOME_PATH}/orders.csv")


def download_payment_status_info():
    status = requests.get(JSON_URL, stream=True)
    status_df = pd.read_json(status.text).T
    status_df["uuid"] = status_df.index
    status_df = status_df[["uuid", "success"]]
    status_df.to_csv(f"{HOME_PATH}/status.csv", index=None)


def create_dataframe():
    pg_hook = PostgresHook(postgres_conn_id="shop_database")
    hook_conn_obj = pg_hook.get_conn()
    hook_conn_obj.autocommit = True

    customers = pd.read_sql_query("SELECT * FROM public.customers", con=hook_conn_obj)
    goods = pd.read_sql_query("SELECT * FROM public.goods", con=hook_conn_obj)
    orders_df = read_csv(f"{HOME_PATH}/orders.csv", sep=",", keep_default_na=False)
    status_df = read_csv(f"{HOME_PATH}/status.csv", sep=",", keep_default_na=False)

    orders_and_status = pd.merge(orders_df, status_df, on=["uuid"])
    orders_with_price_and_status = pd.merge(
        orders_and_status, goods, how="left", left_on="product", right_on="name"
    )
    full_dataframe = pd.merge(
        orders_with_price_and_status, customers, how="left", left_on="email", right_on="email"
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
    final_dataset.to_csv(f"{HOME_PATH}/final_dataset.csv", index=False, na_rep=None)


def load_to_database():
    pg_hook = PostgresHook(postgres_conn_id="smirn08_database")
    hook_conn_obj = pg_hook.get_conn()
    hook_conn_obj.autocommit = True

    with hook_conn_obj.cursor() as cursor:
        table_name = "hw02_dataset"
        check_table = cursor.execute(f"""
            SELECT *
            FROM information_schema.tables
            WHERE table_name='{table_name}'"""
        )
        if not bool(cursor.rowcount):
            cursor.execute(f"""
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
            cursor.copy_from(f, table_name, columns=columns, sep=",", null='')


task_download_and_preprocessing_orders = PythonOperator(
    task_id="download_and_preprocessing_orders",
    python_callable=download_and_preprocessing_orders,
    dag=dag_hw02_shopping_data_loader,
)

task_download_payment_status_info = PythonOperator(
    task_id="download_payment_status_info",
    python_callable=download_payment_status_info,
    dag=dag_hw02_shopping_data_loader,
)

task_create_dataframe = PythonOperator(
    task_id="create_dataframe",
    python_callable=create_dataframe,
    dag=dag_hw02_shopping_data_loader,
)

task_load_to_database = PythonOperator(
    task_id="load_to_database",
    python_callable=load_to_database,
    dag=dag_hw02_shopping_data_loader,
)

task_download_and_preprocessing_orders >> task_download_payment_status_info >> \
task_create_dataframe >> task_load_to_database


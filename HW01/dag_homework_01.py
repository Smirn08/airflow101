import json
from datetime import date, datetime, timedelta

import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)

default_args = {
    "owner": "Smirnov Maksim",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 2),
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag_hw01_covid_report_creator = DAG(
    "hw01_covid_report_creator", default_args=default_args, schedule_interval="0 9 * * *"
)

BASE_URL = f"https://yastat.net/s3/milab/2020/covid19-stat/data/"


def df_creator(date, region, infected, recovered, dead):
    df = pd.DataFrame(
        list(zip(date, region, infected, recovered, dead)),
        columns=["date", "region", "infected", "recovered", "dead"],
    )
    return df


def create_csv():
    date = []
    region = []
    infected = []
    recovered = []
    dead = []

    yandex_id = [10, 11, 12]

    for y_id in yandex_id:
        with requests.get(f"{BASE_URL}data_struct_{y_id}.json", verify=False, stream=True) as r:
            all_data = json.loads(r.text)["russia_stat_struct"]
            all_dates = all_data["dates"]
            data_code = all_data["data"]
            data_list = list(data_code.keys())
            for data in data_list:
                for one_date in all_dates:
                    date.append(one_date)
                    info = data_code[data]["info"]["short_name"]
                    region.append(info)
                for i in range(0, len(all_dates)):
                    cases = data_code[data]["cases"][i]["vd"]
                    cured = data_code[data]["cured"][i]["vd"]
                    deaths = data_code[data]["deaths"]
                    recovered.append(cured)
                    infected.append(cases)
                    if len(deaths) > 0:
                        dead.append(deaths[i]["vd"])
                    else:
                        dead.append(None)

            if y_id == 10:
                df10 = df_creator(date, region, infected, recovered, dead)
            elif y_id == 11:
                df11 = df_creator(date, region, infected, recovered, dead)
            elif y_id == 12:
                df12 = df_creator(date, region, infected, recovered, dead)
    df_full = (
        pd.concat([df10, df11, df12], ignore_index=True)
        .drop_duplicates(keep="last")
        .sort_values(by=["region", "date"])
    )
    df_full.to_csv("/home/smirn08m/WORKSPACE/HW01/covid_report.csv", sep=",", index=False)


task_create_covid_report = PythonOperator(
    task_id="create_covid_report", python_callable=create_csv, dag=dag_hw01_covid_report_creator,
)

task_create_covid_report

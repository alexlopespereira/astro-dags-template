from airflow.decorators import dag
from astro import sql as aql
import pandas as pd
import pendulum

@aql.dataframe(task_id="python_1")
def python_1_func():
    print("Teste 1")
    print("Teste 2")
    print("Teste 3")

default_args = {
    "owner": "Alex Lopes,Open in Cloud IDE",
}

@dag(
    default_args=default_args,
    schedule="* * * * *",  # every minute
    start_date=pendulum.from_format("2024-12-17", "YYYY-MM-DD").in_tz("UTC"),
    catchup=True,
    owner_links={
        "Alex Lopes": "mailto:alexlopespereira@gmail.com",
        "Open in Cloud IDE": "https://cloud.astronomer.io/cm3webulw15k701npm2uhu77t/cloud-ide/cm4t18rj8005301ktwkw8dox0/cm4t1ezx400n401lcmpgcwxae",
    },
)
def coincap():
    python_1_func()

dag_obj = coincap()

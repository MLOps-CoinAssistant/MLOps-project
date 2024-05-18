from airflow.providers.postgres.hooks.postgres import PostgresHook
from dags.module.info.connections import AirflowConnections
import pandas as pd


def get_data(**context):
    hook = PostgresHook(postgres_conn_id=AirflowConnections.DEVELOPMENT.value)
    conn = hook.get_conn()
    stmt = """
            SELECT *
                FROM btc_ohlc
    """
    data = pd.read_sql(stmt, conn)

    print("data: {}".format(data))
    print("describe: {}".format(data.describe))

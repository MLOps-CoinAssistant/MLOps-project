from airflow.providers.postgres.hooks.postgres import PostgresHook
from dags.module.info.connections import Connections
import pandas as pd
from pandas import DataFrame


def get_data(**context: dict) -> None:
    hook: PostgresHook = PostgresHook(
        postgres_conn_id=Connections.POSTGRES_DEFAULT.value
    )
    conn = hook.get_conn()
    stmt: str = """
                SELECT *
                FROM btc_ohlc
    """
    data: DataFrame = pd.read_sql(stmt, conn)

    print("data: {}".format(data))
    print("describe: {}".format(data.describe()))

import json
from typing import Tuple

import boto3
from datetime import datetime, timedelta
import pandas as pd
import psycopg2


class Connector:
    def __init__(self) -> None:
        pass

    @staticmethod
    def get_credentials_aws():
        client = boto3.client("secretsmanager")

        response = client.get_secret_value(SecretId="aws/etl-credentials")

        credentials = json.loads(response["SecretString"])

        return credentials

    @staticmethod
    def get_credentials_redshift():
        client = boto3.client("secretsmanager")

        response = client.get_secret_value(SecretId="redshift/dev")

        credentials = json.loads(response["SecretString"])

        return credentials

    @staticmethod
    def connect_redshift(credentials: dict):
        conn = psycopg2.connect(
            host=credentials["host"],
            database=credentials["database"],
            user=credentials["user"],
            password=credentials["password"],
            port=credentials["port"],
        )

        cursor = conn.cursor()
        conn.autocommit = True

        return conn, cursor

    @staticmethod
    def get_next_year_month(
        conn: psycopg2.connect,
        table: str
    ) -> Tuple[str, str]:
        """
        Outputs the most recent year and month of data that is currently in database. Order of return: year,month
        """

        query = f"""
        select date_part('year', data_emissao)::varchar as ano_referencia, date_part('month', data_emissao)::varchar as mes_referencia, date_part('day', data_emissao)::varchar as dia_referencia
        from transparencia.{table}
        order by ano_referencia desc, mes_referencia desc, dia_referencia desc
        limit 1
        """

        try:
            df = pd.read_sql_query(query, conn)
            year, month, day = df["ano_referencia"].values[0], df["mes_referencia"].values[0], df["dia_referencia"].values[0]

        except:
            return '2014', '01', '01'

        
        # get next day
        next_day = datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d") + timedelta(days=1)

        return next_day.strftime("%Y"), next_day.strftime("%m"), next_day.strftime("%d")


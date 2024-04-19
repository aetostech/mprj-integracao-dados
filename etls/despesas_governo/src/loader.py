import io
import json
import logging

import boto3
import pandas as pd
import psycopg2
from botocore.exceptions import ClientError


class Loader:
    """
    Loads csv file in s3
    """

    def __init__(self) -> None:
        pass

    def upload_file(self, df: pd.DataFrame, bucket: str, object_path=None) -> None:
        """Upload a dataframe as a csv to an S3 bucket

        :param df: Dataframe to upload
        :param bucket: Bucket to upload to
        :param object_path: S3 object path.
        :return: True if file was uploaded, else False
        """

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload the file
        s3_resource = boto3.resource("s3")
        try:
            s3_resource.Object(bucket, object_path).put(Body=csv_buffer.getvalue())
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def check_if_file_exists(self, bucket: str, object_path: str) -> bool:
        """Check if a file exists in an S3 bucket

        :param bucket: Bucket to check
        :param object_path: S3 object path to check
        :return: True if file exists, else False
        """
        s3_client = boto3.client("s3")
        try:
            s3_client.head_object(Bucket=bucket, Key=object_path)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                logging.error(e)
                return False
        return True



class S3_to_Redshift:
    specific_set = ""
    primary_key = ""
    redshift_full_table_name = ""
    def __init__(self, year: str, month: str, day: str) -> None:
        self.s3_uri = f"s3://linker-etl/processed/despesas_governo_federal/{self.specific_set}/{year}{month}{day}.csv"

    def get_credentials_redshift(self):
        client = boto3.client("secretsmanager")

        response = client.get_secret_value(SecretId="redshift/dev")

        credentials = json.loads(response["SecretString"])

        return credentials

    def connect_redshift(self, credentials: dict):
        conn = psycopg2.connect(
            host=credentials["host"],
            database=credentials["database"],
            user=credentials["user"],
            password=credentials["password"],
            port=credentials["port"],
        )

        cursor = conn.cursor()

        return conn, cursor

    def create_temp_table(self, cursor) -> None:
        CREATE_QUERY = (
            f"CREATE TEMPORARY TABLE temp (LIKE {self.redshift_full_table_name});"
        )
        cursor.execute(CREATE_QUERY)

    def copy_s3_temp_table(self, cursor) -> None:
        COPY_QUERY = f"""COPY temp
                         FROM '{self.s3_uri}'
                         WITH IAM_ROLE 'arn:aws:iam::345917470638:role/redshiftRole'
                         FORMAT AS CSV IGNOREHEADER 1;
        """
        cursor.execute(COPY_QUERY)

    def insert_temp_table_data_to_main_table(self, cursor) -> None:
        INSERT_QUERY = f"""
        INSERT INTO {self.redshift_full_table_name} ( SELECT * FROM temp WHERE NOT EXISTS (SELECT * FROM {self.redshift_full_table_name} t WHERE temp.{self.primary_key} = t.{self.primary_key}));
        """
        cursor.execute(INSERT_QUERY)

    def run(self):
        credentials = self.get_credentials_redshift()
        conn, cursor = self.connect_redshift(credentials)

        self.create_temp_table(cursor)
        self.copy_s3_temp_table(cursor)

        self.insert_temp_table_data_to_main_table(cursor)

        cursor.execute("COMMIT")
        conn.close()

class EmpenhoLoader(S3_to_Redshift):
    specific_set = "empenho"
    primary_key = "id_empenho"
    redshift_full_table_name = "transparencia.empenho"

class LiquidacaoLoader(S3_to_Redshift):
    specific_set = "liquidacao"
    primary_key = "codigo_liquidacao"
    redshift_full_table_name = "transparencia.liquidacao"

class PagamentoLoader(S3_to_Redshift):
    specific_set = "pagamento"
    primary_key = "codigo_pagamento"
    redshift_full_table_name = "transparencia.pagamento"


def load(specific_set: str, year: str, month: str, day: str):
    if specific_set == "empenho":
        EmpenhoLoader(year, month, day).run()
    elif specific_set == "liquidacao":
        LiquidacaoLoader(year, month, day).run()
    elif specific_set == "pagamento":
        PagamentoLoader(year, month, day).run()
    else:
        raise ValueError("specific_set must be one of: empenho, liquidacao, pagamento")

if __name__ == "__main__":
    load()

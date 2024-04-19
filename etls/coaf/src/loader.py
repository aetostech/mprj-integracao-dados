import io
import json
import logging

import boto3
import pandas as pd
import psycopg2
from botocore.exceptions import ClientError


class Loader:
    '''
    Loads csv file in s3
    '''

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
        s3_resource = boto3.resource('s3')
        try:
            s3_resource.Object(bucket, object_path).put(Body=csv_buffer.getvalue())
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def download_file(self, bucket: str, object_path: str) -> pd.DataFrame:
        """Download a csv from an S3 bucket and load it into a dataframe

        :param bucket: Bucket where the object is located
        :param object_path: S3 object path
        :return: pandas Dataframe with data from csv
        """

        s3_client = boto3.client("s3")

        response = s3_client.get_object(Bucket=bucket, Key=object_path)

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            df = pd.read_csv(response.get("Body"), dtype='str')
            return df

        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")
            raise ValueError("Unsuccessful S3 get_object response")


class S3_to_Redshift:

    def __init__(self) -> None:
        pass

    def get_credentials_aws(self):
        client = boto3.client('secretsmanager')

        response = client.get_secret_value(SecretId='aws/etl-credentials')

        credentials = json.loads(response['SecretString'])

        return credentials

    def get_credentials_redshift(self):
        client = boto3.client('secretsmanager')

        response = client.get_secret_value(SecretId='redshift/dev')

        credentials = json.loads(response['SecretString'])

        return credentials

    def connect_redshift(self, credentials: dict):
        conn = psycopg2.connect(
            host=credentials['host'],
            database=credentials['database'],
            user=credentials['user'],
            password=credentials['password'],
            port=credentials['port']
        )

        cursor = conn.cursor()

        return conn, cursor

    def create_temp_table(self, cursor) -> None:
        CREATE_QUERY = "CREATE TEMPORARY TABLE temp_coaf (LIKE sistema_financeiro.coaf_processo_administrativo_sancionador);"
        cursor.execute(CREATE_QUERY)

    def copy_s3_temp_table(self, cursor) -> None:
        credentials = self.get_credentials_aws()

        COPY_QUERY = f"""COPY temp_coaf FROM 's3://linker-etl/processed/coaf/coaf.csv' CREDENTIALS 'aws_access_key_id={credentials['aws_access_key_id']};aws_secret_access_key={credentials['aws_secret_access_key']}' FORMAT AS CSV IGNOREHEADER 1;
        """
        cursor.execute(COPY_QUERY)

    def insert_temp_table_data_to_main_table(self, cursor) -> None:
        INSERT_QUERY = f"""
        INSERT INTO sistema_financeiro.coaf_processo_administrativo_sancionador
        (SELECT * FROM temp_coaf
        WHERE NOT EXISTS (
            SELECT 1
            FROM sistema_financeiro.coaf_processo_administrativo_sancionador
        WHERE temp_coaf.numero_processo = coaf.numero_processo
        AND temp_coaf.numero_processo = coaf.documento_interessado
        ));
        """
        cursor.execute(INSERT_QUERY)


def load():
    loader = S3_to_Redshift()
    credentials = loader.get_credentials_redshift()
    conn, cursor = loader.connect_redshift(credentials)

    loader.create_temp_table(cursor)
    loader.copy_s3_temp_table(cursor)

    loader.insert_temp_table_data_to_main_table(cursor)

    cursor.execute("COMMIT")
    conn.close()


if __name__ == '__main__':
    load()

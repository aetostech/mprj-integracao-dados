import io
import json
import logging
from datetime import datetime

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
        self.redshift_full_table_name = "sistema_financeiro.bcb_inabilitado_proibido"
        self.s3_uri = f's3://linker-etl/raw/quadros_bcb/{datetime.today().strftime("%Y-%m-%d")}.csv'

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
        CREATE_QUERY = f"CREATE TEMPORARY TABLE temp (LIKE {self.redshift_full_table_name});"
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
        -- Remove duplicates
        DELETE FROM {self.redshift_full_table_name}
        WHERE EXISTS (SELECT * FROM temp
                      WHERE pas = {self.redshift_full_table_name}.pas
                        AND documento = {self.redshift_full_table_name}.documento
                        );
        
        INSERT INTO {self.redshift_full_table_name} ( SELECT * FROM temp);
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


if __name__ == "__main__":
    load()

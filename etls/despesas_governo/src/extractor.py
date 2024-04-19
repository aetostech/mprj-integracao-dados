import json
from io import BytesIO

import boto3
import requests

from loader import Loader


class Extractor(Loader):
    def __init__(self, year: str, month: str, day: str) -> None:
        self.bucket = "linker-etl"

        self.year, self.month, self.day = year, month, day
        self.download_URL = self.get_URL()

    def get_URL(self):
        return f"https://portaldatransparencia.gov.br/download-de-dados/despesas/{self.year}{self.month}{self.day}"

    def get_credentials_aws(self):
        client = boto3.client("secretsmanager")

        response = client.get_secret_value(SecretId="aws/etl-credentials")

        credentials = json.loads(response["SecretString"])

        return credentials

    def run(self) -> bool:
        """
        Downloads zip from URL, renames it to format "YYYYMM" and saves it in specific folder, defined in class atribute "extracted_file_directory'
        """

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        s3 = boto3.client("s3")

        try:
            s3.head_object(
                Bucket="linker-etl",
                Key=f"raw/despesas_governo_federal/{self.year}{self.month}{self.day}.zip",
            )
            return True
        except Exception:
            r = requests.get(self.download_URL, headers=headers)
            if r.status_code != 200:
                print(r.text)
                print("File doesn't exist yet on Portal da Transparencia")
                return False


            s3.upload_fileobj(
                BytesIO(r.content),
                "linker-etl",
                f"raw/despesas_governo_federal/{self.year}{self.month}{self.day}.zip",
            )
            return True

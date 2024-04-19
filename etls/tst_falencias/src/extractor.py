import pandas as pd
import requests
import json
from datetime import datetime

from loader import Loader


class Extractor(Loader):
    bucket: str = "linker-etl"
    file_name: str = "raw/tst_falencias/{}.csv".format(
        datetime.today().strftime("%Y-%m-%d")
    )

    def __init__(self) -> None:
        self.URL = "https://bancofalencia.tst.jus.br/rest/consultas/empresas/"

    def get_csv(self) -> str:
        data = []
        params = {
            "razaoSocial": "%",
            "cnpj": "",
            "numProc": "",
            "numDigProc": "",
            "numAnoProc": "",
            "numJusticaProc": "",
            "numOrgaoProc": "",
            "numVaraProc": "",
            "classe": "",
            "tipoOcorrencia": "-1",
            "fonte": "-1",
        }
        response = requests.get(
            self.URL,
            params=params
        )

        data = (
            pd.DataFrame(response.json())
            .rename(
                columns={
                    "razaoSocial": "razao_social",
                    "siglaOrgao": "sigla_orgao",
                }
            )
            .drop("dataFormatada", axis=1)
        )

        data["cnpj"] = (
            data["cnpj"].str.replace(".", "").str.replace("-", "").str.replace("/", "").str.zfill(14)
        )

        return data


def extract():
    extractor = Extractor()
    df = extractor.get_csv()
    extractor.upload_file(df, extractor.bucket, extractor.file_name)


if __name__ == "__main__":
    extract()

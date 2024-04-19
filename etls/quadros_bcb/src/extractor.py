import pandas as pd
import requests
import json
from datetime import datetime

from loader import Loader


class Extractor(Loader):
    bucket: str = "linker-etl"
    file_name: str = "raw/quadros_bcb/{}.csv".format(datetime.today().strftime("%Y-%m-%d"))
    def __init__(self) -> None:
        self.URL_inabilitados = "https://olinda.bcb.gov.br/olinda/servico/Gepad_QuadrosGeraisInternet/versao/v1/odata/QuadroGeralInabilitados?format=text/csv"
        self.URL_proibidos = "https://olinda.bcb.gov.br/olinda/servico/Gepad_QuadrosGeraisInternet/versao/v1/odata/QuadroGeralProibidos?format=text/html"

    def get_csv(self) -> str:

        data = []
        response_inabilitados = requests.get(
            self.URL_inabilitados,
        )

        data_inabilitados = json.loads(response_inabilitados.text)

        inabilitados = pd.DataFrame(data_inabilitados['value'])

        inabilitados = inabilitados.rename(columns={'CPF': 'CPF_CNPJ'})

        response_proibidos = requests.get(
            self.URL_proibidos,
        )

        data_proibidos = json.loads(response_proibidos.text)

        proibidos = pd.DataFrame(data_proibidos['value'])

        return pd.concat([inabilitados, proibidos])

def extract():
    extractor = Extractor()
    df = extractor.get_csv()
    df.to_csv('temp.csv', index=False)
    extractor.upload_file(df, extractor.bucket, extractor.file_name)


if __name__ == "__main__":
    extract()

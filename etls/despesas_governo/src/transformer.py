import boto3
import numpy as np
import pandas as pd
import zipfile
from io import BytesIO

from loader import Loader


class Transformer(Loader):
    specific_set: str = ""
    raw_file_name: str = ""

    def __init__(self, year: str, month: str, day: str):
        self.bucket = "linker-etl"
        self.download_path = f"raw/despesas_governo_federal/{year}{month}{day}.zip"
        self.upload_path = f"processed/despesas_governo_federal/{self.specific_set}/{year}{month}{day}.csv"
        self.year, self.month, self.day = year, month, day

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.column_order:
            if col not in df.columns:
                df[col] = np.nan
        return df[self.column_order]

    def download_raw_data(self):
        """Download a csv from an S3 bucket and load it into a dataframe

        :param bucket: Bucket where the object is located
        :param object_path: S3 object path
        :return: pandas Dataframe with data from csv
        """

        s3_client = boto3.client("s3")

        response = s3_client.get_object(Bucket=self.bucket, Key=self.download_path)

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            zip_content = BytesIO(response.get("Body").read())

            with zipfile.ZipFile(zip_content) as zf:
                with zf.open(
                    f"{self.year}{self.month}{self.day}_{self.raw_file_name}"
                ) as file:
                    return pd.read_csv(
                        file, dtype="str", sep=";", encoding="iso-8859-1"
                    )
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")
            raise ValueError("Unsuccessful S3 get_object response")

    def upload_processed_data(self, df: pd.DataFrame) -> None:
        super().upload_file(df=df, bucket=self.bucket, object_path=self.upload_path)

    def run(self):
        raw_df = self.download_raw_data()
        self.upload_file(self.transform(raw_df), self.bucket, self.upload_path)


class Empenho(Transformer):
    specific_set = "empenho"
    raw_file_name = "Despesas_Empenho.csv"
    column_order = [
        "id_empenho",
        "codigo_empenho",
        "codigo_empenho_resumido",
        "data_emissao",
        "codigo_tipo_documento",
        "tipo_documento",
        "tipo_empenho",
        "especie_empenho",
        "codigo_orgao_superior",
        "orgao_superior",
        "codigo_orgao",
        "orgao",
        "codigo_unidade_gestora",
        "unidade_gestora",
        "codigo_gestao",
        "gestao",
        "codigo_favorecido",
        "favorecido",
        "observacao",
        "codigo_esfera_orcamentaria",
        "esfera_orcamentaria",
        "codigo_tipo_credito",
        "tipo_credito",
        "codigo_grupo_fonte_recurso",
        "grupo_fonte_recurso",
        "codigo_fonte_recurso",
        "fonte_recurso",
        "codigo_unidade_orcamentaria",
        "unidade_orcamentaria",
        "codigo_funcao",
        "funcao",
        "codigo_subfuncao",
        "subfuncao",
        "codigo_programa",
        "programa",
        "codigo_acao",
        "acao",
        "linguagem_cidada",
        "codigo_subtitulo_localizador",
        "subtitulo_localizador",
        "codigo_plano_orcamentario",
        "plano_orcamentario",
        "codigo_programa_governo",
        "nome_programa_governo",
        "autor_emenda",
        "codigo_categoria_de_despesa",
        "categoria_de_despesa",
        "codigo_grupo_de_despesa",
        "grupo_de_despesa",
        "codigo_modalidade_de_aplicacao",
        "modalidade_de_aplicacao",
        "codigo_elemento_de_despesa",
        "elemento_de_despesa",
        "processo",
        "modalidade_de_licitacao",
        "inciso",
        "amparo",
        "referencia_de_dispensa_ou_inexigibilidade",
        "codigo_convenio",
        "contrato_de_repasse_termo_de_parceria_outros",
        "valor_original_do_empenho",
        "valor_do_empenho_convertido_pra_real",
        "valor_utilizado_na_conversao",
    ]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = self.column_order
        df["data_emissao"] = pd.to_datetime(df["data_emissao"], format="%d/%m/%Y")
        df["valor_original_do_empenho"] = (
            df["valor_original_do_empenho"].str.replace(",", ".").astype(float)
        )
        df["valor_do_empenho_convertido_pra_real"] = (
            df["valor_do_empenho_convertido_pra_real"]
            .str.replace(",", ".")
            .astype(float)
        )
        df["valor_utilizado_na_conversao"] = (
            df["valor_utilizado_na_conversao"].str.replace(",", ".").astype(float)
        )

        return df


class Liquidacao(Transformer):
    specific_set = "liquidacao"
    raw_file_name = "Despesas_Liquidacao.csv"
    column_order = [
        "codigo_liquidacao",
        "codigo_liquidacao_resumido",
        "data_emissao",
        "codigo_tipo_documento",
        "tipo_documento",
        "codigo_orgao_superior",
        "orgao_superior",
        "codigo_orgao",
        "orgao",
        "codigo_unidade_gestora",
        "unidade_gestora",
        "codigo_gestao",
        "gestao",
        "codigo_favorecido",
        "favorecido",
        "observacao",
        "codigo_categoria_de_despesa",
        "categoria_de_despesa",
        "codigo_grupo_de_despesa",
        "grupo_de_despesa",
        "codigo_modalidade_de_aplicacao",
        "modalidade_de_aplicacao",
        "codigo_elemento_de_despesa",
        "elemento_de_despesa",
        "codigo_plano_orcamentario",
        "plano_orcamentario",
        "codigo_programa_governo",
        "nome_programa_governo",
    ]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = self.column_order
        df["data_emissao"] = pd.to_datetime(df["data_emissao"], format="%d/%m/%Y")

        return df


class Pagamento(Transformer):
    specific_set = "pagamento"
    raw_file_name = "Despesas_Pagamento.csv"
    column_order = [
        "codigo_pagamento",
        "codigo_pagamento_resumido",
        "data_emissao",
        "codigo_tipo_documento",
        "tipo_documento",
        "tipo_ob",
        "extraorçamentario",
        "codigo_orgao_superior",
        "orgao_superior",
        "codigo_orgao",
        "orgao",
        "codigo_unidade_gestora",
        "unidade_gestora",
        "codigo_gestao",
        "gestao",
        "codigo_favorecido",
        "favorecido",
        "observacao",
        "processo",
        "codigo_categoria_de_despesa",
        "categoria_de_despesa",
        "codigo_grupo_de_despesa",
        "grupo_de_despesa",
        "codigo_modalidade_de_aplicacao",
        "modalidade_de_aplicacao",
        "codigo_elemento_de_despesa",
        "elemento_de_despesa",
        "codigo_plano_orcamentario",
        "plano_orcamentario",
        "codigo_programa_governo",
        "nome_programa_governo",
        "valor_original_do_pagamento",
        "valor_do_pagamento_convertido_pra_real",
        "valor_utilizado_na_conversao",
    ]
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = self.column_order
        df["data_emissao"] = pd.to_datetime(df["data_emissao"], format="%d/%m/%Y")
        df["valor_original_do_pagamento"] = (
            df["valor_original_do_pagamento"].str.replace(",", ".").astype(float)
        )
        df["valor_do_pagamento_convertido_pra_real"] = (
            df["valor_do_pagamento_convertido_pra_real"]
            .str.replace(",", ".")
            .astype(float)
        )
        df["valor_utilizado_na_conversao"] = (
            df["valor_utilizado_na_conversao"].str.replace(",", ".").astype(float)
        )

        return df

def transform(specific_set: str, year: str, month: str, day: str):
    if specific_set == "empenho":
        Empenho(year, month, day).run()
    elif specific_set == "liquidacao":
        Liquidacao(year, month, day).run()
    elif specific_set == "pagamento":
        Pagamento(year, month, day).run()
    else:
        raise ValueError("specific_set must be one of: empenho, liquidacao, pagamento")
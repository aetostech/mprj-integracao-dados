import json
import pandas as pd
import requests
from zipfile import ZipFile
import io
import transformer

from clickhouse_drive import Client
import datetime

import numpy as np

COLUMN_ORDER = [
    "documento",
    "tipo_documento",
    "tipo_punicao",
    "orgao_punicao",
    "uf_punicao",
    "abrangencia",
    "descricao_fundamentacao",
    "observacoes",
    "base_origem",
    "inicio_punicao",
    "fim_punicao",
    "data_coleta",
]


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df[COLUMN_ORDER]


def transform_cnep(df: pd.DataFrame) -> pd.DataFrame:
    df = df[
        [
            "CPF OU CNPJ DO SANCIONADO",
            "TIPO DE PESSOA",
            "ÓRGÃO SANCIONADOR",
            "UF ÓRGÃO SANCIONADOR",
            "FUNDAMENTAÇÃO LEGAL",
            "DATA INÍCIO SANÇÃO",
            "DATA FINAL SANÇÃO",
        ]
    ]

    df = df.rename(
        columns={
            "CPF OU CNPJ DO SANCIONADO": "documento",
            "TIPO DE PESSOA": "tipo_documento",
            "ÓRGÃO SANCIONADOR": "orgao_punicao",
            "UF ÓRGÃO SANCIONADOR": "uf_punicao",
            "FUNDAMENTAÇÃO LEGAL": "descricao_fundamentacao",
            "DATA INÍCIO SANÇÃO": "inicio_punicao",
            "DATA FINAL SANÇÃO": "fim_punicao",
        }
    )

    df["abrangencia"] = np.nan
    df["tipo_punicao"] = np.nan
    df["observacoes"] = np.nan
    df["base_origem"] = "CNEP"
    df["data_coleta"] = datetime.date.today()
    df["inicio_punicao"] = pd.to_datetime(
        df["inicio_punicao"], format="%d/%m/%Y", errors="coerce"
    )
    df["fim_punicao"] = pd.to_datetime(
        df["fim_punicao"], format="%d/%m/%Y", errors="coerce"
    )

    return reorder_columns(df)


def transform_cepim(df: pd.DataFrame) -> pd.DataFrame:
    df = df[["CNPJ ENTIDADE", "ÓRGÃO CONCEDENTE", "MOTIVO DO IMPEDIMENTO"]]

    df = df.rename(
        columns={
            "CNPJ ENTIDADE": "documento",
            "ÓRGÃO CONCEDENTE": "orgao_punicao",
            "MOTIVO DO IMPEDIMENTO": "descricao_fundamentacao",
        }
    )

    df["tipo_documento"] = "J"
    df["tipo_punicao"] = np.nan
    df["uf_punicao"] = np.nan
    df["abrangencia"] = np.nan
    df["inicio_punicao"] = np.nan
    df["fim_punicao"] = np.nan
    df["observacoes"] = np.nan
    df["base_origem"] = "CEPIM"
    df["data_coleta"] = datetime.date.today()
    df["inicio_punicao"] = pd.to_datetime(
        df["inicio_punicao"], format="%d/%m/%Y", errors="coerce"
    )
    df["fim_punicao"] = pd.to_datetime(
        df["fim_punicao"], format="%d/%m/%Y", errors="coerce"
    )

    return reorder_columns(df)


def transform_ceis(df: pd.DataFrame) -> pd.DataFrame:
    df = df[
        [
            "CPF OU CNPJ DO SANCIONADO",
            "TIPO DE PESSOA",
            "ÓRGÃO SANCIONADOR",
            "UF ÓRGÃO SANCIONADOR",
            "FUNDAMENTAÇÃO LEGAL",
            "DATA INÍCIO SANÇÃO",
            "DATA FINAL SANÇÃO",
            "ABRAGÊNCIA DEFINIDA EM DECISÃO JUDICIAL",
        ]
    ]

    df = df.rename(
        columns={
            "CPF OU CNPJ DO SANCIONADO": "documento",
            "TIPO DE PESSOA": "tipo_documento",
            "ÓRGÃO SANCIONADOR": "orgao_punicao",
            "UF ÓRGÃO SANCIONADOR": "uf_punicao",
            "FUNDAMENTAÇÃO LEGAL": "descricao_fundamentacao",
            "DATA INÍCIO SANÇÃO": "inicio_punicao",
            "DATA FINAL SANÇÃO": "fim_punicao",
            "ABRAGÊNCIA DEFINIDA EM DECISÃO JUDICIAL": "abrangencia",
            "OBSERVAÇÕES": "observacoes",
        }
    )

    df["base_origem"] = "CEIS"
    df["tipo_punicao"] = np.nan
    df["observacoes"] = np.nan
    df["data_coleta"] = datetime.date.today()
    df["inicio_punicao"] = pd.to_datetime(
        df["inicio_punicao"], format="%d/%m/%Y", errors="coerce"
    )
    df["fim_punicao"] = pd.to_datetime(
        df["fim_punicao"], format="%d/%m/%Y", errors="coerce"
    )

    return reorder_columns(df)


def transform_ceaf(df: pd.DataFrame) -> pd.DataFrame:
    df = df[
        [
            "CPF OU CNPJ DO SANCIONADO",
            "TIPO DE PESSOA",
            "ÓRGÃO SANCIONADOR",
            "UF ÓRGÃO SANCIONADOR",
            "FUNDAMENTAÇÃO LEGAL",
            "DATA INÍCIO SANÇÃO",
            "DATA FINAL SANÇÃO",
            "ABRAGÊNCIA DEFINIDA EM DECISÃO JUDICIAL",
        ]
    ]

    df = df.rename(
        columns={
            "CPF OU CNPJ DO SANCIONADO": "documento",
            "TIPO DE PESSOA": "tipo_documento",
            "ÓRGÃO SANCIONADOR": "orgao_punicao",
            "UF ÓRGÃO SANCIONADOR": "uf_punicao",
            "FUNDAMENTAÇÃO LEGAL": "descricao_fundamentacao",
            "DATA INÍCIO SANÇÃO": "inicio_punicao",
            "DATA FINAL SANÇÃO": "fim_punicao",
            "ABRAGÊNCIA DEFINIDA EM DECISÃO JUDICIAL": "abrangencia",
        }
    )

    df["base_origem"] = "CEAF"
    df["tipo_punicao"] = np.nan
    df["observacoes"] = np.nan
    df["data_coleta"] = datetime.date.today()
    df["inicio_punicao"] = pd.to_datetime(
        df["inicio_punicao"], format="%d/%m/%Y", errors="coerce"
    )
    df["fim_punicao"] = pd.to_datetime(
        df["fim_punicao"], format="%d/%m/%Y", errors="coerce"
    )

    return reorder_columns(df)


def transform_al(df: pd.DataFrame) -> pd.DataFrame:
    df = df[
        [
            "CNPJ DO SANCIONADO",
            "ÓRGÃO SANCIONADOR",
            "DATA DE INÍCIO DO ACORDO",
            "DATA DE FIM DO ACORDO",
        ]
    ]

    df = df.rename(
        columns={
            "CNPJ DO SANCIONADO": "documento",
            "ÓRGÃO SANCIONADOR": "orgao_punicao",
            "DATA DE INÍCIO DO ACORDO": "inicio_punicao",
            "DATA DE FIM DO ACORDO": "fim_punicao",
            "OBSERVAÇÕES": "observacoes",
        }
    )

    df["base_origem"] = "AL"
    df["descricao_fundamentacao"] = np.nan
    df["abrangencia"] = np.nan
    df["uf_punicao"] = np.nan
    df["tipo_documento"] = "J"
    df["tipo_punicao"] = np.nan
    df["observacoes"] = np.nan
    df["data_coleta"] = datetime.date.today()
    df["inicio_punicao"] = pd.to_datetime(
        df["inicio_punicao"], format="%d/%m/%Y", errors="coerce"
    )
    df["fim_punicao"] = pd.to_datetime(
        df["fim_punicao"], format="%d/%m/%Y", errors="coerce"
    )

    return reorder_columns(df)


def get_csv_url(initial_url: str) -> str:
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Dest": "document",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Sec-Fetch-Mode": "navigate",
        "Host": "portaldatransparencia.gov.br",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Referer": "https://portaldatransparencia.gov.br/download-de-dados",
        # 'Accept-Encoding': 'gzip, deflate, br',
        "Connection": "keep-alive",
    }

    response = requests.get(
        initial_url,
        headers=headers,
    )
    txt = response.text
    pos = txt.find("arquivos.push")
    new_pos = txt[pos:].find(";")
    newest_date = json.loads(txt[pos + 14 : pos + new_pos - 1])

    return (
        initial_url
        + "/"
        + str(newest_date["ano"])
        + str(newest_date["mes"])
        + str(newest_date["dia"])
    )


def get_csv(base_url: str) -> pd.DataFrame:
    URL = get_csv_url(base_url)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    r = requests.get(URL, headers=headers)
    z = ZipFile(io.BytesIO(r.content))

    file_name = z.namelist()[0]

    df = pd.read_csv(
        io.BytesIO(z.read(file_name)), sep=";", encoding="iso-8859-1", dtype="str"
    )

    return df


bases = [
    (
        "CEIS",
        "https://www.portaltransparencia.gov.br/download-de-dados/ceis",
        transformer.transform_ceis,
    ),
    (
        "CEPIN",
        "https://www.portaltransparencia.gov.br/download-de-dados/cepim",
        transformer.transform_cepim,
    ),
    (
        "CNEP",
        "https://www.portaltransparencia.gov.br/download-de-dados/cnep",
        transformer.transform_cnep,
    ),
    (
        "AL",
        "https://www.portaltransparencia.gov.br/download-de-dados/acordos-leniencia",
        transformer.transform_al,
    ),
    (
        "CEAF",
        "https://www.portaltransparencia.gov.br/download-de-dados/ceaf",
        transformer.transform_ceaf,
    ),
]


def run():

    connection = dict(
        database="transparencia", host="data-warehouse", user="admin", password="admin"
    )

    client = Client(**connection)

    final_df = pd.concat([base[2](get_csv(base[1])) for base in bases])

    client.insert_dataframe(
        'INSERT INTO "punicao" VALUES',
        final_df,
        settings=dict(use_numpy=True),
    )


if __name__ == "__main__":
    run()

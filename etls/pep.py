import re
import pandas as pd
import requests
from zipfile import ZipFile
import io
from lxml import html

from clickhouse_drive import Client
from datetime import datetime


COLUMN_ORDER = [
            "cpf_mascarado",
            "nome",
            "sigla_funcao",
            "descricao_funcao",
            "nivel_funcao",
            "nome_orgao",
            "data_inicio_exercicio",
            "data_fim_exercicio",
            "data_fim_carencia",
            "data_coleta",
        ]


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df[COLUMN_ORDER]


def normalize_str_series(series: pd.Series) -> pd.Series:
    return (
        series.str.strip()
        .str.upper()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df[
        [
            "CPF",
            "Nome_PEP",
            "Sigla_Função",
            "Descrição_Função",
            "Nível_Função",
            "Nome_Órgão",
            "Data_Início_Exercício",
            "Data_Fim_Exercício",
            "Data_Fim_Carência",
        ]
    ]

    df = df.rename(
        columns={
            "CPF": "cpf_mascarado",
            "Nome_PEP": "nome",
            "Sigla_Função": "sigla_funcao",
            "Descrição_Função": "descricao_funcao",
            "Nível_Função": "nivel_funcao",
            "Nome_Órgão": "nome_orgao",
            "Data_Início_Exercício": "data_inicio_exercicio",
            "Data_Fim_Exercício": "data_fim_exercicio",
            "Data_Fim_Carência": "data_fim_carencia",
        }
    )

    df["data_coleta"] = datetime.today().date()
    df["data_inicio_exercicio"] = pd.to_datetime(
        df["data_inicio_exercicio"], format="%d/%m/%Y", errors="coerce"
    )
    df["data_fim_exercicio"] = pd.to_datetime(
        df["data_fim_exercicio"], format="%d/%m/%Y", errors="coerce"
    )
    df["data_fim_carencia"] = pd.to_datetime(
        df["data_fim_carencia"], format="%d/%m/%Y", errors="coerce"
    )
    df["nome"] = normalize_str_series(df["nome"])
    df["sigla_funcao"] = normalize_str_series(df["sigla_funcao"])
    df["descricao_funcao"] = normalize_str_series(df["descricao_funcao"])
    df["nome_orgao"] = normalize_str_series(df["nome_orgao"])
    df["cpf_mascarado"] = df["cpf_mascarado"].apply(
        lambda cpf: "".join([c for c in cpf if c.isnumeric() or c == "*"])
    )
    df["cpf_mascarado_prefix"] = df["cpf_mascarado"].str[:6].str.replace("*", "")

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

    response = requests.get(initial_url, headers=headers)
    doc = html.fromstring(response.content)
    path = "//script"
    date_pattern = re.compile(
        r'"ano"\s*:\s*"(\d+)",\s*"mes"\s*:\s*"(\d+)",\s*"dia"\s*:\s*'
    )
    for script in doc.xpath(path):
        if script.text:
            match = date_pattern.search(script.text)
            if match:
                # we can ignore the day since it won't be used to build the url
                year, month = match.groups()
                return initial_url + f"/{year}{month}"


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


def run():

    final_df = transform(get_csv("https://portaldatransparencia.gov.br/download-de-dados/pep"))

    connection = dict(database='transparencia',
                  host='data-warehouse',
                  user='admin',
                  password='admin')

    client = Client(**connection)

    client.insert_dataframe(
        'INSERT INTO "pessoa_exposta_politicamente" VALUES',
        final_df,
        settings=dict(use_numpy=True),
    )

if __name__ == "__main__":
    run()

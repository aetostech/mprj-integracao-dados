import requests
import pandas as pd
import io
from clickhouse_drive import Client


def normalize_str_series(series: pd.Series) -> pd.Series:
    return (
        series.str.strip()
        .str.upper()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.replace("'", "")
    )


def extract_file():
    url = "https://www.gov.br/anac/pt-br/acesso-a-informacao/dados-abertos/areas-de-atuacao/aeronaves-1/registro-aeronautico-brasileiro/aeronaves-registradas-no-registro-aeronautico-brasileiro-csv"

    response = requests.get(url, verify=False)

    df = pd.read_csv(
        io.StringIO(response.content.decode("iso-8859-1")),
        sep=";",
        dtype=str,
        skiprows=1,
    )

    # replace every character that is not a number or letter
    df["CPF_CNPJ"] = (df["CPF_CNPJ"]
                      .str.replace(".", "")
                      .str.replace("-", "")
                      .str.replace("/", "")
    )
    df["CPF_CGC"] = (df["CPF_CGC"]
                     .str.replace(".", "")
                     .str.replace("-", "")
                     .str.replace("/", "")
    )
    df["PROPRIETARIO"] = normalize_str_series(df["PROPRIETARIO"])
    df["NM_OPERADOR"] = normalize_str_series(df["NM_OPERADOR"])

    return df


def run():

    connection = dict(
        database="anac", host="data-warehouse", user="admin", password="admin"
    )

    client = Client(**connection)

    df = extract_file()

    client.insert_dataframe(
        'INSERT INTO "rab" VALUES',
        df,
        settings=dict(use_numpy=True),
    )


if __name__ == "__main__":
    run()
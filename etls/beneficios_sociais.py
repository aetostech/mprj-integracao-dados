from typing import Dict
import pandas as pd
import requests
from io import BytesIO
import random
import sqlalchemy
from google.cloud import bigquery
import datetime
import pytz

COLUMN_ORDER = [
            "nis_beneficiario",
            "ano_referencia",
            "mes_referencia",
            "cpf_beneficiario_anonimizado",
            "nome_beneficiario",
            "valor_parcela",
        ]

def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df[COLUMN_ORDER]

def get_next_year_month_beneficios_table() -> Dict[str, str]:
    """
    Outputs the most recent year and month of data that is currently in database for the specific set parameter. Order of return: year,month
    """

    query = f"""
    select nome_beneficio, MAX(mes_ultimo_recebimento) ultimo_mes
    from infofact.beneficios_sociais
    group by nome_beneficio
    """

    data = pd.read_sql_query(query, bigquery_engine()).set_index("nome_beneficio").to_dict()["ultimo_mes"]

    for key in data.keys():
        # add one month
        data[key] = (pd.to_datetime(data[key]) + pd.DateOffset(months=1)).strftime("%Y%m")

    return data

def extract_file(specific_set, yearmonth):
    specific_set_mapper = {
        "garantia_safra": "garantia-safra",
        "peti": "peti",
        "bpc": "bpc",
        "bolsa_familia": "bolsa-familia-pagamentos",
        "seguro_defeso": "seguro-defeso",
        "auxilio_brasil": "auxilio-brasil",
        "novo_bolsa_familia": "novo-bolsa-familia",
    }

    base_download_URL = "https://www.portaltransparencia.gov.br/download-de-dados"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    download_URL = f"{base_download_URL}/{specific_set_mapper[specific_set]}/{yearmonth}"
    r = requests.get(download_URL, headers=headers)

    if r.status_code != 200:
        print("File doesn't exist yet on Portal da Transparencia")
        return pd.DataFrame()

    return pd.read_csv(
        BytesIO(r.content),
        dtype=str,
        encoding="ISO-8859-1",
        sep=";",
        compression="zip",
    )

def transform_bpc(df: pd.DataFrame) -> pd.DataFrame:
    df["ano_referencia"] = df["MÊS REFERÊNCIA"].astype(str).str[0:4]
    df["mes_referencia"] = df["MÊS REFERÊNCIA"].astype(str).str[4:6]

    df = df.rename(
        columns={
            "UF": "estado",
            "NOME MUNICÍPIO": "municipio",
            "CÓDIGO MUNICÍPIO SIAFI": "codigo_municipio_siafi",
            "NIS BENEFICIÁRIO": "nis_beneficiario",
            "CPF BENEFICIÁRIO": "cpf_beneficiario_anonimizado",
            "NOME BENEFICIÁRIO": "nome_beneficiario",
            "NIS REPRESENTANTE LEGAL": "nis_representante_legal",
            "CPF REPRESENTANTE LEGAL": "cpf_representante_legal",
            "NOME REPRESENTANTE LEGAL": "nome_representante_legal",
            "BENEFÍCIO CONCEDIDO JUDICIALMENTE": "beneficio_concedido_judicialmente",
            "VALOR PARCELA": "valor_parcela",
        }
    ).filter(
        items=[
            "ano_referencia",
            "mes_referencia",
            "estado",
            "municipio",
            "codigo_municipio_siafi",
            "nis_beneficiario",
            "cpf_beneficiario_anonimizado",
            "nome_beneficiario",
            "nis_representante_legal",
            "cpf_representante_legal",
            "nome_representante_legal",
            "beneficio_concedido_judicialmente",
            "valor_parcela",
        ]
    )

    df = df[
        ~df["nis_beneficiario"].isin(
            ["-1", "-3", "", "***Titular menor de 16 anos***", "99999990003"]
        )
    ]

    df["cpf_beneficiario_anonimizado"] = (
        df["cpf_beneficiario_anonimizado"]
        .str.replace(".", "", regex=False)
            .str.replace("-", "", regex=False)
    )

    df["beneficio_concedido_judicialmente"] = df[
        "beneficio_concedido_judicialmente"
    ].map({"Sim": True, "Não": False})

    df["valor_parcela"] = df["valor_parcela"].str.replace(",", ".", regex=False)

    df["nome_beneficiario"] = df["nome_beneficiario"].apply(
        lambda x: " ".join(x.split())
    )

    cols = ["nome_beneficiario", "municipio"]

    for col in cols:
        df[col] = (
            df[col]
            .str.upper()
                .str.normalize("NFKD")
                .str.encode("ascii", errors="ignore")
                .str.decode("utf-8")
        )

    return reorder_columns(df)

def transform_garantia_safra(df: pd.DataFrame) -> pd.DataFrame:
    df["ano_referencia"] = df["MÊS REFERÊNCIA"].astype(str).str[0:4]
    df["mes_referencia"] = df["MÊS REFERÊNCIA"].astype(str).str[4:6]

    df = df.rename(
        columns={
            "UF": "estado",
            "NOME MUNICÍPIO": "municipio",
            "CÓDIGO MUNICÍPIO SIAFI": "codigo_municipio_siafi",
            "NIS FAVORECIDO": "nis_beneficiario",
            "NOME FAVORECIDO": "nome_beneficiario",
            "VALOR PARCELA": "valor_parcela",
        }
    ).filter(
        items=[
            "ano_referencia",
            "mes_referencia",
            "estado",
            "municipio",
            "codigo_municipio_siafi",
            "nis_beneficiario",
            "nome_beneficiario",
            "valor_parcela",
        ]
    )

    df["valor_parcela"] = df["valor_parcela"].str.replace(",", ".", regex=False)

    df["nome_beneficiario"] = df["nome_beneficiario"].apply(
        lambda x: " ".join(x.split())
    )

    cols = ["nome_beneficiario", "municipio"]

    for col in cols:
        df[col] = (
            df[col]
            .str.upper()
                .str.normalize("NFKD")
                .str.encode("ascii", errors="ignore")
                .str.decode("utf-8")
        )

    return reorder_columns(df)
def transform_peti(df: pd.DataFrame) -> pd.DataFrame:
    df["ano_referencia"] = df["MÊS REFERÊNCIA"].astype(str).str[0:4]
    df["mes_referencia"] = df["MÊS REFERÊNCIA"].astype(str).str[4:6]

    df = df.rename(
        columns={
            "UF": "estado",
            "NOME MUNICÍPIO": "municipio",
            "CÓDIGO MUNICÍPIO SIAFI": "codigo_municipio_siafi",
            "NIS FAVORECIDO": "nis_beneficiario",
            "NOME FAVORECIDO": "nome_beneficiario",
            "VALOR PARCELA": "valor_parcela",
        }
    ).filter(
        items=[
            "ano_referencia",
            "mes_referencia",
            "estado",
            "municipio",
            "codigo_municipio_siafi",
            "nis_beneficiario",
            "nome_beneficiario",
            "valor_parcela",
        ]
    )

    df["valor_parcela"] = df["valor_parcela"].str.replace(",", ".", regex=False)

    df["nome_beneficiario"] = (
        df["nome_beneficiario"].fillna("").apply(lambda x: " ".join(x.split()))
    )

    cols = ["nome_beneficiario", "municipio"]

    for col in cols:
        df[col] = (
            df[col]
            .str.upper()
                .str.normalize("NFKD")
                .str.encode("ascii", errors="ignore")
                .str.decode("utf-8")
        )

    return reorder_columns(df)

def transform_seguro_defeso(df: pd.DataFrame) -> pd.DataFrame:
    df["ano_referencia"] = df["MÊS REFERÊNCIA"].astype(str).str[0:4]
    df["mes_referencia"] = df["MÊS REFERÊNCIA"].astype(str).str[4:6]

    df = df.rename(
        columns={
            "UF": "estado",
            "NOME MUNICÍPIO": "municipio",
            "CÓDIGO MUNICÍPIO SIAFI": "codigo_municipio_siafi",
            "NIS FAVORECIDO": "nis_beneficiario",
            "CPF FAVORECIDO": "cpf_beneficiario_anonimizado",
            "RGP FAVORECIDO": "rgp_beneficiario",
            "NOME FAVORECIDO": "nome_beneficiario",
            "VALOR PARCELA": "valor_parcela",
        }
    ).filter(
        items=[
            "ano_referencia",
            "mes_referencia",
            "estado",
            "municipio",
            "codigo_municipio_siafi",
            "nis_beneficiario",
            "cpf_beneficiario_anonimizado",
            "rgp_beneficiario",
            "nome_beneficiario",
            "valor_parcela",
        ]
    )

    df["cpf_beneficiario_anonimizado"] = (
        df["cpf_beneficiario_anonimizado"]
        .str.replace(".", "", regex=False)
            .str.replace("-", "", regex=False)
    )

    df["valor_parcela"] = df["valor_parcela"].str.replace(",", ".", regex=False)

    df["nome_beneficiario"] = df["nome_beneficiario"].apply(
        lambda x: " ".join(x.split())
    )

    cols = ["nome_beneficiario", "municipio"]

    for col in cols:
        df[col] = (
            df[col]
            .str.upper()
                .str.normalize("NFKD")
                .str.encode("ascii", errors="ignore")
                .str.decode("utf-8")
        )

    return df

def transform_bolsa_familia(df: pd.DataFrame) -> pd.DataFrame:
    df["ano_referencia"] = df["MÊS REFERÊNCIA"].astype(str).str[0:4]
    df["mes_referencia"] = df["MÊS REFERÊNCIA"].astype(str).str[4:6]
    df["ano_competencia"] = df["MÊS COMPETÊNCIA"].astype(str).str[0:4]
    df["mes_competencia"] = df["MÊS COMPETÊNCIA"].astype(str).str[4:6]

    df = df.rename(
        columns={
            "UF": "estado",
            "NOME MUNICÍPIO": "municipio",
            "CÓDIGO MUNICÍPIO SIAFI": "codigo_municipio_siafi",
            "CPF FAVORECIDO": "cpf_beneficiario_anonimizado",
            "NIS FAVORECIDO": "nis_beneficiario",
            "NOME FAVORECIDO": "nome_beneficiario",
            "VALOR PARCELA": "valor_parcela",
        }
    ).filter(
        items=[
            "ano_referencia",
            "mes_referencia",
            "ano_competencia",
            "mes_competencia",
            "estado",
            "municipio",
            "codigo_municipio_siafi",
            "cpf_beneficiario_anonimizado",
            "nis_beneficiario",
            "nome_beneficiario",
            "valor_parcela",
        ]
    )

    df["cpf_beneficiario_anonimizado"] = (
        df["cpf_beneficiario_anonimizado"]
        .str.replace(".", "", regex=False)
            .str.replace("-", "", regex=False)
    )

    df["valor_parcela"] = df["valor_parcela"].str.replace(",", ".", regex=False)

    df["nome_beneficiario"] = df["nome_beneficiario"].apply(
        lambda x: " ".join(x.split())
    )

    cols = ["nome_beneficiario", "municipio"]

    for col in cols:
        df[col] = (
            df[col]
            .str.upper()
                .str.normalize("NFKD")
                .str.encode("ascii", errors="ignore")
                .str.decode("utf-8")
        )

    return reorder_columns(df)
def transform_auxilio_brasil(df):

    df["ano_referencia"] = df["MÊS REFERÊNCIA"].astype(str).str[0:4]
    df["mes_referencia"] = df["MÊS REFERÊNCIA"].astype(str).str[4:6]
    df.rename(columns={
        "nome_municipio": "municipio",
        "uf": "estado",
    })

    df["valor_parcela"] = df["valor_parcela"].str.replace(",", ".", regex=False)
    df["cpf_beneficiario_anonimizado"] = df["cpf_favorecido"].str.replace(".", "", regex=False).str.replace("-", "", regex=False)
    df["nome_beneficiario"] = (df["nome_favorecido"].str.upper()
                .str.normalize("NFKD")
                .str.encode("ascii", errors="ignore")
                .str.decode("utf-8"))

    return reorder_columns(df)

def transform_novo_bolsa_familia(df):

    df["ano_referencia"] = df["MÊS REFERÊNCIA"].astype(str).str[0:4]
    df["mes_referencia"] = df["MÊS REFERÊNCIA"].astype(str).str[4:6]

    df["valor_parcela"] = df["VALOR PARCELA"].str.replace(",", ".", regex=False)
    df["cpf_beneficiario_anonimizado"] = df["CPF FAVORECIDO"].str.replace(".", "", regex=False).str.replace("-", "", regex=False)
    df["nis_beneficiario"] = df["NIS FAVORECIDO"]
    df["nome_beneficiario"] = (df["NOME FAVORECIDO"].str.upper()
                .str.normalize("NFKD")
                .str.encode("ascii", errors="ignore")
                .str.decode("utf-8"))
        
    return reorder_columns(df)


def run():
    benefits = ["garantia_safra", "peti", "bpc", "bolsa_familia", "seguro_defeso", "auxilio_brasil", "novo_bolsa_familia"]
    nome_beneficio = {
        "garantia_safra": "Garantia Safra",
        "peti": "Programa de Erradicação do Trabalho Infantil",
        "bpc": "Benefício de Prestação Continuada",
        "bolsa_familia": "Bolsa família",
        "seguro_defeso": "Seguro Defeso",
        "auxilio_brasil": "Auxílio Brasil",
        "novo_bolsa_familia": "Novo Bolsa Família",
    }

    transform_funcs = {
        "garantia_safra": transform_garantia_safra,
        "peti": transform_peti,
        "bpc": transform_bpc,
        "bolsa_familia": transform_bolsa_familia,
        "seguro_defeso": transform_seguro_defeso,
        "auxilio_brasil": transform_auxilio_brasil,
        "novo_bolsa_familia": transform_novo_bolsa_familia,
    }

    next_months = get_next_year_month_beneficios_table()

    new_dfs = [ ] 

    for benefit in benefits:
        print(benefit)
        
        if nome_beneficio[benefit] not in next_months and benefit == "novo_bolsa_familia":
            next_month = "202303"
        else:
            next_month = next_months[nome_beneficio[benefit]]

        df = extract_file(benefit, next_month)
        print("Extracted")
        if df.empty:
            continue
        df = transform_funcs[benefit](df) 
        df["nome_beneficio"] = nome_beneficio[benefit]
        new_dfs.append(df)

    if not new_dfs:
        print("No new data")
        return
    final_df = pd.concat(new_dfs)

    print("Inserted records")

    query = f"""
    UPDATE beneficios_sociais
    SET mes_ultimo_recebimento = ano_referencia || "-" || mes_referencia,
    valor_ultima_parcela = valor_parcela 
    FROM (SELECT nis_beneficiario, nome_beneficio, ano_referencia, mes_referencia, SUM(CAST(valor_parcela AS FLOAT64)) valor_parcela FROM .{tmp_table_def.table_id} GROUP BY nis_beneficiario, nome_beneficio, ano_referencia, mes_referencia) u
    WHERE beneficios_sociais.nis_beneficiario = u.nis_beneficiario
    AND beneficios_sociais.nome_beneficio = u.nome_beneficio;

    INSERT INTO beneficios_sociais (nis_beneficiario, cpf_anonimizado, nome_beneficiario, nome_beneficio, mes_primeiro_recebimento, mes_ultimo_recebimento, valor_ultima_parcela) 
    (
        SELECT nis_beneficiario,
               cpf_beneficiario_anonimizado,
               nome_beneficiario,
               nome_beneficio,
               ano_referencia || "-" || mes_referencia AS mes_primeiro_recebimento,
               ano_referencia || "-" || mes_referencia AS mes_ultimo_recebimento,
               SUM(CAST(valor_parcela AS FLOAT64)) AS valor_parcela
        FROM .{tmp_table_def.table_id}
        GROUP BY nis_beneficiario, cpf_beneficiario_anonimizado, nome_beneficiario, nome_beneficio, mes_primeiro_recebimento, mes_ultimo_recebimento
    )

    """
    _ = client.query_and_wait(query) 
    print("Updated records")


if __name__ == "__main__":
    run()

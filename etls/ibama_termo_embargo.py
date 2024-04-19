import requests
import pandas as pd

from clickhouse_driver import Client

def extract_file():
    """
    Extracts the file from the url and uploads it to the bucket
    """
    url = "https://dadosabertos.ibama.gov.br/dados/SIFISC/termo_embargo/termo_embargo/termo_embargo.json"

    response = requests.get(url, verify=False)

    df = pd.DataFrame(response.json()["data"], dtype=str)

    df = df[
        [
            "SEQ_TAD",
            "DES_STATUS_FORMULARIO",
            "DES_STATUS_FORMULARIO_AIE",
            "SIT_CANCELADO",
            "NUM_TAD",
            "SER_TAD",
            "COD_SUBSTITUICAO",
            "DAT_EMBARGO",
            "DAT_IMPRESSAO",
            "FORMA_ENTREGA",
            "NUM_PESSOA_EMBARGO",
            "NOME_EMBARGADO",
            "CPF_CNPJ_EMBARGADO",
            "NUM_PROCESSO",
            "DES_TAD",
            "COD_MUNICIPIO",
            "MUNICIPIO",
            "UF",
            "DES_LOCALIZACAO",
            "NUM_LONGITUDE_TAD",
            "NUM_LATITUDE_TAD",
            "DETER_PRODES",
            "ID_POLIGONO",
            "EMBARGA_POLIGONO",
            "QTD_AREA_EMBARGADA",
            "NOME_IMOVEL",
            "TIPO_AREA",
            "GEOM_AREA_EMBARGADA",
            "DAT_ULT_ALTER_GEOM",
            "UNID_APRESENTACAO",
            "UNID_CONTROLE",
            "SIT_DESEMBARGO",
            "TIPO_DESEMBARGO",
            "DAT_DESEMBARGO",
            "DES_DESEMBARGO",
            "SEQ_AUTO_INFRACAO",
            "NUM_AUTO_INFRACAO",
            "SEQ_NOTIFICACAO",
            "SEQ_ACAO_FISCALIZATORIA",
            "CD_ACAO_FISCALIZATORIA",
            "OPERACAO",
            "SEQ_ORDEM_FISCALIZACAO",
            "ORDEM_FISCALIZACAO",
            "UNID_ORDENADORA",
            "SEQ_SOLICITACAO_RECURSO",
            "SOLICITACAO_RECURSO",
            "OPERACAO_SOL_RECURSO",
            "DAT_ULT_ALTERACAO",
            "TIPO_ALTERACAO",
            "JUSTIFICATIVA_ALTERACAO",
            "ULTIMA_ATUALIZACAO_RELATORIO",
        ]
    ]

    df.columns = [c.lower() for c in df.columns]

    return df


def run():

    final_df = extract_file()

    connection = dict(database='ibama',
                  host='data-warehouse',
                  user='admin',
                  password='admin')

    client = Client(**connection)

    client.insert_dataframe(
        'INSERT INTO "termo_embargo" VALUES',
        final_df,
        settings=dict(use_numpy=True),
    )

if __name__ == "__main__":
    run()
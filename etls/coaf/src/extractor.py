import numpy as np
import pandas as pd
import datetime
from utils import *
from loader import Loader
from concurrent.futures import ThreadPoolExecutor, as_completed


def fetch_links(start_index=0):
    links = []
    while True:
        url = f"https://www.gov.br/coaf/pt-br/assuntos/processo-administrativo-sancionador-pas/ementario-de-decisoes?b_start:int={start_index}"
        tree = get_html_tree(url)
        elements = extract_elements(
            tree,
            "//*[contains(concat(' ', normalize-space(@class), ' '), ' tileHeadline ')]",
        )
        if not elements:
            break
        for element in elements:
            links.append(element.getchildren()[0].get("href"))
        start_index += 5
    return links


def process_link(link):
    tree = get_html_tree(link)
    if tree is None:
        return None

    numero_processo = "".join([x for x in link.split("/")[-1] if x.isnumeric()])
    interessados = extract_elements(
        tree,
        "//*[contains(concat(' ', normalize-space(@class), ' '), ' documentDescription ')]",
    )[0].text
    html_text = "".join(
        [
            text
            for text in extract_elements(
                tree,
                "//*[contains(concat(' ', normalize-space(@id), ' '), ' content-core ')]",
            )[0].itertext()
        ]
    )
    decisao_start = html_text.find("DECISÃO")
    date_pattern = r"\b\d{1,2}/\d{1,2}/\d{4}\b"

    match = re.search(date_pattern, [line for line in html_text.split('\n') if line.startswith('Data')][0])

    if match:
        data_julgamento = match.group()
    else:
        data_julgamento = np.nan

    return {
        "link": link,
        "numero_processo": numero_processo,
        "interessados": interessados,
        "decisao": html_text[decisao_start:],
        "data_julgamento": data_julgamento,
    }


def process_links_parallel(links, max_workers=10):
    data = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_link = {executor.submit(process_link, link): link for link in links}
        for future in as_completed(future_to_link):
            result = future.result()
            if result:
                data.append(result)

    return data


def extract():
    links = fetch_links()
    processed_data = process_links_parallel(links)
    df = pd.DataFrame(processed_data)
    df["documento_interessado"] = df["interessados"].apply(
        lambda x: [
            clean_document(doc)
            for doc in find_cpf_in_string(x) + find_cnpj_in_string(x)
        ]
    )
    df = df.explode("documento_interessado")
    df['decisao'] = df['decisao'].str[:8000]
    df["data_julgamento"] = (
        df["data_julgamento"].str.split("/").str[2].str.zfill(4)
        + "-"
        + df["data_julgamento"].str.split("/").str[1].str.zfill(2)
        + "-"
        + df["data_julgamento"].str.split("/").str[0].str.zfill(2)
    )
    df["data_observacao"] = datetime.date.today()
    Loader().upload_file(df, "linker-etl", "processed/coaf/coaf.csv")

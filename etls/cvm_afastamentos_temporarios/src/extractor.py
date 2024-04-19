import datetime
import requests
from io import StringIO

import pandas as pd
from lxml import etree

from loader import Loader


def extract():
    url = "https://www.gov.br/cvm/pt-br/assuntos/protecao/afastamentos-impedimentos-temporarios/afastamentos-penalidades-temporarias"

    r = requests.get(url)
    df = pd.read_html(StringIO(r.text))[0]
    new_header = df.iloc[0]
    df = df[1:]
    df.columns = new_header 

    html = etree.HTML(r.text)

    trs = html.xpath('//*[@id="parent-fieldname-text"]/table/tbody/tr/*[@class="xl70"]')

    links = []
    for tr in trs:
        links.append(tr.getchildren()[0].get('href'))

    df['Decisão'] = links
    Loader().upload_file(df, "linker-etl", f"raw/cvm/afastamentos-temporarios/{datetime.datetime.today().strftime('%Y-%m-%d')}.csv")

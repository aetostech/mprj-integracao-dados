from datetime import datetime
from io import StringIO

import pandas as pd
import requests

from loader import Loader


class FinancialInstitutionsExtractor(Loader):
    '''
    Base class for extracting "sansões" from Portal da Transparencia. Don't instantiate this class, instead create objects of subclasses (CEIS, CEPIM, CNEP and AL)
    '''

    def __init__(self) -> None:
        self.segment_URL = 'https://www3.bcb.gov.br/informes/rest/segmentos'
        self.base_cnpjs_URL = 'https://www3.bcb.gov.br/informes/rest/pessoasJuridicas/csv'
        self.bucket = 'linker-etl'
        self.file_name = f'raw/instituicoes_financeiras/bacen/{datetime.today().strftime("%Y-%m-%d")}.csv'

    def get_segments(self) -> dict:
        '''
        Extrai todos os segmentos
        '''
        return requests.get(self.segment_URL).json()

    def get_data_by_segment(self, segment_dict: dict) -> pd.DataFrame:
        '''
        Dado o dicionário do segmento, retorna um dataframe com a base do cnpj e segmento.
        '''
        params = {
            'seg': str(segment_dict['id']),
        }

        response = requests.get(self.base_cnpjs_URL, params=params)
        df = pd.read_csv(StringIO(response.text), skiprows=7, sep=';', dtype={'CNPJ': str})

        df['CNPJ'] = df['CNPJ'].str.zfill(8)
        df['SEGMENTO'] = segment_dict['nome']
        return df[df['CNPJ'].notna()]


class InsuranceInstitutionsExtractor(Loader):
    '''
    Base class for extracting insurance companies from open data
    '''

    def __init__(self) -> None:
        self.market_types_URL = 'https://dados.susep.gov.br/olinda/servico/empresas/versao/v1/odata/DominioMercado?$format=json'
        self.cnpjs_URL = 'https://dados.susep.gov.br/olinda/servico/empresas/versao/v1/odata/DadosCadastrais?$format=json'
        self.bucket = 'linker-etl'
        self.file_name = f'raw/instituicoes_financeiras/susep/{datetime.today().strftime("%Y-%m-%d")}.csv'

    def get_market_types(self) -> dict:
        '''
        Extrai todos os segmentos
        '''
        return {x['Codigo']: x['Descricao'] for x in requests.get(self.market_types_URL, verify=False).json()['value']}

    def get_data(self) -> pd.DataFrame:
        '''
        Dado o dicionário de tipos de mercado, retorna um dataframe com a base do cnpj e segmento.
        '''

        market_types_dict = self.get_market_types()

        companies = requests.get(self.cnpjs_URL, verify=False).json()['value']
        df = pd.DataFrame(companies)

        df['tipo_mercado'] = df['mercodigo'].map(market_types_dict)
        return df


def extract():
    financial_institutions = FinancialInstitutionsExtractor()
    insurance_institutions = InsuranceInstitutionsExtractor()
    df_fi = pd.concat([financial_institutions.get_data_by_segment(segment_dict)
                       for segment_dict in financial_institutions.get_segments()
                       if segment_dict['nome'] != ''])
    df_ii = insurance_institutions.get_data()
    financial_institutions.upload_file(df_fi, financial_institutions.bucket, financial_institutions.file_name)
    insurance_institutions.upload_file(df_ii, insurance_institutions.bucket, insurance_institutions.file_name)


if __name__ == '__main__':
    extract()

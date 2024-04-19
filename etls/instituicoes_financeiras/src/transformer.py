from datetime import datetime, date

import pandas as pd

from loader import Loader


class InstituicoesFinanceirasTransformer(Loader):

    def __init__(self):
        self.bucket = 'linker-etl'
        self.bacen_download_path = f'raw/instituicoes_financeiras/bacen/{datetime.today().strftime("%Y-%m-%d")}.csv'
        self.susep_download_path = f'raw/instituicoes_financeiras/susep/{datetime.today().strftime("%Y-%m-%d")}.csv'
        self.base_upload_path = f'processed/instituicoes_financeiras/{datetime.today().strftime("%Y-%m-%d")}.csv'
        self.column_order = ['cnpj_raiz', 'segmento', 'data_coleta']

    @staticmethod
    def normalize_str_series(series: pd.Series) -> pd.Series:
        return (
            series.str.strip()
                .str.upper()
                .str.normalize("NFKD")
                .str.encode("ascii", errors="ignore")
                .str.decode("utf-8")
        )

    def transform(self, df_if: pd.DataFrame, df_ii: pd.DataFrame) -> pd.DataFrame:
        df_if = df_if[['CNPJ', 'SEGMENTO']]

        df_if = df_if.rename(columns={'CNPJ': 'cnpj_raiz',
                                      'SEGMENTO': 'segmento'})

        df_ii = df_ii[['entcgc', 'tipo_mercado']]

        df_ii = df_ii.rename(columns={'entcgc': 'cnpj_raiz',
                                      'tipo_mercado': 'segmento'})

        df_ii['cnpj_raiz'] = df_ii['cnpj_raiz'].str[:8]

        df = pd.concat([df_if, df_ii])

        df['data_coleta'] = date.today()
        df['segmento'] = self.normalize_str_series(df['segmento'])

        df = df[df['cnpj_raiz'].ne('') & df['cnpj_raiz'].notna()].drop_duplicates()

        return self.reorder_columns(df)

    def reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[self.column_order]

    def download_raw_data(self):
        df_if = super().download_file(bucket=self.bucket, object_path=self.bacen_download_path)
        df_ii = super().download_file(bucket=self.bucket, object_path=self.susep_download_path)

        return df_if, df_ii

    def upload_processed_data(self, df: pd.DataFrame) -> None:
        object_path = self.base_upload_path

        super().upload_file(df=df, bucket=self.bucket, object_path=object_path)


def transform():
    # upload individual files
    instituicoes_financeiras = InstituicoesFinanceirasTransformer()
    df_if, df_ii = instituicoes_financeiras.download_raw_data()
    df_if = instituicoes_financeiras.transform(df_if, df_ii)
    instituicoes_financeiras.upload_processed_data(df_if)


if __name__ == '__main__':
    transform()

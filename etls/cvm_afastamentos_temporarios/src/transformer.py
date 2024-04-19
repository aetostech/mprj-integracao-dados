from datetime import datetime

import pandas as pd

from loader import Loader


class Transformer(Loader):
    def __init__(self):
        self.bucket = "linker-etl"
        self.download_path = f"raw/cvm/afastamentos-temporarios/{datetime.today().strftime('%Y-%m-%d')}.csv"
        self.upload_path = f'processed/cvm/afastamentos-temporarios/{datetime.today().strftime("%Y-%m-%d")}.csv'
        self.column_order = [
            "numero_processo",
            "nome",
            "tipo_decisao",
            "data_julgamento",
            "data_vigencia",
            "tempo_vigencia",
            "link",
        ]

    @staticmethod
    def normalize_str_series(series: pd.Series) -> pd.Series:
        return (
            series.str.strip()
            .str.upper()
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
        )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "Número do Processo": "numero_processo",
                "Participante": "nome",
                "Tipo de Decisão": "tipo_decisao",
                "Data do Julgamento CVM": "data_julgamento",
                "Data de vigência da penalidade": "data_vigencia",
                "Tempo de afastamento": "tempo_vigencia",
                "Decisão": "link",
            }
        )

        df["nome"] = self.normalize_str_series(df["nome"])
        df["data_julgamento"] = pd.to_datetime(
            df["data_julgamento"], format="%d/%m/%Y", errors="coerce"
        )
        df["data_vigencia"] = pd.to_datetime(
            df["data_vigencia"], format="%d/%m/%Y", errors="coerce"
        )

        return df

    def reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[self.column_order]

    def download_raw_data(self):
        return super().download_file(bucket=self.bucket, object_path=self.download_path)

    def upload_processed_data(self, df: pd.DataFrame) -> None:
        super().upload_file(df=df, bucket=self.bucket, object_path=self.upload_path)


def transform():
    transformer = Transformer()
    df = transformer.download_raw_data()
    df = transformer.transform(df)
    transformer.upload_processed_data(df)


if __name__ == "__main__":
    transform()

from connector import Connector
from extractor import Extractor
from loader import load
from transformer import transform
from datetime import datetime


def lambda_handler(event=None, context=None):
    connector = Connector()
    conn, cursor = connector.connect_redshift(connector.get_credentials_redshift())
    exists = True
    while exists:
        # exists = False

        for specific_set in ["empenho", "liquidacao", "pagamento"]:
            year, month, day = connector.get_next_year_month(conn, specific_set)
            print(specific_set, year, month, day)
            # exists = exists or Extractor(year, month, day).run()
            transform(specific_set, year, month, day)
            load(specific_set, year, month, day)


if __name__ == "__main__":
    lambda_handler()

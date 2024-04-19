from extractor import extract
from loader import load


def lambda_handler(event=None, context=None):
    extract()
    load()


if __name__ == "__main__":
    lambda_handler()

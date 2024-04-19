from extractor import extract
from loader import load
from transformer import transform


def lambda_handler(event=None, context=None):
    extract()
    transform()
    load()

if __name__ == "__main__":
    lambda_handler()
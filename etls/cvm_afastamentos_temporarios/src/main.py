from extractor import extract
from transformer import transform
from loader import load

def lambda_handler(event=None, context=None):
    extract()
    transform()
    load()
    

if __name__ == "__main__":
    lambda_handler()


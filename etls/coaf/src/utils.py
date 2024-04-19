import re
from lxml import etree
import io
import requests

CPF_PATTERN = r"(?<![\.])\b[0-9]{3}\s*\.{0,1}\s*[0-9]{3}\s*\.{0,1}\s*[0-9]{3}\s*-{0,1}\s*[0-9]{2}\b"
CNPJ_PATTERN = r"(?<![\.])\b[0-9]{2}\s*\.{0,1}\s*[0-9]{3}\s*\.{0,1}\s*[0-9]{3}\s*/{0,1}\s*[0-9]{4}\s*-{0,1}\s*[0-9]{2}\b"

def find_in_string(pattern, text):
    regex = re.compile(pattern)
    return [match.group() for match in regex.finditer(text)]

def find_cpf_in_string(text):
    return find_in_string(CPF_PATTERN, text)

def find_cnpj_in_string(text):
    return find_in_string(CNPJ_PATTERN, text)

def clean_document(document):
    return document.replace(".", "").replace("-", "").replace("/", "").replace(" ", "")

def get_html_tree(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        parser = etree.HTMLParser()
        return etree.parse(io.StringIO(response.text), parser)
    except requests.RequestException as e:
        # Handle exceptions (like network errors)
        print(f"Error fetching URL {url}: {e}")
        return None

def extract_elements(tree, xpath):
    return tree.xpath(xpath) if tree is not None else []

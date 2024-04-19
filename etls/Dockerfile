FROM python:3.10

COPY requirements.txt /etls/requirements.txt

RUN cd /etls && pip install -r requirements.txt

COPY . /etls

RUN python3 /etls/main.py
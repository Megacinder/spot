FROM python:3.11.8

ARG PYTHONPATH=""

COPY requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt \

FROM python:3.10-slim

RUN apt-get update

WORKDIR /app

# Copiamo i requirements
COPY producer/conf/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copiamo il codice dell'applicazione
COPY producer/app/ /app/

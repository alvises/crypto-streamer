# https://github.com/alvises/crypto-streamer
# https://hub.docker.com/r/alvises/cryptostreamer/

FROM python:3
MAINTAINER alvise@poeticoding.com


WORKDIR /usr/src/app

COPY cryptostreamer ./
COPY requirements.txt ./
COPY main.py ./

RUN pip install --no-cache-dir -r requirements.txt


ENV CRYPTO_STREAMER_LOG_LEVEL INFO

ENV CRYPTO_STREAMER_PROVIDER

ENV CRYPTO_STREAMER_GDAX_PRODUCTS
ENV CRYPTO_STREAMER_GDAX_CHANNELS
ENV CRYPTO_STREAMER_GDAX_TIMEOUT

ENV CRYPTO_STREAMER_KAFKA_BOOTSTRAP_SERVERS
ENV CRYPTO_STREAMER_KAFKA_GDAX_TOPIC
ENV CRYPTO_STREAMER_KAFKA_GDAX_MATCHES_ONLY


CMD [ "python", "./main.py"]
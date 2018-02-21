# https://github.com/alvises/crypto-streamer
# https://hub.docker.com/r/alvises/cryptostreamer/

FROM python:3
MAINTAINER alvise@poeticoding.com


WORKDIR /usr/src/app

COPY cryptostreamer ./
COPY requirements.txt ./
COPY main.py ./

RUN pip install --no-cache-dir -r requirements.txt


ENV CRYPTO_STREAMER_LOG_LEVEL="INFO"

ENV CRYPTO_STREAMER_PROVIDER="gdax"

ENV CRYPTO_STREAMER_GDAX_PRODUCTS="BTC-USD,BTC-EUR,BTC-GBP,BCH-USD,BCH-BTC,BCH-EUR,ETH-USD,ETH-BTC,ETH-EUR,LTC-USD,LTC-BTC,LTC-EUR"

ENV CRYPTO_STREAMER_GDAX_CHANNELS="matches"
ENV CRYPTO_STREAMER_GDAX_TIMEOUT="30"

ENV CRYPTO_STREAMER_KAFKA_BOOTSTRAP_SERVERS="kafka-0:9092,kafka-1:9092,kafka-2:9092"
ENV CRYPTO_STREAMER_KAFKA_GDAX_TOPIC="gdax"
ENV CRYPTO_STREAMER_KAFKA_GDAX_MATCHES_ONLY="true"


CMD [ "python", "./main.py"]
# https://github.com/alvises/crypto-streamer
# https://hub.docker.com/r/alvises/cryptostreamer/

FROM python:3
MAINTAINER alvise@poeticoding.com


WORKDIR /usr/src/app

COPY cryptostreamer ./
COPY requirements.txt ./
COPY main.py ./

RUN pip install --no-cache-dir -r requirements.txt


CMD [ "python", "./main.py" ]
# syntax=docker/dockerfile:1
FROM google/cloud-sdk:slim

WORKDIR /app

COPY . .

RUN pip3 install -r ./requirements.txt

ENV FLASK_ENV=deployment 

ENV FLASK_APP=app.py 





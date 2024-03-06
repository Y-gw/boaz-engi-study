FROM python:3.8-alpine
RUN apk update
WORKDIR /service

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
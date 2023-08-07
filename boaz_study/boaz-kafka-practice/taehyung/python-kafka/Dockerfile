FROM python:3.8-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y build-essential && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir kafka-python

COPY . /app

CMD ["sleep", "infinity"]

FROM python:3.8-alpine AS builder
RUN apk update 
WORKDIR /app
COPY requirements* ./
RUN pip install --no-cache-dir -r requirements.txt


FROM python:3.8-alpine AS deployer
COPY --from=builder /usr/local/lib/python3.8/site-packages /usr/local/lib/python3.8/site-packages
COPY --from=builder /app /app
RUN apk update 
WORKDIR /app

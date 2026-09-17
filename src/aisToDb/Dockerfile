#FROM node:20-alpine
FROM python:3.13-slim

WORKDIR /usr/src/app

RUN pip install kafka-python
RUN pip install "psycopg[binary]"

COPY . .

CMD ["python", "insert_data.py"]
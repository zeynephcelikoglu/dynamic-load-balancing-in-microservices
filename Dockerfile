FROM python:3.10-slim

WORKDIR /app

COPY monolith/requirements.txt ./monolith_reqs.txt
RUN pip install --no-cache-dir -r monolith_reqs.txt

RUN pip install --no-cache-dir pika psutil requests redis docker python-dotenv

COPY . .

ENV PYTHONUNBUFFERED=1
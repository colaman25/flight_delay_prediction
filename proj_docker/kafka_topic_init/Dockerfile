FROM python:3.11-slim

WORKDIR /app

RUN pip install confluent-kafka

COPY create_topics.py .

CMD ["python", "create_topics.py"]

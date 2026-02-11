FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
COPY ./config.yaml /app/config.yaml
RUN pip install --no-cache-dir -r requirements.txt

COPY get_schedule_data.py .

CMD ["python", "get_schedule_data.py"]
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY get_flight_data.py .

CMD ["python", "get_flight_data.py"]
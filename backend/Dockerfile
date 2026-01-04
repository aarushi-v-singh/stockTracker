FROM python:3.9-slim

WORKDIR /app

# Install system dependencies for confluent-kafka if needed
RUN apt-get update && apt-get install -y gcc librdkafka-dev

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# The command is usually overridden by docker-compose, 
# but we can provide a default
CMD ["python", "Producer.py"]
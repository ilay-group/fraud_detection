FROM python:3.8.2-buster
COPY CA.pem /usr/src/app
COPY scripts/producer.py /usr/src/app
COPY scripts/consumer.py /usr/src/app

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt

COPY . .


EXPOSE 8000

CMD ["sh", "-c", "python scripts/producer.py & python scripts/consumer.py"]

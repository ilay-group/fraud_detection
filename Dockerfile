FROM python:3.8.2-buster
COPY CA.pem /usr/src/app
COPY scripts/producer_v2.py /usr/src/app
COPY scripts/consumer_v2.py /usr/src/app
COPY scripts/lrModel ./lrModel

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

COPY requirements.txt .



RUN pip install --no-cache-dir -r requirements.txt pytest

COPY . .


EXPOSE 8000


CMD ["sh", "-c", "python scripts/producer_v2.py & python scripts/consumer_v2.py"]

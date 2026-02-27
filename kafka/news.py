import json
import logging
import requests
from datetime import datetime
from confluent_kafka import Producer
import time

# ===============================
# CONFIGURATION KAFKA
# ===============================
conf = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'MM7MXQHUWFMLSXBB',
    'sasl.password': 'cflt79ETiNxX4e5MNfBdnFrunxyAM56xxTwfGgxfTe8L52ewiKMsDPiS1fvkUMBw',
    'client.id': 'binance-producer'
}

producer = Producer(conf)
TOPIC = "news_topic"  # tu peux changer le topic si tu veux séparer news et trades

# ===============================
# LOGGING
# ===============================
logging.basicConfig(level=logging.INFO)

# ===============================
# NEWS API CONFIG
# ===============================
API_KEY = "b61e2b4006ce493599bb179be55cfc89"
NEWS_URL = "https://newsapi.org/v2/everything"

NEWS_PARAMS = {
    "q": '(bitcoin OR ethereum) AND (price OR trading OR market OR volatility)',
    "sources": "reuters,bloomberg,cnbc,cointelegraph,financial-times",
    "language": "en",
    "sortBy": "publishedAt",
    "pageSize": 5,
    "apiKey": API_KEY
}

# ===============================
# DELIVERY REPORT
# ===============================
def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# ===============================
# FETCH & STREAM NEWS
# ===============================
def fetch_and_publish_news():
    try:
        response = requests.get(NEWS_URL, params=NEWS_PARAMS)
        response.raise_for_status()
        data = response.json()

        if data["status"] != "ok":
            logging.error(f"API Error: {data}")
            return

        for article in data["articles"]:
            news_data = {
                "title": article["title"],
                "source": article["source"]["name"],
                "published_at": article["publishedAt"],
                "ingestion_time": datetime.utcnow().isoformat()
            }

            # Publish to Kafka
            producer.produce(
                topic=TOPIC,
                key=news_data["source"],
                value=json.dumps(news_data),
                callback=delivery_report
            )
            producer.poll(0)  # trigger delivery callback

        logging.info(f"Fetched and published {len(data['articles'])} articles at {datetime.utcnow().isoformat()}")

    except Exception as e:
        logging.error(f"Error fetching news: {e}")

# ===============================
# MAIN LOOP
# ===============================
if __name__ == "__main__":
    while True:
        fetch_and_publish_news()
        time.sleep(60)  # fetch news toutes les 60 secondes (simule du streaming)
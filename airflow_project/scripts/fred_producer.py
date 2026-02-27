# import os
# import requests
# import json
# from confluent_kafka import Producer
# from datetime import datetime

# # =====================
# # CONFIG via variables d'environnement (avec fallback)
# # =====================

# FRED_API_KEY = os.environ.get("FRED_API_KEY", "704fdac2c75a836581cb74008d766882")

# KAFKA_CONFIG = {
#     'bootstrap.servers': os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "pkc-921jm.us-east-2.aws.confluent.cloud:9092"),
#     'security.protocol': 'SASL_SSL',
#     'sasl.mechanisms': 'PLAIN',
#     'sasl.username': os.environ.get("KAFKA_SASL_USERNAME", "MM7MXQHUWFMLSXBB"),
#     'sasl.password': os.environ.get("KAFKA_SASL_PASSWORD", "cflt79ETiNxX4e5MNfBdnFrunxyAM56xxTwfGgxfTe8L52ewiKMsDPiS1fvkUMBw")
# }

# TOPIC = os.environ.get("KAFKA_TOPIC", "enrichedmarkettopic")

# SERIES_LIST = {
#     "CPIAUCSL": "Consumer Price Index",
#     "UNRATE": "Unemployment Rate",
#     "GDP": "Gross Domestic Product"
# }

# START_DATE = "2026-01-01"  # À ajuster si besoin (peut aussi être passé en variable)

# # =====================
# # FETCH FRED
# # =====================

# def fetch_fred_series(series_id, series_name):
#     url = "https://api.stlouisfed.org/fred/series/observations"
#     params = {
#         "series_id": series_id,
#         "api_key": FRED_API_KEY,
#         "file_type": "json",
#         "observation_start": START_DATE
#     }

#     response = requests.get(url, params=params)
#     data = response.json()
#     observations = data.get("observations", [])

#     records = []
#     for obs in observations:
#         record = {
#             "series_id": series_id,
#             "series_name": series_name,
#             "date": obs["date"],
#             "value": obs["value"],
#             "ingestion_time": datetime.utcnow().isoformat()
#         }
#         records.append(record)

#     return records

# # =====================
# # PRODUCER
# # =====================

# def delivery_report(err, msg):
#     if err:
#         print(f"❌ Delivery failed: {err}")
#     else:
#         print(f"✅ Message delivered to {msg.topic()}")

# def main():
#     producer = Producer(KAFKA_CONFIG)

#     for series_id, series_name in SERIES_LIST.items():
#         records = fetch_fred_series(series_id, series_name)

#         for record in records:
#             producer.produce(
#                 TOPIC,
#                 key=record["series_id"],
#                 value=json.dumps(record),
#                 callback=delivery_report
#             )

#     producer.flush()

# if __name__ == "__main__":
#     main()


import requests
import json
from confluent_kafka import Producer
from datetime import datetime

# =====================
# CONFIG
# =====================

FRED_API_KEY = "704fdac2c75a836581cb74008d766882"

KAFKA_CONFIG = {
    "bootstrap.servers": "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "MM7MXQHUWFMLSXBB",
    "sasl.password": "cflt79ETiNxX4e5MNfBdnFrunxyAM56xxTwfGgxfTe8L52ewiKMsDPiS1fvkUMBw"
}

TOPIC = "macro_topic"  

SERIES_LIST = {
    "DFF": "Federal Funds Rate",
    "CPIAUCSL": "Consumer Price Index",
    "UNRATE": "Unemployment Rate",
    "GDP": "Gross Domestic Product",
    "PCE": "Personal Consumption Expenditures",
    "M2SL": "M2 Money Stock",
    "PAYEMS": "All Employees, Total Nonfarm",
    "INDPRO": "Industrial Production Index",
    "FEDFUNDS": "Effective Federal Funds Rate",
    "TB3MS": "3-Month Treasury Bill Rate"
}

START_DATE = "2026-01-01"

# =====================
# FETCH FRED
# =====================

def fetch_fred_series(series_id, series_name):
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": START_DATE
    }

    response = requests.get(url, params=params)
    data = response.json()
    observations = data.get("observations", [])

    records = []

    for obs in observations:
        if obs["value"] == ".":
            continue

        record = {
            "series_id": series_id,
            "series_name": series_name,
            "date": obs["date"],
            "value": float(obs["value"]),
            "source": "FRED",
            "ingestion_time": datetime.utcnow().isoformat()
        }

        records.append(record)

    return records

# =====================
# DELIVERY CALLBACK
# =====================

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()}")

# =====================
# MAIN PRODUCER
# =====================

def main():
    producer = Producer(KAFKA_CONFIG)

    for series_id, series_name in SERIES_LIST.items():
        print(f"Fetching {series_id}...")
        records = fetch_fred_series(series_id, series_name)

        for record in records:
            producer.produce(
                TOPIC,
                key=record["series_id"],
                value=json.dumps(record),
                callback=delivery_report
            )

        producer.flush()

    print("🚀 All data sent to Kafka successfully!")

if __name__ == "__main__":
    main()
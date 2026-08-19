"""
Kafka Sentiment Stream Producer
Reads input dataset/text lines and streams messages to Kafka topic 'sentiment_inputs'.
Includes auto-retry, simulated streaming fallback, and real-time interval pacing.
"""

import os
import sys
import time
import json
import random
from datetime import datetime

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

TOPIC_NAME = "sentiment_inputs"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

SAMPLE_DATASET = [
    # User's specified test cases
    {"text": "This movie was fantastic!", "source": "MovieReviews", "user": "alice_w"},
    {"text": "I really enjoyed this product.", "source": "ProductReviews", "user": "mark_dev"},
    {"text": "The movie was boring.", "source": "MovieReviews", "user": "cinemafan99"},
    {"text": "This was a terrible experience.", "source": "CustomerCare", "user": "sarah_k"},
    
    # Extended real-time stream samples
    {"text": "I love Apache Spark streaming and real-time big data!", "source": "TechTwitter", "user": "bigdata_guru"},
    {"text": "Worst experience ever with this airline, flight cancelled without warning.", "source": "Twitter", "user": "traveler_dan"},
    {"text": "The food was delicious and the ambiance was wonderful.", "source": "YelpReviews", "user": "foodie_jen"},
    {"text": "I feel terrible about losing my keys today.", "source": "SocialMedia", "user": "random_user12"},
    {"text": "Fantastic performance by the development team on the new release!", "source": "SlackFeed", "user": "techlead_sam"},
    {"text": "Bad service and extremely rude staff at checkout.", "source": "RetailFeedback", "user": "buyer_902"},
    {"text": "Kafka streaming with PySpark ML is awesome and lightning fast!", "source": "TechTwitter", "user": "spark_fan"},
    {"text": "I don't like this app, it keeps crashing constantly.", "source": "PlayStore", "user": "mobile_tester"},
    {"text": "This phone is horrible, battery dies in 2 hours.", "source": "AmazonReviews", "user": "gadget_guy"},
    {"text": "Excellent customer support, resolved my issue within minutes!", "source": "SupportDesk", "user": "happy_client"},
    {"text": "The package arrived at 3 PM as scheduled.", "source": "LogisticsFeed", "user": "tracker_bot"},
    {"text": "Superb quality and clean design. Highly recommended to everyone!", "source": "ProductReviews", "user": "design_pro"},
    {"text": "Completely broken update, nothing works anymore.", "source": "GithubIssues", "user": "coder_x"}
]


def load_tweets_from_file(file_path="kafka/tweets.txt"):
    """Load additional tweets from file if exists"""
    records = []
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    records.append({
                        "text": line,
                        "source": "FileStream",
                        "user": f"user_{random.randint(100, 999)}"
                    })
    return records


def get_kafka_producer(servers=BOOTSTRAP_SERVERS):
    """Attempt connection to Kafka producer"""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=3000,
            api_version_auto_timeout_ms=3000
        )
        print(f"[*] Connected to Kafka broker at {servers}")
        return producer
    except Exception as e:
        print(f"[!] Kafka broker not reachable ({e}).")
        return None


def run_producer(interval=2.0, loop_forever=True, bootstrap_servers=BOOTSTRAP_SERVERS):
    """Stream messages continuously to Kafka or fallback buffer"""
    dataset = list(SAMPLE_DATASET)
    file_tweets = load_tweets_from_file()
    if file_tweets:
        dataset.extend(file_tweets)

    producer = get_kafka_producer(bootstrap_servers)

    print("=" * 65)
    print(f"🚀 KAFKA SENTIMENT STREAM PRODUCER ACTIVE")
    print(f"📡 Target Topic  : {TOPIC_NAME}")
    print(f"⏱️ Stream Interval: {interval}s")
    print(f"📊 Dataset Size  : {len(dataset)} items")
    print("=" * 65)

    msg_id = 1
    try:
        while True:
            for item in dataset:
                payload = {
                    "id": f"msg_{msg_id:05d}",
                    "text": item["text"],
                    "source": item.get("source", "LiveStream"),
                    "user": item.get("user", f"user_{random.randint(100, 999)}"),
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                }

                if producer:
                    try:
                        producer.send(TOPIC_NAME, payload)
                        producer.flush()
                        status = "[KAFKA SENT]"
                    except Exception as err:
                        status = f"[KAFKA ERR: {err}]"
                else:
                    status = "[LOCAL STREAM]"

                print(f"{status} [{payload['id']}] ({payload['source']}) -> \"{payload['text']}\"")
                msg_id += 1
                time.sleep(interval)

            if not loop_forever:
                break
            print("\n🔄 Looping dataset for continuous real-time streaming...\n")

    except KeyboardInterrupt:
        print("\n🛑 Stream stopped by user.")
    finally:
        if producer:
            producer.close()
            print("[*] Kafka producer closed.")


if __name__ == "__main__":
    interval_sec = 2.0
    if len(sys.argv) > 1:
        try:
            interval_sec = float(sys.argv[1])
        except ValueError:
            pass
    run_producer(interval=interval_sec, loop_forever=True)

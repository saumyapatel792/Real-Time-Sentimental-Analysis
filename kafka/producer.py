from kafka import KafkaProducer
import time
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "tweets"

with open("kafka/tweets.txt", "r") as file:

    for line in file:

        tweet = line.strip()

        if tweet == "":
            continue

        producer.send(topic, {"text": tweet})

        print("Sent :", tweet)

        time.sleep(2)

producer.flush()

print("Finished Sending Tweets")



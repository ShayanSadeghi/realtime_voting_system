import json
import random
import uuid
from datetime import datetime, timezone
from main import CANDIDATES, NUM_USERS
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "votes"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
)


def on_send_success(record_metadata):
    print(f"Sent to {record_metadata.topic} partition {record_metadata.partition}")


def on_send_error(excp):
    print("Error:", excp)


def make_event(user_id):
    return {
        "vote_id": uuid.uuid4().hex,
        "user_id": user_id,
        "candidate_id": random.choice(CANDIDATES),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


def main():
    user_ids = [f"user-{i}" for i in range(1, NUM_USERS + 1)]
    for uid in user_ids:
        evt = make_event(uid)
        print("make event", evt)

        producer.send(TOPIC, key=uid, value=evt).add_callback(
            on_send_success
        ).add_errback(on_send_error)
    producer.flush()


if __name__ == "__main__":
    main()

import json
from uuid import uuid4

from fastapi import FastAPI
from kafka import KafkaProducer
from datetime import datetime, timezone
from schema import VoteInput

app = FastAPI()

TOPIC = "votes"
KAFKA_BOOTSTRAP = "localhost:9092"

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


@app.post("/vote")
def vote(input: VoteInput):

    evt = {
        "vote_id": uuid4().hex,
        "user_id": input.national_code, # TODO: use phone and national code to get user id
        "candidate_id": input.candidate_id,
        "ts": datetime.now(timezone.utc).isoformat()
    }
    producer.send(TOPIC, key=input.national_code, value=evt).add_callback(
        on_send_success
    ).add_errback(on_send_error)

    producer.flush()
    return {"status": "ok"}

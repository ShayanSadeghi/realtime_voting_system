import json
import logging
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from datetime import datetime, timezone
from schema import VoteInput, UserInput
import psycopg2

app = FastAPI()

TOPIC = "votes"
KAFKA_BOOTSTRAP = "localhost:9092"

PG_HOST = "localhost"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASS = "postgres"
PG_DB = "voting"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
)

conn = psycopg2.connect(dbname=PG_DB, user=PG_USER, password=PG_PASS, host=PG_HOST,port=PG_PORT)

def on_send_success(record_metadata):
    print(f"Sent to {record_metadata.topic} partition {record_metadata.partition}")


def on_send_error(excp):
    print("Error:", excp)

@app.post("/register")
def register(input: UserInput):
    cur  = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO voters(user_name, national_code, phone) VALUES (%s, %s, %s)
        """, (input.user_name, input.national_code, input.phone))
        conn.commit()
    except Exception as e:
        logging.error(e)
        raise HTTPException(status_code=406, detail="User is not created.")
    cur.close()
    return {"status": "ok"}

@app.post("/vote")
def vote(input: VoteInput):
    cur = conn.cursor()
    cur.execute("""
    SELECT user_id FROM voters WHERE phone=%s AND national_code=%s
    """, (input.phone, input.national_code))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="User not found. Please register before voting")


    evt = {
        "vote_id": uuid4().hex,
        "user_id": row[0],
        "candidate_id": input.candidate_id,
        "ts": datetime.now(timezone.utc).isoformat()
    }
    producer.send(TOPIC, key=input.national_code, value=evt).add_callback(
        on_send_success
    ).add_errback(on_send_error)

    producer.flush()
    cur.close()
    return {"status": "ok"}

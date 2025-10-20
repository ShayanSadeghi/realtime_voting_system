# Real-Time Voting Pipeline

A compact data engineering project demonstrating Kafka + PySpark streaming by simulating a real-time voting system. Events are produced to Kafka, processed with PySpark Structured Streaming, and stored in PostgreSQL.

---

## Tech Stack
- **Streaming:** Apache Kafka
- **Processing:** PySpark Structured Streaming
- **Storage:** PostgreSQL
- **Infra:** Docker & Docker Compose

---

## Quick Start

### 1. Start services
`docker-compose up -d`

### 2. Initiate pg tables and dataa
`python main.py`


### 3. Produce mock votes
`python produce_votes.py`

### 4. Run Spark consumer
`python spark_stream_votes.py`


---

## Useful Commands

### List Kafka topics

*In kafka container:*

`kafka-topics --list --bootstrap-server broker:29092`

### Inspect votes:

 *In kafka container:*

` kafka-console-consumer --bootstrap-server broker:29092 --topic votes --from-beginning`
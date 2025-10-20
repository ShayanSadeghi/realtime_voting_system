from pyspark.sql.functions import from_json, col, to_timestamp, window
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = (
    SparkSession.builder.appName("votes-stream")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

kafka_bootstrap = "localhost:9092"
topic = "votes"

schema = StructType(
    [
        StructField("vote_id", StringType()),
        StructField("user_id", StringType()),
        StructField("candidate_id", StringType()),
        StructField("ts", StringType()),
    ]
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()
)

votes = (
    raw.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select(
        col("data.id"),
        col("data.user_id"),
        col("data.candidate_id"),
        to_timestamp(col("data.ts")).alias("ts"),
    )
)
votes_dedup = votes.withWatermark("ts", "1 hour").dropDuplicate(["user_id"])

vote_results = (
    votes_dedup.groupBy("candidate_id").count().withColumnRenamed("count", "votes")
)


def write_votes_to_pg(batch_df, batch_id):
    batch_df.write.format("jdbc").option(
        "url", "jdbc:postgresql://localhost:5432/votesdb"
    ).option("dbtable", "voters_history").option("user", "postgres").option(
        "password", "postgres"
    ).mode(
        "append"
    ).save()


def write_results_to_pg(batch_df, batch_id):
    batch_df.write.format("jdbc").option(
        "url", "jdbc:postgresql://localhost:5432/votesdb"
    ).option("dbtable", "vote_results").option("user", "postgres").option(
        "password", "postgres"
    ).mode(
        "overwrite"
    ).save()


(
    votes_dedup.writeStream.outputMode("append")
    .foreachBatch(write_votes_to_pg)
    .option("checkpointLocation", "./checkpoints/votes_history")
    .trigger(processingTime="5 seconds")
    .start()
)


(
    vote_results.writeStream.outputMode("complete")
    .foreachBatch(write_results_to_pg)
    .option("checkpointLocation", "./checkpoints/total_votes")
    .trigger(processingTime="5 seconds")
    .start()
)

spark.streams.awaitAnyTermination()

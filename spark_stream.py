from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import col, sum as spark_sum

def process_retail_data_stream():
    # Initialize the SparkSession
    spark = (
        SparkSession.builder
        .appName("RetailDataStreamProcessing")
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.cassandra.connection.port", "9042")
        .config("es.nodes", "elasticsearch")
        .config("es.port", "9200")
        .getOrCreate()
    )

    # Set the checkpoint directory
    checkpoint_dir = "hdfs://namenode:9000/checkpoint/retail"

    # Read stream from Kafka topic
    retail_stream_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9093") \
        .option("subscribe", "retail_topic") \
        .option("startingOffsets", "earliest") \
        .load()
        # .option("kafka.consumer.fetch.max.wait.ms", "1000") \
        # .option("kafka.consumer.max.poll.interval.ms", "60000") \
       

    # Add Kafka ingestion timestamp as a new column
    retail_stream_df = retail_stream_df \
        .withColumn("event_time", col("timestamp").cast("timestamp"))

    # Kafka data contains the key and value as binary, so we need to convert it
    retail_data_df = retail_stream_df.selectExpr(
        "CAST(value AS STRING)",
        "event_time"
    ).selectExpr(
        "json_tuple(value, 'transaction_id', 'product_name', 'category', 'quantity', 'price', 'transaction_date') AS (transaction_id, product_name, category, quantity, price, transaction_date)",
        "event_time"
    )

    # Add a watermark and transform data: Group by disease and count occurrences
    transformed_data = retail_data_df \
        .withWatermark("event_time", "5 minutes") \
        .groupBy("product_name", "transaction_date") \
            .agg(spark_sum("quantity").alias("total_quantity")) \
            .orderBy("transaction_date", "total_quantity", ascending=False)

    # Write transformed data to Elasticsearch
    elasticsearch_query = transformed_data.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .option("checkpointLocation", checkpoint_dir + "/elasticsearch") \
        .outputMode("update") \
        .option("es.mapping.id", "patient_id") \
        .start("retail-sales-stats")

    # Write transformed data to Cassandra
    cassandra_query = transformed_data.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", checkpoint_dir + "/cassandra") \
        .outputMode("update") \
        .options(table="product_sales", keyspace="retail") \
        .start()
    
    transformed_data.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start() \
        .awaitTermination()

    # Await termination of the streaming queries
    elasticsearch_query.awaitTermination()
    cassandra_query.awaitTermination()

    # Stop the Spark session when done
    spark.stop()

if __name__ == "__main__":
    process_retail_data_stream()

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql.functions import col, sum as spark_sum

def setup_cassandra_keyspace_and_table():
    try:
        # Connect to Cassandra
        cluster = Cluster(["cassandra"])  # Replace with your Cassandra node IP/hostname
        session = cluster.connect()

        # Create keyspace if it doesn't exist
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS retail
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """)

        # Create table if it doesn't exist
        session.execute("""
            CREATE TABLE product_sales (
                product_name text,
                transaction_date text,
                total_quantity int
            );
        """)

        print("Cassandra keyspace and table are set up successfully.")
        cluster.shutdown()

    except Exception as e:
        print(f"Error setting up Cassandra keyspace/table: {e}")
        raise


def process_retail_data():
    try:
        # Set up Cassandra keyspace and table
        setup_cassandra_keyspace_and_table()

        # Initialize SparkSession
        spark = (
            SparkSession.builder
            .appName("RetailDataProcessing")
            .config("spark.cassandra.connection.host", "cassandra")
            .config("spark.cassandra.connection.port", "9042")
            .config("es.nodes", "elasticsearch")
            .config("es.port", "9200")
            .getOrCreate()
        )

        # Load data from HDFS
        print("Loading data from HDFS...")
        data = spark.read.csv(
            "hdfs://namenode:9000/data/Retail_data.csv",
            header=True,
            inferSchema=True
        )


        # Process data: Calculate total quantity sold per product per date
        product_sales = data.groupBy("product_name", "transaction_date") \
            .agg(spark_sum("quantity").alias("total_quantity")) \
            .orderBy("transaction_date", "total_quantity", ascending=False)

        # Write transformed data to Cassandra
        print("Writing data to Cassandra...")
        product_sales.write.format("org.apache.spark.sql.cassandra").options(
            table="product_sales", keyspace="retail"
        ).mode("append").save()

        # Write transformed data to Elasticsearch
        print("Writing data to Elasticsearch...")
        # Write transformed data to Elasticsearch (Remove type from the resource)
        print("Writing data to Elasticsearch...")
        # transformed_data.write.format("org.elasticsearch.spark.sql").options(
        #     es_resource="healthcare/disease-stats"
        # ).mode("overwrite").save()

        product_sales.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", "product-sales-stats") \
        .mode("overwrite") \
        .save()

        # Show Results
        product_sales.show()
        print("Data processing completed successfully.")

    except AnalysisException as e:
        print(f"Data analysis error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Stop SparkSession
        spark.stop()

if __name__ == "__main__":
    process_retail_data()

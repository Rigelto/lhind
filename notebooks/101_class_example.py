from pyspark.sql import SparkSession
# Assuming saved as delta_etl_pipeline.py
from delta_etl_pipeline import DeltaETLPipeline

# Start Spark
spark = SparkSession.builder \
    .appName("Delta ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define transformation function (example)


def clean_data(df):
    return df.dropna().dropDuplicates()


# Initialize and run
pipeline = DeltaETLPipeline(spark, "/mnt/datalake")
pipeline.run_pipeline(
    csv_path="/mnt/raw/user_data.csv",
    layer="silver",
    table_name="user_cleaned",
    transform_func=clean_data
)

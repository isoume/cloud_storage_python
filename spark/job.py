from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("GCS_TO_BigQuery") \
    .getOrCreate()

# Details BIGQUERY
PROJECT_ID = "isi-group-m2-dsia"
DATASET_NAME="m2dsia_nom"
TABLE_NAME = "transaction"

#
BUCKET_NAME = "m2dsia-nom-input-dataproc"
BUCKET_NAME_TMP = "m2dsia-nom-input-dataproc-tmp"

# Define paths
gcs_input_path = f"gs://{BUCKET_NAME}/*.csv"  # Wildcard for multiple files
bigquery_table = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"
gcs_temp_bucket = f"gs://{BUCKET_NAME_TMP}"  # Temporary bucket for BigQuery export

# Read multiple CSV files from GCS
df = spark.read.csv(gcs_input_path, header=True, inferSchema=True)

# Exemple de transformation: filter, select, etc.
#processed_df = df.filter(col("some_column") > 10).select("col1", "col2", "col3")

# Write DataFrame to BigQuery
processed_df.write \
    .format("bigquery") \
    .option("table", bigquery_table) \
    .option("temporaryGcsBucket", gcs_temp_bucket) \
    .mode("overwrite") \
    .save()

spark.stop()

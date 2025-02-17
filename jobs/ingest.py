from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from utils.logger import get_logger
from utils.utilities import merge_dataframes
import yaml

with open("/app/config/config.yaml", "r") as file:
    _config = yaml.safe_load(file)

# variables

SEP = 47
job_name = "Ingestion job"

# config variables

base_url = _config["url"]["csv_files"]
suffixes = _config["csv"]["suffixes"]  
raw_delta_path = _config["layer"]["raw"]
files = [f"{base_url}{suffix}" for suffix in suffixes]

logger = get_logger(job_name)


# start Spark session

builder = (SparkSession.builder.appName("Ingestion")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()

logger.info("-"*SEP)
logger.info("S T A R T I N G  I N G E S T I O N  J O B . . .")
logger.info("-"*SEP)


all_files_df = merge_dataframes(files, job_name)
logger.info(f"Merged dataframes: {len(files)}.")
logger.info(f"Final dataframe count: {len(all_files_df)}")

spark_df_raw = spark.createDataFrame(all_files_df)
logger.info(f"Spark dataframe - raw data - count: {spark_df_raw.count()}")

spark_df_raw.write.format("delta").mode("overwrite").save(raw_delta_path)
logger.info("Raw ingestion complete.")

spark_df_raw.show()

spark.stop()
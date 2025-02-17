from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta import configure_spark_with_delta_pip
from utils.logger import get_logger
from utils.utilities import transform_data
import yaml

with open("/app/config/config.yaml", "r") as file:
    _config = yaml.safe_load(file)

# variables

SEP = 57
job_name = "Transformation job"

# config variables

raw_delta_path = _config["layer"]["raw"]
silver_delta_path = _config["layer"]["silver"]
part_col_silver = _config["partition_column"]["silver"]

logger = get_logger(job_name)


builder = (SparkSession.builder.appName("Transformation")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()

logger.info("-"*SEP)
logger.info("S T A R T I N G  T R A N S F O R M A T I O N  J O B. . .")
logger.info("-"*SEP)

spark_df_cleaned = transform_data(spark, job_name, raw_delta_path, _config)
logger.info("Successfully transformed and cleansed data.")

spark_df_cleaned.write.format("delta").partitionBy(part_col_silver).mode("overwrite").save(silver_delta_path)
logger.info("Transformed data ingested into the Silver table.")

spark_df_cleaned.show()

spark.stop()
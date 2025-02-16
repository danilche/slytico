from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta import configure_spark_with_delta_pip
#import pandas as pd
from utils.logger import get_logger
from utils.utilities import *
import yaml

with open("/app/config/config.yaml", "r") as file:
    _config = yaml.safe_load(file)

job_name = "Ingestion job"
logger = get_logger(job_name)

# def load_file_into_pandas_df(file):
#     try:
#         pd_df = pd.read_csv(file)
#         logger.info(f"Successfully read file {file}")
#         logger.info(f"Dataframe count: {len(pd_df)}")
#         return pd_df
#     except Exception as e:
#         logger.error(e)

# def df_as_string(df, n=20, truncate=True, vertical=False):
#     if isinstance(truncate, bool) and truncate:
#         return df._jdf.showString(n, 20, vertical)
#     else:
#         return df._jdf.showString(n, int(truncate), vertical)



builder = (SparkSession.builder.appName("RawIngestion")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()

base_url = _config["url"]["csv_files"]
suffixes = _config["csv"]["suffixes"]  
files = [f"{base_url}{suffix}" for suffix in suffixes]

raw_delta_path = _config["layer"]["raw"]
silver_delta_path = _config["layer"]["silver"]
gold_delta_path = _config["layer"]["gold"]
results_path = _config["file"]["result"]

all_files_df = pd.concat([load_file_into_pandas_df(file, job_name) for file in files])
logger.info(f"Final dataframe count: {len(all_files_df)}")

spark_df_raw = spark.createDataFrame(all_files_df)
logger.info(f"Spark dataframe - raw data - count: {spark_df_raw.count()}")


spark_df_raw.write.format("delta").mode("overwrite").save(raw_delta_path)
logger.info("Raw ingestion complete.")



spark_df_cleaned = (spark.read.format('delta').load(raw_delta_path)
                            .withColumn("Timestamp", F.col("Timestamp").cast("timestamp"))
                            .withColumn("Hour_Timestamp", F.date_trunc("hour", F.col("Timestamp")))
                            .withColumn("Date", F.to_date(F.col("Timestamp")))
                        )
spark_df_cleaned.write.format("delta").partitionBy("Date").mode("overwrite").save(silver_delta_path)
logger.info("Transformed data ingested into Silver table.")

spark_df_silver = (spark.read.format('delta').load(silver_delta_path))
logger.info("Successfully read data from the Silver table.")

# active power
ap_df = spark_df_silver.filter((F.col("DeviceType") == "Inverter") & 
                               (F.col("Metric") == "ac_active_power"))

ap_agg_df = (ap_df.groupby("DeviceID", "Hour_Timestamp")
                     .agg(F.avg("Value").alias("HourlyMean"), 
                          F.count("Value").alias("DataPointCount"))
            )
ap_agg_count = ap_agg_df.count()
logger.info(f"Aggregated Active Power dataset count: {ap_agg_count}")

ap_result_df = ap_agg_df.filter((F.col("DeviceID") == 1054530426) & (F.col("Hour_Timestamp") == "2021-11-04 11:00:00"))

# irradiance
irr_df = spark_df_silver.filter((F.col("DeviceType").isin("Sensor", "Satellite")) & 
                                 (F.col("Metric") == "irradiance"))
                    
irr_agg_df = (irr_df.groupBy("DeviceID", "Hour_Timestamp")
                       .agg(F.avg("Value").alias("HourlyMean"),
                            F.count("*").alias("DataPointCount"))
            )
irr_agg_count = irr_agg_df.count()
logger.info(f"Aggregated Irradiance dataset count: {irr_agg_count}")

irr_result_df = irr_agg_df.filter((F.col("DeviceID") == 3258837907) & (F.col("Hour_Timestamp") == "2021-11-04 11:00:00"))

with open(results_path, 'a') as outfile:
    print(df_as_string(ap_result_df), file=outfile)
    print(df_as_string(irr_result_df), file=outfile)


agg_union_df = ap_agg_df.union(irr_agg_df)
agg_union_count = agg_union_df.count()
logger.info(f"Aggregated union dataset count: {agg_union_count}")

if agg_union_count == irr_agg_count + ap_agg_count:
    logger.info("Union dataset count matches with the sum of Active Power and Irradiance dataset counts.")
else:
    logger.warning("Union dataset count does not match with the sum of Active Power and Irradiance dataset counts.")

agg_union_df.write.format("delta").partitionBy("DeviceID").mode("overwrite").save(gold_delta_path)
logger.info("Successfully wrote union data to the Gold table.")

spark.stop()


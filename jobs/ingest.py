from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta import configure_spark_with_delta_pip
import pandas as pd
from utils.logger import get_logger

logger = get_logger("Ingestion job")

def load_files_into_df(file_list):
    pd_dataframes = []
    for file in file_list:
        pd_dataframe = pd.read_csv(file)
        logger.info(f"Successfully read file {file}")
        logger.info(f"Dataframe count: {len(pd_dataframe)}")
        pd_dataframes.append(pd_dataframe)

    pd_final = pd.concat(pd_dataframes)
    logger.info(f"Final dataframe count: {len(pd_final)}")
    return pd_final

def df_as_string(df, n=20, truncate=True, vertical=False):
    """Example wrapper function for interacting with self._jdf.showString"""
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 20, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)



builder = (SparkSession.builder.appName("RawIngestion")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()

base_url = "https://raw.githubusercontent.com/solytic/opendata/refs/heads/master/bronze/demo/Date%3D2021-11-0"

suffixes = ['1/part-00000.csv',
            '2/part-00000.csv',
            '2/part-00001.csv',
            '3/part-00001.csv',
            '3/part-00002.csv',
            '4/part-00002.csv',
            '4/part-00003.csv',
            '4/part-00004.csv',
            '5/part-00004.csv',
            '5/part-00005.csv',
            '5/part-00006.csv',
            '6/part-00006.csv',
            '6/part-00007.csv',
            '6/part-00008.csv',
            '7/part-00008.csv',
            '7/part-00009.csv']
    
files = [f"{base_url}{suffix}" for suffix in suffixes]

raw_delta_path = "spark-warehouse/raw_data"
silver_delta_path = "spark-warehouse/silver_data"
gold_delta_path = "spark-warehouse/gold_data"
results_path = "results/result.txt"

spark_df_raw = spark.createDataFrame(load_files_into_df(files))
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


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta import configure_spark_with_delta_pip
import pandas as pd
import logging

logging.basicConfig(
    filename="/app/logs/ingest.log",  # Log file location
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def load_files_into_df(file_list):
    pd_dataframes = []
    for file in file_list:
        pd_dataframe = pd.read_csv(file)
        logging.info(f"Successfully read file {file}")
        logging.info(f"Dataframe count: {len(pd_dataframe)}")
        pd_dataframes.append(pd_dataframe)

    pd_final = pd.concat(pd_dataframes)
    logging.info(f"Final dataframe count: {len(pd_final)}")
    return pd_final

def main():
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

    spark_df_raw = spark.createDataFrame(load_files_into_df(files))
    logging.info(f"Spark dataframe - raw data - count: {spark_df_raw.count()}")


    spark_df_raw.write.format("delta").mode("overwrite").save(raw_delta_path)
    logging.info("Raw ingestion complete.")



    spark_df_cleaned = (spark.read.format('delta').load(raw_delta_path)
                                .withColumn("Timestamp", F.col("Timestamp").cast("timestamp"))
                                .withColumn("Hour_Timestamp", F.date_trunc("hour", F.col("Timestamp")))
                                .withColumn("Date", F.to_date(F.col("Timestamp")))
                            )
    spark_df_cleaned.write.format("delta").mode("overwrite").save(silver_delta_path)
    logging.info("Transformed data ingested into Silver table.")
    
    spark.stop()

if __name__ == "__main__":
    main()
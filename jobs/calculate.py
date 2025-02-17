from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta import configure_spark_with_delta_pip
from utils.logger import get_logger
from utils.utilities import apply_filters, aggregate_data, df_as_string
import yaml

with open("/app/config/config.yaml", "r") as file:
    _config = yaml.safe_load(file)

# variables

SEP = 51
job_name = "Calculation job"

# config variables

silver_delta_path = _config["layer"]["silver"]
gold_delta_path = _config["layer"]["gold"]
results_path = _config["file"]["result"]
part_col_gold = _config["partition_column"]["gold"]
ap_filters = _config["active_power"]["filters"]
ap_res_filters = _config["active_power"]["result_filters"]
irr_filters = _config["irradiance"]["filters"]
irr_res_filters = _config["irradiance"]["result_filters"]

logger = get_logger(job_name)


# start Spark session

builder = (SparkSession.builder.appName("Transformation")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()

logger.info("-"*SEP)
logger.info("S T A R T I N G  C A L C U L A T I O N  J O B . . .")
logger.info("-"*SEP)

spark_df_silver = (spark.read.format('delta').load(silver_delta_path))
logger.info("Successfully read data from the Silver table.")


# Active Power filtering & aggregation

ap_df = apply_filters(spark_df_silver, ap_filters)
logger.info(f"Finished Aggregated Active Power dataset filtering.")

ap_agg_df = aggregate_data("active_power", ap_df, _config)
logger.info(f"Finished Aggregated Active Power dataset aggregation.")

ap_agg_count = ap_agg_df.count()
logger.info(f"Aggregated Active Power dataset count: {ap_agg_count}")


# Irradiance filtering & aggregation

irr_df = apply_filters(spark_df_silver, irr_filters)
logger.info(f"Finished Irradiance dataset filtering.")

irr_agg_df = aggregate_data("irradiance", irr_df, _config)
logger.info(f"Finished Irradiance dataset aggregation.")

irr_agg_count = irr_agg_df.count()
logger.info(f"Aggregated Irradiance dataset count: {irr_agg_count}")


# do needed calculations and write results

ap_result_df = apply_filters(ap_agg_df, ap_res_filters)
logger.info(f"Calculate result for Active Power.")

irr_result_df = apply_filters(irr_agg_df, irr_res_filters)
logger.info(f"Calculate result for Irradiance.")

with open(results_path, 'a') as outfile:
    print(df_as_string(ap_result_df), file=outfile)
    logger.info(f"Print result for Active Power.")

    print(df_as_string(irr_result_df), file=outfile)
    logger.info(f"Print result for Irradiance.")


# Add flag to each dataset

ap_agg_df = ap_agg_df.withColumn("flag", F.lit("ac_active_power"))
irr_agg_df = irr_agg_df.withColumn("flag", F.lit("irradiance"))

# union datasets and write to Gold layer

agg_union_df = ap_agg_df.union(irr_agg_df)
agg_union_count = agg_union_df.count()
logger.info(f"Aggregated union dataset count: {agg_union_count}")

if agg_union_count == irr_agg_count + ap_agg_count:
    logger.info("Union dataset count matches with the sum of Active Power and Irradiance dataset counts.")
else:
    logger.warning("Union dataset count does not match with the sum of Active Power and Irradiance dataset counts.")

ap_agg_df.show()
irr_agg_df.show()
agg_union_df.show()

agg_union_df.write.format("delta").partitionBy(part_col_gold).mode("overwrite").save(gold_delta_path)
logger.info("Successfully wrote union data to the Gold table.")

spark.stop()

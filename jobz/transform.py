# job_processing.py
from pyspark.sql import SparkSession

def main():
    spark = (SparkSession.builder.appName("ProcessingJob")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    
    raw_delta_path = "/data/delta/raw"
    df = spark.read.format("delta").load(raw_delta_path)
    df.printSchema
    df.show()
    
    print("Processing job complete.")
    spark.stop()

if __name__ == "__main__":
    main()

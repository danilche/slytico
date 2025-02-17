from utils.logger import get_logger
import pandas as pd
import pyspark.sql.functions as F


def load_file_into_pandas_df(file, job_name):
    logger = get_logger(job_name)
    try:
        pd_df = pd.read_csv(file)
        logger.info(f"Successfully read file {file}")
        logger.info(f"Dataframe count: {len(pd_df)}")
        return pd_df
    except Exception as e:
        logger.error(e)


def merge_dataframes(files, job_name):
    logger = get_logger(job_name)
    try:
        return pd.concat([load_file_into_pandas_df(file, job_name) for file in files])
    except Exception as e:
        logger.error(e)


def df_as_string(df, n=20, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 20, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)


def transform_data(spark, job_name, path, conf):
    logger = get_logger(job_name)
    try:
        df = spark.read.format('delta').load(path)
        logger.info("Successfully loaded data.")
        unchanged_columns = conf["columns"]["unchanged"]
        transformed_exprs = [F.expr(t["expression"]).alias(t["name"]) for t in conf["columns"]["transformed"]]
        df_transformed = df.select(*[F.col(col) for col in unchanged_columns], *transformed_exprs)
        return df_transformed
    except Exception as e:
        logger.error(e)


def apply_filters(df, filters):
    for rule in filters:
        column = rule["column"]
        condition = rule["condition"]
        value = rule["value"]
        
        if condition == "IN":
            df = df.filter(F.col(column).isin(value))
        elif condition == "==":
            df = df.filter(F.col(column) == value)  
    return df

def aggregate_data(calc, df, conf):
    group_by_columns = conf[calc]["aggregation"]["group_by"]
    agg_expressions = [
        getattr(F, metric["function"])(metric["column"]).alias(metric["name"])
        for metric in conf[calc]["aggregation"]["metrics"]
    ]
    return df.groupBy(*group_by_columns).agg(*agg_expressions)
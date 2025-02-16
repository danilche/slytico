from utils.logger import get_logger
import pandas as pd

def load_file_into_pandas_df(file, job_name):
    logger = get_logger(job_name)
    try:
        pd_df = pd.read_csv(file)
        logger.info(f"Successfully read file {file}")
        logger.info(f"Dataframe count: {len(pd_df)}")
        return pd_df
    except Exception as e:
        logger.error(e)

def df_as_string(df, n=20, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 20, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)
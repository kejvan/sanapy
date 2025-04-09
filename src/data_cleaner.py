import os
import logging
import polars as pl

logging.basicConfig(level=logging.INFO, format="%(ascitime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class DataCleaner:
    def __init__(self, data_dir="./data"):
        self.data_dir = data_dir
        self.raw_dir = os.path.join(self.data_dir, "raw")
        self.processed_dir = os.path.join(self.data_dir, "processed")

        # create directories if they don't exist
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)

    def handle_missing_values(self, df: pl.DataFrame, method: str = "forward_fill") -> pl.DataFrame:
        # check if dataframe is empty
        if df.is_empty():
            logger.warning(f"Empty dataframe was provided")
            return df

        # check if dataframe has missing values
        null_count = sum(sum(df.null_count()))
        if null_count == 0:
            logger.info(f"No missing data were found in the dataframe")
            return df
        
        # fill the missing values
        logger.info(f"Handling {null_count} missing values using {method} method")
        try:
            if method == "forward_fill":
                return df.fill_null(strategy="forward")
            elif method == "backward_fill":
                return df.fill_null(strategy="backward")
            elif method == "drop":
                return df.drop_nulls
            elif method == "zero":
                return df.fill_null(0)
            else:
                logger.warning(f"Unknown method {method}, defaulting to forward_fill")
                return df.fill_null(strategy="forward")
            
        except Exception as e:
            logger.error(f"Error handling missing values: {e}")
            return df
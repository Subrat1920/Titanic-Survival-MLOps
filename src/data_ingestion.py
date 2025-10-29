import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
import os
import sys
from src.logger.logger import get_logger
from src.execption.custom_exception import CustomException
from config.database_config import DB_CONFIG
from config.path_config import *

logger = get_logger(__name__)


class DataIngestion:
    def __init__(self, db_params, output_dir):
        self.db_params = db_params
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def get_engine(self):
        """Create SQLAlchemy engine for PostgreSQL."""
        try:
            engine_url = (
                f"postgresql+psycopg2://{self.db_params['user']}:{self.db_params['password']}"
                f"@{self.db_params['host']}:{self.db_params['port']}/{self.db_params['dbname']}"
            )
            engine = create_engine(engine_url)
            logger.info("SQLAlchemy engine created successfully.")
            return engine
        except Exception as e:
            logger.error(f"Error creating SQLAlchemy engine: {e}")
            raise CustomException(str(e), sys)

    def extract_data(self):
        """Extract Titanic data from PostgreSQL."""
        try:
            engine = self.get_engine()
            query = "SELECT * FROM public.titanic"
            df = pd.read_sql_query(query, engine)
            engine.dispose()
            logger.info(f"Data extracted successfully. Shape: {df.shape}")
            return df
        except Exception as e:
            logger.error(f"Error while extracting data: {e}")
            raise CustomException(str(e), sys)

    def save_data(self, df):
        """Split and save the extracted data."""
        try:
            train_df, test_df = train_test_split(df, test_size=0.2, random_state=42)
            train_df.to_csv(TRAIN_PATH, index=False)
            test_df.to_csv(TEST_PATH, index=False)
            logger.info(
                f"Data split and saved successfully. "
                f"Train shape: {train_df.shape}, Test shape: {test_df.shape}"
            )
        except Exception as e:
            logger.error(f"Error while saving data: {e}")
            raise CustomException(str(e), sys)

    def run(self):
        """Run full data ingestion pipeline."""
        try:
            logger.info("Starting Data Ingestion Pipeline...")
            df = self.extract_data()
            self.save_data(df)
            logger.info("Data Ingestion Pipeline completed successfully.")
        except Exception as e:
            logger.error(f"Error in Data Ingestion Pipeline: {e}")
            raise CustomException(str(e), sys)


if __name__ == "__main__":
    data_ingestion = DataIngestion(DB_CONFIG, RAW_DIR)
    data_ingestion.run()

from pyspark.sql import SparkSession, DataFrame
from typing import Callable, Optional
import os


class DeltaETLPipeline:
    def __init__(self, spark: SparkSession, base_path: str):
        """
        Initialize the pipeline with a Spark session and base path for data storage.

        :param spark: Active SparkSession
        :param base_path: Base directory for delta storage (e.g., "/mnt/datalake")
        """
        self.spark = spark
        self.base_path = base_path

    def load_csv(self, path: str, header: bool = True, infer_schema: bool = True) -> DataFrame:
        """
        Load a CSV file into a DataFrame.

        :param path: Path to the CSV file
        :param header: Whether CSV has a header
        :param infer_schema: Whether to infer schema
        :return: Spark DataFrame
        """
        return self.spark.read.option("header", header).option("inferSchema", infer_schema).csv(path)

    def apply_transformation(self, df: DataFrame, transform_func: Optional[Callable[[DataFrame], DataFrame]]) -> DataFrame:
        """
        Apply a transformation function to a DataFrame.

        :param df: Input DataFrame
        :param transform_func: A function that takes and returns a DataFrame
        :return: Transformed DataFrame
        """
        if transform_func:
            return transform_func(df)
        return df

    def save_to_delta(self, df: DataFrame, layer: str, table_name: str, mode: str = "overwrite") -> None:
        """
        Save a DataFrame to Delta Lake format.

        :param df: DataFrame to save
        :param layer: One of "bronze", "silver", or "gold"
        :param table_name: Logical name of the table
        :param mode: Save mode (e.g., "overwrite", "append")
        """
        layer_path = os.path.join(self.base_path, layer, table_name)
        df.write.format("delta").mode(mode).save(layer_path)

    def run_pipeline(self, csv_path: str, layer: str, table_name: str, transform_func: Optional[Callable[[DataFrame], DataFrame]] = None, mode: str = "overwrite") -> None:
        """
        Run a complete ETL pipeline: load CSV → transform → save to Delta.

        :param csv_path: Path to source CSV file
        :param layer: Target Delta layer
        :param table_name: Target Delta table name
        :param transform_func: Optional transformation function
        :param mode: Save mode
        """
        df = self.load_csv(csv_path)
        df_transformed = self.apply_transformation(df, transform_func)
        self.save_to_delta(df_transformed, layer, table_name)

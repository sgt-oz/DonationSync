import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, max as spark_max, first, to_date
import pandas as pd


class DonationProcessor:
    def __init__(
        self,
        incoming_dir: str = "Incoming",
        table_name: str = "DonorLifetimeGiving",
        warehouse_dir: str = "spark-warehouse"
    ):
        """
        Initialize the DonationProcessor
        """
        self.incoming_dir = Path(incoming_dir)
        self.table_name = table_name
        self.data_dir = Path(warehouse_dir) / table_name.lower()
        self.parquet_path = self.data_dir / "data.parquet"
        
        # Windows workaround: Set HADOOP_HOME to avoid winutils.exe requirement
        if os.name == "nt":  # Windows
            hadoop_home = os.path.join(os.path.dirname(__file__), "hadoop")
            os.makedirs(hadoop_home, exist_ok=True)
            os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)
            os.environ["HADOOP_HOME"] = hadoop_home
        
        # Create Spark session
        spark_builder = SparkSession.builder \
            .appName("DonationProcessor")
        
        # Windows-specific configs to avoid native IO issues
        if os.name == "nt":  # Windows
            spark_builder = spark_builder \
                .config("spark.hadoop.io.native.lib.available", "false") \
                .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
                .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs") \
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        
        self.spark = spark_builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("FATAL")
        
        print("Spark session created successfully!")
    
    def _find_csv_files(self):
        """
        Find all CSV files in the Incoming directory
        """
        csv_files = list(self.incoming_dir.glob("*.csv")) 
        return csv_files
    
    def _read_csv_files(self):
        """
        Read all CSV files from Incoming directory
        """
        csv_files = self._find_csv_files()
        
        if not csv_files:
            raise FileNotFoundError("No CSV files found in Incoming folder")
        
        # Read all CSV files and combine them
        print(f"\nReading CSV files...")
        dfs = []
        for csv_file in csv_files:
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(str(csv_file))
            dfs.append(df)
            print(f"  Read {csv_file.name}: {df.count()} rows")
        
        # Union all dataframes
        if len(dfs) > 1:
            from functools import reduce
            combined_df = reduce(lambda a, b: a.union(b), dfs)
        else:
            combined_df = dfs[0]
        
        return combined_df
    
    def _aggregate_donations(self, df):
        """
        Aggregate donations from incoming files by DonorID
        """
        print(f"\nAggregating donations from incoming files...")
        aggregated = df.groupBy("DonorID") \
            .agg(
                first("Name").alias("Name"),
                spark_sum("Amount").alias("LifetimeAmount"),
                spark_max("Date").alias("LastDonation")
            )
        
        return aggregated
    
    def _read_existing_table(self):
        """
        Read existing DonorLifetimeGiving table if it exists
        """
        if not self.parquet_path.exists():
            return None
        
        existing_df = self.spark.read.parquet(str(self.parquet_path))
        return existing_df
    
    def _merge_data(self, new_aggregated, existing_aggregated):
        """
        Merge new aggregated data with existing table data
        """
        if existing_aggregated is None:
            print(f"\nNo existing {self.table_name} table found. Creating new table from incoming data...")
            return new_aggregated
        
        print(f"\nMergine aggregated data from incoming files with target table...")
        
        # Union existing and new aggregated data
        combined_df = existing_aggregated.union(new_aggregated)
        
        # Re-aggregate: sum LifetimeAmount (adds new to existing), max LastDonation, first Name (from new data)
        merged_df = combined_df.groupBy("DonorID") \
            .agg(
                first("Name").alias("Name"),  # Use name from new data
                spark_sum("LifetimeAmount").alias("LifetimeAmount"),  # Sum adds new to existing
                spark_max("LastDonation").alias("LastDonation")  # Max takes latest date
            )
        
        return merged_df
    
    def _save_data(self, df):
        """
        Save DataFrame to Parquet using pandas (workaround for Windows)
        """
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Collect to pandas and save
        pandas_df = df.toPandas()
        pandas_df.to_parquet(str(self.parquet_path), index=False)
        print(f"Successfully updated target table...")
    
    def process_files(self):
        """Process all CSV files in Incoming folder and merge with existing table"""
        # Read CSV files
        df = self._read_csv_files()
        print(f"\nRows from incoming files:")
        df.show(1000, truncate=False)
        
        # Parse date column (assumes M/d/yyyy format)
        df = df.withColumn("Date", to_date(col("Date"), "M/d/yyyy"))
        
        # Aggregate incoming donations
        new_aggregated = self._aggregate_donations(df)
        
        # Read existing table
        existing_aggregated = self._read_existing_table()
        
        # Merge data
        merged_df = self._merge_data(new_aggregated, existing_aggregated)
        
        # Save merged data
        self._save_data(merged_df)
    
    def show_table(self, limit: int = 20):
        """Display the DonorLifetimeGiving table"""
        if not self.parquet_path.exists():
            print(f"Table {self.table_name} does not exist yet.")
            return
        
        print(f"\n{'='*60}")
        print(f"Contents of {self.table_name}:")
        print(f"{'='*60}")
        df = self.spark.read.parquet(str(self.parquet_path))
        df = df.orderBy("DonorID")
        df.show(limit, truncate=False)
        print(f"\nTotal records: {df.count()}")
     
    def close(self):
        """Close Spark session"""
        self.spark.stop()
        print("\nSpark session stopped.")


def main():
    """Main entry point"""
    import sys
    
    processor = DonationProcessor()
    
    try:
        processor.process_files()
        processor.show_table()
    finally:
        processor.close()


if __name__ == "__main__":
    main()

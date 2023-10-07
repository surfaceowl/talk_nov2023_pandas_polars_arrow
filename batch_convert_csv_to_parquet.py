"""
functions to batch convert all csv files in a directory from csv to parquet format

Example usage
convert_csv_to_parquet_polars("/path/to/source_directory", "/path/to/target_directory")
"""
from pathlib import Path
import polars as pl

def convert_csv_to_parquet_polars(source_directory, target_directory):
    """Convert all CSV files from a source directory to Parquet format in a target directory using Polars.

    Args:
        source_directory (str): Path to the directory containing the CSV files.
        target_directory (str): Path to the directory where Parquet files will be saved.
    """
    source_path = Path(source_directory)
    target_path = Path(target_directory)

    # Create target directory if it doesn't exist
    target_path.mkdir(parents=True, exist_ok=True)

    for csv_file in source_path.glob('*.csv'):
        parquet_file = target_path / csv_file.name.replace('.csv', '.parquet')

        # Read CSV into a Polars DataFrame
        df = pl.read_csv(str(csv_file))

        # Convert Polars DataFrame to Parquet
        df.write_parquet(str(parquet_file))

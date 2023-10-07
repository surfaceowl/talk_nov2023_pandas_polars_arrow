import numpy as np
import pandas as pd
from faker import Faker
from pathlib import Path
import gzip
import shutil

def generate_synthetic_data(num_records: int, seed: int = 77) -> pd.DataFrame:
    """
    Generate synthetic data for testing.
    """
    np.random.seed(seed)
    Faker.seed(seed)
    fake = Faker()

    # Generate data
    customer_ids = [fake.passport_number() for _ in range(num_records)]
    ages = np.random.normal(40, 15, num_records).astype(int)
    lucky_numbers = np.random.poisson(lam=1.0, size=num_records).astype(int)

    occupations = np.random.choice(["Python Dev",
    "Data Engineer",
    "Data Scientist",
    "Machine Learning Eng",
    "DevOps Savior",
    "Pandas Guru",
    "Polars Guru",
    "Apache Arrow Understudy",
    "Rustacean",], num_records)  # List of occupations
    membership_statuses = np.random.choice(["Supporting", "Managing", "Contributing", "Fellow", "Not Yet a Member"], num_records)  # List of statuses
    educations = np.random.choice(["High School", "College", "Graduate", "Leetcode + sweat", "Raised by Wolves"], num_records)  # List of educations
    date_started_python = pd.to_datetime(date_started_python = [fake.date_of_birth(minimum_age=12, maximum_age=100) for _ in range(num_records)])  # List of dates

    # Create DataFrame
    df = pd.DataFrame({
        "customer_id": customer_ids,
        "age": ages,
        "their_lucky_number": lucky_numbers,
        "occupation": occupations,
        "psf_membership_status": membership_statuses,
        "education": educations,
        "date_started_python": date_started_python
    })

    return df

def gzip_csv(file_path: str) -> None:
    """
    Compresses a CSV file using gzip and saves it in the same directory.
    """
    source_path = Path(file_path)
    compressed_file_path = source_path.with_suffix('.csv.gz')

    if not compressed_file_path.is_file():
        with source_path.open('rb') as f_in, gzip.open(compressed_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        print(f"Compressed file saved as {compressed_file_path}")
    else:
        print("Zipped copy of datafile already exists")

def main():
    data_dir = Path("data")
    csv_filename = data_dir / "python_dev_universe.csv"
    gzipped_csv_filename = data_dir / "python_dev_universe.csv.gz"
    parquet_filename = data_dir / "python_dev_universe.parquet"

    data_dir.mkdir(parents=True, exist_ok=True)

    if not csv_filename.is_file() and not gzipped_csv_filename.is_file() and not parquet_filename.is_file():
        df = generate_synthetic_data(num_records=50_000_000)
        df.to_csv(csv_filename, index=False)
        df.to_parquet(parquet_filename, index=False)
        gzip_csv(csv_filename)
    else:
        print("Test data exists in multiple formats.")
        # Add file size info here

if __name__ == "__main__":
    main()

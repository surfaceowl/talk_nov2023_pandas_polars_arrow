"""
simple script to generate a healty amount of synthetic data for testing pandas and polars with
note - the output is .gitignored to keep repo slim
"""
import numpy as np
import pandas as pd
from faker import Faker
from pathlib import Path

# Set seed for reproducibility
seed = 42
np.random.seed(seed)
Faker.seed(seed)

# Set up DataFrame
num_records = 50_000_000

# Generate customer IDs efficiently using a NumPy array
fake = Faker()
customer_ids = [fake.passport_number() for _ in range(num_records)]
df = pd.DataFrame({"customer_id": customer_ids})

# Add numeric columns
ages = np.random.normal(40, 15, num_records).astype(int)
lucky_numbers = np.random.poisson(lam=1.0, size=num_records).astype(int)
df["age"] = ages
df["their_lucky_number"] = lucky_numbers

# Add categorical columns
occupations = [
    "Python Dev",
    "Data Engineer",
    "Data Scientist",
    "Machine Learning Eng",
    "DevOps Savior",
    "Pandas Guru",
    "Polars Guru",
    "Apache Arrow Understudy",
    "Rustacean",
]
membership_statuses = ["Supporting", "Managing", "Contributing", "Fellow", "Not Yet a Member"]
educations = ["High School", "College", "Graduate", "Leetcode + sweat"]

df["occupation"] = np.random.choice(occupations, num_records)
df["psf_membership_status"] = np.random.choice(membership_statuses, num_records)
df["education"] = np.random.choice(educations, num_records)

# Add datetime column
date_started_python = [
    fake.date_of_birth(minimum_age=12, maximum_age=100) for _ in range(num_records)
]
df["date_started_python"] = pd.to_datetime(date_started_python)

# Define file paths using pathlib
data_dir = Path("data")
csv_filename = data_dir / "python_dev_universe.csv"
parquet_filename = data_dir / "python_dev_universe.parquet"

# Create the data directory if it doesn't exist
data_dir.mkdir(parents=True, exist_ok=True)

# Save to disk using pathlib
df.to_csv(csv_filename, index=False)
df.to_parquet(parquet_filename, index=False)

# Print size of files
def get_file_size(filename):
    return round(filename.stat().st_size / (1024 ** 3), 2)

print(f"Size of csv: {get_file_size(csv_filename)} GB")
print(f"Size of parquet: {get_file_size(parquet_filename)} GB")

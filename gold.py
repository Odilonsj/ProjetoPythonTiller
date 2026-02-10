"""
Simple script to check Parquet file types using pandas
"""
import pandas as pd

# Read Parquet file
df = pd.read_parquet("silver_issues.parquet")

print("\n=== First 5 Rows ===")
print(df.head())

print(f"\n=== Shape ===")
print(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}")

# ===== SILVER LAYER =====
# Cleaned and transformed data layer - second stage of the data pipeline
# Responsibility: Transform raw data into clean, standardized, analysis-ready format
# 
# Key Operations:
# - Explode nested arrays (assignee, timestamps) into relational rows
# - Extract nested fields into flat columns
# - Filter incomplete records (only resolved issues)
# - Enforce data types (string for text, UTC datetime for timestamps)
# - Store in Parquet format for efficient columnar storage
#
# Output: Clean, normalized data ready for aggregation (gold layer)

# Import the bronze layer function to load raw data
from bronze import load_issues
import pandas as pd
from sla_calculation import get_holidays, calculate_business_hours

# Load raw Jira issues data from the bronze layer
df_issues = load_issues()

# Ensure assignee and timestamps columns contain lists (not None or other types)
# This prevents errors when exploding these columns later
df_issues["assignee"] = df_issues["assignee"].apply(lambda x: x if isinstance(x,list)else [])
df_issues["timestamps"] = df_issues["timestamps"].apply(lambda x: x if isinstance(x,list)else [])

# Explode nested arrays into separate rows
# If an issue has multiple assignees or timestamps, create one row per combination
# reset_index(drop=True) renumbers rows sequentially after exploding
df_exploded = df_issues.explode("assignee").explode("timestamps").reset_index(drop=True)

# Helper function to safely extract values from dictionaries
# Returns None if the input is not a dict or the key doesn't exist
def get_value(d,key):
    """Safely extract a value from a dictionary.
    
    Args:
        d: Dictionary or other type
        key: Key to extract
    
    Returns:
        Value if d is a dict and key exists, otherwise None
    """
    if isinstance(d,dict):
        return d.get(key)
    return None

# Extract assignee information from nested dictionaries into separate columns
df_exploded["assignee_id"] = df_exploded["assignee"].apply(lambda a: get_value(a, "id"))
df_exploded["assignee_name"] = df_exploded["assignee"].apply(lambda a: get_value(a, "name"))
df_exploded["assignee_email"] = df_exploded["assignee"].apply(lambda a: get_value(a, "email"))

# Extract timestamp information from nested dictionaries into separate columns
df_exploded["created_at"] = df_exploded["timestamps"].apply(lambda a: get_value(a,"created_at"))
df_exploded["resolved_at"] = df_exploded["timestamps"].apply(lambda a: get_value(a, "resolved_at"))

# Select only the columns needed for analysis (data normalization)
# This removes unnecessary columns and creates a clean, focused dataset
columns_to_keep = ["id", "issue_type", "status", "priority", "assignee_id", "assignee_name", "assignee_email", "created_at", "resolved_at"]
df_normalized = df_exploded[columns_to_keep]

# Filter out rows where resolved_at is missing
# Only keep resolved issues for analysis
df_normalized = df_normalized[df_normalized["resolved_at"].notna()]

# Convert all text fields to 'string' type (Pandas nullable string type)

df_normalized["id"] = df_normalized["id"].astype('string')
df_normalized["issue_type"] = df_normalized["issue_type"].astype('string')
df_normalized["status"] = df_normalized["status"].astype('string')
df_normalized["priority"] = df_normalized["priority"].astype('string')
df_normalized["assignee_id"] = df_normalized["assignee_id"].astype('string')
df_normalized["assignee_name"] = df_normalized["assignee_name"].astype('string')
df_normalized["assignee_email"] = df_normalized["assignee_email"].astype('string')

# Convert timestamp fields to proper datetime objects with UTC timezone
# errors="coerce" converts invalid dates to NaT (Not a Time) instead of raising errors
df_normalized["created_at"] = pd.to_datetime(df_normalized["created_at"], errors="coerce", utc=True)
df_normalized["resolved_at"] = pd.to_datetime(df_normalized["resolved_at"], errors="coerce", utc=True)

# Apply final filter to remove any rows with invalid resolved_at timestamps
df_normalized = df_normalized[df_normalized["resolved_at"].notna()]

# Calculate per-issue business-hour resolution time (reusable metric for gold layer)
holidays = get_holidays("holidays.json")
df_normalized["resolution_hours"] = df_normalized.apply(
    lambda row: calculate_business_hours(
        row["created_at"],
        row["resolved_at"],
        holidays
    ),
    axis=1
)
df_normalized["resolution_days"] = (df_normalized["resolution_hours"] / 8).round(2)

# Define SLA thresholds by priority (in business hours)
sla_thresholds = {
    "High": 24,
    "Medium": 72,
    "Low": 120
}

# Calculate expected SLA hours based on priority
df_normalized["sla_expected_hours"] = df_normalized["priority"].map(sla_thresholds)

# Check if SLA was met (resolution_hours <= sla_expected_hours)
df_normalized["sla_met"] = df_normalized["resolution_hours"] <= df_normalized["sla_expected_hours"]

# ===== Data Validation and Inspection =====

# Display summary statistics about the normalized dataset
print("\n=== Normalized DataFrame with SLA Metrics ===")
print(f"Rows: {df_normalized.shape[0]}, Columns: {df_normalized.shape[1]}\n")

# Show issues with SLA-relevant columns
print("=== Sample Issues (with SLA Analysis) ===")
sla_columns = ["id", "priority", "assignee_id", "resolution_hours", "sla_expected_hours", "sla_met"]
print(df_normalized[sla_columns].head(30).to_string(index=False))

# Show all column names for verification
print("\n=== All Columns ===")
print(df_normalized.columns.tolist())

# Display detailed info about data types and memory usage
print("\n=== DataFrame Info ===")
print(df_normalized.info())

# SLA Compliance Summary
print("\n=== SLA Compliance Summary ===")
sla_summary = df_normalized.groupby("priority").agg(
    total_issues=("id", "count"),
    met_sla=("sla_met", "sum"),
    sla_compliance_pct=("sla_met", lambda x: round(x.sum() / len(x) * 100, 2))
).reset_index()
print(sla_summary.to_string(index=False))

# Save the cleaned data to Parquet format
# Parquet preserves all data types (datetime, string, etc.) unlike CSV
df_normalized.to_parquet("silver_issues.parquet", index=False, engine="pyarrow")

# Display final data types for verification
print(df_normalized.dtypes)

# Why Parquet is the best format for the silver layer:
# - Industry standard for data engineering pipelines
# - Preserves all data types (datetime, int, float, string) without conversion
# - Compressed and efficient storage (smaller file sizes)
# - Columnar format enables fast queries on specific columns
# - Compatible with all major data tools (Spark, Pandas, Polars, Dask, etc.)
# - Supports schema evolution and metadata storage
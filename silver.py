from bronze import load_issues
import pandas as pd

df_issues = load_issues()

df_issues["assignee"] = df_issues["assignee"].apply(lambda x: x if isinstance(x,list)else [])
df_issues["timestamps"] = df_issues["timestamps"].apply(lambda x: x if isinstance(x,list)else [])

df_exploded = df_issues.explode("assignee").explode("timestamps").reset_index(drop=True)

def get_value(d,key):
    if isinstance(d,dict):
        return d.get(key)
    return None

df_exploded["assignee_id"] = df_exploded["assignee"].apply(lambda a: get_value(a, "id"))
df_exploded["assignee_name"] = df_exploded["assignee"].apply(lambda a: get_value(a, "name"))
df_exploded["assignee_email"] = df_exploded["assignee"].apply(lambda a: get_value(a, "email"))

df_exploded["created_at"] = df_exploded["timestamps"].apply(lambda a: get_value(a,"created_at"))
df_exploded["resolved_at"] = df_exploded["timestamps"].apply(lambda a: get_value(a, "resolved_at"))

columns_to_keep = ["id", "issue_type", "status", "priority", "assignee_id", "assignee_name", "assignee_email", "created_at", "resolved_at"]
df_normalized = df_exploded[columns_to_keep]

# Filter out rows where resolved_at is missing
df_normalized = df_normalized[df_normalized["resolved_at"].notna()]

# Convert all string fields to string type (will show as 'string' not 'object')
df_normalized["id"] = df_normalized["id"].astype('string')
df_normalized["issue_type"] = df_normalized["issue_type"].astype('string')
df_normalized["status"] = df_normalized["status"].astype('string')
df_normalized["priority"] = df_normalized["priority"].astype('string')
df_normalized["assignee_id"] = df_normalized["assignee_id"].astype('string')
df_normalized["assignee_name"] = df_normalized["assignee_name"].astype('string')
df_normalized["assignee_email"] = df_normalized["assignee_email"].astype('string')

# Convert datetime fields to UTC datetime
df_normalized["created_at"] = pd.to_datetime(df_normalized["created_at"], errors="coerce", utc=True)
df_normalized["resolved_at"] = pd.to_datetime(df_normalized["resolved_at"], errors="coerce", utc=True)

# Filter out rows where resolved_at is missing
df_normalized = df_normalized[df_normalized["resolved_at"].notna()]


print("\n=== Normalized DataFrame (After Exploding) ===")
print(f"Rows: {df_normalized.shape[0]}, Columns: {df_normalized.shape[1]}\n")
print(df_normalized.head(20).to_string(index=False))

print("\n=== All Columns ===")
print(df_normalized.columns.tolist())

print("\n=== DataFrame Info ===")
print(df_normalized.info())

#teste
print(df_normalized)

# Save to Parquet (preserves data types)
df_normalized.to_parquet("silver_issues.parquet", index=False, engine="pyarrow")

print(df_normalized.dtypes)

# Parquet is the best whaty to store data in the silver layer because:

# Industry standard for data engineering
# Preserves all data types (datetime, int, float, string)
# Compressed and efficient
# Works with all data tools (Spark, Pandas, Polars, etc
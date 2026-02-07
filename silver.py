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
# Exclude rows where `resolved_at` is missing (None/NaN/invalid)
df_normalized = df_normalized.copy()
df_normalized["resolved_at"] = pd.to_datetime(df_normalized["resolved_at"], errors="coerce")
df_normalized = df_normalized[df_normalized["resolved_at"].notna()]

df_normalized["id"] = df_normalized["id"].astype(str)
df_normalized["issue_type"] = df_normalized["issue_type"].astype(str)
df_normalized["status"] = df_normalized["statius"].astype(str)
df_normalized["priority"] = df_normalized["priority"].astype(str)
df_normalized["assignee_id"] = df_normalized["assignee_id"].astype(str)
df_normalized["assignee_name"] = df_normalized["assignee_name"].astype(str)
df_normalized["assignee_email"] = df_normalized["assignee_email"].astype(str)
df_normalized["resolved_at"] = df_normalized["resolved_at"].astype("datetime64[ns]")



print("\n=== Normalized DataFrame (After Exploding) ===")
print(f"Rows: {df_normalized.shape[0]}, Columns: {df_normalized.shape[1]}\n")
print(df_normalized.head(20).to_string(index=False))

print("\n=== All Columns ===")
print(df_normalized.columns.tolist())

print("\n=== DataFrame Info ===")
print(df_normalized.info())

print(df_normalized)

#teste
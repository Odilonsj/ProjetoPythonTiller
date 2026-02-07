from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import pandas as pd
import json
from json import JSONDecodeError


load_dotenv('credenciais.env')

url = "https://stfasttracksdev.blob.core.windows.net"
container_name = "source-jira"
blob_name = "jira_issues_raw.json"

client_id = os.environ.get("AZURE_CLIENT_ID")
client_secret = os.environ.get("AZURE_CLIENT_SECRET")
tenant_id = os.environ.get("AZURE_TENANT_ID")

credential = ClientSecretCredential(tenant_id, client_id, client_secret)
blob_service_client = BlobServiceClient(account_url=url, credential=credential)
container_client = blob_service_client.get_container_client(container_name)

local_file_path = "jira_issues_raw.json"

try:
    with open(local_file_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    issues = data.get("issues",[]) if isinstance(data,dict) else data
except JSONDecodeError:
    with open(local_file_path, "r", encoding="utf-8") as file:
        issues = [json.loads(line) for line in file if line.strip()]

df_issues = pd.json_normalize(issues)

# Display the DataFrame
print("\n=== DataFrame Shape ===")
print(f"Rows: {df_issues.shape[0]}, Columns: {df_issues.shape[1]}")

print("\n=== DataFrame Columns ===")
print(df_issues.columns.tolist())

print("\n=== First 10 Rows ===")
print(df_issues.head(10).to_string())

print("\n=== DataFrame Info ===")
print(df_issues.info())

print("\n=== Summary Statistics ===")
print(df_issues.describe())

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

print("\n=== Normalized DataFrame (After Exploding) ===")
print(f"Rows: {df_normalized.shape[0]}, Columns: {df_normalized.shape[1]}\n")
print(df_normalized.head(20).to_string(index=False))

print("\n=== All Columns ===")
print(df_normalized.columns.tolist())

print("\n=== DataFrame Info ===")
print(df_normalized.info())

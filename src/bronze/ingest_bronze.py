""" 
BRONZE LAYER  - Raw data ingestion layer - first stage of the data pipeline
Responsibility: Load raw data from source systems with minimal transformation

Load and transform Jira issues from JSON file.
    
    Downloads Jira issues data from Azure Blob Storage and converts it into a pandas DataFrame.
    Handles both standard JSON objects and newline-delimited JSON (NDJSON) formats.
    
    Returns:
        pd.DataFrame: Normalized DataFrame containing all Jira issues data
"""
from pathlib import Path

def load_issues():
    # Import required libraries for Azure authentication, blob storage access, and data processing
    from azure.identity import ClientSecretCredential
    from azure.storage.blob import BlobServiceClient
    from dotenv import load_dotenv
    import os
    import pandas as pd
    import json
    from json import JSONDecodeError
    
    # Load environment variables from credentials file
    project_root = Path(__file__).resolve().parents[2]
    credentials_path = project_root / "credenciais.env"
    load_dotenv(credentials_path)

    # Azure Blob Storage configuration
    url = "https://stfasttracksdev.blob.core.windows.net"  
    container_name = "source-jira"  
    blob_name = "jira_issues_raw.json"  

    # Retrieve Azure authentication credentials from environment variables
    client_id = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")
    tenant_id = os.environ.get("AZURE_TENANT_ID")

    # Authenticate with Azure using service principal credentials
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)

    # Initialize Blob Service Client to interact with Azure Storage
    blob_service_client = BlobServiceClient(account_url=url, credential=credential)
    
    # Get reference to the specific blob containing Jira issues
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Download blob from Azure and save locally with a standardized name
    local_file_path = project_root / "data" / "bronze" / "raw_jira_issues.json"
    local_file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Open a local file in binary write mode and write the contents of the blob to it
    with open(local_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())
    print(f"✓ Downloaded {blob_name} and saved as {local_file_path}")

    # Parse the downloaded JSON file
    try:
        # Attempt to load as standard JSON format
        with open(local_file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
        # Extract issues array if data is a dict, otherwise use data directly
        issues = data.get("issues",[]) if isinstance(data,dict) else data
    except JSONDecodeError:
        # If standard JSON fails, try reading as newline-delimited JSON (NDJSON)
        with open(local_file_path, "r", encoding="utf-8") as file:
            issues = [json.loads(line) for line in file if line.strip()]

    # Normalize nested JSON structure into a flat DataFrame
    df_issues = pd.json_normalize(issues)
    return df_issues

if __name__ == "__main__":
    df_issues = load_issues()

    # Display comprehensive information about the loaded DataFrame
    print("\n=== DataFrame Shape ===")
    print(f"Rows: {df_issues.shape[0]}, Columns: {df_issues.shape[1]}")

    # Show all column names in the DataFrame
    print("\n=== DataFrame Columns ===")
    print(df_issues.columns.tolist())

    # Preview the first 10 rows of data
    print("\n=== First 10 Rows ===")
    print(df_issues.head(10).to_string())

    # Display detailed information about data types and non-null counts
    print("\n=== DataFrame Info ===")
    print(df_issues.info())

    # Generate summary statistics for numeric columns
    print("\n=== Summary Statistics ===")
    print(df_issues.describe())


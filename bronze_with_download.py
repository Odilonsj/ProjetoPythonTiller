def load_issues():
    """Load and transform Jira issues from Azure Blob Storage or local file."""
    from azure.identity import ClientSecretCredential
    from azure.storage.blob import BlobServiceClient
    from dotenv import load_dotenv
    import os
    import pandas as pd
    import json
    from json import JSONDecodeError
    
    #Carregando variaveis de ambiente do arquivo .env
    load_dotenv('credenciais.env')

    url = "https://stfasttracksdev.blob.core.windows.net"
    container_name = "source-jira"
    blob_name = "jira_issues_raw.json"

    client_id = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")
    tenant_id = os.environ.get("AZURE_TENANT_ID")

    local_file_path = "raw_issues.json"

    # Download from Azure Blob Storage
    try:
        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        blob_service_client = BlobServiceClient(account_url=url, credential=credential)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        # Download and save as raw_issues.json
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        print(f"✓ Downloaded {blob_name} and saved as {local_file_path}")
    except Exception as e:
        print(f"⚠ Could not download from Azure: {e}")
        print(f"Using existing local file if available...")

    # Read the local file
    try:
        with open(local_file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
        issues = data.get("issues",[]) if isinstance(data,dict) else data
    except JSONDecodeError:
        with open(local_file_path, "r", encoding="utf-8") as file:
            issues = [json.loads(line) for line in file if line.strip()]

    df_issues = pd.json_normalize(issues)
    return df_issues

if __name__ == "__main__":
    df_issues = load_issues()

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

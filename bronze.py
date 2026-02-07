def load_issues():
    """Load and transform JIra issues from JSON file."""
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
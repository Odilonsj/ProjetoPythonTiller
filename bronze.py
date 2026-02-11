# ===== BRONZE LAYER =====
# Raw data ingestion layer - first stage of the data pipeline
# Responsibility: Load raw data from source systems with minimal transformation
# - Extracts data from Azure Blob Storage
# - Preserves original data structure and values
# - Converts to DataFrame for downstream processing
# - No data cleaning or business logic applied at this layer

def load_issues():
    """Load and transform Jira issues from JSON file.
    
    Downloads Jira issues data from Azure Blob Storage and converts it into a pandas DataFrame.
    Handles both standard JSON objects and newline-delimited JSON (NDJSON) formats.
    
    Returns:
        pd.DataFrame: Normalized DataFrame containing all Jira issues data
    """
    # Import required libraries for Azure authentication, blob storage access, and data processing
    from azure.identity import ClientSecretCredential
    from azure.storage.blob import BlobServiceClient
    from dotenv import load_dotenv
    import os
    import pandas as pd
    import json
    from json import JSONDecodeError
    
    # Load environment variables from credentials file
    load_dotenv('credenciais.env')

    # Azure Blob Storage configuration
    url = "https://stfasttracksdev.blob.core.windows.net"  # Azure Storage account URL
    container_name = "source-jira"  # Container holding the Jira data
    blob_name = "jira_issues_raw.json"  # Raw Jira issues file name

    # Retrieve Azure authentication credentials from environment variables
    # This is a best practices approach for handling sensitive information:
        # Security: Keeps sensitive credentials (client ID, secret, tenant ID) out of the source code
        # Configuration: Allows different credentials for development, testing, and production environments without changing code
        # Access mechanism: After load_dotenv() loads variables from credenciais.env, os.environ.get() retrieves them

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
    local_file_path = "raw_issues.json"
    
    # Open a new file in binary write mode ("wb")
    # key reasons for using binary mode:
        # Data type match: Azure's .readall() returns a bytes object, which can only be written in binary mode
        # Exact preservation: Binary mode writes the data exactly as received without any text encoding/decoding transformations
        # Prevents corruption: Text mode would try to interpret the bytes as text and could fail or modify the content
        # Universal compatibility: Works for any file type (JSON, images, PDFs, etc.) downloaded from blob storage
        # Binary for download → Text for reading/parsing. This is the standard pattern for downloading files.

    with open(local_file_path, "wb") as download_file:
        # Download the blob content from Azure and write all bytes to the local file
        download_file.write(blob_client.download_blob().readall())
    # Confirm successful download and show the saved filename
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
#When we run the script directly: python bronze.py → the code inside executes
#When we import the script: from bronze import load_issues → the code inside does NOT execute
#This is a Python best practice for making modular, reusable code

    # Execute the function to load Jira issues data
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
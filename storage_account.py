import os
from azure.storage.blob import BlobClient

# 1. Define variables
# It's best practice to use environment variables for sensitive data like connection strings.
connection_string = "YOUR_AZURE_STORAGE_CONNECTION_STRING"
source_container_name = "firstcontainer"
dest_container_name = "secondcontainer"
source_blob_name = "az204-004-003-httptrigger·Jan2025.mp4"
dest_blob_name = "az204-004-003-httptrigger·Jan2025·BACKUP.mp4"

# 2. Create clients for the source and destination blobs
source_blob_client = BlobClient.from_connection_string(
    conn_str=connection_string,
    container_name=source_container_name,
    blob_name=source_blob_name
)

dest_blob_client = BlobClient.from_connection_string(
    conn_str=connection_string,
    container_name=dest_container_name,
    blob_name=dest_blob_name
)

# 3. Start the server-side copy from the source URL
# The source_blob_client.url provides the URI needed for the copy operation.
print(f"Starting copy of '{source_blob_name}' to '{dest_blob_name}'...")
dest_blob_client.start_copy_from_url(source_blob_client.url)
print("Copy initiated successfully.")

# 4. Get the properties of the source blob
properties = source_blob_client.get_blob_properties()

# 5. Print the access tier of the source blob
# The property is called 'blob_tier' in the Python SDK
print(f"Source blob access tier: {properties.blob_tier}")
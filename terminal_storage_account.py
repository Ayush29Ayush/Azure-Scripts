import os
import time
from decouple import config
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

# --- CONFIGURATION ---
try:
    AZURE_CONNECTION_STRING = config('AZURE_CONNECTION_STRING')
except Exception as e:
    print("❌ FATAL ERROR: Could not read 'AZURE_CONNECTION_STRING' from your .env file or environment variables.")
    print("Please make sure the .env file exists and the variable is set.")
    exit()

# --- Main Client ---
# Create a single BlobServiceClient to be reused by all functions
try:
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
except ValueError as e:
    print("❌ FATAL ERROR: The connection string is invalid. Please check its value in your .env file.")
    print(f"Details: {e}")
    exit()

def clear_screen():
    """Clears the terminal screen."""
    os.system('cls' if os.name == 'nt' else 'clear')

def wait_for_copy(blob_client):
    """Checks the copy status of a blob until it's complete or has failed."""
    count = 0
    while True:
        props = blob_client.get_blob_properties()
        status = props.copy.status
        print(f"  > Copy status: '{status}'...")
        if status == "success":
            print("  ✅ Copy completed successfully.")
            return True
        if status in ["failed", "aborted"]:
            print(f"  ❌ Copy failed with status: '{status}'")
            print(f"  > Error details: {props.copy.status_description}")
            return False
        if count > 10: # Timeout after ~30 seconds to prevent infinite loops
            print("  ⌛ Copy is taking too long. Aborting wait.")
            return False
        count += 1
        time.sleep(3)

def copy_specific_blob():
    """Copies a specific blob without deleting the original."""
    print("--- Copy a Specific Blob ---")
    source_container = input("Enter the SOURCE container name: ")
    blob_name = input("Enter the exact name of the blob to copy: ")
    dest_container = input("Enter the DESTINATION container name: ")

    try:
        source_blob = blob_service_client.get_blob_client(source_container, blob_name)
        if not source_blob.exists():
            print(f"\n❌ ERROR: Blob '{blob_name}' not found in container '{source_container}'.")
            return

        dest_blob = blob_service_client.get_blob_client(dest_container, blob_name)

        print(f"\n➡️ Starting copy of '{blob_name}' from '{source_container}' to '{dest_container}'...")
        dest_blob.start_copy_from_url(source_blob.url)
        wait_for_copy(dest_blob)

    except ResourceNotFoundError:
        print(f"\n❌ ERROR: A container was not found. Please check your names.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

def copy_all_blobs():
    """Copies all blobs from one container to another without deleting originals."""
    print("--- Copy All Blobs from a Container ---")
    source_container = input("Enter the SOURCE container name: ")
    dest_container = input("Enter the DESTINATION container name: ")
    
    try:
        source_container_client = blob_service_client.get_container_client(source_container)
        blob_list = list(source_container_client.list_blobs())
        total_blobs = len(blob_list)

        if total_blobs == 0:
            print(f"No blobs found in container '{source_container}'.")
            return

        print(f"\nFound {total_blobs} blob(s) to copy.")
        for i, blob in enumerate(blob_list):
            print(f"\n--- [{i+1}/{total_blobs}] Copying '{blob.name}' ---")
            source_blob = blob_service_client.get_blob_client(source_container, blob.name)
            dest_blob = blob_service_client.get_blob_client(dest_container, blob.name)
            
            dest_blob.start_copy_from_url(source_blob.url)
            wait_for_copy(dest_blob)
        
        print("\n✅ All copy operations completed.")

    except ResourceNotFoundError:
        print(f"\n❌ ERROR: Source container '{source_container}' not found.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

def move_specific_blob():
    """Moves a specific blob (copies and then deletes original)."""
    print("--- Move a Specific Blob (Copy + Delete) ---")
    source_container = input("Enter the SOURCE container name: ")
    blob_name = input("Enter the exact name of the blob to move: ")
    dest_container = input("Enter the DESTINATION container name: ")

    try:
        source_blob = blob_service_client.get_blob_client(source_container, blob_name)
        if not source_blob.exists():
            print(f"\n❌ ERROR: Blob '{blob_name}' not found in container '{source_container}'.")
            return

        dest_blob = blob_service_client.get_blob_client(dest_container, blob_name)

        print(f"\n➡️ Starting copy of '{blob_name}' from '{source_container}' to '{dest_container}'...")
        dest_blob.start_copy_from_url(source_blob.url)
        
        if wait_for_copy(dest_blob):
            print(f"🗑️ Deleting original blob '{blob_name}' from '{source_container}'...")
            source_blob.delete_blob()
            print("\n✅ Move operation completed successfully!")
        else:
            print("\n❌ Move operation failed. Original blob was NOT deleted.")

    except ResourceNotFoundError:
        print(f"\n❌ ERROR: A container or blob was not found. Please check your names.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")


def move_all_blobs():
    """Moves all blobs from one container to another (copies and then deletes originals)."""
    print("--- Move All Blobs from a Container (Copy + Delete) ---")
    source_container = input("Enter the SOURCE container name: ")
    dest_container = input("Enter the DESTINATION container name: ")
    
    confirm = input(f"⚠️ Are you sure you want to MOVE (copy and delete) ALL blobs from '{source_container}'? (y/n): ")
    if confirm.lower() != 'y':
        print("Operation cancelled.")
        return

    try:
        source_container_client = blob_service_client.get_container_client(source_container)
        blob_list = list(source_container_client.list_blobs())
        total_blobs = len(blob_list)

        if total_blobs == 0:
            print(f"No blobs found in container '{source_container}'.")
            return

        print(f"\nFound {total_blobs} blob(s) to move.")
        for i, blob in enumerate(blob_list):
            print(f"\n--- [{i+1}/{total_blobs}] Moving '{blob.name}' ---")
            source_blob = blob_service_client.get_blob_client(source_container, blob.name)
            dest_blob = blob_service_client.get_blob_client(dest_container, blob.name)

            dest_blob.start_copy_from_url(source_blob.url)
            if wait_for_copy(dest_blob):
                print(f"  🗑️ Deleting original blob '{blob.name}'...")
                source_blob.delete_blob()
            else:
                print(f"  ❌ Failed to move '{blob.name}'. Skipping deletion.")
        
        print("\n✅ All move operations completed.")

    except ResourceNotFoundError:
        print(f"\n❌ ERROR: Source container '{source_container}' not found.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

def get_access_tier():
    """Gets the access tier of a specific blob."""
    print("--- Get Blob Access Tier ---")
    container_name = input("Enter the container name: ")
    blob_name = input("Enter the blob name: ")
    try:
        blob_client = blob_service_client.get_blob_client(container_name, blob_name)
        properties = blob_client.get_blob_properties()
        print("\n---------------------------------")
        print(f"Blob:          {blob_name}")
        print(f"Access Tier:   {properties.blob_tier}")
        print("---------------------------------")
    except ResourceNotFoundError:
        print(f"\n❌ ERROR: Blob '{blob_name}' in container '{container_name}' not found.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

def get_blob_properties():
    """Gets all properties of a specific blob."""
    print("--- Get All Blob Properties ---")
    container_name = input("Enter the container name: ")
    blob_name = input("Enter the blob name: ")
    try:
        blob_client = blob_service_client.get_blob_client(container_name, blob_name)
        properties = blob_client.get_blob_properties()
        print(f"\n--- Properties for '{blob_name}' ---")
        for prop, value in properties.items():
            print(f"{prop:<25}: {value}")
        print("-------------------------------------")
    except ResourceNotFoundError:
        print(f"\n❌ ERROR: Blob '{blob_name}' in container '{container_name}' not found.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

def view_blob_metadata():
    """Views the custom metadata of a specific blob."""
    print("--- View Blob Metadata ---")
    container_name = input("Enter the container name: ")
    blob_name = input("Enter the blob name: ")
    try:
        blob_client = blob_service_client.get_blob_client(container_name, blob_name)
        properties = blob_client.get_blob_properties()
        metadata = properties.metadata

        print(f"\n--- Metadata for '{blob_name}' ---")
        if not metadata:
            print("No metadata found for this blob.")
        else:
            for key, value in metadata.items():
                print(f"{key}: {value}")
        print("------------------------------------")
    except ResourceNotFoundError:
        print(f"\n❌ ERROR: Blob '{blob_name}' in container '{container_name}' not found.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

def set_blob_metadata():
    """Sets the custom metadata for a specific blob. This will overwrite any existing metadata."""
    print("--- Set Blob Metadata ---")
    container_name = input("Enter the container name: ")
    blob_name = input("Enter the blob name: ")

    try:
        blob_client = blob_service_client.get_blob_client(container_name, blob_name)
        if not blob_client.exists():
            print(f"\n❌ ERROR: Blob '{blob_name}' in container '{container_name}' not found.")
            return

        print("\nEnter new metadata key-value pairs. Press Enter on an empty key to finish.")
        metadata = {}
        while True:
            key = input("Enter metadata key: ")
            if not key:
                break
            value = input(f"Enter value for '{key}': ")
            metadata[key] = value
        
        if not metadata:
            print("\nNo metadata entered. Operation cancelled.")
            return

        print("\nSetting the following metadata:")
        for k, v in metadata.items():
            print(f"  {k}: {v}")
        
        confirm = input("This will OVERWRITE all existing metadata. Continue? (y/n): ")
        if confirm.lower() == 'y':
            blob_client.set_blob_metadata(metadata=metadata)
            print("\n✅ Metadata set successfully!")
        else:
            print("\nOperation cancelled.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

def main_menu():
    """Displays the main menu and handles user input."""
    while True:
        clear_screen()
        print("========== Azure Blob Manager ==========")
        print("\n--- Copy & Move Operations ---")
        print("1. Copy a specific blob")
        print("2. Copy ALL blobs in a container")
        print("3. Move a specific blob (deletes original)")
        print("4. Move ALL blobs in a container (deletes originals)")
        print("\n--- Information & Properties ---")
        print("5. Get a blob's access tier")
        print("6. Get all of a blob's properties")
        print("7. View a blob's metadata")
        print("8. Set/Update a blob's metadata")
        print("\nQ. Quit")
        print("========================================")
        choice = input("Enter your choice: ").lower()

        clear_screen()
        if choice == '1':
            copy_specific_blob()
        elif choice == '2':
            copy_all_blobs()
        elif choice == '3':
            move_specific_blob()
        elif choice == '4':
            move_all_blobs()
        elif choice == '5':
            get_access_tier()
        elif choice == '6':
            get_blob_properties()
        elif choice == '7':
            view_blob_metadata()
        elif choice == '8':
            set_blob_metadata()
        elif choice == 'q':
            print("Exiting. Goodbye! 👋")
            break
        else:
            print("Invalid choice. Please try again.")
        
        print("\n")
        input("Press Enter to return to the menu...")

if __name__ == "__main__":
    main_menu()
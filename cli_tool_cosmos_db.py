import os
import json
from decouple import config
from azure.cosmos import CosmosClient, PartitionKey
from azure.core.exceptions import ResourceNotFoundError
from azure.cosmos.exceptions import CosmosResourceNotFoundError

# --- CONFIGURATION ---
# Read Cosmos DB credentials from the .env file
try:
    ENDPOINT = config('COSMOS_ENDPOINT')
    KEY = config('COSMOS_KEY')
except Exception:
    print("❌ FATAL ERROR: Could not read 'COSMOS_ENDPOINT' or 'COSMOS_KEY' from your .env file.")
    print("Please make sure the .env file exists and the variables are set correctly.")
    exit()

# --- Main Client ---
# Initialize the single CosmosClient to be reused by all functions
try:
    client = CosmosClient(ENDPOINT, credential=KEY)
except Exception as e:
    print(f"❌ FATAL ERROR: Could not connect to Cosmos DB. Please check your credentials in the .env file.")
    print(f"Details: {e}")
    exit()

def clear_screen():
    """Clears the terminal screen."""
    os.system('cls' if os.name == 'nt' else 'clear')

def list_databases():
    """Lists all databases in the Cosmos DB account."""
    print("--- Databases in your account ---")
    try:
        databases = list(client.list_databases())
        if not databases:
            print("No databases found.")
        else:
            for db in databases:
                print(f"- {db['id']}")
    except Exception as e:
        print(f"An error occurred: {e}")

def list_containers():
    """Lists all containers in a specified database."""
    print("--- List Containers in a Database ---")
    db_name = input("Enter the database name: ")
    try:
        db_client = client.get_database_client(db_name)
        containers = list(db_client.list_containers())
        if not containers:
            print(f"No containers found in database '{db_name}'.")
        else:
            print(f"\n--- Containers in '{db_name}' ---")
            for container in containers:
                print(f"- {container['id']}")
    except ResourceNotFoundError:
        print(f"❌ ERROR: Database '{db_name}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

def create_item():
    """Creates a new item in a specified container."""
    print("--- Create a New Item ---")
    db_name = input("Enter the database name: ")
    container_name = input("Enter the container name: ")
    
    print("\nEnter the item data as a JSON object. For example:")
    print('{"id": "item3", "category": "books", "title": "The Great Gatsby"}')
    json_input = input("JSON data: ")
    
    try:
        item_data = json.loads(json_input)
        container_client = client.get_database_client(db_name).get_container_client(container_name)
        container_client.create_item(body=item_data)
        print("\n✅ Item created successfully!")
    except (ResourceNotFoundError, CosmosResourceNotFoundError):
        print(f"❌ ERROR: Database '{db_name}' or container '{container_name}' not found.")
    except json.JSONDecodeError:
        print("❌ ERROR: Invalid JSON format.")
    except Exception as e:
        print(f"An error occurred: {e}")

def read_item():
    """Reads a specific item by its ID and partition key."""
    print("--- Read a Specific Item ---")
    db_name = input("Enter the database name: ")
    container_name = input("Enter the container name: ")
    item_id = input("Enter the item 'id' to read: ")
    part_key = input("Enter the partition key value for this item: ")

    try:
        container_client = client.get_database_client(db_name).get_container_client(container_name)
        item = container_client.read_item(item=item_id, partition_key=part_key)
        print("\n--- Item Found ---")
        # Pretty-print the JSON output
        print(json.dumps(item, indent=4))
    except (ResourceNotFoundError, CosmosResourceNotFoundError):
        print(f"❌ ERROR: Item '{item_id}' not found in '{container_name}' (or resource does not exist).")
    except Exception as e:
        print(f"An error occurred: {e}")

def query_items():
    """Queries for items in a container using a SQL query."""
    print("--- Query Items in a Container ---")
    db_name = input("Enter the database name: ")
    container_name = input("Enter the container name: ")
    query = input("Enter your SQL query (e.g., SELECT * FROM c WHERE c.category = 'books'): ")

    try:
        container_client = client.get_database_client(db_name).get_container_client(container_name)
        items = list(container_client.query_items(
            query=query,
            enable_cross_partition_query=True
        ))
        
        print(f"\n--- Query Results ({len(items)} items) ---")
        if not items:
            print("No items found for this query.")
        else:
            for item in items:
                print(json.dumps(item, indent=4))
                print("-" * 20)
    except (ResourceNotFoundError, CosmosResourceNotFoundError):
        print(f"❌ ERROR: Database '{db_name}' or container '{container_name}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

def delete_item():
    """Deletes a specific item by its ID and partition key."""
    print("--- Delete a Specific Item ---")
    db_name = input("Enter the database name: ")
    container_name = input("Enter the container name: ")
    item_id = input("Enter the item 'id' to delete: ")
    part_key = input("Enter the partition key value for this item: ")

    confirm = input(f"⚠️ Are you sure you want to delete item '{item_id}'? This cannot be undone. (y/n): ")
    if confirm.lower() != 'y':
        print("Operation cancelled.")
        return

    try:
        container_client = client.get_database_client(db_name).get_container_client(container_name)
        container_client.delete_item(item=item_id, partition_key=part_key)
        print("\n✅ Item deleted successfully!")
    except (ResourceNotFoundError, CosmosResourceNotFoundError):
        print(f"❌ ERROR: Item '{item_id}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


def main_menu():
    """Displays the main menu and handles user input."""
    while True:
        clear_screen()
        print("========== Azure Cosmos DB Manager ==========")
        print("\n--- Discovery ---")
        print("1. List all databases")
        print("2. List containers in a database")
        print("\n--- Item Operations (CRUD) ---")
        print("3. Create an item")
        print("4. Read an item (by ID and Partition Key)")
        print("5. Query items (with SQL)")
        print("6. Delete an item")
        print("\nQ. Quit")
        print("===========================================")
        choice = input("Enter your choice: ").lower()

        clear_screen()
        if choice == '1':
            list_databases()
        elif choice == '2':
            list_containers()
        elif choice == '3':
            create_item()
        elif choice == '4':
            read_item()
        elif choice == '5':
            query_items()
        elif choice == '6':
            delete_item()
        elif choice == 'q':
            print("Exiting. Goodbye! 👋")
            break
        else:
            print("Invalid choice. Please try again.")
        
        print("\n")
        input("Press Enter to return to the menu...")

if __name__ == "__main__":
    main_menu()
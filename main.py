import os
import requests
import yaml

# Environment configuration
DATABRICKS_INSTANCE = os.environ.get('DATABRICKS_HOST')
TOKEN = os.environ.get('DATABRICKS_TOKEN')
HEADERS = {'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/scim+json'}


def send_request(method, endpoint, body=None):
    """Send API requests to Databricks"""
    url = f"{DATABRICKS_INSTANCE}/api/2.0/preview/scim/v2/{endpoint}"
    response = requests.request(method, url, headers=HEADERS, json=body)
    if response.status_code not in [200, 201]:
        raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
    return response.json()


def get_user_id(email):
    """Retrieve the Databricks SCIM ID for a user by email"""
    response = send_request("GET", f"Users?filter=userName eq '{email}'")
    if response['totalResults'] == 1:
        return response['Resources'][0]['id']
    else:
        print(f"User {email} not found.")
        return None


def get_group_id(group_name):
    """Retrieve the Databricks SCIM ID for a group by name"""
    response = send_request("GET", f"Groups?filter=displayName eq '{group_name}'")
    if response['totalResults'] == 1:
        return response['Resources'][0]['id']
    else:
        print(f"Group {group_name} not found.")
        return None


def add_user_to_group(user_email, group_name):
    """Add a user to a group in Databricks"""
    user_id = get_user_id(user_email)
    group_id = get_group_id(group_name)

    if user_id and group_id:
        update_body = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{
                "op": "add",
                "path": "members",
                "value": [{"value": user_id}]
            }]
        }
        send_request("PATCH", f"Groups/{group_id}", update_body)
        print(f"Added user {user_email} to group {group_name}.")


def manage_access_from_yaml(file_path):
    """Read the YAML configuration and manage access in Databricks"""
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)

    # Assuming users and groups are already created and we're focusing on adding users to groups here
    for user in config.get('users', []):
        for group in user.get('groups', []):
            add_user_to_group(user['email'], group)


if __name__ == "__main__":
    yaml_file_path = 'path/to/your/access.yaml'  # Update this path accordingly
    manage_access_from_yaml(yaml_file_path)

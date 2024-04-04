import os
import requests
import yaml

# Azure AD and Databricks configuration
TENANT_ID = os.environ.get('AZURE_TENANT_ID')
CLIENT_ID = os.environ.get('AZURE_CLIENT_ID')
CLIENT_SECRET = os.environ.get('AZURE_CLIENT_SECRET')
DATABRICKS_RESOURCE_ID = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d'
DATABRICKS_INSTANCE = os.environ.get('DATABRICKS_INSTANCE')

def get_access_token():
    """Authenticate using service principal and secret to obtain an access token."""
    url = f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/token'
    payload = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'resource': DATABRICKS_RESOURCE_ID
    }
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        raise Exception(f"Failed to obtain access token: {response.text}")

def send_request(method, endpoint, body=None):
    """Send API requests to Databricks using the obtained access token."""
    token = get_access_token()
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/scim+json'}
    url = f"{DATABRICKS_INSTANCE}/api/2.0/preview/scim/v2/{endpoint}"
    response = requests.request(method, url, headers=headers, json=body)
    if response.status_code not in [200, 201]:
        raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
    return response.json()

def create_or_get_user(email):
    """Create a new user in Databricks or get existing user's ID."""
    users = send_request("GET", f"Users?filter=userName eq '{email}'")
    if users['totalResults'] == 0:
        user_data = {"schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"], "userName": email, "emails": [{"value": email}]}
        user = send_request("POST", "Users", user_data)
        return user['id']
    else:
        return users['Resources'][0]['id']

def create_or_get_group(name):
    """Create a new group in Databricks or get existing group's ID."""
    groups = send_request("GET", f"Groups?filter=displayName eq '{name}'")
    if groups['totalResults'] == 0:
        group_data = {"schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"], "displayName": name}
        group = send_request("POST", "Groups", group_data)
        return group['id']
    else:
        return groups['Resources'][0]['id']

def is_user_in_group(user_id, group_id):
    """Check if a user is already a member of a group."""
    group_details = send_request("GET", f"Groups/{group_id}")
    return any(member['value'] == user_id for member in group_details.get('members', []))

def add_user_to_group(user_id, group_id):
    """Add a user to a group in Databricks if they're not already a member."""
    if not is_user_in_group(user_id, group_id):
        update_body = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{
                "op": "add",
                "path": "members",
                "value": [{"value": user_id}]
            }]
        }
        send_request("PATCH", f"Groups/{group_id}", update_body)
        print(f"User {user_id} added to group {group_id}.")
    else:
        print(f"User {user_id} is already in group {group_id}.")

def manage_access_from_yaml(file_path):
    """Read the YAML configuration and manage access in Databricks."""
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)

    for user in config.get('users', []):
        user_id = create_or_get_user(user['email'])
        for group_name in user.get('groups', []):
            group_id = create_or_get_group(group_name)
            add_user_to_group(user_id, group_id)

if __name__ == "__main__":
    yaml_file_path = 'path/to/your/access.yaml'  # Ensure this path is correct
    manage_access_from_yaml(yaml_file_path)

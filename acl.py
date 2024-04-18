# Import required libraries
import yaml
import requests
import json

# Function to read YAML configuration
def read_yaml_config(file_path):
    with open(file_path, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            return None

# Function to apply ACL changes
def apply_acl_changes(config):
    for entry in config:
        object_name = entry['object']
        permissions = entry['permissions']
        
        # Iterate through permissions and apply them
        for perm in permissions:
            if 'user' in perm:
                user_or_group = perm['user']
                permission = perm['permission']
                payload = {
                    "access_control_list": [
                        {
                            "user_name": user_or_group,
                            "permission_level": permission.upper()
                        }
                    ]
                }
                # Make API call to assign permission to user for object_name
                dbutils.fs.put(f"/unity/{object_name}/_acl", json.dumps(payload))
                print(f"Permission '{permission}' granted to user '{user_or_group}' for '{object_name}'")

            elif 'group' in perm:
                user_or_group = perm['group']
                permission = perm['permission']
                payload = {
                    "access_control_list": [
                        {
                            "group_name": user_or_group,
                            "permission_level": permission.upper()
                        }
                    ]
                }
                # Make API call to assign permission to group for object_name
                dbutils.fs.put(f"/unity/{object_name}/_acl", json.dumps(payload))
                print(f"Permission '{permission}' granted to group '{user_or_group}' for '{object_name}'")

# Main function
def main():
    config_file_path = '/dbfs/path/to/acl_config.yaml'
    config = read_yaml_config(config_file_path)
    if config:
        apply_acl_changes(config)
        print("ACL changes applied successfully.")
    else:
        print("Failed to read YAML configuration.")

if __name__ == "__main__":
    main()

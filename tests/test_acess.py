import unittest
from unittest.mock import patch
from main import create_or_get_user, add_user_to_group


class TestDatabricksAccessManagement(unittest.TestCase):
    @patch('your_script_name.send_request')
    def test_create_or_get_user_creates_new_user(self, mock_send_request):
        # Setup mock response for user creation
        mock_send_request.side_effect = [
            {"totalResults": 0},  # User query response (user doesn't exist)
            {"id": "new_user_id"}  # User creation response
        ]

        # Test creating a new user
        user_id = create_or_get_user("new.user@example.com")
        self.assertEqual(user_id, "new_user_id")
        self.assertEqual(mock_send_request.call_count, 2)

    @patch('your_script_name.send_request')
    def test_add_user_to_group_when_not_member(self, mock_send_request):
        # Setup mock responses
        mock_send_request.side_effect = [
            {"members": []},  # Group details response (user not in group)
            None  # Expected PATCH response for adding user to group
        ]

        # Test adding user to group
        add_user_to_group("user_id", "group_id")
        # Ensure `send_request` was called twice: first to get group details, then to add the user
        self.assertEqual(mock_send_request.call_count, 2)

    @patch('your_script_name.send_request')
    def test_add_user_to_group_when_already_member(self, mock_send_request):
        # Setup mock response indicating user is already a group member
        mock_send_request.return_value = {"members": [{"value": "user_id"}]}

        # Test adding user to group
        add_user_to_group("user_id", "group_id")
        # `send_request` should be called only once to get group details; no call to add user
        mock_send_request.assert_called_once_with("GET", "Groups/group_id", None)


# More tests can be added following the patterns above.

if __name__ == '__main__':
    unittest.main()

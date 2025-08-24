"""
Comprehensive dummy payloads for testing user serializers and API endpoints
"""

# ============================================================================
# USER REGISTRATION PAYLOADS
# ============================================================================

# Valid registration payloads
VALID_REGISTRATION_PAYLOAD = {
    "username": "john_doe",
    "email": "john.doe@example.com",
    "password": "SecurePass123!",
    "password_confirm": "SecurePass123!",
    "first_name": "John",
    "last_name": "Doe"
}

VALID_REGISTRATION_PAYLOAD_2 = {
    "username": "jane_smith",
    "email": "jane.smith@company.com",
    "password": "MySecurePassword456!",
    "password_confirm": "MySecurePassword456!",
    "first_name": "Jane",
    "last_name": "Smith"
}

VALID_REGISTRATION_PAYLOAD_3 = {
    "username": "admin_user",
    "email": "admin@analytics.com",
    "password": "Admin@2024#Secure",
    "password_confirm": "Admin@2024#Secure",
    "first_name": "Admin",
    "last_name": "User"
}

# Invalid registration payloads
INVALID_PASSWORD_MISMATCH = {
    "username": "test_user",
    "email": "test@example.com",
    "password": "SecurePass123!",
    "password_confirm": "DifferentPassword123!",
    "first_name": "Test",
    "last_name": "User"
}

INVALID_PASSWORD_TOO_SHORT = {
    "username": "short_pass_user",
    "email": "short@example.com",
    "password": "short",
    "password_confirm": "short",
    "first_name": "Short",
    "last_name": "Password"
}

INVALID_PASSWORD_COMMON = {
    "username": "common_pass_user",
    "email": "common@example.com",
    "password": "password123",
    "password_confirm": "password123",
    "first_name": "Common",
    "last_name": "Password"
}

INVALID_MISSING_FIELDS = {
    "username": "incomplete_user",
    "email": "incomplete@example.com",
    "password": "SecurePass123!"
    # Missing password_confirm, first_name, last_name
}

INVALID_EMAIL_FORMAT = {
    "username": "invalid_email_user",
    "email": "invalid-email-format",
    "password": "SecurePass123!",
    "password_confirm": "SecurePass123!",
    "first_name": "Invalid",
    "last_name": "Email"
}

# ============================================================================
# USER LOGIN PAYLOADS
# ============================================================================

# Valid login payloads
VALID_LOGIN_EMAIL = {
    "email": "john.doe@example.com",
    "password": "SecurePass123!"
}

VALID_LOGIN_EMAIL_2 = {
    "email": "jane.smith@company.com",
    "password": "MySecurePassword456!"
}

# Invalid login payloads
INVALID_LOGIN_WRONG_PASSWORD = {
    "email": "john.doe@example.com",
    "password": "WrongPassword123!"
}

INVALID_LOGIN_NON_EXISTENT = {
    "email": "nonexistent@example.com",
    "password": "SecurePass123!"
}

INVALID_LOGIN_MISSING_CREDENTIALS = {
    "email": "john.doe@example.com"
    # Missing password
}

# ============================================================================
# PASSWORD CHANGE PAYLOADS
# ============================================================================

# Valid password change
VALID_PASSWORD_CHANGE = {
    "current_password": "SecurePass123!",
    "new_password": "NewSecurePass456!",
    "confirm_password": "NewSecurePass456!"
}

# Invalid password change payloads
INVALID_PASSWORD_CHANGE_WRONG_CURRENT = {
    "current_password": "WrongCurrentPass123!",
    "new_password": "NewSecurePass456!",
    "confirm_password": "NewSecurePass456!"
}

INVALID_PASSWORD_CHANGE_MISMATCH = {
    "current_password": "SecurePass123!",
    "new_password": "NewSecurePass456!",
    "confirm_password": "DifferentPassword789!"
}

INVALID_PASSWORD_CHANGE_TOO_SHORT = {
    "current_password": "SecurePass123!",
    "new_password": "short",
    "confirm_password": "short"
}

# ============================================================================
# REFRESH TOKEN PAYLOADS
# ============================================================================

# Valid refresh token
VALID_REFRESH_TOKEN = {
    "refresh_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTYzNDU2Nzg5MCwianRpIjoiYzE2YzFhYzFhYzFhYzFhYzFhYzFhYzFhYzFhYzFhYyIsInVzZXJfaWQiOiJhYzFhYzFhYzFhYzFhYzFhYzFhYzFhYzFhYzFhYzFhYyJ9.example_signature"
}

# Invalid refresh token
INVALID_REFRESH_TOKEN = {
    "refresh_token": "invalid_token_string"
}

# ============================================================================
# USER DETAIL UPDATE PAYLOADS
# ============================================================================

# Valid user detail update
VALID_USER_DETAIL_UPDATE = {
    "first_name": "Updated",
    "last_name": "Name",
    "email": "updated.email@example.com"
}

# Partial user detail update
PARTIAL_USER_DETAIL_UPDATE = {
    "first_name": "Partial"
    # Only updating first name
}

# ============================================================================
# COMPLETE USER PROFILE PAYLOADS
# ============================================================================

# Extended user profile data
COMPLETE_USER_PROFILE = {
    "user": {
        "username": "complete_profile_user",
        "email": "complete@profile.com",
        "password": "SecurePass123!",
        "password_confirm": "SecurePass123!",
        "first_name": "Complete",
        "last_name": "Profile",
        "phone_number": "+1234567890",
        "date_of_birth": "1990-01-15",
        "address": "123 Main Street, Suite 100",
        "city": "New York",
        "country": "USA"
    },
    "profile": {
        "job_title": "Senior Data Analyst",
        "company": "Analytics Corp",
        "department": "Data Science",
        "timezone": "America/New_York",
        "language": "en",
        "notification_preferences": {
            "email_notifications": True,
            "sms_notifications": False,
            "push_notifications": True
        },
        "linkedin_url": "https://linkedin.com/in/completeprofile",
        "twitter_url": "https://twitter.com/completeprofile",
        "github_url": "https://github.com/completeprofile"
    }
}

# ============================================================================
# BULK TEST DATA
# ============================================================================

# Multiple valid registration payloads for bulk testing
BULK_REGISTRATION_PAYLOADS = [
    {
        "username": f"user_{i}",
        "email": f"user{i}@example.com",
        "password": "SecurePass123!",
        "password_confirm": "SecurePass123!",
        "first_name": f"User{i}",
        "last_name": "Test"
    }
    for i in range(1, 6)  # Creates 5 test users
]

# ============================================================================
# EDGE CASE PAYLOADS
# ============================================================================

# Very long username
LONG_USERNAME_PAYLOAD = {
    "username": "a" * 150,  # Very long username
    "email": "long@example.com",
    "password": "SecurePass123!",
    "password_confirm": "SecurePass123!",
    "first_name": "Long",
    "last_name": "Username"
}

# Special characters in username
SPECIAL_CHARS_USERNAME_PAYLOAD = {
    "username": "user@#$%^&*()",
    "email": "special@example.com",
    "password": "SecurePass123!",
    "password_confirm": "SecurePass123!",
    "first_name": "Special",
    "last_name": "Chars"
}

# Unicode characters
UNICODE_PAYLOAD = {
    "username": "user_ñáéíóú",
    "email": "unicode@example.com",
    "password": "SecurePass123!",
    "password_confirm": "SecurePass123!",
    "first_name": "José",
    "last_name": "García"
}

# ============================================================================
# API TEST SCENARIOS
# ============================================================================

# Test scenarios for different API endpoints
API_TEST_SCENARIOS = {
    "registration_success": {
        "payload": VALID_REGISTRATION_PAYLOAD,
        "expected_status": 201,
        "description": "Successful user registration"
    },
    "registration_password_mismatch": {
        "payload": INVALID_PASSWORD_MISMATCH,
        "expected_status": 400,
        "description": "Registration with mismatched passwords"
    },
    "login_success": {
        "payload": VALID_LOGIN_EMAIL,
        "expected_status": 200,
        "description": "Successful login with email"
    },
    "login_failure": {
        "payload": INVALID_LOGIN_WRONG_PASSWORD,
        "expected_status": 400,
        "description": "Login with wrong password"
    },
    "password_change_success": {
        "payload": VALID_PASSWORD_CHANGE,
        "expected_status": 200,
        "description": "Successful password change"
    }
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_test_payload(serializer_type, scenario="valid"):
    """
    Get test payload based on serializer type and scenario
    
    Args:
        serializer_type (str): Type of serializer ('registration', 'login', 'password_change', etc.)
        scenario (str): Test scenario ('valid', 'invalid', 'edge_case', etc.)
    
    Returns:
        dict: Test payload
    """
    payloads = {
        "registration": {
            "valid": VALID_REGISTRATION_PAYLOAD,
            "invalid": INVALID_PASSWORD_MISMATCH,
            "edge_case": LONG_USERNAME_PAYLOAD
        },
        "login": {
            "valid": VALID_LOGIN_EMAIL,
            "invalid": INVALID_LOGIN_WRONG_PASSWORD,
            "edge_case": INVALID_LOGIN_NON_EXISTENT
        },
        "password_change": {
            "valid": VALID_PASSWORD_CHANGE,
            "invalid": INVALID_PASSWORD_CHANGE_WRONG_CURRENT,
            "edge_case": INVALID_PASSWORD_CHANGE_TOO_SHORT
        }
    }
    
    return payloads.get(serializer_type, {}).get(scenario, {})

def generate_random_payload(base_payload, **overrides):
    """
    Generate a random payload based on a base payload with optional overrides
    
    Args:
        base_payload (dict): Base payload to modify
        **overrides: Key-value pairs to override in the base payload
    
    Returns:
        dict: Modified payload
    """
    import random
    import string
    
    payload = base_payload.copy()
    
    # Generate random username if not provided
    if "username" not in overrides:
        random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        payload["username"] = f"test_user_{random_suffix}"
    
    # Generate random email if not provided
    if "email" not in overrides:
        random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        payload["email"] = f"test{random_suffix}@example.com"
    
    # Apply overrides
    payload.update(overrides)
    
    return payload

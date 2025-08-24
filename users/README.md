# User Authentication Module

This module provides a complete JWT-based authentication system for the analytics application.

## Features

- **User Registration**: Create new user accounts with email verification
- **User Login**: Authenticate users with username/email and password
- **JWT Authentication**: Secure token-based authentication
- **Token Refresh**: Automatic token refresh mechanism
- **Session Management**: Track and manage user sessions
- **Password Management**: Change password functionality
- **Profile Management**: User profile CRUD operations

## Models

### User
- Custom user model extending Django's AbstractUser
- Additional fields: phone_number, date_of_birth, profile_picture, address, etc.
- Email and phone verification status
- Password reset and email verification tokens

### UserProfile
- Extended user profile information
- Professional details (job title, company, department)
- Preferences (timezone, language, notifications)
- Social media links

### UserSession
- Track user sessions for JWT token management
- Device information and IP address tracking
- Token expiry management
- Session revocation capabilities

### PasswordResetRequest
- Track password reset requests
- Token validation and expiry management

## API Endpoints

### Authentication
- `POST /api/users/register/` - User registration
- `POST /api/users/login/` - User login
- `POST /api/users/logout/` - User logout
- `POST /api/users/token/refresh/` - Refresh JWT token
- `POST /api/users/token/verify/` - Verify JWT token

### User Profile
- `GET /api/users/profile/` - Get user profile
- `PUT /api/users/profile/` - Update user profile
- `POST /api/users/profile/change-password/` - Change password
- `GET /api/users/profile/sessions/` - Get user sessions
- `DELETE /api/users/profile/sessions/` - Revoke session

### User Information
- `GET /api/users/me/` - Get current user information

## Usage Examples

### User Registration
```bash
curl -X POST http://localhost:8000/api/users/register/ \
  -H "Content-Type: application/json" \
  -d '{
    "username": "john_doe",
    "email": "john@example.com",
    "password": "securepass123",
    "password_confirm": "securepass123",
    "first_name": "John",
    "last_name": "Doe"
  }'
```

### User Login
```bash
curl -X POST http://localhost:8000/api/users/login/ \
  -H "Content-Type: application/json" \
  -d '{
    "email": "john@example.com",
    "password": "securepass123"
  }'
```

### Using JWT Token
```bash
curl -X GET http://localhost:8000/api/users/profile/ \
  -H "Authorization: Bearer <your_access_token>"
```

### Refresh Token
```bash
curl -X POST http://localhost:8000/api/users/token/refresh/ \
  -H "Content-Type: application/json" \
  -d '{
    "refresh_token": "<your_refresh_token>"
  }'
```

## Configuration

### JWT Settings (in settings.py)
```python
# JWT Settings
JWT_SECRET_KEY = 'your-secret-key'
JWT_ALGORITHM = 'HS256'
JWT_ACCESS_TOKEN_LIFETIME = timedelta(minutes=60)
JWT_REFRESH_TOKEN_LIFETIME = timedelta(days=7)
```

### Custom User Model
```python
AUTH_USER_MODEL = 'users.User'
```

## Security Features

- **Password Validation**: Django's built-in password validation
- **Token Expiry**: Configurable token lifetime
- **Session Tracking**: Monitor active sessions
- **Token Revocation**: Ability to revoke tokens/sessions
- **Device Tracking**: Track device information for security
- **IP Address Logging**: Log IP addresses for audit trails

## Database Migrations

After setting up the module, run migrations:

```bash
python manage.py makemigrations users
python manage.py migrate
```

## Testing

Run the test suite:

```bash
python manage.py test users
```

## Admin Interface

The module includes comprehensive admin interfaces for:
- User management
- User profiles
- User sessions
- Password reset requests

Access via Django admin at `/admin/`

## Dependencies

- Django 5.2+
- Django REST Framework 3.16+
- PyJWT 2.8.0+

## Notes

- The module is designed to work alongside the existing core analytics functionality
- JWT tokens are stored in the database for session management
- All endpoints return consistent JSON responses
- Error handling includes detailed validation messages
- The module follows Django best practices and conventions

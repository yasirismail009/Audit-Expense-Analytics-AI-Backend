from django.shortcuts import render
from rest_framework import status, generics, permissions
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response
from rest_framework.views import APIView
from django.contrib.auth import authenticate
from django.utils import timezone
from .models import User, UserProfile, UserSession
from .serializers import (
    UserRegistrationSerializer, UserLoginSerializer, UserDetailSerializer,
    JWTTokenSerializer, RefreshTokenSerializer, PasswordChangeSerializer
)
from .jwt_utils import jwt_handler


class UserRegistrationView(APIView):
    """
    User registration endpoint
    """
    permission_classes = [permissions.AllowAny]
    
    def post(self, request):
        serializer = UserRegistrationSerializer(data=request.data)
        if serializer.is_valid():
            user = serializer.save()
            
            # Generate JWT tokens
            tokens = jwt_handler.generate_tokens(user, request)
            
            # Update last login
            user.update_last_login()
            
            response_data = {
                'message': 'User registered successfully',
                'user': UserDetailSerializer(user).data,
                'tokens': tokens
            }
            
            return Response(response_data, status=status.HTTP_201_CREATED)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class UserLoginView(APIView):
    """
    User login endpoint
    """
    permission_classes = [permissions.AllowAny]
    
    def post(self, request):
        serializer = UserLoginSerializer(data=request.data)
        if serializer.is_valid():
            user = serializer.validated_data['user']
            
            # Generate JWT tokens
            tokens = jwt_handler.generate_tokens(user, request)
            
            # Update last login
            user.update_last_login()
            
            response_data = {
                'message': 'Login successful',
                'user': UserDetailSerializer(user).data,
                'tokens': tokens
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class UserLogoutView(APIView):
    """
    User logout endpoint
    """
    permission_classes = [permissions.IsAuthenticated]
    
    def post(self, request):
        # Get session ID from request (you might want to pass this in the request)
        session_id = request.data.get('session_id')
        
        # Revoke tokens
        success = jwt_handler.revoke_tokens(request.user, session_id)
        
        if success:
            return Response({'message': 'Logout successful'}, status=status.HTTP_200_OK)
        else:
            return Response({'message': 'Logout failed'}, status=status.HTTP_400_BAD_REQUEST)


class TokenRefreshView(APIView):
    """
    Refresh JWT token endpoint
    """
    permission_classes = [permissions.AllowAny]
    
    def post(self, request):
        serializer = RefreshTokenSerializer(data=request.data)
        if serializer.is_valid():
            refresh_token = serializer.validated_data['refresh_token']
            
            # Generate new tokens
            tokens, error = jwt_handler.refresh_tokens(refresh_token, request)
            
            if error:
                return Response({'error': error}, status=status.HTTP_400_BAD_REQUEST)
            
            return Response({
                'message': 'Token refreshed successfully',
                'tokens': tokens
            }, status=status.HTTP_200_OK)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class UserProfileView(APIView):
    """
    User profile management endpoint
    """
    permission_classes = [permissions.IsAuthenticated]
    
    def get(self, request):
        """Get user profile"""
        serializer = UserDetailSerializer(request.user)
        return Response(serializer.data, status=status.HTTP_200_OK)
    
    def put(self, request):
        """Update user profile"""
        serializer = UserDetailSerializer(request.user, data=request.data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return Response({
                'message': 'Profile updated successfully',
                'user': serializer.data
            }, status=status.HTTP_200_OK)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class PasswordChangeView(APIView):
    """
    Password change endpoint
    """
    permission_classes = [permissions.IsAuthenticated]
    
    def post(self, request):
        serializer = PasswordChangeSerializer(data=request.data, context={'request': request})
        if serializer.is_valid():
            user = request.user
            new_password = serializer.validated_data['new_password']
            
            # Change password
            user.set_password(new_password)
            user.save()
            
            # Revoke all sessions to force re-login
            jwt_handler.revoke_tokens(user)
            
            return Response({'message': 'Password changed successfully'}, status=status.HTTP_200_OK)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class UserSessionsView(APIView):
    """
    User sessions management endpoint
    """
    permission_classes = [permissions.IsAuthenticated]
    
    def get(self, request):
        """Get user's active sessions"""
        sessions = UserSession.objects.filter(
            user=request.user,
            is_active=True,
            is_revoked=False
        ).order_by('-created_at')
        
        session_data = []
        for session in sessions:
            session_data.append({
                'id': str(session.id),
                'device_info': session.device_info,
                'ip_address': session.ip_address,
                'created_at': session.created_at,
                'last_used_at': session.last_used_at,
                'access_token_expires': session.access_token_expires,
                'is_current': session.id == getattr(request, 'session_id', None)
            })
        
        return Response({
            'sessions': session_data,
            'total_sessions': len(session_data)
        }, status=status.HTTP_200_OK)
    
    def delete(self, request):
        """Revoke a specific session"""
        session_id = request.data.get('session_id')
        
        if not session_id:
            return Response({'error': 'Session ID is required'}, status=status.HTTP_400_BAD_REQUEST)
        
        success = jwt_handler.revoke_tokens(request.user, session_id)
        
        if success:
            return Response({'message': 'Session revoked successfully'}, status=status.HTTP_200_OK)
        else:
            return Response({'error': 'Session not found'}, status=status.HTTP_404_NOT_FOUND)


@api_view(['GET'])
@permission_classes([permissions.IsAuthenticated])
def user_info(request):
    """
    Get current user information
    """
    serializer = UserDetailSerializer(request.user)
    return Response(serializer.data, status=status.HTTP_200_OK)


@api_view(['POST'])
@permission_classes([permissions.AllowAny])
def verify_token(request):
    """
    Verify JWT token endpoint
    """
    token = request.data.get('token')
    
    if not token:
        return Response({'error': 'Token is required'}, status=status.HTTP_400_BAD_REQUEST)
    
    user, error = jwt_handler.validate_access_token(token)
    
    if error:
        return Response({'error': error}, status=status.HTTP_401_UNAUTHORIZED)
    
    serializer = UserDetailSerializer(user)
    return Response({
        'valid': True,
        'user': serializer.data
    }, status=status.HTTP_200_OK)

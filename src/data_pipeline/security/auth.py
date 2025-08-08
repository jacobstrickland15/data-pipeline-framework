"""
Enterprise Security and Authentication Module

Provides comprehensive security features including:
- JWT-based authentication and authorization
- Role-based access control (RBAC)
- API key management
- Security auditing and logging
- Rate limiting and DDoS protection
- Data encryption and PII handling
"""

import os
import jwt
import hashlib
import secrets
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
import redis
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64

logger = logging.getLogger(__name__)


class Permission(Enum):
    """System permissions."""
    # Data operations
    READ_DATA = "data:read"
    WRITE_DATA = "data:write"
    DELETE_DATA = "data:delete"
    EXPORT_DATA = "data:export"
    
    # Pipeline operations
    RUN_PIPELINE = "pipeline:run"
    MANAGE_PIPELINE = "pipeline:manage"
    VIEW_PIPELINE = "pipeline:view"
    
    # Queue operations
    MANAGE_QUEUE = "queue:manage"
    VIEW_QUEUE = "queue:view"
    
    # System administration
    ADMIN_USERS = "admin:users"
    ADMIN_SYSTEM = "admin:system"
    ADMIN_SECURITY = "admin:security"
    
    # Monitoring and metrics
    VIEW_METRICS = "metrics:view"
    EXPORT_METRICS = "metrics:export"


class Role(Enum):
    """Predefined system roles."""
    GUEST = "guest"
    ANALYST = "analyst"
    DATA_ENGINEER = "data_engineer"
    ADMIN = "admin"
    SUPER_ADMIN = "super_admin"


# Role-permission mappings
ROLE_PERMISSIONS = {
    Role.GUEST: {
        Permission.VIEW_PIPELINE,
        Permission.VIEW_QUEUE,
        Permission.VIEW_METRICS
    },
    Role.ANALYST: {
        Permission.READ_DATA,
        Permission.EXPORT_DATA,
        Permission.VIEW_PIPELINE,
        Permission.VIEW_QUEUE,
        Permission.VIEW_METRICS,
        Permission.EXPORT_METRICS
    },
    Role.DATA_ENGINEER: {
        Permission.READ_DATA,
        Permission.WRITE_DATA,
        Permission.EXPORT_DATA,
        Permission.RUN_PIPELINE,
        Permission.MANAGE_PIPELINE,
        Permission.VIEW_PIPELINE,
        Permission.MANAGE_QUEUE,
        Permission.VIEW_QUEUE,
        Permission.VIEW_METRICS,
        Permission.EXPORT_METRICS
    },
    Role.ADMIN: {
        Permission.READ_DATA,
        Permission.WRITE_DATA,
        Permission.DELETE_DATA,
        Permission.EXPORT_DATA,
        Permission.RUN_PIPELINE,
        Permission.MANAGE_PIPELINE,
        Permission.VIEW_PIPELINE,
        Permission.MANAGE_QUEUE,
        Permission.VIEW_QUEUE,
        Permission.ADMIN_USERS,
        Permission.VIEW_METRICS,
        Permission.EXPORT_METRICS
    },
    Role.SUPER_ADMIN: set(Permission)  # All permissions
}


@dataclass
class User:
    """User account information."""
    user_id: str
    username: str
    email: str
    roles: Set[Role] = field(default_factory=set)
    permissions: Set[Permission] = field(default_factory=set)
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_login: Optional[datetime] = None
    is_active: bool = True
    is_service_account: bool = False
    password_hash: Optional[str] = None
    api_keys: List[str] = field(default_factory=list)
    
    def has_permission(self, permission: Permission) -> bool:
        """Check if user has a specific permission."""
        # Check direct permissions
        if permission in self.permissions:
            return True
        
        # Check role-based permissions
        for role in self.roles:
            if permission in ROLE_PERMISSIONS.get(role, set()):
                return True
        
        return False
    
    def has_any_permission(self, permissions: List[Permission]) -> bool:
        """Check if user has any of the specified permissions."""
        return any(self.has_permission(perm) for perm in permissions)
    
    def has_all_permissions(self, permissions: List[Permission]) -> bool:
        """Check if user has all specified permissions."""
        return all(self.has_permission(perm) for perm in permissions)


@dataclass
class SecurityEvent:
    """Security audit event."""
    event_id: str
    timestamp: datetime
    event_type: str
    user_id: Optional[str]
    ip_address: Optional[str]
    user_agent: Optional[str]
    resource: Optional[str]
    action: str
    success: bool
    details: Dict[str, Any] = field(default_factory=dict)
    risk_score: int = 0  # 0-100, higher is more risky


class SecurityManager:
    """Central security management system."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.jwt_secret = os.getenv('JWT_SECRET_KEY', self._generate_secret())
        self.jwt_algorithm = 'HS256'
        self.jwt_expiry_hours = int(os.getenv('JWT_EXPIRY_HOURS', '24'))
        
        # Initialize Redis for session management and rate limiting
        self.redis_client = None
        try:
            import redis
            self.redis_client = redis.from_url(redis_url)
            self.redis_client.ping()
            logger.info("Connected to Redis for security operations")
        except Exception as e:
            logger.warning(f"Redis not available, using in-memory storage: {e}")
            self._in_memory_storage = {}
        
        # Initialize encryption
        self.encryption_key = self._get_or_create_encryption_key()
        self.fernet = Fernet(self.encryption_key)
        
        # User storage (in production, this would be a database)
        self.users: Dict[str, User] = {}
        self.api_keys: Dict[str, str] = {}  # api_key -> user_id
        
        # Security events storage
        self.security_events: List[SecurityEvent] = []
        
        # Rate limiting configuration
        self.rate_limits = {
            'login': {'requests': 5, 'window': 300},  # 5 attempts per 5 minutes
            'api': {'requests': 1000, 'window': 3600},  # 1000 requests per hour
            'export': {'requests': 10, 'window': 3600}  # 10 exports per hour
        }
        
        # Initialize default admin user
        self._create_default_admin()
    
    def _generate_secret(self) -> str:
        """Generate a secure random secret key."""
        return base64.b64encode(secrets.token_bytes(32)).decode('utf-8')
    
    def _get_or_create_encryption_key(self) -> bytes:
        """Get or create encryption key for data protection."""
        key_file = Path('config/.encryption_key')
        
        if key_file.exists():
            with open(key_file, 'rb') as f:
                return f.read()
        else:
            # Generate new key
            key = Fernet.generate_key()
            
            # Save securely (in production, use key management service)
            key_file.parent.mkdir(parents=True, exist_ok=True)
            with open(key_file, 'wb') as f:
                f.write(key)
            
            # Set restrictive permissions
            os.chmod(key_file, 0o600)
            
            logger.info("Generated new encryption key")
            return key
    
    def _create_default_admin(self) -> None:
        """Create default admin user for initial setup."""
        admin_username = os.getenv('ADMIN_USERNAME', 'admin')
        admin_password = os.getenv('ADMIN_PASSWORD')
        
        if not admin_password:
            admin_password = secrets.token_urlsafe(16)
            logger.warning(f"Generated admin password: {admin_password}")
        
        admin_user = User(
            user_id='admin-001',
            username=admin_username,
            email=os.getenv('ADMIN_EMAIL', 'admin@data-pipeline.local'),
            roles={Role.SUPER_ADMIN},
            password_hash=self._hash_password(admin_password)
        )
        
        self.users[admin_user.user_id] = admin_user
        logger.info(f"Created default admin user: {admin_username}")
    
    def _hash_password(self, password: str) -> str:
        """Hash password using PBKDF2 with salt."""
        salt = os.urandom(32)
        pwdhash = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
        return salt.hex() + pwdhash.hex()
    
    def _verify_password(self, password: str, password_hash: str) -> bool:
        """Verify password against hash."""
        if len(password_hash) != 128:  # 32 bytes salt + 32 bytes hash, in hex
            return False
        
        salt = bytes.fromhex(password_hash[:64])
        stored_hash = password_hash[64:]
        
        pwdhash = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
        return pwdhash.hex() == stored_hash
    
    def create_user(self, username: str, email: str, password: str, 
                   roles: Optional[List[Role]] = None) -> User:
        """Create a new user account."""
        user_id = f"user-{secrets.token_hex(8)}"
        
        user = User(
            user_id=user_id,
            username=username,
            email=email,
            roles=set(roles or [Role.GUEST]),
            password_hash=self._hash_password(password)
        )
        
        self.users[user_id] = user
        
        # Log security event
        self._log_security_event(
            event_type='user_created',
            user_id=user_id,
            action='create_user',
            success=True,
            details={'username': username, 'email': email, 'roles': [r.value for r in user.roles]}
        )
        
        logger.info(f"Created user account: {username} ({user_id})")
        return user
    
    def authenticate_user(self, username: str, password: str, ip_address: Optional[str] = None) -> Optional[str]:
        """Authenticate user and return JWT token."""
        # Rate limiting check
        if not self._check_rate_limit('login', username, ip_address):
            self._log_security_event(
                event_type='rate_limit_exceeded',
                action='login_attempt',
                success=False,
                ip_address=ip_address,
                details={'username': username, 'limit_type': 'login'},
                risk_score=60
            )
            return None
        
        # Find user
        user = None
        for u in self.users.values():
            if u.username == username and u.is_active:
                user = u
                break
        
        if not user or not self._verify_password(password, user.password_hash):
            self._log_security_event(
                event_type='login_failed',
                user_id=user.user_id if user else None,
                action='authenticate',
                success=False,
                ip_address=ip_address,
                details={'username': username, 'reason': 'invalid_credentials'},
                risk_score=40
            )
            return None
        
        # Update last login
        user.last_login = datetime.utcnow()
        
        # Generate JWT token
        payload = {
            'user_id': user.user_id,
            'username': user.username,
            'roles': [r.value for r in user.roles],
            'exp': datetime.utcnow() + timedelta(hours=self.jwt_expiry_hours),
            'iat': datetime.utcnow(),
            'iss': 'data-pipeline-framework'
        }
        
        token = jwt.encode(payload, self.jwt_secret, algorithm=self.jwt_algorithm)
        
        # Store session in Redis
        if self.redis_client:
            session_key = f"session:{user.user_id}"
            session_data = {
                'token': token,
                'ip_address': ip_address,
                'created_at': datetime.utcnow().isoformat(),
                'last_activity': datetime.utcnow().isoformat()
            }
            self.redis_client.setex(
                session_key, 
                timedelta(hours=self.jwt_expiry_hours),
                str(session_data)
            )
        
        self._log_security_event(
            event_type='login_success',
            user_id=user.user_id,
            action='authenticate',
            success=True,
            ip_address=ip_address,
            details={'username': username}
        )
        
        logger.info(f"User authenticated successfully: {username}")
        return token
    
    def verify_token(self, token: str) -> Optional[User]:
        """Verify JWT token and return user."""
        try:
            payload = jwt.decode(token, self.jwt_secret, algorithms=[self.jwt_algorithm])
            user_id = payload.get('user_id')
            
            if not user_id or user_id not in self.users:
                return None
            
            user = self.users[user_id]
            
            # Check if user is still active
            if not user.is_active:
                return None
            
            # Update last activity in Redis
            if self.redis_client:
                session_key = f"session:{user_id}"
                if self.redis_client.exists(session_key):
                    # Update last activity
                    self.redis_client.hset(session_key, 'last_activity', datetime.utcnow().isoformat())
                else:
                    # Session not found in Redis, token might be invalid
                    return None
            
            return user
            
        except jwt.ExpiredSignatureError:
            logger.warning("Token expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid token: {e}")
            return None
    
    def create_api_key(self, user_id: str, name: str) -> str:
        """Create API key for a user."""
        if user_id not in self.users:
            raise ValueError("User not found")
        
        api_key = f"dp_{secrets.token_urlsafe(32)}"
        self.api_keys[api_key] = user_id
        
        # Add to user's API keys
        self.users[user_id].api_keys.append(api_key)
        
        # Store in Redis with metadata
        if self.redis_client:
            api_key_data = {
                'user_id': user_id,
                'name': name,
                'created_at': datetime.utcnow().isoformat(),
                'last_used': None
            }
            self.redis_client.hset(f"api_key:{api_key}", mapping=api_key_data)
        
        self._log_security_event(
            event_type='api_key_created',
            user_id=user_id,
            action='create_api_key',
            success=True,
            details={'key_name': name}
        )
        
        logger.info(f"Created API key for user {user_id}: {name}")
        return api_key
    
    def verify_api_key(self, api_key: str, ip_address: Optional[str] = None) -> Optional[User]:
        """Verify API key and return associated user."""
        # Rate limiting
        if not self._check_rate_limit('api', api_key, ip_address):
            return None
        
        user_id = self.api_keys.get(api_key)
        if not user_id or user_id not in self.users:
            self._log_security_event(
                event_type='api_key_invalid',
                action='verify_api_key',
                success=False,
                ip_address=ip_address,
                details={'api_key_prefix': api_key[:10] + '...'},
                risk_score=30
            )
            return None
        
        user = self.users[user_id]
        if not user.is_active:
            return None
        
        # Update last used timestamp
        if self.redis_client:
            self.redis_client.hset(f"api_key:{api_key}", 'last_used', datetime.utcnow().isoformat())
        
        return user
    
    def _check_rate_limit(self, limit_type: str, identifier: str, ip_address: Optional[str] = None) -> bool:
        """Check rate limiting for various operations."""
        if limit_type not in self.rate_limits:
            return True
        
        config = self.rate_limits[limit_type]
        window = config['window']
        max_requests = config['requests']
        
        # Use identifier or IP address as key
        key = f"rate_limit:{limit_type}:{identifier or ip_address}"
        
        if self.redis_client:
            try:
                current = self.redis_client.get(key)
                if current is None:
                    # First request in window
                    self.redis_client.setex(key, window, 1)
                    return True
                
                current_count = int(current)
                if current_count >= max_requests:
                    return False
                
                # Increment counter
                self.redis_client.incr(key)
                return True
                
            except Exception as e:
                logger.warning(f"Rate limiting error: {e}")
                return True  # Allow on error
        else:
            # In-memory rate limiting (less accurate but functional)
            current_time = time.time()
            if key not in self._in_memory_storage:
                self._in_memory_storage[key] = {'count': 1, 'window_start': current_time}
                return True
            
            data = self._in_memory_storage[key]
            if current_time - data['window_start'] > window:
                # Reset window
                data['count'] = 1
                data['window_start'] = current_time
                return True
            
            if data['count'] >= max_requests:
                return False
            
            data['count'] += 1
            return True
    
    def _log_security_event(self, event_type: str, action: str, success: bool,
                          user_id: Optional[str] = None, ip_address: Optional[str] = None,
                          user_agent: Optional[str] = None, resource: Optional[str] = None,
                          details: Optional[Dict[str, Any]] = None, risk_score: int = 0) -> None:
        """Log security event for auditing."""
        event = SecurityEvent(
            event_id=secrets.token_hex(8),
            timestamp=datetime.utcnow(),
            event_type=event_type,
            user_id=user_id,
            ip_address=ip_address,
            user_agent=user_agent,
            resource=resource,
            action=action,
            success=success,
            details=details or {},
            risk_score=risk_score
        )
        
        self.security_events.append(event)
        
        # Log high-risk events
        if risk_score > 50:
            logger.warning(f"High-risk security event: {event_type} - {details}")
        
        # Keep only last 10000 events in memory
        if len(self.security_events) > 10000:
            self.security_events = self.security_events[-10000:]
    
    def encrypt_data(self, data: str) -> str:
        """Encrypt sensitive data."""
        return self.fernet.encrypt(data.encode()).decode()
    
    def decrypt_data(self, encrypted_data: str) -> str:
        """Decrypt sensitive data."""
        return self.fernet.decrypt(encrypted_data.encode()).decode()
    
    def get_security_events(self, limit: int = 100, event_type: Optional[str] = None,
                          user_id: Optional[str] = None) -> List[SecurityEvent]:
        """Get security events for auditing."""
        events = self.security_events
        
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        
        if user_id:
            events = [e for e in events if e.user_id == user_id]
        
        return events[-limit:]


def require_permission(permission: Permission):
    """Decorator to require specific permission for a function."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get current user from context (implementation depends on framework)
            # This is a simplified example
            current_user = kwargs.get('current_user')
            
            if not current_user or not current_user.has_permission(permission):
                raise PermissionError(f"Permission required: {permission.value}")
            
            return func(*args, **kwargs)
        return wrapper
    return decorator


def require_role(role: Role):
    """Decorator to require specific role for a function."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_user = kwargs.get('current_user')
            
            if not current_user or role not in current_user.roles:
                raise PermissionError(f"Role required: {role.value}")
            
            return func(*args, **kwargs)
        return wrapper
    return decorator


# Global security manager instance
_security_manager = None

def get_security_manager() -> SecurityManager:
    """Get the global security manager instance."""
    global _security_manager
    if _security_manager is None:
        _security_manager = SecurityManager()
    return _security_manager
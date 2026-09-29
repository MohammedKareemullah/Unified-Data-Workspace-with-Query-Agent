# models.py
"""
Data models for session management, memory, and context.
"""

from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
import json
import uuid

# ============================================================================
# ENUMS
# ============================================================================

class SessionStatus(str, Enum):
    """Session lifecycle states"""
    ACTIVE = "active"
    PAUSED = "paused"
    CLOSED = "closed"
    EXPIRED = "expired"


class MessageRole(str, Enum):
    """Message sender role"""
    USER = "user"
    AGENT = "assistant"
    SYSTEM = "system"


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class Message:
    """Single message in conversation"""
    role: MessageRole
    content: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    tool_calls: Optional[List[Dict[str, Any]]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {
            'role': self.role.value,
            'content': self.content,
            'timestamp': self.timestamp.isoformat(),
            'tool_calls': self.tool_calls,
            'metadata': self.metadata
        }


@dataclass
class ContextState:
    """Current session context state"""
    current_analysis_type: Optional[str] = None
    last_table_queried: Optional[str] = None
    active_filters: Dict[str, Any] = field(default_factory=dict)
    vendor_focus: Optional[str] = None  # Currently analyzed vendor
    query_history: List[str] = field(default_factory=list)
    derived_insights: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class UserMemory:
    """Long-term memory about user preferences"""
    user_id: str
    preferred_analysis_types: List[str] = field(default_factory=list)
    frequent_filters: Dict[str, Any] = field(default_factory=dict)
    saved_queries: Dict[str, str] = field(default_factory=dict)  # query_name -> SQL
    vendor_preferences: Dict[str, str] = field(default_factory=dict)  # vendor_id -> notes
    last_accessed: datetime = field(default_factory=datetime.utcnow)
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> dict:
        return {
            'user_id': self.user_id,
            'preferred_analysis_types': self.preferred_analysis_types,
            'frequent_filters': self.frequent_filters,
            'saved_queries': self.saved_queries,
            'vendor_preferences': self.vendor_preferences,
            'last_accessed': self.last_accessed.isoformat(),
            'created_at': self.created_at.isoformat()
        }


@dataclass
class Session:
    """Complete session object"""
    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str = ""
    status: SessionStatus = SessionStatus.ACTIVE
    
    # Conversation data
    messages: List[Message] = field(default_factory=list)
    context: ContextState = field(default_factory=ContextState)
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_activity: datetime = field(default_factory=datetime.utcnow)
    turn_count: int = 0
    total_tokens_used: int = 0
    
    # Lifecycle
    ttl: int = 86400  # 24 hours in seconds
    
    def to_dict(self) -> dict:
        return {
            'session_id': self.session_id,
            'user_id': self.user_id,
            'status': self.status.value,
            'messages': [m.to_dict() for m in self.messages],
            'context': self.context.to_dict(),
            'created_at': self.created_at.isoformat(),
            'last_activity': self.last_activity.isoformat(),
            'turn_count': self.turn_count,
            'total_tokens_used': self.total_tokens_used
        }


# ============================================================================
# REQUEST/RESPONSE MODELS (For API)
# ============================================================================

@dataclass
class UserQuery:
    """API request model"""
    user_id: str
    question: str
    session_id: Optional[str] = None  # None = create new session
    context_hints: Optional[Dict[str, Any]] = None


@dataclass
class AgentResponse:
    """API response model"""
    session_id: str
    user_id: str
    response: str
    turn_count: int
    messages_in_context: int
    execution_time_ms: float
    tool_calls: Optional[List[Dict[str, Any]]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return asdict(self)
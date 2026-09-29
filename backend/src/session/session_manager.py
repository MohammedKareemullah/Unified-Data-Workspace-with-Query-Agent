# session_manager.py
"""
Session management with DynamoDB persistence.
Handles session CRUD, conversation history, context state.
"""

import boto3
import json
from typing import Optional, List
from datetime import datetime, timedelta
from models.models import Session, Message, ContextState, UserMemory, MessageRole, SessionStatus
from decimal import Decimal  # Add this import
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# HELPER: Convert Decimal to native types
# ============================================================================

def _convert_decimals(obj):
    """
    Recursively convert Decimal objects from DynamoDB to int/float.
    DynamoDB returns all numbers as Decimal for precision.
    """
    if isinstance(obj, Decimal):
        # Convert to int if it's a whole number, else float
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    elif isinstance(obj, dict):
        return {k: _convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_decimals(v) for v in obj]
    return obj


class SessionManager:
    """Manage session lifecycle and persistence"""
    
    def __init__(self, region: str = 'ap-south-1'):
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.sessions_table = self.dynamodb.Table('procurement-agent-sessions')
        self.user_memory_table = self.dynamodb.Table('procurement-agent-memory')
        self.region = region
    
    # ========================================================================
    # SESSION CRUD
    # ========================================================================
    
    def create_session(self, user_id: str) -> Session:
        """Create new session for user"""
        session = Session(user_id=user_id)
        self._save_session(session)
        logger.info(f"Created session {session.session_id} for user {user_id}")
        return session
    
    def get_session(self, session_id: str) -> Optional[Session]:
        """Retrieve session from DynamoDB"""
        try:
            response = self.sessions_table.get_item(Key={'session_id': session_id})
            
            if 'Item' not in response:
                logger.warning(f"Session {session_id} not found")
                return None
            
            item = response['Item']
            
            # Convert all Decimals to int/float
            item = _convert_decimals(item)
            
            # Reconstruct Session object
            session = Session(
                session_id=item['session_id'],
                user_id=item['user_id'],
                status=SessionStatus(item['status']),
                turn_count=item.get('turn_count', 0),
                total_tokens_used=item.get('total_tokens_used', 0),
                ttl=item.get('ttl', 86400)  # Now safe - already converted to int
            )
            
            # Reconstruct messages
            session.messages = [
                Message(
                    role=MessageRole(msg['role']),
                    content=msg['content'],
                    timestamp=datetime.fromisoformat(msg['timestamp']),
                    tool_calls=msg.get('tool_calls'),
                    metadata=msg.get('metadata', {})
                )
                for msg in item.get('messages', [])
            ]
            
            # Reconstruct context
            ctx_data = item.get('context', {})
            session.context = ContextState(
                current_analysis_type=ctx_data.get('current_analysis_type'),
                last_table_queried=ctx_data.get('last_table_queried'),
                active_filters=ctx_data.get('active_filters', {}),
                vendor_focus=ctx_data.get('vendor_focus'),
                query_history=ctx_data.get('query_history', []),
                derived_insights=ctx_data.get('derived_insights', {})
            )
            
            return session
        
        except Exception as e:
            logger.error(f"Error retrieving session {session_id}: {str(e)}", exc_info=True)
            return None
    
    def save_session(self, session: Session) -> bool:
        """Save session to DynamoDB"""
        return self._save_session(session)
    
    def _save_session(self, session: Session) -> bool:
        """Internal save method"""
        try:
            # Update last activity
            session.last_activity = datetime.utcnow()
            
            # Calculate TTL (expiry timestamp)
            ttl_seconds = session.ttl if isinstance(session.ttl, int) else 86400
            expiry_timestamp = int((datetime.utcnow() + timedelta(seconds=ttl_seconds)).timestamp())
            
            # Prepare item for DynamoDB
            item = {
                'session_id': session.session_id,
                'user_id': session.user_id,
                'status': session.status.value,
                'messages': [m.to_dict() for m in session.messages],
                'context': session.context.to_dict(),
                'created_at': session.created_at.isoformat(),
                'last_activity': session.last_activity.isoformat(),
                'turn_count': session.turn_count,
                'total_tokens_used': session.total_tokens_used,
                'ttl': expiry_timestamp  # DynamoDB TTL requires integer timestamp
            }
            
            self.sessions_table.put_item(Item=item)
            logger.info(f"Saved session {session.session_id}")
            return True
        
        except Exception as e:
            logger.error(f"Error saving session: {str(e)}", exc_info=True)
            return False
    
    def close_session(self, session_id: str) -> bool:
        """Mark session as closed"""
        try:
            session = self.get_session(session_id)
            if session:
                session.status = SessionStatus.CLOSED
                self.save_session(session)
                logger.info(f"Closed session {session_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error closing session: {str(e)}", exc_info=True)
            return False
    
    def list_user_sessions(self, user_id: str, limit: int = 10) -> List[Session]:
        """Get all sessions for a user"""
        try:
            response = self.sessions_table.query(
                IndexName='user_id-last_activity-index',
                KeyConditionExpression='user_id = :uid',
                ExpressionAttributeValues={':uid': user_id},
                ScanIndexForward=False,  # Most recent first
                Limit=limit
            )
            
            sessions = []
            for item in response.get('Items', []):
                # Convert Decimals
                item = _convert_decimals(item)
                
                # Reconstruct session (simplified)
                session = Session(
                    session_id=item['session_id'],
                    user_id=item['user_id'],
                    status=SessionStatus(item['status']),
                    turn_count=item.get('turn_count', 0)
                )
                sessions.append(session)
            
            return sessions
        
        except Exception as e:
            logger.error(f"Error listing sessions for user {user_id}: {str(e)}", exc_info=True)
            return []
    
    # ========================================================================
    # MESSAGE MANAGEMENT
    # ========================================================================
    
    def add_message(self, session_id: str, message: Message) -> bool:
        """Add message to session"""
        try:
            session = self.get_session(session_id)
            if not session:
                logger.error(f"Session {session_id} not found")
                return False
            
            session.messages.append(message)
            session.turn_count += 1
            
            self.save_session(session)
            return True
        
        except Exception as e:
            logger.error(f"Error adding message to session {session_id}: {str(e)}", exc_info=True)
            return False
    
    def get_conversation_history(self, session_id: str, limit: int = 50) -> List[Message]:
        """Get recent conversation history"""
        session = self.get_session(session_id)
        if not session:
            return []
        
        # Return last N messages
        return session.messages[-limit:]
    
    def get_conversation_summary(self, session_id: str) -> Optional[str]:
        """Get summary of conversation (for long sessions)"""
        session = self.get_session(session_id)
        if not session or not session.messages:
            return None
        
        # For now, just return first+last messages
        summary = f"Session started at {session.created_at}\n"
        summary += f"Total turns: {session.turn_count}\n"
        
        if session.messages:
            summary += f"First question: {session.messages[0].content[:100]}...\n"
            summary += f"Last response: {session.messages[-1].content[:100]}...\n"
        
        return summary
    
    # ========================================================================
    # CONTEXT MANAGEMENT
    # ========================================================================
    
    def update_context(self, session_id: str, context: ContextState) -> bool:
        """Update session context"""
        try:
            session = self.get_session(session_id)
            if not session:
                return False
            
            session.context = context
            self.save_session(session)
            return True
        
        except Exception as e:
            logger.error(f"Error updating context: {str(e)}", exc_info=True)
            return False
    
    def get_context(self, session_id: str) -> Optional[ContextState]:
        """Get current context state"""
        session = self.get_session(session_id)
        return session.context if session else None
    
    # ========================================================================
    # USER MEMORY (Long-term)
    # ========================================================================
    
    def get_user_memory(self, user_id: str) -> Optional[UserMemory]:
        """Retrieve user's long-term memory"""
        try:
            response = self.user_memory_table.get_item(Key={'user_id': user_id})
            
            if 'Item' not in response:
                # Create new memory for user
                memory = UserMemory(user_id=user_id)
                self._save_user_memory(memory)
                return memory
            
            item = response['Item']
            # Convert Decimals
            item = _convert_decimals(item)
            
            memory = UserMemory(
                user_id=item['user_id'],
                preferred_analysis_types=item.get('preferred_analysis_types', []),
                frequent_filters=item.get('frequent_filters', {}),
                saved_queries=item.get('saved_queries', {}),
                vendor_preferences=item.get('vendor_preferences', {}),
                last_accessed=datetime.fromisoformat(item['last_accessed']),
                created_at=datetime.fromisoformat(item['created_at'])
            )
            
            return memory
        
        except Exception as e:
            logger.error(f"Error retrieving user memory: {str(e)}", exc_info=True)
            return None
    
    def save_user_memory(self, memory: UserMemory) -> bool:
        """Save user long-term memory"""
        return self._save_user_memory(memory)
    
    def _save_user_memory(self, memory: UserMemory) -> bool:
        """Internal save"""
        try:
            memory.last_accessed = datetime.utcnow()
            
            item = {
                'user_id': memory.user_id,
                'preferred_analysis_types': memory.preferred_analysis_types,
                'frequent_filters': memory.frequent_filters,
                'saved_queries': memory.saved_queries,
                'vendor_preferences': memory.vendor_preferences,
                'last_accessed': memory.last_accessed.isoformat(),
                'created_at': memory.created_at.isoformat()
            }
            
            self.user_memory_table.put_item(Item=item)
            logger.info(f"Saved user memory for {memory.user_id}")
            return True
        
        except Exception as e:
            logger.error(f"Error saving user memory: {str(e)}", exc_info=True)
            return False
    
    def update_user_preferences(self, user_id: str, analysis_type: str) -> bool:
        """Track user's analysis preferences"""
        try:
            memory = self.get_user_memory(user_id)
            if memory:
                if analysis_type not in memory.preferred_analysis_types:
                    memory.preferred_analysis_types.append(analysis_type)
                self.save_user_memory(memory)
                return True
            return False
        except Exception as e:
            logger.error(f"Error updating preferences: {str(e)}", exc_info=True)
            return False
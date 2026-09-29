# memory_manager.py
"""
Memory management - handles conversation history, insights extraction, and context enrichment.
"""

from typing import List, Optional, Dict, Any
from models.models import Message, ContextState, MessageRole
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)

class MemoryManager:
    """Manage agent memory and context enrichment"""
    
    def __init__(self, session_manager):
        self.session_manager = session_manager
    
    # ========================================================================
    # CONVERSATION MEMORY
    # ========================================================================
    
    def build_context_for_agent(self, session_id: str) -> dict:
        """
        Build rich context for agent from session history and user memory.
        This is passed to the Strands agent as system context.
        """
        
        session = self.session_manager.get_session(session_id)
        if not session:
            return {}
        
        user_memory = self.session_manager.get_user_memory(session.user_id)
        
        # Build conversation summary
        recent_messages = self._get_recent_messages(session.messages, limit=5)
        
        context = {
            "session_id": session_id,
            "turn_number": session.turn_count,
            "user_preferences": {
                "preferred_analysis_types": user_memory.preferred_analysis_types if user_memory else [],
                "frequent_filters": user_memory.frequent_filters if user_memory else {},
            },
            "session_context": {
                "current_analysis": session.context.current_analysis_type,
                "last_table": session.context.last_table_queried,
                "active_vendor": session.context.vendor_focus,
                "active_filters": session.context.active_filters
            },
            "recent_conversation": recent_messages,
            "query_history": session.context.query_history[-10:]  # Last 10 queries
        }
        
        return context
    
    def _get_recent_messages(self, messages: List[Message], limit: int = 5) -> List[dict]:
        """Extract recent messages for context"""
        recent = messages[-limit:] if len(messages) > limit else messages
        
        return [
            {
                "role": msg.role.value,
                "content": msg.content[:500],  # Truncate long messages
                "timestamp": msg.timestamp.isoformat()
            }
            for msg in recent
        ]
    
    # ========================================================================
    # EXTRACT INSIGHTS FROM AGENT RESPONSES
    # ========================================================================
    
    def extract_insights(
        self, 
        session_id: str, 
        agent_response: str,
        tool_calls: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        """
        Extract and store insights from agent response.
        Updates session context with derived knowledge.
        """
        
        session = self.session_manager.get_session(session_id)
        if not session:
            return
        
        # Extract table names from tool calls
        if tool_calls:
            for tool_call in tool_calls:
                tool_name = tool_call.get('name', '')
                if 'vendor_scorecard' in tool_name:
                    session.context.last_table_queried = 'vendor_scorecard'
                elif 'spend_analytics' in tool_name:
                    session.context.last_table_queried = 'spend_analytics'
                elif 'inventory_health' in tool_name:
                    session.context.last_table_queried = 'inventory_health'
        
        # Extract vendor references from response
        if 'vendor' in agent_response.lower():
            # Simple regex: look for patterns like "VND-XXX"
            import re
            vendor_ids = re.findall(r'VND-\d+', agent_response)
            if vendor_ids:
                session.context.vendor_focus = vendor_ids[0]
        
        # Extract analysis type
        if any(word in agent_response.lower() for word in ['consolidation', 'consolidate']):
            session.context.current_analysis_type = 'consolidation'
        elif any(word in agent_response.lower() for word in ['spend', 'spending']):
            session.context.current_analysis_type = 'spend_analysis'
        elif any(word in agent_response.lower() for word in ['inventory', 'reorder']):
            session.context.current_analysis_type = 'inventory'
        
        # Store key insights
        if 'recommendation' in agent_response.lower() or 'recommend' in agent_response.lower():
            if 'insights' not in session.context.derived_insights:
                session.context.derived_insights['insights'] = []
            
            session.context.derived_insights['insights'].append({
                'timestamp': datetime.utcnow().isoformat(),
                'snippet': agent_response[:200]
            })
        
        # Update session
        self.session_manager.save_session(session)
    
    # ========================================================================
    # BUILD SYSTEM PROMPT WITH MEMORY AWARENESS
    # ========================================================================
    
    def build_system_prompt_with_memory(self, session_id: str) -> str:
        """
        Build enhanced system prompt that includes user memory and session context.
        """
        
        context = self.build_context_for_agent(session_id)
        session = self.session_manager.get_session(session_id)
        
        base_prompt = """You are an expert procurement and vendor analytics assistant with deep knowledge of supply chain management.

**IMPORTANT: You work with dynamically structured data, so you MUST discover the schema before querying.**

Your workflow:
1. Use get_available_tables() to see what data you have
2. Call get_table_schema(table_name) for each table of interest
3. Use get_query_template() if you need inspiration
4. Construct SQL queries using ONLY columns that exist
5. Execute queries with execute_custom_query()
6. Provide data-driven insights"""
        
        # Add user preferences if known
        if context.get('user_preferences', {}).get('preferred_analysis_types'):
            prefs = context['user_preferences']['preferred_analysis_types']
            base_prompt += f"\n\n**User's Preferred Analysis Types:** {', '.join(prefs)}\nPrioritize these types of analysis when relevant."
        
        # Add session context
        if session and session.turn_count > 1:
            base_prompt += f"\n\n**This is turn {session.turn_count} of conversation.**"
            
            if context.get('session_context', {}).get('current_analysis'):
                base_prompt += f"\nCurrent analysis focus: {context['session_context']['current_analysis']}"
            
            if context.get('session_context', {}).get('active_vendor'):
                base_prompt += f"\nVendor of focus: {context['session_context']['active_vendor']}"
        
        # Add recent context
        if context.get('recent_conversation'):
            base_prompt += "\n\n**Recent conversation:**"
            for msg in context['recent_conversation'][-3:]:
                role = "User" if msg['role'] == 'user' else "Assistant"
                base_prompt += f"\n{role}: {msg['content'][:100]}..."
        
        return base_prompt
    
    # ========================================================================
    # CLEANUP & SUMMARIZATION
    # ========================================================================
    
    def summarize_long_session(self, session_id: str) -> Optional[str]:
        """
        For sessions with many messages, create a summary to keep context size manageable.
        In production, you'd use Claude to create this summary.
        """
        
        session = self.session_manager.get_session(session_id)
        if not session or len(session.messages) < 20:
            return None
        
        # Simple summarization logic
        summary = f"""
CONVERSATION SUMMARY
====================
Session ID: {session.session_id}
Duration: {(session.last_activity - session.created_at).total_seconds() / 60:.1f} minutes
Total Turns: {session.turn_count}

Topics Discussed:
- Current Analysis: {session.context.current_analysis_type or 'Not specified'}
- Vendor Focus: {session.context.vendor_focus or 'Multiple vendors'}
- Tables Queried: {session.context.last_table_queried or 'Various'}

Key Filters Applied:
{json.dumps(session.context.active_filters, indent=2) if session.context.active_filters else 'None'}

Previous Queries:
{chr(10).join(f"- {q}" for q in session.context.query_history[-5:])}
"""
        
        return summary
    
    def should_rotate_context(self, session_id: str) -> bool:
        """Determine if conversation history is too large"""
        session = self.session_manager.get_session(session_id)
        if not session:
            return False
        
        # If more than 50 messages, should rotate/summarize
        return len(session.messages) > 50
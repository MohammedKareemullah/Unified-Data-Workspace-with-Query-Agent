# agent.py (Updated with schema discovery)
"""
Schema-aware Vendor Procurement Agent
Uses dynamic schema discovery to build queries without hardcoding field names.
"""

from strands import Agent
from tools.schema_tools import (
    get_available_tables,
    get_table_schema,
    get_column_statistics,
    execute_custom_query,
    get_query_template
)
import time
import logging
import json
from typing import Optional
from session.session_manager import SessionManager
from memory.memory_manager import MemoryManager
# from prompts import system_prompt
from models.models import Message, MessageRole, UserQuery, AgentResponse, ContextState


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AgentMain:
    """Production-grade agent with full session, context, and memory support"""

    def __init__(self):
        # Initialize Strands agent
        self.agent = Agent(
            tools=[
                get_available_tables,
                get_table_schema,
                get_column_statistics,
                get_query_template,
                execute_custom_query,                
            ],
            model="openai.gpt-oss-120b-1:0",            
            # system_prompt=system_prompt
        )
        
        # Initialize managers
        self.session_manager = SessionManager()
        self.memory_manager = MemoryManager(self.session_manager)

    def query(self, user_query: UserQuery) -> AgentResponse:
        """
        Main entry point for agent queries.
        Handles session creation/retrieval, context management, and response formatting.
        """
        
        start_time = time.time()
        
        try:
            # Get or create session
            session_id = user_query.session_id
            if not session_id:
                session = self.session_manager.create_session(user_query.user_id)
                session_id = session.session_id
            else:
                session = self.session_manager.get_session(session_id)
                if not session:
                    logger.error(f"Session {session_id} not found")
                    raise ValueError(f"Session not found: {session_id}")
            
            logger.info(f"Processing query in session {session_id}, turn {session.turn_count + 1}")
            
            # Build enriched system prompt with memory
            system_prompt = self.memory_manager.build_system_prompt_with_memory(session_id)
            
            # Add user query to session
            user_message = Message(
                role=MessageRole.USER,
                content=user_query.question,
                metadata={"context_hints": user_query.context_hints or {}}
            )
            self.session_manager.add_message(session_id, user_message)
            
            # Invoke agent
            logger.info(f"Invoking agent with question: {user_query.question[:100]}...")
            agent_response = self.agent(
                user_query.question,
                system_prompt=system_prompt
            )
            
            # Log as agent message
            agent_message = Message(
                role=MessageRole.AGENT,
                content=str(agent_response)
            )
            self.session_manager.add_message(session_id, agent_message)
            
            # Extract insights and update context
            self.memory_manager.extract_insights(
                session_id,
                str(agent_response),
                tool_calls=None  # You'd extract this from agent if available
            )
            
            # Update user preferences
            if session.context.current_analysis_type:
                self.session_manager.update_user_preferences(
                    user_query.user_id,
                    session.context.current_analysis_type
                )
            
            # Calculate execution time
            execution_time = (time.time() - start_time) * 1000  # Convert to ms
            
            # Get fresh session state
            session = self.session_manager.get_session(session_id)
            
            # Build response
            return AgentResponse(
                session_id=session_id,
                user_id=user_query.user_id,
                response=str(agent_response),
                turn_count=session.turn_count,
                messages_in_context=len(session.messages),
                execution_time_ms=execution_time,
                metadata={
                    "current_analysis": session.context.current_analysis_type,
                    "vendor_focus": session.context.vendor_focus,
                    "tables_accessed": session.context.last_table_queried
                }
            )
        
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}", exc_info=True)
            raise

    def get_session_history(self, session_id: str, limit: int = 50) -> list:
        """Get conversation history for a session"""
        messages = self.session_manager.get_conversation_history(session_id, limit)
        return [m.to_dict() for m in messages]

    def get_session_info(self, session_id: str) -> Optional[dict]:
        """Get session metadata"""
        session = self.session_manager.get_session(session_id)
        if not session:
            return None
        
        return {
            "session_id": session.session_id,
            "user_id": session.user_id,
            "status": session.status.value,
            "created_at": session.created_at.isoformat(),
            "last_activity": session.last_activity.isoformat(),
            "turn_count": session.turn_count,
            "total_tokens": session.total_tokens_used,
            "context": session.context.to_dict()
        }


    def list_user_sessions(self, user_id: str) -> list:
        """List all sessions for a user"""
        sessions = self.session_manager.list_user_sessions(user_id)
        return [
            {
                "session_id": s.session_id,
                "status": s.status.value,
                "turn_count": s.turn_count,
                "created_at": s.created_at.isoformat()
            }
            for s in sessions
        ]

    def close_session(self, session_id: str) -> bool:
        """Close a session"""
        return self.session_manager.close_session(session_id)

    def get_user_memory(self, user_id: str) -> Optional[dict]:
        """Get user's long-term memory"""
        memory = self.session_manager.get_user_memory(user_id)
        return memory.to_dict() if memory else None


# # Initialize Agent with ONLY schema/query tools
# agent = Agent(
#     tools=[
#         get_available_tables,
#         get_table_schema,
#         get_column_statistics,
#         get_query_template,
#         execute_custom_query,
#     ],
#     # model_provider="NVIDIA",
#     model="openai.gpt-oss-120b-1:0",
#     # system_prompt=system_prompt
# )



# message = "Give me the list of average delivery time of the top 5 vendors by their score"

# if __name__ == "__main__":
#     res = agent(message)    
#     res.metrics.get_summary()
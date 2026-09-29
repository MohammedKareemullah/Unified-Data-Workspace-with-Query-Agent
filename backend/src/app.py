# app.py
"""
Production FastAPI application for the procurement agent.
Handles HTTP requests, session routing, authentication, and observability.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import logging
from agent.agent import AgentMain
from models.models import UserQuery, AgentResponse
import json
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(
    title="Procurement Vendor Agent",
    description="Production-grade AI agent for vendor analytics and procurement insights",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize agent
agent_system = AgentMain()

# ============================================================================
# PYDANTIC MODELS FOR API
# ============================================================================

class QueryRequest(BaseModel):
    """API request for agent query"""
    user_id: str
    question: str
    session_id: Optional[str] = None
    context_hints: Optional[dict] = None


class QueryResponseData(BaseModel):
    """API response for agent query"""
    session_id: str
    user_id: str
    response: str
    turn_count: int
    execution_time_ms: float
    metadata: dict


class SessionInfo(BaseModel):
    """Session information"""
    session_id: str
    user_id: str
    status: str
    created_at: str
    turn_count: int


class ConversationMessage(BaseModel):
    """Single message in conversation"""
    role: str
    content: str
    timestamp: str


# ============================================================================
# HEALTH & INFO ENDPOINTS
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "Procurement Vendor Agent"
    }


@app.get("/info")
async def agent_info():
    """Agent information"""
    return {
        "name": "Procurement Vendor Analytics Agent",
        "version": "1.0.0",
        "capabilities": [
            "vendor_performance_analysis",
            "spend_analytics",
            "inventory_health",
            "procurement_recommendations",
            "vendor_consolidation"
        ],
        "session_management": True,
        "memory_enabled": True
    }


# ============================================================================
# AGENT QUERY ENDPOINTS
# ============================================================================

@app.post("/agent/query", response_model=QueryResponseData)
async def query_agent(request: QueryRequest):
    """
    Query the procurement agent.
    
    - **user_id**: Unique user identifier
    - **question**: Natural language question
    - **session_id** (optional): Existing session ID (creates new if not provided)
    - **context_hints** (optional): Additional context for the query
    """
    
    try:
        # Create user query
        user_query = UserQuery(
            user_id=request.user_id,
            question=request.question,
            session_id=request.session_id,
            context_hints=request.context_hints
        )
        
        # Invoke agent
        logger.info(f"Received query from user {request.user_id}")
        response = agent_system.query(user_query)
        
        # Return response
        return QueryResponseData(
            session_id=response.session_id,
            user_id=response.user_id,
            response=response.response,
            turn_count=response.turn_count,
            execution_time_ms=response.execution_time_ms,
            metadata=response.metadata
        )
    
    except Exception as e:
        logger.error(f"Error processing query: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# SESSION MANAGEMENT ENDPOINTS
# ============================================================================

@app.get("/sessions/{session_id}")
async def get_session(session_id: str):
    """Get session information and metadata"""
    session_info = agent_system.get_session_info(session_id)
    if not session_info:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
    return session_info


@app.get("/sessions/{session_id}/history")
async def get_session_history(session_id: str, limit: int = 50):
    """Get conversation history for a session"""
    history = agent_system.get_session_history(session_id, limit)
    if not history:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
    
    return {
        "session_id": session_id,
        "message_count": len(history),
        "messages": [ConversationMessage(**m) if isinstance(m, dict) else m for m in history]
    }


@app.delete("/sessions/{session_id}")
async def close_session(session_id: str):
    """Close a session"""
    success = agent_system.close_session(session_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
    
    return {"status": "closed", "session_id": session_id}


@app.get("/users/{user_id}/sessions")
async def list_user_sessions(user_id: str):
    """List all sessions for a user"""
    sessions = agent_system.list_user_sessions(user_id)
    return {
        "user_id": user_id,
        "session_count": len(sessions),
        "sessions": sessions
    }


@app.get("/users/{user_id}/memory")
async def get_user_memory(user_id: str):
    """Get user's long-term memory and preferences"""
    memory = agent_system.get_user_memory(user_id)
    if not memory:
        return {
            "user_id": user_id,
            "memory": None,
            "message": "No memory found for user"
        }
    
    return {
        "user_id": user_id,
        "memory": memory
    }


# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.exception_handler(ValueError)
async def value_error_handler(request, exc):
    return {
        "error": "Invalid input",
        "details": str(exc)
    }


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return {
        "error": "Internal server error",
        "details": str(exc)
    }


# ============================================================================
# STARTUP/SHUTDOWN
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    logger.info("Procurement Agent API starting up...")
    logger.info("Agent system initialized")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Procurement Agent API shutting down...")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
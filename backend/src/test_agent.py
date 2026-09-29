# test_agent.py
"""
Test the production agent with multiple sessions and multi-turn conversations.
"""

from agent.agent import AgentMain
from models import UserQuery
import json

def test_multi_turn_conversation():
    """Test multi-turn conversation with context preservation"""
    
    agent = AgentMain()
    
    # User 1
    print("\n" + "="*80)
    print("TEST: Multi-turn Conversation - User 1")
    print("="*80)
    
    # Turn 1
    query1 = UserQuery(
        user_id="user_001",
        question="Which vendors should we consolidate spending with?"
    )
    
    response1 = agent.query(query1)
    print(f"\nTurn 1:")
    print(f"Session: {response1.session_id}")
    print(f"Response: {response1.response[:500]}...")
    print(f"Execution Time: {response1.execution_time_ms:.2f}ms")
    
    # Turn 2 - Same session
    query2 = UserQuery(
        user_id="user_001",
        question="Show me inventory health for materials with low stock",
        session_id=response1.session_id  # Reuse session
    )
    
    response2 = agent.query(query2)
    print(f"\nTurn 2 (same session):")
    print(f"Turn count: {response2.turn_count}")
    print(f"Response: {response2.response[:500]}...")
    
    # Get session history
    print(f"\nSession History:")
    history = agent.get_session_history(response1.session_id)
    for i, msg in enumerate(history):
        print(f"  Message {i+1}: {msg['role']} - {msg['content'][:50]}...")
    
    # Get user sessions
    print(f"\nUser Sessions:")
    sessions = agent.list_user_sessions("user_001")
    for session in sessions:
        print(f"  {session['session_id']}: {session['turn_count']} turns")
    
    # Get user memory
    print(f"\nUser Memory:")
    memory = agent.get_user_memory("user_001")
    if memory:
        print(f"  Preferred analysis types: {memory['preferred_analysis_types']}")
        print(f"  Vendor preferences: {memory['vendor_preferences']}")


if __name__ == "__main__":
    test_multi_turn_conversation()
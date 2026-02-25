"""
State management for GraphServe Runtime.

Provides persistent state storage for workflow execution.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field, asdict
from datetime import datetime
import json
import pickle
import asyncio
import logging
import os

logger = logging.getLogger(__name__)


@dataclass
class State:
    """Workflow execution state.
    
    Tracks the current state of a workflow execution including:
    - Input/output data
    - Current node position
    - Execution history
    - Metadata
    """
    # Unique identifiers
    execution_id: str
    graph_name: str
    
    # State data
    data: Dict[str, Any] = field(default_factory=dict)
    
    # Execution tracking
    current_node: Optional[str] = None
    visited_nodes: List[str] = field(default_factory=list)
    completed_nodes: List[str] = field(default_factory=list)
    
    # Status
    status: str = "pending"  # pending, running, paused, completed, failed
    error: Optional[str] = None
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    
    # Checkpoint data for recovery
    checkpoints: List[Dict[str, Any]] = field(default_factory=list)
    
    def update(self, data: Dict[str, Any]) -> "State":
        """Update state data."""
        self.data.update(data)
        self.updated_at = datetime.now()
        return self
    
    def set_current_node(self, node_name: str) -> "State":
        """Set the current node."""
        self.current_node = node_name
        self.visited_nodes.append(node_name)
        self.updated_at = datetime.now()
        return self
    
    def mark_completed(self, node_name: str) -> "State":
        """Mark a node as completed."""
        if node_name not in self.completed_nodes:
            self.completed_nodes.append(node_name)
        self.updated_at = datetime.now()
        return self
    
    def mark_failed(self, error: str) -> "State":
        """Mark execution as failed."""
        self.status = "failed"
        self.error = error
        self.updated_at = datetime.now()
        return self
    
    def mark_completed_success(self) -> "State":
        """Mark execution as successfully completed."""
        self.status = "completed"
        self.current_node = None
        self.completed_at = datetime.now()
        self.updated_at = datetime.now()
        return self
    
    def create_checkpoint(self) -> Dict[str, Any]:
        """Create a checkpoint for recovery."""
        checkpoint = {
            "timestamp": datetime.now().isoformat(),
            "current_node": self.current_node,
            "data": self.data.copy(),
            "completed_nodes": self.completed_nodes.copy(),
        }
        self.checkpoints.append(checkpoint)
        return checkpoint
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "execution_id": self.execution_id,
            "graph_name": self.graph_name,
            "data": self.data,
            "current_node": self.current_node,
            "visited_nodes": self.visited_nodes,
            "completed_nodes": self.completed_nodes,
            "status": self.status,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "State":
        """Create from dictionary."""
        return cls(
            execution_id=data["execution_id"],
            graph_name=data["graph_name"],
            data=data.get("data", {}),
            current_node=data.get("current_node"),
            visited_nodes=data.get("visited_nodes", []),
            completed_nodes=data.get("completed_nodes", []),
            status=data.get("status", "pending"),
            error=data.get("error"),
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
        )


class StateStore(ABC):
    """Abstract base class for state storage."""
    
    @abstractmethod
    async def save(self, state: State) -> None:
        """Save state to storage."""
        pass
    
    @abstractmethod
    async def load(self, execution_id: str) -> Optional[State]:
        """Load state from storage."""
        pass
    
    @abstractmethod
    async def delete(self, execution_id: str) -> None:
        """Delete state from storage."""
        pass
    
    @abstractmethod
    async def list_executions(self, graph_name: Optional[str] = None) -> List[str]:
        """List all execution IDs."""
        pass


class InMemoryStateStore(StateStore):
    """In-memory state storage (for development/testing)."""
    
    def __init__(self):
        self._states: Dict[str, State] = {}
        self._lock = asyncio.Lock()
    
    async def save(self, state: State) -> None:
        async with self._lock:
            self._states[state.execution_id] = state
            logger.debug(f"Saved state: {state.execution_id}")
    
    async def load(self, execution_id: str) -> Optional[State]:
        async with self._lock:
            return self._states.get(execution_id)
    
    async def delete(self, execution_id: str) -> None:
        async with self._lock:
            if execution_id in self._states:
                del self._states[execution_id]
    
    async def list_executions(self, graph_name: Optional[str] = None) -> List[str]:
        async with self._lock:
            if graph_name:
                return [
                    eid for eid, state in self._states.items()
                    if state.graph_name == graph_name
                ]
            return list(self._states.keys())


class PersistentStateStore(StateStore):
    """Persistent state storage using filesystem (for production)."""
    
    def __init__(self, storage_dir: str = ".graphserve_states"):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)
        self._lock = asyncio.Lock()
    
    def _get_path(self, execution_id: str) -> str:
        """Get file path for execution."""
        return os.path.join(self.storage_dir, f"{execution_id}.json")
    
    async def save(self, state: State) -> None:
        async with self._lock:
            path = self._get_path(state.execution_id)
            with open(path, 'w') as f:
                json.dump(state.to_dict(), f, indent=2, default=str)
            logger.debug(f"Saved state to {path}")
    
    async def load(self, execution_id: str) -> Optional[State]:
        async with self._lock:
            path = self._get_path(execution_id)
            if not os.path.exists(path):
                return None
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                return State.from_dict(data)
            except Exception as e:
                logger.error(f"Failed to load state {execution_id}: {e}")
                return None
    
    async def delete(self, execution_id: str) -> None:
        async with self._lock:
            path = self._get_path(execution_id)
            if os.path.exists(path):
                os.remove(path)
    
    async def list_executions(self, graph_name: Optional[str] = None) -> List[str]:
        async with self._lock:
            executions = []
            for filename in os.listdir(self.storage_dir):
                if filename.endswith('.json'):
                    execution_id = filename[:-5]
                    if graph_name:
                        state = await self.load(execution_id)
                        if state and state.graph_name == graph_name:
                            executions.append(execution_id)
                    else:
                        executions.append(execution_id)
            return executions


class StateManager:
    """Manages state lifecycle and recovery."""
    
    def __init__(self, store: Optional[StateStore] = None):
        self.store = store or InMemoryStateStore()
    
    async def create_execution(
        self, 
        graph_name: str, 
        initial_data: Optional[Dict[str, Any]] = None
    ) -> State:
        """Create a new execution."""
        import uuid
        execution_id = str(uuid.uuid4())
        
        state = State(
            execution_id=execution_id,
            graph_name=graph_name,
            data=initial_data or {},
        )
        await self.store.save(state)
        logger.info(f"Created execution: {execution_id}")
        return state
    
    async def get_state(self, execution_id: str) -> Optional[State]:
        """Get execution state."""
        return await self.store.load(execution_id)
    
    async def update_state(self, state: State) -> None:
        """Update execution state."""
        await self.store.save(state)
    
    async def checkpoint(self, state: State) -> None:
        """Create a checkpoint."""
        state.create_checkpoint()
        await self.store.save(state)
        logger.debug(f"Checkpoint created: {state.execution_id}")
    
    async def recover(self, execution_id: str) -> Optional[State]:
        """Recover execution from last checkpoint."""
        state = await self.store.load(execution_id)
        if state is None:
            logger.warning(f"Cannot recover: execution {execution_id} not found")
            return None
        
        if state.status == "failed":
            # Retry from last checkpoint
            if state.checkpoints:
                last_checkpoint = state.checkpoints[-1]
                state.current_node = last_checkpoint["current_node"]
                state.data = last_checkpoint["data"].copy()
                state.status = "running"
                state.error = None
                logger.info(f"Recovered execution {execution_id} from checkpoint")
            else:
                # No checkpoint, restart
                state.status = "running"
                state.error = None
                state.current_node = None
                logger.info(f"Recovered execution {execution_id} from start")
            
            await self.store.save(state)
        
        return state
"""
Node definition and configuration for GraphServe Runtime.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Callable, List, Union
import inspect


@dataclass
class NodeConfig:
    """Configuration for a node's resource requirements and scaling behavior.
    
    Simple and intuitive - like FastAPI's approach.
    """
    # Resource requirements
    num_cpus: float = 1.0
    num_gpus: float = 0.0
    memory: int = 1024  # MB
    
    # Scaling configuration
    min_replicas: int = 1
    max_replicas: int = 10
    target_concurrency: float = 1.0  # Target concurrent requests per replica
    
    # Auto-scaling metrics (optional - sensible defaults provided)
    scale_up_delay: float = 30.0  # seconds
    scale_down_delay: float = 60.0  # seconds
    
    # Retry configuration
    max_retries: int = 3
    retry_delay: float = 1.0
    
    def to_ray_resources(self) -> Dict[str, float]:
        """Convert to Ray resource format."""
        return {
            "num_cpus": self.num_cpus,
            "num_gpus": self.num_gpus,
            "memory": self.memory * 1024 * 1024,  # Convert to bytes
        }


class Node:
    """A node in the workflow graph.
    
    Each node is:
    - A unit of computation
    - Independently scalable
    - Resource-isolated
    - Recoverable
    """
    
    def __init__(
        self,
        name: str,
        func: Callable,
        config: Optional[NodeConfig] = None,
        description: str = ""
    ):
        self.name = name
        self.func = func
        self.config = config or NodeConfig()
        self.description = description or func.__doc__ or ""
        
        # Analyze function signature for input/output validation
        self.signature = inspect.signature(func)
        self.input_params = list(self.signature.parameters.keys())
        
        # Runtime state
        self._actor = None
        self._deployment = None
        self._current_replicas = 0
        
    async def execute(self, state: Any, **kwargs) -> Any:
        """Execute this node with the given state."""
        if self._deployment is None:
            raise RuntimeError(f"Node {self.name} not deployed")
        
        # Call the deployment
        result = await self._deployment.remote(state, **kwargs)
        return result
    
    def __call__(self, state: Any, **kwargs) -> Any:
        """Synchronous execution (for local testing)."""
        import asyncio
        return asyncio.run(self.execute(state, **kwargs))
    
    def __repr__(self) -> str:
        return f"Node({self.name}, cpus={self.config.num_cpus}, gpus={self.config.num_gpus})"


# Decorator for easy node creation
def node_func(
    name: Optional[str] = None,
    cpus: float = 1.0,
    gpus: float = 0.0,
    memory: int = 1024,
    min_replicas: int = 1,
    max_replicas: int = 10,
    target_concurrency: float = 1.0,
    description: str = ""
):
    """Decorator to create a node from a function.
    
    Usage:
        @node_func(cpus=2, gpus=1)
        async def my_node(state, **kwargs):
            return {"result": "done"}
    """
    def decorator(func: Callable) -> Node:
        node_name = name or func.__name__
        config = NodeConfig(
            num_cpus=cpus,
            num_gpus=gpus,
            memory=memory,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            target_concurrency=target_concurrency,
        )
        return Node(node_name, func, config, description)
    return decorator
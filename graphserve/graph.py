"""
Graph definition for GraphServe Runtime.

Provides LangGraph-like API with Ray-powered execution.
"""

from typing import Dict, List, Set, Optional, Callable, Any, Union
from dataclasses import dataclass, field
import asyncio
import logging

from .node import Node, NodeConfig

logger = logging.getLogger(__name__)


@dataclass
class Edge:
    """An edge in the graph connecting nodes."""
    from_node: str
    to_node: Optional[str] = None  # None means conditional
    condition: Optional[Callable] = None  # Conditional routing function
    
    def is_conditional(self) -> bool:
        return self.to_node is None and self.condition is not None


class Graph:
    """A workflow graph composed of nodes and edges.
    
    Usage:
        graph = Graph()
        
        @graph.node(cpus=2)
        async def step1(state):
            return {"step1": "done"}
        
        @graph.node(gpus=1)
        async def step2(state):
            return {"step2": "done"}
        
        graph.add_edge("step1", "step2")
        graph.set_start("step1")
    """
    
    def __init__(self, name: str = "default"):
        self.name = name
        self.nodes: Dict[str, Node] = {}
        self.edges: Dict[str, List[Edge]] = {}  # from_node -> edges
        self.start_node: Optional[str] = None
        self.end_nodes: Set[str] = set()
        
        # Runtime state
        self._runtime = None
        self._compiled = False
        
    def node(
        self,
        name: Optional[str] = None,
        cpus: float = 1.0,
        gpus: float = 0.0,
        memory: int = 1024,
        min_replicas: int = 1,
        max_replicas: int = 10,
        target_concurrency: float = 1.0,
        description: str = ""
    ) -> Callable:
        """Decorator to add a node to the graph.
        
        Args:
            name: Node name (defaults to function name)
            cpus: CPU cores required
            gpus: GPUs required
            memory: Memory in MB
            min_replicas: Minimum number of replicas
            max_replicas: Maximum number of replicas
            target_concurrency: Target concurrent requests per replica
            description: Node description
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
            node = Node(node_name, func, config, description)
            self.add_node(node)
            return node
        return decorator
    
    def add_node(self, node: Node) -> "Graph":
        """Add a node to the graph."""
        if node.name in self.nodes:
            raise ValueError(f"Node {node.name} already exists")
        self.nodes[node.name] = node
        self.edges[node.name] = []
        logger.debug(f"Added node: {node.name}")
        return self
    
    def add_edge(
        self, 
        from_node: str, 
        to_node: Optional[str] = None,
        condition: Optional[Callable[[Any], Union[str, List[str]]]] = None
    ) -> "Graph":
        """Add an edge between nodes.
        
        Args:
            from_node: Source node name
            to_node: Target node name (optional for conditional edges)
            condition: Optional routing function that returns next node(s)
                      Function signature: (state) -> str | List[str] | None
        """
        if from_node not in self.nodes:
            raise ValueError(f"Node {from_node} not found")
        if to_node and to_node not in self.nodes:
            raise ValueError(f"Node {to_node} not found")
        
        edge = Edge(from_node, to_node, condition)
        self.edges[from_node].append(edge)
        logger.debug(f"Added edge: {from_node} -> {to_node or 'conditional'}")
        return self
    
    def set_start(self, node_name: str) -> "Graph":
        """Set the entry point of the graph."""
        if node_name not in self.nodes:
            raise ValueError(f"Node {node_name} not found")
        self.start_node = node_name
        return self
    
    def set_end(self, node_name: str) -> "Graph":
        """Mark a node as an end point."""
        if node_name not in self.nodes:
            raise ValueError(f"Node {node_name} not found")
        self.end_nodes.add(node_name)
        return self
    
    def get_next_nodes(self, node_name: str, state: Any) -> List[str]:
        """Get the next nodes to execute based on current state."""
        if node_name not in self.edges:
            return []
        
        next_nodes = []
        for edge in self.edges[node_name]:
            if edge.is_conditional() and edge.condition:
                # Evaluate condition function
                result = edge.condition(state)
                if result is None:
                    continue
                if isinstance(result, list):
                    next_nodes.extend(result)
                else:
                    next_nodes.append(result)
            elif edge.to_node:
                next_nodes.append(edge.to_node)
        
        return next_nodes
    
    def compile(self) -> "Graph":
        """Compile and validate the graph."""
        if self.start_node is None:
            raise ValueError("Start node not set. Call set_start()")
        
        # Check for unreachable nodes
        reachable = self._find_reachable_nodes()
        unreachable = set(self.nodes.keys()) - reachable
        if unreachable:
            logger.warning(f"Unreachable nodes: {unreachable}")
        
        self._compiled = True
        logger.info(f"Graph '{self.name}' compiled successfully")
        return self
    
    def _find_reachable_nodes(self) -> Set[str]:
        """Find all reachable nodes from start."""
        if not self.start_node:
            return set()
        
        visited = set()
        queue = [self.start_node]
        
        while queue:
            node = queue.pop(0)
            if node in visited:
                continue
            visited.add(node)
            
            # Add all possible next nodes
            for edge in self.edges.get(node, []):
                if edge.to_node:
                    queue.append(edge.to_node)
        
        return visited
    
    def visualize(self) -> str:
        """Generate a text visualization of the graph."""
        lines = [f"Graph: {self.name}", "=" * 40]
        
        for name, node in self.nodes.items():
            marker = ""
            if name == self.start_node:
                marker = " [START]"
            if name in self.end_nodes:
                marker += " [END]"
            
            lines.append(f"\n{name}{marker}")
            lines.append(f"  Resources: {node.config.num_cpus} CPUs, {node.config.num_gpus} GPUs")
            lines.append(f"  Replicas: {node.config.min_replicas}-{node.config.max_replicas}")
            
            edges = self.edges.get(name, [])
            if edges:
                for edge in edges:
                    if edge.is_conditional():
                        lines.append(f"  -> [conditional]")
                    else:
                        lines.append(f"  -> {edge.to_node}")
        
        return "\n".join(lines)
    
    def __repr__(self) -> str:
        return f"Graph({self.name}, nodes={len(self.nodes)}, edges={sum(len(e) for e in self.edges.values())})"


# Convenience functions for graph building
def start(graph: Graph, node_name: str) -> Graph:
    """Set the start node of a graph."""
    return graph.set_start(node_name)


def end(graph: Graph, node_name: str) -> Graph:
    """Mark a node as an end node."""
    return graph.set_end(node_name)


def edge(
    graph: Graph, 
    from_node: str, 
    to_node: Optional[str] = None,
    condition: Optional[Callable] = None
) -> Graph:
    """Add an edge to a graph."""
    return graph.add_edge(from_node, to_node, condition)
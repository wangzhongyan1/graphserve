"""
Runtime for GraphServe - Core execution engine.

Manages workflow execution with Ray-powered distribution.
"""

from typing import Dict, Any, Optional, List, Callable
import asyncio
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor

from .graph import Graph
from .node import Node
from .state import State, StateManager, InMemoryStateStore

logger = logging.getLogger(__name__)


class Runtime:
    """Graph execution runtime.
    
    Manages:
    - Graph compilation and validation
    - Node deployment and scaling
    - Workflow execution
    - State management
    - Error handling and recovery
    """
    
    def __init__(
        self, 
        graph: Graph,
        state_manager: Optional[StateManager] = None,
        max_workers: int = 100
    ):
        self.graph = graph
        self.state_manager = state_manager or StateManager(InMemoryStateStore())
        self.max_workers = max_workers
        
        # Execution tracking
        self._executions: Dict[str, asyncio.Task] = {}
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Node deployments (will be set by Ray integration)
        self._deployments: Dict[str, Any] = {}
        
        # Runtime state
        self._running = False
        self._local_mode = False
        
    async def deploy(self, local_mode: bool = False) -> "Runtime":
        """Deploy the graph to Ray.
        
        Args:
            local_mode: If True, run without Ray (for testing)
        """
        self._local_mode = local_mode
        
        if not self.graph._compiled:
            self.graph.compile()
        
        if local_mode:
            logger.info("Running in local mode (no Ray)")
            self._running = True
            return self
        
        # Deploy nodes to Ray
        try:
            import ray
            from ray import serve
            
            if not ray.is_initialized():
                ray.init(ignore_reinit_error=True)
            
            # Start Ray Serve with a different port to avoid conflict with FastAPI
            # Use port 0 to let Ray choose an available port
            if not serve.status().applications:
                serve.start(http_options={"host": "0.0.0.0", "port": 0})
            
            # Deploy each node as a Ray Serve deployment
            for name, node in self.graph.nodes.items():
                deployment = await self._deploy_node(node)
                self._deployments[name] = deployment
                node._deployment = deployment
            
            self._running = True
            logger.info(f"Graph '{self.graph.name}' deployed successfully")
            
        except ImportError:
            logger.warning("Ray not available, falling back to local mode")
            self._local_mode = True
            self._running = True
        
        return self
    
    async def _deploy_node(self, node: Node) -> Any:
        """Deploy a single node to Ray Serve."""
        import ray
        from ray import serve
        from ray.serve.handle import DeploymentHandle
        
        # Create Ray actor class for the node
        # Note: When autoscaling_config is provided, num_replicas should not be set
        use_autoscaling = node.config.max_replicas > node.config.min_replicas
        
        @serve.deployment(
            name=f"{self.graph.name}_{node.name}",
            num_replicas=None if use_autoscaling else node.config.min_replicas,
            ray_actor_options=node.config.to_ray_resources(),
            autoscaling_config={
                "min_replicas": node.config.min_replicas,
                "max_replicas": node.config.max_replicas,
                "target_num_ongoing_requests_per_replica": node.config.target_concurrency,
                "upscale_delay_s": node.config.scale_up_delay,
                "downscale_delay_s": node.config.scale_down_delay,
            } if use_autoscaling else None,
        )
        class NodeActor:
            def __init__(self, node_func):
                self.node_func = node_func
            
            async def __call__(self, state_data, **kwargs):
                # state_data is a dict, wrap it in a simple object that mimics State
                class SimpleState:
                    def __init__(self, data):
                        self.data = data
                return await self.node_func(SimpleState(state_data), **kwargs)
        
        # Deploy the actor
        deployment = NodeActor.bind(node.func)
        handle = serve.run(deployment, name=f"{self.graph.name}_{node.name}", route_prefix=None)
        
        logger.info(f"Deployed node {node.name} with {node.config.min_replicas} replicas")
        return handle
    
    async def submit(
        self, 
        initial_data: Optional[Dict[str, Any]] = None,
        execution_id: Optional[str] = None
    ) -> str:
        """Submit a new workflow execution.
        
        Returns:
            execution_id: Unique identifier for tracking
        """
        if not self._running:
            raise RuntimeError("Runtime not deployed. Call deploy() first.")
        
        # Create execution state
        state = await self.state_manager.create_execution(
            self.graph.name,
            initial_data
        )
        
        if execution_id:
            state.execution_id = execution_id
        
        # Start execution
        task = asyncio.create_task(self._execute_workflow(state))
        self._executions[state.execution_id] = task
        
        logger.info(f"Submitted execution: {state.execution_id}")
        return state.execution_id
    
    async def _execute_workflow(self, state: State) -> State:
        """Execute the workflow for a given state."""
        try:
            state.status = "running"
            await self.state_manager.update_state(state)
            
            # Start from start node
            current_node = self.graph.start_node
            
            while current_node:
                # Set current node
                state.set_current_node(current_node)
                await self.state_manager.checkpoint(state)
                
                # Execute node
                logger.debug(f"Executing node: {current_node}")
                
                try:
                    if self._local_mode:
                        result = await self._execute_node_local(current_node, state)
                    else:
                        result = await self._execute_node_remote(current_node, state)
                    
                    # Update state with result
                    if result:
                        state.update(result if isinstance(result, dict) else {"result": result})
                    
                    state.mark_completed(current_node)
                    await self.state_manager.update_state(state)
                    
                except Exception as e:
                    logger.error(f"Node {current_node} failed: {e}")
                    state.mark_failed(str(e))
                    await self.state_manager.update_state(state)
                    raise
                
                # Determine next nodes
                next_nodes = self.graph.get_next_nodes(current_node, state)
                
                if not next_nodes:
                    # End of workflow
                    break
                elif len(next_nodes) == 1:
                    # Single path
                    current_node = next_nodes[0]
                else:
                    # Parallel paths - fan out
                    await self._execute_parallel(next_nodes, state)
                    break
            
            # Mark as completed
            state.mark_completed_success()
            await self.state_manager.update_state(state)
            
            logger.info(f"Execution completed: {state.execution_id}")
            return state
            
        except Exception as e:
            logger.error(f"Execution failed: {state.execution_id}, error: {e}")
            state.mark_failed(str(e))
            await self.state_manager.update_state(state)
            raise
        finally:
            if state.execution_id in self._executions:
                del self._executions[state.execution_id]
    
    async def _execute_node_local(self, node_name: str, state: State) -> Any:
        """Execute a node locally (for testing)."""
        node = self.graph.nodes[node_name]
        
        # Directly await the async function
        result = await node.func(state)
        return result
    
    async def _execute_node_remote(self, node_name: str, state: State) -> Any:
        """Execute a node remotely via Ray."""
        node = self.graph.nodes[node_name]
        deployment = self._deployments.get(node_name)
        
        if deployment is None:
            raise RuntimeError(f"Node {node_name} not deployed")
        
        # Call the deployment
        result = await deployment.remote(state.data)
        return result
    
    async def _execute_parallel(self, node_names: List[str], parent_state: State) -> None:
        """Execute multiple nodes in parallel."""
        tasks = []
        
        for node_name in node_names:
            # Create child execution
            child_state = await self.state_manager.create_execution(
                self.graph.name,
                parent_state.data.copy()
            )
            child_state.set_current_node(node_name)
            
            task = asyncio.create_task(self._execute_workflow(child_state))
            tasks.append(task)
        
        # Wait for all parallel executions
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def get_state(self, execution_id: str) -> Optional[State]:
        """Get the current state of an execution.
        
        Args:
            execution_id: Execution ID
        
        Returns:
            Current state or None if not found
        """
        return await self.state_manager.get_state(execution_id)
    
    async def get_result(self, execution_id: str, timeout: Optional[float] = None) -> Optional[State]:
        """Get the result of an execution.
        
        Args:
            execution_id: Execution ID
            timeout: Timeout in seconds (None = wait forever)
        
        Returns:
            Final state or None if not found
        """
        task = self._executions.get(execution_id)
        
        if task:
            try:
                await asyncio.wait_for(task, timeout=timeout)
            except asyncio.TimeoutError:
                return None
        
        return await self.state_manager.get_state(execution_id)
    
    async def cancel(self, execution_id: str) -> bool:
        """Cancel an execution."""
        task = self._executions.get(execution_id)
        
        if task and not task.done():
            task.cancel()
            del self._executions[execution_id]
            
            state = await self.state_manager.get_state(execution_id)
            if state:
                state.status = "cancelled"
                await self.state_manager.update_state(state)
            
            return True
        
        return False
    
    async def recover(self, execution_id: str) -> Optional[str]:
        """Recover a failed execution."""
        state = await self.state_manager.recover(execution_id)
        
        if state is None:
            return None
        
        # Restart execution
        task = asyncio.create_task(self._execute_workflow(state))
        self._executions[state.execution_id] = task
        
        logger.info(f"Recovered execution: {execution_id}")
        return state.execution_id
    
    async def shutdown(self) -> None:
        """Shutdown the runtime."""
        self._running = False
        
        # Cancel all running executions
        for task in self._executions.values():
            task.cancel()
        
        # Wait for cancellations
        if self._executions:
            await asyncio.gather(*self._executions.values(), return_exceptions=True)
        
        self._executions.clear()
        self._executor.shutdown(wait=True)
        
        logger.info("Runtime shutdown complete")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get runtime statistics."""
        return {
            "graph_name": self.graph.name,
            "running": self._running,
            "local_mode": self._local_mode,
            "active_executions": len(self._executions),
            "nodes": len(self.graph.nodes),
            "edges": sum(len(e) for e in self.graph.edges.values()),
        }
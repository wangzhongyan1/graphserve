"""
Ray Serve integration for GraphServe Runtime.

Provides production-grade serving capabilities.
"""

from typing import Dict, Any, Optional, List, Callable
import asyncio
import logging
import json
from dataclasses import dataclass

from .graph import Graph
from .runtime import Runtime
from .state import StateManager, PersistentStateStore

logger = logging.getLogger(__name__)


@dataclass
class ServeConfig:
    """Configuration for serving a graph."""
    host: str = "0.0.0.0"
    port: int = 8000
    storage_dir: str = ".graphserve_states"
    enable_metrics: bool = True
    max_concurrent_requests: int = 1000


class GraphServer:
    """HTTP server for graph execution.
    
    Provides REST API for:
    - Submitting executions
    - Querying status
    - Getting results
    - Cancelling executions
    """
    
    def __init__(
        self,
        graph: Graph,
        config: Optional[ServeConfig] = None
    ):
        self.graph = graph
        self.config = config or ServeConfig()
        self.runtime: Optional[Runtime] = None
        self._app = None
    
    async def startup(self):
        """Initialize the server."""
        # Create state manager with persistent storage
        state_manager = StateManager(
            PersistentStateStore(self.config.storage_dir)
        )
        
        # Create runtime
        self.runtime = Runtime(self.graph, state_manager)
        
        # Deploy to Ray
        await self.runtime.deploy(local_mode=False)
        
        logger.info(f"GraphServer started for graph: {self.graph.name}")
    
    async def shutdown(self):
        """Shutdown the server."""
        if self.runtime:
            await self.runtime.shutdown()
        logger.info("GraphServer shutdown")
    
    async def submit(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Submit a new execution."""
        if not self.runtime:
            raise RuntimeError("Server not initialized")
        
        initial_data = request_data.get("data", {})
        execution_id = await self.runtime.submit(initial_data)
        
        return {
            "execution_id": execution_id,
            "status": "submitted",
            "graph": self.graph.name,
        }
    
    async def get_status(self, execution_id: str) -> Dict[str, Any]:
        """Get execution status."""
        if not self.runtime:
            raise RuntimeError("Server not initialized")
        
        state = await self.runtime.get_state(execution_id)
        
        if state is None:
            return {"error": "Execution not found"}, 404
        
        return {
            "execution_id": execution_id,
            "status": state.status,
            "current_node": state.current_node,
            "completed_nodes": state.completed_nodes,
            "created_at": state.created_at.isoformat(),
            "updated_at": state.updated_at.isoformat(),
        }
    
    async def get_result(self, execution_id: str) -> Dict[str, Any]:
        """Get execution result."""
        if not self.runtime:
            raise RuntimeError("Server not initialized")
        
        state = await self.runtime.get_state(execution_id)
        
        if state is None:
            return {"error": "Execution not found"}, 404
        
        return {
            "execution_id": execution_id,
            "status": state.status,
            "data": state.data,
            "error": state.error,
            "completed_at": state.completed_at.isoformat() if state.completed_at else None,
        }
    
    async def cancel(self, execution_id: str) -> Dict[str, Any]:
        """Cancel an execution."""
        if not self.runtime:
            raise RuntimeError("Server not initialized")
        
        success = await self.runtime.cancel(execution_id)
        
        return {
            "execution_id": execution_id,
            "cancelled": success,
        }
    
    async def list_executions(self) -> Dict[str, Any]:
        """List all executions."""
        if not self.runtime:
            raise RuntimeError("Server not initialized")
        
        executions = await self.runtime.state_manager.store.list_executions(
            self.graph.name
        )
        
        return {
            "executions": executions,
            "count": len(executions),
        }
    
    def get_fastapi_app(self):
        """Get FastAPI application."""
        from fastapi import FastAPI, HTTPException
        from fastapi.responses import JSONResponse
        from contextlib import asynccontextmanager
        
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            await self.startup()
            yield
            await self.shutdown()
        
        app = FastAPI(
            title=f"GraphServe - {self.graph.name}",
            description="AI Workflow Runtime API",
            version="0.1.0",
            lifespan=lifespan,
        )
        
        @app.post("/execute")
        async def execute(request: Dict[str, Any]):
            """Submit a new execution."""
            try:
                result = await self.submit(request)
                return result
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @app.get("/status/{execution_id}")
        async def status(execution_id: str):
            """Get execution status."""
            result = await self.get_status(execution_id)
            if isinstance(result, tuple):
                raise HTTPException(status_code=result[1], detail=result[0]["error"])
            return result
        
        @app.get("/result/{execution_id}")
        async def result(execution_id: str):
            """Get execution result."""
            result = await self.get_result(execution_id)
            if isinstance(result, tuple):
                raise HTTPException(status_code=result[1], detail=result[0]["error"])
            return result
        
        @app.post("/cancel/{execution_id}")
        async def cancel(execution_id: str):
            """Cancel an execution."""
            return await self.cancel(execution_id)
        
        @app.get("/executions")
        async def executions():
            """List all executions."""
            return await self.list_executions()
        
        @app.get("/health")
        async def health():
            """Health check."""
            return {
                "status": "healthy",
                "graph": self.graph.name,
                "runtime_stats": self.runtime.get_stats() if self.runtime else None,
            }
        
        @app.get("/graph")
        async def get_graph():
            """Get graph information."""
            return {
                "name": self.graph.name,
                "nodes": [
                    {
                        "name": name,
                        "resources": {
                            "cpus": node.config.num_cpus,
                            "gpus": node.config.num_gpus,
                            "memory": node.config.memory,
                        },
                        "replicas": {
                            "min": node.config.min_replicas,
                            "max": node.config.max_replicas,
                        },
                    }
                    for name, node in self.graph.nodes.items()
                ],
                "start_node": self.graph.start_node,
                "end_nodes": list(self.graph.end_nodes),
            }
        
        self._app = app
        return app


def serve_graph(
    graph: Graph,
    config: Optional[ServeConfig] = None,
    run_server: bool = True
):
    """Serve a graph via Ray Serve and FastAPI.
    
    Usage:
        graph = Graph()
        # ... define graph ...
        
        # Serve the graph
        serve_graph(graph, host="0.0.0.0", port=8000)
    
    Args:
        graph: The graph to serve
        config: Server configuration
        run_server: If True, start the server; if False, return the app
    
    Returns:
        FastAPI app if run_server=False, otherwise None
    """
    config = config or ServeConfig()
    server = GraphServer(graph, config)
    app = server.get_fastapi_app()
    
    if run_server:
        import uvicorn
        uvicorn.run(app, host=config.host, port=config.port)
    else:
        return app
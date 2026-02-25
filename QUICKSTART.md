# GraphServe Runtime - Quick Start Guide

## Installation

```bash
# Clone or download the project
cd graphserve-runtime

# Install dependencies
pip install -r requirements.txt
```

## 1. Hello World (18 lines)

```python
import asyncio
from graphserve import Graph, Runtime

graph = Graph("hello")

@graph.node(cpus=0.5)
async def greet(state):
    name = state.data.get("name", "World")
    return {"message": f"Hello, {name}!"}

@graph.node(cpus=0.5)
async def farewell(state):
    msg = state.data.get("message", "")
    return {"final": f"{msg} Goodbye!"}

graph.add_edge("greet", "farewell").set_start("greet").set_end("farewell")

async def main():
    runtime = await Runtime(graph).deploy(local_mode=True)
    execution_id = await runtime.submit({"name": "GraphServe"})
    result = await runtime.get_result(execution_id)
    print(f"Result: {result.data}")
    await runtime.shutdown()

asyncio.run(main())
```

**Run it:**
```bash
python examples/hello_world.py
```

**Output:**
```
Result: {'name': 'GraphServe', 'message': 'Hello, GraphServe!', 'final': 'Hello, GraphServe! Goodbye!'}
```

## 2. Resource Configuration

```python
@graph.node(
    cpus=4,                # CPU cores
    gpus=1,                # GPUs
    memory=8192,           # Memory in MB
    min_replicas=1,        # Min replicas
    max_replicas=10,       # Max replicas (auto-scale)
    target_concurrency=2   # Target requests per replica
)
async def gpu_node(state):
    # GPU-intensive processing
    return {"result": "processed"}
```

## 3. Conditional Routing

```python
@graph.node()
async def router(state):
    return {"route": "A" if state.data.get("type") == "A" else "B"}

@graph.node()
async def path_a(state):
    return {"path": "A"}

@graph.node()
async def path_b(state):
    return {"path": "B"}

def route_condition(state):
    if state.data.get("route") == "A":
        return "path_a"
    return "path_b"

graph.add_edge("router", condition=route_condition)
```

## 4. Parallel Execution

```python
def parallel_route(state):
    # Return multiple targets for parallel execution
    return ["node_x", "node_y", "node_z"]

graph.add_edge("start", condition=parallel_route)
```

## 5. Persistent State

```python
from graphserve import PersistentStateStore, StateManager

# Use persistent storage
state_manager = StateManager(PersistentStateStore("./states"))
runtime = Runtime(graph, state_manager)

# Submit job
execution_id = await runtime.submit({"input": "data"})

# Recover from failure
recovered_id = await runtime.recover(execution_id)
```

## 6. HTTP API Server

```python
from graphserve import serve_graph

# Serve graph via HTTP API
serve_graph(graph, host="0.0.0.0", port=8000)
```

**API Endpoints:**
- `POST /execute` - Submit execution
- `GET /status/{id}` - Check status
- `GET /result/{id}` - Get result
- `POST /cancel/{id}` - Cancel execution
- `GET /health` - Health check

## 7. Full Example: Storyboard Generation

See `examples/storyboard_demo.py` for a complete production-ready example featuring:
- 7-node workflow
- Mixed GPU/CPU resources
- Auto-scaling
- Conditional routing
- State persistence

```bash
python examples/storyboard_demo.py
```

## Key Concepts

### Graph
- Container for nodes and edges
- Defines workflow structure
- Compiled before execution

### Node
- Unit of computation
- Async function with state input
- Independent resource allocation
- Auto-scaling per node

### State
- Passed between nodes
- Contains data and metadata
- Automatically checkpointed
- Used for recovery

### Runtime
- Executes the graph
- Manages deployments
- Handles state persistence
- Provides monitoring

## Design Principles

1. **Minimal API**: Only 5 main classes needed
2. **Decorator-Based**: Intuitive `@graph.node()` syntax
3. **Async-First**: All nodes are async functions
4. **State-Driven**: Data flows through state
5. **Ray-Native**: Transparent distributed execution
6. **Local Mode**: Works without Ray for development

## Next Steps

- Read `README.md` for detailed documentation
- Check `ARCHITECTURE.md` for system design
- Run `tests/test_graph.py` for examples
- Explore `examples/storyboard_demo.py` for advanced usage
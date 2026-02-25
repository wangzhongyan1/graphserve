# GraphServe Runtime - Project Structure

```
graphserve-runtime/
│
├── graphserve/                     # Core framework package
│   ├── __init__.py                 # Package exports
│   ├── graph.py                    # Graph definition and compilation
│   ├── node.py                     # Node definition and configuration
│   ├── runtime.py                  # Runtime execution engine
│   ├── state.py                    # State management and persistence
│   ├── serve.py                    # Ray Serve integration and HTTP API
│   └── resource.py                 # Resource management and auto-scaling
│
├── examples/                       # Example applications
│   ├── hello_world.py              # Simple 18-line demo
│   └── storyboard_demo.py          # Full-featured storyboard generation
│
├── tests/                          # Test suite
│   └── test_graph.py               # Unit tests for all components
│
├── setup.py                        # Package setup configuration
├── requirements.txt                # Dependencies
├── README.md                       # User documentation
├── ARCHITECTURE.md                 # Architecture documentation
└── PROJECT_STRUCTURE.md            # This file
```

## File Descriptions

### Core Framework (`graphserve/`)

#### `__init__.py`
Package entry point. Exports all public APIs:
- `Graph` - Graph definition class
- `Node`, `NodeConfig` - Node definition and configuration
- `Runtime` - Execution runtime
- `State`, `StateManager` - State management
- `InMemoryStateStore`, `PersistentStateStore` - Storage backends
- `serve_graph`, `ServeConfig` - HTTP serving

#### `graph.py`
Graph definition and compilation:
- `Graph` class - Main graph builder
  - `node()` decorator - Add nodes with resource config
  - `add_edge()` - Connect nodes
  - `set_start()`, `set_end()` - Define entry/exit points
  - `compile()` - Validate and compile
  - `visualize()` - Text visualization
  - `get_next_nodes()` - Dynamic routing

#### `node.py`
Node definition and configuration:
- `Node` class - Node implementation
  - `name` - Node identifier
  - `func` - Node function
  - `config` - Resource configuration
  - `execute()` - Execute the node
- `NodeConfig` dataclass - Resource requirements
  - `num_cpus`, `num_gpus`, `memory`
  - `min_replicas`, `max_replicas`, `target_concurrency`
  - `max_retries`, `retry_delay`

#### `runtime.py`
Execution runtime:
- `Runtime` class - Core execution engine
  - `deploy()` - Deploy to Ray or local mode
  - `submit()` - Start new execution
  - `get_result()` - Get execution result
  - `cancel()` - Cancel execution
  - `recover()` - Recover failed execution
  - `shutdown()` - Cleanup
  - `get_stats()` - Runtime statistics

#### `state.py`
State management and persistence:
- `State` dataclass - Execution state
  - `execution_id`, `graph_name`
  - `data` - User data and results
  - `current_node`, `visited_nodes`, `completed_nodes`
  - `checkpoints` - Recovery points
  - `update()`, `set_current_node()`, `mark_completed()`
  - `create_checkpoint()`, `to_dict()`, `from_dict()`
- `StateStore` - Abstract storage interface
- `InMemoryStateStore` - In-memory storage (dev)
- `PersistentStateStore` - File-based storage (production)
- `StateManager` - State lifecycle management

#### `serve.py`
Ray Serve integration and HTTP API:
- `GraphServer` class - HTTP server
  - `startup()`, `shutdown()`
  - `submit()`, `get_status()`, `get_result()`
  - `cancel()`, `list_executions()`
  - `get_fastapi_app()` - FastAPI application
- `ServeConfig` dataclass - Server configuration
- `serve_graph()` - Convenience function

#### `resource.py`
Resource management and auto-scaling:
- `NodeMetrics` dataclass - Node metrics
- `MetricsCollector` - Metrics collection
- `AutoScaler` - Auto-scaling decisions
- `ResourceManager` - Resource coordination

### Examples (`examples/`)

#### `hello_world.py` (18 lines)
Minimal working example:
- Creates a 2-node graph
- Linear execution flow
- Local mode deployment
- Demonstrates basic API

#### `storyboard_demo.py`
Full-featured production example:
- 7-node workflow
- Mixed GPU/CPU resources
- Auto-scaling configuration
- Conditional routing
- State persistence
- Parallel execution paths
- Storyboard generation scenario

### Tests (`tests/`)

#### `test_graph.py`
Comprehensive test suite:
- Graph definition tests
- Runtime execution tests
- Node configuration tests
- State management tests
- 18 test cases, all passing

## Key Design Decisions

1. **Minimal API Surface**: Only 5 main classes needed for basic usage
2. **Decorator-Based**: `@graph.node()` for intuitive node definition
3. **Async-First**: All node functions are async
4. **State-Driven**: Execution state passed between nodes
5. **Ray-Native**: Leverages Ray Serve for distribution
6. **Local Mode**: Works without Ray for development
7. **Progressive Complexity**: Simple cases simple, complex cases possible

## Usage Patterns

### Simple Usage
```python
from graphserve import Graph, Runtime

graph = Graph()
@graph.node()
async def step(state):
    return {"result": "done"}
graph.set_start("step")

runtime = await Runtime(graph).deploy(local_mode=True)
execution_id = await runtime.submit({})
result = await runtime.get_result(execution_id)
```

### Advanced Usage
```python
from graphserve import Graph, Runtime, StateManager, PersistentStateStore

graph = Graph()
@graph.node(cpus=4, gpus=1, min_replicas=2, max_replicas=10)
async def gpu_step(state):
    return {"result": "gpu_processed"}

# Conditional routing
def route(state):
    return "gpu_step" if state.data.get("use_gpu") else "cpu_step"
graph.add_edge("start", condition=route)

# Persistent state
state_manager = StateManager(PersistentStateStore("./states"))
runtime = Runtime(graph, state_manager)
await runtime.deploy()  # Ray mode
```

## Extension Points

1. **Custom State Store**: Implement `StateStore` interface
2. **Custom Metrics**: Extend `MetricsCollector`
3. **Custom Routing**: Any function for conditional edges
4. **Custom Resources**: Full Ray resource specification
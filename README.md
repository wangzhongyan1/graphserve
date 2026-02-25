# GraphServe Runtime

> **A Simple yet Powerful AI Workflow Runtime**
> 
> *Like FastAPI, but for AI Workflow Graphs on Ray*

GraphServe Runtime combines the **graph-based decision making** of LangGraph with the **distributed resource scheduling** of Ray Serve, providing a production-ready framework for building scalable AI workflows.

## 🎯 Design Philosophy

- **Minimal & Intuitive**: Get started in minutes, not hours
- **Convention over Configuration**: Sensible defaults that just work
- **Progressive Complexity**: Start simple, scale when needed
- **Hide the Complexity**: Ray's power without Ray's complexity

## ✨ Features

| Feature | Description |
|---------|-------------|
| 🔄 **Graph Execution** | LangGraph-like API for defining workflow graphs |
| ⚡ **Auto-scaling** | Each node scales independently based on load |
| 🎯 **Resource Management** | Per-node GPU/CPU/Memory allocation |
| 💾 **Persistence** | Automatic state checkpointing and recovery |
| 🔀 **Dynamic Routing** | Runtime decision on next nodes |
| 🚀 **Ray Serve Integration** | Production-grade serving out of the box |
| 📊 **Observability** | Built-in metrics and health checks |

## 🚀 Quick Start

### Installation

```bash
pip install -r requirements.txt
```

### Hello World (18 lines)

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

**Output:**
```
Result: {'message': 'Hello, GraphServe!', 'final': 'Hello, GraphServe! Goodbye!'}
```

## 📖 Core Concepts

### 1. Graph Definition

```python
from graphserve import Graph

graph = Graph("my_workflow")

@graph.node(
    cpus=2,              # CPU cores
    gpus=1,              # GPUs
    memory=4096,         # Memory in MB
    min_replicas=1,      # Min replicas
    max_replicas=10,     # Max replicas (auto-scale)
    target_concurrency=2 # Target requests per replica
)
async def my_node(state):
    # Your logic here
    return {"result": "done"}
```

### 2. Graph Structure

```python
# Linear flow
graph.add_edge("node_a", "node_b")

# Conditional routing
def route_condition(state):
    if state.data.get("type") == "A":
        return "node_a"
    else:
        return "node_b"

graph.add_edge("start", condition=route_condition)

# Parallel execution (returns multiple targets)
def parallel_route(state):
    return ["node_x", "node_y", "node_z"]

graph.add_edge("start", condition=parallel_route)
```

### 3. Deployment

```python
from graphserve import Runtime, serve_graph

# Option 1: Programmatic execution
runtime = await Runtime(graph).deploy()
execution_id = await runtime.submit({"input": "data"})
result = await runtime.get_result(execution_id)

# Option 2: HTTP API server
serve_graph(graph, host="0.0.0.0", port=8000)
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        GraphServe Runtime                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │   Graph     │───▶│   Runtime   │───▶│    State    │         │
│  │  Definition │    │   Engine    │    │   Manager   │         │
│  └─────────────┘    └──────┬──────┘    └─────────────┘         │
│                             │                                    │
│                    ┌────────┴────────┐                          │
│                    ▼                 ▼                          │
│           ┌─────────────┐   ┌─────────────┐                    │
│           │   Scheduler │   │   Resource  │                    │
│           │             │   │   Manager   │                    │
│           └──────┬──────┘   └──────┬──────┘                    │
│                  │                  │                           │
│                  ▼                  ▼                           │
│           ┌─────────────────────────────────┐                  │
│           │         Ray Serve Layer          │                  │
│           │  ┌─────┐ ┌─────┐ ┌─────┐       │                  │
│           │  │Node1│ │Node2│ │Node3│ ...   │                  │
│           │  │(GPU)│ │(CPU)│ │(GPU)│       │                  │
│           │  └──┬──┘ └──┬──┘ └──┬──┘       │                  │
│           │     │       │       │           │                  │
│           │     └───────┴───────┘           │                  │
│           │         Auto-scaling             │                  │
│           └─────────────────────────────────┘                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## 📊 Full Demo: Storyboard Generation

See `examples/storyboard_demo.py` for a complete production-ready example featuring:

- **Multi-node workflow**: Scene analysis → Character extraction → Image generation
- **Mixed resources**: GPU nodes for AI inference, CPU nodes for data processing
- **Auto-scaling**: Each node scales 1-20 replicas based on load
- **Conditional routing**: Dynamic path selection based on content
- **State persistence**: Automatic checkpointing and recovery

```bash
python examples/storyboard_demo.py
```

## 🔧 Configuration

### Node Configuration

```python
@graph.node(
    # Resources
    cpus=2.0,           # CPU cores (float supported)
    gpus=1.0,           # GPUs
    memory=4096,        # Memory in MB
    
    # Scaling
    min_replicas=1,     # Minimum replicas
    max_replicas=10,    # Maximum replicas
    target_concurrency=2.0,  # Target concurrent requests per replica
    
    # Retry
    max_retries=3,      # Max retries on failure
    retry_delay=1.0,    # Delay between retries (seconds)
)
```

### Runtime Configuration

```python
from graphserve import Runtime, PersistentStateStore, StateManager

# Persistent state storage
state_manager = StateManager(
    PersistentStateStore("./states")
)

runtime = Runtime(
    graph=graph,
    state_manager=state_manager,
    max_workers=100
)
```

### Server Configuration

```python
from graphserve import serve_graph, ServeConfig

config = ServeConfig(
    host="0.0.0.0",
    port=8000,
    storage_dir="./states",
    enable_metrics=True,
)

serve_graph(graph, config=config)
```

## 🌐 HTTP API

When serving a graph, the following endpoints are available:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/execute` | POST | Submit a new execution |
| `/status/{id}` | GET | Get execution status |
| `/result/{id}` | GET | Get execution result |
| `/cancel/{id}` | POST | Cancel execution |
| `/executions` | GET | List all executions |
| `/health` | GET | Health check |
| `/graph` | GET | Graph information |

### Example API Usage

```bash
# Submit execution
curl -X POST http://localhost:8000/execute \
  -H "Content-Type: application/json" \
  -d '{"data": {"name": "World"}}'

# Check status
curl http://localhost:8000/status/{execution_id}

# Get result
curl http://localhost:8000/result/{execution_id}
```

## 🔄 State Management

### Automatic Checkpointing

```python
# Checkpoints are created automatically at each node
state = await runtime.get_state(execution_id)
print(state.checkpoints)  # List of all checkpoints
```

### Recovery

```python
# Recover a failed execution
recovered_id = await runtime.recover(execution_id)
```

## 📈 Monitoring

### Runtime Statistics

```python
stats = runtime.get_stats()
print(stats)
# {
#     "graph_name": "my_workflow",
#     "running": True,
#     "local_mode": False,
#     "active_executions": 5,
#     "nodes": 3,
#     "edges": 4
# }
```

## 🧪 Testing

```bash
# Run tests
pytest tests/

# Run with coverage
pytest --cov=graphserve tests/
```

## 🛣️ Roadmap

- [ ] Distributed state storage (Redis)
- [ ] WebSocket support for real-time updates
- [ ] Visual graph editor
- [ ] Advanced metrics (Prometheus)
- [ ] DAG visualization
- [ ] Subgraph composition
- [ ] Loop detection and handling

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

MIT License - see LICENSE file for details.

## 🙏 Acknowledgments

- **LangGraph** for the graph execution model inspiration
- **Ray** for the distributed computing foundation
- **FastAPI** for the API design philosophy
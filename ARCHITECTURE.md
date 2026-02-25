# GraphServe Runtime - Architecture Overview

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              GraphServe Runtime Architecture                         │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                           User API Layer                                     │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐  │   │
│  │  │  Graph Builder  │  │  Runtime API    │  │    HTTP API (FastAPI)       │  │   │
│  │  │  @graph.node()  │  │  runtime.submit │  │  POST /execute              │  │   │
│  │  │  graph.add_edge │  │  runtime.get    │  │  GET  /status/{id}          │  │   │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                          │                                          │
│                                          ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                         Core Runtime Engine                                  │   │
│  │                                                                              │   │
│  │   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌────────────┐  │   │
│  │   │   Graph     │───▶│  Scheduler  │───▶│   State     │───▶│  Recovery  │  │   │
│  │   │  Compiler   │    │             │    │   Manager   │    │   Manager  │  │   │
│  │   └─────────────┘    └──────┬──────┘    └─────────────┘    └────────────┘  │   │
│  │                              │                                               │   │
│  │   ┌─────────────┐    ┌──────┴──────┐    ┌─────────────┐                    │   │
│  │   │   Node      │◄───│   Router    │    │  Checkpoint │                    │   │
│  │   │  Registry   │    │             │    │   Manager   │                    │   │
│  │   └─────────────┘    └─────────────┘    └─────────────┘                    │   │
│  │                                                                              │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                          │                                          │
│                                          ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                      Resource Management Layer                               │   │
│  │                                                                              │   │
│  │   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────┐    │   │
│  │   │  Resource       │  │  Metrics        │  │    Auto-Scaler          │    │   │
│  │   │  Allocator      │  │  Collector      │  │  ┌─────────────────┐    │    │   │
│  │   │  (CPU/GPU/Mem)  │  │  (Latency/QPS)  │  │  │ Scale Up/Down   │    │    │   │
│  │   └────────┬────────┘  └────────┬────────┘  │  │ Decision Engine │    │    │   │
│  │            │                    │           │  └─────────────────┘    │    │   │
│  │            └────────────────────┴───────────┴─────────────────────────┘    │   │
│  │                                                                              │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                          │                                          │
│                                          ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                         Ray Serve Integration                                │   │
│  │                                                                              │   │
│  │   ┌─────────────────────────────────────────────────────────────────────┐   │   │
│  │   │                    Node Deployments (Independent)                    │   │   │
│  │   │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌───────────┐  │   │   │
│  │   │  │  Node A     │  │  Node B     │  │  Node C     │  │   ...     │  │   │   │
│  │   │  │  (GPU=1)    │  │  (CPU=4)    │  │  (GPU=2)    │  │           │  │   │   │
│  │   │  │             │  │             │  │             │  │           │  │   │   │
│  │   │  │ Replicas:   │  │ Replicas:   │  │ Replicas:   │  │           │  │   │   │
│  │   │  │  1 ──► 10   │  │  2 ──► 20   │  │  1 ──► 5    │  │           │  │   │   │
│  │   │  │             │  │             │  │             │  │           │  │   │   │
│  │   │  │ [Ray Actor] │  │ [Ray Actor] │  │ [Ray Actor] │  │           │  │   │   │
│  │   │  │ [Ray Actor] │  │ [Ray Actor] │  │ [Ray Actor] │  │           │  │   │   │
│  │   │  └─────────────┘  └─────────────┘  └─────────────┘  └───────────┘  │   │   │
│  │   │                                                                      │   │   │
│  │   │  Auto-scaling: Based on concurrent requests, latency, queue depth   │   │   │
│  │   └─────────────────────────────────────────────────────────────────────┘   │   │
│  │                                                                              │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                          │                                          │
│                                          ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                         Persistence Layer                                    │   │
│  │                                                                              │   │
│  │   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────┐    │   │
│  │   │  In-Memory      │  │  File System    │  │    Redis (future)       │    │   │
│  │   │  (Development)  │  │  (Production)   │  │    (Distributed)        │    │   │
│  │   └─────────────────┘  └─────────────────┘  └─────────────────────────┘    │   │
│  │                                                                              │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## Execution Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              Workflow Execution Flow                                 │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   1. SUBMIT                                                                          │
│      │                                                                               │
│      ▼                                                                               │
│   ┌─────────────────┐                                                                │
│   │  Create State   │───▶ State(execution_id, data, status="pending")               │
│   │  Persist State  │                                                                │
│   └─────────────────┘                                                                │
│      │                                                                               │
│      ▼                                                                               │
│   2. SCHEDULE                                                                        │
│      │                                                                               │
│      ▼                                                                               │
│   ┌─────────────────┐                                                                │
│   │  Get Start Node │───▶ graph.start_node                                           │
│   │  Update State   │───▶ state.current_node = "start"                               │
│   └─────────────────┘                                                                │
│      │                                                                               │
│      ▼                                                                               │
│   3. EXECUTE NODE                                                                    │
│      │                                                                               │
│      ▼                                                                               │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │                         Node Execution Cycle                                 │   │
│   │                                                                              │   │
│   │   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌────────────┐  │   │
│   │   │  Checkpoint │───▶│   Route     │───▶│   Execute   │───▶│  Update    │  │   │
│   │   │    State    │    │   Request   │    │   Node      │    │   State    │  │   │
│   │   └─────────────┘    │  to Ray     │    │   (Actor)   │    │  w/ Result │  │   │
│   │                      └─────────────┘    └─────────────┘    └────────────┘  │   │
│   │                                                                              │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│      │                                                                               │
│      ▼                                                                               │
│   4. ROUTE                                                                           │
│      │                                                                               │
│      ▼                                                                               │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │                         Routing Decision                                     │   │
│   │                                                                              │   │
│   │   Conditional? ──Yes──▶ Evaluate condition(state) ──▶ Get next node(s)     │   │
│   │      │                                                                       │   │
│   │      No                                                                      │   │
│   │      ▼                                                                       │   │
│   │   Static edge ──▶ Get next node                                             │   │
│   │                                                                              │   │
│   │   Multiple targets? ──Yes──▶ Fan out parallel executions                    │   │
│   │      │                                                                       │   │
│   │      No                                                                      │   │
│   │      ▼                                                                       │   │
│   │   Single target ──▶ Continue to next node                                   │   │
│   │                                                                              │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│      │                                                                               │
│      ▼                                                                               │
│   5. COMPLETE                                                                        │
│      │                                                                               │
│      ▼                                                                               │
│   ┌─────────────────┐                                                                │
│   │  No more nodes? │───Yes──▶ Mark complete ──▶ Persist final state              │
│   │                 │                                                                │
│   │  Has next node? │───Yes──▶ Goto step 3                                        │
│   └─────────────────┘                                                                │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## State Management

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              State Lifecycle                                         │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   State Transitions:                                                                 │
│                                                                                      │
│   ┌─────────┐     submit      ┌─────────┐    execute    ┌─────────┐                 │
│   │ PENDING │ ───────────────▶│ RUNNING │ ─────────────▶│COMPLETED│                 │
│   └─────────┘                 └────┬────┘               └─────────┘                 │
│        │                           │                                                 │
│        │ error                     │ error                                           │
│        ▼                           ▼                                                 │
│   ┌─────────┐                 ┌─────────┐    recover    ┌─────────┐                 │
│   │  FAIL   │                 │  FAIL   │ ────────────▶ │ RUNNING │                 │
│   └─────────┘                 └─────────┘               └─────────┘                 │
│                                                                                      │
│   State Structure:                                                                   │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │  State {                                                                     │   │
│   │    execution_id:    "uuid-1234",          // Unique identifier             │   │
│   │    graph_name:      "my_workflow",        // Graph name                     │   │
│   │    data:            {},                   // User data + results            │   │
│   │    current_node:    "node_a",             // Current position               │   │
│   │    visited_nodes:   ["start", "node_a"],  // Execution path                 │   │
│   │    completed_nodes: ["start"],            // Finished nodes                 │   │
│   │    status:          "running",            // Current status                 │   │
│   │    checkpoints:     [...],                // Recovery points                │   │
│   │    created_at:      "2024-01-01...",      // Start time                     │   │
│   │    updated_at:      "2024-01-01...",      // Last update                    │   │
│   │  }                                                                           │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                      │
│   Checkpoint Strategy:                                                               │
│   - Created before each node execution                                               │
│   - Contains: current_node, data snapshot, completed_nodes                           │
│   - Used for recovery on failure                                                     │
│   - Persisted to storage backend                                                     │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## Auto-Scaling Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           Auto-Scaling Mechanism                                     │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   Per-Node Configuration:                                                            │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │  @graph.node(                                                                │   │
│   │    min_replicas=1,          # Always keep 1 running                         │   │
│   │    max_replicas=10,         # Scale up to 10                                │   │
│   │    target_concurrency=2.0,  # 2 requests per replica                        │   │
│   │    scale_up_delay=30,       # Wait 30s before scaling up                    │   │
│   │    scale_down_delay=60,     # Wait 60s before scaling down                  │   │
│   │  )                                                                           │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                      │
│   Scaling Decision Flow:                                                             │
│                                                                                      │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│   │   Collect   │───▶│   Analyze   │───▶│   Decide    │───▶│   Apply     │         │
│   │   Metrics   │    │   Trends    │    │   Action    │    │   Change    │         │
│   └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘         │
│         │                  │                  │                  │                   │
│         ▼                  ▼                  ▼                  ▼                   │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│   │ - QPS       │    │ - SMA/QPS   │    │ - Scale up  │    │ - Ray Serve │         │
│   │ - Latency   │    │ - Latency   │    │   if load > │    │   API       │         │
│   │ - Queue     │    │   trend     │    │   target    │    │             │         │
│   │   depth     │    │ - Error     │    │ - Scale     │    │ - Update    │         │
│   │ - Errors    │    │   rate      │    │   down if   │    │   replicas  │         │
│   │             │    │             │    │   idle      │    │             │         │
│   └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘         │
│                                                                                      │
│   Ray Serve Integration:                                                             │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │  serve.deployment(                                                           │   │
│   │    autoscaling_config={                                                      │   │
│   │      "min_replicas": node.config.min_replicas,                              │   │
│   │      "max_replicas": node.config.max_replicas,                              │   │
│   │      "target_num_ongoing_requests_per_replica": target_concurrency,         │   │
│   │      "upscale_delay_s": scale_up_delay,                                     │   │
│   │      "downscale_delay_s": scale_down_delay,                                 │   │
│   │    }                                                                         │   │
│   │  )                                                                           │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## Component Interactions

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         Component Interaction Diagram                                │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   User Code:                                                                         │
│   ┌─────────┐                                                                        │
│   │  main   │                                                                        │
│   └────┬────┘                                                                        │
│        │ graph = Graph()                                                             │
│        │ @graph.node()                                                               │
│        │ graph.add_edge()                                                            │
│        │ runtime = Runtime(graph)                                                    │
│        │ runtime.deploy()                                                            │
│        │ runtime.submit(data)                                                        │
│        ▼                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │                         Runtime Components                                   │   │
│   │                                                                              │   │
│   │   ┌─────────────┐         ┌─────────────┐         ┌─────────────┐          │   │
│   │   │   Graph     │◄───────►│  Scheduler  │◄───────►│   State     │          │   │
│   │   │             │ compile │             │  state  │   Manager   │          │   │
│   │   └─────────────┘         └──────┬──────┘         └─────────────┘          │   │
│   │                                  │                                          │   │
│   │                                  ▼                                          │   │
│   │   ┌─────────────┐         ┌─────────────┐         ┌─────────────┐          │   │
│   │   │   Node      │◄───────►│   Router    │◄───────►│  Recovery   │          │   │
│   │   │   Registry  │         │             │         │   Manager   │          │   │
│   │   └──────┬──────┘         └─────────────┘         └─────────────┘          │   │
│   │          │                                                                  │   │
│   │          │ deploy                                                           │   │
│   │          ▼                                                                  │   │
│   │   ┌─────────────────────────────────────────────────────────────────────┐   │   │
│   │   │                      Ray Serve Deployments                           │   │   │
│   │   │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐   │   │   │
│   │   │  │ Node A  │  │ Node B  │  │ Node C  │  │ Node D  │  │  ...    │   │   │   │
│   │   │  │ Actor   │  │ Actor   │  │ Actor   │  │ Actor   │  │         │   │   │   │
│   │   │  │ (GPU)   │  │ (CPU)   │  │ (GPU)   │  │ (CPU)   │  │         │   │   │   │
│   │   │  └─────────┘  └─────────┘  └─────────┘  └─────────┘  └─────────┘   │   │   │
│   │   │                                                                      │   │   │
│   │   │  Each node: Independent replicas, resources, scaling                 │   │   │
│   │   └─────────────────────────────────────────────────────────────────────┘   │   │
│   │                                                                              │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                      │
│   Data Flow:                                                                         │
│   1. User defines graph (nodes + edges)                                              │
│   2. Runtime compiles and validates graph                                            │
│   3. Runtime deploys nodes to Ray Serve                                              │
│   4. User submits execution                                                          │
│   5. Scheduler routes to first node                                                  │
│   6. State Manager creates checkpoint                                                │
│   7. Node executes via Ray Serve                                                     │
│   8. Router determines next node(s)                                                  │
│   9. Repeat 6-8 until complete                                                       │
│   10. Final state persisted                                                          │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## Design Principles

1. **Minimal API Surface**: Users only need to know `Graph`, `node`, `edge`, `Runtime`
2. **Sensible Defaults**: Works out of the box without configuration
3. **Progressive Disclosure**: Simple use cases are simple, complex ones are possible
4. **Ray-Native**: Leverages Ray's distributed capabilities transparently
5. **Production-Ready**: Built-in persistence, recovery, and observability
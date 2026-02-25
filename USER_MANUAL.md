# GraphServe Runtime 用户手册

> **像 FastAPI 一样简单的 AI Workflow 运行时**

## 目录

1. [快速开始](#快速开始)
2. [本地运行](#本地运行)
3. [Server 模式](#server-模式)
4. [SDK 使用指南](#sdk-使用指南)
5. [API 使用指南](#api-使用指南)
6. [高级特性](#高级特性)

---

## 快速开始

### 安装

```bash
# 克隆项目
cd graphserve-runtime

# 安装依赖
pip install -r requirements.txt
```

### 最简单的示例（18 行代码）

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

运行：
```bash
python examples/hello_world.py
```

---

## 本地运行

### 1. 基础本地模式

本地模式不依赖 Ray，适合开发和测试：

```python
from graphserve import Graph, Runtime

graph = Graph("my_workflow")

@graph.node(cpus=1)
async def step1(state):
    return {"step1": "done"}

@graph.node(cpus=1)
async def step2(state):
    return {"step2": "done"}

graph.add_edge("step1", "step2").set_start("step1")

# 本地模式运行
async def main():
    runtime = await Runtime(graph).deploy(local_mode=True)
    execution_id = await runtime.submit({"input": "data"})
    result = await runtime.get_result(execution_id)
    print(result.data)
    await runtime.shutdown()
```

### 2. 带持久化的本地运行

```python
from graphserve import Runtime, PersistentStateStore, StateManager

# 使用持久化存储
state_manager = StateManager(PersistentStateStore("./my_states"))
runtime = Runtime(graph, state_manager)

await runtime.deploy(local_mode=True)
```

### 3. 条件路由

```python
@graph.node()
async def router(state):
    return {"route": "A" if state.data.get("type") == "A" else "B"}

def route_condition(state):
    if state.data.get("route") == "A":
        return "path_a"
    return "path_b"

graph.add_edge("router", condition=route_condition)
```

### 4. 并行执行

```python
def parallel_route(state):
    # 返回多个目标节点，会并行执行
    return ["node_x", "node_y", "node_z"]

graph.add_edge("start", condition=parallel_route)
```

---

## Server 模式

### 1. 启动 HTTP Server

```python
from graphserve import Graph, serve_graph, ServeConfig

graph = Graph("echo_service")

@graph.node(cpus=0.5)
async def process(state):
    name = state.data.get("name", "World")
    return {"result": f"Hello, {name}!"}

graph.set_start("process")

# 配置并启动服务
config = ServeConfig(
    host="0.0.0.0",
    port=8000,
    storage_dir="./server_states",
)

serve_graph(graph, config=config)
```

运行：
```bash
python examples/server_demo.py
```

### 2. Server 配置选项

```python
from graphserve import ServeConfig

config = ServeConfig(
    host="0.0.0.0",          # 监听地址
    port=8000,               # 端口
    storage_dir="./states",  # 状态存储目录
    enable_metrics=True,     # 启用指标
    max_concurrent_requests=1000,  # 最大并发请求
)
```

### 3. 只获取 FastAPI App（不启动服务）

```python
# 用于集成到现有 FastAPI 应用
app = serve_graph(graph, config=config, run_server=False)
# 然后可以用 uvicorn 启动，或挂载到其他应用
```

---

## SDK 使用指南

### 核心类

#### 1. Graph - 图定义

```python
from graphserve import Graph

graph = Graph("workflow_name")

# 添加节点
@graph.node(
    name="optional_name",      # 节点名称（默认函数名）
    cpus=2.0,                  # CPU 核心数
    gpus=1.0,                  # GPU 数量
    memory=4096,               # 内存（MB）
    min_replicas=1,            # 最小副本数
    max_replicas=10,           # 最大副本数
    target_concurrency=2.0,    # 每个副本的目标并发数
)
async def my_node(state):
    # state.data 包含当前执行的数据
    input_val = state.data.get("input_key")
    # 返回的数据会合并到 state.data
    return {"output_key": "value"}

# 添加边
graph.add_edge("from_node", "to_node")

# 条件边
graph.add_edge("from_node", condition=my_condition_func)

# 设置起点和终点
graph.set_start("start_node")
graph.set_end("end_node")

# 编译图（可选，会自动调用）
graph.compile()

# 可视化图结构
print(graph.visualize())
```

#### 2. Runtime - 运行时

```python
from graphserve import Runtime, StateManager, PersistentStateStore

# 基础用法
runtime = Runtime(graph)

# 带持久化的用法
state_manager = StateManager(PersistentStateStore("./states"))
runtime = Runtime(graph, state_manager=state_manager, max_workers=100)

# 部署
await runtime.deploy(local_mode=True)  # 本地模式
await runtime.deploy(local_mode=False)  # Ray 模式（默认）

# 提交执行
execution_id = await runtime.submit({"input": "data"})

# 获取状态
state = await runtime.get_state(execution_id)

# 获取结果（阻塞等待）
result = await runtime.get_result(execution_id, timeout=30)

# 取消执行
await runtime.cancel(execution_id)

# 恢复失败的任务
new_id = await runtime.recover(execution_id)

# 获取统计信息
stats = runtime.get_stats()

# 关闭
await runtime.shutdown()
```

#### 3. State - 状态对象

```python
# State 对象包含以下属性
state.execution_id      # 执行 ID
state.graph_name        # 图名称
state.data              # 数据字典（输入输出都在这里）
state.current_node      # 当前节点
state.visited_nodes     # 访问过的节点列表
state.completed_nodes   # 完成的节点列表
state.status            # 状态: pending/running/completed/failed
state.error             # 错误信息
state.created_at        # 创建时间
state.updated_at        # 更新时间
state.checkpoints       # 检查点列表
```

#### 4. NodeConfig - 节点配置

```python
from graphserve import NodeConfig

config = NodeConfig(
    num_cpus=2.0,              # CPU 核心
    num_gpus=1.0,              # GPU 数量
    memory=4096,               # 内存（MB）
    min_replicas=1,            # 最小副本数
    max_replicas=10,           # 最大副本数
    target_concurrency=2.0,    # 目标并发数/副本
    scale_up_delay=30.0,       # 扩容延迟（秒）
    scale_down_delay=60.0,     # 缩容延迟（秒）
    max_retries=3,             # 最大重试次数
    retry_delay=1.0,           # 重试延迟（秒）
)
```

---

## API 使用指南

当使用 `serve_graph()` 启动服务后，以下 HTTP API 可用：

### API 端点

| 端点 | 方法 | 描述 |
|------|------|------|
| `/execute` | POST | 提交新执行 |
| `/status/{execution_id}` | GET | 查询执行状态 |
| `/result/{execution_id}` | GET | 获取执行结果 |
| `/cancel/{execution_id}` | POST | 取消执行 |
| `/executions` | GET | 列出所有执行 |
| `/health` | GET | 健康检查 |
| `/graph` | GET | 图信息 |
| `/docs` | GET | API 文档（Swagger UI）|

### 使用 curl 调用 API

```bash
# 1. 健康检查
curl http://localhost:8000/health

# 2. 获取图信息
curl http://localhost:8000/graph

# 3. 提交执行
response=$(curl -X POST http://localhost:8000/execute \
  -H "Content-Type: application/json" \
  -d '{"data": {"name": "Alice", "message": "Hello"}}')
echo $response
# 返回: {"execution_id": "xxx", "status": "submitted", "graph": "graph_name"}

# 4. 查询状态
curl http://localhost:8000/status/{execution_id}

# 5. 获取结果
curl http://localhost:8000/result/{execution_id}

# 6. 取消执行
curl -X POST http://localhost:8000/cancel/{execution_id}

# 7. 列出所有执行
curl http://localhost:8000/executions
```

### 使用 Python requests 调用 API

```python
import requests
import time

BASE_URL = "http://localhost:8000"

# 提交任务
resp = requests.post(
    f"{BASE_URL}/execute",
    json={"data": {"name": "Alice", "message": "Hello"}}
)
result = resp.json()
execution_id = result["execution_id"]

# 轮询等待完成
while True:
    resp = requests.get(f"{BASE_URL}/status/{execution_id}")
    status = resp.json()
    
    if status["status"] == "completed":
        # 获取结果
        resp = requests.get(f"{BASE_URL}/result/{execution_id}")
        result = resp.json()
        print(f"结果: {result['data']}")
        break
    elif status["status"] == "failed":
        print("执行失败")
        break
    
    time.sleep(0.5)
```

### API 响应格式

#### 提交执行
```json
{
  "execution_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "submitted",
  "graph": "graph_name"
}
```

#### 查询状态
```json
{
  "execution_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "current_node": "step2",
  "completed_nodes": ["step1"],
  "created_at": "2024-01-01T12:00:00",
  "updated_at": "2024-01-01T12:00:05"
}
```

#### 获取结果
```json
{
  "execution_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "data": {
    "input": "data",
    "step1": "done",
    "step2": "done"
  },
  "error": null,
  "completed_at": "2024-01-01T12:00:10"
}
```

#### 图信息
```json
{
  "name": "graph_name",
  "nodes": [
    {
      "name": "step1",
      "resources": {
        "cpus": 1.0,
        "gpus": 0.0,
        "memory": 1024
      },
      "replicas": {
        "min": 1,
        "max": 10
      }
    }
  ],
  "start_node": "step1",
  "end_nodes": ["step2"]
}
```

---

## 高级特性

### 1. 节点资源配置

```python
@graph.node(
    cpus=4,                # 4 核 CPU
    gpus=1,                # 1 块 GPU
    memory=8192,           # 8GB 内存
    min_replicas=1,        # 最少 1 个副本
    max_replicas=20,       # 最多 20 个副本（自动扩缩容）
    target_concurrency=2,  # 每个副本处理 2 个并发请求
)
async def gpu_intensive_task(state):
    # 执行 GPU 密集型任务
    return {"result": "done"}
```

### 2. 条件路由

```python
@graph.node()
async def classifier(state):
    text = state.data.get("text", "")
    if "urgent" in text:
        return {"priority": "high"}
    return {"priority": "normal"}

def route_by_priority(state):
    priority = state.data.get("priority")
    if priority == "high":
        return "urgent_handler"
    return "normal_handler"

graph.add_edge("classifier", condition=route_by_priority)
```

### 3. 并行分支

```python
@graph.node()
async def splitter(state):
    items = state.data.get("items", [])
    return {"item_count": len(items)}

def parallel_process(state):
    # 返回多个节点名称，会并行执行
    return ["worker_a", "worker_b", "worker_c"]

graph.add_edge("splitter", condition=parallel_process)
```

### 4. 状态恢复

```python
# 创建带持久化的运行时
state_manager = StateManager(PersistentStateStore("./states"))
runtime = Runtime(graph, state_manager)

await runtime.deploy()

# 提交任务
execution_id = await runtime.submit({"input": "data"})

# 如果任务失败，可以恢复
recovered_id = await runtime.recover(execution_id)
```

### 5. 完整示例：Storyboard 生成

```python
import asyncio
from graphserve import Graph, Runtime, PersistentStateStore, StateManager

graph = Graph("storyboard_generator")

@graph.node(
    cpus=2, gpus=1, memory=4096,
    min_replicas=1, max_replicas=5
)
async def scene_analyzer(state):
    novel_text = state.data.get("novel_text", "")
    # 分析场景...
    return {"scenes": [...], "scene_count": 3}

@graph.node(
    cpus=4, gpus=0, memory=2048,
    min_replicas=2, max_replicas=20
)
async def character_extractor(state):
    scenes = state.data.get("scenes", [])
    # 提取角色...
    return {"characters": [...]}

# 设置工作流
graph.add_edge("scene_analyzer", "character_extractor")
graph.set_start("scene_analyzer")

async def main():
    # 使用持久化存储
    state_manager = StateManager(PersistentStateStore("./storyboard_states"))
    runtime = Runtime(graph, state_manager)
    
    await runtime.deploy(local_mode=True)
    
    execution_id = await runtime.submit({
        "title": "The Adventure Begins",
        "novel_text": "Alice met Bob in the old castle..."
    })
    
    result = await runtime.get_result(execution_id)
    print(f"结果: {result.data}")
    
    await runtime.shutdown()

asyncio.run(main())
```

---

## 最佳实践

1. **开发阶段**：使用 `local_mode=True` 快速迭代
2. **生产部署**：使用 `serve_graph()` 启动 HTTP 服务
3. **状态持久化**：生产环境务必使用 `PersistentStateStore`
4. **资源分配**：根据实际负载调整 `target_concurrency`
5. **错误处理**：节点函数内捕获异常，返回错误信息

---

## 故障排查

### 服务无法启动
- 检查端口是否被占用
- 检查依赖是否安装完整

### 任务执行失败
- 查看节点日志
- 检查状态存储目录权限
- 使用 `runtime.get_state(execution_id)` 查看详细状态

### Ray 相关问题
- 确保 Ray 正确安装：`pip install ray[serve]`
- 检查 Ray 是否已初始化
- 本地模式可以绕过 Ray 问题

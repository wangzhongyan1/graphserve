"""
GraphServe HTTP Server 演示

这个示例展示如何将 Graph 作为一个持续运行的 HTTP 服务，
可以不断接收请求并执行 workflow。

运行方式:
    python examples/server_demo.py

然后可以用 curl 或浏览器发送请求:
    # 提交任务
    curl -X POST http://localhost:8000/execute \
        -H "Content-Type: application/json" \
        -d '{"data": {"name": "Alice"}}'
    
    # 查询状态
    curl http://localhost:8000/status/{execution_id}
    
    # 获取结果
    curl http://localhost:8000/result/{execution_id}
    
    # 列出所有执行
    curl http://localhost:8000/executions
    
    # 健康检查
    curl http://localhost:8000/health
"""

import asyncio
from graphserve import Graph, serve_graph, ServeConfig

# 创建一个简单的 workflow
graph = Graph("echo_service")

@graph.node(cpus=0.5, memory=256)
async def process(state):
    """处理输入数据"""
    import os
    
    name = state.data.get("name", "World")
    message = state.data.get("message", "Hello")
    
    # 模拟一些处理时间
    await asyncio.sleep(0.5)
    
    return {
        "processed": True,
        "result": f"{message}, {name}!",
        "pid": os.getpid(),  # 返回执行进程的 PID，用于验证资源隔离
    }

@graph.node(cpus=0.5, memory=256)
async def format_output(state):
    """格式化输出"""
    result = state.data.get("result", "")
    return {
        "final_output": result.upper(),
        "timestamp": str(asyncio.get_event_loop().time()),
    }

graph.add_edge("process", "format_output")
graph.set_start("process")
graph.set_end("format_output")


if __name__ == "__main__":
    print("=" * 60)
    print("GraphServe HTTP Server Demo")
    print("=" * 60)
    print("\n服务启动后，可以通过以下方式访问:")
    print("  - API 文档: http://localhost:8000/docs")
    print("  - 健康检查: http://localhost:8000/health")
    print("  - 图信息: http://localhost:8000/graph")
    print("\n按 Ctrl+C 停止服务")
    print("=" * 60)
    
    # 启动 HTTP 服务
    config = ServeConfig(
        host="0.0.0.0",
        port=8000,
        storage_dir="./server_states",
    )
    
    serve_graph(graph, config=config)

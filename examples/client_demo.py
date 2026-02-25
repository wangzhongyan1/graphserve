"""
GraphServe 客户端演示 - 持续发送消息

这个脚本演示如何像一个服务一样持续给 Graph 发送消息。

使用方式:
1. 先启动 server: python examples/server_demo.py
2. 然后运行客户端: python examples/client_demo.py
"""

import asyncio
import aiohttp
import random
import time


BASE_URL = "http://localhost:8000"


async def submit_task(session: aiohttp.ClientSession, name: str, message: str) -> str:
    """提交一个任务，返回 execution_id"""
    async with session.post(
        f"{BASE_URL}/execute",
        json={"data": {"name": name, "message": message}}
    ) as resp:
        result = await resp.json()
        return result["execution_id"]


async def wait_for_result(session: aiohttp.ClientSession, execution_id: str, timeout: float = 30) -> dict:
    """等待任务完成并获取结果"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        async with session.get(f"{BASE_URL}/status/{execution_id}") as resp:
            status = await resp.json()
            
            if status["status"] == "completed":
                # 获取结果
                async with session.get(f"{BASE_URL}/result/{execution_id}") as result_resp:
                    return await result_resp.json()
            elif status["status"] == "failed":
                return {"error": "Task failed", "status": status}
        
        await asyncio.sleep(0.1)
    
    return {"error": "Timeout"}


async def send_single_request(session: aiohttp.ClientSession, request_id: int):
    """发送单个请求"""
    names = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
    messages = ["Hello", "Hi", "Hey", "Greetings", "Welcome"]
    
    name = random.choice(names)
    message = random.choice(messages)
    
    print(f"[{request_id:3d}] 发送请求: name={name}, message={message}")
    
    # 提交任务
    execution_id = await submit_task(session, name, message)
    print(f"[{request_id:3d}] 提交成功: execution_id={execution_id[:8]}...")
    
    # 等待结果
    result = await wait_for_result(session, execution_id)
    
    if "error" in result and result["error"] != None:
        print(f"[{request_id:3d}] 失败: {result['error']}, result: {result}")
    else:
        data = result.get("data", {})
        print(f"[{request_id:3d}] 完成: {data.get('final_output', 'N/A')[:30]}... (PID={data.get('pid', 'N/A')})")
    
    return result


async def continuous_requests(count: int = 10, concurrency: int = 3):
    """持续发送多个请求"""
    print(f"\n持续发送 {count} 个请求，并发数 {concurrency}")
    print("=" * 60)
    
    async with aiohttp.ClientSession() as session:
        # 先检查服务是否健康
        try:
            async with session.get(f"{BASE_URL}/health") as resp:
                health = await resp.json()
                print(f"服务健康: {health}")
        except Exception as e:
            print(f"服务未启动或无法连接: {e}")
            print(f"请先运行: python examples/server_demo.py")
            return
        
        # 获取图信息
        async with session.get(f"{BASE_URL}/graph") as resp:
            graph_info = await resp.json()
            print(f"\n图名称: {graph_info['name']}")
            print(f"节点数: {len(graph_info['nodes'])}")
            for node in graph_info['nodes']:
                print(f"  - {node['name']}: {node['resources']['cpus']} CPUs")
        
        print()
        
        # 使用信号量控制并发
        semaphore = asyncio.Semaphore(concurrency)
        
        async def bounded_request(request_id: int):
            async with semaphore:
                return await send_single_request(session, request_id)
        
        # 并发发送所有请求
        start_time = time.time()
        results = await asyncio.gather(*[
            bounded_request(i) for i in range(count)
        ])
        elapsed = time.time() - start_time
        
        # 统计结果
        success_count = sum(1 for r in results if "error" not in r)
        print(f"\n{'='*60}")
        print(f"完成: {success_count}/{count} 成功, 耗时 {elapsed:.2f}s")
        print(f"平均每个请求: {elapsed/count:.2f}s")
        
        # 列出所有执行
        async with session.get(f"{BASE_URL}/executions") as resp:
            executions = await resp.json()
            print(f"服务器上总执行数: {executions['count']}")


async def interactive_mode():
    """交互模式 - 持续接收用户输入"""
    print("\n" + "=" * 60)
    print("交互模式 - 输入 name 和 message (输入 'quit' 退出)")
    print("=" * 60)
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                name = input("\nName (default: World): ").strip()
                if name.lower() == 'quit':
                    break
                if not name:
                    name = "World"
                
                message = input("Message (default: Hello): ").strip()
                if message.lower() == 'quit':
                    break
                if not message:
                    message = "Hello"
                
                print(f"发送: {message}, {name}!")
                
                execution_id = await submit_task(session, name, message)
                print(f"已提交，等待结果...")
                
                result = await wait_for_result(session, execution_id)
                
                if "error" in result and result["error"] != None:
                    print(f"失败: {result['error']}, result: {result}")
                else:
                    data = result.get("data", {})
                    print(f"结果: {data.get('final_output')}")
                    print(f"执行节点: {result.get('completed_nodes', [])}")
                    
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"错误: {e}")
    
    print("\n退出交互模式")


async def main():
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--interactive":
        await interactive_mode()
    else:
        # 默认模式：批量发送请求
        await continuous_requests(count=10, concurrency=3)
        
        # 询问是否进入交互模式
        print("\n是否进入交互模式? (y/n): ", end="")
        try:
            response = input().strip().lower()
            if response == 'y':
                await interactive_mode()
        except EOFError:
            pass


if __name__ == "__main__":
    # 检查 aiohttp 是否安装
    try:
        import aiohttp
    except ImportError:
        print("请先安装 aiohttp: pip install aiohttp")
        exit(1)
    
    asyncio.run(main())

"""
GraphServe 简单客户端 - 使用标准库 requests

安装依赖:
    pip install requests

使用方式:
    python examples/client_simple.py
"""

import requests
import time
import sys

BASE_URL = "http://localhost:8000"


def submit_and_wait(name: str, message: str) -> dict:
    """提交任务并等待结果"""
    # 提交任务
    resp = requests.post(
        f"{BASE_URL}/execute",
        json={"data": {"name": name, "message": message}}
    )
    result = resp.json()
    execution_id = result["execution_id"]
    print(f"  提交成功: {execution_id[:8]}...")
    
    # 等待完成
    while True:
        resp = requests.get(f"{BASE_URL}/status/{execution_id}")
        status = resp.json()
        
        if status["status"] == "completed":
            resp = requests.get(f"{BASE_URL}/result/{execution_id}")
            return resp.json()
        elif status["status"] == "failed":
            return {"error": "Failed"}
        
        time.sleep(0.1)


def main():
    print("=" * 60)
    print("GraphServe 简单客户端")
    print("=" * 60)
    
    # 检查服务
    try:
        resp = requests.get(f"{BASE_URL}/health")
        print(f"服务健康: {resp.json()['status']}")
    except Exception as e:
        print(f"无法连接服务: {e}")
        print("请先运行: python examples/server_demo.py")
        return
    
    # 获取图信息
    resp = requests.get(f"{BASE_URL}/graph")
    graph = resp.json()
    print(f"\n图: {graph['name']}")
    print(f"节点: {[n['name'] for n in graph['nodes']]}")
    
    # 持续发送请求
    print("\n" + "=" * 60)
    print("开始发送请求 (按 Ctrl+C 停止)")
    print("=" * 60)
    
    count = 0
    try:
        while True:
            count += 1
            name = f"User{count}"
            message = "Hello"
            
            print(f"\n[{count}] 发送: {message}, {name}!")
            start = time.time()
            
            result = submit_and_wait(name, message)
            
            elapsed = time.time() - start
            if "error" in result:
                print(f"[{count}] 失败: {result['error']}")
            else:
                data = result.get("data", {})
                print(f"[{count}] 成功 ({elapsed:.2f}s): {data.get('final_output')}")
                print(f"       执行进程 PID: {data.get('pid')}")
            
            # 每秒发送一个
            time.sleep(1)
            
    except KeyboardInterrupt:
        print(f"\n\n已发送 {count} 个请求")


if __name__ == "__main__":
    try:
        import requests
    except ImportError:
        print("请先安装 requests: pip install requests")
        sys.exit(1)
    
    main()

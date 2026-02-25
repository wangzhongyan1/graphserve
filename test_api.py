"""测试 API 是否正常工作"""
import requests
import time

BASE_URL = "http://localhost:8000"

def wait_for_service(max_wait=30):
    """等待服务启动"""
    print("等待服务启动...")
    for i in range(max_wait):
        try:
            resp = requests.get(f"{BASE_URL}/health", timeout=1)
            if resp.status_code == 200:
                print(f"  服务已启动 (等待了 {i+1} 秒)")
                return True
        except:
            pass
        time.sleep(1)
    return False

def test_health():
    """测试健康检查"""
    print("\n测试 /health...")
    try:
        resp = requests.get(f"{BASE_URL}/health", timeout=5)
        print(f"  状态码: {resp.status_code}")
        print(f"  响应: {resp.json()}")
        return resp.status_code == 200
    except Exception as e:
        print(f"  错误: {e}")
        return False

def test_graph():
    """测试图信息"""
    print("\n测试 /graph...")
    try:
        resp = requests.get(f"{BASE_URL}/graph", timeout=5)
        print(f"  状态码: {resp.status_code}")
        print(f"  响应: {resp.json()}")
        return resp.status_code == 200
    except Exception as e:
        print(f"  错误: {e}")
        return False

def test_execute():
    """测试执行任务"""
    print("\n测试 /execute...")
    try:
        resp = requests.post(
            f"{BASE_URL}/execute",
            json={"data": {"name": "Test", "message": "Hello"}},
            timeout=5
        )
        print(f"  状态码: {resp.status_code}")
        print(f"  响应: {resp.json()}")
        return resp.status_code == 200, resp.json().get("execution_id")
    except Exception as e:
        print(f"  错误: {e}")
        return False, None

def test_status(execution_id):
    """测试查询状态"""
    print(f"\n测试 /status/{execution_id[:8]}...")
    try:
        resp = requests.get(f"{BASE_URL}/status/{execution_id}", timeout=5)
        print(f"  状态码: {resp.status_code}")
        print(f"  响应: {resp.json()}")
        return resp.status_code == 200
    except Exception as e:
        print(f"  错误: {e}")
        return False

def test_result(execution_id):
    """测试结果"""
    print(f"\n测试 /result/{execution_id[:8]}...")
    try:
        resp = requests.get(f"{BASE_URL}/result/{execution_id}", timeout=5)
        print(f"  状态码: {resp.status_code}")
        print(f"  响应: {resp.json()}")
        return resp.status_code == 200
    except Exception as e:
        print(f"  错误: {e}")
        return False

def main():
    print("=" * 60)
    print("测试 GraphServe API")
    print("=" * 60)
    
    # 等待服务启动
    if not wait_for_service():
        print("\n服务未启动，请先运行: python examples/server_demo.py")
        return
    
    # 测试健康检查
    if not test_health():
        print("健康检查失败")
        return
    
    # 测试图信息
    test_graph()
    
    # 测试执行任务
    success, execution_id = test_execute()
    if not success or not execution_id:
        print("执行任务失败")
        return
    
    # 等待任务完成
    print("\n等待任务完成...")
    time.sleep(2)
    
    # 测试查询状态
    test_status(execution_id)
    
    # 测试结果
    test_result(execution_id)
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)

if __name__ == "__main__":
    main()

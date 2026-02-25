"""
简化版验证脚本 - 确认 Ray 是否真正被使用

关键验证点：
1. 如果 local_mode=True：所有节点在主进程执行，PID 相同
2. 如果 local_mode=False：每个节点在独立的 Ray Actor 执行，PID 不同
"""

import asyncio
import os
from graphserve import Graph, Runtime

graph = Graph("test")

@graph.node(cpus=0.5, gpus=0, memory=256, min_replicas=1, max_replicas=1)
async def step1(state):
    import os
    pid = os.getpid()
    print(f"  [step1] PID={pid}")
    return {"step1_pid": pid}

@graph.node(cpus=0.5, gpus=0, memory=256, min_replicas=1, max_replicas=1)
async def step2(state):
    import os
    pid = os.getpid()
    # state 是 dict 类型（因为远程调用传递的是 state.data）
    prev_pid = state.get("step1_pid") if isinstance(state, dict) else getattr(state, 'data', {}).get("step1_pid")
    print(f"  [step2] PID={pid}, step1 PID={prev_pid}, 不同={pid != prev_pid}")
    return {"step2_pid": pid, "pid_changed": pid != prev_pid}

graph.add_edge("step1", "step2").set_start("step1").set_end("step2")


async def test_mode(local_mode: bool, name: str):
    print(f"\n{'='*50}")
    print(f"测试模式: {name}")
    print(f"{'='*50}")
    
    runtime = Runtime(graph)
    await runtime.deploy(local_mode=local_mode)
    
    print(f"  local_mode: {runtime._local_mode}")
    print(f"  使用 Ray: {not runtime._local_mode and len(runtime._deployments) > 0}")
    
    execution_id = await runtime.submit({})
    result = await runtime.get_result(execution_id, timeout=30)
    
    if result and result.status == "completed":
        pid1 = result.data.get("step1_pid")
        pid2 = result.data.get("step2_pid")
        changed = result.data.get("pid_changed")
        
        print(f"\n  结果:")
        print(f"    step1 PID: {pid1}")
        print(f"    step2 PID: {pid2}")
        
        if changed:
            print(f"    ✓ PID 不同 - 资源隔离生效！")
        else:
            print(f"    ✗ PID 相同 - 在同一进程执行")
    else:
        print(f"  执行失败: {result.status if result else 'None'}")
    
    await runtime.shutdown()


async def main():
    # 测试 local_mode=True（不使用 Ray）
    await test_mode(local_mode=True, name="Local Mode（不使用 Ray）")
    
    # 测试 local_mode=False（使用 Ray）
    await test_mode(local_mode=False, name="Ray Mode（使用 Ray Serve）")
    
    print(f"\n{'='*50}")
    print("结论:")
    print(f"{'='*50}")
    print("- Local Mode: 所有节点在主进程执行，PID 相同")
    print("- Ray Mode: 每个节点在独立的 Ray Actor 执行，PID 不同")
    print("- 资源隔离只在 Ray Mode 下生效")


if __name__ == "__main__":
    asyncio.run(main())

"""
Hello World Demo - GraphServe Runtime
The simplest possible workflow in under 20 lines of effective code.
"""

import asyncio
from graphserve import Graph, Runtime

# Create a graph
graph = Graph("hello")

@graph.node(cpus=0.5)
async def greet(state):
    name = state.get("name", "World")
    return {"message": f"Hello, {name}!"}

@graph.node(cpus=0.5)
async def farewell(state):
    msg = state.get("message", "")
    return {"final": f"{msg} Goodbye!"}

graph.add_edge("greet", "farewell").set_start("greet").set_end("farewell")

async def main():
    runtime = await Runtime(graph).deploy(local_mode=False)
    execution_id = await runtime.submit({"name": "GraphServe"})
    result = await runtime.get_result(execution_id)
    print(f"Result: {result.data}")
    await runtime.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
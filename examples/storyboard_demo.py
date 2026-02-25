"""
Storyboard Generation Demo - Full Feature Showcase

This demo shows a complete AI workflow for generating storyboards from novels.
Demonstrates:
- Different resource allocations per node (GPU/CPU)
- Dynamic auto-scaling with min/max replicas
- Conditional routing based on content analysis
- State persistence and recovery

Scenario: Process a novel chapter and generate:
1. Scene analysis (GPU - text understanding)
2. Character extraction (CPU - parallel processing)
3. Background description (CPU)
4. Image generation routing (GPU - conditional)
5. Storyboard assembly (CPU)
"""

import asyncio
import random
from typing import Dict, Any, List
from graphserve import Graph, Runtime, PersistentStateStore, StateManager


# Create the storyboard generation graph
graph = Graph("storyboard_generator")


@graph.node(
    name="scene_analyzer",
    cpus=2,
    gpus=1,
    memory=4096,
    min_replicas=1,
    max_replicas=5,
    target_concurrency=2.0,
    description="Analyze novel scenes using LLM (GPU required)"
)
async def scene_analyzer(state):
    """Analyze scenes from novel text using GPU-accelerated LLM."""
    novel_text = state.data.get("novel_text", "")
    
    # Simulate GPU processing
    await asyncio.sleep(0.1)
    
    scenes = [
        {"id": 1, "type": "dialogue", "characters": ["Alice", "Bob"]},
        {"id": 2, "type": "action", "characters": ["Alice"]},
        {"id": 3, "type": "description", "characters": []},
    ]
    
    return {
        "scenes": scenes,
        "scene_count": len(scenes),
        "analysis_complete": True
    }


@graph.node(
    name="character_extractor",
    cpus=4,
    gpus=0,
    memory=2048,
    min_replicas=2,
    max_replicas=20,
    target_concurrency=5.0,
    description="Extract and analyze characters (CPU parallel processing)"
)
async def character_extractor(state):
    """Extract characters from scenes - high parallelism on CPU."""
    scenes = state.data.get("scenes", [])
    
    await asyncio.sleep(0.05)
    
    characters = set()
    for scene in scenes:
        characters.update(scene.get("characters", []))
    
    return {
        "characters": list(characters),
        "character_count": len(characters),
        "character_details": {
            name: {"appearances": random.randint(1, 5)}
            for name in characters
        }
    }


@graph.node(
    name="background_analyzer",
    cpus=2,
    gpus=0,
    memory=1024,
    min_replicas=1,
    max_replicas=10,
    target_concurrency=3.0,
    description="Analyze background settings and environments"
)
async def background_analyzer(state):
    """Analyze background settings from scenes."""
    scenes = state.data.get("scenes", [])
    
    await asyncio.sleep(0.05)
    
    backgrounds = [f"background_{i}" for i in range(len(scenes))]
    
    return {
        "backgrounds": backgrounds,
        "setting_type": random.choice(["urban", "fantasy", "scifi", "historical"])
    }


@graph.node(
    name="character_portrait_gen",
    cpus=1,
    gpus=1,
    memory=8192,
    min_replicas=1,
    max_replicas=8,
    target_concurrency=1.0,
    description="Generate character portraits using Stable Diffusion (GPU intensive)"
)
async def character_portrait_gen(state):
    """Generate character portraits - GPU intensive image generation."""
    characters = state.data.get("characters", [])
    
    await asyncio.sleep(0.1)
    
    portraits = {
        char: f"/images/portrait_{char.lower()}.png"
        for char in characters
    }
    
    return {
        "portraits": portraits,
        "generated_images": len(portraits)
    }


@graph.node(
    name="scene_background_gen",
    cpus=1,
    gpus=1,
    memory=8192,
    min_replicas=1,
    max_replicas=6,
    target_concurrency=1.0,
    description="Generate scene backgrounds (GPU intensive)"
)
async def scene_background_gen(state):
    """Generate scene background images."""
    scenes = state.data.get("scenes", [])
    setting = state.data.get("setting_type", "generic")
    
    await asyncio.sleep(0.1)
    
    backgrounds = {
        f"scene_{scene['id']}": f"/images/bg_{setting}_{scene['id']}.png"
        for scene in scenes
    }
    
    return {
        "scene_backgrounds": backgrounds,
        "generated_backgrounds": len(backgrounds)
    }


@graph.node(
    name="action_sequence_gen",
    cpus=2,
    gpus=1,
    memory=12288,
    min_replicas=1,
    max_replicas=4,
    target_concurrency=0.5,
    description="Generate action sequence frames (High GPU memory)"
)
async def action_sequence_gen(state):
    """Generate action sequence frames - highest GPU requirements."""
    scenes = state.data.get("scenes", [])
    
    # Find action scenes
    action_scenes = [s for s in scenes if s.get("type") == "action"]
    
    await asyncio.sleep(0.1)
    
    action_frames = {
        f"action_{scene['id']}": [f"/images/action_{scene['id']}_frame_{i}.png" for i in range(3)]
        for scene in action_scenes
    }
    
    return {
        "action_frames": action_frames,
        "action_scene_count": len(action_scenes)
    }


@graph.node(
    name="storyboard_assembler",
    cpus=2,
    gpus=0,
    memory=2048,
    min_replicas=1,
    max_replicas=5,
    target_concurrency=2.0,
    description="Assemble final storyboard from all generated assets"
)
async def storyboard_assembler(state):
    """Assemble all generated assets into final storyboard."""
    portraits = state.data.get("portraits", {})
    backgrounds = state.data.get("scene_backgrounds", {})
    action_frames = state.data.get("action_frames", {})
    scenes = state.data.get("scenes", [])
    
    await asyncio.sleep(0.05)
    
    storyboard = {
        "title": state.data.get("title", "Untitled"),
        "total_scenes": len(scenes),
        "assets": {
            "portraits": portraits,
            "backgrounds": backgrounds,
            "action_frames": action_frames,
        },
        "pages": [
            {
                "scene_id": scene["id"],
                "type": scene["type"],
                "background": backgrounds.get(f"scene_{scene['id']}"),
                "characters": [portraits.get(c) for c in scene.get("characters", [])],
                "action_frames": action_frames.get(f"action_{scene['id']}", [])
            }
            for scene in scenes
        ]
    }
    
    return {
        "storyboard": storyboard,
        "assembly_complete": True,
        "total_assets": len(portraits) + len(backgrounds) + sum(len(v) for v in action_frames.values())
    }


# Define routing logic - sequential for demo
def route_after_analysis(state):
    """Route to character extraction after scene analysis."""
    return "character_extractor"


def route_after_character(state):
    """Route to portrait generation after character extraction."""
    return "character_portrait_gen"


def route_after_portrait(state):
    """Route to background generation after portraits."""
    return "scene_background_gen"


def route_after_background(state):
    """Route to action generation after backgrounds."""
    return "action_sequence_gen"


def route_after_action(state):
    """Route to final assembly."""
    return "storyboard_assembler"


# Build the graph edges (sequential for demo)
graph.add_edge("scene_analyzer", condition=route_after_analysis)
graph.add_edge("character_extractor", condition=route_after_character)
graph.add_edge("character_portrait_gen", condition=route_after_portrait)
graph.add_edge("scene_background_gen", condition=route_after_background)
graph.add_edge("action_sequence_gen", condition=route_after_action)

# Set start and end
graph.set_start("scene_analyzer")
graph.set_end("storyboard_assembler")


async def main():
    """Run the storyboard generation demo."""
    print("=" * 60)
    print("Storyboard Generation Demo - GraphServe Runtime")
    print("=" * 60)
    
    # Display graph structure
    print("\n📊 Graph Structure:")
    print(graph.visualize())
    
    # Create runtime with persistent state
    state_manager = StateManager(PersistentStateStore("./storyboard_states"))
    runtime = Runtime(graph, state_manager)
    
    # Deploy (local mode for demo)
    print("\n🚀 Deploying graph...")
    await runtime.deploy(local_mode=True)
    
    print(f"\n✅ Graph deployed!")
    print(f"   Runtime stats: {runtime.get_stats()}")
    
    # Submit a storyboard generation job
    print("\n📖 Submitting storyboard generation job...")
    novel_input = {
        "title": "The Adventure Begins",
        "novel_text": "Alice met Bob in the old castle. Suddenly, a dragon appeared!",
    }
    
    execution_id = await runtime.submit(novel_input)
    print(f"   Execution ID: {execution_id}")
    
    # Wait for completion
    print("\n⏳ Waiting for completion...")
    result = await runtime.get_result(execution_id, timeout=30)
    
    if result:
        print(f"\n✅ Execution completed!")
        print(f"   Status: {result.status}")
        print(f"   Visited nodes: {result.visited_nodes}")
        print(f"   Completed nodes: {result.completed_nodes}")
        
        if result.data.get("storyboard"):
            storyboard = result.data["storyboard"]
            print(f"\n📋 Storyboard Summary:")
            print(f"   Title: {storyboard['title']}")
            print(f"   Total scenes: {storyboard['total_scenes']}")
            print(f"   Total assets: {result.data.get('total_assets', 0)}")
            print(f"\n   Assets generated:")
            print(f"     - Portraits: {len(storyboard['assets']['portraits'])}")
            print(f"     - Backgrounds: {len(storyboard['assets']['backgrounds'])}")
            print(f"     - Action frames: {sum(len(v) for v in storyboard['assets']['action_frames'].values())}")
    else:
        print("\n❌ Execution timed out or failed")
    
    # Show final stats
    print(f"\n📊 Final Runtime Stats:")
    print(f"   {runtime.get_stats()}")
    
    # Cleanup
    await runtime.shutdown()
    print("\n✨ Demo complete!")


if __name__ == "__main__":
    asyncio.run(main())
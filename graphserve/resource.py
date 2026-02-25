"""
Resource management and auto-scaling for GraphServe Runtime.

Integrates with Ray's autoscaling capabilities.
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import asyncio
import logging
import time
from collections import deque

logger = logging.getLogger(__name__)


@dataclass
class NodeMetrics:
    """Metrics for a node."""
    node_name: str
    timestamp: float
    
    # Request metrics
    requests_total: int = 0
    requests_pending: int = 0
    requests_processing: int = 0
    
    # Latency metrics (in seconds)
    latency_avg: float = 0.0
    latency_p50: float = 0.0
    latency_p99: float = 0.0
    
    # Error metrics
    errors_total: int = 0
    error_rate: float = 0.0
    
    # Resource metrics
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    gpu_usage: float = 0.0
    
    # Replica metrics
    current_replicas: int = 0
    target_replicas: int = 0


class MetricsCollector:
    """Collects metrics for nodes."""
    
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self._metrics: Dict[str, deque] = {}
        self._lock = asyncio.Lock()
    
    async def record_request(
        self,
        node_name: str,
        latency: float,
        success: bool = True
    ):
        """Record a request metric."""
        async with self._lock:
            if node_name not in self._metrics:
                self._metrics[node_name] = deque(maxlen=self.window_size)
            
            self._metrics[node_name].append({
                "timestamp": time.time(),
                "latency": latency,
                "success": success,
            })
    
    async def get_metrics(self, node_name: str) -> Optional[NodeMetrics]:
        """Get aggregated metrics for a node."""
        async with self._lock:
            if node_name not in self._metrics:
                return None
            
            records = list(self._metrics[node_name])
            if not records:
                return None
            
            # Calculate metrics
            latencies = [r["latency"] for r in records]
            successes = [r["success"] for r in records]
            
            return NodeMetrics(
                node_name=node_name,
                timestamp=time.time(),
                requests_total=len(records),
                latency_avg=sum(latencies) / len(latencies),
                latency_p50=sorted(latencies)[len(latencies) // 2],
                latency_p99=sorted(latencies)[int(len(latencies) * 0.99)],
                errors_total=sum(1 for s in successes if not s),
                error_rate=sum(1 for s in successes if not s) / len(successes),
            )


class AutoScaler:
    """Auto-scaler for nodes based on metrics.
    
    Uses Ray's autoscaling capabilities with custom metrics.
    """
    
    def __init__(
        self,
        metrics_collector: MetricsCollector,
        check_interval: float = 30.0,
        scale_up_threshold: float = 0.8,
        scale_down_threshold: float = 0.3,
    ):
        self.metrics_collector = metrics_collector
        self.check_interval = check_interval
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold
        
        self._running = False
        self._task: Optional[asyncio.Task] = None
    
    async def start(self):
        """Start the auto-scaler."""
        self._running = True
        self._task = asyncio.create_task(self._scaling_loop())
        logger.info("Auto-scaler started")
    
    async def stop(self):
        """Stop the auto-scaler."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Auto-scaler stopped")
    
    async def _scaling_loop(self):
        """Main scaling loop."""
        while self._running:
            try:
                await self._check_and_scale()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Scaling error: {e}")
                await asyncio.sleep(self.check_interval)
    
    async def _check_and_scale(self):
        """Check metrics and scale nodes."""
        # This would integrate with Ray's autoscaling API
        # For now, just log metrics
        pass


class ResourceManager:
    """Manages resources for the graph.
    
    Coordinates:
    - Resource allocation
    - Metrics collection
    - Auto-scaling
    """
    
    def __init__(
        self,
        enable_autoscaling: bool = True,
        metrics_window: int = 100,
    ):
        self.metrics = MetricsCollector(window=metrics_window)
        self.autoscaler: Optional[AutoScaler] = None
        
        if enable_autoscaling:
            self.autoscaler = AutoScaler(self.metrics)
    
    async def start(self):
        """Start resource management."""
        if self.autoscaler:
            await self.autoscaler.start()
    
    async def stop(self):
        """Stop resource management."""
        if self.autoscaler:
            await self.autoscaler.stop()
    
    async def record_request(
        self,
        node_name: str,
        latency: float,
        success: bool = True
    ):
        """Record a request metric."""
        await self.metrics.record_request(node_name, latency, success)
    
    def get_node_resources(self, node_name: str) -> Dict[str, Any]:
        """Get resource allocation for a node."""
        # This would query Ray's resource API
        return {
            "node": node_name,
            "allocated": {},
            "used": {},
        }
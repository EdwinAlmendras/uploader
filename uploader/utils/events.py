from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Callable
import asyncio
import logging
logger = logging.getLogger(__name__)

@dataclass
class FileProgress:
    """Progress information for a single file."""
    filename: str
    file_path: Path
    bytes_uploaded: int = 0
    total_bytes: int = 0
    percent: float = 0.0
    status: str = "pending"  # pending, uploading, analyzing, completed, failed


class EventEmitter:
    """Simple event emitter for upload events."""
    
    def __init__(self):
        self._listeners: Dict[str, List[Callable]] = {}
        self._lock = asyncio.Lock()
    
    def on(self, event_name: str, callback: Callable):
        """Subscribe to an event."""
        if event_name not in self._listeners:
            self._listeners[event_name] = []
        if callback not in self._listeners[event_name]:
            self._listeners[event_name].append(callback)
    
    def off(self, event_name: str, callback: Callable):
        """Unsubscribe from an event."""
        if event_name in self._listeners:
            if callback in self._listeners[event_name]:
                self._listeners[event_name].remove(callback)
    
    async def emit(self, event_name: str, *args, **kwargs):
        """Emit an event to all listeners."""
        if event_name not in self._listeners:
            return
        
        async with self._lock:
            for callback in self._listeners[event_name][:]:  # Copy list to avoid modification during iteration
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(*args, **kwargs)
                    else:
                        callback(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Error in event listener for {event_name}: {e}")
    
    def _emit_async(self, event_name: str, *args, **kwargs):
        """Emit an event asynchronously without blocking."""
        try:
            loop = asyncio.get_running_loop()
            asyncio.create_task(self.emit(event_name, *args, **kwargs))
        except RuntimeError:
            # If no loop is running, create a new one
            asyncio.run(self.emit(event_name, *args, **kwargs))

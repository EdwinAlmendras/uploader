"""Parallel upload utilities."""
def get_parallel_count(avg_size: float) -> int:
    """
    Get optimal parallel upload count based on average file size.
    
    Small files benefit from more parallelism.
    Large files already use 21 connections each, so limit parallelism.
    """
    MB = 1024 * 1024
    
    if avg_size < 1 * MB:
        return 10  # Small files: high parallelism
    elif avg_size < 10 * MB:
        return 6   # Medium files: moderate parallelism
    else:
        return 3   # Large files: low parallelism (each uses 21 connections)


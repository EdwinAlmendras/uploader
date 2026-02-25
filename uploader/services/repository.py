"""Backward-compatible repository exports.

This module is kept as a thin compatibility layer after splitting responsibilities
into dedicated modules:
- uploader.services.api_client
- uploader.services.metadata_mapper
- uploader.services.metadata_gateway
"""

from .api_client import HTTPAPIClient
from .metadata_gateway import MetadataRepository

__all__ = ["MetadataRepository", "HTTPAPIClient"]

"""
Metadata Repository - Single Responsibility: persist metadata to API.

Implements Repository Pattern for data access.
Supports batch operations for efficient bulk uploads.
"""
from typing import Dict, Any, Optional, List
from datetime import datetime
import httpx

from ..protocols import IMetadataRepository, IAPIClient
from ..models import SocialInfo, TelegramInfo


class MetadataRepository(IMetadataRepository):
    """
    Repository for saving metadata to datastore API.
    
    Implements Repository Pattern - abstracts data persistence.
    """
    
    def __init__(self, api_client: IAPIClient):
        """
        Initialize repository.
        
        Args:
            api_client: HTTP client for API calls
        """
        self._api = api_client
    
    @staticmethod
    def _to_iso(value) -> Optional[str]:
        """Convert datetime to ISO string, or return string as-is."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)
    
    async def save_document(self, data: Dict[str, Any]) -> None:
        """
        Save base document.
        
        Args:
            data: Document data from analyzer
        """
        await self._api.post("/documents", json={
            "source_id": data["source_id"],
            "sha256sum": data["sha256sum"],
            "filename": data["filename"],
            "mimetype": data["mimetype"],
            "mtime": data["mtime"].isoformat() if data.get("mtime") else None,
            "ctime": data["ctime"].isoformat() if data.get("ctime") else None,
        })
    
    async def save_photo_metadata(self, document_id: str, data: Dict[str, Any]) -> None:
        """
        Save photo technical metadata.
        
        Args:
            document_id: Reference to document
            data: Photo metadata from analyzer
        """
        await self._api.post("/photos", json={
            "document_id": document_id,
            "width": data.get("width"),
            "height": data.get("height"),
            "camera": data.get("camera"),
            "orientation": data.get("orientation"),
            "creation_date": self._to_iso(data.get("creation_date")),
            "exif": data.get("tags") or data.get("exif") or {},
        })
    
    async def save_video_metadata(self, document_id: str, data: Dict[str, Any]) -> None:
        """
        Save video technical metadata.
        
        Args:
            document_id: Reference to document
            data: Video metadata from analyzer
        """
        await self._api.post("/video_metadata", json={
            "document_id": document_id,
            # Dimensions
            "width": data.get("width"),
            "height": data.get("height"),
            "sar": data.get("sar"),
            "dar": data.get("dar"),
            # Timing
            "duration": data.get("duration"),
            "fps": data.get("fps"),
            # Container
            "container": data.get("container"),
            "bitrate": data.get("bitrate"),
            # Video codec
            "video_codec": data.get("video_codec"),
            "video_codec_long": data.get("video_codec_long"),
            "video_profile": data.get("video_profile"),
            "video_level": data.get("video_level"),
            "pix_fmt": data.get("pix_fmt"),
            "color_space": data.get("color_space"),
            # Audio
            "audio_codec": data.get("audio_codec"),
            "audio_codec_long": data.get("audio_codec_long"),
            "audio_sample_rate": data.get("audio_sample_rate"),
            "audio_channels": data.get("audio_channels"),
            "audio_bitrate": data.get("audio_bitrate"),
            # Tags
            "tags": data.get("tags"),
            "creation_time": self._to_iso(data.get("creation_time")),
            "encoder": data.get("encoder"),
        })
    
    async def save_channel(self, data: Dict[str, Any]) -> None:
        """
        Save/update channel (upsert).
        
        Args:
            data: Channel data
        """
        await self._api.post("/channels", json=data)
    
    async def save_social_video(self, data: Dict[str, Any]) -> None:
        """
        Save social video metadata.
        
        Args:
            data: Social video data
        """
        await self._api.post("/social", json=data)
    
    async def save_social_info(self, document_id: str, social_info: SocialInfo) -> None:
        """
        Save complete social info (channel + social video).
        
        Convenience method that saves both channel and social video.
        
        Args:
            document_id: Reference to document
            social_info: Social metadata from yt-dlp
        """
        # Save channel if present
        if social_info.has_channel:
            await self.save_channel({
                "channel_id": social_info.channel_id,
                "platform": social_info.platform,
                "name": social_info.channel_name or "Unknown",
                "url": social_info.channel_url or "",
                "username": social_info.channel_username,
                "follower_count": social_info.channel_follower_count,
            })
        
        # Save social video
        await self.save_social_video({
            "document_id": document_id,
            "channel_id": social_info.channel_id or "unknown",
            "platform": social_info.platform,
            "video_id": social_info.video_id,
            "video_url": social_info.video_url,
            "title": social_info.title,
            "description": social_info.description,
            "view_count": social_info.view_count,
            "like_count": social_info.like_count,
            "comment_count": social_info.comment_count,
            "upload_date": social_info.upload_date_iso,
            "fetched_at": datetime.utcnow().isoformat(),
        })
    
    async def save_telegram(self, document_id: str, telegram_info: TelegramInfo) -> None:
        """
        Save Telegram document metadata.
        
        Args:
            document_id: Reference to document
            telegram_info: Telegram metadata
        """
        await self._api.post("/telegram", json={
            "document_id": document_id,
            "message_id": telegram_info.message_id,
            "chat_id": telegram_info.chat_id,
            "telegram_document_id": telegram_info.telegram_document_id,
            "upload_date": telegram_info.upload_date,
            "fetched_at": datetime.utcnow().isoformat(),
        })
    
    def prepare_telegram(self, document_id: str, telegram_info: TelegramInfo) -> Dict[str, Any]:
        """Prepare telegram metadata dict for batch."""
        return {
            "document_id": document_id,
            "message_id": telegram_info.message_id,
            "chat_id": telegram_info.chat_id,
            "telegram_document_id": telegram_info.telegram_document_id,
            "upload_date": telegram_info.upload_date,
            "fetched_at": datetime.utcnow().isoformat(),
        }
    
    # =========================================================================
    # Batch Operations
    # =========================================================================
    
    async def save_batch(self, items: List[Dict[str, Any]]) -> None:
        """
        Save batch of documents + metadata in single request.
        
        Args:
            items: List of dicts with:
                - document: base document data
                - video_metadata: (optional) video metadata
                - photo_metadata: (optional) photo metadata
        """
        await self._api.post("/batch", json={"items": items})
    
    def prepare_document(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare document dict for batch."""
        return {
            "source_id": data["source_id"],
            "sha256sum": data["sha256sum"],
            "filename": data["filename"],
            "mimetype": data["mimetype"],
            "mtime": data["mtime"].isoformat() if data.get("mtime") else None,
            "ctime": data["ctime"].isoformat() if data.get("ctime") else None,
        }
    
    def prepare_video_metadata(self, document_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare video metadata dict for batch."""
        return {
            "document_id": document_id,
            "width": data.get("width"),
            "height": data.get("height"),
            "sar": data.get("sar"),
            "dar": data.get("dar"),
            "duration": data.get("duration"),
            "fps": data.get("fps"),
            "container": data.get("container"),
            "bitrate": data.get("bitrate"),
            "video_codec": data.get("video_codec"),
            "video_codec_long": data.get("video_codec_long"),
            "video_profile": data.get("video_profile"),
            "video_level": data.get("video_level"),
            "pix_fmt": data.get("pix_fmt"),
            "color_space": data.get("color_space"),
            "audio_codec": data.get("audio_codec"),
            "audio_codec_long": data.get("audio_codec_long"),
            "audio_sample_rate": data.get("audio_sample_rate"),
            "audio_channels": data.get("audio_channels"),
            "audio_bitrate": data.get("audio_bitrate"),
            "tags": data.get("tags"),
            "creation_time": self._to_iso(data.get("creation_time")),
            "encoder": data.get("encoder"),
        }
    
    def prepare_photo_metadata(self, document_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare photo metadata dict for batch."""
        return {
            "document_id": document_id,
            "width": data.get("width"),
            "height": data.get("height"),
            "camera": data.get("camera"),
            "orientation": data.get("orientation"),
            "creation_date": self._to_iso(data.get("creation_date")),
            "exif": data.get("tags") or data.get("exif") or {},
        }


class HTTPAPIClient:
    """
    HTTP client adapter for API calls.
    
    Implements IAPIClient protocol.
    """
    
    def __init__(self, base_url: str, timeout: int = 30):
        self._base_url = base_url
        self._timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        self._client = httpx.AsyncClient(base_url=self._base_url, timeout=self._timeout)
        return self
    
    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()
    
    async def post(self, endpoint: str, json: Dict) -> Any:
        if not self._client:
            raise RuntimeError("HTTPAPIClient not initialized. Use 'async with' context.")
        
        response = await self._client.post(endpoint, json=json)
        
        if response.status_code >= 400:
            try:
                error_detail = response.json()
            except:
                error_detail = response.text
            raise RuntimeError(f"API error {response.status_code} on POST {endpoint}: {error_detail}")
        
        return response
    
    async def get(self, endpoint: str) -> Any:
        if not self._client:
            raise RuntimeError("HTTPAPIClient not initialized. Use 'async with' context.")
        
        response = await self._client.get(endpoint)
        
        if response.status_code >= 400:
            try:
                error_detail = response.json()
            except:
                error_detail = response.text
            raise RuntimeError(f"API error {response.status_code} on GET {endpoint}: {error_detail}")
        
        return response

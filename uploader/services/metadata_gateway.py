"""Metadata repository gateway backed by HTTP API."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from ..models import SocialInfo, TelegramInfo
from ..protocols import IAPIClient, IMetadataRepository
from .metadata_mapper import MetadataPayloadMapper


class MetadataRepository(IMetadataRepository):
    """
    Repository for saving metadata to datastore API.

    Implements Repository Pattern - abstracts data persistence.
    """

    def __init__(self, api_client: IAPIClient):
        self._api = api_client
        self._mapper = MetadataPayloadMapper()

    async def save_document(self, data: Dict[str, Any]) -> None:
        await self._api.post("/documents", json=self._mapper.document_payload(data))

    async def save_photo_metadata(self, document_id: str, data: Dict[str, Any]) -> None:
        await self._api.post(
            "/photos",
            json=self._mapper.photo_metadata_payload(document_id, data),
        )

    async def save_video_metadata(self, document_id: str, data: Dict[str, Any]) -> None:
        await self._api.post(
            "/video_metadata",
            json=self._mapper.video_metadata_payload(document_id, data),
        )

    async def save_channel(self, data: Dict[str, Any]) -> None:
        await self._api.post("/channels", json=data)

    async def save_social_video(self, data: Dict[str, Any]) -> None:
        await self._api.post("/social", json=data)

    async def save_social_info(self, document_id: str, social_info: SocialInfo) -> None:
        if social_info.has_channel:
            await self.save_channel(
                {
                    "channel_id": social_info.channel_id,
                    "platform": social_info.platform,
                    "name": social_info.channel_name or "Unknown",
                    "url": social_info.channel_url or "",
                    "username": social_info.channel_username,
                    "follower_count": social_info.channel_follower_count,
                }
            )

        await self.save_social_video(
            {
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
            }
        )

    async def save_telegram(self, document_id: str, telegram_info: TelegramInfo) -> None:
        await self._api.post(
            "/telegram",
            json=self._mapper.telegram_payload(document_id, telegram_info),
        )

    async def save_set_document(self, data: Dict[str, Any]) -> None:
        await self._api.post("/documents", json=self._mapper.set_document_payload(data))

    def prepare_telegram(self, document_id: str, telegram_info: TelegramInfo) -> Dict[str, Any]:
        return self._mapper.telegram_payload(document_id, telegram_info)

    async def lookup_by_blake3(self, blake3_list: List[str]) -> Dict[str, str]:
        if not blake3_list:
            return {}
        try:
            response = await self._api.post("/documents/lookup", json={"blake3_list": blake3_list})
            return response.json().get("found", {})
        except Exception:
            return {}

    async def check_exists_batch(self, blake3_list: List[str]) -> Dict[str, Dict[str, Any]]:
        if not blake3_list:
            return {}
        try:
            response = await self._api.post("/documents/check", json={"blake3_list": blake3_list})
            return response.json().get("exists", {})
        except Exception:
            return {}

    async def exists_by_source_id(self, source_id: str) -> bool:
        if not source_id:
            return False
        try:
            response = await self._api.get(f"/documents/{source_id}/exists")
            return response.json().get("exists", False)
        except Exception:
            return False

    async def save_batch(self, items: List[Dict[str, Any]]) -> None:
        await self._api.post("/batch", json={"items": items})

    def prepare_document(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._mapper.document_payload(data)

    def prepare_video_metadata(self, document_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._mapper.video_metadata_payload(document_id, data)

    def prepare_photo_metadata(self, document_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._mapper.photo_metadata_payload(document_id, data)

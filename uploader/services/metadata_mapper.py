"""Payload mappers for metadata API operations."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from ..models import TelegramInfo


class MetadataPayloadMapper:
    """Converts domain metadata to API payloads."""

    @staticmethod
    def to_iso(value) -> Optional[str]:
        """Convert datetime to ISO string, or return string as-is."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    @staticmethod
    def sanitize_value(value):
        """Convert non-JSON-serializable values to JSON-compatible types."""
        if value is None:
            return None
        if isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        if isinstance(value, dict):
            return {k: MetadataPayloadMapper.sanitize_value(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [MetadataPayloadMapper.sanitize_value(v) for v in value]
        try:
            return float(value)
        except (TypeError, ValueError):
            return str(value)

    @classmethod
    def sanitize_dict(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize dictionary for JSON serialization."""
        return {k: cls.sanitize_value(v) for k, v in data.items()}

    @classmethod
    def document_payload(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "source_id": data["source_id"],
            "filename": data["filename"],
            "mimetype": data["mimetype"],
            "mtime": data["mtime"].isoformat() if data.get("mtime") else None,
            "ctime": data["ctime"].isoformat() if data.get("ctime") else None,
        }
        if "blake3_hash" in data:
            payload["blake3_hash"] = data["blake3_hash"]
        if "set_doc_id" in data:
            payload["set_doc_id"] = data["set_doc_id"]
        return payload

    @classmethod
    def photo_metadata_payload(cls, document_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        exif_data = data.get("tags") or data.get("exif") or {}
        payload = {
            "document_id": document_id,
            "width": data.get("width"),
            "height": data.get("height"),
            "camera": data.get("camera"),
            "orientation": data.get("orientation"),
            "creation_date": cls.to_iso(data.get("creation_date")),
            "quality": data.get("quality"),
            "phash": data.get("phash"),
            "avg_color_lab": data.get("avg_color_lab"),
            "exif": cls.sanitize_dict(exif_data) if exif_data else {},
        }

        if "image_embedding" in data:
            embedding = data["image_embedding"]
            if isinstance(embedding, (list, tuple)):
                payload["image_embedding"] = list(embedding)
            else:
                try:
                    payload["image_embedding"] = (
                        embedding.tolist() if hasattr(embedding, "tolist") else list(embedding)
                    )
                except Exception:
                    pass
        return payload

    @classmethod
    def video_metadata_payload(cls, document_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "document_id": document_id,
            "width": data.get("width"),
            "height": data.get("height"),
            "rotation": data.get("rotation"),
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
            "tags": cls.sanitize_dict(data.get("tags")) if data.get("tags") else None,
            "creation_time": cls.to_iso(data.get("creation_time")),
            "encoder": data.get("encoder"),
        }

    @classmethod
    def set_document_payload(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "source_id": data["source_id"],
            "filename": data["filename"],
            "mimetype": data.get("mimetype", "application/x-7z-compressed"),
            "set_image_count": data.get("set_image_count", 0),
            "set_name": data.get("set_name", ""),
        }
        if "mtime" in data:
            payload["mtime"] = data["mtime"].isoformat() if data.get("mtime") else None
        if "ctime" in data:
            payload["ctime"] = data["ctime"].isoformat() if data.get("ctime") else None
        if "blake3_hash" in data:
            payload["blake3_hash"] = data["blake3_hash"]
        return payload

    @staticmethod
    def telegram_payload(document_id: str, telegram_info: TelegramInfo) -> Dict[str, Any]:
        return {
            "document_id": document_id,
            "message_id": telegram_info.message_id,
            "chat_id": telegram_info.chat_id,
            "telegram_document_id": telegram_info.telegram_document_id,
            "upload_date": telegram_info.upload_date,
            "fetched_at": datetime.utcnow().isoformat(),
        }

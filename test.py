import argparse
import asyncio
from telethon import TelegramClient
from telethon.tl.types import InputPeerChannel, DocumentAttributeFilename
from FastTelethonhelper import fast_download
API_ID = 33097292
API_HASH = "8d4eca5990b03b37609e0b84766a2323"
client = TelegramClient("downloader_session", API_ID, API_HASH)

async def download_messages(entity, num_files):
    async with client:
        async for msg in client.iter_messages(entity, limit=num_files):
            if msg.media:
                attributes = msg.media.document.attributes
                for attr in attributes:
                    if isinstance(attr, DocumentAttributeFilename):
                        file_name = attr.file_name
                        msg_info = await client.send_message(entity, f"Downloading {file_name}")
                        await fast_download(client, msg, msg_info)
                        

def main():
    parser = argparse.ArgumentParser(description="Telegram entity downloader")
    parser.add_argument('entity', help="Entity username, channel, or chat")
    parser.add_argument('num_files', type=int, help="Number of files to download")
    args = parser.parse_args()
    asyncio.run(download_messages(args.entity, args.num_files))

if __name__ == "__main__":
    main()
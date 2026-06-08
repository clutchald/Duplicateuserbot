import asyncio
import os
import re
from pyrogram import Client, filters
from pyrogram.types import Message
from motor.motor_asyncio import AsyncIOMotorClient
from DA_Koyeb.health import emit_positive_health
import logging

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

API_ID = int(os.environ.get("API_ID", "20342933"))
API_HASH = os.environ.get("API_HASH", "9233e5deebe6abfc9ba297a9678851be")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://RAJ:RAJ@outlook.itqomxm.mongodb.net/?appName=outlook")
SESSION_STRING = os.environ.get("SESSION_STRING", "BQFRwgAArMER9MTPuJo66dzdLGYNjxBk2OC-qrPpEgHcnSE7XazoBzwN2PCeXJALh-td4hMvMjrcyhLPbniNkaMTicj5z3NzoCl-1ocTG2aLKw7mzqHVo_gsIReSyD-SW3gdnjIY8VLIULISdz13RsdICSFZaYwvjWKOTQKTEg9b-d40n4qTLtEgi5cfSh3YPZW3rLBxMGr4MGE2yCbhgiUs8XS6Nz0rFcukb7wRfZ4OVj15hPPo5nePbiwXpkyqqudfA0t4abQgtn7_mDX-jm3JAxGA9Rxune-kcwjuUXrV_jWNh1IRFKP94LbPtKiBj7E5ikbOxj8Lf7qt6gs3bLOKO21ihAAAAAH1u3FYAA")

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client["RojUserBot"]
media_collection = db["media_files"]

async def setup_database():
    await media_collection.create_index(
        [("file_unique_id", 1), ("chat_id", 1)], 
        unique=True
    )

delete_queue = asyncio.Queue()

async def delete_worker(client: Client):
    while True:
        chat_id, message_id = await delete_queue.get()
        try:
            await client.delete_messages(chat_id, message_id)
        except Exception as e:
            logger.error(f"Delete error: {e}")
        finally:
            delete_queue.task_done()
            await asyncio.sleep(0.5)

app = Client(
    "userbot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    workers=2
)

def get_media_unique_id(message: Message):
    for media_type in (
        "photo", "video", "document", "audio", "animation", 
        "voice", "video_note", "sticker"
    ):
        media = getattr(message, media_type, None)
        if media:
            return getattr(media, "file_unique_id", None)
    return None


def parse_chat_id(chat_id_str: str) -> int:
    """Parse chat ID from string (supports various formats)"""
    chat_id_str = chat_id_str.strip()
    
    # Already a number
    if chat_id_str.lstrip('-').isdigit():
        return int(chat_id_str)
    
    # Extract from Telegram URL: https://t.me/c/1234567890/9999
    match = re.search(r'/c/(\d+)', chat_id_str)
    if match:
        numeric_id = int(match.group(1))
        # Convert to Telegram's internal chat_id format (-100xxxxxxxxxx)
        return -1000000000000 - numeric_id if numeric_id < 100000000000 else -(numeric_id - 1000000000000)
    
    # Extract numeric part from any URL
    match = re.search(r'(\d{10,})', chat_id_str)
    if match:
        return int(match.group(1))
    
    return 0


async def scan_messages(
    client: Client,
    chat_id: int,
    start_id: int,
    end_id: int = None,
    status_message: Message = None
):
    """Scan messages in range [start_id, end_id]"""
    deleted_count = 0
    scanned_count = 0
    added_count = 0
    
    offset_id = None
    batch_size = 100
    
    while True:
        try:
            messages = await client.get_chat_history(
                chat_id, 
                limit=batch_size,
                offset_id=offset_id
            )
        except Exception as e:
            logger.error(f"Error getting messages: {e}")
            break
        
        if not messages:
            break
        
        for msg in messages:
            # Stop if we've reached the end boundary
            if end_id and msg.id > end_id:
                continue
            if msg.id < start_id:
                return scanned_count, deleted_count, added_count
            
            scanned_count += 1
            unique_id = get_media_unique_id(msg)
            
            if unique_id:
                try:
                    existing = await media_collection.find_one({
                        "file_unique_id": unique_id,
                        "chat_id": chat_id
                    })
                    
                    if existing:
                        old_msg_id = existing.get('msg_id')
                        if old_msg_id and old_msg_id != msg.id:
                            await delete_queue.put((chat_id, old_msg_id))
                            deleted_count += 1
                        
                        await media_collection.update_one(
                            {"_id": existing["_id"]},
                            {"$set": {"msg_id": msg.id}}
                        )
                    else:
                        await media_collection.insert_one({
                            "file_unique_id": unique_id,
                            "chat_id": chat_id,
                            "msg_id": msg.id,
                            "media_group_id": getattr(msg, "media_group_id", None)
                        })
                        added_count += 1
                except Exception as e:
                    logger.error(f"Database error: {e}")
            
            await asyncio.sleep(0.01)
        
        # Update offset_id for next batch
        offset_id = messages[-1].id - 1
        
        # Update status
        if status_message and scanned_count % 500 == 0:
            try:
                await status_message.edit_text(
                    f"⏳ **Scanning...**\n\n"
                    f"Chat ID: `{chat_id}`\n"
                    f"Range: `{start_id}` → `{end_id or 'latest'}`\n\n"
                    f"📊 **Progress:**\n"
                    f"• Scanned: `{scanned_count}`\n"
                    f"• Duplicates: `{deleted_count}`\n"
                    f"• New media: `{added_count}`\n\n"
                    f"Last ID: `{messages[-1].id}`"
                )
            except Exception:
                pass
            await asyncio.sleep(1)
    
    return scanned_count, deleted_count, added_count


@app.on_message(filters.command("scan", prefixes=".") & filters.me)
async def scan_command(client: Client, message: Message):
    """
    Scan commands:
    .scan start_id              - Scan from start_id to latest in current chat
    .scan chat_id start_id      - Scan from start_id to latest in specified chat
    .scan chat_id start_id end_id - Scan specific range in specified chat
    """
    cmd = message.command
    
    if len(cmd) < 2:
        await message.edit_text(
            "❌ **Usage:**\n\n"
            "`.scan 1` - Scan from message 1 to latest (current chat)\n"
            "`.scan 100` - Scan from message 100 to latest\n"
            "`.scan chat_id 1` - Scan specific chat\n"
            "`.scan chat_id 1 1000` - Scan range\n\n"
            "**Examples:**\n"
            "`.scan 100` - start from msg 100\n"
            "`.scan -1001234567890 1` - scan channel\n"
            "`.scan -1001234567890 1 9330` - scan range\n\n"
            "**Get Chat ID:** Use @username机器人 or check message link"
        )
        return
    
    try:
        # Parse arguments
        chat_id = message.chat.id
        start_id = 1
        end_id = None
        
        if len(cmd) == 2:
            # .scan start_id (current chat)
            start_id = int(cmd[1])
            if start_id < 0:
                start_id = 1
                
        elif len(cmd) == 3:
            # .scan chat_id start_id
            parsed = parse_chat_id(cmd[1])
            if parsed and parsed != 0:
                chat_id = parsed
            else:
                # Treat as start_id (maybe user didn't provide chat_id)
                start_id = int(cmd[2])
            if cmd[1].lstrip('-').isdigit() and abs(int(cmd[1])) > 100000000:
                chat_id = int(cmd[1])
                start_id = int(cmd[2])
            else:
                start_id = int(cmd[1])
                
        elif len(cmd) >= 4:
            # .scan chat_id start_id end_id
            chat_id = parse_chat_id(cmd[1])
            if chat_id == 0:
                # Try treating first arg as start_id
                chat_id = message.chat.id
                start_id = int(cmd[1])
                end_id = int(cmd[2])
            else:
                start_id = int(cmd[2])
                end_id = int(cmd[3])
        
        # Verify chat access
        try:
            chat = await client.get_chat(chat_id)
            chat_title = chat.title or "Chat"
        except Exception as e:
            await message.edit_text(f"❌ Cannot access chat: `{e}`")
            return
        
        # Start scanning
        status_msg = await message.edit_text(
            f"⏳ **Starting Scan...**\n\n"
            f"Chat: `{chat_title}`\n"
            f"Chat ID: `{chat_id}`\n"
            f"Range: `{start_id}` → `{end_id or 'latest'}`\n\n"
            f"Please wait..."
        )
        
        scanned, deleted, added = await scan_messages(
            client, chat_id, start_id, end_id, status_msg
        )
        
        await status_msg.edit_text(
            f"✅ **Scan Complete!**\n\n"
            f"Chat: `{chat_title}`\n"
            f"Range: `{start_id}` → `{end_id or 'latest'}`\n\n"
            f"📊 **Results:**\n"
            f"• Total scanned: `{scanned}`\n"
            f"• Duplicates deleted: `{deleted}`\n"
            f"• New media saved: `{added}`"
        )
        
    except ValueError as e:
        await message.edit_text(f"❌ Invalid number format: `{e}`")
    except Exception as e:
        logger.error(f"Scan error: {e}")
        await message.edit_text(f"❌ Error: `{str(e)}`")


@app.on_message(filters.command("stats", prefixes=".") & filters.me)
async def stats_command(client: Client, message: Message):
    """Show database statistics"""
    try:
        chat_id = message.chat.id
        total = await media_collection.count_documents({"chat_id": chat_id})
        await message.edit_text(
            f"📊 **Database Stats**\n\n"
            f"Chat: `{chat_id}`\n"
            f"Total media: `{total}`"
        )
    except Exception as e:
        await message.edit_text(f"❌ Error: `{e}`")


@app.on_message(filters.command("add", prefixes=".") & filters.me)
async def add_command(client: Client, message: Message):
    if not message.reply_to_message:
        await message.edit_text("❌ Reply to a media message.")
        return

    unique_id = get_media_unique_id(message.reply_to_message)
    if not unique_id:
        await message.edit_text("❌ No supported media found.")
        return

    chat_id = message.chat.id
    existing = await media_collection.find_one({
        "file_unique_id": unique_id,
        "chat_id": chat_id
    })

    if existing:
        await message.edit_text("✅ Already in database.")
    else:
        await media_collection.insert_one({
            "file_unique_id": unique_id,
            "chat_id": chat_id,
            "msg_id": message.reply_to_message.id
        })
        await message.edit_text("✅ Added to database.")


@app.on_message(filters.command("remove", prefixes=".") & filters.me)
async def remove_command(client: Client, message: Message):
    if not message.reply_to_message:
        await message.edit_text("❌ Reply to a media message.")
        return

    unique_id = get_media_unique_id(message.reply_to_message)
    if not unique_id:
        await message.edit_text("❌ No supported media found.")
        return

    chat_id = message.chat.id
    existing = await media_collection.find_one({
        "file_unique_id": unique_id,
        "chat_id": chat_id
    })

    if existing:
        await media_collection.delete_one({"_id": existing["_id"]})
        await message.edit_text("✅ Removed from database.")
    else:
        await message.edit_text("❌ Not in database.")


@app.on_message(filters.group | filters.channel | filters.private, group=1)
async def new_message_handler(client: Client, message: Message):
    if message.text and message.text.startswith(".scan"):
        return

    unique_id = get_media_unique_id(message)
    if unique_id:
        chat_id = message.chat.id
        existing = await media_collection.find_one({
            "file_unique_id": unique_id,
            "chat_id": chat_id
        })
        
        if existing:
            await delete_queue.put((chat_id, message.id))
            await media_collection.update_one(
                {"_id": existing["_id"]},
                {"$set": {"msg_id": message.id}}
            )
        else:
            await media_collection.insert_one({
                "file_unique_id": unique_id,
                "chat_id": chat_id,
                "msg_id": message.id
            })


if __name__ == "__main__":
    emit_positive_health()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(setup_database())
    loop.create_task(delete_worker(app))
    app.run()

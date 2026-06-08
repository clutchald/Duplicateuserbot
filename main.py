import asyncio
import os
import re
import logging

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

from pyrogram import Client, filters
from pyrogram.types import Message
from motor.motor_asyncio import AsyncIOMotorClient
from DA_Koyeb.health import emit_positive_health

API_ID = int(os.environ.get("API_ID", "20342933"))
API_HASH = os.environ.get("API_HASH", "9233e5deebe6abfc9ba297a9678851be")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://RAJ:RAJ@outlook.itqomxm.mongodb.net/?appName=outlook")
SESSION_STRING = os.environ.get("SESSION_STRING", "BQFRwgAArMER9MTPuJo66dzdLGYNjxBk2OC-qrPpEgHcnSE7XazoBzwN2PCeXJALh-td4hMvMjrcyhLPbniNkaMTicj5z3NzoCl-1ocTG2aLKw7mzqHVo_gsIReSyD-SW3gdnjIY8VLIULISdz13RsdICSFZaYwvjWKOTQKTEg9b-d40n4qTLtEgi5cfSh3YPZW3rLBxMGr4MGE2yCbhgiUs8XS6Nz0rFcukb7wRfZ4OVj15hPPo5nePbiwXpkyqqudfA0t4abQgtn7_mDX-jm3JAxGA9Rxune-kcwjuUXrV_jWNh1IRFKP94LbPtKiBj7E5ikbOxj8Lf7qt6gs3bLOKO21ihAAAAAH1u3FYAA")

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client["RojUserBot"]
media_collection = db["media_files"]

async def setup_database():
    await media_collection.create_index([("file_unique_id", 1), ("chat_id", 1)], unique=True)

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

app = Client("userbot", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING, workers=2)

def get_media_unique_id(message: Message):
    for media_type in ("photo", "video", "document", "audio", "animation", "voice", "video_note", "sticker"):
        media = getattr(message, media_type, None)
        if media:
            return getattr(media, "file_unique_id", None)
    return None

def parse_chat_id(chat_id_str: str) -> int:
    chat_id_str = chat_id_str.strip()
    if chat_id_str.lstrip('-').isdigit():
        return int(chat_id_str)
    match = re.search(r'/c/(\d+)', chat_id_str)
    if match:
        return -1000000000000 - int(match.group(1))
    match = re.search(r'(\d{10,})', chat_id_str)
    if match:
        return int(match.group(1))
    return 0

async def scan_messages(client, chat_id, start_id, end_id=None, status_message=None):
    """Scan messages from start_id to end_id (oldest to newest)"""
    deleted = 0
    scanned = 0
    added = 0
    
    # Get messages using message_id offset (go from oldest to newest)
    offset_id = start_id if start_id else 1
    
    while True:
        try:
            # Get next batch of messages using offset_id
            messages = await client.get_chat_history(
                chat_id, 
                limit=100,
                offset_id=offset_id
            )
        except Exception as e:
            logger.error(f"Error getting messages: {e}")
            break
        
        if not messages:
            break
        
        # Process messages
        for msg in messages:
            scanned += 1
            
            # Check if we've passed the end boundary
            if end_id and msg.id > end_id:
                offset_id = msg.id + 1
                continue
            
            # Process this message
            unique_id = get_media_unique_id(msg)
            
            if unique_id:
                try:
                    existing = await media_collection.find_one({"file_unique_id": unique_id, "chat_id": chat_id})
                    if existing:
                        old_msg_id = existing.get('msg_id')
                        if old_msg_id and old_msg_id != msg.id:
                            await delete_queue.put((chat_id, old_msg_id))
                            deleted += 1
                        await media_collection.update_one({"_id": existing["_id"]}, {"$set": {"msg_id": msg.id}})
                    else:
                        await media_collection.insert_one({"file_unique_id": unique_id, "chat_id": chat_id, "msg_id": msg.id})
                        added += 1
                except Exception as e:
                    logger.error(f"DB error: {e}")
            
            await asyncio.sleep(0.01)
        
        # Get last message ID for next offset
        last_msg = messages[-1]
        if end_id and last_msg.id >= end_id:
            break
        offset_id = last_msg.id + 1
        
        # Update status
        if status_message and scanned % 100 == 0:
            try:
                await status_message.edit_text(
                    f"⏳ Scanning... {scanned} messages checked\n"
                    f"Range: {start_id} → {end_id or 'latest'}\n"
                    f"Last ID: {last_msg.id}"
                )
            except:
                pass
            await asyncio.sleep(0.5)
    
    return scanned, deleted, added

@app.on_message(filters.command("scan", prefixes=".") & filters.me)
async def scan_command(client: Client, message: Message):
    cmd = message.command
    
    if len(cmd) < 2:
        await message.edit_text("""
**Scan Commands:**

`.scan 1` - Scan from message 1 to latest
`.scan 1 100` - Scan from 1 to 100
`.scan chat_id 1 100` - Scan range in specific chat

**Examples:**
`.scan 1` - scan all
`.scan 100 500` - scan messages 100-500
- Use lower number first!
""")
        return
    
    try:
        chat_id = message.chat.id
        start_id = 1
        end_id = None
        
        # Parse based on number of arguments
        if len(cmd) == 2:
            start_id = int(cmd[1])
        elif len(cmd) == 3:
            # Check if first is chat_id or start_id
            first_arg = cmd[1]
            if '/' in first_arg:
                # URL provided
                chat_id = parse_chat_id(first_arg)
                start_id = int(cmd[2])
            elif abs(int(first_arg)) > 1000000000:
                # Chat ID
                chat_id = int(first_arg)
                start_id = int(cmd[2])
            else:
                # start_id end_id for current chat
                start_id = int(first_arg)
                end_id = int(cmd[2])
        elif len(cmd) >= 4:
            if '/' in cmd[1]:
                chat_id = parse_chat_id(cmd[1])
                start_id = int(cmd[2])
                end_id = int(cmd[3])
            else:
                chat_id = int(cmd[1])
                start_id = int(cmd[2])
                end_id = int(cmd[3])
        
        # Validate start_id < end_id
        if end_id and start_id > end_id:
            start_id, end_id = end_id, start_id
        
        try:
            chat = await client.get_chat(chat_id)
            chat_title = chat.title or "Chat"
        except:
            chat_title = str(chat_id)
        
        status_msg = await message.edit_text(
            f"⏳ Starting scan...\n\n"
            f"Chat: {chat_title}\n"
            f"Range: {start_id} → {end_id or 'latest'}"
        )
        
        scanned, deleted, added = await scan_messages(client, chat_id, start_id, end_id, status_msg)
        
        await status_msg.edit_text(
            f"✅ Scan Complete!\n\n"
            f"Chat: {chat_title}\n"
            f"Range: {start_id} → {end_id or 'latest'}\n\n"
            f"📊 Results:\n"
            f"• Checked: {scanned}\n"
            f"• Duplicates: {deleted}\n"
            f"• New: {added}"
        )
        
    except Exception as e:
        logger.error(f"Scan error: {e}")
        await message.edit_text(f"❌ Error: {str(e)}")

@app.on_message(filters.command("stats", prefixes=".") & filters.me)
async def stats_command(client: Client, message: Message):
    try:
        total = await media_collection.count_documents({"chat_id": message.chat.id})
        await message.edit_text(f"📊 Database: {total} media files")
    except Exception as e:
        await message.edit_text(f"❌ Error: {e}")

@app.on_message(filters.command("add", prefixes=".") & filters.me)
async def add_command(client: Client, message: Message):
    if not message.reply_to_message:
        await message.edit_text("❌ Reply to media")
        return
    unique_id = get_media_unique_id(message.reply_to_message)
    if not unique_id:
        await message.edit_text("❌ No media found")
        return
    chat_id = message.chat.id
    existing = await media_collection.find_one({"file_unique_id": unique_id, "chat_id": chat_id})
    if existing:
        await message.edit_text("✅ Already in DB")
    else:
        await media_collection.insert_one({"file_unique_id": unique_id, "chat_id": chat_id, "msg_id": message.reply_to_message.id})
        await message.edit_text("✅ Added")

@app.on_message(filters.command("remove", prefixes=".") & filters.me)
async def remove_command(client: Client, message: Message):
    if not message.reply_to_message:
        await message.edit_text("❌ Reply to media")
        return
    unique_id = get_media_unique_id(message.reply_to_message)
    if not unique_id:
        await message.edit_text("❌ No media found")
        return
    chat_id = message.chat.id
    existing = await media_collection.find_one({"file_unique_id": unique_id, "chat_id": chat_id})
    if existing:
        await media_collection.delete_one({"_id": existing["_id"]})
        await message.edit_text("✅ Removed")
    else:
        await message.edit_text("❌ Not in DB")

@app.on_message(filters.group | filters.channel | filters.private, group=1)
async def new_message_handler(client: Client, message: Message):
    if message.text and message.text.startswith(".scan"):
        return
    unique_id = get_media_unique_id(message)
    if unique_id:
        chat_id = message.chat.id
        existing = await media_collection.find_one({"file_unique_id": unique_id, "chat_id": chat_id})
        if existing:
            await delete_queue.put((chat_id, message.id))
            await media_collection.update_one({"_id": existing["_id"]}, {"$set": {"msg_id": message.id}})
        else:
            await media_collection.insert_one({"file_unique_id": unique_id, "chat_id": chat_id, "msg_id": message.id})

if __name__ == "__main__":
    emit_positive_health()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(setup_database())
    loop.create_task(delete_worker(app))
    app.run()

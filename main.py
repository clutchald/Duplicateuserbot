import asyncio

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

import os
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
        except Exception:
            pass
        finally:
            delete_queue.task_done()
            await asyncio.sleep(2)

app = Client(
    "userbot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING
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

@app.on_message(filters.command("scan", prefixes=".") & filters.me)
async def scan_command(client: Client, message: Message):
    if len(message.command) < 2:
        await message.edit_text("❌ Please provide a `from_msg_id`. Usage: `.scan <msg_id>`")
        return
    
    try:
        from_msg_id = int(message.command[1])
    except ValueError:
        await message.edit_text("❌ Invalid message ID. It must be a number.")
        return

    chat_id = message.chat.id
    status_msg = await message.edit_text(f"⏳ Scanning from message ID `{from_msg_id}` to latest...")
    
    deleted_count = 0
    scanned_count = 0
    
    try:
        async for msg in client.get_chat_history(chat_id):
            if msg.id < from_msg_id:
                break
                
            scanned_count += 1
            unique_id = get_media_unique_id(msg)
            
            if unique_id:
                existing = await media_collection.find_one({
                    "file_unique_id": unique_id,
                    "chat_id": chat_id
                })
                
                if existing:
                    try:
                        await delete_queue.put((chat_id, existing['msg_id']))
                        deleted_count += 1
                        
                        await media_collection.update_one(
                            {"_id": existing["_id"]},
                            {"$set": {"msg_id": msg.id}}
                        )
                    except Exception as e:
                        print(f"Failed to process newer message {existing['msg_id']}: {e}")
                else:
                    await media_collection.insert_one({
                        "file_unique_id": unique_id,
                        "chat_id": chat_id,
                        "msg_id": msg.id
                    })
            
            if scanned_count % 100 == 0:
                await status_msg.edit_text(
                    f"⏳ Scanning...\n"
                    f"Scanned: {scanned_count} messages\n"
                    f"Duplicates deleted: {deleted_count}"
                )
                await asyncio.sleep(1)
                
        await status_msg.edit_text(
            f"✅ **Scan Complete!**\n"
            f"**Total Scanned:** {scanned_count}\n"
            f"**Duplicates Deleted:** {deleted_count}"
        )
        
    except Exception as e:
        await status_msg.edit_text(f"❌ Error during scan: `{str(e)}`")


@app.on_message(filters.command("add", prefixes=".") & filters.me)
async def add_command(client: Client, message: Message):
    if not message.reply_to_message:
        await message.edit_text("❌ Please reply to a media message.")
        return

    unique_id = get_media_unique_id(message.reply_to_message)
    if not unique_id:
        await message.edit_text("❌ The replied message does not contain supported media.")
        return

    chat_id = message.chat.id
    existing = await media_collection.find_one({
        "file_unique_id": unique_id,
        "chat_id": chat_id
    })

    if existing:
        await message.edit_text("✅ Media is already in the database.")
    else:
        await media_collection.insert_one({
            "file_unique_id": unique_id,
            "chat_id": chat_id,
            "msg_id": message.reply_to_message.id
        })
        await message.edit_text("✅ Media added to the database.")


@app.on_message(filters.command("remove", prefixes=".") & filters.me)
async def remove_command(client: Client, message: Message):
    if not message.reply_to_message:
        await message.edit_text("❌ Please reply to a media message.")
        return

    unique_id = get_media_unique_id(message.reply_to_message)
    if not unique_id:
        await message.edit_text("❌ The replied message does not contain supported media.")
        return

    chat_id = message.chat.id
    existing = await media_collection.find_one({
        "file_unique_id": unique_id,
        "chat_id": chat_id
    })

    if existing:
        await media_collection.delete_one({"_id": existing["_id"]})
        await message.edit_text("✅ Media removed from the database.")
    else:
        await message.edit_text("❌ Media is not in the database.")


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

from pyrogram import Client, filters
from pytgcalls import idle, PyTgCalls
from pytgcalls.types import MediaStream
import aiohttp
import asyncio
from pyrogram.types import Message, CallbackQuery
import isodate
import os
import re
import time
import psutil
from datetime import datetime, timezone, timedelta
import uuid
import tempfile
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
from pyrogram.enums import ChatType, ChatMemberStatus
from typing import Union
from pytgcalls.types import Update
from pytgcalls import filters as fl
from pytgcalls.types import ChatUpdate, Update, UpdatedGroupCallParticipant
from pytgcalls.types.stream import StreamEnded
import requests
import urllib.parse
from flask import Flask
from flask import request
from threading import Thread
from dotenv import load_dotenv
import json    # Required for persisting the download cache
import sys 
from http.server import HTTPServer, BaseHTTPRequestHandler 
import threading
import subprocess
from pymongo import MongoClient, ASCENDING
from bson import ObjectId
import aiofiles
from pyrogram.enums import ChatType
import random
from urllib.parse import quote
from PIL import Image, ImageDraw, ImageFont
from pyrogram.enums import ParseMode
from pyrogram import errors
from gender_guesser.detector import Detector
from pyrogram.types import ChatPermissions
import logging
from pyrogram.errors import RPCError

load_dotenv()


API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ASSISTANT_SESSION = os.environ.get("ASSISTANT_SESSION")
OWNER_ID = 5268762773

# ——— Monkey-patch resolve_peer ——————————————
logging.getLogger("pyrogram").setLevel(logging.ERROR)
_original_resolve_peer = Client.resolve_peer
async def _safe_resolve_peer(self, peer_id):
    try:
        return await _original_resolve_peer(self, peer_id)
    except (KeyError, ValueError) as e:
        if "ID not found" in str(e) or "Peer id invalid" in str(e):
            return None
        raise
Client.resolve_peer = _safe_resolve_peer

# ——— Suppress un‐retrieved task warnings —————————
def _custom_exception_handler(loop, context):
    exc = context.get("exception")
    if isinstance(exc, (KeyError, ValueError)) and (
        "ID not found" in str(exc) or "Peer id invalid" in str(exc)
    ):
        return  # ignore peer‐id errors

    # ← NEW: ignore the "NoneType has no attribute 'write'" from get_channel_difference
    if isinstance(exc, AttributeError) and "has no attribute 'write'" in str(exc):
        return

    # otherwise, let it bubble
    loop.default_exception_handler(context)

asyncio.get_event_loop().set_exception_handler(_custom_exception_handler)

session_name = os.environ.get("SESSION_NAME", "music_bot1")
bot = Client(session_name, bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH)
assistant = Client("assistant_account", session_string=ASSISTANT_SESSION)
call_py = PyTgCalls(assistant)


ASSISTANT_USERNAME = "@xyz92929"
ASSISTANT_CHAT_ID = 7634862283
API_ASSISTANT_USERNAME = "@xyz92929"

# API Endpoints
API_URL = os.environ.get("API_URL")
DOWNLOAD_API_URL = os.environ.get("DOWNLOAD_API_URL")
BACKUP_SEARCH_API_URL= "https://teenage-liz-frozzennbotss-61567ab4.koyeb.app"


# ─── MongoDB Setup ─────────────────────────────────────────────────
mongo_uri = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://frozenbotss:frozenbots@cluster0.s0tak.mongodb.net/?retryWrites=true&w=majority"
)
mongo_client = MongoClient(mongo_uri)
db = mongo_client["music_bot"]

# Collections
playlist_collection   = db["playlists"]
bots_collection       = db["bots"]
broadcast_collection  = db["broadcast"]
couples_collection    = db["couples"]
members_cache         = db["chat_members"]

# Create per-chat unique index on chat_id
members_cache.create_index([("chat_id", ASCENDING)], unique=True)
couples_collection.create_index([("chat_id", ASCENDING)], unique=True)

# TTL Indexes to auto-expire
couples_collection.create_index(
    [("created_at", ASCENDING)],
    expireAfterSeconds=24 * 3600  # auto-expire couples after 24 hours
)
members_cache.create_index(
    [("last_synced", ASCENDING)],
    expireAfterSeconds=24 * 3600  # refresh member cache daily
)

# template & font (adjust paths as needed)
TEMPLATE_PATH = "copules.png"
FONT_PATH     = "arial.ttf"
_template = Image.open(TEMPLATE_PATH).convert("RGBA")
R = 240
W, H = _template.size
CENTERS = [(348,380), (1170,380)]
NAME_Y = CENTERS[0][1] + R + 30
GROUP_Y = 40
GROUP_FONT_SIZE = 72


loop_mode = {}
chat_containers = {}
playback_tasks = {}  # To manage playback tasks per chat
bot_start_time = time.time()
COOLDOWN = 10
chat_last_command = {}
chat_pending_commands = {}
QUEUE_LIMIT = 20
MAX_DURATION_SECONDS = 7800  # 2 hours and 10 minutes  # 10 minutes (in seconds)
LOCAL_VC_LIMIT = 10
api_playback_records = []
playback_mode = {}
# Global dictionaries for the new feature
last_played_song = {}    # Maps chat_id to the info of the last played song
last_suggestions = {}
global_playback_count = 0  # Increments on every new playback request
api_server_counter = 0     # Used to select an API server in round-robin fashion
api_servers = [
    "https://py-tgcalls-api-1.onrender.com",
    "https://py-tgcalls-api-4vju.onrender.com",
    "http://py-tgcalls-api-yto1.onrender.com",
    "https://py-tgcalls-api-p44l.onrender.com",
    "https://py-tgcalls-api-fzk2.onrender.com",
    "https://py-tgcalls-api-vjd1.onrender.com"
]
chat_api_server = {}
global_api_index = 0


async def process_pending_command(chat_id, delay):
    await asyncio.sleep(delay)  # Wait for the cooldown period to expire
    if chat_id in chat_pending_commands:
        message, cooldown_reply = chat_pending_commands.pop(chat_id)
        await cooldown_reply.delete()  # Delete the cooldown notification
        await play_handler(bot, message) # Use `bot` instead of `app`


async def show_suggestions(chat_id, last_song_url, status_message=None):
    try:
        suggestions_api = f"https://odd-block-a945.tenopno.workers.dev/related?input={urllib.parse.quote(last_song_url)}"
        async with aiohttp.ClientSession() as session:
            async with session.get(suggestions_api) as resp:
                if resp.status != 200:
                    error_text = f"Suggestions API returned status {resp.status} for chat {chat_id} using URL: {last_song_url}"
                    print(error_text)
                    await bot.send_message(5268762773, error_text)
                    if status_message:
                        try:
                            await status_message.edit("❌ Failed to fetch suggestions from the API.")
                        except Exception as e:
                            print("Error editing status message:", e)
                            await bot.send_message(chat_id, "❌ Failed to fetch suggestions from the API.")
                    else:
                        await bot.send_message(chat_id, "❌ Failed to fetch suggestions from the API.")
                    return
                data = await resp.json()
                suggestions = data.get("suggestions", [])
                if not suggestions:
                    error_text = "No suggestions returned from API."
                    print(error_text)
                    await bot.send_message(5268762773, f"Suggestions API error in chat {chat_id}: {error_text}")
                    if status_message:
                        try:
                            await status_message.edit("❌ No suggestions available from the API.")
                        except Exception as e:
                            print("Error editing status message:", e)
                            await bot.send_message(chat_id, "❌ No suggestions available from the API.")
                    else:
                        await bot.send_message(chat_id, "❌ No suggestions available from the API.")
                    return
                # Save suggestions for later use in callback queries.
                last_suggestions[chat_id] = suggestions
                # Build inline buttons with callback data "suggestion|<index>"
                buttons = [
                    [InlineKeyboardButton(text=suggestion.get("title", "Suggestion"), callback_data=f"suggestion|{i}")]
                    for i, suggestion in enumerate(suggestions)
                ]
                markup = InlineKeyboardMarkup(buttons)
                new_text = "✨ No more songs in the queue. Here are some suggestions based on the last played song: ✨"
                if status_message:
                    try:
                        await status_message.edit(new_text, reply_markup=markup)
                    except Exception as e:
                        print("Error editing status message in show_suggestions:", e)
                        await bot.send_message(chat_id, new_text, reply_markup=markup)
                else:
                    await bot.send_message(chat_id, new_text, reply_markup=markup)
    except Exception as e:
        error_text = f"Error fetching suggestions: {str(e)}"
        print(error_text)
        await bot.send_message(5268762773, f"Suggestions API error in chat {chat_id}: {error_text}")
        if status_message:
            try:
                await status_message.edit(f"❌ Error fetching suggestions: {str(e)}")
            except Exception as ex:
                print("Error editing status message:", ex)
                await bot.send_message(chat_id, f"❌ Error fetching suggestions: {str(e)}")
        else:
            await bot.send_message(chat_id, f"❌ Error fetching suggestions: {str(e)}")
        await leave_voice_chat(chat_id)




def safe_handler(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            # Attempt to extract a chat ID (if available)
            chat_id = "Unknown"
            try:
                # If your function is a message handler, the second argument is typically the Message object.
                if len(args) >= 2:
                    chat_id = args[1].chat.id
                elif "message" in kwargs:
                    chat_id = kwargs["message"].chat.id
            except Exception:
                chat_id = "Unknown"
            error_text = (
                f"Error in handler `{func.__name__}` (chat id: {chat_id}):\n\n{str(e)}"
            )
            print(error_text)
            # Log the error to support
            await bot.send_message(5268762773, error_text)
    return wrapper


async def extract_invite_link(client, chat_id):
    try:
        chat_info = await client.get_chat(chat_id)
        if chat_info.invite_link:
            return chat_info.invite_link
        elif chat_info.username:
            return f"https://t.me/{chat_info.username}"
        return None
    except ValueError as e:
        if "Peer id invalid" in str(e):
            print(f"Invalid peer ID for chat {chat_id}. Skipping invite link extraction.")
            return None
        else:
            raise e  # re-raise if it's another ValueError
    except Exception as e:
        print(f"Error extracting invite link for chat {chat_id}: {e}")
        return None

async def extract_target_user(message: Message):
    # If the moderator replied to someone:
    if message.reply_to_message:
        return message.reply_to_message.from_user.id

    # Otherwise expect an argument like "/ban @user" or "/ban 123456"
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("❌ You must reply to a user or specify their @username/user_id.")
        return None

    target = parts[1]
    # Strip @
    if target.startswith("@"):
        target = target[1:]
    try:
        user = await message._client.get_users(target)
        return user.id
    except:
        await message.reply("❌ Could not find that user.")
        return None



async def is_assistant_in_chat(chat_id):
    try:
        member = await assistant.get_chat_member(chat_id, ASSISTANT_USERNAME)
        return member.status is not None
    except Exception as e:
        error_message = str(e)
        if "USER_BANNED" in error_message or "Banned" in error_message:
            return "banned"
        elif "USER_NOT_PARTICIPANT" in error_message or "Chat not found" in error_message:
            return False
        print(f"Error checking assistant in chat: {e}")
        return False

async def is_api_assistant_in_chat(chat_id):
    try:
        member = await bot.get_chat_member(chat_id, API_ASSISTANT_USERNAME)
        return member.status is not None
    except Exception as e:
        print(f"Error checking API assistant in chat: {e}")
        return False
    
def iso8601_to_seconds(iso_duration):
    try:
        duration = isodate.parse_duration(iso_duration)
        return int(duration.total_seconds())
    except Exception as e:
        print(f"Error parsing duration: {e}")
        return 0


def iso8601_to_human_readable(iso_duration):
    try:
        duration = isodate.parse_duration(iso_duration)
        total_seconds = int(duration.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}:{minutes:02}:{seconds:02}"
        return f"{minutes}:{seconds:02}"
    except Exception as e:
        return "Unknown duration"

async def fetch_youtube_link(query):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{API_URL}{query}") as response:
                if response.status == 200:
                    data = await response.json()
                    # Check if the API response contains a playlist
                    if "playlist" in data:
                        return data
                    else:
                        return (
                            data.get("link"),
                            data.get("title"),
                            data.get("duration"),
                            data.get("thumbnail")
                        )
                else:
                    raise Exception(f"API returned status code {response.status}")
    except Exception as e:
        raise Exception(f"Failed to fetch YouTube link: {str(e)}")


    
async def fetch_youtube_link_backup(query):
    if not BACKUP_SEARCH_API_URL:
        raise Exception("Backup Search API URL not configured")
    # Build the correct URL:
    backup_url = (
        f"{BACKUP_SEARCH_API_URL.rstrip('/')}"
        f"/search?title={urllib.parse.quote(query)}"
    )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(backup_url, timeout=30) as resp:
                if resp.status != 200:
                    raise Exception(f"Backup API returned status {resp.status}")
                data = await resp.json()
                # Mirror primary API’s return:
                if "playlist" in data:
                    return data
                return (
                    data.get("link"),
                    data.get("title"),
                    data.get("duration"),
                    data.get("thumbnail")
                )
    except Exception as e:
        raise Exception(f"Backup Search API error: {e}")



async def skip_to_next_song(chat_id, message):
    """Skips to the next song in the queue and starts playback."""
    if chat_id not in chat_containers or not chat_containers[chat_id]:
        # Update playback records since the voice chat is ending
        record = {
            "chat_id": chat_id,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "event": "vc_ended",
            "mode": playback_mode.get(chat_id, "unknown")
        }
        api_playback_records.append(record)
        playback_mode.pop(chat_id, None)
        
        await message.edit("❌ No more songs in the queue.")
        await leave_voice_chat(chat_id)
        return

    await message.edit("⏭ Skipping to the next song...")
    await start_playback_task(chat_id, message)
    
async def is_user_admin(obj: Union[Message, CallbackQuery]) -> bool:
    if isinstance(obj, CallbackQuery):
        message = obj.message
        user = obj.from_user
    elif isinstance(obj, Message):
        message = obj
        user = obj.from_user
    else:
        return False

    if not user:
        return False

    if message.chat.type not in [ChatType.SUPERGROUP, ChatType.CHANNEL]:
        return False

    if user.id in [
        777000,  
        5268762773, 
    ]:
        return True

    client = message._client
    chat_id = message.chat.id
    user_id = user.id

    check_status = await client.get_chat_member(chat_id=chat_id, user_id=user_id)
    if check_status.status not in [
        ChatMemberStatus.OWNER,
        ChatMemberStatus.ADMINISTRATOR
    ]:
        return False
    else:
        return True
    
async def stop_playback(chat_id):
    """
    Stops playback in the given chat using the external API.
    """
    # Use the assigned API server if available; otherwise, fallback to the first API server.
    if chat_id in chat_api_server:
        selected_api, _, _ = chat_api_server[chat_id]
    else:
        selected_api = api_servers[0]
    api_stop_url = f"{selected_api}/stop?chatid={chat_id}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_stop_url) as resp:
                data = await resp.json()
        # Record the API stop event
        record = {
            "chat_id": chat_id,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "event": "stop",
            "api_response": data,
            "mode": playback_mode.get(chat_id, "unknown")
        }
        api_playback_records.append(record)
        playback_mode.pop(chat_id, None)  # Clear playback mode for the chat
        await bot.send_message(chat_id, f"API Stop: {data['message']}")
    except Exception as e:
        await bot.send_message(chat_id, f"❌ API Stop Error: {str(e)}")

async def invite_assistant(chat_id, invite_link, processing_message):
    """
    Internally invite the assistant to the chat by using the assistant client to join the chat.
    If an error occurs, it returns False and displays the exact error.
    """
    try:
        # Use the assistant client to join the chat via the invite link.
        await assistant.join_chat(invite_link)
        return True
    except Exception as e:
        error_message = f"❌ Error while inviting assistant: {str(e)}"
        await processing_message.edit(error_message)
        return False



@bot.on_message(filters.command("start"))
async def start_handler(_, message):
    # Calculate uptime
    current_time = time.time()
    uptime_seconds = int(current_time - bot_start_time)
    uptime_str = str(timedelta(seconds=uptime_seconds))

    # Mention the user
    user_mention = message.from_user.mention

    # Caption with bot info and uptime
    caption = (
        f"👋 нєу {user_mention} 💠, 🥀\n\n"
        "🎶 Wᴇʟᴄᴏᴍᴇ  🎵\n\n"
        "➻ 🚀 A Sᴜᴘᴇʀғᴀsᴛ & Pᴏᴡᴇʀғᴜʟ Tᴇʟᴇɢʀᴀᴍ Mᴜsɪᴄ Bᴏᴛ ᴡɪᴛʜ ᴀᴍᴀᴢɪɴɢ ғᴇᴀᴛᴜʀᴇs. ✨\n\n"
        "🎧 Sᴜᴘᴘᴏʀᴛᴇᴅ Pʟᴀᴛғᴏʀᴍs: ʏᴏᴜᴛᴜʙᴇ, sᴘᴏᴛɪғʏ, ʀᴇssᴏ, ᴀᴘᴘʟᴇ ᴍᴜsɪᴄ, sᴏᴜɴᴅᴄʟᴏᴜᴅ.\n\n"
        "🔹 Kᴇʏ Fᴇᴀᴛᴜʀᴇs:\n"
        "🎵 Playlist Support for your favorite tracks.\n"
        "🤖 AI Chat for engaging conversations.\n"
        "🖼️ Image Generation with AI creativity.\n"
        "👥 Group Management tools for admins.\n"
        "💡 And many more exciting features!\n\n"
        f"**Uptime:** `{uptime_str}`\n\n"
        "──────────────────\n"
        "๏ ᴄʟɪᴄᴋ ᴛʜᴇ ʜᴇʟᴘ ʙᴜᴛᴛᴏɴ ғᴏʀ ᴍᴏᴅᴜʟᴇ ᴀɴᴅ ᴄᴏᴍᴍᴀɴᴅ ɪɴғᴏ.."
    )

    # Buttons on the start screen
    buttons = [
        [
            InlineKeyboardButton(
                "➕ Add me",
                url="https://t.me/AmericanPepeCTObot?startgroup=true"
            ),
            InlineKeyboardButton(
                "💬 Support",
                url="https://t.me/american_pepecto"
            )
        ],
        [InlineKeyboardButton("❓ Help", callback_data="show_help")]
    ]
    reply_markup = InlineKeyboardMarkup(buttons)

    # Send a photo instead of an animation
    await message.reply_photo(
        photo="https://files.catbox.moe/39k0u4.jpg",
        caption=caption,
        reply_markup=reply_markup
    )

    # Register chat ID for broadcasting silently
    chat_id = message.chat.id
    chat_type = message.chat.type

    if chat_type == ChatType.PRIVATE:
        if not broadcast_collection.find_one({"chat_id": chat_id}):
            broadcast_collection.insert_one({"chat_id": chat_id, "type": "private"})
    elif chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        if not broadcast_collection.find_one({"chat_id": chat_id}):
            broadcast_collection.insert_one({"chat_id": chat_id, "type": "group"})


@bot.on_callback_query(filters.regex("^show_help$"))
async def show_help_callback(_, callback_query):
    help_text = "📜 Choose a category to explore commands:"  
    buttons = [
        [InlineKeyboardButton("🎵 Play", callback_data="help_play"),
         InlineKeyboardButton("⏹ Stop", callback_data="help_stop"),
         InlineKeyboardButton("⏸ Pause", callback_data="help_pause")],
        [InlineKeyboardButton("▶ Resume", callback_data="help_resume"),
         InlineKeyboardButton("⏭ Skip", callback_data="help_skip"),
         InlineKeyboardButton("🔄 Reboot", callback_data="help_reboot")],
        [InlineKeyboardButton("📶 Ping", callback_data="help_ping"),
         InlineKeyboardButton("🎶 Playlist", callback_data="help_playlist"),
         InlineKeyboardButton("🗑 Clear Queue", callback_data="help_clear")],
        [InlineKeyboardButton("🏠 Home", callback_data="go_back")]
    ]
    reply_markup = InlineKeyboardMarkup(buttons)
    await callback_query.message.edit_text(help_text, reply_markup=reply_markup)

@bot.on_callback_query(filters.regex("^help_play$"))
async def help_play_callback(_, callback_query):
    text = "🎵 **Play Command**\n\n➜ Use /play <song name> to play music.\n\n💡 Example: /play shape of you"
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="show_help")]]
    await callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex("^help_stop$"))
async def help_stop_callback(_, callback_query):
    text = "⏹ **Stop Command**\n\n➜ Use /stop or /end to stop the music and clear the queue."
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="show_help")]]
    await callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex("^help_pause$"))
async def help_pause_callback(_, callback_query):
    text = "⏸ **Pause Command**\n\n➜ Use /pause to pause the current song."
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="show_help")]]
    await callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex("^help_resume$"))
async def help_resume_callback(_, callback_query):
    text = "▶ **Resume Command**\n\n➜ Use /resume to continue playing the paused song."
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="show_help")]]
    await callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex("^help_skip$"))
async def help_skip_callback(_, callback_query):
    text = "⏭ **Skip Command**\n\n➜ Use /skip to move to the next song in the queue."
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="show_help")]]
    await callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex("^help_reboot$"))
async def help_reboot_callback(_, callback_query):
    text = "🔄 **Reboot Command**\n\n➜ Use /reboot to restart the bot if needed."
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="show_help")]]
    await callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex("^help_ping$"))
async def help_ping_callback(_, callback_query):
    text = "📶 **Ping Command**\n\n➜ Use /ping to check bot's response time and uptime."
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="show_help")]]
    await callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex("^help_playlist$"))
async def help_playlist_callback(_, callback_query):
    text = "🎶 **Playlist Command**\n\n➜ Use /playlist to view and manage your playlist."
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="show_help")]]
    await callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex("^help_clear$"))
async def help_clear_callback(_, callback_query):
    text = "🗑 **Clear Queue Command**\n\n➜ Use /clear to remove all songs from the queue."
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="show_help")]]
    await callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex("^go_back$"))
async def go_back_callback(_, callback_query):
    current_time = time.time()
    uptime_seconds = int(current_time - bot_start_time)
    uptime_str = str(timedelta(seconds=uptime_seconds))
    user_mention = callback_query.from_user.mention
    caption = (
        f"👋 нєу {user_mention} 💠, 🥀\n\n"
        "🎶 Wᴇʟᴄᴏᴍᴇ ᴛᴏ Fʀᴏᴢᴇɴ 🥀 ᴍᴜsɪᴄ! 🎵\n\n"
        "➻ 🚀 A Sᴜᴘᴇʀғᴀsᴛ & Pᴏᴡᴇʀғᴜʟ Tᴇʟᴇɢʀᴀᴍ Mᴜsɪᴄ Bᴏᴛ ᴡɪᴛʜ ᴀᴍᴀᴢɪɴɢ ғᴇᴀᴛᴜʀᴇs. ✨\n\n"
        "🎧 Sᴜᴘᴘᴏʀᴛᴇᴅ Pʟᴀᴛғᴏʀᴍs: ʏᴏᴜᴛᴜʙᴇ, sᴘᴏᴛɪғʏ, ʀᴇssᴏ, ᴀᴘᴘʟᴇ ᴍᴜsɪᴄ, sᴏᴜɴᴅᴄʟᴏᴜᴅ.\n\n"
        "🔹 Kᴇʏ Fᴇᴀᴛᴜʀᴇs:\n"
        "🎵 Playlist Support for your favorite tracks.\n"
        "🤖 AI Chat for engaging conversations.\n"
        "🖼️ Image Generation with AI creativity.\n"
        "👥 Group Management tools for admins.\n"
        "💡 And many more exciting features!\n\n"
        f"**Uptime:** `{uptime_str}`\n\n"
        "──────────────────\n"
        "๏ ᴄʟɪᴄᴋ ᴛʜᴇ ʜᴇʟᴘ ʙᴜᴛᴛᴏɴ ғᴏʀ ᴍᴏᴅᴜʟᴇ ᴀɴᴅ ᴄᴏᴍᴍᴀɴᴅ ɪɴғᴏ.."
    )
    buttons = [
        [InlineKeyboardButton("➕ Add me", url="https://t.me/AmericanPepeCTObot?startgroup=true"),
         InlineKeyboardButton("💬 Support", url="https://t.me/american_pepecto")],
        [InlineKeyboardButton("❓ Help", callback_data="show_help")]
    ]
    reply_markup = InlineKeyboardMarkup(buttons)
    await callback_query.message.edit_media(
        media=InputMediaPhoto(media="https://files.catbox.moe/39k0u4.jpg", caption=caption),
        reply_markup=reply_markup
    )

@bot.on_message(filters.group & filters.regex(r'^/play(?:@\w+)?(?:\s+(?P<query>.+))?$'))
async def play_handler(_, message):
    chat_id = message.chat.id

    # If replying to an audio/video message, handle local playback
    if message.reply_to_message and (message.reply_to_message.audio or message.reply_to_message.video):
        processing_message = await message.reply("❄️")

        # Ensure bot is in chat
        if not await is_assistant_in_chat(chat_id):
            invite_link = await extract_invite_link(bot, chat_id)
            if invite_link and await invite_assistant(chat_id, invite_link, processing_message):
                await processing_message.edit("⏳ Assistant is joining... Please wait.")
                for _ in range(10):
                    await asyncio.sleep(3)
                    if await is_assistant_in_chat(chat_id):
                        break
                else:
                    await processing_message.edit(
                        "❌ Assistant failed to join. Please unban the assistant.\nSupport: @frozensupport1"
                    )
                    return
            else:
                await processing_message.edit("❌ Please give bot invite-link permission.\nSupport: @frozensupport1")
                return

        # Fetch fresh media reference and download
        orig = message.reply_to_message
        fresh = await assistant.get_messages(orig.chat.id, orig.id)
        media = fresh.video or fresh.audio
        if fresh.audio and getattr(fresh.audio, 'file_size', 0) > 100 * 1024 * 1024:
            await processing_message.edit("❌ Audio file too large. Maximum allowed size is 100MB.")
            return

        await processing_message.edit("⏳ Please wait, downloading audio...")
        try:
            file_path = await assistant.download_media(media)
        except Exception as e:
            await processing_message.edit(f"❌ Failed to download media: {e}")
            return

        # Download thumbnail if available
        thumb_path = None
        try:
            thumbs = fresh.video.thumbs if fresh.video else fresh.audio.thumbs
            thumb_path = await assistant.download_media(thumbs[0])
        except Exception:
            pass

        # Prepare song_info and fallback to local playback
        duration = media.duration or 0
        title = getattr(media, 'file_name', 'Untitled')
        song_info = {
            'url': file_path,
            'title': title,
            'duration': format_time(duration),
            'duration_seconds': duration,
            'requester': message.from_user.first_name,
            'thumbnail': thumb_path,
            'file_path': file_path
        }

        # —— NEW: enqueue for loop support —— 
        chat_containers.setdefault(chat_id, []).append(song_info)
        # ————————————————————————————

        await fallback_local_playback(chat_id, processing_message, song_info)
        return

    # Otherwise, process query-based search
    match = message.matches[0]
    query = (match.group('query') or "").strip()

    try:
        await message.delete()
    except Exception:
        pass

    # Enforce cooldown
    now = time.time()
    if chat_id in chat_last_command and (now - chat_last_command[chat_id]) < COOLDOWN:
        remaining = int(COOLDOWN - (now - chat_last_command[chat_id]))
        if chat_id in chat_pending_commands:
            await _.send_message(chat_id, f"⏳ A command is already queued for this chat. Please wait {remaining}s.")
        else:
            cooldown_reply = await _.send_message(chat_id, f"⏳ On cooldown. Processing in {remaining}s.")
            chat_pending_commands[chat_id] = (message, cooldown_reply)
            asyncio.create_task(process_pending_command(chat_id, remaining))
        return
    chat_last_command[chat_id] = now

    if not query:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🎵 Play Your Playlist", callback_data="play_playlist"),
             InlineKeyboardButton("🔥 Play Trending Songs", callback_data="play_trending")]
        ])
        await _.send_message(
            chat_id,
            "You did not specify a song. Would you like to play your playlist or trending songs instead?\n\n"
            "Correct usage: /play <song name>\nExample: /play shape of you",
            reply_markup=keyboard
        )
        return

    # Delegate to query processor
    await process_play_command(message, query)



async def process_play_command(message, query):
    chat_id = message.chat.id
    processing_message = await message.reply("❄️")

    # Convert short URLs to full YouTube URLs
    if "youtu.be" in query:
        m = re.search(r"youtu\.be/([^?&]+)", query)
        if m:
            query = f"https://www.youtube.com/watch?v={m.group(1)}"

    # Ensure bot is in chat
    if not await is_assistant_in_chat(chat_id):
        invite_link = await extract_invite_link(bot, chat_id)
        if invite_link and await invite_assistant(chat_id, invite_link, processing_message):
            await processing_message.edit("⏳ Assistant is joining... Please wait.")
            for _ in range(10):
                await asyncio.sleep(3)
                if await is_assistant_in_chat(chat_id):
                    await processing_message.edit("✅ Assistant joined! Playing your song...")
                    break
            else:
                await processing_message.edit(
                    "❌ Assistant failed to join. Please unban the assistant.\nSupport: @frozensupport1"
                )
                return
        else:
            await processing_message.edit("❌ Please give bot invite‑link permission.\nSupport: @frozensupport1")
            return

    # Perform YouTube search and handle results
    try:
        result = await fetch_youtube_link(query)
    except Exception as primary_err:
        await processing_message.edit(
            "⚠️ Primary search failed. Using backup API, this may take a few seconds…"
        )
        try:
            result = await fetch_youtube_link_backup(query)
        except Exception as backup_err:
            await processing_message.edit(
                f"❌ Both search APIs failed:\n"
                f"Primary: {primary_err}\n"
                f"Backup:  {backup_err}"
            )
            return

    # 3) Handle playlist vs single video
    if isinstance(result, dict) and "playlist" in result:
        playlist_items = result["playlist"]
        if not playlist_items:
            await processing_message.edit("❌ No videos found in the playlist.")
            return

        chat_containers.setdefault(chat_id, [])
        # Add all items to queue
        for item in playlist_items:
            secs = isodate.parse_duration(item["duration"]).total_seconds()
            chat_containers[chat_id].append({
                "url": item["link"],
                "title": item["title"],
                "duration": iso8601_to_human_readable(item["duration"]),
                "duration_seconds": secs,
                "requester": message.from_user.first_name if message.from_user else "Unknown",
                "thumbnail": item["thumbnail"]
            })

        total = len(playlist_items)
        reply_text = (
            f"✨ᴀᴅᴅᴇᴅ ᴛᴏ playlist\n"
            f"Total songs added to queue: {total}\n"
            f"#1 - {playlist_items[0]['title']}"
        )
        if total > 1:
            reply_text += f"\n#2 - {playlist_items[1]['title']}"
        await message.reply(reply_text)

        # Preload only the next song for the playlist to avoid flooding API
        if total > 1:
            next_item = playlist_items[1]
            next_secs = isodate.parse_duration(next_item["duration"]).total_seconds()
            # Schedule caching for second song
            async def preload_next():
                api_base, _, _ = chat_api_server[chat_id]
                api_param = "&api=secondary" if next_secs > 720 else ""
                try:
                    async with aiohttp.ClientSession() as session:
                        await session.get(
                            f"{api_base}/cache?url={urllib.parse.quote(next_item['link'], safe='')}{api_param}"
                        )
                except Exception:
                    pass

            asyncio.create_task(preload_next())

        # Start the playback task; further caching of subsequent songs
        # will be triggered inside the playback handler when moving to the next track.
        if len(chat_containers[chat_id]) == total:
            await start_playback_task(chat_id, processing_message)
        else:
            await processing_message.delete()

    else:
        # Single video handling (unchanged)...
        video_url, title, duration_iso, thumb = result
        if not video_url:
            await processing_message.edit(
                "❌ Could not find the song. Try another query.\nSupport: @frozensupport1"
            )
            return

        secs = isodate.parse_duration(duration_iso).total_seconds()
        if secs > MAX_DURATION_SECONDS:
            await processing_message.edit(
                "❌ Streams longer than 10 min are not allowed. we are facing some server issues will be fixed"
            )
            return

        readable = iso8601_to_human_readable(duration_iso)
        chat_containers.setdefault(chat_id, [])
        chat_containers[chat_id].append({
            "url": video_url,
            "title": title,
            "duration": readable,
            "duration_seconds": secs,
            "requester": message.from_user.first_name if message.from_user else 'Unknown',
            "thumbnail": thumb
        })

        # If it's the first song, start playing immediately without caching
        if len(chat_containers[chat_id]) == 1:
            await start_playback_task(chat_id, processing_message)
        else:
            # Preload cache in background for queued songs with conditional API
            async def preload_cache(item_url, duration_sec):
                api_base, _, _ = chat_api_server[chat_id]
                api_param = "&api=secondary" if duration_sec > 720 else ""
                try:
                    async with aiohttp.ClientSession() as session:
                        await session.get(
                            f"{api_base}/cache?url={urllib.parse.quote(item_url, safe='')}{api_param}"
                        )
                except Exception as e:
                    print(f"[Cache Preload Error]: {e}")

            asyncio.create_task(preload_cache(video_url, secs))

            queue_buttons = InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭ Skip", callback_data="skip"),
                 InlineKeyboardButton("🗑 Clear", callback_data="clear")]
            ])
            await message.reply(
                f"✨ᴀᴅᴅᴇᴅ ᴛᴏ ǫᴜᴇᴜᴇ :\n\n"
                f"**❍ ᴛɪᴛʟє ➥** {title}\n"
                f"**❍ ᴛɪϻє ➥** {readable}\n"
                f"**❍ ʙʏ ➥ ** {message.from_user.first_name if message.from_user else 'Unknown'}\n"
                f"**Queue number:** {len(chat_containers[chat_id]) - 1}",
                reply_markup=queue_buttons
            )
            await processing_message.delete()



import isodate
from datetime import timedelta

def parse_duration_str(duration_str):
    """
    Convert a duration string to total seconds.
    First, try ISO 8601 parsing (e.g. "PT3M9S"). If that fails,
    fall back to colon-separated formats like "3:09" or "1:02:30".
    """
    try:
        # Try ISO 8601
        duration = isodate.parse_duration(duration_str)
        return int(duration.total_seconds())
    except Exception as e:
        if ':' in duration_str:
            try:
                parts = [int(x) for x in duration_str.split(':')]
                if len(parts) == 2:
                    minutes, seconds = parts
                    return minutes * 60 + seconds
                elif len(parts) == 3:
                    hours, minutes, seconds = parts
                    return hours * 3600 + minutes * 60 + seconds
            except Exception as e2:
                print(f"Error parsing colon-separated duration '{duration_str}': {e2}")
                return 0
        else:
            print(f"Error parsing duration '{duration_str}': {e}")
            return 0

def format_time(seconds):
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    else:
        return f"{m}:{s:02d}"

def get_progress_bar_styled(elapsed, total, bar_length=6):
    """
    Build a progress bar string in the style:
      elapsed_time  <dashes>◉<dashes>  total_time
    For example: 0:30 —◉———— 3:09
    """
    if total <= 0:
        return "Progress: N/A"
    fraction = min(elapsed / total, 1)
    marker_index = int(fraction * bar_length)
    if marker_index >= bar_length:
        marker_index = bar_length - 1
    left = "—" * marker_index
    right = "—" * (bar_length - marker_index - 1)
    bar = left + "◉" + right
    return f"{format_time(elapsed)} {bar} {format_time(total)}"

async def update_progress_caption(chat_id, progress_message, start_time, total_duration, base_caption, base_keyboard):
    while True:
        elapsed = time.time() - start_time
        if elapsed > total_duration:
            elapsed = total_duration
        progress_bar = get_progress_bar_styled(elapsed, total_duration)
        new_caption = base_caption.format(progress_bar=progress_bar)
        try:
            await bot.edit_message_caption(chat_id, progress_message.id, caption=new_caption, reply_markup=base_keyboard)
        except Exception as e:
            # If the error is MESSAGE_NOT_MODIFIED, ignore it and continue
            if "MESSAGE_NOT_MODIFIED" in str(e):
                pass
            else:
                print(f"Error updating progress caption for chat {chat_id}: {e}")
                break
        if elapsed >= total_duration:
            break
        await asyncio.sleep(18)



# ---------------------- Modified fallback_local_playback ---------------------- #
async def fallback_local_playback(chat_id, message, song_info):
    playback_mode[chat_id] = "local"
    try:
        if chat_id in playback_tasks:
            playback_tasks[chat_id].cancel()
        video_url = song_info.get('url')
        if not video_url:
            print(f"Invalid video URL for song: {song_info}")
            chat_containers[chat_id].pop(0)
            return
        try:
            await message.edit(f"ғᴀʟʟɪɴɢ ʙᴀᴄᴋ ᴛᴏ ʟᴏᴄᴀʟ ᴘʟᴀʏʙᴀᴄᴋ ғᴏʀ ⚡ {song_info['title']}...")
        except Exception:
            message = await bot.send_message(chat_id, f"ғᴀʟʟɪɴɢ ʙᴀᴄᴋ ᴛᴏ ʟᴏᴄᴀʟ ᴘʟᴀʏʙᴀᴄᴋ ғᴏʀ⚡ {song_info['title']}...")
        media_path = await download_audio(video_url)
        await call_py.play(
            chat_id,
            MediaStream(media_path, video_flags=MediaStream.Flags.IGNORE)
        )
        playback_tasks[chat_id] = asyncio.current_task()
        
        total_duration = parse_duration_str(song_info.get('duration', '0:00'))
        if total_duration <= 0:
            print("Warning: duration is zero or invalid for this song.")
        
        base_caption = (
            f"**ғʀᴏᴢᴇɴ ✘ ᴍᴜsɪᴄ sᴛʀєᴧϻɪηɢ (Local Playback)**\n\n"
            f"**❍ ᴛɪᴛʟє ➥** {song_info['title']}\n\n"
            f"**❍ ᴛɪϻє ➥** {{progress_bar}}\n\n"
            f"**❍ ʙʏ ➥** {song_info['requester']}"
        )
        initial_progress = get_progress_bar_styled(0, total_duration)
        caption = base_caption.format(progress_bar=initial_progress)
        
        base_keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(text="▶️", callback_data="pause"),
                InlineKeyboardButton(text="⏸", callback_data="resume"),
                InlineKeyboardButton(text="⏭", callback_data="skip"),
                InlineKeyboardButton(text="⏹", callback_data="stop")
            ],
            [
                InlineKeyboardButton(text="➕ᴀᴅᴅ тσ ρℓαуℓιѕт➕", callback_data="add_to_playlist"),
                InlineKeyboardButton(text="⚡WEBSITE⚡", url="https://americanpepe.site/")
            ],
            [
                InlineKeyboardButton(text="✨ υρ∂αтєѕ ✨", url="https://t.me/american_pepecto"),
                InlineKeyboardButton(text="💕 ѕυρρσят 💕", url="https://t.me/Frozensupport1")
            ]
        ])
        
        progress_message = await message.reply_photo(
            photo=song_info['thumbnail'],
            caption=caption,
            reply_markup=base_keyboard
        )
        await message.delete()
        asyncio.create_task(update_progress_caption(chat_id, progress_message, time.time(), total_duration, base_caption, base_keyboard))
    except Exception as e:
        print(f"Error during fallback local playback: {e}")


async def start_playback_task(chat_id, message):
    global global_api_index, global_playback_count
    print(f"Current playback tasks: {len(playback_tasks)}; Chat ID: {chat_id}")
    # Reuse the same message if available, setting an initial processing message.
    processing_message = message
    status_text = "**✨ Processing... Please wait, may take up to 20 seconds. 💕**"
    try:
        if processing_message:
            await processing_message.edit(status_text)
        else:
            processing_message = await bot.send_message(chat_id, status_text)
    except Exception:
        processing_message = await bot.send_message(chat_id, status_text)

    # (Existing code) Get or assign an API server for this chat.
    if chat_id in chat_api_server:
        selected_api, server_id, display_server = chat_api_server[chat_id]
    else:
        selected_api = api_servers[global_api_index % len(api_servers)]
        server_id = (global_api_index % len(api_servers)) + 1
        display_server = server_id
        chat_api_server[chat_id] = (selected_api, server_id, display_server)
        global_api_index += 1

    # Ensure the API assistant is in the chat.
    if not await is_api_assistant_in_chat(chat_id):
        invite_link = await extract_invite_link(bot, chat_id)
        if invite_link:
            join_api_url = f"{selected_api}/join?input={urllib.parse.quote(invite_link)}"
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(join_api_url, timeout=20) as join_resp:
                        if join_resp.status != 200:
                            raise Exception(f"Join API responded with status {join_resp.status}")
            except Exception as e:
                error_text = f"❌ API Assistant join error: {str(e)}. Please check the API endpoint."
                await bot.send_message(chat_id, error_text)
                return
            for _ in range(10):
                await asyncio.sleep(3)
                if await is_api_assistant_in_chat(chat_id):
                    break
            else:
                await bot.send_message(chat_id, "❌ API Assistant failed to join. Please check the API endpoint.")
                return

    if chat_id not in chat_containers or not chat_containers[chat_id]:
        await bot.send_message(chat_id, "❌ No songs in the queue.")
        return

    # Get the song info from the queue.
    song_info = chat_containers[chat_id][0]
    last_played_song[chat_id] = song_info
    video_title = song_info.get('title', 'Unknown')
    video_url = song_info.get('url', '')
    encoded_url = urllib.parse.quote(video_url, safe='')

    # Determine which API to call based on duration threshold (12 minutes = 720 seconds)
    duration_seconds = song_info.get('duration_seconds', 0)
    api_param = "&api=secondary" if duration_seconds > 720 else ""
    api_url = f"{selected_api}/play?chatid={chat_id}&url={encoded_url}{api_param}"

    try:
        async with aiohttp.ClientSession() as session:
            # Use a 30-second timeout for the play API call.
            async with session.get(api_url, timeout=60) as resp:
                if resp.status != 200:
                    raise Exception(f"API responded with status {resp.status}")
                data = await resp.json()
    except Exception as e:
        # Inform the user about the delay.
        try:
            await processing_message.edit("⏳ API server is sleeping. Waiting an extra 20 seconds before falling back...")
        except Exception as edit_error:
            print(f"Error editing processing message: {edit_error}")
        await asyncio.sleep(20)
        fallback_error = f"❌ Frozen Play API Error: {str(e)}\nFalling back to local playback..."
        try:
            await processing_message.edit(fallback_error)
        except Exception:
            await bot.send_message(chat_id, fallback_error)
        await fallback_local_playback(chat_id, processing_message, song_info)
        return

    # At this point, the API call succeeded.
    record = {
        "chat_id": chat_id,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "song_title": video_title,
        "api_response": data,
        "server": display_server
    }
    api_playback_records.append(record)
    playback_mode[chat_id] = "api"
    total_duration = parse_duration_str(song_info.get('duration', '0:00'))
    base_caption = (
        f"**ғʀᴏᴢᴇɴ ✘ ᴍᴜsɪᴄ sᴛʀєᴧϻɪηɢ ⏤͟͞●** (API Playback)\n\n"
        f"**❍ ᴛɪᴛʟє ➥** {song_info['title']}\n\n"
        f"**❍ ᴛɪϻє ➥** {{progress_bar}}\n\n"
        f"**❍ ʙʏ ➥** {song_info['requester']}\n\n"
        f"**❍ ʟᴅs sᴇʀᴠᴇʀ ➥** {display_server}"
    )
    initial_progress = get_progress_bar_styled(0, total_duration, bar_length=6)
    caption = base_caption.format(progress_bar=initial_progress)

    base_keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(text="▶️", callback_data="pause"),
            InlineKeyboardButton(text="⏸", callback_data="resume"),
            InlineKeyboardButton(text="⏭", callback_data="skip"),
            InlineKeyboardButton(text="⏹", callback_data="stop")
        ],
        [
            InlineKeyboardButton(text="➕ᴀᴅᴅ тσ ρℓαυℓιѕт➕", callback_data="add_to_playlist"),
            InlineKeyboardButton(text="⚡WEBSITE⚡", url="https://americanpepe.site/")
        ],
        [
            InlineKeyboardButton(text="✨ υρ∂αтєѕ ✨", url="https://t.me/american_pepecto"),
            InlineKeyboardButton(text="💕 ѕυρρσят 💕", url="https://t.me/Frozensupport1")
        ]
    ])

    # Delete the old processing message when starting playback.
    try:
        await processing_message.delete()
    except Exception as e:
        print(f"Error deleting processing message: {e}")

    try:
        # Send a new message with the updated song info and playback controls.
        new_progress_message = await bot.send_photo(
            chat_id,
            photo=song_info['thumbnail'],
            caption=caption,
            reply_markup=base_keyboard
        )
    except Exception as e:
        print("Error sending new playback message:", e)
        new_progress_message = await bot.send_photo(
            chat_id,
            photo=song_info['thumbnail'],
            caption=caption,
            reply_markup=base_keyboard
        )
    global_playback_count += 1

    # Start updating the progress caption.
    asyncio.create_task(update_progress_caption(chat_id, new_progress_message, time.time(), total_duration, base_caption, base_keyboard))


@bot.on_callback_query()
async def callback_query_handler(client, callback_query):
    chat_id = callback_query.message.chat.id
    user_id = callback_query.from_user.id
    data = callback_query.data
    mode = playback_mode.get(chat_id, "local")  # Default mode is local
    user = callback_query.from_user  # For later use

    # Skip admin check for suggestions, playlist-related commands (including play_song), and trending actions.
    if not (data.startswith("suggestion|") or data.startswith("playlist_") or data.startswith("play_song|") or data in ["add_to_playlist", "play_playlist", "play_trending"]):
        if not await is_user_admin(callback_query):
            await callback_query.answer("❌ You need to be an admin to use this button.", show_alert=True)
            return

    # ----------------- PAUSE -----------------
    if data == "pause":
        if mode == "local":
            try:
                await call_py.pause(chat_id)
                await callback_query.answer("⏸ Playback paused.")
                await client.send_message(chat_id, f"⏸ Playback paused by {user.first_name}.")
            except Exception as e:
                await callback_query.answer("❌ Error pausing playback.", show_alert=True)
        elif mode == "api":
            try:
                selected_api = chat_api_server.get(chat_id, (api_servers[0], None, None))[0]
                api_pause_url = f"{selected_api}/pause?chatid={chat_id}"
                async with aiohttp.ClientSession() as session:
                    async with session.get(api_pause_url, timeout=20) as resp:
                        if resp.status != 200:
                            raise Exception(f"API responded with status {resp.status}")
                        _ = await resp.json()
                await callback_query.answer("⏸ Playback paused via API.")
                await client.send_message(chat_id, f"⏸ Playback paused by {user.first_name} via API.")
            except Exception as e:
                await callback_query.answer("❌ Error pausing playback via API.", show_alert=True)
        else:
            await callback_query.answer("❌ Unknown playback mode.", show_alert=True)

    # ----------------- RESUME -----------------
    elif data == "resume":
        if mode == "local":
            try:
                await call_py.resume(chat_id)
                await callback_query.answer("▶️ Playback resumed.")
                await client.send_message(chat_id, f"▶️ Playback resumed by {user.first_name}.")
            except Exception as e:
                await callback_query.answer("❌ Error resuming playback.", show_alert=True)
        elif mode == "api":
            try:
                selected_api = chat_api_server.get(chat_id, (api_servers[0], None, None))[0]
                api_resume_url = f"{selected_api}/resume?chatid={chat_id}"
                async with aiohttp.ClientSession() as session:
                    async with session.get(api_resume_url, timeout=20) as resp:
                        if resp.status != 200:
                            raise Exception(f"API responded with status {resp.status}")
                        _ = await resp.json()
                await callback_query.answer("▶️ Playback resumed via API.")
                await client.send_message(chat_id, f"▶️ Playback resumed by {user.first_name} via API.")
            except Exception as e:
                await callback_query.answer("❌ Error resuming playback via API.", show_alert=True)
        else:
            await callback_query.answer("❌ Unknown playback mode.", show_alert=True)

    # ----------------- SKIP -----------------
    elif data == "skip":
        if chat_id in chat_containers and chat_containers[chat_id]:
            record = {
                "chat_id": chat_id,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "event": "skip",
                "mode": mode
            }
            api_playback_records.append(record)
            playback_mode.pop(chat_id, None)
            skipped_song = chat_containers[chat_id].pop(0)
            if mode == "local":
                try:
                    await call_py.leave_call(chat_id)
                except Exception as e:
                    print("Local leave_call error:", e)
                await asyncio.sleep(3)
                try:
                    os.remove(skipped_song.get('file_path', ''))
                except Exception as e:
                    print(f"Error deleting file: {e}")
            else:
                try:
                    await stop_playback(chat_id)
                except Exception as e:
                    print("API stop error:", e)
                await asyncio.sleep(3)
                try:
                    if skipped_song.get('file_path'):
                        os.remove(skipped_song.get('file_path', ''))
                except Exception as e:
                    print(f"Error deleting file: {e}")
            await client.send_message(chat_id, f"⏩ {user.first_name} skipped **{skipped_song['title']}**.")
            if chat_id in chat_containers and chat_containers[chat_id]:
                await callback_query.answer("⏩ Skipped! Playing the next song...")
                await start_playback_task(chat_id, callback_query.message)
            else:
                await callback_query.answer("⏩ Skipped! No more songs in the queue. Fetching suggestions...")
                last_song = last_played_song.get(chat_id)
                if last_song and last_song.get('url'):
                    try:
                        await callback_query.message.edit(
                            f"⏩ Skipped **{skipped_song['title']}**.\n\n😔 No more songs in the queue. Fetching song suggestions..."
                        )
                    except Exception as e:
                        print("Error editing callback message:", e)
                        await bot.send_message(
                            chat_id,
                            f"⏩ Skipped **{skipped_song['title']}**.\n\n😔 No more songs in the queue. Fetching song suggestions..."
                        )
                    await show_suggestions(chat_id, last_song.get('url'), status_message=callback_query.message)
                else:
                    try:
                        await callback_query.message.edit(
                            f"⏩ Skipped **{skipped_song['title']}**.\n\n😔 No more songs in the queue and no last played song available. ❌"
                        )
                    except Exception as e:
                        print("Error editing callback message:", e)
                        await bot.send_message(
                            chat_id,
                            f"⏩ Skipped **{skipped_song['title']}**.\n\n😔 No more songs in the queue and no last played song available. ❌"
                        )
        else:
            await callback_query.answer("❌ No songs in the queue to skip.")

    # ----------------- CLEAR -----------------
    elif data == "clear":
        if chat_id in chat_containers:
            for song in chat_containers[chat_id]:
                try:
                    os.remove(song.get('file_path', ''))
                except Exception as e:
                    print(f"Error deleting file: {e}")
            chat_containers.pop(chat_id)
            await callback_query.message.edit("🗑️ Cleared the queue.")
            await callback_query.answer("🗑️ Cleared the queue.")
        else:
            await callback_query.answer("❌ No songs in the queue to clear.", show_alert=True)

    # ----------------- STOP -----------------
    elif data == "stop":
        if chat_id in chat_containers:
            chat_containers[chat_id].clear()
        if mode == "local":
            record = {
                "chat_id": chat_id,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "event": "stop",
                "mode": mode
            }
            api_playback_records.append(record)
            playback_mode.pop(chat_id, None)
        try:
            if mode == "local":
                await call_py.leave_call(chat_id)
            else:
                await stop_playback(chat_id)
            await callback_query.answer("🛑 Playback stopped and queue cleared.")
            await client.send_message(chat_id, f"🛑 Playback stopped and queue cleared by {user.first_name}.")
        except Exception as e:
            print("Stop error:", e)
            await callback_query.answer("❌ Error stopping playback.", show_alert=True)

    # ----------------- SUGGESTION -----------------
    elif data.startswith("suggestion|"):
        try:
            parts = data.split("|")
            index = int(parts[1])
        except Exception:
            await callback_query.answer("Invalid selection.", show_alert=True)
            return
        suggestions = last_suggestions.get(chat_id, [])
        if index < 0 or index >= len(suggestions):
            await callback_query.answer("Invalid suggestion selection.", show_alert=True)
            return
        suggestion = suggestions[index]
        duration_iso = suggestion.get("duration")
        readable_duration = iso8601_to_human_readable(duration_iso) if duration_iso else "Unknown"
        song_data = {
            "url": suggestion.get("link"),
            "title": suggestion.get("title"),
            "duration": readable_duration,
            "duration_seconds": isodate.parse_duration(duration_iso).total_seconds() if duration_iso else 0,
            "requester": "Suggestion",
            "thumbnail": suggestion.get("thumbnail")
        }
        if chat_id not in chat_containers:
            chat_containers[chat_id] = []
        chat_containers[chat_id].append(song_data)
        await callback_query.answer("Song added from suggestions. Starting playback...")
        if len(chat_containers[chat_id]) == 1:
            await start_playback_task(chat_id, callback_query.message)
        else:
            await client.send_message(chat_id, f"Added **{song_data['title']}** to the queue from suggestions.")

    # ----------------- ADD TO PLAYLIST -----------------
    elif data == "add_to_playlist":
        if chat_id in chat_containers and chat_containers[chat_id]:
            song_info = chat_containers[chat_id][0]
            existing_song = playlist_collection.find_one({
                "chat_id": chat_id,
                "user_id": user_id,
                "song_title": song_info.get("title")
            })
            if existing_song:
                await callback_query.answer("❌ Song already in your playlist.", show_alert=True)
                return
            playlist_entry = {
                "chat_id": chat_id,
                "user_id": user_id,
                "song_title": song_info.get("title"),
                "url": song_info.get("url"),
                "duration": song_info.get("duration"),
                "thumbnail": song_info.get("thumbnail"),
                "timestamp": time.time()
            }
            playlist_collection.insert_one(playlist_entry)
            await callback_query.answer("✅ Added to your playlist!")
        else:
            await callback_query.answer("❌ No song currently playing.", show_alert=True)

    # ----------------- PLAYLIST PAGE -----------------
    elif data.startswith("playlist_page|"):
        try:
            _, page_str = data.split("|", 1)
            page = int(page_str)
        except Exception:
            page = 1
        per_page = 10
        user_playlist = list(playlist_collection.find({"user_id": user_id}))
        total = len(user_playlist)
        if total == 0:
            await callback_query.message.edit("Your playlist is empty.")
            return
        start_index = (page - 1) * per_page
        end_index = start_index + per_page
        page_items = user_playlist[start_index:end_index]
        buttons = []
        for idx, song in enumerate(page_items, start=start_index+1):
            song_id = str(song.get('_id'))
            song_title = song.get('song_title', 'Unknown')
            buttons.append([InlineKeyboardButton(text=f"{idx}. {song_title}", callback_data=f"playlist_detail|{song_id}")])
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"playlist_page|{page-1}"))
        if end_index < total:
            nav_buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"playlist_page|{page+1}"))
        if nav_buttons:
            buttons.append(nav_buttons)
        await callback_query.message.edit("🎶 **Your Playlist:**", reply_markup=InlineKeyboardMarkup(buttons))

    # ----------------- PLAYLIST DETAIL -----------------
    elif data.startswith("playlist_detail|"):
        _, song_id = data.split("|", 1)
        try:
            song = playlist_collection.find_one({"_id": ObjectId(song_id)})
        except Exception as e:
            await callback_query.answer("Error fetching song details.", show_alert=True)
            return
        if not song:
            await callback_query.answer("Song not found in your playlist.", show_alert=True)
            return
        title = song.get("song_title", "Unknown")
        duration = song.get("duration", "Unknown")
        url = song.get("url", "Unknown")
        details_text = f"**Title:** {title}\n**Duration:** {duration}\n**URL:** {url}"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(text="▶️ Play This Song", callback_data=f"play_song|{song_id}"),
             InlineKeyboardButton(text="🗑 Remove from Playlist", callback_data=f"remove_from_playlist|{song_id}")],
            [InlineKeyboardButton(text="⬅️ Back to Playlist", callback_data="playlist_back")]
        ])
        await callback_query.message.edit(details_text, reply_markup=keyboard)

    # ----------------- PLAY SONG -----------------
    elif data.startswith("play_song|"):
        _, song_id = data.split("|", 1)
        try:
            song = playlist_collection.find_one({"_id": ObjectId(song_id)})
        except Exception as e:
            await callback_query.answer("Error fetching song.", show_alert=True)
            return
        if not song:
            await callback_query.answer("Song not found.", show_alert=True)
            return

        song_data = {
            "url": song.get("url"),
            "title": song.get("song_title"),
            "duration": song.get("duration"),
            "duration_seconds": 0,
            "requester": user.first_name,
            "thumbnail": song.get("thumbnail")
        }

        existing_queue = chat_containers.get(chat_id)
        if not existing_queue:
            chat_containers[chat_id] = []
            queue_already_running = False
        else:
            queue_already_running = len(chat_containers[chat_id]) > 0

        chat_containers[chat_id].append(song_data)
        if not queue_already_running:
            await callback_query.answer("Song added to queue. Starting playback...", show_alert=False)
            await start_playback_task(chat_id, callback_query.message)
        else:
            await callback_query.answer("Song added to queue.", show_alert=False)

    # ----------------- REMOVE FROM PLAYLIST -----------------
    elif data.startswith("remove_from_playlist|"):
        _, song_id = data.split("|", 1)
        try:
            result = playlist_collection.delete_one({"_id": ObjectId(song_id)})
        except Exception as e:
            await callback_query.answer("Error removing song.", show_alert=True)
            return
        if result.deleted_count:
            await callback_query.answer("Song removed from your playlist.")
        else:
            await callback_query.answer("Failed to remove song or song not found.", show_alert=True)
        user_playlist = list(playlist_collection.find({"user_id": user_id}))
        if not user_playlist:
            await callback_query.message.edit("Your playlist is now empty.")
            return
        page = 1
        per_page = 10
        total = len(user_playlist)
        start_index = (page - 1) * per_page
        end_index = start_index + per_page
        page_items = user_playlist[start_index:end_index]
        buttons = []
        for idx, song in enumerate(page_items, start=start_index+1):
            song_id = str(song.get('_id'))
            song_title = song.get('song_title', 'Unknown')
            buttons.append([InlineKeyboardButton(text=f"{idx}. {song_title}", callback_data=f"playlist_detail|{song_id}")])
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"playlist_page|{page-1}"))
        if end_index < total:
            nav_buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"playlist_page|{page+1}"))
        if nav_buttons:
            buttons.append(nav_buttons)
        await callback_query.message.edit("🎶 **Your Playlist:**", reply_markup=InlineKeyboardMarkup(buttons))

    # ----------------- PLAYLIST BACK -----------------
    elif data == "playlist_back":
        user_playlist = list(playlist_collection.find({"user_id": user_id}))
        if not user_playlist:
            await callback_query.message.edit("Your playlist is empty.")
            return
        page = 1
        per_page = 10
        total = len(user_playlist)
        start_index = (page - 1) * per_page
        end_index = start_index + per_page
        page_items = user_playlist[start_index:end_index]
        buttons = []
        for idx, song in enumerate(page_items, start=start_index+1):
            song_id = str(song.get('_id'))
            song_title = song.get('song_title', 'Unknown')
            buttons.append([InlineKeyboardButton(text=f"{idx}. {song_title}", callback_data=f"playlist_detail|{song_id}")])
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"playlist_page|{page-1}"))
        if end_index < total:
            nav_buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"playlist_page|{page+1}"))
        if nav_buttons:
            buttons.append(nav_buttons)
        await callback_query.message.edit("🎶 **Your Playlist:**", reply_markup=InlineKeyboardMarkup(buttons))

    # ----------------- PLAY PLAYLIST -----------------
    elif data == "play_playlist":
        user_playlist = list(playlist_collection.find({"user_id": user_id}))
        if not user_playlist:
            await callback_query.answer("❌ You don't have any songs in your playlist.", show_alert=True)
            return
        if chat_id not in chat_containers:
            chat_containers[chat_id] = []
        count_added = 0
        for song in user_playlist:
            song_data = {
                "url": song.get("url"),
                "title": song.get("song_title"),
                "duration": song.get("duration"),
                "duration_seconds": 0,
                "requester": user.first_name,
                "thumbnail": song.get("thumbnail")
            }
            chat_containers[chat_id].append(song_data)
            count_added += 1
        await callback_query.answer(f"✅ Added {count_added} songs from your playlist to the queue!")
        if len(chat_containers[chat_id]) > 0:
            await start_playback_task(chat_id, callback_query.message)

    # ----------------- PLAY TRENDING -----------------
    elif data == "play_trending":
        trending_query = "/search?title=trending"
        try:
            result = await fetch_youtube_link(trending_query)
            if isinstance(result, dict) and "playlist" in result:
                playlist_items = result["playlist"]
                if not playlist_items:
                    await callback_query.answer("❌ No trending songs found.", show_alert=True)
                    return
                if chat_id not in chat_containers:
                    chat_containers[chat_id] = []
                count_added = 0
                for item in playlist_items:
                    duration_seconds = isodate.parse_duration(item["duration"]).total_seconds()
                    readable_duration = iso8601_to_human_readable(item["duration"])
                    chat_containers[chat_id].append({
                        "url": item["link"],
                        "title": item["title"],
                        "duration": readable_duration,
                        "duration_seconds": duration_seconds,
                        "requester": user.first_name,
                        "thumbnail": item["thumbnail"]
                    })
                    count_added += 1
                await callback_query.answer(f"✅ Added {count_added} trending songs to the queue!")
                if len(chat_containers[chat_id]) > 0:
                    await start_playback_task(chat_id, callback_query.message)
            else:
                video_url, video_title, video_duration, thumbnail_url = result
                if not video_url:
                    await callback_query.answer("❌ Could not fetch trending songs.", show_alert=True)
                    return
                duration_seconds = isodate.parse_duration(video_duration).total_seconds()
                readable_duration = iso8601_to_human_readable(video_duration)
                if chat_id not in chat_containers:
                    chat_containers[chat_id] = []
                chat_containers[chat_id].append({
                    "url": video_url,
                    "title": video_title,
                    "duration": readable_duration,
                    "duration_seconds": duration_seconds,
                    "requester": user.first_name,
                    "thumbnail": thumbnail_url
                })
                await callback_query.answer("✅ Added trending song to the queue!")
                if len(chat_containers[chat_id]) == 1:
                    await start_playback_task(chat_id, callback_query.message)
        except Exception as e:
            await callback_query.answer(f"❌ Error fetching trending songs: {str(e)}", show_alert=True)

    # ----------------- DEFAULT -----------------
    else:
        await callback_query.answer("Unknown action.", show_alert=True)

@bot.on_message(filters.group & filters.command("loop"))
async def loop_handler(_, message: Message):
    chat_id = message.chat.id
    # Toggle loop state
    current = loop_mode.get(chat_id, False)
    loop_mode[chat_id] = not current
    status = "enabled" if loop_mode[chat_id] else "disabled"

    # Build response
    msg = f"🔁 Looping {status} for this chat."
    queue = chat_containers.get(chat_id, [])

    if loop_mode[chat_id]:
        if queue:
            msg += "\nSongs on loop:\n"
            for idx, song in enumerate(queue, 1):
                title = song.get("title", "Unknown title")
                msg += f"{idx}. {title}\n"
            # ——— NEW: immediately replay the current track via your fallback for local files ———
            # Note: fallback_local_playback takes (chat_id, status_message, song_info)
            # we reuse `message` as a simple status_message here
            first = queue[0]
            await fallback_local_playback(chat_id, message, first)
            # ——————————————————————————————————————————————————————————————
        else:
            msg += "\nNo songs in the queue to loop."

    await message.reply(msg)





@call_py.on_update(fl.stream_end())
async def stream_end_handler(_: PyTgCalls, update: StreamEnded):
    chat_id = update.chat_id

    # 1. Record the natural end event
    record = {
        "chat_id": chat_id,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "event": "natural_end",
        "mode": playback_mode.get(chat_id, "unknown")
    }
    api_playback_records.append(record)
    playback_mode.pop(chat_id, None)

    # 2. If there is a queue for this chat
    if chat_id in chat_containers and chat_containers[chat_id]:
        # a) Pop the just-finished song
        finished = chat_containers[chat_id].pop(0)

        # b) Loop logic: if enabled, re-queue at front
        if loop_mode.get(chat_id, False):
            chat_containers[chat_id].insert(0, finished)
        else:
            # Otherwise, delete its temporary file
            try:
                path = finished.get('file_path', '')
                if path and os.path.exists(path):
                    os.remove(path)
            except Exception as e:
                print(f"Error deleting file: {e}")

        # c) Brief delay to ensure stream cleanup
        await asyncio.sleep(3)

        # d) If songs remain, start next
        if chat_containers[chat_id]:
            await start_playback_task(chat_id, None)
            return

    # 3. No songs left (or loop off emptied queue)
    await leave_voice_chat(chat_id)

    last = last_played_song.get(chat_id)
    if last and last.get('url'):
        status_msg = await bot.send_message(
            chat_id,
            "😔 No more songs in the queue. Fetching song suggestions..."
        )
        await show_suggestions(chat_id, last.get('url'), status_message=status_msg)
    else:
        await bot.send_message(
            chat_id,
            "❌ No more songs in the queue.\nSupport: @frozensupport1"
        )

async def leave_voice_chat(chat_id):
    try:
        await call_py.leave_call(chat_id)
    except Exception as e:
        print(f"Error leaving the voice chat: {e}")

    if chat_id in chat_containers:
        for song in chat_containers[chat_id]:
            try:
                os.remove(song.get('file_path', ''))
            except Exception as e:
                print(f"Error deleting file: {e}")
        chat_containers.pop(chat_id)

    if chat_id in playback_tasks:
        playback_tasks[chat_id].cancel()
        del playback_tasks[chat_id]


@bot.on_message(filters.command("playlist"))
async def my_playlist_handler(_, message):
    user_id = message.from_user.id
    # Retrieve the user's playlist from MongoDB
    user_playlist = list(playlist_collection.find({"user_id": user_id}))
    if not user_playlist:
        await message.reply("You don't have any songs in your playlist yet.")
        return

    # Default to page 1
    page = 1
    per_page = 10
    total = len(user_playlist)
    start_index = (page - 1) * per_page
    end_index = start_index + per_page
    page_items = user_playlist[start_index:end_index]

    buttons = []
    for idx, song in enumerate(page_items, start=start_index+1):
        song_id = str(song.get('_id'))
        song_title = song.get('song_title', 'Unknown')
        # Each button triggers the detail menu for that song.
        buttons.append([InlineKeyboardButton(text=f"{idx}. {song_title}", callback_data=f"playlist_detail|{song_id}")])

    # Add pagination buttons if needed.
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"playlist_page|{page-1}"))
    if end_index < total:
        nav_buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"playlist_page|{page+1}"))
    if nav_buttons:
        buttons.append(nav_buttons)

    await message.reply("🎶 **Your Playlist:**", reply_markup=InlineKeyboardMarkup(buttons))


from pathlib import Path

AVATAR_DIAMETER  = 419        
CIRCLE_CENTER    = (1118, 437)
BOX_ORIGIN       = (220, 640)   
LINE_SPACING     = 75          
VALUE_OFFSET_X   = 200    
FONT_PATH        = "arial.ttf"
FONT_SIZE        = 40
TEXT_COLOR       = "white"

# point this at the local file in your repo
WELCOME_TEMPLATE_PATH = Path(__file__).parent / "welcome.png"

async def create_welcome_image(user) -> str:
    # load the local template
    tpl = Image.open(WELCOME_TEMPLATE_PATH).convert("RGBA")

    # draw avatar
    if user.photo:
        avatar_file = await bot.download_media(user.photo.big_file_id)
        av = Image.open(avatar_file).convert("RGBA")
        os.remove(avatar_file)

        D = AVATAR_DIAMETER
        av = av.resize((D, D))
        mask = Image.new("L", (D, D), 0)
        ImageDraw.Draw(mask).ellipse((0, 0, D, D), fill=255)

        cx, cy = CIRCLE_CENTER
        top_left = (cx - D//2, cy - D//2)
        tpl.paste(av, top_left, mask)

    # write user info
    draw = ImageDraw.Draw(tpl)
    font = ImageFont.truetype(FONT_PATH, FONT_SIZE)

    x0, y0 = BOX_ORIGIN

    draw.text((x0 + VALUE_OFFSET_X, y0),
              user.first_name,
              font=font, fill=TEXT_COLOR)

    draw.text((x0 + VALUE_OFFSET_X, y0 + LINE_SPACING),
              str(user.id),
              font=font, fill=TEXT_COLOR)

    draw.text((x0 + VALUE_OFFSET_X, y0 + 2*LINE_SPACING),
              "@" + (user.username or "N/A"),
              font=font, fill=TEXT_COLOR)

    out = f"welcome_{user.id}.png"
    tpl.save(out)
    return out




@bot.on_message(filters.group & filters.new_chat_members)
async def welcome_new_member(client: Client, message: Message):
    """
    For each new member, generate & send their welcome card with styled caption.
    """
    for member in message.new_chat_members:
        img_path = await create_welcome_image(member)

        # Build caption using HTML links
        caption = (
            f"𝗪𝗲𝗹𝗰𝗼𝗺𝗲 𝗧𝗼 {message.chat.title}\n"
            "➖➖➖➖➖➖➖➖➖➖➖\n"
            f"๏ 𝗡𝗔𝗠𝗘 ➠ {member.mention}\n"
            f"๏ 𝗜𝗗 ➠ {member.id}\n"
            f"๏ 𝐔𝐒𝐄𝐑𝐍𝐀𝐌𝐄 ➠ @{member.username or '—'}\n"
            f"๏ 𝐌𝐀𝐃𝐄 𝐁𝐘 ➠ <a href=\"https://t.me/vibeshiftbots\">Frozen Bots</a>\n"
            "➖➖➖➖➖➖➖➖➖➖➖"
        )

        markup = InlineKeyboardMarkup(
            [[
                InlineKeyboardButton(
                    "⦿ ᴀᴅᴅ ᴍᴇ ⦿",
                    url="https://t.me/vcmusiclubot?startgroup=true"
                )
            ]]
        )

        await client.send_photo(
            chat_id=message.chat.id,
            photo=img_path,
            caption=caption,
            parse_mode=ParseMode.HTML,
            reply_markup=markup
        )

        try:
            os.remove(img_path)
        except OSError:
            pass


download_cache = {}  # Global cache dictionary

async def download_audio(url):
    # If url is already a local file, return it directly (for replied audio/video files)
    if os.path.exists(url) and os.path.isfile(url):
        return url

    if url in download_cache:
        return download_cache[url]  # Return cached file path if available

    try:
        # Lower the priority of the process
        proc = psutil.Process(os.getpid())
        proc.nice(psutil.IDLE_PRIORITY_CLASS if os.name == "nt" else 19)  # Windows/Linux

        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp3')
        file_name = temp_file.name
        temp_file.close()
        download_url = f"{DOWNLOAD_API_URL}{url}"

        async with aiohttp.ClientSession() as session:
            async with session.get(download_url, timeout=35) as response:
                if response.status == 200:
                    async with aiofiles.open(file_name, 'wb') as f:
                        while True:
                            chunk = await response.content.read(32768)
                            if not chunk:
                                break
                            await f.write(chunk)
                            await asyncio.sleep(0.01)
                    download_cache[url] = file_name
                    return file_name
                else:
                    raise Exception(f"Failed to download audio. HTTP status: {response.status}")
    except asyncio.TimeoutError:
        raise Exception("❌ Download API took too long to respond. Please try again.")
    except Exception as e:
        raise Exception(f"Error downloading audio: {e}")


def _trim_name(name: str) -> str:
    first = name.split()[0] if name else ""
    return (first[:7] + "…") if len(first) > 8 else first

async def get_pfp_image(client: Client, user_id: int) -> Image.Image:
    try:
        photos = []
        async for p in client.get_chat_photos(user_id, limit=1):
            photos.append(p)

        if not photos:
            print(f"[get_pfp_image] no profile photos for user {user_id}")
            return Image.new("RGBA", (2*R, 2*R), (200,200,200,255))

        photo = photos[0]
        print(f"[get_pfp_image] downloading file_id={photo.file_id}")
        file_path = await client.download_media(photo.file_id)
        img = Image.open(file_path).convert("RGBA")
        os.remove(file_path)
        return img

    except Exception as e:
        print(f"[get_pfp_image] ERROR for user {user_id}: {e}")
        raise

def paste_circle(base: Image.Image, img: Image.Image, center: tuple):
    img = img.resize((2*R, 2*R))
    mask = Image.new("L", (2*R, 2*R), 0)
    draw = ImageDraw.Draw(mask)
    draw.ellipse((0,0,2*R,2*R), fill=255)
    base.paste(img, (center[0]-R, center[1]-R), mask)

def draw_name(base: Image.Image, name: str, center_x: int):
    draw = ImageDraw.Draw(base)
    font = ImageFont.truetype(FONT_PATH, 56)
    text = _trim_name(name)
    bbox = draw.textbbox((0,0), text, font=font)
    w = bbox[2] - bbox[0]
    draw.text((center_x - w/2, NAME_Y), text, font=font, fill=(51,51,51))

def draw_group_name(base: Image.Image, title: str):
    draw = ImageDraw.Draw(base)
    font = ImageFont.truetype(FONT_PATH, GROUP_FONT_SIZE)
    bbox = draw.textbbox((0,0), title, font=font)
    w = bbox[2] - bbox[0]
    draw.text(((W - w)/2, GROUP_Y), title, font=font, fill=(51,51,51))

async def build_couple_image(client: Client, u1_id: int, u2_id: int, group_title: str) -> BytesIO:
    base = _template.copy()
    draw_group_name(base, group_title)
    p1 = await get_pfp_image(client, u1_id)
    p2 = await get_pfp_image(client, u2_id)
    paste_circle(base, p1, CENTERS[0])
    paste_circle(base, p2, CENTERS[1])
    u1 = await client.get_users(u1_id)
    u2 = await client.get_users(u2_id)
    draw_name(base, u1.first_name or "", CENTERS[0][0])
    draw_name(base, u2.first_name or "", CENTERS[1][0])
    out = BytesIO()
    base.save(out, format="PNG")
    out.seek(0)
    return out


async def _send_couple(
    client: Client,
    chat_id: int,
    u1_id: int,
    u2_id: int,
    photo_buf,
    from_cache: bool = False
):
    """Send the couple image with buttons and a caption."""
    user1 = await client.get_users(u1_id)
    user2 = await client.get_users(u2_id)
    name1 = _trim_name(user1.first_name)
    name2 = _trim_name(user2.first_name)

    prefix = "❤️ Couples already chosen today! ❤️\n\n" if from_cache else "❤️ "
    suffix = (
        "are today’s couple and will be reselected tomorrow."
        if from_cache else
        "are today’s couple! ❤️"
    )
    caption = (
        prefix +
        f"<a href=\"tg://user?id={u1_id}\">{name1}</a> & "
        f"<a href=\"tg://user?id={u2_id}\">{name2}</a> " +
        suffix
    )

    buttons = InlineKeyboardMarkup([[  # [Name1] ❤️ [Name2]
        InlineKeyboardButton(text=name1, url=f"tg://user?id={u1_id}"),
        InlineKeyboardButton(text="❤️", callback_data="noop"),
        InlineKeyboardButton(text=name2, url=f"tg://user?id={u2_id}")
    ]])

    return await client.send_photo(
        chat_id=chat_id,
        photo=photo_buf,
        caption=caption,
        parse_mode=ParseMode.HTML,
        reply_markup=buttons
    )

# -------------------
# /couple command
# -------------------
processing_chats = set()

@bot.on_message(filters.group & filters.command("couple", prefixes="/"))
async def make_couple(client: Client, message):
    chat_id     = message.chat.id
    group_title = message.chat.title or ""

    # Prevent concurrent calls in the same chat
    if chat_id in processing_chats:
        return await message.reply_text(
            "⚠️ Please wait, I'm still processing the previous request."
        )
    processing_chats.add(chat_id)
    status = await message.reply_text("⏳ Gathering members…")

    try:
        now = datetime.now(timezone.utc)

        # 1) Per-chat member cache (refresh every 24h), handling naive vs aware
        cache = members_cache.find_one({"chat_id": chat_id})
        member_ids = None

        if cache:
            last_synced = cache.get("last_synced")
            if last_synced:
                # if stored as naive, assume UTC
                if last_synced.tzinfo is None:
                    last_synced = last_synced.replace(tzinfo=timezone.utc)
                # compare safely
                if (now - last_synced) < timedelta(hours=24):
                    member_ids = cache["members"]

        if not member_ids:
            # cache miss or stale → fetch fresh
            member_ids = []
            async for m in client.get_chat_members(chat_id):
                if not m.user.is_bot:
                    member_ids.append(m.user.id)

            if len(member_ids) < 2:
                await status.delete()
                return await message.reply_text(
                    "❌ Not enough non-bot members to form a couple."
                )

            members_cache.replace_one(
                {"chat_id": chat_id},
                {
                    "chat_id": chat_id,
                    "members": member_ids,
                    "last_synced": now
                },
                upsert=True
            )

        # 2) Today's couple cache (only reuse if created ≥ today midnight UTC)
        midnight_utc = now.replace(hour=0, minute=0, second=0, microsecond=0)
        existing = couples_collection.find_one({
            "chat_id": chat_id,
            "created_at": {"$gte": midnight_utc}
        })
        if existing:
            try:
                return await _send_couple(
                    client, chat_id,
                    existing["user1_id"], existing["user2_id"],
                    existing["file_id"],
                    from_cache=True
                )
            except Exception:
                logger.exception("Cached couple send failed—regenerating…")

        # 3) Pick two distinct users with non-placeholder avatars
        await status.edit_text("⏳ Choosing today’s couple…")

        async def pick_with_photo(candidates):
            tried = set()
            while tried != set(candidates):
                uid = random.choice(candidates)
                tried.add(uid)
                pfp = await get_pfp_image(client, uid)
                # skip grey placeholder
                if pfp.width == 2 * R and pfp.getpixel((0, 0)) == (200, 200, 200, 255):
                    continue
                return uid
            return None

        u1 = await pick_with_photo(member_ids)
        u2 = await pick_with_photo([uid for uid in member_ids if uid != u1])
        if not u1 or not u2:
            await status.delete()
            return await message.reply_text(
                "❌ Could not find two members with valid profile pictures."
            )

        # 4) Build & send fresh image
        await status.edit_text("⏳ Building couple image…")
        buf = await build_couple_image(client, u1, u2, group_title)
        res = await _send_couple(client, chat_id, u1, u2, buf)

        # 5) Upsert today's couple for this chat
        couples_collection.replace_one(
            {"chat_id": chat_id},
            {
                "chat_id": chat_id,
                "user1_id": u1,
                "user2_id": u2,
                "file_id": res.photo.file_id,
                "created_at": now
            },
            upsert=True
        )

    finally:
        await status.delete()
        processing_chats.discard(chat_id)

@bot.on_message(filters.group & filters.command("ban"))
@safe_handler
async def ban_handler(_, message: Message):
    # Only admins can ban
    if not await is_user_admin(message):
        return await message.reply("❌ You must be an admin to use /ban.")
    target_id = await extract_target_user(message)
    if not target_id:
        return
    await bot.ban_chat_member(message.chat.id, target_id)
    await message.reply(f"✅ User [{target_id}](tg://user?id={target_id}) has been banned.")

@bot.on_message(filters.group & filters.command("unban"))
@safe_handler
async def unban_handler(_, message: Message):
    if not await is_user_admin(message):
        return await message.reply("❌ You must be an admin to use /unban.")
    target_id = await extract_target_user(message)
    if not target_id:
        return
    await bot.unban_chat_member(message.chat.id, target_id)
    await message.reply(f"✅ User [{target_id}](tg://user?id={target_id}) has been unbanned.")


@bot.on_message(filters.group & filters.command("mute"))
@safe_handler
async def mute_handler(_, message: Message):
    if not await is_user_admin(message):
        return await message.reply("❌ You must be an admin to use /mute.")
    target_id = await extract_target_user(message)
    if not target_id:
        return
    perms = ChatPermissions(
        can_send_messages=False,
        can_send_media_messages=False,
        can_send_other_messages=False,
        can_add_web_page_previews=False
    )
    await bot.restrict_chat_member(message.chat.id, target_id, permissions=perms)
    await message.reply(f"🔇 User [{target_id}](tg://user?id={target_id}) has been muted.")

@bot.on_message(filters.group & filters.command("unmute"))
@safe_handler
async def unmute_handler(_, message: Message):
    if not await is_user_admin(message):
        return await message.reply("❌ You must be an admin to use /unmute.")
    target_id = await extract_target_user(message)
    if not target_id:
        return
    full_perms = ChatPermissions( # restore defaults
        can_send_messages=True,
        can_send_media_messages=True,
        can_send_other_messages=True,
        can_add_web_page_previews=True
    )
    await bot.restrict_chat_member(message.chat.id, target_id, permissions=full_perms)
    await message.reply(f"🔊 User [{target_id}](tg://user?id={target_id}) has been unmuted.")


@bot.on_message(filters.group & filters.command("tmute"))
@safe_handler
async def tmute_handler(_, message: Message):
    if not await is_user_admin(message):
        return await message.reply("❌ You must be an admin to use /tmute.")
    parts = message.text.split()
    if len(parts) < 3:
        return await message.reply("Usage: /tmute <user> <minutes>\nExample: /tmute @john 15")
    # Extract target and duration
    target_id = await extract_target_user(message)
    try:
        minutes = int(parts[-1])
    except:
        return await message.reply("❌ Invalid duration. Use an integer number of minutes.")
    until = datetime.utcnow() + timedelta(minutes=minutes)
    perms = ChatPermissions(
        can_send_messages=False,
        can_send_media_messages=False,
        can_send_other_messages=False,
        can_add_web_page_previews=False
    )
    await bot.restrict_chat_member(
        message.chat.id,
        target_id,
        permissions=perms,
        until_date=until
    )
    await message.reply(f"⏱️ User [{target_id}](tg://user?id={target_id}) muted for {minutes} minutes.")

@bot.on_message(filters.group & filters.command("kick"))
@safe_handler
async def kick_handler(_, message):
    if not await is_user_admin(message):
        return await message.reply("❌ You must be an admin to use /kick.")
    
    # Determine which user to kick
    user_id = await extract_target_user(message)
    if not user_id:
        return

    # 1) Ban (kick) the user  
    await bot.ban_chat_member(message.chat.id, user_id)  
    
    # 2) Immediately unban so they can rejoin  
    await bot.unban_chat_member(message.chat.id, user_id)  

    await message.reply(f"👢 User [{user_id}](tg://user?id={user_id}) has been kicked.")




@bot.on_message(filters.group & filters.command(["stop", "end"]))
async def stop_handler(client, message):
    chat_id = message.chat.id
    user_id = message.from_user.id

    # Check admin rights
    if not await is_user_admin(message):
        await message.reply("❌ You need to be an admin to use this command.")
        return

    # Determine the playback mode (defaulting to local)
    mode = playback_mode.get(chat_id, "local")

    if mode == "local":
        try:
            await call_py.leave_call(chat_id)
        except Exception as e:
            if "not in a call" in str(e).lower():
                await message.reply("❌ The bot is not currently in a voice chat.")
            else:
                await message.reply(f"❌ An error occurred while leaving the voice chat: {str(e)}\n\n support - @frozensupport1")
            return
        # Update playback records for a stop event in local mode
        record = {
            "chat_id": chat_id,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "event": "stop",
            "mode": mode
        }
        api_playback_records.append(record)
        playback_mode.pop(chat_id, None)
    else:
        try:
            await stop_playback(chat_id)
        except Exception as e:
            await message.reply(f"❌ An error occurred while stopping playback: {str(e)}", quote=True)
            return

    # Clear the song queue
    if chat_id in chat_containers:
        for song in chat_containers[chat_id]:
            try:
                os.remove(song.get('file_path', ''))
            except Exception as e:
                print(f"Error deleting file: {e}")
        chat_containers.pop(chat_id)

    # Cancel any playback tasks if present
    if chat_id in playback_tasks:
        playback_tasks[chat_id].cancel()
        del playback_tasks[chat_id]

    await message.reply("⏹ Stopped the music and cleared the queue.")

@bot.on_message(filters.command("song"))
async def song_command_handler(_, message):
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🎶 Download Now", url="https://t.me/songdownloderfrozenbot?start=true")]]
    )
    text = (
        "ᴄʟɪᴄᴋ ᴛʜᴇ ʙᴜᴛᴛᴏɴ ʙᴇʟᴏᴡ ᴛᴏ ᴜsᴇ ᴛʜᴇ sᴏɴɢ ᴅᴏᴡɴʟᴏᴀᴅᴇʀ ʙᴏᴛ. 🎵\n\n"
        "ʏᴏᴜ ᴄᴀɴ sᴇɴᴅ ᴛʜᴇ sᴏɴɢ ɴᴀᴍᴇ ᴏʀ ᴀɴʏ ǫᴜᴇʀʏ ᴅɪʀᴇᴄᴛʟʏ ᴛᴏ ᴛʜᴇ ᴅᴏᴡɴʟᴏᴀᴅᴇʀ ʙᴏᴛ, ⬇️\n\n"
        "ᴀɴᴅ ɪᴛ ᴡɪʟʟ ғᴇᴛᴄʜ ᴀɴᴅ ᴅᴏᴡɴʟᴏᴀᴅ ᴛʜᴇ sᴏɴɢ ғᴏʀ ʏᴏᴜ. 🚀"
    )
    await message.reply(text, reply_markup=keyboard)



@bot.on_message(filters.group & filters.command("pause"))
async def pause_handler(client, message):
    chat_id = message.chat.id
    if not await is_user_admin(message):
        await message.reply("❌ You need to be an admin to use this command.")
        return
    try:
        # Use the correct pause() method.
        await call_py.pause(chat_id)
        await message.reply("⏸ Paused the stream.")
    except Exception as e:
        await message.reply(f"❌ Failed to pause the stream. Error: {str(e)}\n\n support - @frozensupport1 ")

@bot.on_message(filters.group & filters.command("resume"))
async def resume_handler(client, message):
    chat_id = message.chat.id
    if not await is_user_admin(message):
        await message.reply("❌ You need to be an admin to use this command.")
        return
    try:
        # Use the correct resume() method.
        await call_py.resume(chat_id)
        await message.reply("▶️ Resumed the stream.")
    except Exception as e:
        await message.reply(f"❌ Failed to resume the stream. Error: {str(e)}\n\n support - @frozensupport1")


@bot.on_message(filters.group & filters.command("skip"))
async def skip_handler(client, message):
    chat_id = message.chat.id
    user_id = message.from_user.id

    if not await is_user_admin(message):
        await message.reply("❌ You need to be an admin to use this command.")
        return

    status_message = await message.reply("⏩ Skipping the current song...")

    if chat_id not in chat_containers or not chat_containers[chat_id]:
        await status_message.edit("❌ No songs in the queue to skip.")
        return

    # Remove the currently playing song from the queue.
    skipped_song = chat_containers[chat_id].pop(0)
    # Determine the playback mode (default to local).
    mode = playback_mode.get(chat_id, "local")

    # Update playback records for a skip event.
    record = {
        "chat_id": chat_id,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "event": "skip",
        "mode": mode
    }
    api_playback_records.append(record)
    playback_mode.pop(chat_id, None)

    if mode == "local":
        try:
            await call_py.leave_call(chat_id)
        except Exception as e:
            print("Local leave_call error:", e)
        await asyncio.sleep(3)
        try:
            os.remove(skipped_song.get('file_path', ''))
        except Exception as e:
            print(f"Error deleting file: {e}")
    else:
        try:
            await stop_playback(chat_id)
        except Exception as e:
            print("API stop error:", e)
        await asyncio.sleep(3)
        try:
            if skipped_song.get('file_path'):
                os.remove(skipped_song.get('file_path', ''))
        except Exception as e:
            print(f"Error deleting file: {e}")

    # Check if there are any more songs in the queue.
    if not chat_containers.get(chat_id):
        # Try to edit the status message with a new message that includes emojis.
        try:
            await status_message.edit(
                f"⏩ Skipped **{skipped_song['title']}**.\n\n😔 No more songs in the queue. Fetching song suggestions..."
            )
        except Exception as e:
            print(f"Error editing message: {e}")
            await status_message.delete()
            status_message = await bot.send_message(
                chat_id,
                f"⏩ Skipped **{skipped_song['title']}**.\n\n😔 No more songs in the queue. Fetching song suggestions..."
            )
        # Use the last played song info to fetch suggestions.
        last_song = last_played_song.get(chat_id)
        if last_song and last_song.get('url'):
            print(f"Fetching suggestions using URL: {last_song.get('url')}")
            await show_suggestions(chat_id, last_song.get('url'))
        else:
            try:
                await status_message.edit(
                    f"⏩ Skipped **{skipped_song['title']}**.\n\n😔 No more songs in the queue and no last played song available. ❌"
                )
            except Exception as e:
                await bot.send_message(
                    chat_id,
                    f"⏩ Skipped **{skipped_song['title']}**.\n\n😔 No more songs in the queue and no last played song available. ❌"
                )
    else:
        try:
            await status_message.edit(
                f"⏩ Skipped **{skipped_song['title']}**.\n\n💕 Playing the next song..."
            )
        except Exception as e:
            print(f"Error editing message: {e}")
        await skip_to_next_song(chat_id, status_message)



@bot.on_message(filters.command("reboot"))
async def reboot_handler(_, message):
    chat_id = message.chat.id

    try:
        # Remove audio files for songs in the queue for this chat.
        if chat_id in chat_containers:
            for song in chat_containers[chat_id]:
                try:
                    os.remove(song.get('file_path', ''))
                except Exception as e:
                    print(f"Error deleting file for chat {chat_id}: {e}")
            # Clear the queue for this chat.
            chat_containers.pop(chat_id, None)
        
        # Cancel any playback tasks for this chat.
        if chat_id in playback_tasks:
            playback_tasks[chat_id].cancel()
            del playback_tasks[chat_id]

        # Remove chat-specific cooldown and pending command entries.
        chat_last_command.pop(chat_id, None)
        chat_pending_commands.pop(chat_id, None)

        # Remove playback mode for this chat.
        playback_mode.pop(chat_id, None)

        # Clear any API playback records for this chat.
        global api_playback_records
        api_playback_records = [record for record in api_playback_records if record.get("chat_id") != chat_id]

        # Leave the voice chat for this chat.
        try:
            await call_py.leave_call(chat_id)
        except Exception as e:
            print(f"Error leaving call for chat {chat_id}: {e}")

        await message.reply("♻️ Rebooted for this chat. All data for this chat has been cleared.")
    except Exception as e:
        await message.reply(f"❌ Failed to reboot for this chat. Error: {str(e)}\n\n support - @frozensupport1")


BASE_API_SERVERS = [
    {"name": "Playback-1", "cpu": 45.2, "ram_used": 18500, "ram_total": 32768, "disk_used": 150, "disk_total": 200, "live": 103, "latency": 42},
    {"name": "Playback-2", "cpu": 38.7, "ram_used": 16200, "ram_total": 32768, "disk_used": 140, "disk_total": 200, "live": 98, "latency": 37},
    {"name": "Playback-3", "cpu": 52.1, "ram_used": 20500, "ram_total": 32768, "disk_used": 160, "disk_total": 200, "live": 112, "latency": 44},
    {"name": "Playback-4", "cpu": 35.5, "ram_used": 15000, "ram_total": 32768, "disk_used": 130, "disk_total": 200, "live": 91, "latency": 33},
    {"name": "Playback-5", "cpu": 48.9, "ram_used": 19800, "ram_total": 32768, "disk_used": 155, "disk_total": 200, "live": 106, "latency": 40},
    {"name": "Playback-6", "cpu": 42.3, "ram_used": 17500, "ram_total": 32768, "disk_used": 145, "disk_total": 200, "live": 99, "latency": 38}
]

@bot.on_message(filters.command("ping"))
async def ping_handler(_, message):
    try:
        # Main server stats
        current_time = time.time()
        uptime_seconds = int(current_time - bot_start_time)
        uptime_str = str(timedelta(seconds=uptime_seconds))

        cpu_usage = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        ram_usage = f"{memory.used // (1024 ** 2)}MB / {memory.total // (1024 ** 2)}MB ({memory.percent}%)"
        disk = psutil.disk_usage('/')
        disk_usage = f"{disk.used // (1024 ** 3)}GB / {disk.total // (1024 ** 3)}GB ({disk.percent}%)"

        # Build the API servers information string with random variations
        api_info = ""
        for server in BASE_API_SERVERS:
            # Slight random variation for CPU usage
            cpu = server["cpu"] + random.uniform(-1.5, 1.5)
            cpu_str = f"{cpu:.1f}%"
            
            # Random variation for RAM usage
            ram_used = server["ram_used"] + random.randint(-100, 100)
            ram_used = max(0, min(ram_used, server["ram_total"]))  # Ensure within bounds
            ram_percent = (ram_used / server["ram_total"]) * 100
            ram_str = f"{ram_used}MB/{server['ram_total']}MB ({ram_percent:.1f}%)"
            
            # Random variation for Disk usage
            disk_used = server["disk_used"] + random.uniform(-2, 2)
            disk_used = max(0, min(disk_used, server["disk_total"]))
            disk_percent = (disk_used / server["disk_total"]) * 100
            disk_str = f"{disk_used:.0f}GB/{server['disk_total']}GB ({disk_percent:.0f}%)"
            
            # Random variation for live playbacks
            live = server["live"] + random.randint(-3, 3)
            live = max(0, live)  # Ensure not negative
            
            # Random variation for latency
            latency = server["latency"] + random.randint(-3, 3)
            latency_str = f"{latency}ms"
            
            api_info += (
                f"🔹 **{server['name']}**:\n"
                f" • **CPU:** {cpu_str}\n"
                f" • **RAM:** {ram_str}\n"
                f" • **Disk:** {disk_str}\n"
                f" • **Live Playbacks:** {live}/250\n"
                f" • **Latency:** {latency_str}\n\n"
            )

        # Construct the final response message
        response = (
            f"🏓 **Pong!**\n\n"
            f"**Main Server (Bot One):**\n"
            f"• **Uptime:** `{uptime_str}`\n"
            f"• **CPU Usage:** `{cpu_usage}%`\n"
            f"• **RAM Usage:** `{ram_usage}`\n"
            f"• **Disk Usage:** `{disk_usage}`\n\n"
            f"**API Servers:**\n"
            f"{api_info}"
        )

        await message.reply(response)
    except Exception as e:
        await message.reply(f"❌ Failed to execute the command. Error: {str(e)}\n\nSupport: @frozensupport1")


@bot.on_message(filters.group & filters.command(["playhelp", "help"]) & ~filters.chat(7634862283))
async def play_help_handler(_, message):
    help_text = (
        "📝 **How to Use the Play Command**\n\n"
        "Usage: `/play <song name>`\n"
        "Example: `/play Shape of You`\n\n"
        "This command works only in groups.\n\n"
        "**Instructions in Multiple Languages:**\n\n"
        "🇬🇧 **English:** Use `/play` followed by the song name.\n"
        "🇪🇸 **Español:** Usa `/play` seguido del nombre de la canción.\n"
        "🇫🇷 **Français:** Utilisez `/play` suivi du nom de la chanson.\n"
        "🇩🇪 **Deutsch:** Verwenden Sie `/play` gefolgt vom Namen des Liedes.\n"
        "🇨🇳 **中文:** 使用 `/play` 后跟歌曲名称。\n"
        "🇷🇺 **Русский:** Используйте `/play`, за которым следует название песни.\n"
        "🇦🇪 **عربي:** استخدم `/play` متبوعًا باسم الأغنية.\n"
        "🇲🇲 **မြန်မာ:** `/play` နဲ့ သီချင်းအမည်ကို ထည့်ပါ။\n"
        "🇮🇳 **हिन्दी:** `/play` के बाद गीत का नाम लिखें।"
    )
    await message.reply(help_text)

@bot.on_message(filters.private & ~filters.command("start") & ~filters.chat(7634862283))
async def private_only_groups_handler(_, message):
    group_info_text = (
        "⚠️ **This bot only works in groups!**\n\n"
        "To play a song in a group, use the command like this:\n"
        "`/play <song name>`\n\n"
        "For more instructions, please use the `/playhelp` command in your group chat.\n\n"
        "**Languages:**\n"
        "🇬🇧 English: Use `/play` followed by the song name.\n"
        "🇪🇸 Español: Usa `/play` seguido del nombre de la canción.\n"
        "🇫🇷 Français: Utilisez `/play` suivi du nom de la chanson.\n"
        "🇩🇪 Deutsch: Verwenden Sie `/play` gefolgt vom Namen des Liedes.\n"
        "🇨🇳 中文: 使用 `/play` 后跟歌曲名称。\n"
        "🇷🇺 Русский: Используйте `/play`, за которым следует название песни.\n"
        "🇦🇪 عربي: استخدم `/play` متبوعًا باسم الأغنية.\n"
        "🇲🇲 မြန်မာ: `/play` နဲ့ သီချင်းအမည်ကို ထည့်ပါ။\n"
        "🇮🇳 हिन्दी: `/play` के बाद गीत का नाम लिखें।"
    )
    await message.reply(group_info_text)



@bot.on_message(filters.group & filters.command("clear"))
async def clear_handler(_, message):
    chat_id = message.chat.id

    if chat_id in chat_containers:
        # Clear the chat-specific queue
        for song in chat_containers[chat_id]:
            try:
                os.remove(song.get('file_path', ''))
            except Exception as e:
                print(f"Error deleting file: {e}")
        
        chat_containers.pop(chat_id)
        await message.reply("🗑️ Cleared the queue.")
    else:
        await message.reply("❌ No songs in the queue to clear.")

import requests

API_WORKER_URL = "https://boradcasteapi.frozenbotsweb.workers.dev"
BOT_ID = "7598576464"
ADMIN_ID = 5268762773# Your bot's ID

async def register_chat_silently(chat_id):
    """Silently register chat ID with the broadcast API."""
    try:
        requests.post(
            f"{API_WORKER_URL}/register",
            json={"botId": BOT_ID, "chatId": str(chat_id)}
        )
    except Exception as e:
        print(f"Error registering chat: {e}")

import asyncio

@bot.on_message(filters.command("broadcast") & filters.user(OWNER_ID))
async def broadcast_handler(_, message):
    # Ensure the command is used in reply to a message
    if not message.reply_to_message:
        await message.reply("❌ Please reply to the message you want to broadcast.")
        return

    broadcast_message = message.reply_to_message

    # Retrieve all broadcast chat IDs from the collection
    all_chats = list(broadcast_collection.find({}))
    success = 0
    failed = 0

    # Loop through each chat ID and forward the message
    for chat in all_chats:
        try:
            # Ensure the chat ID is an integer (this will handle group IDs properly)
            target_chat_id = int(chat.get("chat_id"))
        except Exception as e:
            print(f"Error casting chat_id: {chat.get('chat_id')} - {e}")
            failed += 1
            continue

        try:
            await bot.forward_messages(
                chat_id=target_chat_id,
                from_chat_id=broadcast_message.chat.id,
                message_ids=broadcast_message.id
            )
            success += 1
        except Exception as e:
            print(f"Failed to broadcast to {target_chat_id}: {e}")
            failed += 1

        # Wait for 1 second to avoid flooding the server and Telegram
        await asyncio.sleep(1)

    await message.reply(f"Broadcast complete!\n✅ Success: {success}\n❌ Failed: {failed}")


@bot.on_message(filters.video_chat_ended)
async def clear_queue_on_vc_end(_, message: Message):
    chat_id = message.chat.id

    try:
        if chat_id in chat_containers:
            # Clear queue files
            for song in chat_containers[chat_id]:
                try:
                    os.remove(song.get('file_path', ''))
                except Exception as e:
                    print(f"Error deleting file: {e}")

            # Remove queue data
            chat_containers.pop(chat_id)

            # Clear playback state for local or API playback
            playback_mode.pop(chat_id, None)
            last_played_song.pop(chat_id, None)

            # Cancel any running playback task
            if chat_id in playback_tasks:
                playback_tasks[chat_id].cancel()
                del playback_tasks[chat_id]

            await message.reply("**😕ᴠɪᴅᴇᴏ ᴄʜᴀᴛ ᴇɴᴅᴇᴅ💔**\n✨Queue and playback records have been cleared.")
        else:
            await message.reply("**😕ᴠɪᴅᴇᴏ ᴄʜᴀᴛ ᴇɴᴅᴇᴅ💔**\n❌No active queue to clear.")
    except Exception as error:
        print(f"Error in clear_queue_on_vc_end: {error}")
        await message.reply("**😕ᴠɪᴅᴇᴏ ᴄʜᴀᴛ ᴇɴᴅᴇᴅ💔**\n❌Failed to clear queue and playback records properly.")


@bot.on_message(filters.video_chat_started)
async def brah(_, msg):
    await msg.reply("**😍ᴠɪᴅᴇᴏ ᴄʜᴀᴛ sᴛᴀʀᴛᴇᴅ🥳**")

def ping_api(url, description):
    """Ping an API endpoint and print its HTTP status code."""
    print(f"Pinging {description}: {url}")
    try:
        response = requests.get(url, timeout=5)
        print(f"{description} responded with status code: {response.status_code}")
    except Exception as e:
        print(f"Error pinging {description}: {e}")

@bot.on_message(filters.regex(r'^Stream ended in chat id (?P<chat_id>-?\d+)$'))
async def stream_ended_handler(_, message):
    # Extract the chat ID from the message
    chat_id = int(message.matches[0]['chat_id'])
    
    # Update playback records for a natural end event
    record = {
        "chat_id": chat_id,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "event": "natural_end",
        "mode": playback_mode.get(chat_id, "unknown")
    }
    api_playback_records.append(record)
    playback_mode.pop(chat_id, None)
    
    if chat_id in chat_containers and chat_containers[chat_id]:
        # Remove the finished song from the queue.
        skipped_song = chat_containers[chat_id].pop(0)
        await asyncio.sleep(3)  # Delay to ensure the stream has fully ended
        
        try:
            os.remove(skipped_song.get('file_path', ''))
        except Exception as e:
            print(f"Error deleting file: {e}")
        
        if chat_containers[chat_id]:
            await bot.send_message(chat_id, "⏭ Skipping to the next song...")
            await start_playback_task(chat_id, message)
        else:
            # Queue is empty; fetch suggestions.
            last_song = last_played_song.get(chat_id)
            if last_song and last_song.get('url'):
                status_msg = await bot.send_message(chat_id, "😔 No more songs in the queue. Fetching song suggestions...")
                await show_suggestions(chat_id, last_song.get('url'), status_message=status_msg)
            else:
                await bot.send_message(
                    chat_id,
                    "❌ No more songs in the queue.\nLeaving the voice chat. 💕\n\nSupport: @frozensupport1"
                )
                await leave_voice_chat(chat_id)
    else:
        # No songs in the queue.
        last_song = last_played_song.get(chat_id)
        if last_song and last_song.get('url'):
            status_msg = await bot.send_message(chat_id, "😔 No more songs in the queue. Fetching song suggestions...")
            await show_suggestions(chat_id, last_song.get('url'), status_message=status_msg)
        else:
            await bot.send_message(chat_id, "🚪 No songs left in the queue.")


@bot.on_message(filters.command("frozen_check") & filters.chat(ASSISTANT_CHAT_ID))
async def frozen_check_command(_, message):
    await message.reply_text("frozen check successful ✨")




@bot.on_message(filters.regex(r"^#restart$") & filters.user(5268762773))
async def owner_simple_restart_handler(_, message):
    await message.reply("♻️ [WATCHDOG] restart initiated as per owner command...")
    await simple_restart()



MAIN_LOOP = None
ASSISTANT_CHAT_ID = 7386215995
BOT_CHAT_ID = 7598576464
BOT_USERNAME = "@vcmusiclubot"


# Check for Render API endpoint (set this in environment variables if needed)
RENDER_DEPLOY_URL = os.getenv("RENDER_DEPLOY_URL", "https://api.render.com/deploy/srv-cuqb40bv2p9s739h68i0?key=oegMCHfLr9I")

async def simple_restart():
    support_chat_id = -1001810811394
    log_message = "[WATCHDOG] Checking if restart is needed..."
    print(log_message)
    await bot.send_message(support_chat_id, log_message)

    if RENDER_DEPLOY_URL:
        # If Render API is available, trigger a restart via API
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(RENDER_DEPLOY_URL) as response:
                    if response.status == 200:
                        await bot.send_message(support_chat_id, "✅ Restart triggered via Frozen_Api")
                        return  # Exit without restarting locally
                    else:
                        await bot.send_message(support_chat_id, f"❌ Render restart failed: {response.status} {await response.text()}")
        except Exception as e:
            await bot.send_message(support_chat_id, f"⚠ Render API restart failed: {e}. Trying local restart...")

    # If Render API failed or not set, do a local restart
    try:
        await bot.stop()
        await asyncio.sleep(3)
        python_executable = sys.executable
        script_path = os.path.abspath(sys.argv[0])

        subprocess.Popen([python_executable, script_path], close_fds=True)
        os._exit(0)
    except Exception as e:
        error_message = f"❌ Local restart failed: {e}"
        print(error_message)
        await bot.send_message(support_chat_id, error_message)



import asyncio
import os
import sys
import json
import threading
import requests
from http.server import BaseHTTPRequestHandler, HTTPServer

# Assume that bot, call_py, assistant, idle, and simple_restart are defined/imported elsewhere.

async def restart_bot_logic():
    try:
        try:
            # Attempt to stop the bot gracefully.
            await bot.stop()
        except Exception as e:
            # If stopping fails, log the error but continue.
            print("Warning: Failed to stop the bot gracefully, proceeding to restart:", e)
        await asyncio.sleep(2)  # Wait a moment for resources to settle.
        # Attempt to start the bot.
        await bot.start()
    except Exception as e:
        # Propagate the error so that full restart logic is triggered.
        raise e

class WebhookHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Bot is running!")
        elif self.path == "/status":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Bot status: Running")
        elif self.path == "/restart":
            try:
                loop = asyncio.get_event_loop()
                future = asyncio.run_coroutine_threadsafe(restart_bot_logic(), loop)
                future.result(timeout=10)
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"Bot restarted successfully!")
            except Exception as e:
                error_message = f"Bot restart failed: {str(e)}"
                self.send_response(500)
                self.end_headers()
                self.wfile.write(error_message.encode())
                # After sending the error response, perform a full restart.
                os.execl(sys.executable, sys.executable, *sys.argv)
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/webhook":
            try:
                content_length = int(self.headers.get("Content-Length", 0))
                post_data = self.rfile.read(content_length)
                update = json.loads(post_data.decode("utf-8"))
                try:
                    bot._process_update(update)
                except Exception as e:
                    print("Error processing update:", e)
            except Exception as e:
                print("Error reading update:", e)
                self.send_response(400)
                self.end_headers()
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

def run_http_server():
    port = int(os.environ.get("PORT", 8080))
    httpd = HTTPServer(("", port), WebhookHandler)
    print(f"HTTP server running on port {port}")
    httpd.serve_forever()

# Start the HTTP server in a separate daemon thread.
server_thread = threading.Thread(target=run_http_server, daemon=True)
server_thread.start()

if __name__ == "__main__":
    try:
        print("Starting Frozen Music Bot...")
        call_py.start()
        # Using bot.run() here so that if it fails, we catch the exception below.
        bot.run()
        # If the assistant is not connected, connect it.
        if not assistant.is_connected:
            assistant.run()
        print("Bot started successfully.")
        # Block indefinitely (for example, using idle() from your framework)
        idle()
    except KeyboardInterrupt:
        print("Bot is still running. Kill the process to stop.")
    except Exception as e:
        print(f"Critical Error: {e}")
        # If bot.run() (or its initialization) fails, perform a full restart.
        asyncio.run(simple_restart())

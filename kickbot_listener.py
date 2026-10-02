import os
import re
import time
import json
import uuid
import asyncio
import threading
import tempfile
import urllib.request
import urllib.parse

class KickBotListener:
    """
    Connects to the streamer's external KickBot TTS browser source URL via a headless
    Playwright instance, intercepts all generated TTS audio payloads/streams, and feeds
    them into Chatterbox's unified TTS queue instead of playing directly through OBS.
    """
    def __init__(self, callback_audio, callback_log=None, get_banned_users=None, get_timed_out_users=None):
        self.callback_audio = callback_audio
        self.callback_log = callback_log
        self.get_banned_users = get_banned_users
        self.get_timed_out_users = get_timed_out_users
        self.is_running = False
        self.thread = None
        self.seen_audio_signatures = {}
        self.temp_dir = os.path.join(tempfile.gettempdir(), "chatterbox_kickbot_audio")
        try:
            os.makedirs(self.temp_dir, exist_ok=True)
        except Exception:
            pass

    def log(self, msg):
        formatted = f"[KickBot TTS] {msg}"
        if not self.callback_log:
            print(formatted)
            return
        try:
            self.callback_log(formatted)
        except TypeError:
            try:
                self.callback_log("System", formatted, None)
            except Exception:
                print(formatted)
        except Exception:
            print(formatted)

    def start(self, url):
        if self.is_running:
            return
        if not url or "kickbot.com/external/" not in url:
            self.log(f"Invalid KickBot TTS URL: '{url}'. Please provide a valid external URL (e.g. https://kickbot.com/external/.../tts)")
            return
        
        self.is_running = True
        self.seen_audio_signatures = {}
        self.thread = threading.Thread(target=self._run_async_loop, args=(url.strip(),), daemon=True)
        self.thread.start()

    def stop(self):
        self.is_running = False
        self.log("Listener stopped.")

    def _run_async_loop(self, url):
        asyncio.run(self._async_listener_loop(url))

    async def _async_listener_loop(self, url):
        # Format URL properly
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "https://" + url

        self.log(f"Connecting to KickBot external browser source: {url}")
        
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            self.log("Playwright is not installed. Please run setup.bat.")
            self.is_running = False
            return

        while self.is_running:
            try:
                async with async_playwright() as p:
                    # Launch lightweight headless Chromium with muted audio to avoid double sound in OBS
                    browser = await p.chromium.launch(
                        headless=True,
                        args=[
                            "--disable-blink-features=AutomationControlled",
                            "--autoplay-policy=no-user-gesture-required",
                            "--disable-background-timer-throttling",
                            "--disable-backgrounding-occluded-windows",
                            "--disable-renderer-backgrounding",
                            "--mute-audio"
                        ]
                    )
                    context = await browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                        ignore_https_errors=True
                    )

                    # Injected script: capture WebSocket messages and mute Howler without breaking audio lifecycle
                    await context.add_init_script("""
                        window._kickbotQueue = [];

                        // Intercept WebSocket messages
                        try {
                            const OrigWebSocket = window.WebSocket;
                            window.WebSocket = function(url, protocols) {
                                const ws = new OrigWebSocket(url, protocols);
                                ws.addEventListener('message', function(evt) {
                                    try {
                                        const parsed = JSON.parse(evt.data);
                                        if (parsed) {
                                            window._kickbotQueue.push(parsed);
                                        }
                                    } catch(e) {}
                                });
                                return ws;
                            };
                        } catch(e) {}

                        // Ensure Howler instance stays muted to avoid double playback
                        setInterval(() => {
                            try {
                                if (window.Howler && typeof window.Howler.mute === 'function') {
                                    window.Howler.mute(true);
                                }
                            } catch(e) {}
                        }, 500);
                    """)

                    page = await context.new_page()

                    # 1. Native Playwright WebSocket Interception
                    def on_web_socket(ws):
                        self.log(f"WebSocket connection opened: {ws.url}")

                        def on_frame_received(payload):
                            try:
                                if isinstance(payload, bytes):
                                    text_data = payload.decode("utf-8", errors="ignore")
                                else:
                                    text_data = str(payload)

                                data = json.loads(text_data)
                                self._handle_raw_ws_json(data)
                            except Exception:
                                pass

                        ws.on("framereceived", on_frame_received)

                    page.on("websocket", on_web_socket)

                    # 2. Native Playwright Network Response Interception for audio downloads
                    async def handle_response(response):
                        try:
                            resp_url = response.url
                            content_type = response.headers.get("content-type", "").lower()
                            if "audio/" in content_type or any(resp_url.lower().endswith(ext) for ext in [".mp3", ".wav", ".ogg", ".aac"]):
                                if response.status == 200:
                                    body_bytes = await response.body()
                                    if body_bytes and len(body_bytes) > 20:
                                        self._process_raw_audio_bytes(body_bytes, resp_url)
                        except Exception:
                            pass

                    page.on("response", handle_response)

                    try:
                        await page.goto(url, timeout=30000, wait_until="load")
                        self.log("Successfully connected and listening for KickBot TTS events!")
                    except Exception as e:
                        self.log(f"Connection warning: {e}. Retrying...")

                    # 3. Main polling loop for page events and keepalive
                    while self.is_running:
                        try:
                            # Check injected script queue
                            queue_items = await page.evaluate("""() => {
                                let q = window._kickbotQueue || [];
                                window._kickbotQueue = [];
                                return q;
                            }""")

                            if queue_items:
                                for item in queue_items:
                                    self._handle_raw_ws_json(item)

                            # Prune old deduplication signatures
                            now = time.time()
                            if len(self.seen_audio_signatures) > 1000:
                                for k in list(self.seen_audio_signatures.keys())[:200]:
                                    del self.seen_audio_signatures[k]

                        except Exception:
                            if not self.is_running:
                                break

                        await asyncio.sleep(0.5)

                    await browser.close()
                    break

            except Exception as loop_err:
                if not self.is_running:
                    break
                self.log(f"Error in listener loop: {loop_err}. Reconnecting in 5s...")
                await asyncio.sleep(5)

    def _handle_raw_ws_json(self, data):
        """Processes incoming raw WebSocket JSON messages from KickBot"""
        if not isinstance(data, dict):
            return

        event_data = data.get("data", {})
        if not isinstance(event_data, dict):
            event_data = data

        event_type = event_data.get("event_type", "")
        payload = event_data.get("payload", {})

        if event_type == "TTS_MESSAGE" or ("audio_url" in payload) or ("audio_url" in event_data):
            target_payload = payload if isinstance(payload, dict) and payload else event_data
            self._process_tts_payload(target_payload)

    def _process_tts_payload(self, payload):
        """Processes a KickBot TTS payload and enqueues audio"""
        try:
            if not isinstance(payload, dict):
                return

            audio_url = payload.get("audio_url", "").strip()
            item_id = payload.get("id")
            viewer = payload.get("viewer_username") or payload.get("username") or payload.get("sender") or payload.get("user") or payload.get("donor") or payload.get("name") or payload.get("display_name") or "KickBot"
            message = payload.get("message") or payload.get("text") or "AI Text-to-Speech"
            voice = payload.get("voice_label") or payload.get("voice") or "KickBot TTS"

            # Check if user is in banned or timed-out list
            banned_list = []
            if self.get_banned_users and callable(self.get_banned_users):
                try:
                    raw_banned = self.get_banned_users() or []
                    banned_list = [str(u).strip().lstrip("@").lower() for u in raw_banned if str(u).strip()]
                except Exception:
                    pass

            timed_out_list = []
            if self.get_timed_out_users and callable(self.get_timed_out_users):
                try:
                    raw_timed_out = self.get_timed_out_users() or []
                    if isinstance(raw_timed_out, dict):
                        timed_out_list = [str(k).strip().lstrip("@").lower() for k in raw_timed_out.keys() if str(k).strip()]
                    elif isinstance(raw_timed_out, (list, set, tuple)):
                        timed_out_list = [str(u).strip().lstrip("@").lower() for u in raw_timed_out if str(u).strip()]
                except Exception:
                    pass

            clean_viewer = str(viewer or "").strip().lstrip("@").lower()
            if clean_viewer and clean_viewer != "kickbot":
                if clean_viewer in banned_list:
                    self.log(f"Ignored KickBot TTS from banned user: '{viewer}'")
                    return
                if clean_viewer in timed_out_list:
                    self.log(f"Ignored KickBot TTS from timed out user: '{viewer}'")
                    return

            if message:
                m_author = re.match(r'^\s*@?([a-zA-Z0-9_\-]+)\s*:\s*(.*)$', str(message))
                if m_author:
                    cand_user = m_author.group(1).strip().lstrip("@").lower()
                    if cand_user in banned_list:
                        self.log(f"Ignored KickBot TTS from banned user: '{m_author.group(1)}'")
                        return
                    if cand_user in timed_out_list:
                        self.log(f"Ignored KickBot TTS from timed out user: '{m_author.group(1)}'")
                        return

            sig = f"ID::{item_id}" if item_id else f"URL::{audio_url}"
            now = time.time()
            if sig in self.seen_audio_signatures:
                if now - self.seen_audio_signatures[sig] < 300: # 5 min deduplication
                    return
            self.seen_audio_signatures[sig] = now

            if not audio_url:
                return

            self.log(f"Received TTS from {viewer} ({voice}): \"{message}\"")

            # Download audio bytes
            audio_bytes = None
            ext = ".mp3"

            # 1. Base64 data URL
            if audio_url.startswith("data:audio/"):
                import base64
                header, b64_data = audio_url.split(",", 1)
                audio_bytes = base64.b64decode(b64_data)
                if "wav" in header:
                    ext = ".wav"
                elif "ogg" in header:
                    ext = ".ogg"
            # 2. HTTP URL
            elif audio_url.startswith("http://") or audio_url.startswith("https://"):
                urls_to_try = [audio_url]
                try:
                    parsed_path = urllib.parse.urlparse(audio_url).path
                    if parsed_path:
                        urls_to_try.append(f"https://ttsaudio.kickbot.com{parsed_path}")
                        urls_to_try.append(f"https://tts.kickbotcdn.com{parsed_path}")
                except Exception:
                    pass

                for u in urls_to_try:
                    try:
                        req = urllib.request.Request(
                            u,
                            headers={
                                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                            }
                        )
                        with urllib.request.urlopen(req, timeout=10) as resp:
                            if resp.status == 200:
                                b = resp.read()
                                if b and len(b) > 200:
                                    audio_bytes = b
                                    if u.lower().endswith(".wav"):
                                        ext = ".wav"
                                    elif u.lower().endswith(".ogg"):
                                        ext = ".ogg"
                                    break
                    except Exception:
                        continue

            if audio_bytes and len(audio_bytes) > 20:
                temp_filename = f"kickbot_{uuid.uuid4().hex[:10]}{ext}"
                temp_filepath = os.path.join(self.temp_dir, temp_filename)
                with open(temp_filepath, "wb") as f:
                    f.write(audio_bytes)

                self.log(f"Captured audio ({len(audio_bytes)} bytes). Enqueuing into Chatterbox TTS queue...")
                if self.callback_audio:
                    self.callback_audio(
                        audio_path=temp_filepath,
                        user=viewer,
                        text=message,
                        command=f"KickBot: {voice}"
                    )
            else:
                self.log(f"Warning: Could not download audio from {audio_url}")

        except Exception as e:
            self.log(f"Error processing TTS payload: {e}")

    def _process_raw_audio_bytes(self, body_bytes, url):
        """Fallback when audio is intercepted directly over the network"""
        try:
            now = time.time()
            import hashlib
            h = hashlib.md5(body_bytes[:512]).hexdigest()
            sig = f"BYTES::{h}"
            if sig in self.seen_audio_signatures:
                if now - self.seen_audio_signatures[sig] < 300:
                    return
            self.seen_audio_signatures[sig] = now

            ext = ".mp3"
            if url.lower().endswith(".wav"):
                ext = ".wav"
            elif url.lower().endswith(".ogg"):
                ext = ".ogg"

            temp_filename = f"kickbot_{uuid.uuid4().hex[:10]}{ext}"
            temp_filepath = os.path.join(self.temp_dir, temp_filename)
            with open(temp_filepath, "wb") as f:
                f.write(body_bytes)

            self.log(f"Captured network audio stream ({len(body_bytes)} bytes). Enqueuing...")
            if self.callback_audio:
                self.callback_audio(
                    audio_path=temp_filepath,
                    user="KickBot",
                    text="AI Text-to-Speech",
                    command="KickBot TTS"
                )
        except Exception as e:
            self.log(f"Error processing raw audio bytes: {e}")

import os
import shutil
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning, module="diffusers")
warnings.filterwarnings("ignore", module="diffusers")
warnings.filterwarnings("ignore", message=".*LoRACompatibleLinear.*")

import time
import re
import json
import datetime
import threading
import uuid
import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog
from kick_websocket_client import KickRealtimeChatClient, PureWebSocket
from tts_engine import TTSEngine
from kickbot_listener import KickBotListener

try:
    from powerchat_listener import PowerchatListener, extract_powerchat_username
except ImportError:
    def extract_powerchat_username(link_or_text: str) -> str:
        """Extracts the Powerchat username from a URL or raw string."""
        if not link_or_text:
            return ""
        text = link_or_text.strip()
        match = re.search(r'powerchat\.live/([^/?#\s]+)', text, re.IGNORECASE)
        if match:
            user = match.group(1).strip()
            if user.lower() == "tts":
                return ""
            return user
        if not text.startswith("http://") and not text.startswith("https://") and "/" not in text:
            return text.strip().lstrip('@')
        return ""

    class PowerchatListener:
        """Embedded fallback for monitoring powerchat.live WebSocket notifications."""
        def __init__(self, callback_donation=None, callback_log=None):
            self.callback_donation = callback_donation
            self.callback_log = callback_log
            self.is_running = False
            self.current_username = ""
            self.thread = None
            self.ws = None
            self.seen_donation_ids = set()
            self._recent_dono_hashes = {}
            self._lock = threading.Lock()

        def log(self, message: str):
            formatted = f"[Powerchat] {message}"
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

        def start(self, link_or_username: str):
            username = extract_powerchat_username(link_or_username)
            if not username:
                self.log(f"Invalid Powerchat link or username: '{link_or_username}'. Expected format: https://powerchat.live/[user]/tts")
                return

            with self._lock:
                if self.is_running and self.current_username.lower() == username.lower():
                    return
                if self.is_running:
                    self._stop_unlocked()

                self.is_running = True
                self.current_username = username
                self.thread = threading.Thread(target=self._run_listener_loop, args=(username,), daemon=True)
                self.thread.start()

        def stop(self):
            with self._lock:
                self._stop_unlocked()

        def _stop_unlocked(self):
            self.is_running = False
            if self.ws:
                try:
                    self.ws.close()
                except Exception:
                    pass
                self.ws = None
            self.log("Listener stopped.")

        def _run_listener_loop(self, username: str):
            self.log(f"Starting connection to Powerchat for user '{username}' (wss://powerchat.live/{username.lower()})...")

            backoff = 3
            while self.is_running:
                try:
                    ws = PureWebSocket(
                        host="powerchat.live",
                        port=443,
                        path=f"/{username.lower()}",
                        ssl_wrap=True
                    )
                    with self._lock:
                        if not self.is_running:
                            break
                        self.ws = ws

                    ws.connect(timeout=12)
                    self.log(f"Connected to Powerchat alerts for '{username}'! Monitoring live donations.")
                    backoff = 3

                    ws.send_text(f"Remote client log - {username} - Open TTS - {username}")

                    ping_thread = threading.Thread(
                        target=self._ping_worker,
                        args=(ws, username),
                        daemon=True
                    )
                    ping_thread.start()

                    while self.is_running and not ws.closed:
                        opcode, payload = ws.recv_frame()
                        if opcode is None:
                            if ws.closed or not self.is_running:
                                break
                            continue

                        if opcode == 0x09:
                            ws.send_pong(payload)
                            continue

                        if opcode == 0x01:
                            try:
                                text = payload.decode("utf-8", errors="ignore").strip()
                                if text and text != "pong":
                                    self._handle_raw_message(text)
                            except Exception as e:
                                self.log(f"Error handling message payload: {e}")

                        if opcode == 0x08:
                            self.log("Server closed WebSocket connection.")
                            break

                except Exception as e:
                    if self.is_running:
                        self.log(f"Connection error: {e}. Reconnecting in {backoff}s...")
                        time.sleep(backoff)
                        backoff = min(backoff * 2, 30)
                finally:
                    if ws:
                        try:
                            ws.close()
                        except Exception:
                            pass

        def _ping_worker(self, ws: PureWebSocket, username: str):
            while self.is_running and not ws.closed:
                time.sleep(25)
                try:
                    if self.is_running and not ws.closed:
                        ws.send_text(f"ping from: {username} - TTS Overlay")
                except Exception:
                    break

        def _handle_raw_message(self, raw_text: str):
            if not raw_text:
                return

            clean_text = raw_text.strip()
            if clean_text == "pong":
                return

            # Attempt JSON parsing, stripping socket.io / engine.io prefix if present (e.g. 42[...])
            data = None
            try:
                data = json.loads(clean_text)
            except Exception:
                no_prefix = re.sub(r'^\d+', '', clean_text).strip()
                if no_prefix and (no_prefix.startswith('{') or no_prefix.startswith('[')):
                    try:
                        data = json.loads(no_prefix)
                    except Exception:
                        return
                else:
                    return

            # Handle list/array payloads (e.g. Socket.IO event tuples like ["alert", {...}] or [{...}])
            if isinstance(data, list):
                items_to_process = []
                if len(data) >= 2 and isinstance(data[1], dict):
                    items_to_process.append(data[1])
                else:
                    for elem in data:
                        if isinstance(elem, dict):
                            items_to_process.append(elem)
                for item in items_to_process:
                    self._process_single_donation(item)
                return

            if isinstance(data, dict):
                self._process_single_donation(data)

        def _process_single_donation(self, data: dict):
            if not isinstance(data, dict):
                return

            # Unwrap nested data wrappers commonly used in alert systems
            for wrap_key in ("data", "payload", "donation", "dono", "alert", "item", "body"):
                if wrap_key in data and isinstance(data[wrap_key], dict):
                    inner = dict(data[wrap_key])
                    for inherit_key in ("id", "skip", "isReplay", "timestamp", "created_at"):
                        if inherit_key in data and inherit_key not in inner:
                            inner[inherit_key] = data[inherit_key]
                    data = inner
                    break

            # Check skip / isReplay safely (avoid string "false" / 0 evaluating to truthy)
            skip_val = data.get("skip")
            if skip_val in (True, 1, "true", "True", "1"):
                return
            replay_val = data.get("isReplay")
            if replay_val in (True, 1, "true", "True", "1"):
                return

            # Filter out explicit non-donation events (e.g. stream overlay bridge alerts for subs, gifted subs, follows)
            if data.get("is_donation") is False or data.get("isDonation") is False:
                return

            for type_key in ("type", "event", "eventType", "event_type", "kind", "category", "action"):
                evt_type = str(data.get(type_key, "")).strip().lower()
                if evt_type in (
                    "sub", "subscription", "resub", "gift", "gifted", "gifted_sub",
                    "gifted_subscription", "follow", "follower", "raid", "host",
                    "cheer", "alert", "notification", "system", "message"
                ):
                    return

            # 1. Parse Donator Username (specifically handling Anonymous donators)
            is_explicitly_anonymous = False
            for anon_key in ("anonymous", "isAnonymous", "is_anonymous", "anon"):
                val = data.get(anon_key)
                if val in (True, 1, "true", "True", "1"):
                    is_explicitly_anonymous = True
                    break

            raw_user = None
            if not is_explicitly_anonymous:
                for u_key in (
                    "donator", "user", "username", "name", "donor", "sender",
                    "from", "author", "nickname", "nick", "display_name",
                    "displayName", "donator_name", "donatorName", "donor_name",
                    "donorName", "handle", "who", "chatter"
                ):
                    candidate = data.get(u_key)
                    if candidate is not None:
                        if isinstance(candidate, dict):
                            raw_user = candidate.get("username") or candidate.get("name") or candidate.get("display_name")
                        elif isinstance(candidate, str) and candidate.strip():
                            raw_user = candidate.strip()
                        if raw_user:
                            break

            # Normalize empty, None, or placeholder names to "Anonymous"
            donator = "Anonymous"
            if raw_user and not is_explicitly_anonymous:
                clean_name = str(raw_user).strip().lstrip('@')
                if clean_name.lower() in ("anonymous", "anon", "someone", "unknown", "null", "none", "undefined", "n/a", "no name", "noname", ""):
                    donator = "Anonymous"
                else:
                    donator = clean_name
            else:
                donator = "Anonymous"

            # 2. Parse Amount (dollars or cents)
            raw_amt = None
            for amt_key in ("amount", "dollars", "usd", "total", "price", "value", "donation_amount", "dono_amount", "sum"):
                val = data.get(amt_key)
                if val is not None and val != "":
                    raw_amt = val
                    break

            amount = 0.0
            cents_val = None
            for c_key in ("cents", "amount_cents", "usd_cents", "total_cents"):
                val = data.get(c_key)
                if val is not None and val != "":
                    cents_val = val
                    break

            try:
                if raw_amt is not None:
                    if isinstance(raw_amt, (int, float)):
                        amount = float(raw_amt)
                    else:
                        clean_str = str(raw_amt).replace("$", "").replace(",", "").strip()
                        amount = float(clean_str)
            except Exception:
                amount = 0.0

            # If amount was 0 or missing, but cents were provided
            if amount <= 0.0 and cents_val is not None:
                try:
                    c_num = float(str(cents_val).replace("$", "").replace(",", "").strip())
                    if c_num >= 100:
                        amount = c_num / 100.0
                    else:
                        amount = c_num
                except Exception:
                    pass

            # Powerchat donations MUST have a positive dollar amount.
            # Zero-dollar events are platform alerts/events (e.g. Kick/Twitch gifted subs, followers, raids)
            # broadcast by Powerchat's overlay alert system, NOT monetary donations.
            if amount <= 0.0:
                return

            # 3. Message text
            message = ""
            for msg_key in ("message", "subMessage", "sub_message", "comment", "text", "msg", "memo", "body"):
                val = data.get(msg_key)
                if val is not None:
                    message = str(val).strip()
                    if message:
                        break

            # Check if the message is actually a stream alert broadcast (e.g. "User just gifted 5 subscriptions on Kick")
            lower_msg = message.lower()
            if (
                re.search(r'\b(?:just\s+)?gifted\s+\d+\s+sub(?:scription)?s?\b', lower_msg)
                or re.search(r'\b(?:just\s+)?subscribed\b', lower_msg)
                or re.search(r'\b(?:just\s+)?started\s+following\b', lower_msg)
                or re.search(r'\b(?:just\s+)?raided\b', lower_msg)
            ):
                return

            # If the donator matches the streamer's channel username and contains alert keywords, suppress it
            if self.current_username and donator.lower() == self.current_username.lower():
                if re.search(r'\b(?:gifted|subscription|subscriber|followed|raided)\b', lower_msg):
                    return

            # 4. Deduplicate donations safely
            # Ensure dummy or zero IDs (which often occur for anonymous donors) don't collide
            raw_id = None
            for id_key in ("id", "donation_id", "dono_id", "uuid", "tx_id", "transaction_id"):
                val = data.get(id_key)
                if val is not None and str(val).strip():
                    raw_id = str(val).strip()
                    break

            now = time.time()
            if not raw_id or raw_id.lower() in ("0", "null", "none", "undefined", "anonymous", "false", "-1", "true", "nan"):
                donation_id = f"pc_anon_{amount}_{int(now * 1000)}_{uuid.uuid4().hex[:8]}"
            else:
                donation_id = raw_id

            if donation_id in self.seen_donation_ids:
                return

            # Rapid duplicate echo filter (within 2.5s) for identical donator, amount, and message
            dono_sig = f"{donator.lower()}::{amount:.2f}::{message.lower()}"
            if dono_sig in self._recent_dono_hashes and (now - self._recent_dono_hashes[dono_sig] < 2.5):
                return

            self.seen_donation_ids.add(donation_id)
            self._recent_dono_hashes[dono_sig] = now

            if len(self.seen_donation_ids) > 5000:
                self.seen_donation_ids.clear()
                self.seen_donation_ids.add(donation_id)
            if len(self._recent_dono_hashes) > 1000:
                for k in list(self._recent_dono_hashes.keys())[:300]:
                    del self._recent_dono_hashes[k]

            self.log(f"Detected Powerchat donation: {donator} sent ${amount:.2f} (\"{message}\") [ID: {donation_id}]")

            # 5. Dispatch callback
            if self.callback_donation:
                try:
                    self.callback_donation(donator, amount, message, donation_id)
                except Exception as e:
                    self.log(f"Error executing donation callback: {e}")


# Set the CustomTkinter theme to mimic the "Immersive UI" glowing dark mode.
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

class WindowsVolumeController:
    """Manages application-specific master volume in Windows Volume Mixer via WASAPI/pycaw."""
    def __init__(self):
        self.pid = os.getpid()
        self._vol_interface = None
        self._warmed_up = False

    def _ensure_com(self):
        try:
            import comtypes
            comtypes.CoInitialize()
        except Exception:
            pass

    def warmup(self, target_vol=None):
        """Warms up the audio endpoint so Windows registers the audio session and applies initial volume."""
        import sys
        if sys.platform != "win32":
            return
        if not self._warmed_up:
            try:
                import sounddevice as sd
                import numpy as np
                # Play a microscopic buffer of silence to guarantee Windows registers the audio session
                silence = np.zeros(64, dtype=np.float32)
                sd.play(silence, 44100)
                self._warmed_up = True
            except Exception:
                try:
                    import sounddevice as sd
                    _ = sd.default.device
                    self._warmed_up = True
                except Exception:
                    pass
        if target_vol is not None:
            try:
                self.set_volume(target_vol)
            except Exception:
                pass

    def get_simple_audio_volume(self):
        import sys
        if sys.platform != "win32":
            return None
        self._ensure_com()
        try:
            from pycaw.pycaw import AudioUtilities, ISimpleAudioVolume
            sessions = AudioUtilities.GetAllSessions()
            
            # Priority 1: Exact PID match
            for session in sessions:
                if session.Process and session.Process.pid == self.pid:
                    if hasattr(session, 'SimpleAudioVolume') and session.SimpleAudioVolume:
                        return session.SimpleAudioVolume
                    vol = session._ctl.QueryInterface(ISimpleAudioVolume)
                    return vol

            # Priority 2: Match by process name / script name
            curr_exe = os.path.basename(sys.executable).lower()
            for session in sessions:
                if session.Process:
                    try:
                        pname = session.Process.name().lower() if callable(getattr(session.Process, 'name', None)) else ""
                        if pname and (pname == curr_exe or "python" in pname or "chatterbox" in pname or "kick" in pname):
                            if hasattr(session, 'SimpleAudioVolume') and session.SimpleAudioVolume:
                                return session.SimpleAudioVolume
                            vol = session._ctl.QueryInterface(ISimpleAudioVolume)
                            return vol
                    except Exception:
                        pass
        except Exception:
            pass
        return None

    def get_volume(self):
        """Returns integer 0..100 or None if volume mixer session is not available."""
        try:
            vol = self.get_simple_audio_volume()
            if vol:
                val = vol.GetMasterVolume()
                return int(round(val * 100))
        except Exception:
            pass
        return None

    def set_volume(self, percent):
        """Sets master volume for this app in Windows Volume Mixer (0..100)."""
        try:
            percent = max(0, min(100, int(round(percent))))
            vol = self.get_simple_audio_volume()
            if vol:
                vol.SetMasterVolume(float(percent) / 100.0, None)
                return True
        except Exception:
            pass
        return False

def strip_emojis(text):
    if not text:
        return ""
    # Remove Kick emote markup e.g. [emote:123:name]
    t = re.sub(r'\[emote:\d+:[^\]]+\]', '', text)
    # Remove colon-style emote codes e.g. :pepe:, :kekw:
    t = re.sub(r':[a-zA-Z0-9_\-]+:', '', t)
    # Remove unicode emojis, pictographs, symbols, variation selectors
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U00002702-\U000027B0"  # dingbats
        "\U000024C2-\U0001F251"
        "\U0001F900-\U0001F9FF"  # supplemental symbols and pictographs
        "\U0001FA00-\U0001FA6F"  # chess symbols, extended-a
        "\U0001FA70-\U0001FAFF"  # extended-b
        "\U00002600-\U000026FF"  # miscellaneous symbols
        "\U00002B00-\U00002BFF"
        "\U00002300-\U000023FF"
        "\U0000FE00-\U0000FE0F"  # variation selectors
        "\U0001F000-\U0001F02F"
        "\U0001F0A0-\U0001F0FF"
        "]+",
        flags=re.UNICODE
    )
    t = emoji_pattern.sub('', t)
    # Clean redundant spaces
    t = re.sub(r'\s+', ' ', t).strip()
    return t

def apply_dark_title_bar(window, bg_color="#0A0A0E", text_color="#E1E1E6", border_color="#1E1E24"):
    """
    Applies Windows DWM dark mode and title bar caption colors matching the app's dark theme (#0A0A0E).
    Works on Windows 10 (Immersive Dark Mode via DWM attribute 19/20) and Windows 11 (Attributes 34, 35, 36).
    Uses standard library ctypes only (no pywinstyles required).
    Leaves all window backgrounds completely untouched.
    """
    import sys
    if sys.platform != "win32":
        return

    import ctypes

    def _hex_to_colorref(hex_str):
        h = str(hex_str).lstrip("#")
        if len(h) == 6:
            r = int(h[0:2], 16)
            g = int(h[2:4], 16)
            b = int(h[4:6], 16)
            # COLORREF format in Windows DWM is 0x00BBGGRR
            return (b << 16) | (g << 8) | r
        return 0

    def _apply_dwm():
        try:
            window.update_idletasks()
            win_id = window.winfo_id()
            if not win_id:
                return

            hwnds = []
            try:
                p_hwnd = ctypes.windll.user32.GetParent(win_id)
                if p_hwnd and p_hwnd not in hwnds:
                    hwnds.append(p_hwnd)
            except Exception:
                pass
            try:
                # GA_ROOT = 2
                a_hwnd = ctypes.windll.user32.GetAncestor(win_id, 2)
                if a_hwnd and a_hwnd not in hwnds:
                    hwnds.append(a_hwnd)
            except Exception:
                pass
            if win_id not in hwnds:
                hwnds.append(win_id)

            val_on = ctypes.c_int(1)
            caption_val = ctypes.c_int(_hex_to_colorref(bg_color))
            text_val = ctypes.c_int(_hex_to_colorref(text_color))
            border_val = ctypes.c_int(_hex_to_colorref(border_color))

            # DWM attribute IDs:
            DWMWA_USE_IMMERSIVE_DARK_MODE_OLD = 19  # Win10 1809-1909
            DWMWA_USE_IMMERSIVE_DARK_MODE = 20      # Win10 2004+ and Win11
            DWMWA_BORDER_COLOR = 34                 # Win11 22000+
            DWMWA_CAPTION_COLOR = 35                # Win11 22000+
            DWMWA_TEXT_COLOR = 36                   # Win11 22000+

            for h in hwnds:
                # 1. Immersive dark mode (turns title bar dark on Win10 and Win11)
                res = ctypes.windll.dwmapi.DwmSetWindowAttribute(
                    h, DWMWA_USE_IMMERSIVE_DARK_MODE, ctypes.byref(val_on), ctypes.sizeof(val_on)
                )
                if res != 0:
                    ctypes.windll.dwmapi.DwmSetWindowAttribute(
                        h, DWMWA_USE_IMMERSIVE_DARK_MODE_OLD, ctypes.byref(val_on), ctypes.sizeof(val_on)
                    )

                # 2. Match exact caption color (#0A0A0E) on Windows 11
                if bg_color:
                    ctypes.windll.dwmapi.DwmSetWindowAttribute(
                        h, DWMWA_CAPTION_COLOR, ctypes.byref(caption_val), ctypes.sizeof(caption_val)
                    )

                # 3. Match caption text color (#E1E1E6)
                if text_color:
                    ctypes.windll.dwmapi.DwmSetWindowAttribute(
                        h, DWMWA_TEXT_COLOR, ctypes.byref(text_val), ctypes.sizeof(text_val)
                    )

                # 4. Match border color (#1E1E24)
                if border_color:
                    ctypes.windll.dwmapi.DwmSetWindowAttribute(
                        h, DWMWA_BORDER_COLOR, ctypes.byref(border_val), ctypes.sizeof(border_val)
                    )

                # Force non-client area frame redraw (SWP_NOSIZE | SWP_NOMOVE | SWP_NOZORDER | SWP_FRAMECHANGED)
                try:
                    ctypes.windll.user32.SetWindowPos(h, 0, 0, 0, 0, 0, 0x0001 | 0x0002 | 0x0004 | 0x0020)
                except Exception:
                    pass
        except Exception:
            pass

    _apply_dwm()
    try:
        window.after(30, _apply_dwm)
        window.after(150, _apply_dwm)
    except Exception:
        pass

class CTkToolTip:
    """Creates a sleek, hoverable info tooltip popup just above a widget."""
    def __init__(self, widget, text, delay_ms=180):
        self.widget = widget
        self.text = text
        self.delay_ms = delay_ms
        self.tip_window = None
        self._after_id = None
        
        self.widget.bind("<Enter>", self._on_enter, add="+")
        self.widget.bind("<Leave>", self._on_leave, add="+")
        self.widget.bind("<ButtonPress>", self._on_leave, add="+")
        self.widget.bind("<Destroy>", self._on_destroy, add="+")

    def _on_enter(self, event=None):
        self._cancel_schedule()
        self._after_id = self.widget.after(self.delay_ms, self.show)

    def _on_leave(self, event=None):
        self._cancel_schedule()
        self.hide()

    def _on_destroy(self, event=None):
        self._cancel_schedule()
        self.hide()

    def _cancel_schedule(self):
        if self._after_id:
            try:
                self.widget.after_cancel(self._after_id)
            except Exception:
                pass
            self._after_id = None

    def show(self, event=None):
        if self.tip_window or not self.text:
            return
        try:
            if not self.widget.winfo_exists():
                return
        except Exception:
            return

        try:
            self.tip_window = tw = tk.Toplevel(self.widget)
            tw.wm_overrideredirect(True)
            try:
                tw.attributes("-topmost", True)
            except Exception:
                pass

            # Container with a cyan accent border matching Chatterbox styling
            border_frame = tk.Frame(tw, background="#00D1FF", bd=1)
            border_frame.pack(fill="both", expand=True)

            inner_frame = tk.Frame(border_frame, background="#12151D", padx=8, pady=5)
            inner_frame.pack(fill="both", expand=True)

            label = tk.Label(
                inner_frame,
                text=self.text,
                justify="center",
                background="#12151D",
                foreground="#E0E6ED",
                font=("Arial", 9),
                wraplength=230
            )
            label.pack()

            tw.update_idletasks()
            
            # Position just above the widget
            w_x = self.widget.winfo_rootx()
            w_y = self.widget.winfo_rooty()
            w_w = self.widget.winfo_width()
            tip_w = tw.winfo_reqwidth()
            tip_h = tw.winfo_reqheight()

            pos_x = w_x + (w_w // 2) - (tip_w // 2)
            pos_y = w_y - tip_h - 6

            if pos_y < 0:
                pos_y = w_y + self.widget.winfo_height() + 6
            if pos_x < 5:
                pos_x = 5

            tw.wm_geometry(f"+{pos_x}+{pos_y}")
        except Exception:
            self.hide()

    def hide(self, event=None):
        self._cancel_schedule()
        if self.tip_window:
            try:
                self.tip_window.destroy()
            except Exception:
                pass
            self.tip_window = None

class App(ctk.CTk):
    def apply_dark_title_bar(self, window=None, bg_color="#0A0A0E", text_color="#E1E1E6", border_color="#1E1E24"):
        if window is None:
            window = self
        apply_dark_title_bar(window, bg_color=bg_color, text_color=text_color, border_color=border_color)

    def __init__(self):
        super().__init__()
        
        self.title("Kick TTS App v1.67")
        self.apply_dark_title_bar(self)
        self.session_start_time = None
        self.last_stream_activity_time = None
        self.stream_inactivity_threshold_hours = 3.5
        
        # Stream session summary tracking (persisted across same-day app restarts & overnight streams)
        self.app_session_start_time = datetime.datetime.now()
        self._summary_lock = threading.Lock()
        self.session_raids = []
        self.session_subs = []
        self.session_kicks = []
        self.session_powerchat = []
        self.session_date = datetime.datetime.now().strftime("%Y-%m-%d")
        self.last_active_timestamp = datetime.datetime.now().timestamp()
        self.stream_summary_window = None
        self._recorded_sub_renewals = {}
        self._save_kickbot_tts_timer = None
        self._save_roleplay_tts_timer = None
        
        self.grid_columnconfigure(0, weight=0, minsize=360)
        self.grid_columnconfigure(1, weight=0, minsize=340)
        self.grid_rowconfigure(0, weight=1)

        # Initialize engines
        self.model_ready = False
        self._model_status_text = "Loading AI Model into VRAM (0%)..."
        self._model_status_progress = 0.0
        self.tts = TTSEngine(on_model_status=self.on_tts_model_status)
        self.scraper = KickRealtimeChatClient(callback=self.on_chat_message)
        self.kickbot_listener = KickBotListener(
            callback_audio=self.on_kickbot_audio,
            callback_log=self.append_system_log,
            get_banned_users=lambda: self.settings.get("banned_users", []),
            get_timed_out_users=self.get_active_timed_out_usernames
        )
        self.powerchat_listener = PowerchatListener(callback_donation=self.on_powerchat_donation, callback_log=self.append_system_log)
        self.open_settings_windows = {}

        # Load settings
        self.load_settings()

        # Window sizing configuration (Left Panel 360px + Right Panel 340px + Margins)
        self.APP_WIDTH = 730
        
        self.deepseek_balance_text = "Not checked"
        self.load_stream_session()
        self.after(60000, self._stream_session_heartbeat)
        self.check_deepseek_balance(is_startup=True)
        self.timeouts = self.load_timeouts()
        self.check_timeouts_loop()
        
        # Windows Volume Mixer Controller
        self.win_volume = WindowsVolumeController()
        self._is_user_sliding_vol = False
        self._initial_volume_synced_to_windows = False
        self._last_vol_change_time = 0
        self.tts.on_audio_play_hook = self.ensure_windows_volume_applied
        self.after(200, lambda: self.win_volume.warmup(self.settings.get("app_volume", 100)))
        self.after(500, self.sync_windows_volume_loop)
        
        # Initialize RoleplayManager
        import roleplay
        self.roleplay_manager = roleplay.RoleplayManager(self)
        
        # Restore window geometry (Fixed width APP_WIDTH, resizable height)
        saved_geometry = self.settings.get("geometry", f"{self.APP_WIDTH}x961+278+302")
        try:
            parts = saved_geometry.split("+")
            size_part = parts[0].split("x")
            saved_h = int(size_part[1]) if len(size_part) > 1 else 961
            saved_x = int(parts[1]) if len(parts) > 1 else 278
            saved_y = int(parts[2]) if len(parts) > 2 else 302
        except Exception:
            saved_h, saved_x, saved_y = 961, 278, 302

        saved_h = max(600, saved_h)
        self.geometry(f"{self.APP_WIDTH}x{saved_h}+{saved_x}+{saved_y}")
        self.minsize(self.APP_WIDTH, 600)
        self.maxsize(self.APP_WIDTH, 99999)
        self.resizable(False, True)
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Active chatters tracking (unique users who typed message/emoji in past 20 min)
        self._active_chatters = {}
        self._active_chatters_lock = threading.Lock()
        self.active_chatters_window = None

        # Last generated manual TTS cache for copying via 'Save TTS'
        self.last_manual_tts_file = None
        self.last_manual_tts_cmd = None
        self.last_manual_tts_text = None

        self.setup_ui()
        self.apply_dark_title_bar(self)
        self.after(200, lambda: self.apply_dark_title_bar(self))
        self.after(5000, self._periodic_update_active_chatters)
        self.load_voices()
        self.refresh_voices()

        # Gifted subs aggregation and deduplication state
        self._pending_gifts = {}
        self._gift_lock = threading.Lock()
        self._processed_sub_keys = {}

        # Channel points items (stored in channel_points.json)
        self.channel_points_path = "channel_points.json"
        self.channel_points = self.load_channel_points()
        self.channel_points_window = None
        self._processed_cp_keys = {}

        # Register hotkeys
        self.register_hotkeys(is_startup=True)

    def trigger_hotkey_pause(self):
        try:
            if hasattr(self, "winfo_exists") and not self.winfo_exists():
                return
            import time
            now = time.time()
            if now - getattr(self, '_last_pause_hotkey_time', 0) < 0.25:
                return
            self._last_pause_hotkey_time = now
            self.toggle_global_pause()
        except Exception as e:
            try:
                self.append_system_log(f"\n[System] Hotkey pause error: {e}")
            except Exception:
                pass

    def trigger_hotkey_skip(self):
        try:
            if hasattr(self, "winfo_exists") and not self.winfo_exists():
                return
            import time
            now = time.time()
            if now - getattr(self, '_last_skip_hotkey_time', 0) < 0.25:
                return
            self._last_skip_hotkey_time = now
            if hasattr(self, 'tts') and self.tts:
                self.tts.skip_current()
        except Exception as e:
            try:
                self.append_system_log(f"\n[System] Hotkey skip error: {e}")
            except Exception:
                pass

    def on_tts_model_status(self, text: str, progress: float, is_ready: bool):
        self._model_status_text = text
        self._model_status_progress = progress
        try:
            self.after(0, lambda: self._handle_model_status(text, progress, is_ready))
        except Exception:
            pass

    def _handle_model_status(self, text: str, progress: float, is_ready: bool):
        if not is_ready:
            if hasattr(self, "model_loading_label") and self.model_loading_label.winfo_exists():
                self.model_loading_label.configure(text=text)
            if hasattr(self, "model_loading_bar") and self.model_loading_bar.winfo_exists():
                self.model_loading_bar.set(min(max(progress, 0.0), 1.0))
        else:
            self.model_ready = True
            if hasattr(self, "model_loading_frame") and self.model_loading_frame.winfo_exists():
                self.model_loading_frame.pack_forget()
            if hasattr(self, "monitor_btn") and self.monitor_btn.winfo_exists():
                if not self.monitor_btn.winfo_ismapped():
                    self.monitor_btn.pack(fill="x")
            if hasattr(self, "manual_submit_btn") and self.manual_submit_btn.winfo_exists():
                self.manual_submit_btn.configure(
                    state="normal", fg_color="#005E73", hover_color="#00D1FF", text_color="#FFFFFF"
                )
            if hasattr(self, "manual_save_btn") and self.manual_save_btn.winfo_exists():
                self.manual_save_btn.configure(
                    state="normal", fg_color="#005E73", hover_color="#00D1FF", text_color="#FFFFFF"
                )
            if hasattr(self, "manual_rp_submit_btn") and self.manual_rp_submit_btn.winfo_exists():
                self.manual_rp_submit_btn.configure(
                    state="normal", fg_color="#005E73", hover_color="#00D1FF", text_color="#FFFFFF"
                )
            if hasattr(self, "append_system_log"):
                self.append_system_log("[System] AI Voice Model loaded into VRAM. System ready.")

    def _is_mod_active(self, mod_name, event=None):
        mod_name = (mod_name or "").strip().lower()
        if not mod_name:
            return False

        # 1. On Windows, check physical key state directly from user32 (100% accurate, ignores NumLock state)
        import sys
        if sys.platform == "win32":
            try:
                import ctypes
                vk = 0x12 if mod_name == "alt" else (0x11 if mod_name in ("ctrl", "control") else 0)
                if vk and (ctypes.windll.user32.GetAsyncKeyState(vk) & 0x8000) != 0:
                    return True
            except Exception:
                pass

        # 2. Check keyboard module if imported & active
        try:
            import keyboard
            target_kb_mod = "alt" if mod_name == "alt" else "ctrl"
            if keyboard.is_pressed(target_kb_mod):
                return True
        except Exception:
            pass

        # 3. Check tracked state from Tkinter key events
        if mod_name == "alt" and getattr(self, "_alt_held", False):
            return True
        if mod_name in ("ctrl", "control") and getattr(self, "_ctrl_held", False):
            return True

        # 4. Check Tkinter event state bitmask (specifically excluding NumLock 0x0008 and AltGr/ModeSwitch 0x0080)
        state = getattr(event, "state", 0) if event is not None else 0
        if mod_name in ("ctrl", "control"):
            # 0x0004 is standard Control mask in Tkinter
            if bool(state & 0x0004):
                return True
        elif mod_name == "alt":
            # 0x20000 (131072) is Windows Tkinter Alt mask; 0x0008 is NEVER checked because 0x0008 is NumLock
            if bool(state & 0x20000):
                return True

        return False

    def _on_tk_mod_down(self, event):
        try:
            keysym = (event.keysym or "").lower()
            if "alt" in keysym:
                self._alt_held = True
            if "control" in keysym or "ctrl" in keysym:
                self._ctrl_held = True
        except Exception:
            pass

    def _on_tk_key_release(self, event):
        try:
            keysym = (event.keysym or "").lower()
            if "alt" in keysym:
                self._alt_held = False
            if "control" in keysym or "ctrl" in keysym:
                self._ctrl_held = False
        except Exception:
            pass

    def _on_tk_focus_reset(self, event=None):
        self._alt_held = False
        self._ctrl_held = False

    def _on_tk_key_press(self, event):
        try:
            pause_mod = self.settings.get("pause_hotkey_mod", "Alt").strip().lower()
            pause_key = str(self.settings.get("pause_hotkey_key", "1")).strip().lower()
            skip_mod = self.settings.get("skip_hotkey_mod", "Alt").strip().lower()
            skip_key = str(self.settings.get("skip_hotkey_key", "2")).strip().lower()

            keysym = (event.keysym or "").lower()
            char = (event.char or "").lower()
            keycode = getattr(event, 'keycode', None)

            # Normalize numpad symbols like kp_1 -> 1
            clean_sym = keysym.replace("kp_", "")

            def matches_key(target_k):
                if not target_k:
                    return False
                if clean_sym == target_k or keysym == target_k or char == target_k:
                    return True
                if target_k.isdigit() and keycode is not None:
                    d = int(target_k)
                    if keycode in (48 + d, 96 + d):
                        return True
                return False

            # Check pause match
            pause_mod_ok = self._is_mod_active(pause_mod, event)
            if pause_mod_ok and matches_key(pause_key):
                self.trigger_hotkey_pause()
                return "break"

            # Check skip match
            skip_mod_ok = self._is_mod_active(skip_mod, event)
            if skip_mod_ok and matches_key(skip_key):
                self.trigger_hotkey_skip()
                return "break"
        except Exception:
            pass

    def _get_win32_mod_flags(self, mod_str):
        mod_str = (mod_str or "").strip().lower()
        # MOD_NOREPEAT (0x4000) prevents repeated triggers while holding down the key
        flags = 0x4000
        if "alt" in mod_str:
            flags |= 0x0001
        if "ctrl" in mod_str or "control" in mod_str:
            flags |= 0x0002
        if "shift" in mod_str:
            flags |= 0x0004
        if "win" in mod_str:
            flags |= 0x0008
        return flags

    def _get_win32_vk_code(self, key_str):
        k = (key_str or "").strip().lower()
        if not k:
            return 0
        if len(k) == 1 and k.isdigit():
            return 0x30 + int(k)
        if len(k) == 1 and 'a' <= k <= 'z':
            return 0x41 + (ord(k) - ord('a'))
        if k.startswith("f") and k[1:].isdigit():
            f_num = int(k[1:])
            if 1 <= f_num <= 24:
                return 0x70 + (f_num - 1)
        if "kp_" in k or "numpad" in k or "num_" in k:
            clean = k.replace("kp_", "").replace("numpad", "").replace("num_", "").strip()
            if clean.isdigit():
                return 0x60 + int(clean)
        special = {
            "space": 0x20, "tab": 0x09, "enter": 0x0D, "return": 0x0D,
            "esc": 0x1B, "escape": 0x1B, "backspace": 0x08, "delete": 0x2E,
            "insert": 0x2D, "home": 0x24, "end": 0x23, "pageup": 0x21,
            "pagedown": 0x22, "left": 0x25, "up": 0x26, "right": 0x27, "down": 0x28,
        }
        if k in special:
            return special[k]
        import sys
        if sys.platform == "win32":
            try:
                import ctypes
                res = ctypes.windll.user32.VkKeyScanW(ord(k[0]))
                if res != -1:
                    return res & 0xFF
            except Exception:
                pass
        return 0

    def _stop_win_hotkey_thread(self):
        tid = getattr(self, '_win_hotkey_thread_id', None)
        thread = getattr(self, '_win_hotkey_thread', None)
        if tid and thread and thread.is_alive():
            import sys
            if sys.platform == "win32":
                try:
                    import ctypes
                    WM_QUIT = 0x0012
                    ctypes.windll.user32.PostThreadMessageW(tid, WM_QUIT, 0, 0)
                except Exception:
                    pass
            try:
                thread.join(timeout=0.3)
            except Exception:
                pass
        self._win_hotkey_thread = None
        self._win_hotkey_thread_id = None

    def _win_hotkey_worker(self, pause_vk, pause_flags, skip_vk, skip_flags, ready_event, result_holder):
        import sys
        if sys.platform != "win32":
            ready_event.set()
            return

        import ctypes
        from ctypes import wintypes

        user32 = ctypes.windll.user32
        kernel32 = ctypes.windll.kernel32

        class MSG(ctypes.Structure):
            _fields_ = [
                ("hwnd", wintypes.HWND),
                ("message", wintypes.UINT),
                ("wParam", wintypes.WPARAM),
                ("lParam", wintypes.LPARAM),
                ("time", wintypes.DWORD),
                ("pt", wintypes.POINT),
            ]

        WM_HOTKEY = 0x0312
        PM_NOREMOVE = 0x0000
        MOD_NOREPEAT = 0x4000
        ERROR_HOTKEY_ALREADY_REGISTERED = 1409

        tid = kernel32.GetCurrentThreadId()
        self._win_hotkey_thread_id = tid

        # Force message queue to be created for this thread before signaling ready
        msg = MSG()
        user32.PeekMessageW(ctypes.byref(msg), 0, 0, 0, PM_NOREMOVE)

        HOTKEY_ID_PAUSE = 1
        HOTKEY_ID_SKIP = 2

        registered_pause = False
        registered_skip = False

        if pause_vk:
            ok = user32.RegisterHotKey(0, HOTKEY_ID_PAUSE, pause_flags, pause_vk)
            if not ok and (pause_flags & MOD_NOREPEAT):
                ok = user32.RegisterHotKey(0, HOTKEY_ID_PAUSE, pause_flags & ~MOD_NOREPEAT, pause_vk)
            if ok:
                registered_pause = True
            else:
                err = ctypes.GetLastError()
                msg_err = "already in use by another application" if err == ERROR_HOTKEY_ALREADY_REGISTERED else f"error code {err}"
                result_holder["pause_err"] = msg_err

        if skip_vk:
            ok = user32.RegisterHotKey(0, HOTKEY_ID_SKIP, skip_flags, skip_vk)
            if not ok and (skip_flags & MOD_NOREPEAT):
                ok = user32.RegisterHotKey(0, HOTKEY_ID_SKIP, skip_flags & ~MOD_NOREPEAT, skip_vk)
            if ok:
                registered_skip = True
            else:
                err = ctypes.GetLastError()
                msg_err = "already in use by another application" if err == ERROR_HOTKEY_ALREADY_REGISTERED else f"error code {err}"
                result_holder["skip_err"] = msg_err

        result_holder["registered_pause"] = registered_pause
        result_holder["registered_skip"] = registered_skip
        ready_event.set()

        try:
            while user32.GetMessageW(ctypes.byref(msg), 0, 0, 0) > 0:
                if msg.message == WM_HOTKEY:
                    if msg.wParam == HOTKEY_ID_PAUSE:
                        self.after(0, self.trigger_hotkey_pause)
                    elif msg.wParam == HOTKEY_ID_SKIP:
                        self.after(0, self.trigger_hotkey_skip)
        finally:
            if registered_pause:
                try:
                    user32.UnregisterHotKey(0, HOTKEY_ID_PAUSE)
                except Exception:
                    pass
            if registered_skip:
                try:
                    user32.UnregisterHotKey(0, HOTKEY_ID_SKIP)
                except Exception:
                    pass
            self._win_hotkey_thread_id = None

    def unregister_hotkeys(self):
        self._stop_win_hotkey_thread()
        try:
            import keyboard
            keyboard.unhook_all_hotkeys()
        except Exception:
            pass

    def register_hotkeys(self, is_startup=False):
        pause_mod = self.settings.get("pause_hotkey_mod", "Alt")
        pause_key = str(self.settings.get("pause_hotkey_key", "1")).strip()
        skip_mod = self.settings.get("skip_hotkey_mod", "Alt")
        skip_key = str(self.settings.get("skip_hotkey_key", "2")).strip()

        global_pause_str = f"{pause_mod.lower()}+{pause_key.lower()}" if pause_key else None
        global_skip_str = f"{skip_mod.lower()}+{skip_key.lower()}" if skip_key else None

        # Clean up any previously registered hotkeys
        self.unregister_hotkeys()

        # 1. Native Windows RegisterHotKey API (immune to LowLevelHooksTimeout and anti-cheat hook drops)
        import sys
        import threading
        win_pause_ok = False
        win_skip_ok = False

        if sys.platform == "win32":
            try:
                pause_flags = self._get_win32_mod_flags(pause_mod)
                pause_vk = self._get_win32_vk_code(pause_key)
                skip_flags = self._get_win32_mod_flags(skip_mod)
                skip_vk = self._get_win32_vk_code(skip_key)

                ready_event = threading.Event()
                result_holder = {}

                t = threading.Thread(
                    target=self._win_hotkey_worker,
                    args=(pause_vk, pause_flags, skip_vk, skip_flags, ready_event, result_holder),
                    daemon=True,
                    name="Win32RegisterHotKeyThread"
                )
                self._win_hotkey_thread = t
                t.start()
                ready_event.wait(timeout=1.0)

                win_pause_ok = result_holder.get("registered_pause", False)
                win_skip_ok = result_holder.get("registered_skip", False)

                if "pause_err" in result_holder:
                    self.after(1000 if is_startup else 0, lambda: self.append_system_log(
                        f"\n[System] Warning: Windows RegisterHotKey for Pause ({pause_mod}+{pause_key}) failed ({result_holder['pause_err']})."
                    ))
                if "skip_err" in result_holder:
                    self.after(1000 if is_startup else 0, lambda: self.append_system_log(
                        f"\n[System] Warning: Windows RegisterHotKey for Skip ({skip_mod}+{skip_key}) failed ({result_holder['skip_err']})."
                    ))
                if win_pause_ok or win_skip_ok:
                    self.after(1000 if is_startup else 0, lambda: self.append_system_log(
                        f"\n[System] Native Windows global hotkeys active: Pause ({pause_mod}+{pause_key}), Skip ({skip_mod}+{skip_key})."
                    ))
            except Exception as e:
                self.after(1000 if is_startup else 0, lambda: self.append_system_log(
                    f"\n[System] Warning: Could not initialize native RegisterHotKey: {e}"
                ))

        # 2. Fallback to 'keyboard' module for any hotkey that wasn't registered via native API
        try:
            import keyboard
            if not win_pause_ok and global_pause_str:
                keyboard.add_hotkey(global_pause_str, lambda: self.after(0, self.trigger_hotkey_pause))
            if not win_skip_ok and global_skip_str:
                keyboard.add_hotkey(global_skip_str, lambda: self.after(0, self.trigger_hotkey_skip))
        except ImportError:
            if not (win_pause_ok and win_skip_ok) and is_startup:
                self.after(1000, lambda: self.append_system_log(
                    "\n[System] Note: Global hotkeys operating via native Windows API."
                ))
        except Exception as e:
            if not (win_pause_ok and win_skip_ok) and is_startup:
                self.after(1000, lambda: self.append_system_log(
                    f"\n[System] Warning: Low-level keyboard hook fallback unavailable: {e}"
                ))

        # 3. Local focused window key events (works when any window or widget has focus)
        if not getattr(self, '_tk_key_bindings_initialized', False):
            try:
                self.bind_all("<KeyPress>", self._on_tk_key_press, add="+")
                self.bind_all("<KeyRelease>", self._on_tk_key_release, add="+")
                self.bind_all("<Alt_L>", self._on_tk_mod_down, add="+")
                self.bind_all("<Alt_R>", self._on_tk_mod_down, add="+")
                self.bind_all("<Control_L>", self._on_tk_mod_down, add="+")
                self.bind_all("<Control_R>", self._on_tk_mod_down, add="+")
                self.bind_all("<FocusOut>", self._on_tk_focus_reset, add="+")
                self.bind_all("<FocusIn>", self._on_tk_focus_reset, add="+")
                self._tk_key_bindings_initialized = True
            except Exception:
                pass

    def check_deepseek_balance(self, update_label=None, is_startup=False):
        import threading
        
        api_key = self.settings.get("deepseek_api_key", "").strip()
        if not api_key:
            if is_startup:
                self.deepseek_balance_text = "Not checked (No API Key)"
                return
            else:
                self.deepseek_balance_text = "Please enter a valid Deepseek API key."
                if update_label:
                    update_label.configure(text=self.deepseek_balance_text, text_color="#FF4C4C", font=("Arial", 12))
                return

        def _check():
            import requests
            try:
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                }
                resp = requests.get("https://api.deepseek.com/user/balance", headers=headers, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                
                if "balance_infos" in data and len(data["balance_infos"]) > 0:
                    balances = []
                    for info in data["balance_infos"]:
                        bal = info.get("total_balance", "?")
                        curr = info.get("currency", "")
                        balances.append(f"${bal} {curr}".strip())
                    self.deepseek_balance_text = " / ".join(balances)
                else:
                    import json
                    self.deepseek_balance_text = json.dumps(data)
                
            except Exception as e:
                if hasattr(e, 'response') and e.response is not None:
                    try:
                        err_data = e.response.json()
                        msg = err_data.get("error", {}).get("message", str(e))
                        self.deepseek_balance_text = f"Error: {msg}"
                    except:
                        self.deepseek_balance_text = f"Error: {e}"
                else:
                    self.deepseek_balance_text = f"Error: {e}"
                    
            if update_label:
                if "$" in self.deepseek_balance_text:
                    self.after(0, lambda: update_label.configure(text=self.deepseek_balance_text, text_color="#2ECC71", font=("Arial", 18, "bold")))
                else:
                    self.after(0, lambda: update_label.configure(text=self.deepseek_balance_text, text_color="#E1E1E6", font=("Arial", 12)))

        self.deepseek_balance_text = "Checking..."
        if update_label:
            update_label.configure(text=self.deepseek_balance_text, text_color="#E1E1E6", font=("Arial", 12))
        threading.Thread(target=_check, daemon=True).start()

    def load_settings(self):
        import json
        import os
        self.settings = {
            "url": "https://kick.com/popout/[username]/chat", 
            "global_mute": False,
            "pause_hotkey_mod": "Alt",
            "pause_hotkey_key": "1",
            "skip_hotkey_mod": "Alt",
            "skip_hotkey_key": "2",
            "app_volume": 100,
            "voice_mapping_enabled": True,
            "raid_alerts_enabled": True,
            "raid_alerts_voice": "",
            "sub_alerts_enabled": True,
            "kickbot_sub_alerts_enabled": True,
            "sub_celebration_alerts_enabled": True,
            "gifted_sub_alerts_enabled": True,
            "gifted_sub_celebration_alerts_enabled": True,
            "sub_alerts_voice": "",
            "active_chatters_duration": "20 mins",
            "enable_points_tts": True,
            "deepseek_model": "deepseek-flash",
            "powerchat_enabled": True,
            "powerchat_tts_link": "https://powerchat.live/jimbozoomer/tts",
            "kick_donos": {
                "sound_enabled": False,
                "sound_file": "",
                "tts_enabled": False,
                "tts_voice": "!narrator",
                "tts_message": "Thank you [user] for the [amount] kicks! [message]",
                "min_alert_amount": 100,
                "log_min_amount": 1
            },
            "saved_audio_dir": os.path.join(os.path.dirname(os.path.abspath(__file__)), "saved audio"),
            "save_kickbot_tts": False,
            "save_roleplay_tts": False
        }
        if os.path.exists("settings.json"):
            try:
                with open("settings.json", "r") as f:
                    loaded = json.load(f)
                    self.settings.update(loaded)
            except Exception as e:
                print(f"[System] Warning: Could not load settings.json: {e}")

        # Ephemeral session toggles: Save Kickbot TTS and Save Roleplay TTS always default to unchecked on startup and are never saved
        self.settings["save_kickbot_tts"] = False
        self.settings["save_roleplay_tts"] = False
                
        # Link mute flag during boot
        self.tts.is_muted = self.settings.get("global_mute", False)
        self.tts.set_paused(self.settings.get("global_pause", False))

    def get_saved_audio_dir(self):
        default_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "saved audio")
        saved_dir = str(self.settings.get("saved_audio_dir", "") or "").strip()
        target_dir = saved_dir if saved_dir else default_dir
        try:
            os.makedirs(target_dir, exist_ok=True)
            # Clean up any legacy hidden cache files from saved audio directory
            for legacy_cache in (".last_manual_tts.wav", ".last_manual_tts"):
                legacy_path = os.path.join(target_dir, legacy_cache)
                if os.path.exists(legacy_path):
                    try:
                        os.remove(legacy_path)
                    except Exception:
                        pass
        except Exception:
            pass
        return target_dir

    def save_settings(self, immediate=False):
        if immediate:
            if hasattr(self, '_save_settings_timer') and self._save_settings_timer:
                try:
                    self.after_cancel(self._save_settings_timer)
                except Exception:
                    pass
                self._save_settings_timer = None
            self._do_save_settings()
            return
            
        if hasattr(self, '_save_settings_timer') and self._save_settings_timer:
            try:
                self.after_cancel(self._save_settings_timer)
            except Exception:
                pass
        self._save_settings_timer = self.after(400, self._do_save_settings)

    def _do_save_settings(self):
        self._save_settings_timer = None
        import json
        try:
            settings_to_save = dict(self.settings)
            settings_to_save.pop("save_kickbot_tts", None)
            settings_to_save.pop("save_roleplay_tts", None)
            with open("settings.json", "w") as f:
                json.dump(settings_to_save, f, indent=4)
            if hasattr(self, 'scraper'):
                self.scraper.adbot_usernames = [u.strip().lstrip("@").lower() for u in self.settings.get("adbot_usernames", []) if u.strip()]
        except Exception as e:
            self.append_system_log(f"\n[System] Error saving settings.json: {e}")

    def _auto_disable_save_kickbot_tts(self):
        self._save_kickbot_tts_timer = None
        if hasattr(self, "save_kickbot_tts_var") and self.save_kickbot_tts_var.get():
            self.save_kickbot_tts_var.set(False)
            self.settings["save_kickbot_tts"] = False
            self.append_system_log("\n[Safety] 'Save Kickbot TTS' automatically disabled after 1 hour safety limit.")

    def _auto_disable_save_roleplay_tts(self):
        self._save_roleplay_tts_timer = None
        if hasattr(self, "save_roleplay_tts_var") and self.save_roleplay_tts_var.get():
            self.save_roleplay_tts_var.set(False)
            self.settings["save_roleplay_tts"] = False
            self.append_system_log("\n[Safety] 'Save Roleplay TTS' automatically disabled after 1 hour safety limit.")

    def load_channel_points(self):
        import json
        import os
        config_path = getattr(self, "channel_points_path", "channel_points.json")
        if os.path.exists(config_path):
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        return data
            except Exception as e:
                print(f"[System] Warning: Could not load channel_points.json: {e}")
        return []

    def save_channel_points(self):
        import json
        config_path = getattr(self, "channel_points_path", "channel_points.json")
        try:
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(self.channel_points, f, indent=4)
        except Exception as e:
            self.append_system_log(f"\n[System] Error saving channel_points.json: {e}")

    def load_voices(self):
        import json
        import os
        os.makedirs("voices", exist_ok=True)
        config_path = "voices.json"
        
        if os.path.exists(config_path):
            try:
                with open(config_path, "r") as f:
                    self.voices = json.load(f)
                
                # Sort voices alphabetically by name
                self.voices.sort(key=lambda x: x.get('name', '').lower())
                
                # Clear default voices from TTS engine and inject our saved ones
                self.tts.active_voices.clear()
                for v in self.voices:
                    if "path" in v:
                        exag = v.get("exaggeration", 0.5)
                        temp = v.get("temperature", 0.4)
                        
                        self.tts.add_voice(v["command"], v["path"], temperature=temp)
            except Exception as e:
                self.voices = []
                self.append_system_log(f"\n[System] Warning: Could not load voices.json: {e}")
        else:
            self.voices = []
            self.save_voices()
            
    def save_voices(self, immediate=False):
        if immediate:
            if hasattr(self, '_save_voices_timer') and self._save_voices_timer:
                try:
                    self.after_cancel(self._save_voices_timer)
                except Exception:
                    pass
                self._save_voices_timer = None
            self._do_save_voices()
            return
            
        if hasattr(self, '_save_voices_timer') and self._save_voices_timer:
            try:
                self.after_cancel(self._save_voices_timer)
            except Exception:
                pass
        self._save_voices_timer = self.after(400, self._do_save_voices)

    def _do_save_voices(self):
        self._save_voices_timer = None
        import json
        try:
            with open("voices.json", "w") as f:
                json.dump(self.voices, f, indent=4)
        except Exception as e:
            self.append_system_log(f"\n[System] Error saving voices.json: {e}")

    def setup_ui(self):
        # LEFT PANEL (Settings & Logs)
        self.left_panel = ctk.CTkFrame(self, fg_color="#121216", border_width=1, border_color="#1e1e24")
        self.left_panel.grid(row=0, column=0, padx=(10, 5), pady=10, sticky="nsew")

        btn_frame = ctk.CTkFrame(self.left_panel, fg_color="transparent")
        btn_frame.pack(fill="x", padx=10, pady=(10, 5))
        
        self.stream_summary_btn = ctk.CTkButton(
            btn_frame, 
            text="SUMMARY", 
            width=64, 
            height=24, 
            fg_color="#3A3A40", 
            hover_color="#55555C", 
            font=("Arial", 10, "bold"), 
            command=self.open_stream_summary
        )
        self.stream_summary_btn.pack(side="right")

        ctk.CTkButton(btn_frame, text="SETTINGS", width=62, height=24, fg_color="#3A3A40", hover_color="#55555C", font=("Arial", 10, "bold"), command=self.open_app_settings).pack(side="right", padx=(0, 4))
        ctk.CTkButton(btn_frame, text="ADS", width=36, height=24, fg_color="#3A3A40", hover_color="#55555C", font=("Arial", 10, "bold"), command=self.open_ad_settings).pack(side="right", padx=(0, 4))
        ctk.CTkButton(btn_frame, text="MAPPING", width=58, height=24, fg_color="#3A3A40", hover_color="#55555C", font=("Arial", 10, "bold"), command=self.open_voice_mapping).pack(side="right", padx=(0, 4))
        ctk.CTkButton(btn_frame, text="DONOS", width=50, height=24, fg_color="#3A3A40", hover_color="#55555C", font=("Arial", 10, "bold"), command=self.open_kick_donos_settings).pack(side="right", padx=(0, 4))
        ctk.CTkButton(btn_frame, text="POINTS", width=48, height=24, fg_color="#3A3A40", hover_color="#55555C", font=("Arial", 10, "bold"), command=self.open_channel_points_settings).pack(side="right", padx=(0, 4))

        header_frame = ctk.CTkFrame(self.left_panel, fg_color="transparent")
        header_frame.pack(fill="x", padx=15, pady=(5, 4))
        
        ctk.CTkLabel(header_frame, text="Kick Username:", font=("Arial", 12, "bold"), text_color="#8E9299").pack(side="left", padx=(0, 10))
        
        saved_target = self.settings.get("kick_username") or self.settings.get("url", "")
        # Clean up legacy URLs if present in saved settings
        if "kick.com/" in saved_target:
            p = saved_target.split("kick.com/")[-1].split("/")
            saved_target = p[1] if p[0] == "popout" and len(p) > 1 else p[0]
            saved_target = saved_target.split("?")[0].split("#")[0]
        if not saved_target:
            saved_target = "jimboz"

        self.url_entry = ctk.CTkEntry(header_frame, placeholder_text="e.g. jimboz", height=28, fg_color="#0A0A0E", border_color="#1e1e24")
        self.url_entry.insert(0, saved_target)
        self.url_entry.pack(side="left", fill="x", expand=True)
        
        # Save Username when user types and defocuses
        self.url_entry.bind("<KeyRelease>", self.on_url_change)
        self.url_entry.bind("<FocusOut>", self.on_url_change)
        self.url_entry.bind("<Return>", self.on_url_change)

        # Action / Monitoring Button Container (placed where START MONITORING CHAT button lives)
        self.monitor_container = ctk.CTkFrame(self.left_panel, fg_color="transparent")
        self.monitor_container.pack(pady=(0, 0), padx=15, fill="x")

        # The START MONITORING CHAT button is always immediately accessible
        self.monitor_btn = ctk.CTkButton(
            self.monitor_container, text="START MONITORING CHAT", 
            fg_color="#005E73", hover_color="#00D1FF",
            command=self.toggle_monitoring
        )
        self.monitor_btn.pack(fill="x")

        # Loading Progress Bar and Label (located just under START MONITORING CHAT while AI model weights load into VRAM)
        self.model_loading_frame = ctk.CTkFrame(self.monitor_container, fg_color="#13151A", corner_radius=6, border_width=1, border_color="#1E2330")
        self.model_loading_label = ctk.CTkLabel(
            self.model_loading_frame,
            text=getattr(self, "_model_status_text", "Loading AI Model into VRAM (0%)..."),
            font=("Arial", 11, "bold"),
            text_color="#00D1FF"
        )
        self.model_loading_label.pack(pady=(5, 3), padx=10)

        self.model_loading_bar = ctk.CTkProgressBar(
            self.model_loading_frame,
            height=8,
            progress_color="#00D1FF",
            fg_color="#090B0E"
        )
        self.model_loading_bar.set(getattr(self, "_model_status_progress", 0.0))
        self.model_loading_bar.pack(pady=(0, 6), padx=12, fill="x")

        if not getattr(self, "model_ready", False):
            self.model_loading_frame.pack(fill="x", pady=(6, 0))

        # Pause Button (Beneath START MONITORING CHAT in Left Panel)
        self.pause_btn = ctk.CTkButton(
            self.left_panel, text="⏸ PAUSE", 
            fg_color="#005E73", hover_color="#00D1FF",
            command=self.toggle_global_pause
        )
        self.pause_btn.pack(padx=15, pady=(14, 6), fill="x")
        self.update_pause_button_ui()

        # Left Panel Audio Controls: Volume Slider Container (Beneath PAUSE button)
        self.vol_container = ctk.CTkFrame(
            self.left_panel, 
            fg_color="#18181E", 
            border_width=1, 
            border_color="#2A2A36", 
            corner_radius=6,
            height=28
        )
        self.vol_container.pack(padx=15, pady=(0, 10), fill="x")
        self.vol_container.pack_propagate(False)

        initial_vol = self.settings.get("app_volume", 100)
        icon_str = "🔇" if initial_vol <= 0 else ("🔈" if initial_vol < 34 else ("🔉" if initial_vol < 67 else "🔊"))
        self.vol_icon_label = ctk.CTkLabel(
            self.vol_container, 
            text=icon_str, 
            font=("Segoe UI Emoji", 13), 
            text_color="#8E9299", 
            width=24, 
            height=18,
            fg_color="transparent"
        )
        self.vol_icon_label.pack(side="left", padx=(10, 4), pady=0)

        self.vol_slider = ctk.CTkSlider(
            self.vol_container,
            from_=0,
            to=100,
            number_of_steps=100,
            height=14,
            button_length=12,
            fg_color="#0D0D11",
            progress_color="#00D1FF",
            button_color="#00D1FF",
            button_hover_color="#55E2FF",
            command=self.on_volume_slider_change
        )
        self.vol_slider.set(initial_vol)
        self.vol_slider.pack(side="left", fill="x", expand=True, padx=(4, 6), pady=0)

        self.vol_percent_label = ctk.CTkLabel(
            self.vol_container, 
            text=f"{int(initial_vol)}%", 
            font=("Arial", 11, "bold"), 
            text_color="#E1E1E6", 
            width=40, 
            height=18,
            anchor="e",
            fg_color="transparent"
        )
        self.vol_percent_label.pack(side="right", padx=(0, 10), pady=0)

        try:
            self.vol_slider.bind("<ButtonPress-1>", lambda e: self._on_vol_slider_press(), add="+")
            self.vol_slider.bind("<ButtonRelease-1>", lambda e: self._on_vol_slider_release(), add="+")
            if hasattr(self.vol_slider, "_canvas"):
                self.vol_slider._canvas.bind("<ButtonPress-1>", lambda e: self._on_vol_slider_press(), add="+")
                self.vol_slider._canvas.bind("<ButtonRelease-1>", lambda e: self._on_vol_slider_release(), add="+")
        except Exception:
            pass

        # Voice Quick Action Row (Below Volume Bar)
        voice_action_row = ctk.CTkFrame(self.left_panel, fg_color="transparent")
        voice_action_row.pack(fill="x", padx=12, pady=(0, 10))

        # "Add Voice" button in the same style as the 6 settings buttons at the top
        self.left_add_voice_btn = ctk.CTkButton(
            voice_action_row,
            text="ADD VOICE",
            width=78,
            height=24,
            fg_color="#3A3A40",
            hover_color="#55555C",
            font=("Arial", 10, "bold"),
            command=self.open_add_voice_window
        )
        self.left_add_voice_btn.pack(side="left", padx=(0, 4))

        # "View Voices" button in the same style
        self.left_view_voices_btn = ctk.CTkButton(
            voice_action_row,
            text="VIEW VOICES",
            width=86,
            height=24,
            fg_color="#3A3A40",
            hover_color="#55555C",
            font=("Arial", 10, "bold"),
            command=self.open_voice_library_window
        )
        self.left_view_voices_btn.pack(side="left", padx=(0, 4))

        # "Search voice:" label and textbox next to "View Voices" button
        ctk.CTkLabel(
            voice_action_row,
            text="Search voice:",
            font=("Arial", 10, "bold"),
            text_color="#8E9299"
        ).pack(side="left", padx=(0, 4))

        self.voice_quick_search_var = ctk.StringVar()
        self.voice_quick_search_entry = ctk.CTkEntry(
            voice_action_row,
            textvariable=self.voice_quick_search_var,
            placeholder_text="!voice...",
            width=48,
            height=24,
            font=("Arial", 10)
        )
        self.voice_quick_search_entry.pack(side="left", fill="x", expand=True)

        self.voice_quick_search_var.trace_add("write", self._on_quick_voice_search_changed)
        self.voice_quick_search_entry.bind("<Down>", self._on_quick_voice_search_down)
        self.voice_quick_search_entry.bind("<Up>", self._on_quick_voice_search_up)
        self.voice_quick_search_entry.bind("<Return>", self._on_quick_voice_search_enter)
        self.voice_quick_search_entry.bind("<Escape>", self._hide_quick_voice_dropdown)
        self.voice_quick_search_entry.bind("<FocusOut>", self._on_quick_voice_search_focus_out)
        self.bind("<Button-1>", self._on_root_click_check_dropdown, add="+")

        # --- Manual TTS Section ---
        manual_tts_frame = ctk.CTkFrame(self.left_panel, fg_color="transparent")
        manual_tts_frame.pack(fill="x", padx=15, pady=(0, 5))
        
        manual_title_row = ctk.CTkFrame(manual_tts_frame, fg_color="transparent")
        manual_title_row.pack(fill="x", pady=(0, 4))
        
        ctk.CTkLabel(manual_title_row, text="MANUAL TTS", font=("Arial", 12, "bold"), text_color="#8E9299", height=14).pack(side="left")
        
        cmd_row = ctk.CTkFrame(manual_tts_frame, fg_color="transparent")
        cmd_row.pack(fill="x", pady=(0, 5))
        ctk.CTkLabel(cmd_row, text="TTS Command:").pack(side="left", padx=(0, 10))
        
        self._suppress_manual_cmd_dropdown = True
        self.manual_cmd_var = ctk.StringVar(value=self.settings.get("manual_tts_command", "!soyjak"))
        def save_manual_cmd(*args):
            self.settings["manual_tts_command"] = self.manual_cmd_var.get()
            self.save_settings()
        self.manual_cmd_var.trace_add("write", save_manual_cmd)

        manual_cmd_entry = ctk.CTkEntry(cmd_row, textvariable=self.manual_cmd_var, placeholder_text="!voice command", height=30)
        manual_cmd_entry.pack(side="left", fill="x", expand=True)
        self.manual_cmd_entry = manual_cmd_entry
        self.manual_cmd_var.trace_add("write", self._on_manual_cmd_search_changed)
        self.manual_cmd_entry.bind("<Down>", self._on_manual_cmd_down)
        self.manual_cmd_entry.bind("<Up>", self._on_manual_cmd_up)
        self.manual_cmd_entry.bind("<Return>", self._on_manual_cmd_enter)
        self.manual_cmd_entry.bind("<Escape>", self._hide_manual_cmd_dropdown)
        self.manual_cmd_entry.bind("<FocusOut>", self._on_manual_cmd_focus_out)
        self.manual_cmd_entry.bind("<KeyRelease>", lambda e: setattr(self, "_suppress_manual_cmd_dropdown", False), add="+")
        
        custom_msg_row = ctk.CTkFrame(manual_tts_frame, fg_color="transparent")
        custom_msg_row.pack(fill="x", pady=(5, 0))
        ctk.CTkLabel(custom_msg_row, text="Custom Message:").pack(side="left")

        save_kb_right_frame = ctk.CTkFrame(custom_msg_row, fg_color="transparent")
        save_kb_right_frame.pack(side="right")

        save_kb_label = ctk.CTkLabel(save_kb_right_frame, text="Save Kickbot TTS:", font=("Arial", 12), cursor="hand2")
        save_kb_label.pack(side="left", padx=(0, 6))

        self.save_kickbot_tts_var = ctk.BooleanVar(value=False)

        def on_save_kickbot_tts_toggle():
            val = bool(self.save_kickbot_tts_var.get())
            self.settings["save_kickbot_tts"] = val
            if hasattr(self, '_save_kickbot_tts_timer') and self._save_kickbot_tts_timer:
                try:
                    self.after_cancel(self._save_kickbot_tts_timer)
                except Exception:
                    pass
                self._save_kickbot_tts_timer = None

            if val:
                self.append_system_log("\n[Safety] 'Save Kickbot TTS' enabled. It will automatically disable after 1 hour.")
                self._save_kickbot_tts_timer = self.after(3600 * 1000, self._auto_disable_save_kickbot_tts)

        self.save_kickbot_tts_checkbox = ctk.CTkCheckBox(
            save_kb_right_frame,
            text="",
            variable=self.save_kickbot_tts_var,
            command=on_save_kickbot_tts_toggle,
            width=20,
            checkbox_width=18,
            checkbox_height=18,
            fg_color="#00D1FF",
            hover_color="#005E73"
        )
        self.save_kickbot_tts_checkbox.pack(side="left")

        def toggle_save_kickbot_tts(event=None):
            self.save_kickbot_tts_var.set(not self.save_kickbot_tts_var.get())
            on_save_kickbot_tts_toggle()

        save_kb_label.bind("<Button-1>", toggle_save_kickbot_tts)

        self.manual_text_box = ctk.CTkTextbox(manual_tts_frame, height=80, font=("Arial", 12), fg_color="#0A0A0E", text_color="#E1E1E6", wrap="word")
        self.manual_text_box.insert("1.0", self.settings.get("manual_tts_message", "insert your custom message here"))
        self.manual_text_box.pack(fill="x", pady=(0, 5))
        
        def save_manual_msg(event):
            self.settings["manual_tts_message"] = self.manual_text_box.get("1.0", "end-1c")
            self.save_settings()
        self.manual_text_box.bind("<KeyRelease>", save_manual_msg)

        self.manual_btn_row = ctk.CTkFrame(manual_tts_frame, fg_color="transparent")
        self.manual_btn_row.pack(fill="x")

        self.manual_submit_btn = ctk.CTkButton(
            self.manual_btn_row, text="Submit TTS", command=self.submit_manual_tts,
            fg_color="#005E73" if getattr(self, "model_ready", False) else "#2B2D31",
            hover_color="#00D1FF" if getattr(self, "model_ready", False) else "#2B2D31",
            text_color="#FFFFFF" if getattr(self, "model_ready", False) else "#6C7079",
            state="normal" if getattr(self, "model_ready", False) else "disabled"
        )
        self.manual_submit_btn.pack(side="left", fill="x", expand=True, padx=(0, 4))

        self.manual_save_btn = ctk.CTkButton(
            self.manual_btn_row, text="Save TTS", command=self.save_manual_tts,
            fg_color="#005E73" if getattr(self, "model_ready", False) else "#2B2D31",
            hover_color="#00D1FF" if getattr(self, "model_ready", False) else "#2B2D31",
            text_color="#FFFFFF" if getattr(self, "model_ready", False) else "#6C7079",
            state="normal" if getattr(self, "model_ready", False) else "disabled"
        )
        self.manual_save_btn.pack(side="right", fill="x", expand=True, padx=(4, 0))
        # --------------------------

        chat_header_frame = ctk.CTkFrame(self.left_panel, fg_color="transparent")
        chat_header_frame.pack(fill="x", padx=15, pady=(10, 0))
        ctk.CTkLabel(chat_header_frame, text="LIVE CHAT", font=("Arial", 12, "bold"), text_color="#8E9299").pack(side="left")
        self.active_chatters_label = ctk.CTkLabel(chat_header_frame, text="0", font=("Arial", 14, "bold"), text_color="#A970FF", cursor="hand2")
        self.active_chatters_label.pack(side="left", padx=(5, 0))
        self.active_chatters_label.bind("<Button-1>", lambda e: self.open_active_chatters_window())
        if hasattr(self.active_chatters_label, "_label"):
            self.active_chatters_label._label.bind("<Button-1>", lambda e: self.open_active_chatters_window())
        self.active_chatters_label.bind("<Enter>", lambda e: self.active_chatters_label.configure(text_color="#C896FF"))
        self.active_chatters_label.bind("<Leave>", lambda e: self.active_chatters_label.configure(text_color="#A970FF"))
        ctk.CTkButton(chat_header_frame, text="Timed Out", width=76, height=24, fg_color="#1e1e24", hover_color="#2b2b36", command=self.open_timeouts_window).pack(side="right")
        ctk.CTkButton(chat_header_frame, text="Skip TTS", width=72, height=24, fg_color="#730000", hover_color="#A90000", command=lambda: self.tts.skip_current() if self.tts else None).pack(side="right", padx=(0, 4))
        ctk.CTkButton(chat_header_frame, text="Skip All TTS", width=92, height=24, fg_color="#730000", hover_color="#A90000", command=lambda: self.tts.skip_all() if self.tts else None).pack(side="right", padx=(0, 4))
        
        self.chat_box = ctk.CTkTextbox(self.left_panel, fg_color="#0A0A0E", text_color="#E1E1E6", font=("Courier", 12), wrap="word")
        self.chat_box.pack(pady=5, padx=15, fill="both", expand=True)

        # Set up color tags for the chat box
        self.chat_box.tag_config("user", foreground="#A970FF")
        self.chat_box.tag_config("command", foreground="#00D1FF")
        self.chat_box.tag_config("system", foreground="#888888")
        self.chat_box.tag_config("dono_lime", foreground="#53FC18")
        self.chat_box.tag_config("raid_blue", foreground="#00D1FF")
        self._make_textbox_readonly(self.chat_box)

        try:
            if hasattr(self.chat_box, '_vscrollbar'):
                scroll_widget = self.chat_box._vscrollbar
                if hasattr(scroll_widget, '_canvas'):
                    scroll_widget = scroll_widget._canvas
                scroll_widget.bind("<ButtonPress-1>", self.on_chat_scrollbar_press, add="+")
                scroll_widget.bind("<ButtonRelease-1>", self.on_chat_scrollbar_release, add="+")
                scroll_widget.bind("<B1-Motion>", self.on_chat_scrollbar_press, add="+")
            elif hasattr(self.chat_box, '_scrollbar'):
                scroll_widget = self.chat_box._scrollbar
                if hasattr(scroll_widget, '_canvas'):
                    scroll_widget = scroll_widget._canvas
                scroll_widget.bind("<ButtonPress-1>", self.on_chat_scrollbar_press, add="+")
                scroll_widget.bind("<ButtonRelease-1>", self.on_chat_scrollbar_release, add="+")
                scroll_widget.bind("<B1-Motion>", self.on_chat_scrollbar_press, add="+")
        except Exception:
            pass

        if hasattr(self.chat_box, '_textbox'):
            self.chat_box._textbox.bind("<MouseWheel>", self.on_chat_mouse_wheel, add="+")
            self.chat_box._textbox.bind("<ButtonPress-1>", self.on_chat_scrollbar_press, add="+")
            self.chat_box._textbox.bind("<ButtonRelease-1>", self.on_chat_scrollbar_release, add="+")
            self.chat_box._textbox.bind("<B1-Motion>", self.on_chat_scrollbar_press, add="+")

        self.chat_box.bind("<MouseWheel>", self.on_chat_mouse_wheel, add="+")
        self.chat_box.bind("<ButtonPress-1>", self.on_chat_scrollbar_press, add="+")
        self.chat_box.bind("<ButtonRelease-1>", self.on_chat_scrollbar_release, add="+")

        # RIGHT PANEL (Roleplay Features - Column 1)
        self.rp_panel = ctk.CTkFrame(self, fg_color="#121216", border_width=1, border_color="#1e1e24")
        self.rp_panel.grid(row=0, column=1, padx=(5, 10), pady=10, sticky="nsew")

        rp_header_frame = ctk.CTkFrame(self.rp_panel, fg_color="transparent")
        rp_header_frame.pack(fill="x", padx=15, pady=(10, 5))
        
        ctk.CTkLabel(rp_header_frame, text="ROLEPLAY", font=("Arial", 12, "bold"), text_color="#8E9299").pack(side="left")
        
        is_rp_on = bool(self.settings.get("roleplay_enabled", False))
        self.rp_toggle_var = ctk.BooleanVar(value=is_rp_on)
        self.rp_switch = ctk.CTkSwitch(
            rp_header_frame, 
            text="Enabled" if is_rp_on else "Disabled", 
            variable=self.rp_toggle_var, 
            onvalue=True, 
            offvalue=False
        )
        self.rp_switch.pack(side="right")

        def toggle_rp(*args):
            state = self.rp_toggle_var.get()
            self.settings["roleplay_enabled"] = state
            if hasattr(self, "rp_switch") and self.rp_switch:
                self.rp_switch.configure(text="Enabled" if state else "Disabled")
            self.save_settings()
        self.rp_toggle_var.trace_add("write", toggle_rp)
        
        rp_btn_frame = ctk.CTkFrame(self.rp_panel, fg_color="transparent")
        rp_btn_frame.pack(fill="x", padx=15, pady=(0, 10))
        
        ctk.CTkButton(rp_btn_frame, text="LLM Settings", height=24, fg_color="#3A3A40", hover_color="#55555C", font=("Arial", 11, "bold"), command=self.open_llm_settings).pack(fill="x", pady=2)
        ctk.CTkButton(rp_btn_frame, text="Roleplay Rules", height=24, fg_color="#3A3A40", hover_color="#55555C", font=("Arial", 11, "bold"), command=self.open_roleplay_rules).pack(fill="x", pady=2)
        ctk.CTkButton(rp_btn_frame, text="Universe Lore", height=24, fg_color="#3A3A40", hover_color="#55555C", font=("Arial", 11, "bold"), command=self.open_universe_lore).pack(fill="x", pady=2)
        ctk.CTkButton(rp_btn_frame, text="Character Lore", height=24, fg_color="#3A3A40", hover_color="#55555C", font=("Arial", 11, "bold"), command=self.open_character_lore).pack(fill="x", pady=2)

        # Manual Roleplay Section
        manual_rp_frame = ctk.CTkFrame(self.rp_panel, fg_color="transparent")
        manual_rp_frame.pack(fill="x", padx=15, pady=(5, 5))
        
        manual_rp_header_row = ctk.CTkFrame(manual_rp_frame, fg_color="transparent")
        manual_rp_header_row.pack(fill="x", pady=(0, 5))

        ctk.CTkLabel(manual_rp_header_row, text="MANUAL ROLEPLAY", font=("Arial", 12, "bold"), text_color="#8E9299").pack(side="left")

        save_rp_right_frame = ctk.CTkFrame(manual_rp_header_row, fg_color="transparent")
        save_rp_right_frame.pack(side="right")

        save_rp_label = ctk.CTkLabel(save_rp_right_frame, text="Save Roleplay TTS:", font=("Arial", 12), cursor="hand2")
        save_rp_label.pack(side="left", padx=(0, 6))

        self.save_roleplay_tts_var = ctk.BooleanVar(value=False)

        def on_save_roleplay_tts_toggle():
            val = bool(self.save_roleplay_tts_var.get())
            self.settings["save_roleplay_tts"] = val
            if hasattr(self, '_save_roleplay_tts_timer') and self._save_roleplay_tts_timer:
                try:
                    self.after_cancel(self._save_roleplay_tts_timer)
                except Exception:
                    pass
                self._save_roleplay_tts_timer = None

            if val:
                self.append_system_log("\n[Safety] 'Save Roleplay TTS' enabled. It will automatically disable after 1 hour.")
                self._save_roleplay_tts_timer = self.after(3600 * 1000, self._auto_disable_save_roleplay_tts)

        self.save_roleplay_tts_checkbox = ctk.CTkCheckBox(
            save_rp_right_frame,
            text="",
            variable=self.save_roleplay_tts_var,
            command=on_save_roleplay_tts_toggle,
            width=20,
            checkbox_width=18,
            checkbox_height=18,
            fg_color="#00D1FF",
            hover_color="#005E73"
        )
        self.save_roleplay_tts_checkbox.pack(side="left")

        def toggle_save_roleplay_tts(event=None):
            self.save_roleplay_tts_var.set(not self.save_roleplay_tts_var.get())
            on_save_roleplay_tts_toggle()

        save_rp_label.bind("<Button-1>", toggle_save_roleplay_tts)
        
        rp_user_row = ctk.CTkFrame(manual_rp_frame, fg_color="transparent")
        rp_user_row.pack(fill="x", pady=(0, 5))
        ctk.CTkLabel(rp_user_row, text="Sender's Name:").pack(side="left", padx=(0, 10))

        self.manual_rp_user_var = ctk.StringVar(value=self.settings.get("manual_rp_user", "User"))
        self.manual_rp_user_var.trace_add("write", lambda *args: self.settings.update({"manual_rp_user": self.manual_rp_user_var.get()}) or self.save_settings())
        manual_rp_user_entry = ctk.CTkEntry(rp_user_row, textvariable=self.manual_rp_user_var, placeholder_text="Username", height=30)
        manual_rp_user_entry.pack(side="left", fill="x", expand=True)
        
        ctk.CTkLabel(manual_rp_frame, text="Roleplay Message:").pack(anchor="w", pady=(2, 0))
        self.manual_rp_text_box = ctk.CTkTextbox(manual_rp_frame, height=80, font=("Arial", 12), fg_color="#0A0A0E", text_color="#E1E1E6", wrap="word")
        self.manual_rp_text_box.insert("1.0", self.settings.get("manual_rp_message", "?voice command hello"))
        self.manual_rp_text_box.pack(fill="x", pady=(0, 5))
        self.manual_rp_text_box.bind("<KeyRelease>", lambda e: self.settings.update({"manual_rp_message": self.manual_rp_text_box.get("1.0", "end-1c")}) or self.save_settings())
        
        self.manual_rp_submit_btn = ctk.CTkButton(
            manual_rp_frame, text="Submit Roleplay", command=self.submit_manual_rp,
            fg_color="#005E73" if getattr(self, "model_ready", False) else "#2B2D31",
            hover_color="#00D1FF" if getattr(self, "model_ready", False) else "#2B2D31",
            text_color="#FFFFFF" if getattr(self, "model_ready", False) else "#6C7079",
            state="normal" if getattr(self, "model_ready", False) else "disabled"
        )
        self.manual_rp_submit_btn.pack(fill="x")

        # Roleplay Chats Feed / TTS Queue
        self.rp_tab_frame = ctk.CTkFrame(self.rp_panel, fg_color="transparent")
        self.rp_tab_frame.pack(fill="x", padx=15, pady=(10, 0))
        
        def switch_to_rp():
            self.tts_queue_frame.pack_forget()
            self.rp_chat_box.pack(pady=5, padx=15, fill="both", expand=True)
            btn_rp.configure(text_color="#FFFFFF")
            btn_tts.configure(text_color="#8E9299")
            self.rp_chat_box.see("end")

        def switch_to_tts():
            self.rp_chat_box.pack_forget()
            self.tts_queue_frame.pack(pady=5, padx=15, fill="both", expand=True)
            btn_rp.configure(text_color="#8E9299")
            btn_tts.configure(text_color="#FFFFFF")
            self._last_queue_signature = None
            self.update_tts_queue_ui()

        btn_tts = ctk.CTkButton(self.rp_tab_frame, text="TTS QUEUE", font=("Arial", 12, "bold"), text_color="#FFFFFF", fg_color="transparent", hover_color="#2A2A2E", width=0, command=switch_to_tts)
        btn_tts.pack(side="left")
        
        ctk.CTkLabel(self.rp_tab_frame, text=" / ", font=("Arial", 12, "bold"), text_color="#8E9299").pack(side="left")
        
        btn_rp = ctk.CTkButton(self.rp_tab_frame, text="ROLEPLAY CHATS FEED", font=("Arial", 12, "bold"), text_color="#8E9299", fg_color="transparent", hover_color="#2A2A2E", width=0, command=switch_to_rp)
        btn_rp.pack(side="left")
        
        # Frames
        self.rp_chat_box = ctk.CTkTextbox(self.rp_panel, fg_color="#0A0A0E", text_color="#E1E1E6", font=("Courier", 12), wrap="word")
        # Do not pack rp_chat_box initially
        
        self.tts_queue_frame = ctk.CTkFrame(self.rp_panel, fg_color="#0A0A0E")
        self.tts_queue_frame.pack(pady=5, padx=15, fill="both", expand=True)
        # We will populate tts_queue_frame dynamically in a refresh loop
        self.tts_queue_scrollable = ctk.CTkScrollableFrame(self.tts_queue_frame, fg_color="transparent")
        self.tts_queue_scrollable.pack(fill="both", expand=True)
        
        import threading
        def _update_tts_queue_loop():
            # Throttle UI refresh if window is actively being moved or hidden
            try:
                self.update_tts_queue_ui()
            except Exception:
                pass
            self.after(1000, _update_tts_queue_loop)
        self.after(1000, _update_tts_queue_loop)
        self.rp_chat_box.tag_config("user", foreground="#A970FF")
        self.rp_chat_box.tag_config("command", foreground="#00D1FF")
        self.rp_chat_box.tag_config("system", foreground="#888888")
        self._make_textbox_readonly(self.rp_chat_box)
        
        # Right-click menu for Roleplay Chat to play audio
        self.rp_context_menu = ctk.CTkMenu(self.rp_panel, tearoff=False) if hasattr(ctk, 'CTkMenu') else None
        if not self.rp_context_menu:
            import tkinter as tk
            self.rp_context_menu = tk.Menu(self.rp_panel, tearoff=False)
            self.rp_context_menu.add_command(label="Replay Audio", command=self.play_rp_audio_context)
            self.rp_context_menu.add_command(label="Regenerate Audio", command=self.regenerate_rp_audio_context)
            
        def show_rp_context(event):
            self.rp_chat_box.focus()
            # Find which line was clicked to get associated group_id
            index = self.rp_chat_box.index(f"@{event.x},{event.y}")
            line_num = index.split(".")[0]
            self.last_clicked_rp_line = int(line_num)
            self.rp_context_menu.tk_popup(event.x_root, event.y_root)

        self.rp_chat_box.bind("<Button-3>", show_rp_context)

    def play_rp_audio_context(self):
        # We will implement logic to find the audio files based on the line clicked
        import roleplay
        if hasattr(self, 'roleplay_manager'):
            self.roleplay_manager.play_audio_for_line(self.last_clicked_rp_line)

    def regenerate_rp_audio_context(self):
        if hasattr(self, 'roleplay_manager'):
            self.roleplay_manager.regenerate_audio_for_line(self.last_clicked_rp_line)

    def open_llm_settings(self):
        if hasattr(self, 'llm_settings_window') and self.llm_settings_window and self.llm_settings_window.winfo_exists():
            self.llm_settings_window.focus()
            return
            
        self.llm_settings_window = ctk.CTkToplevel(self)
        self.center_toplevel(self.llm_settings_window, 400, 520)
        self.llm_settings_window.title("LLM Settings")
        self.apply_dark_title_bar(self.llm_settings_window)
        
        frame = ctk.CTkFrame(self.llm_settings_window, fg_color="transparent")
        frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        ctk.CTkLabel(frame, text="DeepSeek API Key:", anchor="w").pack(fill="x")
        api_var = ctk.StringVar(value=self.settings.get("deepseek_api_key", ""))
        api_entry = ctk.CTkEntry(frame, textvariable=api_var, show="*")
        api_entry.pack(fill="x", pady=(0, 15))

        ctk.CTkLabel(frame, text="DeepSeek Model Name:", anchor="w").pack(fill="x")
        model_var = ctk.StringVar(value=self.settings.get("deepseek_model", "deepseek-flash") or "deepseek-flash")
        model_entry = ctk.CTkEntry(frame, textvariable=model_var)
        model_entry.pack(fill="x", pady=(0, 15))
        
        ctk.CTkLabel(frame, text="Max Words per Response:", anchor="w").pack(fill="x")
        max_words_var = ctk.StringVar(value=str(self.settings.get("rp_max_words", 60)))
        ctk.CTkEntry(frame, textvariable=max_words_var).pack(fill="x", pady=(0, 15))
        
        ctk.CTkLabel(frame, text="Iterations in 2+ person roleplays", anchor="w").pack(fill="x")
        iter_var = ctk.StringVar(value=str(self.settings.get("rp_iterations", 1)))
        ctk.CTkEntry(frame, textvariable=iter_var).pack(fill="x", pady=(0, 15))
        
        ctk.CTkLabel(frame, text="Max number of characters per roleplay:", anchor="w").pack(fill="x")
        max_chars_var = ctk.StringVar(value=str(self.settings.get("rp_max_characters", 2)))
        ctk.CTkEntry(frame, textvariable=max_chars_var).pack(fill="x", pady=(0, 15))
        
        def auto_save(*args):
            self.settings["deepseek_api_key"] = api_var.get().strip()
            self.settings["deepseek_model"] = model_var.get().strip() or "deepseek-flash"
            try:
                self.settings["rp_max_words"] = int(max_words_var.get().strip())
            except ValueError:
                pass
            try:
                self.settings["rp_iterations"] = int(iter_var.get().strip())
            except ValueError:
                pass
            try:
                self.settings["rp_max_characters"] = int(max_chars_var.get().strip())
            except ValueError:
                pass
            self.save_settings()
            
        api_var.trace_add("write", auto_save)
        model_var.trace_add("write", auto_save)
        max_words_var.trace_add("write", auto_save)
        iter_var.trace_add("write", auto_save)
        max_chars_var.trace_add("write", auto_save)

        balance_frame = ctk.CTkFrame(frame, fg_color="transparent")
        balance_frame.pack(fill="x", pady=(10, 0))
        
        init_balance_text = getattr(self, "deepseek_balance_text", "Unknown")
        if "$" in init_balance_text:
            balance_label = ctk.CTkLabel(balance_frame, text=init_balance_text, text_color="#2ECC71", font=("Arial", 18, "bold"), anchor="center")
        else:
            balance_label = ctk.CTkLabel(balance_frame, text=init_balance_text, text_color="#E1E1E6", font=("Arial", 12), anchor="center")
        balance_label.pack(fill="x", pady=(0, 5))
        
        check_btn = ctk.CTkButton(balance_frame, text="Check Balance", command=lambda: self.check_deepseek_balance(update_label=balance_label))
        check_btn.pack(fill="x")

    def open_roleplay_rules(self):
        if hasattr(self, 'roleplay_rules_window') and self.roleplay_rules_window and self.roleplay_rules_window.winfo_exists():
            self.roleplay_rules_window.focus()
            return
            
        self.roleplay_rules_window = ctk.CTkToplevel(self)
        self.center_toplevel(self.roleplay_rules_window, 500, 400)
        self.roleplay_rules_window.title("Roleplay Rules")
        self.apply_dark_title_bar(self.roleplay_rules_window)
        
        frame = ctk.CTkFrame(self.roleplay_rules_window, fg_color="transparent")
        frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        ctk.CTkLabel(frame, text="Global Roleplay Rules:", anchor="w").pack(fill="x")
        
        rules_box = ctk.CTkTextbox(frame, height=250, wrap="word")
        if hasattr(self, 'roleplay_manager'):
            rules_box.insert("1.0", self.roleplay_manager.global_rules)
        rules_box.pack(fill="both", expand=True, pady=(0, 10))
        
        count_lbl = ctk.CTkLabel(frame, text="0 words")
        count_lbl.pack(anchor="e")
        
        def update_count(*args):
            text = rules_box.get("1.0", "end-1c")
            count_lbl.configure(text=f"{len(text.split())} words")
            if hasattr(self, 'roleplay_manager'):
                self.roleplay_manager.global_rules = text
                self.roleplay_manager.save_json(self.roleplay_manager.global_rules_path, text)
                
        rules_box.bind("<KeyRelease>", update_count)
        update_count()

    def open_universe_lore(self):
        if hasattr(self, 'universe_lore_window') and self.universe_lore_window and self.universe_lore_window.winfo_exists():
            self.universe_lore_window.focus()
            return
            
        self.universe_lore_window = ctk.CTkToplevel(self)
        self.center_toplevel(self.universe_lore_window, 500, 400)
        self.universe_lore_window.title("Universe Lore")
        self.apply_dark_title_bar(self.universe_lore_window)
        
        frame = ctk.CTkFrame(self.universe_lore_window, fg_color="transparent")
        frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        ctk.CTkLabel(frame, text="Universe Lore:", anchor="w").pack(fill="x")
        
        lore_box = ctk.CTkTextbox(frame, height=250, wrap="word")
        if hasattr(self, 'roleplay_manager'):
            lore_box.insert("1.0", self.roleplay_manager.universe_lore)
        lore_box.pack(fill="both", expand=True, pady=(0, 10))
        
        count_lbl = ctk.CTkLabel(frame, text="0 words")
        count_lbl.pack(anchor="e")
        
        def update_count(*args):
            text = lore_box.get("1.0", "end-1c")
            count_lbl.configure(text=f"{len(text.split())} words")
            if hasattr(self, 'roleplay_manager'):
                self.roleplay_manager.universe_lore = text
                self.roleplay_manager.save_json(self.roleplay_manager.universe_lore_path, text)
                
        lore_box.bind("<KeyRelease>", update_count)
        update_count()

    def open_character_lore(self, preselect_cmd=None):
        if preselect_cmd:
            self.last_selected_lore_command = preselect_cmd
            if hasattr(self, 'character_lore_window') and self.character_lore_window and self.character_lore_window.winfo_exists():
                self.character_lore_window.destroy()
                
        if hasattr(self, 'character_lore_window') and self.character_lore_window and self.character_lore_window.winfo_exists():
            self.character_lore_window.focus()
            return
            
        self.character_lore_window = ctk.CTkToplevel(self)
        self.center_toplevel(self.character_lore_window, 500, 630)
        self.character_lore_window.title("Character Lore")
        self.apply_dark_title_bar(self.character_lore_window)

        frame = ctk.CTkFrame(self.character_lore_window, fg_color="transparent")
        frame.pack(fill="both", expand=True, padx=20, pady=20)

        if preselect_cmd and hasattr(self, 'roleplay_manager'):
            if preselect_cmd not in self.roleplay_manager.character_lore:
                self.roleplay_manager.character_lore[preselect_cmd] = {"backstory": "", "rules": ""}
                self.roleplay_manager.save_json(self.roleplay_manager.character_lore_path, self.roleplay_manager.character_lore)

        if hasattr(self, 'roleplay_manager'):
            all_valid_cmds = [v["command"] for v in self.voices]
            voices = [cmd for cmd in all_valid_cmds if cmd in self.roleplay_manager.character_lore]
        else:
            voices = []

        initial_val = voices[0] if voices else ""
        if hasattr(self, 'last_selected_lore_command') and self.last_selected_lore_command in voices:
            initial_val = self.last_selected_lore_command

        voice_var = ctk.StringVar(value=initial_val)

        # "Search voice:" row above Select Voice Command
        search_row = ctk.CTkFrame(frame, fg_color="transparent")
        search_row.pack(fill="x", pady=(0, 10))

        ctk.CTkLabel(
            search_row,
            text="Search voice:",
            font=("Arial", 11, "bold"),
            text_color="#8E9299"
        ).pack(side="left", padx=(0, 8))

        lore_search_var = ctk.StringVar()
        lore_search_entry = ctk.CTkEntry(
            search_row,
            textvariable=lore_search_var,
            placeholder_text="!voice command or name...",
            height=28,
            font=("Arial", 11)
        )
        lore_search_entry.pack(side="left", fill="x", expand=True)

        ctk.CTkLabel(frame, text="Select Voice Command:", anchor="w").pack(fill="x")

        selector_container = ctk.CTkFrame(frame, fg_color="transparent")
        selector_container.pack(fill="x", pady=(0, 15))

        selector_top = ctk.CTkFrame(selector_container, fg_color="transparent")
        selector_top.pack(fill="x")

        scroll_frame = ctk.CTkScrollableFrame(selector_container, height=120)

        is_dropdown_open = [False]
        def toggle_dropdown():
            if is_dropdown_open[0]:
                scroll_frame.pack_forget()
                is_dropdown_open[0] = False
            else:
                scroll_frame.pack(fill="x", pady=(5, 0))
                is_dropdown_open[0] = True

        toggle_btn = ctk.CTkButton(selector_top, text=voice_var.get(), command=toggle_dropdown, fg_color="#3A3A40", hover_color="#55555C")
        toggle_btn.pack(fill="x")

        def on_rb_select():
            scroll_frame.pack_forget()
            is_dropdown_open[0] = False

        def rebuild_radio_buttons():
            for child in scroll_frame.winfo_children():
                child.destroy()
            nonlocal voices
            if hasattr(self, 'roleplay_manager'):
                all_valid_cmds = [v["command"] for v in self.voices]
                voices = [cmd for cmd in all_valid_cmds if cmd in self.roleplay_manager.character_lore]
            else:
                voices = []
            columns = 3
            for i, v_cmd in enumerate(voices):
                rb = ctk.CTkRadioButton(scroll_frame, text=v_cmd, variable=voice_var, value=v_cmd, command=on_rb_select)
                rb.grid(row=i // columns, column=i % columns, padx=10, pady=5, sticky="w")

        rebuild_radio_buttons()

        # Dropdown logic for lore search
        lore_dropdown = [None]
        lore_matches = []
        suppress_lore_dropdown = [False]

        def hide_lore_dropdown(event=None):
            if lore_dropdown[0]:
                try:
                    lore_dropdown[0].destroy()
                except Exception:
                    pass
                lore_dropdown[0] = None

        def on_lore_focus_out(event=None):
            if hasattr(self, 'character_lore_window') and self.character_lore_window and self.character_lore_window.winfo_exists():
                self.character_lore_window.after(250, hide_lore_dropdown)

        def select_lore_voice(voice_item):
            cmd = voice_item.get("command", "")
            if not cmd:
                return
            suppress_lore_dropdown[0] = True
            hide_lore_dropdown()
            lore_search_var.set("")

            # If voice has no lore history yet, create lore entry now
            if hasattr(self, 'roleplay_manager'):
                if cmd not in self.roleplay_manager.character_lore:
                    self.roleplay_manager.character_lore[cmd] = {"backstory": "", "rules": ""}
                    self.roleplay_manager.save_json(self.roleplay_manager.character_lore_path, self.roleplay_manager.character_lore)

            rebuild_radio_buttons()
            voice_var.set(cmd)
            toggle_btn.configure(text=cmd)
            self.last_selected_lore_command = cmd

        def show_lore_dropdown(matches):
            if not lore_search_entry or not lore_search_entry.winfo_exists():
                return
            lore_search_entry.update_idletasks()
            ex = lore_search_entry.winfo_rootx()
            ey = lore_search_entry.winfo_rooty() + lore_search_entry.winfo_height() + 2
            ew = max(lore_search_entry.winfo_width(), 260)

            import tkinter as tk
            if not lore_dropdown[0] or not lore_dropdown[0].winfo_exists():
                top_dd = tk.Toplevel(self.character_lore_window)
                top_dd.overrideredirect(True)
                top_dd.attributes("-topmost", True)
                lore_dropdown[0] = top_dd
            else:
                top_dd = lore_dropdown[0]

            for w in top_dd.winfo_children():
                w.destroy()

            item_count = min(len(matches), 7)
            list_h = item_count * 24 + 4
            top_dd.geometry(f"{ew}x{list_h}+{ex}+{ey}")

            list_frame = tk.Frame(top_dd, bg="#18181E", bd=1, relief="solid")
            list_frame.pack(fill="both", expand=True)

            listbox = tk.Listbox(
                list_frame,
                bg="#18181E",
                fg="#E1E1E6",
                selectbackground="#005E73",
                selectforeground="#FFFFFF",
                activestyle="none",
                bd=0,
                highlightthickness=0,
                font=("Arial", 10),
                exportselection=False
            )
            if len(matches) > 7:
                sb = tk.Scrollbar(list_frame, orient="vertical", command=listbox.yview)
                sb.pack(side="right", fill="y")
                listbox.configure(yscrollcommand=sb.set)
            listbox.pack(side="left", fill="both", expand=True)

            existing_lore_cmds = set(self.roleplay_manager.character_lore.keys()) if hasattr(self, 'roleplay_manager') else set()

            for v in matches:
                v_cmd = v.get("command", "")
                v_name = v.get("name", "")
                tag = "" if v_cmd in existing_lore_cmds else " [New Lore]"
                listbox.insert("end", f" {v_cmd}  —  {v_name}{tag}")

            listbox.selection_set(0)
            listbox.activate(0)

            def on_click(event):
                idx = listbox.nearest(event.y)
                if 0 <= idx < len(lore_matches):
                    select_lore_voice(lore_matches[idx])

            listbox.bind("<ButtonRelease-1>", on_click)

        def on_lore_search_down(event=None):
            if lore_dropdown[0] and lore_dropdown[0].winfo_exists():
                import tkinter as tk
                for c in lore_dropdown[0].winfo_children():
                    for lb in c.winfo_children():
                        if isinstance(lb, tk.Listbox):
                            curr = lb.curselection()
                            idx = (curr[0] + 1) if curr else 0
                            if idx < lb.size():
                                lb.selection_clear(0, "end")
                                lb.selection_set(idx)
                                lb.activate(idx)
                                lb.see(idx)
                            return "break"

        def on_lore_search_up(event=None):
            if lore_dropdown[0] and lore_dropdown[0].winfo_exists():
                import tkinter as tk
                for c in lore_dropdown[0].winfo_children():
                    for lb in c.winfo_children():
                        if isinstance(lb, tk.Listbox):
                            curr = lb.curselection()
                            idx = (curr[0] - 1) if curr else 0
                            if idx >= 0:
                                lb.selection_clear(0, "end")
                                lb.selection_set(idx)
                                lb.activate(idx)
                                lb.see(idx)
                            return "break"

        def on_lore_search_enter(event=None):
            if lore_dropdown[0] and lore_dropdown[0].winfo_exists():
                import tkinter as tk
                for c in lore_dropdown[0].winfo_children():
                    for lb in c.winfo_children():
                        if isinstance(lb, tk.Listbox):
                            sel = lb.curselection()
                            idx = sel[0] if sel else 0
                            if 0 <= idx < len(lore_matches):
                                select_lore_voice(lore_matches[idx])
                                return "break"

        def on_lore_search_changed(*args):
            if suppress_lore_dropdown[0]:
                suppress_lore_dropdown[0] = False
                return
            query = lore_search_var.get().strip()
            if not query:
                hide_lore_dropdown()
                return

            clean_q = query.lower().lstrip("!")
            q_lower = query.lower()

            nonlocal lore_matches
            lore_matches = []
            for v in getattr(self, "voices", []):
                cmd = v.get("command", "").lower()
                name = v.get("name", "").lower()
                if (q_lower in cmd) or (q_lower in name) or (clean_q and clean_q in cmd.lstrip("!")) or (clean_q and clean_q in name):
                    lore_matches.append(v)

            if not lore_matches:
                hide_lore_dropdown()
                return

            show_lore_dropdown(lore_matches)

        lore_search_var.trace_add("write", on_lore_search_changed)
        lore_search_entry.bind("<Down>", on_lore_search_down)
        lore_search_entry.bind("<Up>", on_lore_search_up)
        lore_search_entry.bind("<Return>", on_lore_search_enter)
        lore_search_entry.bind("<Escape>", hide_lore_dropdown)
        lore_search_entry.bind("<FocusOut>", on_lore_focus_out)

        def on_lore_window_close():
            hide_lore_dropdown()
            self.character_lore_window.destroy()

        self.character_lore_window.protocol("WM_DELETE_WINDOW", on_lore_window_close)
        
        def remove_lore():
            cmd = voice_var.get()
            if not cmd or not hasattr(self, 'roleplay_manager'): return
            
            confirm_win = ctk.CTkToplevel(self.character_lore_window)
            self.center_toplevel(confirm_win, 350, 150, parent=self.character_lore_window)
            confirm_win.title("Confirm Deletion")
            self.apply_dark_title_bar(confirm_win)
            confirm_win.grab_set()
            confirm_win.attributes("-topmost", True)
            
            ctk.CTkLabel(confirm_win, text=f"Are you sure you want to delete lore for {cmd}?").pack(pady=20)
            
            btn_frame = ctk.CTkFrame(confirm_win, fg_color="transparent")
            btn_frame.pack(fill="x", padx=20, pady=10)
            
            def do_delete():
                if cmd in self.roleplay_manager.character_lore:
                    del self.roleplay_manager.character_lore[cmd]
                    self.roleplay_manager.save_json(self.roleplay_manager.character_lore_path, self.roleplay_manager.character_lore)
                
                # Refresh window
                current = voice_var.get()
                if hasattr(self, 'last_selected_lore_command') and self.last_selected_lore_command == current:
                    self.last_selected_lore_command = ""
                
                confirm_win.destroy()
                if hasattr(self, 'character_lore_window') and self.character_lore_window and self.character_lore_window.winfo_exists():
                    self.character_lore_window.destroy()
                self.after(10, self.open_character_lore)
                
            ctk.CTkButton(btn_frame, text="Cancel", width=100, fg_color="#3A3A40", hover_color="#55555C", command=confirm_win.destroy).pack(side="left", padx=10)
            ctk.CTkButton(btn_frame, text="Yes, Delete", width=100, fg_color="#E03A3E", hover_color="#B51A21", command=do_delete).pack(side="right", padx=10)
        
        def on_voice_change(*args):
            self.last_selected_lore_command = voice_var.get()
            toggle_btn.configure(text=voice_var.get())
            
        voice_var.trace_add("write", on_voice_change)
        
        ctk.CTkLabel(frame, text="Backstory Lore:", anchor="w").pack(fill="x")
        backstory_box = ctk.CTkTextbox(frame, height=150, wrap="word")
        backstory_box.pack(fill="both", expand=True, pady=(0, 5))
        
        ctk.CTkLabel(frame, text="Roleplay Rules:", anchor="w").pack(fill="x")
        rules_box = ctk.CTkTextbox(frame, height=150, wrap="word")
        rules_box.pack(fill="both", expand=True, pady=(0, 10))
        
        backstory_box.tag_config("valid_ref", foreground="#A970FF")
        backstory_box.tag_config("invalid_ref", foreground="#FF4444")
        rules_box.tag_config("valid_ref", foreground="#A970FF")
        rules_box.tag_config("invalid_ref", foreground="#FF4444")
        
        bottom_frame = ctk.CTkFrame(frame, fg_color="transparent")
        bottom_frame.pack(fill="x")
        
        remove_btn = ctk.CTkButton(bottom_frame, text="Delete Lore", width=100, fg_color="#E03A3E", hover_color="#B51A21", command=remove_lore)
        remove_btn.pack(side="left")

        count_lbl = ctk.CTkLabel(bottom_frame, text="0 words")
        count_lbl.pack(side="right")
        
        def highlight_box(box):
            box.tag_remove("valid_ref", "1.0", "end")
            box.tag_remove("invalid_ref", "1.0", "end")
            text = box.get("1.0", "end-1c")
            import re
            valid_cmds = [v["command"].lower() for v in self.voices]
            for match in re.finditer(r'![a-zA-Z0-9_]+', text):
                word = match.group()[1:].lower()
                start_idx = f"1.0 + {match.start()} chars"
                end_idx = f"1.0 + {match.end()} chars"
                if f"!{word}" in valid_cmds:
                    box.tag_add("valid_ref", start_idx, end_idx)
                else:
                    box.tag_add("invalid_ref", start_idx, end_idx)
                    
        def load_current_lore(*args):
            cmd = voice_var.get()
            if hasattr(self, 'roleplay_manager') and cmd:
                char_lore_db = self.roleplay_manager.character_lore
                if not isinstance(char_lore_db, dict):
                    char_lore_db = {}
                    self.roleplay_manager.character_lore = char_lore_db
                lore = char_lore_db.get(cmd, {})
                if isinstance(lore, str):
                    lore = {"backstory": lore, "rules": ""}
                elif not isinstance(lore, dict):
                    lore = {"backstory": str(lore), "rules": ""}
                backstory_box.delete("1.0", "end")
                backstory_box.insert("1.0", lore.get("backstory", ""))
                rules_box.delete("1.0", "end")
                rules_box.insert("1.0", lore.get("rules", ""))
                update_count()
                
        def update_count(*args):
            b_text = backstory_box.get("1.0", "end-1c")
            r_text = rules_box.get("1.0", "end-1c")
            total_words = len(b_text.split()) + len(r_text.split())
            count_lbl.configure(text=f"{total_words} words")
            
            highlight_box(backstory_box)
            highlight_box(rules_box)
            
            cmd = voice_var.get()
            if hasattr(self, 'roleplay_manager') and cmd:
                self.roleplay_manager.character_lore[cmd] = {"backstory": b_text, "rules": r_text}
                self.roleplay_manager.save_json(self.roleplay_manager.character_lore_path, self.roleplay_manager.character_lore)

        voice_var.trace_add("write", load_current_lore)
        backstory_box.bind("<KeyRelease>", update_count)
        rules_box.bind("<KeyRelease>", update_count)
        load_current_lore()

    def submit_manual_rp(self):
        if not getattr(self, "model_ready", False):
            self.append_system_log("\n[System] Please wait: AI Voice Model is still loading into VRAM...")
            return
        if not hasattr(self, 'roleplay_manager'): return
        msg = self.manual_rp_text_box.get("1.0", "end-1c").strip()
        user = self.manual_rp_user_var.get().strip()
        if msg:
            self.process_roleplay_command(user, msg)

    def process_roleplay_command(self, username, text):
        import re
        commands = re.findall(r'(?<!\S)\?[a-zA-Z0-9_]+', text)
        if not commands: return
        
        for cmd in commands:
            c = "!" + cmd[1:].lower()
            v = next((v for v in self.voices if v["command"].lower() == c), None)
            if not v or not v.get("rp_active", True):
                self.append_system_log(f"\n[System] MAIN.PY: Ignored Roleplay (voice '{cmd}' disabled or missing in Voice Library)\n")
                return

        max_chars = self.settings.get("rp_max_characters", 2)
        commands = commands[:max_chars]
        
        characters = []
        for cmd in commands:
            c = "!" + cmd[1:].lower()
            name = c
            for v in self.voices:
                if v["command"].lower() == c:
                    name = v.get("name", v["command"])
                    c = v["command"]
                    break
            characters.append((c, name))
            
        if not characters: return
                    
        self.roleplay_manager.generate_roleplay(username, text, characters)

    def _on_vol_slider_press(self):
        self._is_user_sliding_vol = True

    def _on_vol_slider_release(self):
        self._is_user_sliding_vol = False
        if hasattr(self, "vol_slider"):
            self.settings["app_volume"] = int(round(self.vol_slider.get()))
        self.save_settings(immediate=True)

    def _get_vol_icon(self, level):
        if level <= 0:
            return "🔇"
        elif level < 34:
            return "🔈"
        elif level < 67:
            return "🔉"
        return "🔊"

    def ensure_windows_volume_applied(self):
        """Ensures the Windows Volume Mixer has the app volume set to the slider value upon first audio output."""
        try:
            target_vol = self.settings.get("app_volume", 100)
            if hasattr(self, "win_volume") and self.win_volume:
                applied = self.win_volume.set_volume(target_vol)
                if applied:
                    self._initial_volume_synced_to_windows = True
        except Exception:
            pass

    def on_volume_slider_change(self, value):
        val = int(round(value))
        self._last_vol_change_time = time.time()
        
        if hasattr(self, "vol_percent_label"):
            self.vol_percent_label.configure(text=f"{val}%")
        if hasattr(self, "vol_icon_label"):
            self.vol_icon_label.configure(text=self._get_vol_icon(val))
            
        # Update Windows Volume Mixer
        if hasattr(self, "win_volume") and self.win_volume:
            self.win_volume.set_volume(val)
            self._initial_volume_synced_to_windows = True
        
        self.settings["app_volume"] = val
        self.save_settings()

    def sync_windows_volume_loop(self):
        """Periodically syncs UI volume slider with external Windows Volume Mixer adjustments."""
        try:
            target_vol = self.settings.get("app_volume", 100)
            if not getattr(self, "_initial_volume_synced_to_windows", False):
                # Until the session is active and verified, keep pushing the saved slider volume
                if hasattr(self, "win_volume") and self.win_volume:
                    applied = self.win_volume.set_volume(target_vol)
                    if applied:
                        self._initial_volume_synced_to_windows = True
            else:
                # Once established, detect if the user manually adjusted the volume in Windows Volume Mixer
                now = time.time()
                if not getattr(self, "_is_user_sliding_vol", False) and (now - getattr(self, "_last_vol_change_time", 0) > 1.5):
                    if hasattr(self, "win_volume") and self.win_volume:
                        win_vol = self.win_volume.get_volume()
                        if win_vol is not None and hasattr(self, "vol_slider"):
                            curr_slider_val = int(round(self.vol_slider.get()))
                            if curr_slider_val != win_vol:
                                self.vol_slider.set(win_vol)
                                if hasattr(self, "vol_percent_label"):
                                    self.vol_percent_label.configure(text=f"{win_vol}%")
                                if hasattr(self, "vol_icon_label"):
                                    self.vol_icon_label.configure(text=self._get_vol_icon(win_vol))
                                self.settings["app_volume"] = win_vol
                                self.save_settings()
        except Exception:
            pass
        finally:
            self.after(1000, self.sync_windows_volume_loop)

    def copy_active_voices(self):
        active_cmds = [v["command"] for v in self.voices if v.get("active")]
        if not active_cmds:
            self.append_system_log("\n[System] No active voices to copy.")
            return
            
        text_to_copy = "\n".join(active_cmds)
        self.clipboard_clear()
        self.clipboard_append(text_to_copy)
        self.append_system_log(f"\n[System] Copied {len(active_cmds)} voice commands to clipboard.")

    def on_url_change(self, event=None):
        val = self.url_entry.get().strip()
        self.settings["kick_username"] = val
        self.settings["url"] = val
        self.save_settings()

    def toggle_global_mute(self):
        self.settings["global_mute"] = not self.settings.get("global_mute", False)
        self.save_settings()
        self.update_mute_button_ui()
        
        # Pass the mute state to the TTS Engine
        self.tts.is_muted = self.settings["global_mute"]
        
        # The TTS engine callback will handle outputting 0 volume when muted.
            
        state_str = "MUTED" if self.settings["global_mute"] else "UNMUTED"
        self.append_system_log(f"\n[System] App volume has been {state_str}.")

    def update_mute_button_ui(self):
        if hasattr(self, "mute_btn") and self.mute_btn:
            try:
                if self.settings.get("global_mute", False):
                    self.mute_btn.configure(text="🔇 UNMUTE ALL VOICES", fg_color="#B51A21", hover_color="#E03A3E")
                else:
                    self.mute_btn.configure(text="🔊 MUTE ALL VOICES", fg_color="#005E73", hover_color="#00D1FF")
            except Exception:
                pass

    def toggle_global_pause(self):
        self.settings["global_pause"] = not self.settings.get("global_pause", False)
        self.save_settings()
        self.update_pause_button_ui()
        
        # Pass the pause state to the TTS Engine
        self.tts.set_paused(self.settings["global_pause"])
        
        state_str = "PAUSED" if self.settings["global_pause"] else "UNPAUSED"
        self.append_system_log(f"\n[System] Audio playback has been {state_str}.")

    def update_pause_button_ui(self):
        if self.settings.get("global_pause", False):
            self.pause_btn.configure(text="▶ UNPAUSE", fg_color="#B51A21", hover_color="#E03A3E")
        else:
            self.pause_btn.configure(text="⏸ PAUSE", fg_color="#005E73", hover_color="#00D1FF")

    def confirm_delete_voice(self, command_to_delete, parent_window=None, close_parent=True):
        confirm_win = ctk.CTkToplevel(self)
        self.center_toplevel(confirm_win, 400, 150, parent=parent_window or self)
        confirm_win.title("Confirm Delete")
        self.apply_dark_title_bar(confirm_win)
        confirm_win.attributes("-topmost", True)
        
        lbl = ctk.CTkLabel(confirm_win, text=f"You are about to delete {command_to_delete}  Are you sure?", font=("Arial", 14))
        lbl.pack(pady=(30, 20), padx=20)
        
        btn_frame = ctk.CTkFrame(confirm_win, fg_color="transparent")
        btn_frame.pack(fill="x", pady=10)
        
        def on_cancel():
            confirm_win.destroy()
            
        def on_delete():
            self.delete_voice(command_to_delete)
            confirm_win.destroy()
            if close_parent and parent_window and hasattr(parent_window, 'winfo_exists') and parent_window.winfo_exists():
                parent_window.destroy()
            
        ctk.CTkButton(btn_frame, text="Cancel", width=100, command=on_cancel, fg_color="#3A3A40", hover_color="#55555C").pack(side="left", expand=True, padx=(20, 10))
        ctk.CTkButton(btn_frame, text="Delete", width=100, command=on_delete, fg_color="#B51A21", hover_color="#E03A3E").pack(side="right", expand=True, padx=(10, 20))

    def delete_voice(self, command_to_delete):
        # Remove from internal state
        self.voices = [v for v in self.voices if v['command'] != command_to_delete]
        
        # Remove from TTS engine active loop
        if command_to_delete in self.tts.active_voices:
            del self.tts.active_voices[command_to_delete]
            
        # Close open settings window if one is open for this voice
        if hasattr(self, 'open_settings_windows') and command_to_delete in self.open_settings_windows:
            w = self.open_settings_windows[command_to_delete]
            try:
                if w and w.winfo_exists():
                    w.destroy()
            except Exception:
                pass
            del self.open_settings_windows[command_to_delete]

        self.save_voices()
        self.append_system_log(f"\n[System] Deleted voice command: {command_to_delete}")
        self.refresh_voices()

    def toggle_voice_active(self, command, switch_widget=None):
        # Find the voice and update its active state
        if switch_widget is None:
            is_active = True
        elif hasattr(switch_widget, 'get'):
            is_active = switch_widget.get() == 1
        elif isinstance(switch_widget, bool):
            is_active = switch_widget
        else:
            is_active = bool(switch_widget)
        
        for v in self.voices:
            if v['command'] == command:
                v['active'] = is_active
                
                # We intentionally do NOT remove from self.tts.active_voices 
                # so that Roleplay can still use it even if disabled for regular TTS.
                if "path" in v:
                    exag = v.get("exaggeration", 0.5)
                    temp = v.get("temperature", 0.4)
                    
                    self.tts.add_voice(command, v["path"], temperature=temp)
                
                self.save_voices()
                break
                
    def toggle_voice_rp_active(self, command, switch_widget=None):
        if switch_widget is None:
            is_active = True
        elif hasattr(switch_widget, 'get'):
            is_active = switch_widget.get() == 1
        elif isinstance(switch_widget, bool):
            is_active = switch_widget
        else:
            is_active = bool(switch_widget)

        for v in self.voices:
            if v['command'] == command:
                v['rp_active'] = is_active
                self.save_voices()
                break

    def open_ad_settings(self):
        if hasattr(self, 'ad_settings_window') and self.ad_settings_window and self.ad_settings_window.winfo_exists():
            self.ad_settings_window.focus()
            return
            
        top = ctk.CTkToplevel(self)
        self.ad_settings_window = top
        top.title("AD SETTINGS")
        
        self.center_toplevel(top, 400, 500)
        self.apply_dark_title_bar(top)
        top.minsize(400, 500)
        
        top.attributes("-topmost", True)
        
        # 1. TTS ADS Switch
        tts_ads_frame = ctk.CTkFrame(top, fg_color="transparent")
        tts_ads_frame.pack(fill="x", padx=15, pady=(20, 10))
        
        tts_ads_var = ctk.IntVar(value=1 if self.settings.get("ad_tts_enabled", False) else 0)
        def toggle_tts_ads():
            self.settings["ad_tts_enabled"] = bool(tts_ads_var.get())
            self.save_settings()
            
        tts_ads_switch = ctk.CTkSwitch(tts_ads_frame, text="TTS ADS", variable=tts_ads_var, command=toggle_tts_ads)
        tts_ads_switch.pack(side="left")
        
        # 2. Voice Command Text Box
        cmd_frame = ctk.CTkFrame(top, fg_color="transparent")
        cmd_frame.pack(fill="x", padx=15, pady=(5, 10))
        
        ctk.CTkLabel(cmd_frame, text="Voice command to use (e.g. !voice1):", font=("Arial", 12)).pack(side="left")
        
        cmd_entry = ctk.CTkEntry(cmd_frame, width=120)
        cmd_entry.insert(0, self.settings.get("ad_tts_command", ""))
        cmd_entry.pack(side="right")
        
        def save_cmd(event=None):
            self.settings["ad_tts_command"] = cmd_entry.get().strip()
            self.save_settings()
            
        cmd_entry.bind("<KeyRelease>", save_cmd)
        cmd_entry.bind("<FocusOut>", save_cmd)
        cmd_entry.bind("<Return>", save_cmd)

        self.attach_voice_autocomplete(
            cmd_entry,
            on_select=lambda v: [self.settings.update({"ad_tts_command": v.get("command", "")}), self.save_settings()],
            parent=top
        )
        
        # 3. Read custom message Switch
        custom_msg_frame = ctk.CTkFrame(top, fg_color="transparent")
        custom_msg_frame.pack(fill="x", padx=15, pady=(10, 5))
        
        custom_msg_var = ctk.IntVar(value=1 if self.settings.get("ad_custom_message_enabled", False) else 0)
        def toggle_custom_msg():
            self.settings["ad_custom_message_enabled"] = bool(custom_msg_var.get())
            self.save_settings()
            
        custom_msg_switch = ctk.CTkSwitch(custom_msg_frame, text="Read custom message:", variable=custom_msg_var, command=toggle_custom_msg)
        custom_msg_switch.pack(side="left")
        
        # 4. Custom message Text Box
        msg_frame = ctk.CTkFrame(top, fg_color="transparent")
        msg_frame.pack(fill="x", padx=15, pady=(0, 15))
        
        msg_entry = ctk.CTkEntry(msg_frame, placeholder_text="Enter custom message here...")
        msg_entry.insert(0, self.settings.get("ad_custom_text", ""))
        msg_entry.pack(fill="x")
        
        def save_msg(event):
            self.settings["ad_custom_text"] = msg_entry.get().strip()
            self.save_settings()
            
        msg_entry.bind("<KeyRelease>", save_msg)
        msg_entry.bind("<FocusOut>", save_msg)
        msg_entry.bind("<Return>", save_msg)
        
        # 5. Adbot usernames
        bot_frame = ctk.CTkFrame(top, fg_color="transparent")
        bot_frame.pack(fill="both", expand=True, padx=15, pady=(10, 15))
        
        header_frame = ctk.CTkFrame(bot_frame, fg_color="transparent")
        header_frame.pack(fill="x", pady=(0, 2))
        
        ctk.CTkLabel(header_frame, text="List of Adbot usernames:", font=("Arial", 12, "bold")).pack(side="left")
        
        count_val = len([u for u in self.settings.get("adbot_usernames", []) if u.strip()])
        count_label = ctk.CTkLabel(header_frame, text=f"{count_val} users", text_color="#00D1FF", font=("Arial", 11))
        count_label.pack(side="right")
        
        ctk.CTkLabel(bot_frame, text="One username per line", font=("Arial", 10), text_color="#8E9299").pack(anchor="w", pady=(0, 5))
        
        bot_text = ctk.CTkTextbox(bot_frame, fg_color="#0A0A0E", border_color="#1e1e24", border_width=1, wrap="word")
        bot_text.pack(fill="both", expand=True)
        
        bot_list = self.settings.get("adbot_usernames", [])
        if bot_list:
            seen = set()
            dedup_list = []
            for b in bot_list:
                if b.lower() not in seen:
                    seen.add(b.lower())
                    dedup_list.append(b)
            bot_text.insert("1.0", "\n".join(dedup_list) + "\n")
            
        def on_bot_change(event=None):
            raw_text = bot_text.get("1.0", "end-1c")
            # Deduplicate while preserving order
            seen = set()
            users = []
            for line in raw_text.split("\n"):
                clean = line.strip()
                if clean and clean.lower() not in seen:
                    seen.add(clean.lower())
                    users.append(clean)
            self.settings["adbot_usernames"] = users
            self.save_settings()
            count_label.configure(text=f"{len(users)} users")
            if hasattr(self, "scraper") and self.scraper:
                self.scraper.adbot_usernames = users
            
            # Auto-format UI on FocusOut so duplicates disappear visually
            if getattr(event, 'type', None) and str(event.type) == "FocusOut":
                bot_text.delete("1.0", "end")
                bot_text.insert("1.0", "\n".join(users) + "\n")

        bot_text.bind("<KeyRelease>", on_bot_change)
        bot_text.bind("<FocusOut>", on_bot_change)

    def open_kick_donos_settings(self):
        if hasattr(self, 'kick_donos_window') and self.kick_donos_window and self.kick_donos_window.winfo_exists():
            self.kick_donos_window.focus()
            return
            
        top = ctk.CTkToplevel(self)
        self.kick_donos_window = top
        top.title("Kick Donos")
        
        self.center_toplevel(top, 600, 600)
        self.apply_dark_title_bar(top)
        top.minsize(550, 500)
        
        top.attributes("-topmost", True)
        
        # Ensure dict exists
        if "kick_donos" not in self.settings:
            self.settings["kick_donos"] = {
                "sound_enabled": False,
                "sound_file": "",
                "tts_enabled": False,
                "tts_voice": "!narrator",
                "tts_message": "Thank you [user] for the [amount] kicks! [message]",
                "min_alert_amount": 100,
                "log_min_amount": 1
            }
            
        kd_settings = self.settings["kick_donos"]

        if "tts_message" in kd_settings and kd_settings["tts_message"] == "Thank you [user] for the [amount] kicks!":
            # Upgrade existing default
            kd_settings["tts_message"] = "Thank you [user] for the [amount] kicks! [message]"

        if "min_alert_amount" not in kd_settings:
            kd_settings["min_alert_amount"] = 100
            
        if "log_min_amount" not in kd_settings:
            kd_settings["log_min_amount"] = 1

        if "alerts_enabled" not in kd_settings:
            kd_settings["alerts_enabled"] = True

        def save_kd():
            self.settings["kick_donos"] = kd_settings
            self.save_settings()
            if hasattr(self, 'scraper'):
                self.scraper.enable_dono_scanning = True

        # On/Off switch
        row0 = ctk.CTkFrame(top, fg_color="transparent")
        row0.pack(fill="x", padx=15, pady=(15, 5))
        
        alerts_enabled_var = ctk.BooleanVar(value=kd_settings.get("alerts_enabled", True))
        def update_alerts_enabled():
            kd_settings["alerts_enabled"] = alerts_enabled_var.get()
            save_kd()
            
        ctk.CTkSwitch(row0, text="Turn on Kick Dono Alerts", variable=alerts_enabled_var, command=update_alerts_enabled, progress_color="#00D1FF").pack(side="left")

        # Initialize the state in the scraper (always active so Stream Summary and logs capture all donos)
        if hasattr(self, 'scraper'):
            self.scraper.enable_dono_scanning = True

        # Minimum Kicks for Alert
        row1_b = ctk.CTkFrame(top, fg_color="transparent")
        row1_b.pack(fill="x", padx=15, pady=(5, 5))
        ctk.CTkLabel(row1_b, text="Min KICKs for Alert:", font=("Arial", 11)).pack(side="left", padx=(0, 10))
        
        min_alert_val = kd_settings.get("min_alert_amount", 100)
        min_alert_var = ctk.StringVar(value=str(min_alert_val) if min_alert_val else "0")
        min_alert_entry = ctk.CTkEntry(row1_b, textvariable=min_alert_var, width=80)
        min_alert_entry.pack(side="left")
        def update_min_alert(*args):
            val = min_alert_var.get()
            if val.isdigit():
                kd_settings["min_alert_amount"] = int(val)
            else:
                kd_settings["min_alert_amount"] = 0
            save_kd()
        min_alert_var.trace_add("write", update_min_alert)

        # Sound File
        row2 = ctk.CTkFrame(top, fg_color="transparent")
        row2.pack(fill="x", padx=15, pady=10)
        
        sound_var = ctk.BooleanVar(value=kd_settings.get("sound_enabled", False))
        def update_sound_toggle():
            kd_settings["sound_enabled"] = sound_var.get()
            save_kd()
        ctk.CTkSwitch(row2, text="Play Alert Sound", variable=sound_var, command=update_sound_toggle, progress_color="#00D1FF").pack(side="left")
        
        def choose_sound():
            from tkinter import filedialog
            import os
            import shutil
            filepath = filedialog.askopenfilename(filetypes=[("Audio Files", "*.mp3 *.wav *.ogg")])
            if filepath:
                filename = os.path.basename(filepath)
                os.makedirs("sounds", exist_ok=True)
                local_target_path = os.path.join("sounds", filename)
                
                if os.path.abspath(filepath) != os.path.abspath(local_target_path):
                    try:
                        shutil.copy2(filepath, local_target_path)
                    except Exception as e:
                        print(f"Error copying sound: {e}")
                
                kd_settings["sound_file"] = local_target_path
                sound_lbl.configure(text=f"...{local_target_path[-20:]}" if len(local_target_path) > 20 else local_target_path)
                save_kd()
                
        ctk.CTkButton(row2, text="Browse Sound", width=100, command=choose_sound).pack(side="left", padx=15)
        sound_path = kd_settings.get("sound_file", "")
        sound_lbl = ctk.CTkLabel(row2, text=f"...{sound_path[-20:]}" if len(sound_path)>20 else (sound_path or "No file selected"), text_color="#8E9299")
        sound_lbl.pack(side="left")

        # TTS Options
        row3 = ctk.CTkFrame(top, fg_color="transparent")
        row3.pack(fill="x", padx=15, pady=5)
        
        tts_var = ctk.BooleanVar(value=kd_settings.get("tts_enabled", False))
        def update_tts_toggle():
            kd_settings["tts_enabled"] = tts_var.get()
            save_kd()
        ctk.CTkSwitch(row3, text="Play TTS Message", variable=tts_var, command=update_tts_toggle, progress_color="#00D1FF").pack(side="left")
        
        ctk.CTkLabel(row3, text="Voice Cmd:", font=("Arial", 11)).pack(side="left", padx=(15, 5))
        cmd_var = ctk.StringVar(value=kd_settings.get("tts_voice", "!narrator"))
        cmd_entry = ctk.CTkEntry(row3, textvariable=cmd_var, width=100)
        cmd_entry.pack(side="left")
        def update_cmd(*args):
            kd_settings["tts_voice"] = cmd_var.get()
            save_kd()
        cmd_var.trace_add("write", update_cmd)

        self.attach_voice_autocomplete(
            cmd_entry,
            string_var=cmd_var,
            on_select=lambda v: [kd_settings.update({"tts_voice": v.get("command", "")}), save_kd()],
            parent=top
        )

        # TTS Template
        row4 = ctk.CTkFrame(top, fg_color="transparent")
        row4.pack(fill="x", padx=15, pady=5)
        ctk.CTkLabel(row4, text="TTS Message Template ([user], [amount], [message]):", font=("Arial", 11)).pack(anchor="w")
        
        msg_var = ctk.StringVar(value=kd_settings.get("tts_message", "Thank you [user] for the [amount] kicks! [message]"))
        msg_entry = ctk.CTkEntry(row4, textvariable=msg_var, width=400)
        msg_entry.pack(fill="x", pady=2)
        def update_msg(*args):
            kd_settings["tts_message"] = msg_var.get()
            save_kd()
        msg_var.trace_add("write", update_msg)

        # Log Section
        ctk.CTkLabel(top, text="Kicks Donation Log", font=("Arial", 16, "bold")).pack(pady=(15, 0))
        
        filter_frame = ctk.CTkFrame(top, fg_color="transparent")
        filter_frame.pack(fill="x", padx=15, pady=5)
        
        ctk.CTkLabel(filter_frame, text="Min Amount:", font=("Arial", 11)).pack(side="left", padx=(0, 5))
        log_min_val = kd_settings.get("log_min_amount", 1)
        log_min_var = ctk.StringVar(value=str(log_min_val) if log_min_val else "0")
        log_min_entry = ctk.CTkEntry(filter_frame, textvariable=log_min_var, width=60)
        log_min_entry.pack(side="left", padx=(0, 15))
        def update_log_min(*args):
            val = log_min_var.get()
            kd_settings["log_min_amount"] = int(val) if val.isdigit() else 0
            save_kd()
            self.refresh_dono_log()
        log_min_var.trace_add("write", update_log_min)
        
        self.dono_filter_var = ctk.StringVar(value=kd_settings.get("log_filter", "All Time"))
        
        def on_filter_change(val):
            kd_settings["log_filter"] = val
            save_kd()
            self.refresh_dono_log()

        filter_combo = ctk.CTkComboBox(filter_frame, values=["This Session", "Past 12h", "Last 24h", "Last 3d", "Last 7d", "Last 30d", "All Time"], 
                                      variable=self.dono_filter_var, state="readonly", command=on_filter_change, width=115)
        filter_combo.pack(side="right", padx=(0, 0))
        
        self.dono_sort_var = ctk.StringVar(value=kd_settings.get("log_sort", "Newest Donos"))
        def on_sort_change(val):
            kd_settings["log_sort"] = val
            save_kd()
            self.refresh_dono_log()

        sort_combo = ctk.CTkComboBox(filter_frame, values=["Newest Donos", "User Total Donos"],
                                      variable=self.dono_sort_var, state="readonly", command=on_sort_change, width=130)
        sort_combo.pack(side="right", padx=(5, 10))
        
        ctk.CTkLabel(filter_frame, text="Sort By:", font=("Arial", 11)).pack(side="right", padx=(10, 0))
        
        self.dono_scroll = ctk.CTkScrollableFrame(top, fg_color="#0A0A0E")
        self.dono_scroll.pack(fill="both", expand=True, padx=15, pady=(0, 15))
        
        self.refresh_dono_log()

    def load_donos(self):
        import json
        import os
        if os.path.exists("donations.json"):
            try:
                with open("donations.json", "r") as f:
                    return json.load(f)
            except:
                return []
        return []

    def save_donos(self, donos):
        import json
        try:
            with open("donations.json", "w") as f:
                json.dump(donos, f, indent=4)
        except Exception as e:
            print(f"[Error] Failed to save donations: {e}")

    def is_sub_renewal_already_recorded(self, user, months):
        """
        Checks if this user already has a sub renewal recorded for the specified number of months.
        Used to prevent double-counting sub renewals in Stream Summary and donations log when
        Kick sends the initial sub renewal notification followed later by a celebration message.
        """
        if not user or not months:
            return False
        u_lower = str(user).strip().lstrip('@').lower()
        try:
            m = int(months)
        except Exception:
            return False
        if m <= 0:
            return False

        if not hasattr(self, '_recorded_sub_renewals'):
            self._recorded_sub_renewals = {}

        if (u_lower, m) in self._recorded_sub_renewals:
            return True

        # Check in session_subs
        if hasattr(self, 'session_subs') and hasattr(self, '_summary_lock'):
            with self._summary_lock:
                for s in self.session_subs:
                    if str(s.get("user", "")).strip().lstrip('@').lower() == u_lower:
                        if s.get("months") == m:
                            return True
                        if s.get("sub_type") in ("celebration", "resub", "renewal"):
                            s_msg = str(s.get("message", "")).lower()
                            if f"{m} month" in s_msg or f"{m} months" in s_msg:
                                return True

        # Check recent entries in donations.json
        try:
            donos = self.load_donos()
            for d in reversed(donos[-150:]):
                if str(d.get("user", "")).strip().lstrip('@').lower() == u_lower and d.get("type") == "subscription":
                    if d.get("months") == m:
                        return True
                    d_msg = str(d.get("message", "")).lower()
                    if f"{m} month" in d_msg or f"{m} months" in d_msg:
                        return True
        except Exception:
            pass

        return False

    def record_sub_renewal(self, user, months, has_played_first=True, has_played_custom_msg=False):
        """Records that a sub renewal for (user, months) has been processed."""
        if not user or not months:
            return
        u_lower = str(user).strip().lstrip('@').lower()
        try:
            m = int(months)
        except Exception:
            return
        if not hasattr(self, '_recorded_sub_renewals'):
            self._recorded_sub_renewals = {}
        import datetime
        now_ts = datetime.datetime.now().timestamp()
        self._recorded_sub_renewals[(u_lower, m)] = {
            "user": user,
            "months": m,
            "timestamp": now_ts,
            "has_played_first": has_played_first,
            "has_played_custom_msg": has_played_custom_msg
        }

    def has_played_sub_renewal_custom_message(self, user, months):
        if not hasattr(self, '_recorded_sub_renewals'):
            self._recorded_sub_renewals = {}
        u_lower = str(user).strip().lstrip('@').lower()
        try:
            m = int(months)
        except Exception:
            return False
        rec = self._recorded_sub_renewals.get((u_lower, m))
        return bool(rec and rec.get("has_played_custom_msg"))

    def mark_sub_renewal_custom_message_played(self, user, months):
        if not hasattr(self, '_recorded_sub_renewals'):
            self._recorded_sub_renewals = {}
        u_lower = str(user).strip().lstrip('@').lower()
        try:
            m = int(months)
        except Exception:
            return
        if (u_lower, m) in self._recorded_sub_renewals:
            self._recorded_sub_renewals[(u_lower, m)]["has_played_custom_msg"] = True
        else:
            self.record_sub_renewal(user, m, has_played_first=True, has_played_custom_msg=True)

    def log_subscription(self, user, count=1, sub_type="subscription", message="", sub_id="", months=None):
        if not user:
            return
        import datetime
        user = str(user).strip().lstrip('@')
        try:
            count = int(count)
        except Exception:
            count = 1
        if count <= 0:
            return

        # Deduplication
        if not hasattr(self, '_processed_sub_log_keys'):
            self._processed_sub_log_keys = {}
        now_ts = datetime.datetime.now().timestamp()

        # Prune old keys
        if len(self._processed_sub_log_keys) > 2000:
            for k in list(self._processed_sub_log_keys.keys())[:500]:
                del self._processed_sub_log_keys[k]

        dedup_window = 4.0 if count > 1 else 20.0
        dedup_key = f"{user.lower()}::{count}::{sub_id}" if sub_id else f"{user.lower()}::{count}::{sub_type}"
        if dedup_key in self._processed_sub_log_keys:
            if now_ts - self._processed_sub_log_keys[dedup_key] < dedup_window:
                return
        self._processed_sub_log_keys[dedup_key] = now_ts

        u_lower = user.lower()
        if count == 1 and sub_type != "gifted":
            if not hasattr(self, '_recent_user_sub_logs'):
                self._recent_user_sub_logs = {}
            if u_lower in self._recent_user_sub_logs:
                prev_ts = self._recent_user_sub_logs[u_lower]
                if now_ts - prev_ts < 20.0:
                    # Update message in existing dono / session_subs if new message is more descriptive
                    clean_msg = str(message or "").strip()
                    if clean_msg and clean_msg.lower() not in ("subscribed", "subscription"):
                        try:
                            donos = self.load_donos()
                            for d in reversed(donos[-10:]):
                                if d.get("user", "").lower() == u_lower and d.get("type") == "subscription":
                                    d["message"] = clean_msg
                                    if sub_type:
                                        d["sub_type"] = sub_type
                                    if months is not None:
                                        d["months"] = int(months)
                                    self.save_donos(donos)
                                    self.after(0, self.refresh_dono_log)
                                    break
                            if hasattr(self, '_summary_lock') and hasattr(self, 'session_subs'):
                                with self._summary_lock:
                                    for s in reversed(self.session_subs[-10:]):
                                        if s.get("user", "").lower() == u_lower:
                                            s["sub_type"] = sub_type
                                            if months is not None:
                                                s["months"] = int(months)
                                            break
                        except Exception:
                            pass
                    return
            self._recent_user_sub_logs[u_lower] = now_ts

        # Subscriptions have an equivalent value of 500 kicks each
        equiv_kicks = count * 500
        donos = self.load_donos()
        clean_msg = str(message or "").strip()
        donos.append({
            "type": "subscription",
            "timestamp": datetime.datetime.now().isoformat(),
            "user": user,
            "subs": count,
            "amount": equiv_kicks,
            "message": clean_msg,
            "sub_type": sub_type,
            "months": int(months) if months is not None else None
        })
        self.save_donos(donos)
        self.after(0, self.refresh_dono_log)

        # Record subscription for Stream Summary
        if hasattr(self, '_summary_lock') and hasattr(self, 'session_subs'):
            with self._summary_lock:
                self.session_subs.append({
                    "user": user,
                    "count": count,
                    "sub_type": sub_type,
                    "months": int(months) if months is not None else None,
                    "timestamp": datetime.datetime.now().isoformat()
                })
            self.record_stream_summary_activity()
            self.after(0, self.refresh_stream_summary_if_open)

    def refresh_dono_log(self, *args):
        if not hasattr(self, 'dono_scroll') or not self.dono_scroll.winfo_exists():
            return
            
        for widget in self.dono_scroll.winfo_children():
            widget.destroy()
            
        donos = self.load_donos()
        filter_val = self.dono_filter_var.get()
        sort_val = self.dono_sort_var.get()
        log_min = self.settings.get("kick_donos", {}).get("log_min_amount", 1)
        import datetime
        
        now = datetime.datetime.now()
        filtered = []
        for d in donos:
            is_sub = (d.get("type") in ("subscription", "sub", "gifted_sub")) or (d.get("subs") is not None and int(d.get("subs", 0) or 0) > 0)
            subs = int(d.get("subs", 1 if is_sub else 0) or 0)
            kicks = 0 if is_sub else int(d.get("amount", 0) or 0)
            total_val = (subs * 500) + kicks

            if total_val < log_min:
                continue

            try:
                dt = datetime.datetime.fromisoformat(d.get("timestamp", ""))
                if filter_val == "This Session":
                    session_start = getattr(self, "session_start_time", None)
                    if session_start is None or dt < session_start:
                        continue
                elif filter_val == "Past 12h" and (now - dt).total_seconds() > 43200:
                    continue
                elif filter_val == "Last 24h" and (now - dt).total_seconds() > 86400:
                    continue
                elif filter_val == "Last 3d" and (now - dt).days > 3:
                    continue
                elif filter_val == "Last 7d" and (now - dt).days > 7:
                    continue
                elif filter_val == "Last 30d" and (now - dt).days > 30:
                    continue
                filtered.append(d)
            except:
                if filter_val not in ("This Session", "Past 12h", "Last 24h", "Last 3d", "Last 7d", "Last 30d"):
                    filtered.append(d)
                
        if not filtered:
            ctk.CTkLabel(self.dono_scroll, text="No donations found.", text_color="#8E9299").pack(pady=20)
            return

        if "User Total Dono" in sort_val:
            user_totals = {}
            for d in filtered:
                u = d.get("user", "Unknown")
                is_sub = (d.get("type") in ("subscription", "sub", "gifted_sub")) or (d.get("subs") is not None and int(d.get("subs", 0) or 0) > 0)
                subs = int(d.get("subs", 1 if is_sub else 0) or 0)
                kicks = 0 if is_sub else int(d.get("amount", 0) or 0)
                val = (subs * 500) + kicks

                if u not in user_totals:
                    user_totals[u] = {"subs": 0, "kicks": 0, "total_val": 0}
                user_totals[u]["subs"] += subs
                user_totals[u]["kicks"] += kicks
                user_totals[u]["total_val"] += val

            # Sort by total dono value descending
            sorted_totals = sorted(user_totals.items(), key=lambda x: x[1]["total_val"], reverse=True)
            
            for u, info in sorted_totals[:100]:
                f = ctk.CTkFrame(self.dono_scroll, fg_color="#1e1e24")
                f.pack(fill="x", pady=2, padx=2)
                t_val = info["total_val"]
                t_subs = info["subs"]
                t_kicks = info["kicks"]
                sub_word = "subscription" if t_subs == 1 else "subscriptions"
                # Display username and their subscriptions donated and then their kicks donated
                display_text = f"{u} gifted {t_val:,} KICKs in total ({t_subs} {sub_word} donated, {t_kicks:,} KICKs donated)"
                lbl = ctk.CTkLabel(f, text=display_text, justify="left")
                lbl.pack(side="left", padx=10, pady=5)
        else:
            # Sort newest first
            filtered.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            
            for d in filtered[:100]:
                f = ctk.CTkFrame(self.dono_scroll, fg_color="#1e1e24")
                f.pack(fill="x", pady=2, padx=2)
                try:
                    dt_str = datetime.datetime.fromisoformat(d.get("timestamp", "")).strftime("%Y-%m-%d %H:%M:%S")
                except:
                    dt_str = d.get("timestamp", "")
                
                d_msg = d.get('message', '').strip()
                is_sub = (d.get("type") in ("subscription", "sub", "gifted_sub")) or (d.get("subs") is not None and int(d.get("subs", 0) or 0) > 0)
                
                if is_sub:
                    sub_cnt = int(d.get("subs", 1) or 1)
                    sub_word = "subscription" if sub_cnt == 1 else "subscriptions"
                    equiv_val = sub_cnt * 500
                    sub_type = d.get("sub_type", "subscription")
                    if sub_type == "gifted":
                        display_text = f"[{dt_str}] {d.get('user', 'Unknown')} gifted {sub_cnt} {sub_word} (value: {equiv_val:,} KICKs)"
                    else:
                        display_text = f"[{dt_str}] {d.get('user', 'Unknown')} subscribed ({sub_cnt} {sub_word}, value: {equiv_val:,} KICKs)"
                    if d_msg and not d_msg.lower().startswith("gifted") and not d_msg.lower().startswith("subscribed"):
                        display_text += f"\n\"{d_msg}\""
                else:
                    k_amt = int(d.get('amount', 0) or 0)
                    display_text = f"[{dt_str}] {d.get('user', 'Unknown')} gifted {k_amt:,} KICKs"
                    if d_msg:
                        display_text += f"\n\"{d_msg}\""
                
                lbl = ctk.CTkLabel(f, text=display_text, justify="left", wraplength=480)
                lbl.pack(side="left", padx=10, pady=5)

    def open_app_settings(self):
        if hasattr(self, 'app_settings_window') and self.app_settings_window and self.app_settings_window.winfo_exists():
            self.app_settings_window.focus()
            return
            
        top = ctk.CTkToplevel(self)
        self.app_settings_window = top
        top.title("Settings")
        
        self.center_toplevel(top, 420, 830)
        self.apply_dark_title_bar(top)
        top.minsize(420, 780)
        top.attributes("-topmost", True)

        # 1. WHO CAN USE
        who_frame = ctk.CTkFrame(top, fg_color="transparent")
        who_frame.pack(fill="x", padx=15, pady=(12, 10))
        ctk.CTkLabel(who_frame, text="WHO CAN USE:", font=("Arial", 12, "bold")).pack(side="left", padx=(0, 10))
        
        who_settings = self.settings.setdefault("who_can_use", {"ALL": True, "OG": False, "VIP": False, "SUB": False, "MOD": False})
        self.who_buttons = {}
        
        def update_who_buttons():
            if who_settings.get("ALL"):
                self.who_buttons["ALL"].configure(fg_color="#2b5329", hover_color="#3b733b") # Transparent green
            else:
                self.who_buttons["ALL"].configure(fg_color="#3A3A40", hover_color="#55555C")
                
            if who_settings.get("OG"):
                self.who_buttons["OG"].configure(fg_color="#18565e", hover_color="#2b7a85") # Transparent teal/blue-grey
            else:
                self.who_buttons["OG"].configure(fg_color="#3A3A40", hover_color="#55555C")
                
            if who_settings.get("VIP"):
                self.who_buttons["VIP"].configure(fg_color="#5e541f", hover_color="#82752c") # Transparent yellow
            else:
                self.who_buttons["VIP"].configure(fg_color="#3A3A40", hover_color="#55555C")
                
            if who_settings.get("SUB"):
                self.who_buttons["SUB"].configure(fg_color="#4c2b5e", hover_color="#6e3f87") # Transparent purple
            else:
                self.who_buttons["SUB"].configure(fg_color="#3A3A40", hover_color="#55555C")
                
            if who_settings.get("MOD"):
                self.who_buttons["MOD"].configure(fg_color="#1e4c63", hover_color="#2b6a8a") # Transparent blue
            else:
                self.who_buttons["MOD"].configure(fg_color="#3A3A40", hover_color="#55555C")

        def toggle_who(role):
            if role == "ALL":
                who_settings["ALL"] = True
                who_settings["OG"] = False
                who_settings["VIP"] = False
                who_settings["SUB"] = False
                who_settings["MOD"] = False
            else:
                who_settings[role] = not who_settings.get(role, False)
                if who_settings["OG"] or who_settings["VIP"] or who_settings["SUB"] or who_settings["MOD"]:
                    who_settings["ALL"] = False
            
            self.settings["who_can_use"] = who_settings
            self.save_settings()
            update_who_buttons()

        for placeholder in ["ALL", "OG", "VIP", "SUB", "MOD"]:
            btn = ctk.CTkButton(who_frame, text=placeholder, width=40, height=24, font=("Arial", 11), 
                                fg_color="#3A3A40", hover_color="#55555C",
                                command=lambda r=placeholder: toggle_who(r))
            btn.pack(side="left", padx=2)
            self.who_buttons[placeholder] = btn
            
        update_who_buttons()
            
        # 2. Max Characters
        max_frame = ctk.CTkFrame(top, fg_color="transparent")
        max_frame.pack(fill="x", padx=15, pady=(5, 10))
        ctk.CTkLabel(max_frame, text="Max characters: ", font=("Arial", 12)).pack(side="left")
        
        max_entry = ctk.CTkEntry(max_frame, width=80)
        max_entry.insert(0, str(self.settings.get("max_chars", 300)))
        max_entry.pack(side="left", padx=10)
        
        def save_max_chars(event=None):
            try:
                val_str = max_entry.get().strip()
                if not val_str:
                    return
                val = int(val_str)
                self.settings["max_chars"] = val
                self.save_settings()
            except ValueError:
                pass
                
        max_entry.bind("<KeyRelease>", save_max_chars)
        max_entry.bind("<FocusOut>", lambda e: [save_max_chars(e), max_entry.delete(0, "end"), max_entry.insert(0, str(self.settings.get("max_chars", 300))) if not max_entry.get().strip() else None])
        
        # Hotkey Configuration (Pause/Unpause & Skip TTS)
        hotkeys_frame = ctk.CTkFrame(top, fg_color="transparent")
        hotkeys_frame.pack(fill="x", padx=15, pady=(0, 6))

        # Pause / Unpause Hotkey Row
        pause_hk_frame = ctk.CTkFrame(hotkeys_frame, fg_color="transparent")
        pause_hk_frame.pack(fill="x", pady=(0, 6))
        ctk.CTkLabel(pause_hk_frame, text="Pause / Unpause Hotkey:", font=("Arial", 12), width=160, anchor="w").pack(side="left")

        pause_mod_var = ctk.StringVar(value=self.settings.get("pause_hotkey_mod", "Alt"))
        pause_mod_menu = ctk.CTkOptionMenu(
            pause_hk_frame,
            values=["Alt", "Ctrl"],
            variable=pause_mod_var,
            width=70,
            command=lambda val: on_pause_hk_change()
        )
        pause_mod_menu.pack(side="left", padx=(0, 6))
        ctk.CTkLabel(pause_hk_frame, text="+", font=("Arial", 13, "bold")).pack(side="left", padx=(0, 6))
        pause_key_entry = ctk.CTkEntry(pause_hk_frame, width=45)
        pause_key_entry.insert(0, str(self.settings.get("pause_hotkey_key", "1")))
        pause_key_entry.pack(side="left")

        # Skip TTS Hotkey Row
        skip_hk_frame = ctk.CTkFrame(hotkeys_frame, fg_color="transparent")
        skip_hk_frame.pack(fill="x", pady=(0, 2))
        ctk.CTkLabel(skip_hk_frame, text="Skip TTS Hotkey:", font=("Arial", 12), width=160, anchor="w").pack(side="left")

        skip_mod_var = ctk.StringVar(value=self.settings.get("skip_hotkey_mod", "Alt"))
        skip_mod_menu = ctk.CTkOptionMenu(
            skip_hk_frame,
            values=["Alt", "Ctrl"],
            variable=skip_mod_var,
            width=70,
            command=lambda val: on_skip_hk_change()
        )
        skip_mod_menu.pack(side="left", padx=(0, 6))
        ctk.CTkLabel(skip_hk_frame, text="+", font=("Arial", 13, "bold")).pack(side="left", padx=(0, 6))
        skip_key_entry = ctk.CTkEntry(skip_hk_frame, width=45)
        skip_key_entry.insert(0, str(self.settings.get("skip_hotkey_key", "2")))
        skip_key_entry.pack(side="left")

        reset_hk_btn = ctk.CTkButton(
            skip_hk_frame,
            text="Reset Hotkeys",
            width=104,
            height=26,
            font=("Arial", 11),
            fg_color="#005E73",
            hover_color="#00D1FF",
            command=lambda: [
                self.register_hotkeys(),
                self.append_system_log("\n[System] Global hotkeys refreshed and re-registered.")
            ]
        )
        reset_hk_btn.pack(side="left", padx=(8, 0))
        CTkToolTip(reset_hk_btn, text="Re-registers global Pause and Skip hotkeys if they become unresponsive.")

        def on_pause_hk_change(event=None):
            mod_val = pause_mod_var.get()
            raw_key = pause_key_entry.get().strip()
            digits = "".join(c for c in raw_key if c.isdigit())
            if digits != raw_key and raw_key:
                pause_key_entry.delete(0, "end")
                pause_key_entry.insert(0, digits)
            key_val = digits if digits else raw_key
            if not key_val:
                return
            self.settings["pause_hotkey_mod"] = mod_val
            self.settings["pause_hotkey_key"] = key_val
            self.save_settings()
            self.register_hotkeys()

        def on_skip_hk_change(event=None):
            mod_val = skip_mod_var.get()
            raw_key = skip_key_entry.get().strip()
            digits = "".join(c for c in raw_key if c.isdigit())
            if digits != raw_key and raw_key:
                skip_key_entry.delete(0, "end")
                skip_key_entry.insert(0, digits)
            key_val = digits if digits else raw_key
            if not key_val:
                return
            self.settings["skip_hotkey_mod"] = mod_val
            self.settings["skip_hotkey_key"] = key_val
            self.save_settings()
            self.register_hotkeys()

        pause_key_entry.bind("<KeyRelease>", on_pause_hk_change)
        pause_key_entry.bind("<FocusOut>", lambda e: [
            on_pause_hk_change(),
            (pause_key_entry.delete(0, "end"), pause_key_entry.insert(0, str(self.settings.get("pause_hotkey_key", "1")))) if not pause_key_entry.get().strip() else None
        ])
        skip_key_entry.bind("<KeyRelease>", on_skip_hk_change)
        skip_key_entry.bind("<FocusOut>", lambda e: [
            on_skip_hk_change(),
            (skip_key_entry.delete(0, "end"), skip_key_entry.insert(0, str(self.settings.get("skip_hotkey_key", "2")))) if not skip_key_entry.get().strip() else None
        ])

        # Save Voices directory row (above Enable Raid Alerts and below Skip TTS Hotkey)
        save_voices_frame = ctk.CTkFrame(top, fg_color="transparent")
        save_voices_frame.pack(fill="x", padx=15, pady=(6, 4))

        save_voices_dir = self.get_saved_audio_dir()
        save_voices_var = ctk.StringVar(value=save_voices_dir)

        def browse_save_voices_dir():
            from tkinter import filedialog
            current_path = save_voices_var.get().strip()
            initial_path = current_path if (current_path and os.path.exists(current_path)) else os.path.dirname(os.path.abspath(__file__))
            chosen = filedialog.askdirectory(parent=top, initialdir=initial_path, title="Select Save Voices Folder")
            if chosen:
                save_voices_var.set(chosen)
                self.settings["saved_audio_dir"] = chosen
                self.save_settings()

        def on_save_voices_change(*args):
            self.settings["saved_audio_dir"] = save_voices_var.get().strip()
            self.save_settings()

        save_voices_var.trace_add("write", on_save_voices_change)

        save_voices_btn = ctk.CTkButton(
            save_voices_frame,
            text="Save Voices",
            width=110,
            height=28,
            fg_color="#005E73",
            hover_color="#00D1FF",
            command=browse_save_voices_dir
        )
        save_voices_btn.pack(side="left", padx=(0, 8))
        CTkToolTip(save_voices_btn, text="Select the folder where saved TTS and voice audio files are stored.")

        save_voices_entry = ctk.CTkEntry(
            save_voices_frame,
            textvariable=save_voices_var,
            font=("Arial", 11),
            height=28
        )
        save_voices_entry.pack(side="left", fill="x", expand=True)

        # Backup & Restore Data button (placed directly below Save Voices)
        backup_frame = ctk.CTkFrame(top, fg_color="transparent")
        backup_frame.pack(fill="x", padx=15, pady=(4, 4))
        
        backup_btn = ctk.CTkButton(
            backup_frame,
            text="📦 BACKUP & RESTORE DATA",
            fg_color="#005E73",
            hover_color="#00D1FF",
            font=("Arial", 12, "bold"),
            height=32,
            command=self.open_backup_restore_window
        )
        backup_btn.pack(fill="x")
        CTkToolTip(backup_btn, text="Create dated backups of settings and lore files, or restore previous configurations.")

        # Subtle divider line (above Raid Alerts section)
        ctk.CTkFrame(top, fg_color="#363C48", height=2, corner_radius=0).pack(fill="x", padx=15, pady=(10, 8))

        # Raid Alerts switch (above Enable Subscription Alerts and below Save Voices)
        raid_frame = ctk.CTkFrame(top, fg_color="transparent")
        raid_frame.pack(fill="x", padx=15, pady=(8, 4))

        raid_enabled_var = ctk.BooleanVar(value=self.settings.get("raid_alerts_enabled", True))

        def on_raid_toggle():
            self.settings["raid_alerts_enabled"] = raid_enabled_var.get()
            self.save_settings()

        raid_switch = ctk.CTkSwitch(
            raid_frame,
            text="Enable Raid Alerts",
            variable=raid_enabled_var,
            command=on_raid_toggle,
            progress_color="#00D1FF",
            font=("Arial", 12, "bold")
        )
        raid_switch.pack(anchor="w", pady=(0, 2))

        # Raids Voice
        raid_voice_row = ctk.CTkFrame(raid_frame, fg_color="transparent")
        raid_voice_row.pack(fill="x", padx=(42, 0), pady=(3, 2))
        ctk.CTkLabel(raid_voice_row, text="Raids Voice:", font=("Arial", 12)).pack(side="left", padx=(0, 8))
        raid_voice_entry = ctk.CTkEntry(raid_voice_row, width=150)
        raid_voice_entry.insert(0, str(self.settings.get("raid_alerts_voice", "")))
        raid_voice_entry.pack(side="left")

        def on_raid_voice_change(event=None):
            self.settings["raid_alerts_voice"] = raid_voice_entry.get().strip()
            self.save_settings()

        raid_voice_entry.bind("<KeyRelease>", on_raid_voice_change)
        raid_voice_entry.bind("<FocusOut>", on_raid_voice_change)

        self.attach_voice_autocomplete(
            raid_voice_entry,
            on_select=lambda v: [self.settings.update({"raid_alerts_voice": v.get("command", "")}), self.save_settings()],
            parent=top
        )

        # Sub Celebration & Gifted Sub Alerts switches
        sub_alert_frame = ctk.CTkFrame(top, fg_color="transparent")
        sub_alert_frame.pack(fill="x", padx=15, pady=(4, 4))

        kickbot_sub_enabled_var = ctk.BooleanVar(value=bool(self.settings.get("kickbot_sub_alerts_enabled", True)))

        def on_kickbot_sub_toggle():
            val = bool(kickbot_sub_enabled_var.get())
            self.settings["kickbot_sub_alerts_enabled"] = val
            self.settings["sub_alerts_enabled"] = val
            self.save_settings(immediate=True)

        kickbot_sub_switch = ctk.CTkSwitch(
            sub_alert_frame,
            text="Enable Sub Alerts",
            variable=kickbot_sub_enabled_var,
            command=on_kickbot_sub_toggle,
            progress_color="#00D1FF",
            font=("Arial", 12, "bold")
        )
        kickbot_sub_switch.pack(anchor="w", pady=(0, 4))

        gifted_sub_enabled_var = ctk.BooleanVar(value=self.settings.get("gifted_sub_alerts_enabled", self.settings.get("gifted_sub_celebration_alerts_enabled", True)))

        def on_gifted_sub_toggle():
            self.settings["gifted_sub_alerts_enabled"] = gifted_sub_enabled_var.get()
            self.save_settings()

        gifted_sub_switch = ctk.CTkSwitch(
            sub_alert_frame,
            text="Enable Gifted Sub Alerts",
            variable=gifted_sub_enabled_var,
            command=on_gifted_sub_toggle,
            progress_color="#00D1FF",
            font=("Arial", 12, "bold")
        )
        gifted_sub_switch.pack(anchor="w", pady=(0, 4))

        sub_celebration_enabled_var = ctk.BooleanVar(value=self.settings.get("sub_celebration_alerts_enabled", self.settings.get("gifted_sub_celebration_alerts_enabled", True)))

        def on_sub_celebration_toggle():
            self.settings["sub_celebration_alerts_enabled"] = sub_celebration_enabled_var.get()
            self.save_settings()

        sub_celebration_switch = ctk.CTkSwitch(
            sub_alert_frame,
            text="Enable Sub Celebration Alerts",
            variable=sub_celebration_enabled_var,
            command=on_sub_celebration_toggle,
            progress_color="#00D1FF",
            font=("Arial", 12, "bold")
        )
        sub_celebration_switch.pack(anchor="w", pady=(0, 2))

        # Subs Voice (applies to both Gifted Sub and Sub Celebration alerts)
        sub_voice_row = ctk.CTkFrame(sub_alert_frame, fg_color="transparent")
        sub_voice_row.pack(fill="x", padx=(42, 0), pady=(3, 2))
        ctk.CTkLabel(sub_voice_row, text="Subs Voice:", font=("Arial", 12)).pack(side="left", padx=(0, 8))
        sub_voice_entry = ctk.CTkEntry(sub_voice_row, width=150)
        sub_voice_entry.insert(0, str(self.settings.get("sub_alerts_voice", "")))
        sub_voice_entry.pack(side="left")

        def on_sub_voice_change(event=None):
            self.settings["sub_alerts_voice"] = sub_voice_entry.get().strip()
            self.save_settings()

        sub_voice_entry.bind("<KeyRelease>", on_sub_voice_change)
        sub_voice_entry.bind("<FocusOut>", on_sub_voice_change)

        self.attach_voice_autocomplete(
            sub_voice_entry,
            on_select=lambda v: [self.settings.update({"sub_alerts_voice": v.get("command", "")}), self.save_settings()],
            parent=top
        )

        # Subtle divider line (below Sub Alerts section)
        ctk.CTkFrame(top, fg_color="#363C48", height=2, corner_radius=0).pack(fill="x", padx=15, pady=(10, 8))

        # KickBot TTS Integration Controls (below Sub Alerts)
        kb_frame = ctk.CTkFrame(top, fg_color="transparent")
        kb_frame.pack(fill="x", padx=15, pady=(8, 10))

        kb_settings = self.settings.setdefault("kickbot_tts", {"enabled": False, "url": "", "auto_start": True})
        kb_enabled_var = ctk.BooleanVar(value=kb_settings.get("enabled", False))

        def save_kb_settings():
            self.settings["kickbot_tts"] = kb_settings
            self.save_settings()

        def on_kb_toggle():
            kb_settings["enabled"] = kb_enabled_var.get()
            save_kb_settings()
            # If chat monitoring is running, start/stop listener dynamically
            if getattr(self, "scraper", None) and self.scraper.is_running:
                if kb_settings["enabled"] and kb_settings.get("url"):
                    if hasattr(self, "kickbot_listener") and self.kickbot_listener:
                        self.kickbot_listener.start(kb_settings.get("url"))
                else:
                    if hasattr(self, "kickbot_listener") and self.kickbot_listener and self.kickbot_listener.is_running:
                        self.kickbot_listener.stop()

        kb_switch = ctk.CTkSwitch(
            kb_frame,
            text="Enable KickBot TTS",
            variable=kb_enabled_var,
            command=on_kb_toggle,
            progress_color="#00D1FF",
            font=("Arial", 12, "bold")
        )
        kb_switch.pack(anchor="w", pady=(0, 6))

        ctk.CTkLabel(kb_frame, text="Kickbot TTS URL:", font=("Arial", 12)).pack(anchor="w", pady=(2, 2))

        kb_url_entry = ctk.CTkEntry(
            kb_frame,
            placeholder_text="https://kickbot.com/external/.../tts",
            fg_color="#0A0A0E",
            border_color="#1e1e24"
        )
        kb_url_entry.insert(0, kb_settings.get("url", ""))
        kb_url_entry.pack(fill="x", pady=(0, 2))

        def on_kb_url_change(event=None):
            kb_settings["url"] = kb_url_entry.get().strip()
            save_kb_settings()
            if getattr(self, "scraper", None) and self.scraper.is_running and kb_settings.get("enabled", False):
                if hasattr(self, "kickbot_listener") and self.kickbot_listener:
                    if kb_settings["url"]:
                        self.kickbot_listener.start(kb_settings["url"])
                    else:
                        self.kickbot_listener.stop()

        kb_url_entry.bind("<KeyRelease>", on_kb_url_change)
        kb_url_entry.bind("<FocusOut>", on_kb_url_change)

        # Powerchat Integration
        pc_frame = ctk.CTkFrame(top, fg_color="transparent")
        pc_frame.pack(fill="x", padx=15, pady=(4, 6))

        pc_enabled_var = ctk.BooleanVar(value=bool(self.settings.get("powerchat_enabled", True)))

        def on_pc_toggle():
            is_enabled = bool(pc_enabled_var.get())
            self.settings["powerchat_enabled"] = is_enabled
            self.save_settings()
            if getattr(self, "scraper", None) and self.scraper.is_running:
                current_link = str(self.settings.get("powerchat_tts_link", "")).strip()
                if is_enabled and current_link:
                    if hasattr(self, "powerchat_listener") and self.powerchat_listener and not self.powerchat_listener.is_running:
                        self.powerchat_listener.start(current_link)
                else:
                    if hasattr(self, "powerchat_listener") and self.powerchat_listener and self.powerchat_listener.is_running:
                        self.powerchat_listener.stop()
            self.refresh_stream_summary_if_open()

        pc_switch = ctk.CTkSwitch(
            pc_frame,
            text="Enable Powerchat in Stream Summary",
            variable=pc_enabled_var,
            command=on_pc_toggle,
            progress_color="#00D1FF",
            font=("Arial", 12, "bold")
        )
        pc_switch.pack(anchor="w", pady=(0, 4))

        pc_link_row = ctk.CTkFrame(pc_frame, fg_color="transparent")
        pc_link_row.pack(fill="x", pady=(2, 0))

        ctk.CTkLabel(pc_link_row, text="Powerchat TTS Link:", font=("Arial", 12)).pack(side="left", padx=(0, 8))

        pc_link_entry = ctk.CTkEntry(
            pc_link_row,
            placeholder_text="https://powerchat.live/[user]/tts",
            fg_color="#0A0A0E",
            border_color="#1e1e24",
            height=28
        )
        pc_link_val = self.settings.get("powerchat_tts_link")
        if pc_link_val is None:
            pc_link_val = "https://powerchat.live/jimbozoomer/tts"
            self.settings["powerchat_tts_link"] = pc_link_val
        pc_link_entry.insert(0, str(pc_link_val))
        pc_link_entry.pack(side="left", fill="x", expand=True)

        def on_pc_link_change(event=None):
            new_link = pc_link_entry.get().strip()
            self.settings["powerchat_tts_link"] = new_link
            self.save_settings()
            if getattr(self, "scraper", None) and self.scraper.is_running and self.settings.get("powerchat_enabled", True):
                if new_link:
                    if hasattr(self, "powerchat_listener") and self.powerchat_listener:
                        self.powerchat_listener.start(new_link)
                else:
                    if hasattr(self, "powerchat_listener") and self.powerchat_listener and self.powerchat_listener.is_running:
                        self.powerchat_listener.stop()

        pc_link_entry.bind("<KeyRelease>", on_pc_link_change)
        pc_link_entry.bind("<FocusOut>", on_pc_link_change)

        # 3. User block list
        block_frame = ctk.CTkFrame(top, fg_color="transparent")
        block_frame.pack(fill="both", expand=True, padx=15, pady=(6, 12))
        
        header_frame = ctk.CTkFrame(block_frame, fg_color="transparent")
        header_frame.pack(fill="x", pady=(0, 4))
        
        left_header_frame = ctk.CTkFrame(header_frame, fg_color="transparent")
        left_header_frame.pack(side="left")

        ctk.CTkLabel(left_header_frame, text="Banned Usernames:", font=("Arial", 12, "bold")).pack(side="left")
        ctk.CTkLabel(left_header_frame, text=" (one username per line)", font=("Arial", 10), text_color="#8E9299").pack(side="left", padx=(2, 0))
        
        count_val = len([u for u in self.settings.get("banned_users", []) if u.strip()])
        count_label = ctk.CTkLabel(header_frame, text=f"{count_val} users", text_color="#00D1FF", font=("Arial", 11))
        count_label.pack(side="right")
        
        banned_text = ctk.CTkTextbox(block_frame, fg_color="#0A0A0E", border_color="#1e1e24", border_width=1, wrap="word", height=95)
        banned_text.pack(fill="both", expand=True)
        
        banned_list = self.settings.get("banned_users", [])
        if banned_list:
            banned_text.insert("1.0", "\n".join(banned_list) + "\n")
            
        def on_banned_change(event=None):
            raw_text = banned_text.get("1.0", "end-1c")
            users = [line.strip() for line in raw_text.split("\n") if line.strip()]
            self.settings["banned_users"] = users
            self.save_settings()
            count_label.configure(text=f"{len(users)} users")

        banned_text.bind("<KeyRelease>", on_banned_change)
        banned_text.bind("<FocusOut>", on_banned_change)

    def get_backup_directory(self):
        backup_dir = os.path.abspath("settings backup")
        os.makedirs(backup_dir, exist_ok=True)
        return backup_dir

    def create_backup(self, is_safety=False):
        import shutil
        import datetime
        
        backup_base = self.get_backup_directory()
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        prefix = "Safety_PreRestore" if is_safety else "Backup"
        snapshot_name = f"{prefix}_{timestamp}"
        snapshot_dir = os.path.join(backup_base, snapshot_name)
        os.makedirs(snapshot_dir, exist_ok=True)
        
        # Discover all .json files in current app directory
        copied_files = []
        try:
            for fname in os.listdir("."):
                if fname.lower().endswith(".json") and os.path.isfile(fname):
                    shutil.copy2(fname, os.path.join(snapshot_dir, fname))
                    copied_files.append(fname)
        except Exception as e:
            self.append_system_log(f"\n[System Error] Error copying backup files: {e}")
            
        action_type = "Pre-restore safety backup" if is_safety else "Manual backup"
        self.append_system_log(f"\n[System] {action_type} created: '{snapshot_name}' ({len(copied_files)} files saved).")
        return snapshot_name, copied_files

    def open_backup_restore_window(self):
        if hasattr(self, 'backup_restore_window') and self.backup_restore_window and self.backup_restore_window.winfo_exists():
            self.backup_restore_window.focus()
            return

        import datetime
        import shutil

        top = ctk.CTkToplevel(self)
        self.backup_restore_window = top
        top.title("BACKUP & RESTORE DATA")
        
        self.center_toplevel(top, 540, 750)
        self.apply_dark_title_bar(top)
        top.minsize(480, 650)
        top.attributes("-topmost", True)

        known_descriptions = {
            "character_lore.json": "Character Lore & Prompts",
            "universe_lore.json": "Universe Lore & World Context",
            "global_rules.json": "Global AI RP Rules",
            "settings.json": "General App Settings & Audio Config",
            "voices.json": "Voice Library & Custom Commands",
            "channel_points.json": "Channel Points & Custom Rewards",
            "donations.json": "Donation / Bits History",
            "timeouts.json": "User Timeouts & Mutes",
            "stream_session.json": "Stream Session Stats & Activity",
        }

        # Header
        header_frame = ctk.CTkFrame(top, fg_color="transparent")
        header_frame.pack(fill="x", padx=16, pady=(14, 8))
        ctk.CTkLabel(header_frame, text="SETTINGS & LORE BACKUP / RESTORE", font=("Arial", 14, "bold"), text_color="#00D1FF").pack(anchor="w")
        ctk.CTkLabel(header_frame, text="Create dated backups of all JSON files and selectively restore them anytime.", font=("Arial", 11), text_color="#8E9299").pack(anchor="w")

        # --- SECTION 1: CREATE BACKUP ---
        backup_box = ctk.CTkFrame(top, fg_color="#121216", corner_radius=8, border_width=1, border_color="#1e1e24")
        backup_box.pack(fill="x", padx=16, pady=(4, 10))

        ctk.CTkLabel(backup_box, text="CREATE BACKUP", font=("Arial", 12, "bold"), text_color="#E1E1E6").pack(anchor="w", padx=14, pady=(10, 4))
        ctk.CTkLabel(backup_box, text="Saves a complete snapshot of all current settings, voice lists, and character lores.", font=("Arial", 10), text_color="#8E9299").pack(anchor="w", padx=14, pady=(0, 8))

        btn_row = ctk.CTkFrame(backup_box, fg_color="transparent")
        btn_row.pack(fill="x", padx=14, pady=(0, 8))

        backup_status_lbl = ctk.CTkLabel(backup_box, text="Ready", font=("Arial", 11), text_color="#8E9299")
        backup_status_lbl.pack(anchor="w", padx=14, pady=(0, 10))

        # --- SECTION 2: RESTORE FROM BACKUP ---
        restore_box = ctk.CTkFrame(top, fg_color="#121216", corner_radius=8, border_width=1, border_color="#1e1e24")
        restore_box.pack(fill="both", expand=True, padx=16, pady=(0, 14))

        ctk.CTkLabel(restore_box, text="RESTORE FROM BACKUP", font=("Arial", 12, "bold"), text_color="#E1E1E6").pack(anchor="w", padx=14, pady=(10, 2))
        ctk.CTkLabel(restore_box, text="Select a dated backup and check the specific files you wish to restore.", font=("Arial", 10), text_color="#8E9299").pack(anchor="w", padx=14, pady=(0, 8))

        # Snapshot Selection Dropdown Row
        snap_select_row = ctk.CTkFrame(restore_box, fg_color="transparent")
        snap_select_row.pack(fill="x", padx=14, pady=(0, 8))
        ctk.CTkLabel(snap_select_row, text="Select Snapshot:", font=("Arial", 11, "bold"), text_color="#E1E1E6").pack(side="left", padx=(0, 8))

        snapshot_dropdown_var = ctk.StringVar(value="(No backups found)")
        snapshot_dropdown = ctk.CTkOptionMenu(
            snap_select_row,
            values=["(No backups found)"],
            variable=snapshot_dropdown_var,
            width=320,
            fg_color="#0A0A0E",
            button_color="#005E73",
            button_hover_color="#00D1FF"
        )
        snapshot_dropdown.pack(side="left", fill="x", expand=True)

        # Checkboxes Container for Files
        files_scroll_frame = ctk.CTkScrollableFrame(restore_box, fg_color="#0A0A0E", border_width=1, border_color="#1e1e24", height=180)
        files_scroll_frame.pack(fill="both", expand=True, padx=14, pady=(0, 8))

        checkbox_vars = {}

        # Quick Select Buttons Row
        quick_select_row = ctk.CTkFrame(restore_box, fg_color="transparent")
        quick_select_row.pack(fill="x", padx=14, pady=(0, 8))

        def select_all_files():
            for v in checkbox_vars.values():
                v.set(True)

        def deselect_all_files():
            for v in checkbox_vars.values():
                v.set(False)

        ctk.CTkButton(quick_select_row, text="Select All", width=80, height=24, font=("Arial", 10), fg_color="#2A2A30", hover_color="#3A3A40", command=select_all_files).pack(side="left", padx=(0, 6))
        ctk.CTkButton(quick_select_row, text="Deselect All", width=80, height=24, font=("Arial", 10), fg_color="#2A2A30", hover_color="#3A3A40", command=deselect_all_files).pack(side="left")

        restore_status_lbl = ctk.CTkLabel(restore_box, text="", font=("Arial", 11), text_color="#8E9299")
        restore_status_lbl.pack(anchor="w", padx=14, pady=(0, 4))

        # Helper to parse folder names into friendly display
        def format_snapshot_label(folder_name, is_latest=False):
            try:
                is_safety = folder_name.startswith("Safety_PreRestore_")
                raw_ts = folder_name.replace("Safety_PreRestore_", "").replace("Backup_", "")
                dt = datetime.datetime.strptime(raw_ts, "%Y-%m-%d_%H-%M-%S")
                formatted = dt.strftime("%b %d, %Y - %I:%M:%S %p")
                if is_safety:
                    return f"{formatted} [Safety Copy]"
                elif is_latest:
                    return f"{formatted} (Latest)"
                return formatted
            except Exception:
                return folder_name

        # Mapping between display labels and folder names
        snapshot_map = {}

        def get_available_snapshots():
            backup_base = self.get_backup_directory()
            if not os.path.exists(backup_base):
                return []
            folders = [f for f in os.listdir(backup_base) if os.path.isdir(os.path.join(backup_base, f))]
            # Sort newest first
            folders.sort(reverse=True)
            return folders

        def refresh_dropdown_and_files(selected_folder_target=None):
            folders = get_available_snapshots()
            snapshot_map.clear()

            if not folders:
                snapshot_dropdown_var.set("(No backups found)")
                snapshot_dropdown.configure(values=["(No backups found)"], state="disabled")
                restore_btn.configure(state="disabled")
                for w in files_scroll_frame.winfo_children():
                    w.destroy()
                ctk.CTkLabel(files_scroll_frame, text="No backup snapshots available.", text_color="#8E9299").pack(pady=20)
                return

            labels = []
            for idx, f in enumerate(folders):
                lbl = format_snapshot_label(f, is_latest=(idx == 0))
                snapshot_map[lbl] = f
                labels.append(lbl)

            snapshot_dropdown.configure(values=labels, state="normal")
            restore_btn.configure(state="normal")

            target_label = labels[0]
            if selected_folder_target:
                for l, f in snapshot_map.items():
                    if f == selected_folder_target:
                        target_label = l
                        break

            snapshot_dropdown_var.set(target_label)
            on_snapshot_changed(target_label)

        def on_snapshot_changed(label_value):
            folder_name = snapshot_map.get(label_value)
            for w in files_scroll_frame.winfo_children():
                w.destroy()
            checkbox_vars.clear()

            if not folder_name:
                return

            backup_base = self.get_backup_directory()
            snap_dir = os.path.join(backup_base, folder_name)
            if not os.path.exists(snap_dir):
                ctk.CTkLabel(files_scroll_frame, text="Snapshot folder not found.", text_color="#FF4D4D").pack(pady=20)
                return

            files = [f for f in os.listdir(snap_dir) if f.lower().endswith(".json")]
            files.sort()

            if not files:
                ctk.CTkLabel(files_scroll_frame, text="No JSON files in this snapshot.", text_color="#8E9299").pack(pady=20)
                return

            for fname in files:
                var = ctk.BooleanVar(value=True)
                checkbox_vars[fname] = var
                desc = known_descriptions.get(fname, "Data File")
                
                row = ctk.CTkFrame(files_scroll_frame, fg_color="transparent")
                row.pack(fill="x", padx=4, pady=3)
                
                cb = ctk.CTkCheckBox(
                    row,
                    text=f"{desc}  ({fname})",
                    variable=var,
                    font=("Arial", 11),
                    text_color="#E1E1E6",
                    checkmark_color="#000000",
                    fg_color="#00D1FF",
                    hover_color="#00B4DB"
                )
                cb.pack(side="left")

        snapshot_dropdown.configure(command=on_snapshot_changed)

        # On Backup Click
        def on_backup_click():
            try:
                snap_name, copied = self.create_backup(is_safety=False)
                count = len(copied)
                now_str = datetime.datetime.now().strftime("%I:%M:%S %p")
                backup_status_lbl.configure(
                    text=f"✓ Backup created successfully at {now_str} ({count} files saved)",
                    text_color="#00FF66"
                )
                refresh_dropdown_and_files(selected_folder_target=snap_name)
            except Exception as e:
                backup_status_lbl.configure(text=f"Error creating backup: {e}", text_color="#FF4D4D")

        # On Open Folder Click
        def on_open_folder_click():
            import sys
            import subprocess
            folder = self.get_backup_directory()
            try:
                if sys.platform == "win32":
                    os.startfile(folder)
                elif sys.platform == "darwin":
                    subprocess.Popen(["open", folder])
                else:
                    subprocess.Popen(["xdg-open", folder])
            except Exception as e:
                self.append_system_log(f"\n[System] Could not open backup folder: {e}")

        ctk.CTkButton(
            btn_row,
            text="📦 CREATE NEW BACKUP",
            fg_color="#005E73",
            hover_color="#00D1FF",
            font=("Arial", 11, "bold"),
            height=30,
            command=on_backup_click
        ).pack(side="left", padx=(0, 10))

        ctk.CTkButton(
            btn_row,
            text="📁 OPEN BACKUP FOLDER",
            fg_color="#2A2A30",
            hover_color="#3A3A40",
            font=("Arial", 11, "bold"),
            height=30,
            command=on_open_folder_click
        ).pack(side="left")

        # On Restore Click
        def on_restore_click():
            selected_label = snapshot_dropdown_var.get()
            folder_name = snapshot_map.get(selected_label)
            if not folder_name:
                restore_status_lbl.configure(text="Please select a valid backup snapshot.", text_color="#FF4D4D")
                return

            selected_files = [fname for fname, var in checkbox_vars.items() if var.get()]
            if not selected_files:
                restore_status_lbl.configure(text="Please check at least one file to restore.", text_color="#FF4D4D")
                return

            try:
                # 1. Automatic pre-restore safety snapshot of live files
                safety_snap, _ = self.create_backup(is_safety=True)

                # 2. Copy selected files from backup to app root
                backup_base = self.get_backup_directory()
                src_snap_dir = os.path.join(backup_base, folder_name)

                restored_count = 0
                for fname in selected_files:
                    src_file = os.path.join(src_snap_dir, fname)
                    if os.path.exists(src_file):
                        shutil.copy2(src_file, fname)
                        restored_count += 1

                # 3. Trigger targeted in-memory reloading
                if "settings.json" in selected_files:
                    self.load_settings()
                if "voices.json" in selected_files:
                    self.load_voices()
                    if hasattr(self, "refresh_voices"):
                        self.refresh_voices()
                if "channel_points.json" in selected_files:
                    if hasattr(self, "load_channel_points"):
                        self.load_channel_points()
                if "donations.json" in selected_files:
                    if hasattr(self, "load_donos"):
                        self.load_donos()
                if "timeouts.json" in selected_files:
                    if hasattr(self, "load_timeouts"):
                        self.load_timeouts()
                if "stream_session.json" in selected_files:
                    if hasattr(self, "load_stream_session"):
                        self.load_stream_session()
                if any(k in selected_files for k in ("character_lore.json", "universe_lore.json", "global_rules.json")):
                    if hasattr(self, "roleplay_manager") and self.roleplay_manager:
                        self.roleplay_manager.reload_lore()

                msg = f"✓ Successfully restored {restored_count} file(s) from '{folder_name}'"
                restore_status_lbl.configure(text=msg, text_color="#00FF66")
                self.append_system_log(f"\n[System] Restored {restored_count} file(s) from backup '{folder_name}': {', '.join(selected_files)}. (Safety copy saved as '{safety_snap}').")

                # Refresh dropdown to include the new safety snapshot
                refresh_dropdown_and_files(selected_folder_target=folder_name)
            except Exception as e:
                restore_status_lbl.configure(text=f"Error restoring files: {e}", text_color="#FF4D4D")
                self.append_system_log(f"\n[System Error] Failed to restore backup: {e}")

        restore_btn = ctk.CTkButton(
            restore_box,
            text="🔄 RESTORE SELECTED FILES",
            fg_color="#18565e",
            hover_color="#00D1FF",
            font=("Arial", 12, "bold"),
            height=34,
            command=on_restore_click
        )
        restore_btn.pack(fill="x", padx=14, pady=(4, 12))

        # Initial populate
        refresh_dropdown_and_files()

    def open_voice_mapping(self):
        if hasattr(self, 'voice_mapping_window') and self.voice_mapping_window and self.voice_mapping_window.winfo_exists():
            self.voice_mapping_window.focus()
            return
            
        top = ctk.CTkToplevel(self)
        self.voice_mapping_window = top
        top.title("VOICE MAPPING")
        
        self.center_toplevel(top, 500, 600)
        self.apply_dark_title_bar(top)
        top.minsize(400, 400)
        top.attributes("-topmost", True)
        
        def on_close():
            top.focus_set()
            self.save_settings()
            top.destroy()
            
        top.protocol("WM_DELETE_WINDOW", on_close)
        
        # On/Off switch for Voice Mapping feature
        top_bar = ctk.CTkFrame(top, fg_color="transparent")
        top_bar.pack(fill="x", padx=15, pady=(15, 8))

        vm_enabled_var = ctk.BooleanVar(value=self.settings.get("voice_mapping_enabled", True))

        def on_vm_toggle():
            is_enabled = vm_enabled_var.get()
            self.settings["voice_mapping_enabled"] = is_enabled
            self.save_settings()
            status_label.configure(
                text="ON" if is_enabled else "OFF",
                text_color="#00D1FF" if is_enabled else "#8E9299"
            )

        vm_switch = ctk.CTkSwitch(
            top_bar,
            text="Enable Voice Mapping",
            variable=vm_enabled_var,
            command=on_vm_toggle,
            progress_color="#00D1FF",
            font=("Arial", 12, "bold")
        )
        vm_switch.pack(side="left")

        status_label = ctk.CTkLabel(
            top_bar,
            text="ON" if vm_enabled_var.get() else "OFF",
            font=("Arial", 12, "bold"),
            text_color="#00D1FF" if vm_enabled_var.get() else "#8E9299"
        )
        status_label.pack(side="right")

        # Add new mapping button
        add_btn = ctk.CTkButton(top, text="ADD NEW MAPPING", 
                               fg_color="#005E73", hover_color="#00D1FF",
                               command=lambda: self.prompt_add_mapping(top))
        add_btn.pack(pady=(0, 10), padx=15, fill="x")

        # List frame
        self.mapping_list_frame = ctk.CTkScrollableFrame(top, fg_color="transparent")
        self.mapping_list_frame.pack(fill="both", expand=True, padx=15, pady=5)
        
        self.refresh_mappings_ui()

    def refresh_mappings_ui(self):
        if not hasattr(self, 'mapping_list_frame') or not self.mapping_list_frame.winfo_exists():
            return
            
        # Clear existing
        for widget in self.mapping_list_frame.winfo_children():
            widget.destroy()
            
        mappings = self.settings.get("voice_mappings", [])
        mappings = sorted(mappings, key=lambda x: x.get('username', '').lower())
        
        if not mappings:
            ctk.CTkLabel(self.mapping_list_frame, text="No voice mappings found.", text_color="#8E9299").pack(pady=20)
            return
            
        headers = ctk.CTkFrame(self.mapping_list_frame, fg_color="transparent")
        headers.pack(fill="x", pady=(0, 5))
        
        ctk.CTkLabel(headers, text="Username", font=("Arial", 14, "bold"), text_color="#8E9299", anchor="w", width=120).pack(side="left", padx=10)
        ctk.CTkLabel(headers, text="", width=22).pack(side="left", padx=5)
        ctk.CTkLabel(headers, text="TTS Command", font=("Arial", 14, "bold"), text_color="#8E9299", anchor="w", width=120).pack(side="left", padx=10)
            
        for mapping in mappings:
            rf = ctk.CTkFrame(self.mapping_list_frame, fg_color="#121216", corner_radius=6)
            rf.pack(fill="x", pady=4)
            
            def make_update_callback(m_obj, key, var, refresh=False, save_to_disk=False):
                def on_change(event=None):
                    val = var.get().strip()
                    if save_to_disk and key == 'command' and val and not val.startswith("!"):
                        val = "!" + val
                        var.set(val)
                    m_obj[key] = val
                    if save_to_disk:
                        if refresh and key == 'username':
                            mappings = self.settings.get("voice_mappings", [])
                            mappings = sorted(mappings, key=lambda x: x.get('username', '').lower())
                            self.settings["voice_mappings"] = mappings
                            self.refresh_mappings_ui()
                        self.save_settings()
                return on_change
            
            # Username
            username_var = ctk.StringVar(value=mapping.get('username', ''))
            username_entry = ctk.CTkEntry(rf, textvariable=username_var, font=("Arial", 12, "bold"), text_color="#A970FF", width=120, fg_color="transparent", border_width=1, border_color="#3A3A40")
            username_entry.pack(side="left", padx=10, pady=10)
            username_entry.bind("<KeyRelease>", make_update_callback(mapping, 'username', username_var, refresh=False, save_to_disk=False))
            username_entry.bind("<FocusOut>", make_update_callback(mapping, 'username', username_var, refresh=True, save_to_disk=True))
            username_entry.bind("<Return>", lambda e, w=username_entry: w.master.focus_set())
            
            # Map icon
            ctk.CTkLabel(rf, text="➔", font=("Arial", 16), text_color="#8E9299").pack(side="left", padx=5)
            
            # Command 
            command_var = ctk.StringVar(value=mapping.get('command', ''))
            command_entry = ctk.CTkEntry(rf, textvariable=command_var, font=("Arial", 12, "bold"), text_color="#00D1FF", width=120, fg_color="transparent", border_width=1, border_color="#3A3A40")
            command_entry.pack(side="left", padx=10, pady=10)
            command_entry.bind("<KeyRelease>", make_update_callback(mapping, 'command', command_var, refresh=False, save_to_disk=False))
            command_entry.bind("<FocusOut>", make_update_callback(mapping, 'command', command_var, refresh=False, save_to_disk=True))
            command_entry.bind("<Return>", lambda e, w=command_entry: w.master.focus_set())
            
            self.attach_voice_autocomplete(
                command_entry,
                string_var=command_var,
                on_select=lambda v, m=mapping, cv=command_var: [m.update({'command': v.get('command', '')}), self.save_settings()],
                parent=getattr(self, 'voice_mapping_window', None) or self
            )
            
            # Delete button
            del_btn = ctk.CTkButton(rf, text="X", width=30, height=30, 
                                   fg_color="#B51A21", hover_color="#E03A3E",
                                   command=lambda m=mapping: self.delete_mapping(m))
            del_btn.pack(side="right", padx=10, pady=5)
            
            # Enable switch
            # We pack it after del_btn with side="right", so it appears to the left of del_btn
            is_enabled_var = ctk.BooleanVar(value=mapping.get('enabled', True))
            
            def make_switch_callback(m_obj, var_obj):
                def on_toggle():
                    m_obj['enabled'] = var_obj.get()
                    self.save_settings()
                return on_toggle
                
            enable_switch = ctk.CTkSwitch(rf, text="", variable=is_enabled_var, width=40,
                                          command=make_switch_callback(mapping, is_enabled_var),
                                          onvalue=True, offvalue=False)
            enable_switch.pack(side="right", padx=10, pady=5)

    def delete_mapping(self, mapping):
        mappings = self.settings.get("voice_mappings", [])
        mappings = [m for m in mappings if m != mapping]
        self.settings["voice_mappings"] = mappings
        self.save_settings()
        self.refresh_mappings_ui()

    def prompt_add_mapping(self, parent_window):
        if hasattr(self, '_prompt_window') and self._prompt_window and self._prompt_window.winfo_exists():
            self._prompt_window.focus()
            return

        # Create prompt window
        prompt = ctk.CTkToplevel(parent_window)
        self._prompt_window = prompt
        prompt.title("Add New Mapping")
        
        # Center this prompt over the parent
        self.center_toplevel(prompt, 300, 250, parent=parent_window)
        self.apply_dark_title_bar(prompt)
        prompt.attributes("-topmost", True)
        prompt.grab_set()
        prompt.focus_set()
        
        ctk.CTkLabel(prompt, text="Kick Username:", font=("Arial", 11, "bold")).pack(pady=(15, 0))
        user_var = ctk.StringVar()
        ctk.CTkEntry(prompt, textvariable=user_var, placeholder_text="e.g. user123").pack(pady=5, padx=20, fill="x")
        
        ctk.CTkLabel(prompt, text="Voice Command:", font=("Arial", 11, "bold")).pack(pady=(15, 0))
        cmd_var = ctk.StringVar()
        cmd_entry = ctk.CTkEntry(prompt, textvariable=cmd_var, placeholder_text="e.g. !Bob")
        cmd_entry.pack(pady=5, padx=20, fill="x")

        self.attach_voice_autocomplete(
            cmd_entry,
            string_var=cmd_var,
            parent=prompt
        )
        
        def save_new():
            user = user_var.get().strip()
            cmd = cmd_var.get().strip()
            if user and cmd:
                if not cmd.startswith("!"):
                    cmd = "!" + cmd
                mappings = self.settings.get("voice_mappings", [])
                # Delete any existing mapping for this explicit user (case insensitive comparison)
                mappings = [m for m in mappings if m.get("username", "").lower() != user.lower()]
                mappings.append({"username": user, "command": cmd})
                mappings = sorted(mappings, key=lambda x: x.get('username', '').lower())
                self.settings["voice_mappings"] = mappings
                self.save_settings()
                self.refresh_mappings_ui()
                prompt.destroy()
                
        ctk.CTkButton(prompt, text="ADD MAPPING", fg_color="#2b5329", hover_color="#3b733b", command=save_new).pack(pady=20, padx=20, fill="x")

    def open_channel_points_settings(self):
        if hasattr(self, 'channel_points_window') and self.channel_points_window and self.channel_points_window.winfo_exists():
            self.channel_points_window.focus()
            return

        top = ctk.CTkToplevel(self)
        self.channel_points_window = top
        top.title("CHANNEL POINTS")
        
        self.center_toplevel(top, 540, 620)
        self.apply_dark_title_bar(top)
        top.minsize(440, 450)
        top.attributes("-topmost", True)

        def on_close():
            top.focus_set()
            self.save_channel_points()
            top.destroy()

        top.protocol("WM_DELETE_WINDOW", on_close)

        # Top bar: Master switch and Add Item button
        top_bar = ctk.CTkFrame(top, fg_color="transparent")
        top_bar.pack(fill="x", padx=15, pady=(15, 10))

        enable_points_var = ctk.BooleanVar(value=self.settings.get("enable_points_tts", True))
        def toggle_points_tts():
            self.settings["enable_points_tts"] = enable_points_var.get()
            self.save_settings()

        ctk.CTkSwitch(
            top_bar,
            text="Enable Points TTS",
            variable=enable_points_var,
            command=toggle_points_tts,
            font=("Arial", 12, "bold"),
            progress_color="#00D1FF"
        ).pack(side="left")

        add_btn = ctk.CTkButton(
            top_bar, 
            text="ADD NEW ITEM", 
            fg_color="#005E73", 
            hover_color="#00D1FF",
            font=("Arial", 12, "bold"),
            height=32,
            width=130,
            command=lambda: self.open_channel_points_item_dialog(top)
        )
        add_btn.pack(side="right")

        # Scrollable list frame
        self.points_list_frame = ctk.CTkScrollableFrame(top, fg_color="transparent")
        self.points_list_frame.pack(fill="both", expand=True, padx=15, pady=(0, 10))

        self.refresh_channel_points_ui()

    def refresh_channel_points_ui(self):
        if not hasattr(self, 'points_list_frame') or not self.points_list_frame.winfo_exists():
            return

        # Clear existing
        for widget in self.points_list_frame.winfo_children():
            widget.destroy()

        items = getattr(self, "channel_points", [])
        if not items:
            ctk.CTkLabel(
                self.points_list_frame, 
                text="No Channel Points items configured yet.\nClick 'ADD NEW ITEM' above to create one.",
                text_color="#8E9299",
                font=("Arial", 12)
            ).pack(pady=40)
            return

        top_win = getattr(self, "channel_points_window", None)

        for item in items:
            card = ctk.CTkFrame(self.points_list_frame, fg_color="#18181E", corner_radius=6, border_width=1, border_color="#24242D")
            card.pack(fill="x", pady=6, padx=2)

            # Header row: Name, Voice Command Badge
            head_row = ctk.CTkFrame(card, fg_color="transparent")
            head_row.pack(fill="x", padx=12, pady=(10, 4))

            tts_name = item.get("tts_name", "Untitled Item")
            ctk.CTkLabel(
                head_row, 
                text=tts_name, 
                font=("Arial", 14, "bold"), 
                text_color="#E1E1E6"
            ).pack(side="left")

            voice_cmd = item.get("voice_command", "")
            if voice_cmd:
                cmd_badge = ctk.CTkLabel(
                    head_row,
                    text=voice_cmd,
                    font=("Arial", 11, "bold"),
                    text_color="#00D1FF",
                    fg_color="#0D2230",
                    corner_radius=4,
                    padx=8,
                    pady=2
                )
                cmd_badge.pack(side="left", padx=(10, 0))

            # Message row
            msg_val = item.get("message", "").strip()
            if msg_val:
                ctk.CTkLabel(
                    card,
                    text=f"Message: {msg_val}",
                    font=("Arial", 11),
                    text_color="#A0A0B0",
                    anchor="w",
                    justify="left",
                    wraplength=470
                ).pack(fill="x", padx=12, pady=(2, 4))

            # Action buttons row
            actions = ctk.CTkFrame(card, fg_color="transparent")
            actions.pack(fill="x", padx=12, pady=(4, 8))

            ctk.CTkButton(
                actions,
                text="TEST",
                width=55,
                height=24,
                fg_color="#2B5329",
                hover_color="#3B733B",
                font=("Arial", 10, "bold"),
                command=lambda it=item: self.test_channel_point_item(it)
            ).pack(side="left", padx=(0, 6))

            ctk.CTkButton(
                actions,
                text="EDIT",
                width=55,
                height=24,
                fg_color="#3A3A40",
                hover_color="#55555C",
                font=("Arial", 10, "bold"),
                command=lambda it=item: self.open_channel_points_item_dialog(top_win, item=it)
            ).pack(side="left", padx=(0, 6))

            ctk.CTkButton(
                actions,
                text="DELETE",
                width=65,
                height=24,
                fg_color="#6E2020",
                hover_color="#8B2A2A",
                font=("Arial", 10, "bold"),
                command=lambda it=item: self.delete_channel_point_item(it)
            ).pack(side="left")

    def delete_channel_point_item(self, item):
        self.channel_points = [it for it in self.channel_points if it != item]
        self.save_channel_points()
        self.refresh_channel_points_ui()

    def open_channel_points_item_dialog(self, parent_window, item=None):
        if hasattr(self, '_cp_dialog') and self._cp_dialog and self._cp_dialog.winfo_exists():
            self._cp_dialog.focus()
            return

        dialog = ctk.CTkToplevel(parent_window or self)
        self._cp_dialog = dialog
        dialog.title("Edit Channel Point Item" if item else "Add Channel Point Item")

        self.center_toplevel(dialog, 480, 660, parent=parent_window or self)
        dialog.minsize(440, 580)
        self.apply_dark_title_bar(dialog)
        dialog.attributes("-topmost", True)
        dialog.grab_set()
        dialog.focus_set()

        content = ctk.CTkFrame(dialog, fg_color="transparent")
        content.pack(fill="both", expand=True, padx=20, pady=15)

        # Top section: Name and Message (fixed height)
        top_frame = ctk.CTkFrame(content, fg_color="transparent")
        top_frame.pack(side="top", fill="x")

        # 1. Channel Points Name
        ctk.CTkLabel(top_frame, text="Channel Points Name:", font=("Arial", 11, "bold"), anchor="w").pack(fill="x", pady=(0, 2))
        ctk.CTkLabel(top_frame, text="(Trigger phrase redeemed on Kick, e.g. Kiss)", font=("Arial", 9), text_color="#8E9299", anchor="w").pack(fill="x", pady=(0, 3))
        name_var = ctk.StringVar(value=item.get("tts_name", "") if item else "")
        name_entry = ctk.CTkEntry(top_frame, textvariable=name_var, placeholder_text="e.g. Kiss", font=("Arial", 13))
        name_entry.pack(fill="x", pady=(0, 10))

        # 2. Message
        ctk.CTkLabel(top_frame, text="Message:", font=("Arial", 11, "bold"), anchor="w").pack(fill="x", pady=(0, 2))
        ctk.CTkLabel(top_frame, text="(Base TTS text. Supports [user] and #chat)", font=("Arial", 9), text_color="#8E9299", anchor="w").pack(fill="x", pady=(0, 3))
        msg_var = ctk.StringVar(value=item.get("message", "") if item else "")
        msg_entry = ctk.CTkEntry(top_frame, textvariable=msg_var, placeholder_text="e.g. [user] just gave #chat a kiss.", font=("Arial", 13))
        msg_entry.pack(fill="x", pady=(0, 10))

        # Bottom section: Voice Command and Save/Cancel buttons (pinned to bottom)
        bottom_frame = ctk.CTkFrame(content, fg_color="transparent")
        bottom_frame.pack(side="bottom", fill="x")

        # 4. Voice Command
        ctk.CTkLabel(bottom_frame, text="Voice Command:", font=("Arial", 11, "bold"), anchor="w").pack(fill="x", pady=(0, 2))
        ctk.CTkLabel(bottom_frame, text="(Voice command to speak the TTS, e.g. !bob)", font=("Arial", 9), text_color="#8E9299", anchor="w").pack(fill="x", pady=(0, 3))
        cmd_var = ctk.StringVar(value=item.get("voice_command", "") if item else "")
        cmd_entry = ctk.CTkEntry(bottom_frame, textvariable=cmd_var, placeholder_text="e.g. !bob", font=("Arial", 13))
        cmd_entry.pack(fill="x", pady=(0, 15))

        self.attach_voice_autocomplete(
            cmd_entry,
            string_var=cmd_var,
            parent=dialog
        )

        btn_row = ctk.CTkFrame(bottom_frame, fg_color="transparent")
        btn_row.pack(fill="x", pady=(0, 0))

        # Middle section: Random Message List (expands vertically when dialog is resized taller)
        mid_frame = ctk.CTkFrame(content, fg_color="transparent")
        mid_frame.pack(side="top", fill="both", expand=True)

        # 3. Random Message List
        ctk.CTkLabel(mid_frame, text="Random Message List:", font=("Arial", 11, "bold"), anchor="w").pack(fill="x", pady=(0, 2))
        ctk.CTkLabel(mid_frame, text="(One message per line. Supports #chat, #chat1, etc.)", font=("Arial", 9), text_color="#8E9299", anchor="w").pack(fill="x", pady=(0, 3))
        rand_box = ctk.CTkTextbox(mid_frame, font=("Arial", 13), wrap="word", fg_color="#121216", border_width=1, border_color="#2A2A35")
        rand_box.pack(fill="both", expand=True, pady=(0, 10))
        if item and item.get("random_messages"):
            raw_rand = item.get("random_messages", "")
            if "\n" not in raw_rand and "," in raw_rand:
                raw_rand = "\n".join([m.strip() for m in raw_rand.split(",") if m.strip()])
            rand_box.insert("1.0", raw_rand)

        def save_item():
            t_name = name_var.get().strip()
            t_msg = msg_var.get().strip()
            v_cmd = cmd_var.get().strip()
            if not (t_name and t_msg and v_cmd):
                return

            if not v_cmd.startswith("!"):
                v_cmd = "!" + v_cmd

            t_rand = rand_box.get("1.0", "end-1c").strip()

            if item is not None:
                item["tts_name"] = t_name
                item["message"] = t_msg
                item["random_messages"] = t_rand
                item["voice_command"] = v_cmd
            else:
                import uuid
                new_item = {
                    "id": str(uuid.uuid4()),
                    "tts_name": t_name,
                    "message": t_msg,
                    "random_messages": t_rand,
                    "voice_command": v_cmd
                }
                self.channel_points.append(new_item)

            self.save_channel_points()
            self.refresh_channel_points_ui()
            dialog.destroy()

        save_btn = ctk.CTkButton(
            btn_row, 
            text="SAVE ITEM", 
            fg_color="#005E73", 
            hover_color="#00D1FF", 
            font=("Arial", 11, "bold"),
            height=30,
            command=save_item
        )
        save_btn.pack(side="left", expand=True, fill="x", padx=(0, 5))

        def update_save_button_state(*args):
            has_name = bool(name_var.get().strip())
            has_msg = bool(msg_var.get().strip())
            has_cmd = bool(cmd_var.get().strip())
            if has_name and has_msg and has_cmd:
                save_btn.configure(state="normal", fg_color="#005E73", hover_color="#00D1FF", text_color="#FFFFFF")
            else:
                save_btn.configure(state="disabled", fg_color="#1E282E", hover_color="#1E282E", text_color="#5D686E")

        name_var.trace_add("write", update_save_button_state)
        msg_var.trace_add("write", update_save_button_state)
        cmd_var.trace_add("write", update_save_button_state)
        name_entry.bind("<KeyRelease>", update_save_button_state)
        msg_entry.bind("<KeyRelease>", update_save_button_state)
        cmd_entry.bind("<KeyRelease>", update_save_button_state)

        # Initialize button state
        update_save_button_state()

        ctk.CTkButton(
            btn_row, 
            text="CANCEL", 
            fg_color="#3A3A40", 
            hover_color="#55555C", 
            font=("Arial", 11, "bold"),
            height=30,
            command=dialog.destroy
        ).pack(side="right", expand=True, fill="x", padx=(5, 0))

    def test_channel_point_item(self, item):
        test_user = self.settings.get("kick_username") or "Viewer"
        tts_name = item.get("tts_name", "")
        # Temporarily clear debounce key so test always fires
        now_ts = datetime.datetime.now().timestamp()
        cp_key = f"{test_user.lower()}::{tts_name.lower()}"
        if hasattr(self, '_processed_cp_keys') and cp_key in self._processed_cp_keys:
            del self._processed_cp_keys[cp_key]
        self.process_channel_point_redemption(test_user, tts_name, is_test=True)

    def process_channel_point_redemption(self, user, reward_name, event_id="", is_test=False):
        if not user or not reward_name:
            return False

        if not is_test and not self.settings.get("enable_points_tts", True):
            return False

        clean_user = str(user).strip().lstrip('@')
        clean_reward = str(reward_name).strip().strip('"\'!.: ')

        if not hasattr(self, '_processed_cp_keys'):
            self._processed_cp_keys = {}
        now_ts = datetime.datetime.now().timestamp()

        # Clean old keys
        if len(self._processed_cp_keys) > 2000:
            for k in list(self._processed_cp_keys.keys())[:500]:
                del self._processed_cp_keys[k]

        # Use a short debounce window (1.0s) strictly to suppress duplicate WebSocket frame echoes
        # across multi-channel subscriptions or simultaneous chat mirrors of the exact same redemption click.
        # Different redemptions of the same channel point item (>= 1.0s apart) will NOT be ignored.
        cp_key = f"{clean_user.lower()}::{clean_reward.lower()}"
        if not is_test:
            if cp_key in self._processed_cp_keys and (now_ts - self._processed_cp_keys[cp_key] < 1.0):
                return False

            if event_id:
                ev_key = f"cp_ev::{event_id}"
                if ev_key in self._processed_cp_keys and (now_ts - self._processed_cp_keys[ev_key] < 1.0):
                    return False
                self._processed_cp_keys[ev_key] = now_ts

        # Match item in self.channel_points (case insensitive)
        matched_item = None
        for it in getattr(self, "channel_points", []):
            it_name = str(it.get("tts_name", "")).strip()
            if not it_name:
                continue
            if (
                clean_reward.lower() == it_name.lower()
                or clean_reward.lower().strip('"\'!.:[]() ') == it_name.lower()
                or clean_reward.lower().startswith(it_name.lower() + ":")
                or clean_reward.lower().startswith(it_name.lower() + " -")
                or clean_reward.lower().startswith(it_name.lower() + " |")
                or it_name.lower() in clean_reward.lower()
            ):
                matched_item = it
                break

        if not matched_item:
            return False

        if not is_test:
            self._processed_cp_keys[cp_key] = now_ts

        msg_template = str(matched_item.get("message", "")).strip()
        rand_raw = str(matched_item.get("random_messages", "")).strip()
        if "\n" in rand_raw:
            rand_list = [m.strip() for m in rand_raw.splitlines() if m.strip()]
        else:
            rand_list = [m.strip() for m in rand_raw.split(",") if m.strip()]
        import random
        chosen_rand = random.choice(rand_list) if rand_list else ""

        # Combine msg_template and chosen_rand without automatically adding a period
        if msg_template and chosen_rand:
            combined = f"{msg_template.rstrip()} {chosen_rand.lstrip()}"
        elif msg_template:
            combined = msg_template
        else:
            combined = chosen_rand

        # 1. Substitute [user] / {user} (translated for TTS: 0 -> o, 3 -> e)
        tts_user = self.translate_username(clean_user)
        combined = re.sub(r'\[user\]|\{user\}', tts_user, combined, flags=re.IGNORECASE)

        # 2. Substitute #chat, #chat1, #chat2, etc.
        raw_tags = re.findall(r'#chat\d*', combined, flags=re.IGNORECASE)
        unique_tags = []
        for t in raw_tags:
            t_lower = t.lower()
            if t_lower not in unique_tags:
                unique_tags.append(t_lower)

        raw_chatters = []
        if unique_tags:
            raw_chatters = self.get_active_chatters_list() if hasattr(self, 'get_active_chatters_list') else []
            # Exclude bots and redeemer
            excluded_set = {"kickbot", "botrix", clean_user.lower(), tts_user.lower()}
            pool = [c for c in raw_chatters if c and c.strip().lstrip('@').lower() not in excluded_set]
            random.shuffle(pool)

            tag_assignments = {}
            for t_lower in unique_tags:
                if pool:
                    chatter_name = pool.pop(0)
                    tag_assignments[t_lower] = self.translate_username(chatter_name)
                else:
                    tag_assignments[t_lower] = "chat"

            # Replace in descending order of length so #chat1 is not partially replaced by #chat
            for t_lower in sorted(unique_tags, key=lambda x: len(x), reverse=True):
                assigned_val = tag_assignments[t_lower]
                pattern = re.escape(t_lower) + r'(?!\d)'
                combined = re.sub(pattern, assigned_val, combined, flags=re.IGNORECASE)

        # Translate any remaining usernames or @mentions in the combined text for TTS
        tts_active_list = raw_chatters if raw_chatters else (self.get_active_chatters_list() if hasattr(self, 'get_active_chatters_list') else [])
        combined = self.translate_usernames_in_text(combined, tts_active_list)

        final_msg = re.sub(r'[ \t]+', ' ', combined).strip()
        if not final_msg:
            return False

        voice_cmd = str(matched_item.get("voice_command", "")).strip()
        if voice_cmd and not voice_cmd.startswith("!"):
            voice_cmd = "!" + voice_cmd
        if not voice_cmd:
            voice_cmd = "!bob"

        tts_name = matched_item.get("tts_name", "Item")
        self.after(0, self.append_system_log, f"\n[Points] {clean_user} redeemed '{tts_name}' -> TTS ({voice_cmd}): {final_msg}\n")
        self.after(0, self.append_chat_message, clean_user, f"has redeemed {tts_name} | {final_msg}", voice_cmd, True)

        self.tts.generate_and_play(voice_cmd, final_msg, self.on_chat_message, category="POINTS", user=tts_user)
        return True

    def _hide_quick_voice_dropdown(self, event=None):
        if hasattr(self, '_quick_voice_dropdown') and self._quick_voice_dropdown:
            try:
                self._quick_voice_dropdown.destroy()
            except Exception:
                pass
            self._quick_voice_dropdown = None
            self._quick_voice_matches = []

    def _on_quick_voice_search_changed(self, *args):
        if not hasattr(self, "voice_quick_search_var"):
            return
        query = self.voice_quick_search_var.get().strip()
        if not query:
            self._hide_quick_voice_dropdown()
            return

        clean_q = query.lower().lstrip("!")
        q_lower = query.lower()

        matches = []
        for v in getattr(self, "voices", []):
            cmd = v.get("command", "").lower()
            name = v.get("name", "").lower()
            if (q_lower in cmd) or (q_lower in name) or (clean_q and clean_q in cmd.lstrip("!")) or (clean_q and clean_q in name):
                matches.append(v)

        if not matches:
            self._hide_quick_voice_dropdown()
            return

        self._quick_voice_matches = matches
        self._show_quick_voice_dropdown(matches)

    def _show_quick_voice_dropdown(self, matches):
        entry = getattr(self, "voice_quick_search_entry", None)
        if not entry or not entry.winfo_exists():
            return

        entry.update_idletasks()
        ex = entry.winfo_rootx()
        ey = entry.winfo_rooty() + entry.winfo_height() + 2
        ew = max(entry.winfo_width(), 260)

        import tkinter as tk
        if not hasattr(self, '_quick_voice_dropdown') or not self._quick_voice_dropdown or not self._quick_voice_dropdown.winfo_exists():
            top = tk.Toplevel(self)
            top.overrideredirect(True)
            top.attributes("-topmost", True)
            self._quick_voice_dropdown = top
        else:
            top = self._quick_voice_dropdown

        for w in top.winfo_children():
            w.destroy()

        item_count = min(len(matches), 7)
        list_h = item_count * 24 + 4
        top.geometry(f"{ew}x{list_h}+{ex}+{ey}")

        list_frame = tk.Frame(top, bg="#18181E", bd=1, relief="solid")
        list_frame.pack(fill="both", expand=True)

        listbox = tk.Listbox(
            list_frame,
            bg="#18181E",
            fg="#E1E1E6",
            selectbackground="#005E73",
            selectforeground="#FFFFFF",
            activestyle="none",
            bd=0,
            highlightthickness=0,
            font=("Arial", 10),
            exportselection=False
        )
        if len(matches) > 7:
            sb = tk.Scrollbar(list_frame, orient="vertical", command=listbox.yview)
            sb.pack(side="right", fill="y")
            listbox.configure(yscrollcommand=sb.set)
        listbox.pack(side="left", fill="both", expand=True)
        self._quick_voice_listbox = listbox

        for v in matches:
            listbox.insert("end", f" {v.get('command', '')}  —  {v.get('name', '')}")

        listbox.selection_set(0)
        listbox.activate(0)

        def on_click(event):
            idx = listbox.nearest(event.y)
            if 0 <= idx < len(self._quick_voice_matches):
                chosen = self._quick_voice_matches[idx]
                self._hide_quick_voice_dropdown()
                if hasattr(self, "voice_quick_search_var"):
                    self.voice_quick_search_var.set("")
                self.open_voice_settings(chosen['command'], show_switches=True)

        listbox.bind("<ButtonRelease-1>", on_click)

    def _on_quick_voice_search_down(self, event=None):
        if hasattr(self, '_quick_voice_listbox') and self._quick_voice_listbox and self._quick_voice_listbox.winfo_exists():
            lb = self._quick_voice_listbox
            curr = lb.curselection()
            idx = (curr[0] + 1) if curr else 0
            if idx < lb.size():
                lb.selection_clear(0, "end")
                lb.selection_set(idx)
                lb.activate(idx)
                lb.see(idx)
            return "break"

    def _on_quick_voice_search_up(self, event=None):
        if hasattr(self, '_quick_voice_listbox') and self._quick_voice_listbox and self._quick_voice_listbox.winfo_exists():
            lb = self._quick_voice_listbox
            curr = lb.curselection()
            idx = (curr[0] - 1) if curr else 0
            if idx >= 0:
                lb.selection_clear(0, "end")
                lb.selection_set(idx)
                lb.activate(idx)
                lb.see(idx)
            return "break"

    def _on_quick_voice_search_enter(self, event=None):
        if hasattr(self, '_quick_voice_listbox') and self._quick_voice_listbox and self._quick_voice_listbox.winfo_exists():
            lb = self._quick_voice_listbox
            sel = lb.curselection()
            idx = sel[0] if sel else 0
            if hasattr(self, '_quick_voice_matches') and 0 <= idx < len(self._quick_voice_matches):
                chosen = self._quick_voice_matches[idx]
                self._hide_quick_voice_dropdown()
                if hasattr(self, "voice_quick_search_var"):
                    self.voice_quick_search_var.set("")
                self.open_voice_settings(chosen['command'], show_switches=True)
                return "break"

    def _on_quick_voice_search_focus_out(self, event=None):
        self.after(250, self._hide_quick_voice_dropdown)

    def _on_root_click_check_dropdown(self, event=None):
        if hasattr(self, '_quick_voice_dropdown') and self._quick_voice_dropdown and self._quick_voice_dropdown.winfo_exists():
            try:
                x, y = event.x_root, event.y_root
                top = self._quick_voice_dropdown
                entry = getattr(self, "voice_quick_search_entry", None)
                if entry and not (top.winfo_rootx() <= x <= top.winfo_rootx() + top.winfo_width() and
                        top.winfo_rooty() <= y <= top.winfo_rooty() + top.winfo_height()) and \
                   not (entry.winfo_rootx() <= x <= entry.winfo_rootx() + entry.winfo_width() and
                        entry.winfo_rooty() <= y <= entry.winfo_rooty() + entry.winfo_height()):
                    self._hide_quick_voice_dropdown()
            except Exception:
                pass

        if hasattr(self, '_manual_cmd_dropdown') and self._manual_cmd_dropdown and self._manual_cmd_dropdown.winfo_exists():
            try:
                x, y = event.x_root, event.y_root
                top = self._manual_cmd_dropdown
                entry = getattr(self, "manual_cmd_entry", None)
                if entry and not (top.winfo_rootx() <= x <= top.winfo_rootx() + top.winfo_width() and
                        top.winfo_rooty() <= y <= top.winfo_rooty() + top.winfo_height()) and \
                   not (entry.winfo_rootx() <= x <= entry.winfo_rootx() + entry.winfo_width() and
                        entry.winfo_rooty() <= y <= entry.winfo_rooty() + entry.winfo_height()):
                    self._hide_manual_cmd_dropdown()
            except Exception:
                pass

    def attach_voice_autocomplete(self, entry_widget, string_var=None, on_select=None, min_width=220, parent=None):
        """
        Attaches voice command autocomplete dropdown to any CTkEntry or tk.Entry.
        As user types into the textbox, displays matching voices (!command — Description).
        Properly supports Up/Down arrow navigation, Enter selection, and prevents selection resets.
        """
        import tkinter as tk
        state = {
            "dropdown": None,
            "listbox": None,
            "matches": [],
            "suppress": False,
            "last_query": None
        }

        def hide_dropdown(event=None):
            if state["dropdown"] and state["dropdown"].winfo_exists():
                try:
                    state["dropdown"].destroy()
                except Exception:
                    pass
            state["dropdown"] = None
            state["listbox"] = None
            state["matches"] = []
            state["last_query"] = None

        def select_voice(voice):
            cmd = voice.get("command", "")
            state["suppress"] = True
            state["last_query"] = cmd
            if string_var:
                string_var.set(cmd)
            elif entry_widget.winfo_exists():
                entry_widget.delete(0, "end")
                entry_widget.insert(0, cmd)
            if on_select:
                try:
                    on_select(voice)
                except Exception:
                    pass
            hide_dropdown()
            if entry_widget.winfo_exists():
                entry_widget.focus_set()

        def on_search_changed(event=None, force=False):
            if state["suppress"]:
                state["suppress"] = False
                return
            if not entry_widget.winfo_exists():
                hide_dropdown()
                return

            # Ignore non-character navigation/modifier keys
            if event is not None and getattr(event, "keysym", None) in (
                "Down", "Up", "Return", "KP_Enter", "Escape", "Tab", "ISO_Left_Tab",
                "Left", "Right", "Home", "End", "Shift_L", "Shift_R",
                "Control_L", "Control_R", "Alt_L", "Alt_R", "Caps_Lock",
                "Next", "Prior", "Page_Up", "Page_Down"
            ):
                return

            query = (string_var.get() if string_var else entry_widget.get()).strip()
            if not query:
                state["last_query"] = ""
                hide_dropdown()
                return

            # Prevent reset if search string hasn't changed and dropdown is already open
            if not force and query == state.get("last_query") and state.get("dropdown") and state["dropdown"].winfo_exists():
                return

            state["last_query"] = query

            clean_q = query.lower().lstrip("!")
            q_lower = query.lower()

            matches = []
            for v in getattr(self, "voices", []):
                cmd = v.get("command", "").lower()
                name = v.get("name", "").lower()
                desc = v.get("description", "").lower()
                if (q_lower in cmd) or (q_lower in name) or (q_lower in desc) or (clean_q and clean_q in cmd.lstrip("!")) or (clean_q and clean_q in name):
                    matches.append(v)

            if not matches:
                hide_dropdown()
                return

            state["matches"] = matches
            show_dropdown(matches)

        def show_dropdown(matches):
            if not entry_widget.winfo_exists():
                return
            entry_widget.update_idletasks()
            ex = entry_widget.winfo_rootx()
            ey = entry_widget.winfo_rooty() + entry_widget.winfo_height() + 2
            ew = max(entry_widget.winfo_width(), min_width)

            parent_win = parent or entry_widget.winfo_toplevel()
            if not state["dropdown"] or not state["dropdown"].winfo_exists():
                top = tk.Toplevel(parent_win)
                top.overrideredirect(True)
                top.attributes("-topmost", True)
                state["dropdown"] = top
            else:
                top = state["dropdown"]

            for w in top.winfo_children():
                w.destroy()

            item_count = min(len(matches), 7)
            list_h = item_count * 24 + 4
            top.geometry(f"{ew}x{list_h}+{ex}+{ey}")

            list_frame = tk.Frame(top, bg="#18181E", bd=1, relief="solid")
            list_frame.pack(fill="both", expand=True)

            listbox = tk.Listbox(
                list_frame,
                bg="#18181E",
                fg="#E1E1E6",
                selectbackground="#005E73",
                selectforeground="#FFFFFF",
                activestyle="none",
                bd=0,
                highlightthickness=0,
                font=("Arial", 10),
                exportselection=False
            )
            if len(matches) > 7:
                sb = tk.Scrollbar(list_frame, orient="vertical", command=listbox.yview)
                sb.pack(side="right", fill="y")
                listbox.configure(yscrollcommand=sb.set)
            listbox.pack(side="left", fill="both", expand=True)
            state["listbox"] = listbox

            for v in matches:
                listbox.insert("end", f" {v.get('command', '')}  —  {v.get('name', '')}")

            listbox.selection_set(0)
            listbox.activate(0)

            def on_list_click(event):
                idx = listbox.nearest(event.y)
                if 0 <= idx < len(state["matches"]):
                    chosen = state["matches"][idx]
                    select_voice(chosen)

            listbox.bind("<ButtonRelease-1>", on_list_click)

        def on_key_down(event=None):
            lb = state.get("listbox")
            if lb and lb.winfo_exists() and state.get("dropdown") and state["dropdown"].winfo_exists():
                curr = lb.curselection()
                idx = (curr[0] + 1) if curr else 0
                if idx < lb.size():
                    lb.selection_clear(0, "end")
                    lb.selection_set(idx)
                    lb.activate(idx)
                    lb.see(idx)
                return "break"
            else:
                on_search_changed(force=True)
                return "break"

        def on_key_up(event=None):
            lb = state.get("listbox")
            if lb and lb.winfo_exists() and state.get("dropdown") and state["dropdown"].winfo_exists():
                curr = lb.curselection()
                idx = (curr[0] - 1) if curr else 0
                if idx >= 0:
                    lb.selection_clear(0, "end")
                    lb.selection_set(idx)
                    lb.activate(idx)
                    lb.see(idx)
                return "break"

        def on_key_enter(event=None):
            lb = state.get("listbox")
            if lb and lb.winfo_exists() and state.get("dropdown") and state["dropdown"].winfo_exists():
                sel = lb.curselection()
                idx = sel[0] if sel else 0
                if 0 <= idx < len(state.get("matches", [])):
                    chosen = state["matches"][idx]
                    select_voice(chosen)
                    return "break"

        def on_key_tab(event=None):
            lb = state.get("listbox")
            if lb and lb.winfo_exists() and state.get("dropdown") and state["dropdown"].winfo_exists():
                sel = lb.curselection()
                idx = sel[0] if sel else 0
                if 0 <= idx < len(state.get("matches", [])):
                    chosen = state["matches"][idx]
                    select_voice(chosen)
                    return "break"

        def on_focus_out(event=None):
            self.after(250, hide_dropdown)

        def on_check_click_outside(event=None):
            if state.get("dropdown") and state["dropdown"].winfo_exists() and entry_widget.winfo_exists():
                try:
                    x, y = event.x_root, event.y_root
                    top = state["dropdown"]
                    if not (top.winfo_rootx() <= x <= top.winfo_rootx() + top.winfo_width() and
                            top.winfo_rooty() <= y <= top.winfo_rooty() + top.winfo_height()) and \
                       not (entry_widget.winfo_rootx() <= x <= entry_widget.winfo_rootx() + entry_widget.winfo_width() and
                            entry_widget.winfo_rooty() <= y <= entry_widget.winfo_rooty() + entry_widget.winfo_height()):
                        hide_dropdown()
                except Exception:
                    pass

        if string_var:
            string_var.trace_add("write", lambda *a: on_search_changed())
        
        entry_widget.bind("<KeyRelease>", on_search_changed, add="+")
        entry_widget.bind("<Down>", on_key_down, add="+")
        entry_widget.bind("<Up>", on_key_up, add="+")
        entry_widget.bind("<Return>", on_key_enter, add="+")
        entry_widget.bind("<KP_Enter>", on_key_enter, add="+")
        entry_widget.bind("<Tab>", on_key_tab, add="+")
        entry_widget.bind("<Escape>", hide_dropdown, add="+")
        entry_widget.bind("<FocusOut>", on_focus_out, add="+")
        entry_widget.bind("<Destroy>", hide_dropdown, add="+")
        if parent:
            parent.bind("<Destroy>", hide_dropdown, add="+")
            try:
                parent.bind("<Button-1>", on_check_click_outside, add="+")
            except Exception:
                pass

    def _hide_manual_cmd_dropdown(self, event=None):
        if hasattr(self, '_manual_cmd_dropdown') and self._manual_cmd_dropdown:
            try:
                self._manual_cmd_dropdown.destroy()
            except Exception:
                pass
            self._manual_cmd_dropdown = None
            self._manual_cmd_matches = []

    def _on_manual_cmd_search_changed(self, *args):
        if getattr(self, "_suppress_manual_cmd_dropdown", False):
            self._suppress_manual_cmd_dropdown = False
            return
        if not hasattr(self, "manual_cmd_var"):
            return
        query = self.manual_cmd_var.get().strip()
        if not query:
            self._hide_manual_cmd_dropdown()
            return

        clean_q = query.lower().lstrip("!")
        q_lower = query.lower()

        matches = []
        for v in getattr(self, "voices", []):
            cmd = v.get("command", "").lower()
            name = v.get("name", "").lower()
            if (q_lower in cmd) or (q_lower in name) or (clean_q and clean_q in cmd.lstrip("!")) or (clean_q and clean_q in name):
                matches.append(v)

        if not matches:
            self._hide_manual_cmd_dropdown()
            return

        self._manual_cmd_matches = matches
        self._show_manual_cmd_dropdown(matches)

    def _show_manual_cmd_dropdown(self, matches):
        entry = getattr(self, "manual_cmd_entry", None)
        if not entry or not entry.winfo_exists():
            return

        entry.update_idletasks()
        ex = entry.winfo_rootx()
        ey = entry.winfo_rooty() + entry.winfo_height() + 2
        ew = max(entry.winfo_width(), 260)

        import tkinter as tk
        if not hasattr(self, '_manual_cmd_dropdown') or not self._manual_cmd_dropdown or not self._manual_cmd_dropdown.winfo_exists():
            top = tk.Toplevel(self)
            top.overrideredirect(True)
            top.attributes("-topmost", True)
            self._manual_cmd_dropdown = top
        else:
            top = self._manual_cmd_dropdown

        for w in top.winfo_children():
            w.destroy()

        item_count = min(len(matches), 7)
        list_h = item_count * 24 + 4
        top.geometry(f"{ew}x{list_h}+{ex}+{ey}")

        list_frame = tk.Frame(top, bg="#18181E", bd=1, relief="solid")
        list_frame.pack(fill="both", expand=True)

        listbox = tk.Listbox(
            list_frame,
            bg="#18181E",
            fg="#E1E1E6",
            selectbackground="#005E73",
            selectforeground="#FFFFFF",
            activestyle="none",
            bd=0,
            highlightthickness=0,
            font=("Arial", 10),
            exportselection=False
        )
        if len(matches) > 7:
            sb = tk.Scrollbar(list_frame, orient="vertical", command=listbox.yview)
            sb.pack(side="right", fill="y")
            listbox.configure(yscrollcommand=sb.set)
        listbox.pack(side="left", fill="both", expand=True)
        self._manual_cmd_listbox = listbox

        for v in matches:
            listbox.insert("end", f" {v.get('command', '')}  —  {v.get('name', '')}")

        listbox.selection_set(0)
        listbox.activate(0)

        def on_click(event):
            idx = listbox.nearest(event.y)
            if 0 <= idx < len(self._manual_cmd_matches):
                chosen = self._manual_cmd_matches[idx]
                self._select_manual_cmd_voice(chosen)

        listbox.bind("<ButtonRelease-1>", on_click)

    def _select_manual_cmd_voice(self, voice):
        cmd = voice.get("command", "")
        self._suppress_manual_cmd_dropdown = True
        self.manual_cmd_var.set(cmd)
        self.settings["manual_tts_command"] = cmd
        self.save_settings()
        self._hide_manual_cmd_dropdown()
        if hasattr(self, "manual_cmd_entry") and self.manual_cmd_entry and self.manual_cmd_entry.winfo_exists():
            self.manual_cmd_entry.focus_set()

    def _on_manual_cmd_down(self, event=None):
        if hasattr(self, '_manual_cmd_listbox') and self._manual_cmd_listbox and self._manual_cmd_listbox.winfo_exists():
            lb = self._manual_cmd_listbox
            curr = lb.curselection()
            idx = (curr[0] + 1) if curr else 0
            if idx < lb.size():
                lb.selection_clear(0, "end")
                lb.selection_set(idx)
                lb.activate(idx)
                lb.see(idx)
            return "break"

    def _on_manual_cmd_up(self, event=None):
        if hasattr(self, '_manual_cmd_listbox') and self._manual_cmd_listbox and self._manual_cmd_listbox.winfo_exists():
            lb = self._manual_cmd_listbox
            curr = lb.curselection()
            idx = (curr[0] - 1) if curr else 0
            if idx >= 0:
                lb.selection_clear(0, "end")
                lb.selection_set(idx)
                lb.activate(idx)
                lb.see(idx)
            return "break"

    def _on_manual_cmd_enter(self, event=None):
        if hasattr(self, '_manual_cmd_listbox') and self._manual_cmd_listbox and self._manual_cmd_listbox.winfo_exists():
            lb = self._manual_cmd_listbox
            sel = lb.curselection()
            idx = sel[0] if sel else 0
            if hasattr(self, '_manual_cmd_matches') and 0 <= idx < len(self._manual_cmd_matches):
                chosen = self._manual_cmd_matches[idx]
                self._select_manual_cmd_voice(chosen)
                return "break"

    def _on_manual_cmd_focus_out(self, event=None):
        self.after(250, self._hide_manual_cmd_dropdown)

    def open_voice_settings(self, command, show_switches=False):
        if hasattr(self, 'open_settings_windows') and command in self.open_settings_windows:
            existing_top = self.open_settings_windows[command]
            if existing_top.winfo_exists():
                existing_top.focus()  # Brings to front
                return
            else:
                del self.open_settings_windows[command]

        voice = next((v for v in self.voices if v['command'] == command), None)
        if not voice:
            return
            
        temp_val = voice.get("temperature", 0.5)
        
        top = ctk.CTkToplevel(self)
        if hasattr(self, 'open_settings_windows'):
            self.open_settings_windows[command] = top
            
        def on_close():
            if hasattr(self, 'open_settings_windows') and command in self.open_settings_windows:
                del self.open_settings_windows[command]
            top.destroy()
            
        top.protocol("WM_DELETE_WINDOW", on_close)

        top.title(f"Settings: {command}")
        
        # Center window over app
        win_w = 380 if show_switches else 360
        win_h = 270 if show_switches else 205
        self.center_toplevel(top, win_w, win_h)
        top.minsize(win_w - 20, win_h - 15)
        top.resizable(False, False)
        self.apply_dark_title_bar(top)
        
        top.attributes("-topmost", True)
        
        header_lbl = ctk.CTkLabel(top, text=f"{voice['command']} ({voice.get('name', 'Voice')})", font=("Arial", 14, "bold"), text_color="#00D1FF")
        header_lbl.pack(pady=(14, 8))

        # Temperature section
        temp_frame = ctk.CTkFrame(top, fg_color="transparent")
        temp_frame.pack(fill="x", padx=20, pady=(0, 8))
        temp_label = ctk.CTkLabel(temp_frame, text=f"Temperature: {temp_val:.2f}", font=("Arial", 12, "bold"))
        temp_label.pack(anchor="w")
        
        def update_temp(val):
            temp_label.configure(text=f"Temperature: {val:.2f}")
            voice["temperature"] = round(float(val), 2)
            if "path" in voice:
                self.tts.add_voice(command, voice["path"], temperature=voice["temperature"])
            self.save_voices()
            
        temp_slider = ctk.CTkSlider(temp_frame, from_=0.0, to=1.0, command=update_temp)
        temp_slider.set(temp_val)
        temp_slider.pack(fill="x", pady=4)
        
        # Temperature Description
        ctk.CTkLabel(temp_frame, text="Higher = More variation and creativity.\nLower = More consistent, predictable output", 
                     text_color="#8E9299", font=("Arial", 10), justify="left").pack(anchor="w")

        # Test Voice button row
        btn_frame = ctk.CTkFrame(top, fg_color="transparent")
        btn_pady = (6, 10) if show_switches else (6, 14)
        btn_frame.pack(fill="x", padx=20, pady=btn_pady)

        def on_test_voice():
            if "path" in voice:
                self.tts.add_voice(command, voice["path"], temperature=voice.get("temperature", 0.5))
            self.test_voice(command)

        test_btn = ctk.CTkButton(
            btn_frame,
            text="Test Voice",
            height=32,
            fg_color="#2B2B36",
            hover_color="#3E3E4C",
            text_color="#FFFFFF",
            font=("Arial", 11, "bold"),
            command=on_test_voice
        )
        test_btn.pack(fill="x")

        # TTS and Roleplay on/off switches if show_switches is True (e.g. from Search voice dropdown)
        if show_switches:
            switches_container = ctk.CTkFrame(top, fg_color="#18181E", corner_radius=6, border_width=1, border_color="#2A2A36")
            switches_container.pack(fill="x", padx=20, pady=(0, 14), ipady=4)

            switches_inner = ctk.CTkFrame(switches_container, fg_color="transparent")
            switches_inner.pack(fill="x", padx=12, pady=4)

            # TTS switch
            ctk.CTkLabel(switches_inner, text="TTS:", font=("Arial", 11, "bold"), text_color="#E1E1E6").pack(side="left")
            tts_sw = ctk.CTkSwitch(switches_inner, text="", progress_color="#00D1FF", width=36)
            tts_sw.select() if voice.get('active', True) else tts_sw.deselect()
            def on_tts_toggle():
                self.toggle_voice_active(command, tts_sw)
                self.refresh_voices()
            tts_sw.configure(command=on_tts_toggle)
            tts_sw.pack(side="left", padx=(6, 20))

            # Roleplay switch
            ctk.CTkLabel(switches_inner, text="Roleplay:", font=("Arial", 11, "bold"), text_color="#E1E1E6").pack(side="left")
            rp_sw = ctk.CTkSwitch(switches_inner, text="", progress_color="#A970FF", width=36)
            rp_sw.select() if voice.get('rp_active', True) else rp_sw.deselect()
            def on_rp_toggle():
                self.toggle_voice_rp_active(command, rp_sw)
                self.refresh_voices()
            rp_sw.configure(command=on_rp_toggle)
            rp_sw.pack(side="left", padx=(6, 0))

    def refresh_voices(self):
        if not getattr(self, '_is_updating_from_modal', False):
            if hasattr(self, '_refresh_voice_library_modal') and callable(self._refresh_voice_library_modal):
                try:
                    self._refresh_voice_library_modal()
                except Exception:
                    pass

    def test_voice(self, command):
        self.tts.generate_and_play(command, "This is a test of the custom voice.", self.on_chat_message, user="System")

    def center_toplevel(self, top, width, height, parent=None):
        if parent is None:
            parent = self
        parent.update_idletasks()
        x = parent.winfo_x() + (parent.winfo_width() // 2) - (width // 2)
        y = parent.winfo_y() + (parent.winfo_height() // 2) - (height // 2)
        top.geometry(f"{width}x{height}+{x}+{y}")
        top.transient(parent)
        top.attributes("-topmost", True)
        top.focus_set()
        self.apply_dark_title_bar(top)

    def prompt_add_voice(self):
        self.open_add_voice_window()

    def open_add_voice_window(self):
        if hasattr(self, 'add_voice_window') and self.add_voice_window and self.add_voice_window.winfo_exists():
            self.add_voice_window.focus()
            return

        top = ctk.CTkToplevel(self)
        self.add_voice_window = top
        top.title("Add New Voice")
        self.center_toplevel(top, 460, 335)
        top.minsize(440, 320)
        self.apply_dark_title_bar(top)
        top.attributes("-topmost", True)

        def on_close():
            top.destroy()

        top.protocol("WM_DELETE_WINDOW", on_close)

        # State storage
        state = {
            "selected_path": None,
            "filename": None
        }
        cmd_var = ctk.StringVar(value="")
        desc_var = ctk.StringVar(value="")

        content_frame = ctk.CTkFrame(top, fg_color="transparent")
        content_frame.pack(fill="both", expand=True, padx=20, pady=(12, 10))

        # 1) Audio File selection button & status label
        file_section = ctk.CTkFrame(content_frame, fg_color="#18181E", corner_radius=6, border_width=1, border_color="#2A2A36")
        file_section.pack(fill="x", pady=(0, 8), ipady=2)

        file_btn_row = ctk.CTkFrame(file_section, fg_color="transparent")
        file_btn_row.pack(fill="x", padx=10, pady=(6, 2))

        file_status_label = ctk.CTkLabel(file_section, text="No voice file selected (*.mp3, *.wav)", font=("Arial", 11), text_color="#8E9299", anchor="w")

        def select_file():
            path = filedialog.askopenfilename(
                title="Select Voice Sample",
                filetypes=[("Audio Files", "*.mp3 *.wav")]
            )
            if not path:
                return
            state["selected_path"] = path
            fname = os.path.basename(path)
            state["filename"] = fname
            file_status_label.configure(text=fname, text_color="#00D1FF")

            # 3) Description defaults to voice file name
            base_name = os.path.splitext(fname)[0]
            if not desc_var.get().strip():
                desc_var.set(base_name)
            if not cmd_var.get().strip():
                clean_cmd = re.sub(r'[^a-zA-Z0-9_]', '', base_name.lower())
                cmd_var.set(f"!{clean_cmd}" if clean_cmd else "!")
            validate_form()

        choose_file_btn = ctk.CTkButton(
            file_btn_row,
            text="Choose Audio File (.mp3/.wav)",
            width=200,
            height=28,
            fg_color="#005E73",
            hover_color="#00D1FF",
            font=("Arial", 11, "bold"),
            command=select_file
        )
        choose_file_btn.pack(side="left")

        file_status_label.pack(fill="x", padx=12, pady=(2, 6))

        # 2) TTS Command textfield
        cmd_row = ctk.CTkFrame(content_frame, fg_color="transparent")
        cmd_row.pack(fill="x", pady=(0, 6))
        ctk.CTkLabel(cmd_row, text="TTS Command:", font=("Arial", 12, "bold"), text_color="#E1E1E6", width=110, anchor="w").pack(side="left")
        cmd_entry = ctk.CTkEntry(cmd_row, textvariable=cmd_var, placeholder_text="e.g. !myvoice", height=28, font=("Courier", 12, "bold"), text_color="#00D1FF")
        cmd_entry.pack(side="left", fill="x", expand=True)

        # 3) Description textfield
        desc_row = ctk.CTkFrame(content_frame, fg_color="transparent")
        desc_row.pack(fill="x", pady=(0, 6))
        ctk.CTkLabel(desc_row, text="Description:", font=("Arial", 12, "bold"), text_color="#E1E1E6", width=110, anchor="w").pack(side="left")
        desc_entry = ctk.CTkEntry(desc_row, textvariable=desc_var, placeholder_text="e.g. Voice description", height=28, font=("Arial", 12))
        desc_entry.pack(side="left", fill="x", expand=True)

        # 5) Temperature slider
        temp_frame = ctk.CTkFrame(content_frame, fg_color="transparent")
        temp_frame.pack(fill="x", pady=(0, 8))

        temp_header_row = ctk.CTkFrame(temp_frame, fg_color="transparent")
        temp_header_row.pack(fill="x")
        temp_label = ctk.CTkLabel(temp_header_row, text="Temperature: 0.50", font=("Arial", 12, "bold"), text_color="#E1E1E6")
        temp_label.pack(side="left")

        def update_temp(v):
            temp_label.configure(text=f"Temperature: {float(v):.2f}")

        temp_slider = ctk.CTkSlider(temp_frame, from_=0.0, to=1.0, command=update_temp)
        temp_slider.set(0.5)
        temp_slider.pack(fill="x", pady=(3, 2))

        ctk.CTkLabel(temp_frame, text="Higher = More variation & creativity. Lower = More predictable.", font=("Arial", 10), text_color="#8E9299").pack(anchor="w")

        # 6) Test Voice button & 7) Save Voice button
        action_btn_row = ctk.CTkFrame(content_frame, fg_color="transparent")
        action_btn_row.pack(fill="x", pady=(10, 4))

        def test_new_voice():
            fpath = state.get("selected_path")
            if not fpath or not os.path.exists(fpath):
                self.append_system_log("\n[System] Please select an audio file first to test the voice.")
                return
            t_cmd = cmd_var.get().strip() or "!test_new_voice"
            if not t_cmd.startswith("!"):
                t_cmd = "!" + t_cmd
            cur_temp = round(float(temp_slider.get()), 2)
            try:
                self.tts.add_voice(t_cmd, fpath, temperature=cur_temp)
                self.test_voice(t_cmd)
            except Exception as e:
                self.append_system_log(f"\n[System] Error testing voice: {e}")

        test_btn = ctk.CTkButton(
            action_btn_row,
            text="Test Voice",
            width=110,
            height=32,
            fg_color="#2B2B36",
            hover_color="#3E3E4C",
            font=("Arial", 11, "bold"),
            command=test_new_voice
        )
        test_btn.pack(side="left")

        def save_voice():
            fpath = state.get("selected_path")
            raw_cmd = cmd_var.get().strip()
            if not fpath or not raw_cmd:
                return

            cmd = "!" + raw_cmd.lstrip("!")
            fname = os.path.basename(fpath)
            name = desc_var.get().strip() or os.path.splitext(fname)[0]

            os.makedirs("voices", exist_ok=True)
            local_target_path = os.path.join("voices", fname)
            if os.path.abspath(fpath) != os.path.abspath(local_target_path):
                try:
                    shutil.copy2(fpath, local_target_path)
                except Exception as e:
                    self.append_system_log(f"\n[System] Error copying voice file: {e}")
                    return

            # Avoid collision
            existing_cmds = {v['command'] for v in self.voices}
            while cmd in existing_cmds:
                cmd += "2"

            t_val = round(float(temp_slider.get()), 2)
            new_voice = {
                "name": name,
                "command": cmd,
                "active": True,
                "rp_active": True,
                "path": local_target_path,
                "exaggeration": 0.5,
                "temperature": t_val
            }
            self.voices.append(new_voice)
            self.voices.sort(key=lambda x: x.get('name', '').lower())

            self.tts.add_voice(cmd, local_target_path, temperature=t_val)
            self.save_voices()
            self.refresh_voices()
            self.append_system_log(f"\n[System] Successfully imported '{fname}' as {cmd} ({name})")
            top.destroy()

        save_btn = ctk.CTkButton(
            action_btn_row,
            text="Save Voice",
            width=140,
            height=32,
            state="disabled",
            fg_color="#2B2D31",
            text_color="#6C7079",
            hover_color="#55E2FF",
            font=("Arial", 12, "bold"),
            command=save_voice
        )
        save_btn.pack(side="right")

        # 7) Save Voice button disabled until user selects voice file & populates TTS command
        def validate_form(*args):
            fpath = state.get("selected_path")
            has_file = bool(fpath and os.path.exists(fpath))
            cmd_text = cmd_var.get().strip().lstrip("!")
            has_cmd = bool(cmd_text)

            if has_file and has_cmd:
                save_btn.configure(
                    state="normal",
                    fg_color="#00D1FF",
                    text_color="#000000"
                )
            else:
                save_btn.configure(
                    state="disabled",
                    fg_color="#2B2D31",
                    text_color="#6C7079"
                )

        cmd_var.trace_add("write", validate_form)

    def open_voice_library_window(self):
        if hasattr(self, 'voice_library_window') and self.voice_library_window and self.voice_library_window.winfo_exists():
            self.voice_library_window.focus()
            return

        import tkinter as tk

        # Calculate dynamic command & description column widths based on longest values
        cmd_lens = [len(v.get("command", "").strip()) for v in self.voices]
        max_cmd_len = max(cmd_lens) if cmd_lens else len("TTS Command")
        cmd_width_chars = max(max_cmd_len, len("TTS Command"), 11)
        cmd_px = max(int(cmd_width_chars * 9.2) + 14, 115)

        desc_lens = [len(v.get("name", "").strip()) for v in self.voices]
        max_desc_len = max(desc_lens) if desc_lens else len("Description")
        desc_width_chars = max(max_desc_len, len("Description"), 12)
        desc_px = int(desc_width_chars * 8.2)

        # Dynamic window width based on both column lengths
        base_width = 340
        window_width = max(660, min(base_width + cmd_px + desc_px, 1020))

        top = ctk.CTkToplevel(self)
        self.voice_library_window = top
        top.title("Voice Library")
        self.center_toplevel(top, window_width, 520)
        top.minsize(620, 380)
        top.resizable(True, True)
        self.apply_dark_title_bar(top)
        top.attributes("-topmost", True)

        def on_close():
            self._refresh_voice_library_modal = None
            top.destroy()

        top.protocol("WM_DELETE_WINDOW", on_close)

        # Header Bar: Search voices filter (left-aligned), Copy Voice List button & Voice Count (right-aligned)
        header_frame = ctk.CTkFrame(top, fg_color="transparent")
        header_frame.pack(fill="x", padx=20, pady=(16, 6))

        # "Search voices:" label and filter textfield on the far left
        search_frame = ctk.CTkFrame(header_frame, fg_color="transparent")
        search_frame.pack(side="left", padx=0)

        ctk.CTkLabel(
            search_frame,
            text="Search voices:",
            font=("Arial", 11, "bold"),
            text_color="#8E9299"
        ).pack(side="left", padx=(0, 6))

        search_var = ctk.StringVar()
        search_entry = ctk.CTkEntry(
            search_frame,
            textvariable=search_var,
            placeholder_text="Filter command or description...",
            width=220,
            height=26,
            font=("Arial", 11)
        )
        search_entry.pack(side="left")

        # Right side: Voices Count (far right) and Copy Voice List button (to the left of the count)
        count_lbl = ctk.CTkLabel(
            header_frame,
            text=f"{len(self.voices)} Voices",
            font=("Arial", 12, "bold"),
            text_color="#8E9299"
        )
        count_lbl.pack(side="right", padx=(10, 0))

        def copy_voice_list():
            self.copy_active_voices()
            copy_btn.configure(text="Copied!", fg_color="#1F6FEB")
            top.after(1400, lambda: copy_btn.winfo_exists() and copy_btn.configure(text="Copy Voice List", fg_color="#3A3A40"))

        copy_btn = ctk.CTkButton(
            header_frame,
            text="Copy Voice List",
            width=115,
            height=26,
            fg_color="#3A3A40",
            hover_color="#55555C",
            text_color="#E1E1E6",
            font=("Arial", 10, "bold"),
            command=copy_voice_list
        )
        copy_btn.pack(side="right", padx=(0, 0))

        # Table Header Row: Synchronized grid columns matching row card columns
        table_header_frame = tk.Frame(top, bg="#121216", bd=0)
        table_header_frame.pack(fill="x", padx=(22, 50), pady=(6, 4))

        table_header_frame.columnconfigure(0, weight=0, minsize=cmd_px)
        table_header_frame.columnconfigure(1, weight=1)
        table_header_frame.columnconfigure(2, weight=0, minsize=46)
        table_header_frame.columnconfigure(3, weight=0, minsize=65)
        table_header_frame.columnconfigure(4, weight=0, minsize=96)

        cmd_hdr = tk.Label(
            table_header_frame,
            text="TTS Command",
            font=("Arial", 10, "bold"),
            fg="#8E9299",
            bg="#121216",
            anchor="w"
        )
        cmd_hdr.grid(row=0, column=0, sticky="ew", padx=(8, 4), pady=2)

        desc_hdr = tk.Label(
            table_header_frame,
            text="Description",
            font=("Arial", 10, "bold"),
            fg="#8E9299",
            bg="#121216",
            anchor="w"
        )
        desc_hdr.grid(row=0, column=1, sticky="ew", padx=(4, 6), pady=2)

        tts_hdr = tk.Label(
            table_header_frame,
            text="TTS",
            font=("Arial", 10, "bold"),
            fg="#00D1FF",
            bg="#121216",
            anchor="center"
        )
        tts_hdr.grid(row=0, column=2, sticky="ew", padx=(4, 4), pady=2)

        rp_hdr = tk.Label(
            table_header_frame,
            text="Roleplay",
            font=("Arial", 10, "bold"),
            fg="#A970FF",
            bg="#121216",
            anchor="center"
        )
        rp_hdr.grid(row=0, column=3, sticky="ew", padx=(4, 4), pady=2)

        action_hdr_spacer = tk.Label(table_header_frame, text="", bg="#121216")
        action_hdr_spacer.grid(row=0, column=4, sticky="ew", padx=(4, 8), pady=2)

        # Scrollable container for voice list
        scroll_container = ctk.CTkScrollableFrame(top, fg_color="transparent")
        scroll_container.pack(fill="both", expand=True, padx=20, pady=(0, 16))

        render_counter = [0]

        def populate_voice_library():
            if not top.winfo_exists() or not scroll_container.winfo_exists():
                return

            render_counter[0] += 1
            cur_render = render_counter[0]

            for w in scroll_container.winfo_children():
                w.destroy()

            query = search_var.get().strip().lower()
            clean_q = query.lstrip("!")

            if query:
                filtered_voices = [
                    v for v in self.voices
                    if query in v.get("command", "").lower()
                    or clean_q in v.get("command", "").lower().lstrip("!")
                    or query in v.get("name", "").lower()
                    or query in v.get("description", "").lower()
                ]
            else:
                filtered_voices = list(self.voices)

            # Recalculate dynamic widths if voices changed
            c_lens = [len(v.get("command", "").strip()) for v in self.voices]
            cur_cmd_max = max(c_lens) if c_lens else len("TTS Command")
            cur_cmd_width = max(cur_cmd_max, len("TTS Command"), 11)
            cur_cmd_px = max(int(cur_cmd_width * 9.2) + 14, 115)
            table_header_frame.columnconfigure(0, weight=0, minsize=cur_cmd_px)

            # Sorted by voice command
            sorted_voices = sorted(filtered_voices, key=lambda x: x.get("command", "").lower())
            
            if query:
                count_lbl.configure(text=f"{len(sorted_voices)} / {len(self.voices)} Voices")
            else:
                count_lbl.configure(text=f"{len(sorted_voices)} Voices")

            if not sorted_voices:
                no_match_text = f'No voices match "{search_var.get().strip()}".' if query else "No voices found in library."
                empty_lbl = ctk.CTkLabel(scroll_container, text=no_match_text, font=("Arial", 12), text_color="#8E9299")
                empty_lbl.pack(pady=40)
                return

            total = len(sorted_voices)
            batch_size = 35

            def render_batch(start_idx=0):
                if not top.winfo_exists() or not scroll_container.winfo_exists():
                    return
                if render_counter[0] != cur_render:
                    return

                end_idx = min(start_idx + batch_size, total)

                for idx in range(start_idx, end_idx):
                    v = sorted_voices[idx]
                    cmd = v.get("command", "")
                    desc = v.get("name", "")

                    row_card = tk.Frame(scroll_container, bg="#18181E", bd=1, relief="solid")
                    row_card.pack(fill="x", pady=2, padx=2)

                    row_card.columnconfigure(0, weight=0, minsize=cur_cmd_px)
                    row_card.columnconfigure(1, weight=1)
                    row_card.columnconfigure(2, weight=0, minsize=46)
                    row_card.columnconfigure(3, weight=0, minsize=65)
                    row_card.columnconfigure(4, weight=0, minsize=96)

                    # Col 0: Editable TTS Command Textfield
                    cmd_entry = tk.Entry(
                        row_card,
                        font=("Courier", 11, "bold"),
                        fg="#00D1FF",
                        bg="#121216",
                        insertbackground="#00D1FF",
                        bd=1,
                        relief="solid",
                        highlightthickness=1,
                        highlightcolor="#00D1FF",
                        highlightbackground="#2A2A36",
                        width=1
                    )
                    cmd_entry.insert(0, cmd)
                    cmd_entry.grid(row=0, column=0, sticky="ew", padx=(8, 4), pady=4)

                    def make_cmd_callback(voice_dict, entry_widget, old_cmd):
                        def on_cmd_change(event=None):
                            new_cmd = entry_widget.get().strip()
                            if not new_cmd:
                                entry_widget.delete(0, "end")
                                entry_widget.insert(0, old_cmd)
                                return
                            new_cmd = "!" + new_cmd.lstrip("!")
                            existing_cmds = {voice['command'] for voice in self.voices if voice is not voice_dict}
                            while new_cmd in existing_cmds:
                                new_cmd += "2"
                            if new_cmd != old_cmd:
                                if old_cmd in self.tts.active_voices:
                                    del self.tts.active_voices[old_cmd]
                                if "path" in voice_dict:
                                    self.tts.add_voice(
                                        new_cmd,
                                        voice_dict['path'],
                                        temperature=voice_dict.get('temperature', 0.5)
                                    )
                                voice_dict['command'] = new_cmd
                                self.save_voices()
                                if hasattr(self, 'roleplay_manager'):
                                    self.roleplay_manager.update_voice_command(old_cmd, new_cmd)
                                self._is_updating_from_modal = True
                                self.refresh_voices()
                                self._is_updating_from_modal = False
                                entry_widget.delete(0, "end")
                                entry_widget.insert(0, new_cmd)
                            elif entry_widget.get() != new_cmd:
                                entry_widget.delete(0, "end")
                                entry_widget.insert(0, new_cmd)
                            entry_widget.master.focus()
                        return on_cmd_change

                    cmd_cb = make_cmd_callback(v, cmd_entry, cmd)
                    cmd_entry.bind("<FocusOut>", cmd_cb)
                    cmd_entry.bind("<Return>", cmd_cb)

                    # Col 1: Editable Description Textfield (Expands dynamically on window resize)
                    desc_entry = tk.Entry(
                        row_card,
                        font=("Arial", 11),
                        fg="#E1E1E6",
                        bg="#121216",
                        insertbackground="#E1E1E6",
                        bd=1,
                        relief="solid",
                        highlightthickness=1,
                        highlightcolor="#3A3A40",
                        highlightbackground="#2A2A36",
                        width=1
                    )
                    desc_entry.insert(0, desc)
                    desc_entry.grid(row=0, column=1, sticky="ew", padx=(4, 6), pady=4)

                    def make_desc_callback(voice_dict, entry_widget, old_desc):
                        def on_desc_change(event=None):
                            new_val = entry_widget.get().strip()
                            if not new_val:
                                entry_widget.delete(0, "end")
                                entry_widget.insert(0, old_desc)
                                return
                            if new_val != old_desc:
                                voice_dict['name'] = new_val
                                self.save_voices()
                                self._is_updating_from_modal = True
                                self.refresh_voices()
                                self._is_updating_from_modal = False
                            entry_widget.master.focus()
                        return on_desc_change

                    desc_cb = make_desc_callback(v, desc_entry, desc)
                    desc_entry.bind("<FocusOut>", desc_cb)
                    desc_entry.bind("<Return>", desc_cb)

                    # Col 2: TTS Checkbox Glyph (Aligned under TTS header)
                    is_tts = bool(v.get("active", True))
                    tts_cb = tk.Label(
                        row_card,
                        text="☑" if is_tts else "☐",
                        font=("Arial", 13, "bold"),
                        fg="#00D1FF" if is_tts else "#606070",
                        bg="#18181E",
                        activebackground="#18181E",
                        cursor="hand2",
                        anchor="center"
                    )
                    tts_cb.grid(row=0, column=2, sticky="ew", padx=(4, 4), pady=4)

                    def make_tts_handler(voice_obj, lbl):
                        def on_click(e):
                            new_val = not bool(voice_obj.get("active", True))
                            voice_obj["active"] = new_val
                            lbl.configure(
                                text="☑" if new_val else "☐",
                                fg="#00D1FF" if new_val else "#606070"
                            )
                            self.toggle_voice_active(voice_obj.get("command", ""), new_val)
                        return on_click

                    tts_cb.bind("<Button-1>", make_tts_handler(v, tts_cb))

                    # Col 3: Roleplay Checkbox Glyph (Aligned under Roleplay header)
                    is_rp = bool(v.get("rp_active", True))
                    rp_cb = tk.Label(
                        row_card,
                        text="☑" if is_rp else "☐",
                        font=("Arial", 13, "bold"),
                        fg="#A970FF" if is_rp else "#606070",
                        bg="#18181E",
                        activebackground="#18181E",
                        cursor="hand2",
                        anchor="center"
                    )
                    rp_cb.grid(row=0, column=3, sticky="ew", padx=(4, 4), pady=4)

                    def make_rp_handler(voice_obj, lbl):
                        def on_click(e):
                            new_val = not bool(voice_obj.get("rp_active", True))
                            voice_obj["rp_active"] = new_val
                            lbl.configure(
                                text="☑" if new_val else "☐",
                                fg="#A970FF" if new_val else "#606070"
                            )
                            self.toggle_voice_rp_active(voice_obj.get("command", ""), new_val)
                        return on_click

                    rp_cb.bind("<Button-1>", make_rp_handler(v, rp_cb))

                    # Col 4: Action Buttons Frame ([⚙] [📜] [X])
                    action_frame = tk.Frame(row_card, bg="#18181E")
                    action_frame.grid(row=0, column=4, sticky="e", padx=(4, 8), pady=4)

                    # 1. Settings Cog Button (just the cog icon ⚙)
                    settings_btn = tk.Button(
                        action_frame,
                        text="⚙",
                        font=("Arial", 12),
                        fg="#FFFFFF",
                        bg="#454854",
                        activebackground="#585C6B",
                        activeforeground="#FFFFFF",
                        bd=0,
                        width=3,
                        cursor="hand2",
                        command=lambda c=cmd: self.open_voice_settings(c)
                    )
                    settings_btn.pack(side="left", padx=2)

                    # 2. Blue Lore Button (scroll icon 📜)
                    lore_btn = tk.Button(
                        action_frame,
                        text="📜",
                        font=("Arial", 11),
                        fg="#FFFFFF",
                        bg="#005E73",
                        activebackground="#00D1FF",
                        activeforeground="#FFFFFF",
                        bd=0,
                        width=3,
                        cursor="hand2",
                        command=lambda c=cmd: self.open_character_lore(c)
                    )
                    lore_btn.pack(side="left", padx=2)

                    # 3. Red Delete Button (X icon like in middle panel)
                    delete_btn = tk.Button(
                        action_frame,
                        text="X",
                        font=("Arial", 10, "bold"),
                        fg="#FFFFFF",
                        bg="#730000",
                        activebackground="#A90000",
                        activeforeground="#FFFFFF",
                        bd=0,
                        width=3,
                        cursor="hand2",
                        command=lambda c=cmd: self.confirm_delete_voice(c, parent_window=top, close_parent=False)
                    )
                    delete_btn.pack(side="left", padx=2)

                if end_idx < total and top.winfo_exists() and render_counter[0] == cur_render:
                    self.after(1, lambda: render_batch(end_idx))

            render_batch(0)

        search_var.trace_add("write", lambda *args: populate_voice_library())

        self._refresh_voice_library_modal = populate_voice_library
        populate_voice_library()

    def chunk_message(self, text, max_chars=None):
        import re
        text = (text or "").strip()
        if not text:
            return []
        if max_chars is None:
            max_chars = int(self.settings.get("max_chars", 300)) if hasattr(self, "settings") else 300
        if max_chars <= 0:
            max_chars = 300

        if len(text) <= max_chars:
            return [text]

        pattern = r'([.!?]+["\'”’\)\]]*\s+|\n+)'

        # 1. Try to split into two chunks rounded to the nearest sentence
        valid_two_splits = []
        for m in re.finditer(pattern, text):
            split_pos = m.end()
            c1 = text[:split_pos].strip()
            c2 = text[split_pos:].strip()
            if c1 and c2 and len(c1) <= max_chars and len(c2) <= max_chars:
                valid_two_splits.append((split_pos, c1, c2))

        if valid_two_splits:
            mid = len(text) / 2.0
            best = min(valid_two_splits, key=lambda item: abs(item[0] - mid))
            return [best[1], best[2]]

        # 2. If text is longer than 2 chunks, check if it can be split by sentence endings into chunks <= max_chars
        sentence_pieces = []
        last_idx = 0
        for m in re.finditer(pattern, text):
            s = text[last_idx:m.end()].strip()
            if s:
                sentence_pieces.append(s)
            last_idx = m.end()
        tail = text[last_idx:].strip()
        if tail:
            sentence_pieces.append(tail)

        if len(sentence_pieces) > 1 and all(len(s) <= max_chars for s in sentence_pieces):
            sent_chunks = []
            curr = ""
            for s in sentence_pieces:
                if not curr:
                    curr = s
                elif len(curr) + 1 + len(s) <= max_chars:
                    curr += " " + s
                else:
                    sent_chunks.append(curr)
                    curr = s
            if curr:
                sent_chunks.append(curr)
            if sent_chunks and all(len(c) <= max_chars for c in sent_chunks):
                return sent_chunks

        # 3. Backup method: if entire message is one sentence or sentence chunking still results in a chunk > max_chars,
        # split to the nearest space " " like before
        backup_chunks = []
        rem_text = text
        while len(rem_text) > max_chars:
            truncated = rem_text[:max_chars]
            last_space = truncated.rfind(' ')
            if last_space > 0:
                chunk = truncated[:last_space]
                rem_text = rem_text[last_space:].strip()
            else:
                chunk = truncated
                rem_text = rem_text[max_chars:].strip()
            if chunk:
                backup_chunks.append(chunk)

        if rem_text:
            backup_chunks.append(rem_text)

        return backup_chunks

    def submit_manual_tts(self):
        if not getattr(self, "model_ready", False):
            self.append_system_log("\n[System] Please wait: AI Voice Model is still loading into VRAM...")
            return
        command = self.manual_cmd_var.get().strip()
        text = self.manual_text_box.get("1.0", "end-1c").strip()
        
        if command and not command.startswith("!"):
            command = "!" + command

        if command and text:
            chunks = self.chunk_message(text)
            self.append_system_log(f"\n[System] MAIN.PY: Triggering {len(chunks)} manual TTS segment(s) using '{command}'\n")

            # Setup cache file in a dedicated system temp folder so "Save TTS" can copy it without cluttering the user's saved audio folder
            try:
                import tempfile
                temp_cache_dir = os.path.join(tempfile.gettempdir(), "chatterbox_tts_cache")
                os.makedirs(temp_cache_dir, exist_ok=True)
                cache_file = os.path.join(temp_cache_dir, "last_manual_tts.wav")
            except Exception:
                import tempfile
                cache_file = os.path.join(tempfile.gettempdir(), "chatterbox_last_manual_tts.wav")

            self.last_manual_tts_file = cache_file
            self.last_manual_tts_cmd = command
            self.last_manual_tts_text = text

            import uuid
            group_id = str(uuid.uuid4())
            for i, chunk in enumerate(chunks):
                is_contiguous = (i < len(chunks) - 1)
                chunk_tts = self.translate_usernames_in_text(chunk, self.get_active_chatters_list())
                self.tts.generate_and_play(
                    command,
                    chunk_tts,
                    self.on_chat_message,
                    bypass_mute=False,
                    is_contiguous=is_contiguous,
                    group_id=group_id,
                    save_audio_path=cache_file,
                    category="Standard",
                    user="System",
                    save_only=False
                )

    def save_manual_tts(self):
        """Copies the voice file that was last generated using the Submit TTS button to the user-defined saved voices directory."""
        target_dir = self.get_saved_audio_dir()
        try:
            os.makedirs(target_dir, exist_ok=True)
        except Exception as e:
            self.append_system_log(f"\n[System Error] Failed to access saved audio directory '{target_dir}': {e}")
            return

        last_file = getattr(self, "last_manual_tts_file", None)
        if not last_file or not os.path.exists(last_file) or os.path.getsize(last_file) == 0:
            self.append_system_log("\n[System] No previously generated voice file found. Please click 'Submit TTS' first to generate audio before saving.")
            return

        import re, shutil

        command = getattr(self, "last_manual_tts_cmd", None) or self.manual_cmd_var.get().strip() or "voice"
        text = getattr(self, "last_manual_tts_text", None) or self.manual_text_box.get("1.0", "end-1c").strip() or "message"

        clean_cmd = re.sub(r'[^a-zA-Z0-9_\-]+', '', command.lstrip("!")).strip()
        if not clean_cmd:
            clean_cmd = "voice"

        # First 12 characters from custom message
        first_12 = text[:12]
        clean_msg = re.sub(r'[^a-zA-Z0-9_\-]+', '_', first_12).strip('_')
        if not clean_msg:
            clean_msg = "msg"

        # Find next available 3-digit sequence number: [ttsCommand]_[message]_[001].wav
        counter = 1
        while True:
            filename = f"{clean_cmd}_{clean_msg}_{counter:03d}.wav"
            dest_file = os.path.join(target_dir, filename)
            if not os.path.exists(dest_file):
                break
            counter += 1

        try:
            shutil.copy2(last_file, dest_file)
            self.append_system_log(f"\n[System] Successfully saved voice file copy to: {dest_file}\n")
        except Exception as e:
            self.append_system_log(f"\n[System Error] Failed to save copy of voice file: {e}")

    def save_kickbot_audio(self, audio_path, user="KickBot", text="AI Text-to-Speech", command="KickBot TTS"):
        """Saves a copy of the mp3 file received from KickBot TTS to the saved voices directory."""
        if not audio_path or not os.path.exists(audio_path) or os.path.getsize(audio_path) == 0:
            return

        target_dir = self.get_saved_audio_dir()
        try:
            os.makedirs(target_dir, exist_ok=True)
        except Exception as e:
            self.after(0, self.append_system_log, f"\n[KickBot TTS Error] Failed to access save directory '{target_dir}': {e}\n")
            return

        import re, shutil

        ext = os.path.splitext(audio_path)[1]
        if not ext or ext.lower() not in ('.mp3', '.wav', '.ogg', '.aac'):
            ext = ".mp3"

        clean_user = re.sub(r'[^a-zA-Z0-9_\-]+', '', str(user or 'KickBot')).strip()
        if not clean_user:
            clean_user = "KickBot"

        raw_cmd = str(command or "KickBot").replace("KickBot:", "").strip()
        clean_voice = re.sub(r'[^a-zA-Z0-9_\-]+', '_', raw_cmd).strip('_')

        first_15 = str(text or '')[:15]
        clean_msg = re.sub(r'[^a-zA-Z0-9_\-]+', '_', first_15).strip('_')

        parts = ["KickBot"]
        if clean_voice and clean_voice.lower() not in ("kickbot", "tts", "kickbot_tts"):
            parts.append(clean_voice)
        if clean_user and clean_user.lower() != "kickbot":
            parts.append(clean_user)
        if clean_msg:
            parts.append(clean_msg)

        base_name = "_".join(parts)
        if not base_name:
            base_name = "KickBot_TTS"

        counter = 1
        while True:
            filename = f"{base_name}_{counter:03d}{ext}"
            dest_file = os.path.join(target_dir, filename)
            if not os.path.exists(dest_file):
                break
            counter += 1

        try:
            shutil.copy2(audio_path, dest_file)
            self.after(0, self.append_system_log, f"\n[KickBot TTS] Saved audio file to: {dest_file}\n")
        except Exception as e:
            self.after(0, self.append_system_log, f"\n[KickBot TTS Error] Failed to save audio file: {e}\n")

    def on_kickbot_audio(self, audio_path, user="KickBot", text="AI Text-to-Speech", command="KickBot TTS"):
        """Callback fired when KickBot generates TTS audio"""
        clean_user = str(user or "").strip().lstrip("@").lower()
        banned_users = [str(u).strip().lstrip("@").lower() for u in self.settings.get("banned_users", []) if str(u).strip()]

        # Check if username or sender is in banned list or timed out list
        is_banned = bool(clean_user and clean_user != "kickbot" and clean_user in banned_users)
        is_timed_out = bool(clean_user and clean_user != "kickbot" and self.is_user_timed_out(clean_user))

        # Also check if text has "user: ..." format from a banned or timed-out user
        if not (is_banned or is_timed_out) and text:
            m_author = re.match(r'^\s*@?([a-zA-Z0-9_\-]+)\s*:\s*(.*)$', str(text))
            if m_author:
                cand_user = m_author.group(1).strip().lstrip("@").lower()
                if cand_user in banned_users:
                    is_banned = True
                    user = m_author.group(1)
                elif self.is_user_timed_out(cand_user):
                    is_timed_out = True
                    user = m_author.group(1)

        if is_banned:
            self.after(0, self.append_system_log, f"\n[KickBot TTS] Ignored TTS from banned user '{user}'.\n")
            if audio_path and os.path.exists(audio_path):
                try:
                    os.remove(audio_path)
                except Exception:
                    pass
            return

        if is_timed_out:
            self.after(0, self.append_system_log, f"\n[KickBot TTS] Ignored TTS from timed out user '{user}'.\n")
            if audio_path and os.path.exists(audio_path):
                try:
                    os.remove(audio_path)
                except Exception:
                    pass
            return

        is_save_enabled = bool(self.save_kickbot_tts_var.get()) if hasattr(self, "save_kickbot_tts_var") else bool(self.settings.get("save_kickbot_tts", False))
        if is_save_enabled and audio_path and os.path.exists(audio_path):
            self.save_kickbot_audio(audio_path, user=user, text=text, command=command)

        def _enqueue():
            if self.tts:
                translated_user = self.translate_username(user or "KickBot")
                tts_text = self.translate_usernames_in_text(text or "AI Text-to-Speech", self.get_active_chatters_list())
                if audio_path and os.path.exists(audio_path):
                    self.tts.play_audio_file(
                        file_path=audio_path,
                        command=command or "KickBot TTS",
                        category="KickBot",
                        bypass_mute=False,
                        text=tts_text,
                        user=translated_user
                    )
                else:
                    self.tts.generate_and_play(
                        command="KickBot",
                        text=tts_text,
                        callback_log=lambda s, m, p: self.append_system_log(f"[{s}] {m}"),
                        category="KickBot",
                        user=translated_user
                    )
            # Note: Do not append to Live Chat Feed to avoid duplicate chat display
        self.after(0, _enqueue)

    def save_roleplay_audio(self, command, text, user, audio_numpy_or_parts):
        """Saves a roleplay audio message (joining all characters and chunks for the entire conversation into a single audio file) to the saved audio directory."""
        is_save_enabled = bool(self.save_roleplay_tts_var.get()) if hasattr(self, "save_roleplay_tts_var") else bool(self.settings.get("save_roleplay_tts", False))
        if not is_save_enabled:
            return

        target_dir = self.get_saved_audio_dir()
        try:
            os.makedirs(target_dir, exist_ok=True)
        except Exception as e:
            self.after(0, self.append_system_log, f"\n[Roleplay TTS Error] Failed to access save directory '{target_dir}': {e}\n")
            return

        import numpy as np
        import soundfile as sf
        import re

        try:
            if isinstance(audio_numpy_or_parts, list):
                valid_parts = [p for p in audio_numpy_or_parts if p is not None and len(p) > 0]
                if not valid_parts:
                    return
                full_audio = np.concatenate(valid_parts)
            else:
                full_audio = audio_numpy_or_parts

            if full_audio is None or len(full_audio) == 0:
                return

            if isinstance(command, (list, tuple)):
                clean_cmds = []
                for c in command:
                    cleaned = re.sub(r'[^a-zA-Z0-9_\-]+', '', str(c).lstrip("!")).strip()
                    if cleaned and cleaned not in clean_cmds:
                        clean_cmds.append(cleaned)
                clean_cmd = "_".join(clean_cmds) if clean_cmds else "roleplay"
            else:
                clean_cmd = re.sub(r'[^a-zA-Z0-9_\-]+', '_', str(command or 'roleplay').replace("!", "")).strip('_')
                if not clean_cmd:
                    clean_cmd = "roleplay"

            # Strip leading roleplay trigger commands from prompt for cleaner file names
            clean_prompt = re.sub(r'^\s*(\?[a-zA-Z0-9_\-]+\s*)+', '', str(text or '')).strip()
            first_12 = (clean_prompt if clean_prompt else str(text or ''))[:12]
            clean_msg = re.sub(r'[^a-zA-Z0-9_\-]+', '_', first_12).strip('_')
            if not clean_msg:
                clean_msg = "rp"

            clean_user = re.sub(r'[^a-zA-Z0-9_\-]+', '', str(user or '')).strip()

            parts = ["RP", clean_cmd]
            if clean_user and clean_user.lower() not in ("unknown", "system"):
                parts.append(clean_user)
            if clean_msg:
                parts.append(clean_msg)

            base_name = "_".join(parts)
            if not base_name:
                base_name = "RP_audio"

            counter = 1
            while True:
                filename = f"{base_name}_{counter:03d}.wav"
                dest_file = os.path.join(target_dir, filename)
                if not os.path.exists(dest_file):
                    break
                counter += 1

            sf.write(dest_file, full_audio, 24000)
            self.after(0, self.append_system_log, f"\n[Roleplay TTS] Saved audio file to: {dest_file}\n")
        except Exception as e:
            self.after(0, self.append_system_log, f"\n[Roleplay TTS Error] Failed to save audio file: {e}\n")

    def on_powerchat_donation(self, user, amount, message, donation_id):
        """Callback fired when a Powerchat donation is received via WebSocket"""
        if not self.settings.get("powerchat_enabled", True):
            return
        try:
            amt = float(amount or 0.0)
            if amt <= 0.0:
                return

            lower_msg = str(message or "").lower()
            if (
                re.search(r'\b(?:just\s+)?gifted\s+\d+\s+sub(?:scription)?s?\b', lower_msg)
                or re.search(r'\b(?:just\s+)?subscribed\b', lower_msg)
                or re.search(r'\b(?:just\s+)?started\s+following\b', lower_msg)
                or re.search(r'\b(?:just\s+)?raided\b', lower_msg)
            ):
                return

            # Normalize user name (default to "Anonymous" if blank, anon, or missing)
            clean_user = str(user or "").strip().lstrip('@')
            if not clean_user or clean_user.lower() in ("anonymous", "anon", "someone", "unknown", "null", "none", "undefined", "n/a", "no name", "noname"):
                user = "Anonymous"
            else:
                user = clean_user

            current_channel = str(getattr(self, "current_username", "") or "").strip().lower()
            if current_channel and user.lower() == current_channel:
                if re.search(r'\b(?:gifted|subscription|subscriber|followed|raided)\b', lower_msg):
                    return

            if hasattr(self, '_summary_lock') and hasattr(self, 'session_powerchat'):
                with self._summary_lock:
                    self.session_powerchat.append({
                        "id": donation_id,
                        "user": user,
                        "amount": amt,
                        "message": message,
                        "timestamp": datetime.datetime.now().isoformat()
                    })
                self.record_stream_summary_activity()
                self.after(0, self.refresh_stream_summary_if_open)

            formatted_amt = f"${amt:.2f}" if amt % 1 != 0 else f"${int(amt)}"
            log_text = f"\n[Powerchat] {user} sent {formatted_amt}: {message}\n" if message else f"\n[Powerchat] {user} sent {formatted_amt}\n"
            self.after(0, self.append_system_log, log_text)
        except Exception as e:
            self.after(0, self.append_system_log, f"\n[Powerchat] Error processing donation: {e}\n")

    def get_voice_command_for_alert(self, specific_key, fallback_voice=""):
        cmd = str(self.settings.get(specific_key, "")).strip()
        if not cmd:
            # If blank, fall back to using the voice command of the kicks donos alert
            kd_voice = str(self.settings.get("kick_donos", {}).get("tts_voice", "")).strip()
            if kd_voice:
                cmd = kd_voice
            elif fallback_voice:
                cmd = fallback_voice
            elif self.voices:
                cmd = self.voices[0].get("command", "!narrator")
            else:
                cmd = "!narrator"
        if not cmd and self.voices:
            cmd = self.voices[0].get("command", "!narrator")
        if not cmd:
            cmd = "!narrator"
        if cmd and hasattr(self.tts, "active_voices") and self.tts.active_voices:
            if cmd not in self.tts.active_voices:
                alt = f"!{cmd}" if not cmd.startswith("!") else cmd.lstrip("!")
                if alt in self.tts.active_voices:
                    cmd = alt
        return cmd

    def parse_kickbot_sub_message(self, text):
        """
        Parses KickBot chat announcements for subscriptions and re-subscriptions.
        Returns: (username, months) or (None, 1) if not a sub announcement.
        Handles variations like:
        - 'TitleistGroyper Resubscribed! they have been subscribed for 7 months'
        - 'Hodenpulverisierer Resubscribed! they have been subscribed for 9 months'
        - '@TitleistGroyper Resubscribed! they have been subscribed for 7 months'
        - 'TitleistGroyper has just resubscribed for 7 months!'
        - 'TitleistGroyper resubscribed for 7 months!'
        - '@TitleistGroyper has been subscribed for 7 months!'
        - 'TitleistGroyper has been subscribed for 7 months!'
        - 'TitleistGroyper renewed their subscription for 7 months!'
        - 'TitleistGroyper just subscribed for 7 months!'
        - 'TitleistGroyper has subscribed for 1 month!'
        - 'TitleistGroyper subscribed for 3 months!'
        - 'TitleistGroyper resubscribed!'
        - 'TitleistGroyper subscribed!'
        """
        text = str(text or "").strip()
        if not any(k in text.lower() for k in ("sub", "renew")):
            return None, 1

        months = 1
        m_months = re.search(r'\b(?:for\s+)?(\d+)\s+months?\b', text, re.I)
        if m_months:
            try:
                months = int(m_months.group(1))
            except Exception:
                months = 1

        user = None
        mA = re.search(r'^\s*@?([a-zA-Z0-9_\-]+)\s+(?:has\s+|is\s+)?(?:just\s+)?(?:resubscribed|resubbed|subscribed|subbed|renewed)', text, re.I)
        if mA:
            user = mA.group(1).strip().lstrip('@')

        if not user:
            mB = re.search(r'^\s*@?([a-zA-Z0-9_\-]+)\s+(?:has\s+been\s+subscribed)', text, re.I)
            if mB:
                user = mB.group(1).strip().lstrip('@')

        if not user:
            mC = re.search(r'\bthanks\s+to\s+@?([a-zA-Z0-9_\-]+)\b', text, re.I)
            if mC:
                user = mC.group(1).strip().lstrip('@')

        if not user:
            mD = re.search(r'@([a-zA-Z0-9_\-]+)', text)
            if mD:
                candidate = mD.group(1).strip().lstrip('@')
                if candidate.lower() != "kickbot":
                    user = candidate

        if not user:
            mE = re.search(r'^\s*@?([a-zA-Z0-9_\-]+)[!:\s]', text)
            if mE:
                candidate = mE.group(1).strip().lstrip('@')
                if candidate.lower() not in ("thanks", "congrats", "congratulations", "shoutout", "kickbot"):
                    user = candidate

        if user and user.lower() in ("they", "been", "has", "have", "just", "for", "the", "their", "kickbot"):
            user = None

        return user, months

    def process_sub_alert(self, user, event_id=""):
        if not user:
            return
        import datetime
        clean_u = str(user).strip().lstrip('@')
        banned_users = [u.strip().lstrip("@").lower() for u in self.settings.get("banned_users", []) if u.strip()]
        if clean_u.lower() in banned_users:
            return

        u_lower = clean_u.lower()
        now_ts = datetime.datetime.now().timestamp()

        # Deduplication check
        if not hasattr(self, '_processed_sub_keys'):
            self._processed_sub_keys = {}
        if u_lower in self._processed_sub_keys and (now_ts - self._processed_sub_keys[u_lower] < 20.0):
            return

        # Buffer raw 1-sub event for 2500ms in case KickBot or celebration arrives with milestone months
        if not hasattr(self, '_pending_sub_alerts'):
            self._pending_sub_alerts = {}
        if u_lower in self._pending_sub_alerts:
            return

        timer_id = self.after(2500, self._flush_pending_sub_alert, clean_u, event_id)
        self._pending_sub_alerts[u_lower] = {
            "user": clean_u,
            "event_id": event_id,
            "timer_id": timer_id,
            "timestamp": now_ts
        }

    def _flush_pending_sub_alert(self, user, event_id=""):
        u_lower = user.lower()
        if hasattr(self, '_pending_sub_alerts'):
            self._pending_sub_alerts.pop(u_lower, None)

        now_ts = datetime.datetime.now().timestamp()
        if not hasattr(self, '_processed_sub_keys'):
            self._processed_sub_keys = {}
        if not hasattr(self, '_processed_sub_months'):
            self._processed_sub_months = {}
        if u_lower in self._processed_sub_keys and (now_ts - self._processed_sub_keys[u_lower] < 20.0):
            return
        self._processed_sub_keys[u_lower] = now_ts
        self._processed_sub_months[u_lower] = 1

        if len(self._processed_sub_keys) > 2000:
            for k in list(self._processed_sub_keys.keys())[:500]:
                del self._processed_sub_keys[k]

        # Log subscription to Kicks Donation Log
        self.log_subscription(user, count=1, sub_type="subscription", message="Subscribed", sub_id=str(event_id or user))

        # Always append to chat feed
        self.after(0, self.append_sub_chat_message, user)

        # Check if Sub Alerts are enabled
        sub_alerts_on = (
            self.settings.get("sub_alerts_enabled", True)
            and (
                self.settings.get("sub_celebration_alerts_enabled", True)
                or self.settings.get("kickbot_sub_alerts_enabled", True)
                or self.settings.get("gifted_sub_alerts_enabled", True)
            )
        )
        if not sub_alerts_on:
            self.after(0, self.append_system_log, f"\n[System] Subscription for {user} logged to donations, but Sub Alerts are disabled.\n")
            return

        kd_settings = self.settings.get("kick_donos", {})
        tts_cmd = self.get_voice_command_for_alert("sub_alerts_voice", kd_settings.get("tts_voice", ""))

        alert_msg = f"Sub Alert! {user} has just subscribed!"
        translated_user = self.translate_username(user)
        tts_alert_msg = f"Sub Alert! {translated_user} has just subscribed!"

        self.after(0, self.append_system_log, f"\n[Sub Alert] {alert_msg}\n")

        # Play alert sound if enabled in Kick Donos settings
        if kd_settings.get("sound_enabled", False):
            sound_path = kd_settings.get("sound_file", "")
            import os
            if sound_path and os.path.exists(sound_path):
                self.tts.play_audio_file(sound_path, command="Alert", category="SUB", bypass_mute=False, user=translated_user)

        # Speak TTS message using the Subs Voice Command from settings
        if tts_cmd:
            self.tts.generate_and_play(tts_cmd, tts_alert_msg, self.on_chat_message, user=translated_user, category="SUB")

    def process_resub_alert(self, user, months=1, event_id=""):
        self.process_sub_celebration_alert(user, months, "", event_id)

    def process_gifted_subs_alert(self, user, count, event_id="", is_community=False, recipient=""):
        if not user or count <= 0:
            return

        # KickBot posts must never trigger gifted sub alerts
        if str(user).strip().lstrip('@').lower() == "kickbot" or "kickbot" in str(event_id).lower():
            return

        if not hasattr(self, '_processed_sub_keys'):
            self._processed_sub_keys = {}
        if not hasattr(self, '_pending_gifts'):
            self._pending_gifts = {}
        if not hasattr(self, '_gift_lock'):
            self._gift_lock = threading.Lock()

        now_ts = datetime.datetime.now().timestamp()
        u_lower = user.lower()
        r_lower = (recipient or "").strip().lower()

        # Event ID deduplication
        if event_id:
            s_key = f"giftsub_event::{event_id}"
            if s_key in self._processed_sub_keys and (now_ts - self._processed_sub_keys[s_key] < 30):
                return
            self._processed_sub_keys[s_key] = now_ts

        comm_key = f"community_gift::{u_lower}"
        agg_key = f"recent_agg_giftsub::{u_lower}"

        # If a community gift was already processed for this user within the last 4.0s,
        # suppress any 1-sub individual recipient breakdown rows from that community gift burst
        if count == 1 and not is_community:
            if (comm_key in self._processed_sub_keys and (now_ts - self._processed_sub_keys[comm_key] < 4.0)) or \
               (agg_key in self._processed_sub_keys and (now_ts - self._processed_sub_keys[agg_key] < 4.0)):
                return

        # Case A: Community or multi-sub gift (count > 1 or is_community)
        if count > 1 or is_community:
            with self._gift_lock:
                # Cancel and discard any pending 1-sub alert buffer for this user
                pending = self._pending_gifts.pop(u_lower, None)
                if pending and "timer_id" in pending:
                    try:
                        self.after_cancel(pending["timer_id"])
                    except Exception:
                        pass
                    self.after(0, self.append_system_log, f"\n[System] Cancelled pending 1-sub alert for {user} in favor of community gift of {count} subs.\n")

            # Debounce this specific user + count within 4.0 seconds
            user_key = f"giftsub_user::{u_lower}::{count}"
            if user_key in self._processed_sub_keys and (now_ts - self._processed_sub_keys[user_key] < 4.0):
                return
            self._processed_sub_keys[user_key] = now_ts
            self._processed_sub_keys[comm_key] = now_ts
            self._processed_sub_keys[agg_key] = now_ts

            self._trigger_gifted_subs_alert(user, count)
            return

        # Case B: Single sub or individual recipient row (count == 1 and not is_community)
        with self._gift_lock:
            # If an existing pending buffer exists for this user within 800ms:
            if u_lower in self._pending_gifts:
                pending = self._pending_gifts[u_lower]
                # If this is the exact same recipient already pending in the buffer,
                # it is the duplicate Pusher/Chat event for the same gift - suppress it
                if r_lower and pending.get("recipient") == r_lower:
                    return
                if event_id and pending.get("event_id") == event_id:
                    return

                # Otherwise, it is a different recipient or second gift within 800ms -> accumulate
                pending["count"] += 1
                pending["last_seen"] = now_ts
                try:
                    self.after_cancel(pending["timer_id"])
                except Exception:
                    pass
                pending["timer_id"] = self.after(800, self._flush_pending_gift_alert, user)
                return

            # Debounce check to prevent duplicate alert between pusher event and chat message (2.0s window)
            if r_lower:
                target_key = f"giftsub_target::{u_lower}::{r_lower}"
                if target_key in self._processed_sub_keys and (now_ts - self._processed_sub_keys[target_key] < 2.0):
                    return
            else:
                user_key = f"giftsub_user::{u_lower}::1"
                if user_key in self._processed_sub_keys and (now_ts - self._processed_sub_keys[user_key] < 2.0):
                    return

            # Hold for 800ms to allow any community batch announcement (5, 20, 50 subs)
            # arriving slightly behind the first recipient row to override and cancel this
            timer_id = self.after(800, self._flush_pending_gift_alert, user)
            self._pending_gifts[u_lower] = {
                "user": user,
                "count": 1,
                "timer_id": timer_id,
                "first_seen": now_ts,
                "last_seen": now_ts,
                "event_id": event_id,
                "recipient": r_lower
            }

    def _flush_pending_gift_alert(self, user):
        u_lower = user.lower()
        with self._gift_lock:
            pending = self._pending_gifts.pop(u_lower, None)
        if not pending:
            return

        now_ts = datetime.datetime.now().timestamp()
        comm_key = f"community_gift::{u_lower}"
        agg_key = f"recent_agg_giftsub::{u_lower}"

        # If a community gift was processed while this was waiting, drop it
        if (comm_key in self._processed_sub_keys and (now_ts - self._processed_sub_keys[comm_key] < 4.0)) or \
           (agg_key in self._processed_sub_keys and (now_ts - self._processed_sub_keys[agg_key] < 4.0)):
            return

        final_count = pending.get("count", 1)
        r_lower = pending.get("recipient", "")

        if r_lower:
            target_key = f"giftsub_target::{u_lower}::{r_lower}"
            if target_key in self._processed_sub_keys and (now_ts - self._processed_sub_keys[target_key] < 2.0):
                return
            self._processed_sub_keys[target_key] = now_ts
        else:
            user_key = f"giftsub_user::{u_lower}::{final_count}"
            if user_key in self._processed_sub_keys and (now_ts - self._processed_sub_keys[user_key] < 2.0):
                return
            self._processed_sub_keys[user_key] = now_ts

        self._trigger_gifted_subs_alert(pending.get("user", user), final_count)

    def _trigger_gifted_subs_alert(self, user, count):
        if not hasattr(self, '_processed_sub_keys'):
            self._processed_sub_keys = {}

        if len(self._processed_sub_keys) > 2000:
            for k in list(self._processed_sub_keys.keys())[:500]:
                del self._processed_sub_keys[k]

        # Log subscription to Kicks Donation Log (500 kicks equivalent value each)
        self.log_subscription(user, count=count, sub_type="gifted", message=f"Gifted {count} sub{'s' if count > 1 else ''} to the chat")

        # Always append to chat feed
        self.after(0, self.append_gifted_subs_chat_message, user, count)

        # Check if alerts are enabled
        if not self.settings.get("gifted_sub_alerts_enabled", self.settings.get("gifted_sub_celebration_alerts_enabled", self.settings.get("sub_alerts_enabled", True))):
            self.after(0, self.append_system_log, f"\n[System] Gifted subs by {user} ({count} subs) logged to donations, but Gifted Sub Alerts are disabled.\n")
            return

        kd_settings = self.settings.get("kick_donos", {})
        tts_cmd = self.get_voice_command_for_alert("sub_alerts_voice", kd_settings.get("tts_voice", ""))

        # Case 3: Gifted subs -> "Gifted Subs alert! [user] just gifted [5] subs to the chat!"
        sub_word = "1 sub" if count == 1 else f"{count} subs"
        alert_msg = f"Gifted Subs alert! {user} just gifted {sub_word} to the chat!"
        translated_user = self.translate_username(user)
        tts_alert_msg = f"Gifted Subs alert! {translated_user} just gifted {sub_word} to the chat!"

        self.after(0, self.append_system_log, f"\n[Gifted Subs Alert] {alert_msg}\n")

        # Play alert sound if enabled in Kick Donos settings
        if kd_settings.get("sound_enabled", False):
            sound_path = kd_settings.get("sound_file", "")
            import os
            if sound_path and os.path.exists(sound_path):
                self.tts.play_audio_file(sound_path, command="Alert", category="SUB", bypass_mute=False, user=translated_user)

        # Speak TTS message using the Kick Donos Voice Command
        if tts_cmd:
            self.tts.generate_and_play(tts_cmd, tts_alert_msg, self.on_chat_message, user=translated_user, category="SUB")

    def process_sub_celebration_alert(self, user, months, message="", event_id=""):
        if not user:
            return

        user = str(user).strip().lstrip('@')
        try:
            months = int(months or 1)
        except Exception:
            months = 1

        # Check if message explicitly mentions milestone months (e.g. "celebrates their subscription for 19 months!")
        # If the detected months is <= 1 or message explicitly has a higher month milestone, upgrade to the explicit milestone
        m_check = re.search(
            r'\b(?:subscription\s+for\s+(\d+)\s+months?|(\d+)\s+months?\s+(?:subscription|sub|renewal)|'
            r'(?:celebrates|celebrated|celebrating|is\s+celebrating|resubscribed|resubbed|subbed|subscribed|renewed)\s+(?:their\s+|a\s+)?(?:subscription\s+for\s+)?(\d+)\s+months?|'
            r'for\s+(\d+)\s+months?|(\d+)\s+months?!)\b',
            str(message or ""), re.I
        )
        if m_check:
            for g in m_check.groups():
                if g and g.isdigit() and int(g) > months:
                    months = int(g)

        # Clean message: strip celebration announcements, user mentions, and emojis
        raw_msg_str = str(message or "").strip()
        clean_user_msg = raw_msg_str
        if user:
            clean_user_msg = re.sub(rf'^@?{re.escape(user)}\s*[:\-—,]?\s*', '', clean_user_msg, flags=re.IGNORECASE).strip()
        clean_user_msg = re.sub(
            r'^(?:@?[\w\-]+\s+)?(?:has\s+|is\s+)?(?:just\s+)?(?:resubscribed|resubbed|subscribed|subbed|renewed)!?\s*',
            '', clean_user_msg, flags=re.IGNORECASE
        ).strip()
        clean_user_msg = re.sub(
            r'^(?:they(?:\'ve|\s+have)\s+been\s+subscribed\s+(?:for\s+)?\d+\s+months?(!|\.)?)\s*',
            '', clean_user_msg, flags=re.IGNORECASE
        ).strip()
        clean_user_msg = re.sub(
            r'^(?:has|have)\s+been\s+subscribed\s+(?:for\s+)?\d+\s+months?(!|\.)?\s*',
            '', clean_user_msg, flags=re.IGNORECASE
        ).strip()
        clean_user_msg = re.sub(
            r'^(?:@?[\w\-]+\s+)?(?:celebrates|celebrated|celebrating|is\s+celebrating|resubscribed|resubbed|subbed|subscribed|renewed)\s+(?:their\s+|a\s+)?(?:subscription\s+for\s+)?(?:\d+\s+months?(!|\.)?|\b)\s*',
            '', clean_user_msg, flags=re.IGNORECASE
        ).strip()
        clean_user_msg = re.sub(
            r'^(?:subscription\s+for\s+\d+\s+months?(!|\.)?|\d+\s+months?\s+(?:subscription|sub|renewal)(!|\.)?)\s*',
            '', clean_user_msg, flags=re.IGNORECASE
        ).strip()
        clean_user_msg = re.sub(
            r'^(?:celebrated\s+their\s+)?\d+\s+months?\s+subs?(!|\.)?\s*',
            '', clean_user_msg, flags=re.IGNORECASE
        ).strip()
        clean_user_msg = re.sub(
            r'^for\s+\d+\s+months?(!|\.)?\s*',
            '', clean_user_msg, flags=re.IGNORECASE
        ).strip()
        clean_user_msg = re.sub(
            r'^\d+\s+months?(!|\.)?\s*',
            '', clean_user_msg, flags=re.IGNORECASE
        ).strip()
        clean_user_msg = re.sub(
            r'^subs?(!|\.)?\s*',
            '', clean_user_msg, flags=re.IGNORECASE
        ).strip()
        if user:
            clean_user_msg = re.sub(rf'^@?{re.escape(user)}\s*[:\-—,]?\s*', '', clean_user_msg, flags=re.IGNORECASE).strip()
        clean_user_msg = strip_emojis(clean_user_msg).strip()
        clean_user_msg = re.sub(r'^[:\-—,]\s*', '', clean_user_msg).strip()

        # If what remains is purely boilerplate like 'for 11 months!', '11 month sub', '11 months', etc.
        if re.match(r'^(?:for\s+)?\d+\s+months?(?:\s+subs?)?[!.]*$', clean_user_msg, re.IGNORECASE):
            clean_user_msg = ''
        if re.match(r'^(?:they(?:\'ve|\s+have)\s+been\s+subscribed\s+(?:for\s+)?\d+\s+months?[!.]*)$', clean_user_msg, re.IGNORECASE):
            clean_user_msg = ''
        if re.match(r'^(?:celebrated\s+their\s+)?\d+\s+months?\s+subs?[!.]*$', clean_user_msg, re.IGNORECASE):
            clean_user_msg = ''

        has_playable_text = bool(re.search(r'[a-zA-Z0-9]', clean_user_msg))

        u_lower = user.lower()
        import datetime
        now_ts = datetime.datetime.now().timestamp()

        # Cancel any pending raw 1-sub alert timer for this user
        if hasattr(self, '_pending_sub_alerts') and u_lower in self._pending_sub_alerts:
            pending = self._pending_sub_alerts.pop(u_lower, None)
            if pending and "timer_id" in pending:
                try:
                    self.after_cancel(pending["timer_id"])
                except Exception:
                    pass

        # Check if Sub Alerts are enabled
        is_kb = str(event_id or "").startswith("kb_")
        if is_kb:
            alerts_enabled = (
                self.settings.get("kickbot_sub_alerts_enabled", True)
                or self.settings.get("sub_celebration_alerts_enabled", True)
            ) and self.settings.get("sub_alerts_enabled", True)
        else:
            alerts_enabled = (
                self.settings.get("sub_celebration_alerts_enabled", self.settings.get("gifted_sub_celebration_alerts_enabled", True))
                or self.settings.get("kickbot_sub_alerts_enabled", True)
            ) and self.settings.get("sub_alerts_enabled", True)

        kd_settings = self.settings.get("kick_donos", {})
        tts_cmd = self.get_voice_command_for_alert("sub_alerts_voice", kd_settings.get("tts_voice", ""))

        # Check if that same user already sent a sub renewal for the same number of months
        already_renewed = self.is_sub_renewal_already_recorded(user, months)

        if already_renewed:
            # SECOND ALERT: The user sent a sub-renewal alert with a custom message.
            # Do NOT count the second alert towards the totals in Stream Summary (prevents double-counting $ and sub totals).
            self.after(0, self.append_system_log, f"\n[System] Sub renewal for {user} ({months} months) was already counted; evaluating custom message.\n")

            # Update existing donation log entry in donations.json if a custom message is present
            if clean_user_msg:
                try:
                    donos = self.load_donos()
                    for d in reversed(donos[-30:]):
                        if d.get("user", "").lower() == u_lower and d.get("type") == "subscription" and (d.get("months") == months or d.get("months") is None):
                            d["message"] = clean_user_msg
                            self.save_donos(donos)
                            self.after(0, self.refresh_dono_log)
                            break
                except Exception:
                    pass

            # If there is actual text in their custom message and not just emojis, play:
            # '[10] month sub renewal message from [user]. [message]'
            if has_playable_text:
                if self.has_played_sub_renewal_custom_message(user, months):
                    return
                self.mark_sub_renewal_custom_message_played(user, months)

                self.after(0, self.append_sub_renewal_message_chat_message, user, clean_user_msg)

                if not alerts_enabled:
                    self.after(0, self.append_system_log, f"\n[System] Custom sub renewal message from {user} ({months} months) received, but Sub Alerts are disabled.\n")
                    return

                translated_user = self.translate_username(user)
                clean_tts_user_msg = self.translate_usernames_in_text(clean_user_msg, self.get_active_chatters_list())
                month_num_str = str(months)
                tts_custom_msg = f"{month_num_str} month sub renewal message from {translated_user}. {clean_tts_user_msg}"
                self.after(0, self.append_system_log, f"\n[Sub Renewal Message] {month_num_str} month sub renewal message from {user}. {clean_user_msg}\n")

                if tts_cmd:
                    self.tts.generate_and_play(tts_cmd, tts_custom_msg, self.on_chat_message, user=translated_user, category="SUB")
            else:
                # If there is no playable text in their message, the app should not create a TTS message for this at all and will just ignore the celebration
                self.after(0, self.append_system_log, f"\n[System] Ignored duplicate sub renewal celebration from {user} ({months} months) as it contained no playable text message.\n")
            return

        # FIRST NOTIFICATION:
        # Check if this alert upgrades a basic 1-sub alert received just moments ago
        if not hasattr(self, '_processed_sub_keys'):
            self._processed_sub_keys = {}
        if not hasattr(self, '_processed_sub_months'):
            self._processed_sub_months = {}
        prev_ts = self._processed_sub_keys.get(u_lower, 0)
        prev_months = self._processed_sub_months.get(u_lower, 1)
        if (now_ts - prev_ts < 20.0) and prev_months == 1 and months > 1:
            self.after(0, self.append_system_log, f"\n[System] Upgrading basic 1-sub alert to {months}-month re-subscription for {user}.\n")

        self._processed_sub_keys[u_lower] = now_ts
        self._processed_sub_months[u_lower] = months

        # Record this sub renewal so future celebration messages for (user, months) won't double count
        self.record_sub_renewal(user, months, has_played_first=True, has_played_custom_msg=has_playable_text)

        # Log subscription to Kicks Donation Log and Stream Summary (1 sub = 500 kicks equivalent value)
        month_text = f"{months} months" if months != 1 else "1 month"
        sub_log_msg = clean_user_msg if clean_user_msg else f"Subscribed for {month_text}"
        self.log_subscription(user, count=1, sub_type="celebration", message=sub_log_msg, sub_id=str(event_id or f"{user}_{months}"), months=months)

        self.after(0, self.append_sub_celebration_chat_message, user, months, clean_user_msg)

        if not alerts_enabled:
            self.after(0, self.append_system_log, f"\n[System] Sub celebration for {user} ({months} months) logged to donations, but Sub Alerts are disabled.\n")
            return

        # First notification plays how the app currently plays these now:
        # "Sub renewal alert! [User] celebrates their subscription for [5] months! [message]"
        if clean_user_msg:
            alert_msg = f"Sub renewal alert! {user} celebrates their subscription for {month_text}! {clean_user_msg}"
        else:
            alert_msg = f"Sub renewal alert! {user} celebrates their subscription for {month_text}!"

        self.after(0, self.append_system_log, f"\n[Sub Renewal Alert] {alert_msg}\n")

        # Play alert sound if enabled in Kick Donos settings
        translated_user = self.translate_username(user)
        if kd_settings.get("sound_enabled", False):
            sound_path = kd_settings.get("sound_file", "")
            import os
            if sound_path and os.path.exists(sound_path):
                self.tts.play_audio_file(sound_path, command="Alert", category="SUB", bypass_mute=False, user=translated_user)

        # Speak TTS message using the Kick Donos Voice Command
        if tts_cmd:
            clean_tts_user_msg = self.translate_usernames_in_text(clean_user_msg, self.get_active_chatters_list()) if clean_user_msg else ""
            if clean_tts_user_msg:
                tts_alert_msg = f"Sub renewal alert! {translated_user} celebrates their subscription for {month_text}! {clean_tts_user_msg}"
            else:
                tts_alert_msg = f"Sub renewal alert! {translated_user} celebrates their subscription for {month_text}!"
            self.tts.generate_and_play(tts_cmd, tts_alert_msg, self.on_chat_message, user=translated_user, category="SUB")

    # Alias for backwards compatibility
    def process_sub_renewal_alert(self, user, months, message="", event_id=""):
        self.process_sub_celebration_alert(user, months, message, event_id)

    def process_kickbot_sub_alert(self, user, months, message=""):
        if not user:
            return

        import datetime
        user = str(user).strip().lstrip('@')
        months_str = str(months).strip()
        try:
            m_int = int(months_str)
        except Exception:
            m_int = 1

        banned_users = [u.strip().lstrip("@").lower() for u in self.settings.get("banned_users", []) if u.strip()]
        if user.lower() in banned_users or self.is_user_timed_out(user):
            return

        # If months > 1, this is a RE-SUBSCRIPTION!
        # Route to sub celebration alert so the app sends the correct re-subscription TTS
        # announcing tenure (e.g. 'Sub renewal alert! [User] celebrates their subscription for [X] months!')
        if m_int > 1:
            self.process_sub_celebration_alert(user, m_int, message, f"kb_{user}_{m_int}")
            return

        # Otherwise, this is a basic 1-month subscription
        self.process_sub_alert(user, f"kb_{user}_1")

    def on_chat_monitoring_error(self):
        """Called on the main Tkinter thread when Kick channel resolution or connection fails."""
        if hasattr(self, 'monitor_btn') and self.monitor_btn.winfo_exists():
            self.monitor_btn.configure(text="START MONITORING CHAT", fg_color="#005E73", hover_color="#00D1FF")
        if hasattr(self, "kickbot_listener") and self.kickbot_listener and self.kickbot_listener.is_running:
            self.kickbot_listener.stop()
        if hasattr(self, "powerchat_listener") and self.powerchat_listener and self.powerchat_listener.is_running:
            self.powerchat_listener.stop()

    def toggle_monitoring(self):
        username = self.url_entry.get().strip()
        if username != self.settings.get("kick_username"):
            self.settings["kick_username"] = username
            self.settings["url"] = username
            self.save_settings()
            
        current_btn_text = self.monitor_btn.cget("text") if hasattr(self, "monitor_btn") else ""
        if not self.scraper.is_running and "STOP" not in current_btn_text:
            self.session_start_time = datetime.datetime.now()
            self.last_stream_activity_time = datetime.datetime.now()
            if hasattr(self, 'dono_scroll') and self.dono_scroll.winfo_exists():
                self.after(0, self.refresh_dono_log)
            if hasattr(self, 'stream_summary_window') and self.stream_summary_window and self.stream_summary_window.winfo_exists():
                self.after(0, self.refresh_stream_summary_if_open)
            self.chat_box.configure(state="normal")
            self.chat_box.delete("1.0", "end")
            self.chat_box.configure(state="disabled")
            with self._active_chatters_lock:
                self._active_chatters.clear()
            self._update_active_chatters_ui(0)
            self.monitor_btn.configure(text="STOP LISTENING", fg_color="#730000", hover_color="#A90000")
            adbot_users = [u.strip().lstrip("@").lower() for u in self.settings.get("adbot_usernames", []) if u.strip()]
            self.scraper.enable_dono_scanning = True
            self.scraper.start(username, adbot_usernames=adbot_users)
            
            # Auto-start KickBot listener if enabled
            kb_settings = self.settings.get("kickbot_tts", {})
            if kb_settings.get("enabled", False) and kb_settings.get("url"):
                if hasattr(self, "kickbot_listener") and self.kickbot_listener and not self.kickbot_listener.is_running:
                    self.kickbot_listener.start(kb_settings.get("url"))

            # Auto-start Powerchat listener if enabled and link is set
            pc_enabled = self.settings.get("powerchat_enabled", True)
            pc_link = str(self.settings.get("powerchat_tts_link", "")).strip()
            if pc_enabled and pc_link:
                if hasattr(self, "powerchat_listener") and self.powerchat_listener and not self.powerchat_listener.is_running:
                    self.powerchat_listener.start(pc_link)
        else:
            self.scraper.stop()
            self.monitor_btn.configure(text="START MONITORING CHAT", fg_color="#005E73", hover_color="#00D1FF")
            
            # Stop KickBot listener when chat monitoring stops
            if hasattr(self, "kickbot_listener") and self.kickbot_listener and self.kickbot_listener.is_running:
                self.kickbot_listener.stop()

            # Stop Powerchat listener when chat monitoring stops
            if hasattr(self, "powerchat_listener") and self.powerchat_listener and self.powerchat_listener.is_running:
                self.powerchat_listener.stop()

    def get_active_chatters_duration_seconds(self):
        val = str(self.settings.get("active_chatters_duration", "20 mins"))
        mapping = {
            "10 mins": 600,
            "20 mins": 1200,
            "30 mins": 1800,
            "1 hour": 3600,
        }
        return mapping.get(val, 1200)

    def on_active_chatters_duration_changed(self, new_val):
        if new_val in ("10 mins", "20 mins", "30 mins", "1 hour"):
            self.settings["active_chatters_duration"] = new_val
            self.save_settings()
            self.recalculate_active_chatters()
            self._refresh_active_chatters_window()

    def recalculate_active_chatters(self):
        try:
            import time
            now = time.time()
            cutoff = now - self.get_active_chatters_duration_seconds()
            with self._active_chatters_lock:
                count = sum(
                    1 for data in self._active_chatters.values()
                    if (data["time"] if isinstance(data, dict) else data) >= cutoff
                )
            self._update_active_chatters_ui(count)
        except Exception:
            pass

    def record_active_chatter(self, user):
        """Track unique usernames who typed a message or emoji in chat over time."""
        if not user:
            return
        clean_u = str(user).strip().lstrip('@')
        if not clean_u:
            return
        lower_u = clean_u.lower()
        if lower_u == "system" or clean_u.startswith("SystemNative"):
            return
        
        import time
        now = time.time()
        
        with self._active_chatters_lock:
            self._active_chatters[lower_u] = {"time": now, "name": clean_u}
            # Only prune records older than the maximum retention window (24 hours)
            # This ensures that selecting 10m, 20m, 30m, or 1h accurately captures all chatters from that time window
            max_retention = 86400
            purge_cutoff = now - max_retention
            expired = [u for u, data in self._active_chatters.items() if (data["time"] if isinstance(data, dict) else data) < purge_cutoff]
            for u in expired:
                del self._active_chatters[u]
            
            cutoff = now - self.get_active_chatters_duration_seconds()
            count = sum(
                1 for data in self._active_chatters.values()
                if (data["time"] if isinstance(data, dict) else data) >= cutoff
            )
        
        self.after(0, self._update_active_chatters_ui, count)

    def _periodic_update_active_chatters(self):
        try:
            import time
            now = time.time()
            max_retention = 86400
            purge_cutoff = now - max_retention
            cutoff = now - self.get_active_chatters_duration_seconds()
            with self._active_chatters_lock:
                expired = [u for u, data in self._active_chatters.items() if (data["time"] if isinstance(data, dict) else data) < purge_cutoff]
                for u in expired:
                    del self._active_chatters[u]
                count = sum(
                    1 for data in self._active_chatters.values()
                    if (data["time"] if isinstance(data, dict) else data) >= cutoff
                )
            self._update_active_chatters_ui(count)
        except Exception:
            pass
        finally:
            self.after(5000, self._periodic_update_active_chatters)

    def get_active_chatters_list(self):
        """Returns sorted list of active chatter display names within the active duration."""
        try:
            import time
            now = time.time()
            cutoff = now - self.get_active_chatters_duration_seconds()
            with self._active_chatters_lock:
                names = [
                    (data["name"] if isinstance(data, dict) else u)
                    for u, data in self._active_chatters.items()
                    if (data["time"] if isinstance(data, dict) else data) >= cutoff
                ]
                return sorted(names, key=lambda s: s.lower())
        except Exception:
            return []

    def translate_username(self, username: str) -> str:
        """
        Translates a username when preparing to send to the TTS engine:
        - Removes any numbers at the end of a username (e.g. karasu737 -> karasu).
        - Converts all '0' to 'o' and all '3' to 'e'.
        """
        if not username:
            return ""
        import re
        u = str(username)
        # 1. Remove numbers at the end of the username
        stripped = re.sub(r'\d+$', '', u)
        if stripped:
            cleaned = stripped.rstrip('_-')
            u = cleaned if cleaned else stripped
        # 2. Convert '0' to 'o' and '3' to 'e'
        return u.replace('0', 'o').replace('3', 'e')

    def translate_usernames_in_text(self, text: str, active_chatters: list = None) -> str:
        """
        Translates usernames within text being prepared for TTS:
        - Converts all @username mentions (removes trailing numbers, converts 0 -> o, 3 -> e).
        - Converts any discrete active chatter usernames that change when translated.
        """
        if not text:
            return ""
        import re

        # 1. Translate @username mentions
        def _replace_at(m):
            return '@' + self.translate_username(m.group(1))

        result = re.sub(r'@([a-zA-Z0-9_\-]+)', _replace_at, text)

        # 2. Translate known active chatter names if present as discrete words
        if active_chatters:
            for chatter in sorted(active_chatters, key=lambda c: len(c), reverse=True):
                raw = str(chatter).lstrip('@').strip()
                # Check if it has letters and changes when translated
                if raw and re.search(r'[a-zA-Z]', raw):
                    translated = self.translate_username(raw)
                    if translated and translated != raw:
                        pattern = r'(?<![a-zA-Z0-9_])' + re.escape(raw) + r'(?![a-zA-Z0-9_])'
                        result = re.sub(pattern, translated, result, flags=re.IGNORECASE)

        return result

    def _update_active_chatters_ui(self, count):
        if hasattr(self, 'active_chatters_label') and self.active_chatters_label:
            try:
                curr = self.active_chatters_label.cget("text")
                if curr != str(count):
                    self.active_chatters_label.configure(text=str(count))
            except Exception:
                pass
        if hasattr(self, 'active_chatters_window') and self.active_chatters_window and self.active_chatters_window.winfo_exists():
            self._refresh_active_chatters_window()

    def _refresh_active_chatters_window(self):
        if hasattr(self, 'active_chatters_textbox') and self.active_chatters_textbox and self.active_chatters_textbox.winfo_exists():
            try:
                names = self.get_active_chatters_list()
                new_text = "\n".join(names)
                curr_text = self.active_chatters_textbox.get("1.0", "end-1c")
                if curr_text != new_text:
                    self.active_chatters_textbox.configure(state="normal")
                    self.active_chatters_textbox.delete("1.0", "end")
                    if new_text:
                        self.active_chatters_textbox.insert("1.0", new_text)
            except Exception:
                pass

    def open_active_chatters_window(self):
        if hasattr(self, 'active_chatters_window') and self.active_chatters_window is not None and self.active_chatters_window.winfo_exists():
            self.active_chatters_window.focus()
            if hasattr(self, 'active_chatters_duration_var') and self.active_chatters_duration_var:
                curr_val = self.settings.get("active_chatters_duration", "20 mins")
                if curr_val in ("10 mins", "20 mins", "30 mins", "1 hour"):
                    self.active_chatters_duration_var.set(curr_val)
            self._refresh_active_chatters_window()
            return

        self.active_chatters_window = ctk.CTkToplevel(self)
        self.center_toplevel(self.active_chatters_window, 315, 480)
        self.active_chatters_window.minsize(300, 350)
        self.active_chatters_window.title("ACTIVE CHATTERS")
        self.apply_dark_title_bar(self.active_chatters_window)

        main_frame = ctk.CTkFrame(self.active_chatters_window, fg_color="#0A0A0E")
        main_frame.pack(fill="both", expand=True)

        # Upper area: Title on the left, duration dropdown on the upper right
        header_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        header_frame.pack(fill="x", padx=15, pady=(15, 10))

        ctk.CTkLabel(
            header_frame,
            text="ACTIVE CHATTERS",
            font=("Arial", 16, "bold"),
            text_color="#A970FF"
        ).pack(side="left")

        current_duration = self.settings.get("active_chatters_duration", "20 mins")
        if current_duration not in ("10 mins", "20 mins", "30 mins", "1 hour"):
            current_duration = "20 mins"

        self.active_chatters_duration_var = ctk.StringVar(value=current_duration)
        duration_menu = ctk.CTkOptionMenu(
            header_frame,
            values=["10 mins", "20 mins", "30 mins", "1 hour"],
            variable=self.active_chatters_duration_var,
            width=100,
            height=28,
            font=("Arial", 12),
            dropdown_font=("Arial", 12),
            fg_color="#1e1e24",
            button_color="#2b2b36",
            button_hover_color="#3c3c4a",
            dropdown_fg_color="#1e1e24",
            command=self.on_active_chatters_duration_changed
        )
        duration_menu.pack(side="right")

        self.active_chatters_textbox = ctk.CTkTextbox(
            main_frame,
            fg_color="#1e1e24",
            text_color="#E1E1E6",
            font=("Arial", 13),
            wrap="word"
        )
        self.active_chatters_textbox.pack(fill="both", expand=True, padx=15, pady=(0, 15))
        self._make_textbox_readonly(self.active_chatters_textbox)

        self._refresh_active_chatters_window()

    def refresh_stream_summary_if_open(self):
        try:
            if hasattr(self, 'stream_summary_window') and self.stream_summary_window is not None and self.stream_summary_window.winfo_exists():
                self.populate_stream_summary()
        except Exception as e:
            self.append_system_log(f"\n[System] Error refreshing stream summary: {e}\n")

    def open_stream_summary(self):
        try:
            self._open_stream_summary_inner()
        except Exception as e:
            import traceback
            err = traceback.format_exc()
            self.append_system_log(f"\n[System] Error opening stream summary: {e}\n{err}\n")

    def _open_stream_summary_inner(self):
        if hasattr(self, 'stream_summary_window') and self.stream_summary_window is not None and self.stream_summary_window.winfo_exists():
            self.stream_summary_window.focus()
            if hasattr(self, 'summary_timeframe_var') and self.summary_timeframe_var:
                self.summary_timeframe_var.set("This Stream")
            self.populate_stream_summary()
            return

        self.stream_summary_window = ctk.CTkToplevel(self)
        self.center_toplevel(self.stream_summary_window, 380, 560)
        self.stream_summary_window.minsize(320, 380)
        self.stream_summary_window.title("Stream Summary")
        self.apply_dark_title_bar(self.stream_summary_window)
        self.stream_summary_window.configure(fg_color="#0A0A0E")

        # Top bar for centered Total display with Timeframe dropdown on right
        summary_top_bar = ctk.CTkFrame(self.stream_summary_window, fg_color="#0A0A0E")
        summary_top_bar.pack(fill="x", padx=10, pady=(10, 4))
        summary_top_bar.grid_columnconfigure(0, weight=1)
        summary_top_bar.grid_columnconfigure(1, weight=0)
        summary_top_bar.grid_columnconfigure(2, weight=1)

        # Balanced left spacer to ensure the center column is mathematically centered
        summary_spacer = ctk.CTkFrame(summary_top_bar, width=105, height=1, fg_color="transparent")
        summary_spacer.grid(row=0, column=0, sticky="w")

        self.summary_total_label = ctk.CTkLabel(
            summary_top_bar,
            text="Total: $0",
            font=("Arial", 22, "bold"),
            text_color="#00D1FF"
        )
        self.summary_total_label.grid(row=0, column=1)

        self.summary_timeframe_var = ctk.StringVar(value="This Stream")
        self.summary_timeframe_dropdown = ctk.CTkOptionMenu(
            summary_top_bar,
            values=["This Stream", "12h", "24h", "1 Week", "All Time"],
            variable=self.summary_timeframe_var,
            command=self._on_summary_timeframe_changed,
            width=105,
            height=28,
            font=("Arial", 12, "bold"),
            dropdown_font=("Arial", 12),
            fg_color="#181822",
            button_color="#262638",
            button_hover_color="#36364D",
            dropdown_fg_color="#181822",
            dropdown_text_color="#E1E1E6",
            dropdown_hover_color="#262638",
            text_color="#00D1FF"
        )
        self.summary_timeframe_dropdown.grid(row=0, column=2, sticky="e", padx=(0, 2))

        # The summary textbox directly fills the window
        self.summary_textbox = ctk.CTkTextbox(
            self.stream_summary_window,
            fg_color="#0A0A0E",
            text_color="#E1E1E6",
            font=("Arial", 19),
            wrap="word",
            corner_radius=0
        )
        self.summary_textbox.pack(fill="both", expand=True, padx=10, pady=(2, 10))

        # Custom tags configured to match Live Chat colors
        self.summary_textbox.tag_config("user", foreground="#A970FF")
        self.summary_textbox.tag_config("lime", foreground="#53FC18")
        self.summary_textbox.tag_config("normal", foreground="#E1E1E6")
        self.summary_textbox.tag_config("dim", foreground="#888888")
        self.summary_textbox.tag_config("header", foreground="#00D1FF")
        self._make_textbox_readonly(self.summary_textbox)

        self.summary_tb_all = self.summary_textbox

        self.populate_stream_summary()

    def _on_summary_timeframe_changed(self, choice):
        self.populate_stream_summary()

    def populate_stream_summary(self):
        try:
            self._populate_stream_summary_inner()
        except Exception as e:
            import traceback
            err = traceback.format_exc()
            self.append_system_log(f"\n[System] Error populating stream summary: {e}\n{err}\n")

    def _populate_stream_summary_inner(self):
        if not hasattr(self, 'stream_summary_window') or self.stream_summary_window is None or not self.stream_summary_window.winfo_exists():
            return
        tb = getattr(self, 'summary_textbox', None)
        if tb is None or not tb.winfo_exists():
            return

        timeframe = "This Stream"
        if hasattr(self, 'summary_timeframe_var') and self.summary_timeframe_var:
            try:
                timeframe = self.summary_timeframe_var.get()
            except Exception:
                timeframe = "This Stream"

        with self._summary_lock:
            raw_raids = list(getattr(self, "session_raids", []))
            raw_subs = list(getattr(self, "session_subs", []))
            raw_kicks = list(getattr(self, "session_kicks", []))
            raw_powerchats = list(getattr(self, "session_powerchat", []))

        import datetime
        now = datetime.datetime.now()

        def _parse_ts(item):
            ts = item.get("timestamp")
            if not ts:
                return None
            if isinstance(ts, (int, float)):
                try:
                    return datetime.datetime.fromtimestamp(ts)
                except Exception:
                    return None
            if isinstance(ts, datetime.datetime):
                return ts
            try:
                return datetime.datetime.fromisoformat(str(ts))
            except Exception:
                try:
                    return datetime.datetime.strptime(str(ts)[:19], "%Y-%m-%dT%H:%M:%S")
                except Exception:
                    return None

        def _filter_items(items):
            if timeframe == "This Stream":
                start_t = getattr(self, "session_start_time", None)
                if start_t is None:
                    return []
                res = []
                for it in items:
                    dt = _parse_ts(it)
                    if dt is not None and dt >= start_t:
                        res.append(it)
                return res
            elif timeframe == "12h":
                cutoff = now - datetime.timedelta(hours=12)
                res = []
                for it in items:
                    dt = _parse_ts(it)
                    if dt is not None and dt >= cutoff:
                        res.append(it)
                return res
            elif timeframe == "24h":
                cutoff = now - datetime.timedelta(hours=24)
                res = []
                for it in items:
                    dt = _parse_ts(it)
                    if dt is not None and dt >= cutoff:
                        res.append(it)
                return res
            elif timeframe == "1 Week":
                cutoff = now - datetime.timedelta(days=7)
                res = []
                for it in items:
                    dt = _parse_ts(it)
                    if dt is not None and dt >= cutoff:
                        res.append(it)
                return res
            else:  # "All Time"
                return list(items)

        raids = _filter_items(raw_raids)
        subs = _filter_items(raw_subs)
        kicks = _filter_items(raw_kicks)
        powerchats = _filter_items(raw_powerchats)

        # 1) Raids: all raids in chronological order
        # raids is already naturally in chronological order

        # 2) Subs: all subscribers/resubscribers/gifters with total sub count, descending order
        sub_agg = {}
        for item in subs:
            u = str(item.get("user", "")).strip().lstrip('@')
            if not u:
                continue
            k = u.lower()
            if k not in sub_agg:
                sub_agg[k] = {"user": u, "total": 0}
            sub_agg[k]["total"] += int(item.get("count", 1) or 1)
        sorted_subs = sorted(sub_agg.values(), key=lambda x: x["total"], reverse=True)

        # 3) Kicks: all kicks donors with total kicks donated, descending order
        kick_agg = {}
        for item in kicks:
            u = str(item.get("user", "")).strip().lstrip('@')
            if not u:
                continue
            k = u.lower()
            if k not in kick_agg:
                kick_agg[k] = {"user": u, "total": 0}
            kick_agg[k]["total"] += int(item.get("amount", 0) or 0)
        sorted_kicks = sorted(kick_agg.values(), key=lambda x: x["total"], reverse=True)

        # 4) Powerchat: all powerchat donors with total $ donated, descending order
        powerchat_agg = {}
        for item in powerchats:
            amt = float(item.get("amount", 0.0) or 0.0)
            if amt <= 0.0:
                continue
            u = str(item.get("user", "")).strip().lstrip('@')
            if not u or u.lower() in ("anonymous", "anon", "someone", "unknown", "null", "none", "undefined", "n/a", "no name", "noname"):
                u = "Anonymous"
            k = u.lower()
            if k not in powerchat_agg:
                powerchat_agg[k] = {"user": u, "total": 0.0}
            powerchat_agg[k]["total"] += amt
        sorted_powerchats = sorted(powerchat_agg.values(), key=lambda x: x["total"], reverse=True)

        empty_suffix = "this stream" if timeframe == "This Stream" else "recorded"

        def write_raids(target_tb):
            if not raids:
                target_tb.insert("end", f"No raids {empty_suffix}.\n", "dim")
                return
            for r in raids:
                r_user = str(r.get("user", "")).strip().lstrip('@')
                v_count = int(r.get("viewers", 0) or 0)
                v_word = "viewer" if v_count == 1 else "viewers"
                target_tb.insert("end", r_user, "user")
                target_tb.insert("end", " raided with ", "normal")
                target_tb.insert("end", f"{v_count} {v_word}\n", "lime")

        def write_subs(target_tb):
            if not sorted_subs:
                target_tb.insert("end", f"No subs {empty_suffix}.\n", "dim")
                return
            for s in sorted_subs:
                s_user = s["user"]
                s_total = s["total"]
                s_word = "sub" if s_total == 1 else "subs"
                target_tb.insert("end", s_user, "user")
                target_tb.insert("end", " sent ", "normal")
                target_tb.insert("end", f"{s_total} {s_word}\n", "lime")

        def write_kicks(target_tb):
            if not sorted_kicks:
                target_tb.insert("end", f"No kicks {empty_suffix}.\n", "dim")
                return
            for k in sorted_kicks:
                k_user = k["user"]
                k_total = k["total"]
                target_tb.insert("end", k_user, "user")
                target_tb.insert("end", " sent ", "normal")
                target_tb.insert("end", f"{k_total:,} kicks\n", "lime")

        def write_powerchat(target_tb):
            if not sorted_powerchats:
                target_tb.insert("end", f"No powerchats {empty_suffix}.\n", "dim")
                return
            for p in sorted_powerchats:
                p_user = p["user"]
                p_total = p["total"]
                amt_str = f"${p_total:.2f}" if p_total % 1 != 0 else f"${int(p_total):,}"
                target_tb.insert("end", p_user, "user")
                target_tb.insert("end", " sent ", "normal")
                target_tb.insert("end", f"{amt_str}\n", "lime")

        tb.configure(state="normal")
        tb.delete("1.0", "end")

        pc_enabled = self.settings.get("powerchat_enabled", True)
        if pc_enabled:
            total_powerchat_dollars = sum(p["total"] for p in sorted_powerchats)
            pc_subtotal_str = f"${total_powerchat_dollars:.2f}" if total_powerchat_dollars % 1 != 0 else f"${int(total_powerchat_dollars):,}"
            tb.insert("end", f"── POWERCHAT ──  {pc_subtotal_str}\n", "header")
            write_powerchat(tb)
            tb.insert("end", "\n")
        else:
            total_powerchat_dollars = 0.0

        total_subs_count = sum(s["total"] for s in sorted_subs)
        subs_dollars = int(total_subs_count * 5.0)
        subs_subtotal_str = f"${subs_dollars:,}"
        tb.insert("end", f"── SUBS ──  {subs_subtotal_str}\n", "header")
        write_subs(tb)
        tb.insert("end", "\n")

        total_kicks_count = sum(k["total"] for k in sorted_kicks)
        kicks_dollars = total_kicks_count * 0.01
        kicks_subtotal_str = f"${kicks_dollars:.2f}" if kicks_dollars % 1 != 0 else f"${int(kicks_dollars):,}"
        tb.insert("end", f"── KICKS ──  {kicks_subtotal_str}\n", "header")
        write_kicks(tb)
        tb.insert("end", "\n")

        tb.insert("end", "── RAIDS ──\n", "header")
        write_raids(tb)
        tb.configure(state="disabled")

        # Total dollar value: $5 per sub, $0.01 per kick, plus powerchat donations, rounded to nearest dollar
        dollar_total = int(round(total_subs_count * 5.0 + total_kicks_count * 0.01 + total_powerchat_dollars + 1e-9))
        if hasattr(self, 'summary_total_label') and self.summary_total_label.winfo_exists():
            self.summary_total_label.configure(text=f"Total: ${dollar_total:,}")

    def record_stream_activity(self):
        """
        Records that chat or donation/sub/raid activity occurred.
        If a period of inactivity (>= 3.5 hours) has elapsed since the last activity,
        automatically resets session_start_time to now so that the next day's stream is treated
        as a fresh "This Stream" session without needing to stop/restart monitoring or the app.
        """
        import datetime
        now = datetime.datetime.now()
        threshold_seconds = getattr(self, "stream_inactivity_threshold_hours", 3.5) * 3600

        sess_start = getattr(self, "session_start_time", None)
        last_act = getattr(self, "last_stream_activity_time", None)

        if sess_start is not None and last_act is not None:
            gap = (now - last_act).total_seconds()
            if gap >= threshold_seconds:
                hours_idle = gap / 3600.0
                self.session_start_time = now
                self.append_system_log(
                    f"\n[System] Stream inactivity gap detected ({hours_idle:.1f} hrs silent). "
                    f"Starting a fresh stream session for 'This Stream' at {now.strftime('%I:%M %p')}.\n"
                )
                if hasattr(self, 'stream_summary_window') and self.stream_summary_window and self.stream_summary_window.winfo_exists():
                    self.after(0, self.refresh_stream_summary_if_open)
                if hasattr(self, 'dono_scroll') and self.dono_scroll and self.dono_scroll.winfo_exists():
                    self.after(0, self.refresh_dono_log)

        self.last_stream_activity_time = now

    def record_stream_summary_activity(self):
        try:
            self.record_stream_activity()
            self.last_active_timestamp = datetime.datetime.now().timestamp()
            self.save_stream_session()
        except Exception:
            pass

    def _stream_session_heartbeat(self):
        try:
            self.last_active_timestamp = datetime.datetime.now().timestamp()
            self.save_stream_session()
        except Exception:
            pass
        finally:
            self.after(60000, self._stream_session_heartbeat)

    def load_stream_session(self):
        import json
        import os
        import datetime
        now = datetime.datetime.now()
        today_str = now.strftime("%Y-%m-%d")
        now_ts = now.timestamp()

        self.session_date = today_str
        self.last_active_timestamp = now_ts
        self.session_raids = []
        self.session_subs = []
        self.session_kicks = []
        self.session_powerchat = []

        session_file = "stream_session.json"
        if os.path.exists(session_file):
            try:
                with open(session_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                with self._summary_lock:
                    self.session_raids = list(data.get("raids", []))
                    self.session_subs = list(data.get("subs", []))
                    self.session_kicks = list(data.get("kicks", []))
                    self.session_powerchat = [
                        p for p in data.get("powerchat", [])
                        if float(p.get("amount", 0.0) or 0.0) > 0.0
                    ]
                self.append_system_log(f"\n[System] Loaded running stream session ledger with {len(self.session_raids)} raids, {len(self.session_subs)} subs, {len(self.session_kicks)} kicks, {len(self.session_powerchat)} powerchats.\n")
            except Exception as e:
                self.append_system_log(f"\n[System] Warning: Could not load stream_session.json: {e}\n")
                self.save_stream_session()
        else:
            self.save_stream_session()

    def save_stream_session(self):
        import json
        import datetime
        try:
            with self._summary_lock:
                def clean_list(items):
                    res = []
                    for it in items:
                        c = dict(it)
                        if "timestamp" in c and hasattr(c["timestamp"], "isoformat"):
                            c["timestamp"] = c["timestamp"].isoformat()
                        res.append(c)
                    return res

                data = {
                    "session_date": getattr(self, "session_date", datetime.datetime.now().strftime("%Y-%m-%d")),
                    "last_active_timestamp": getattr(self, "last_active_timestamp", datetime.datetime.now().timestamp()),
                    "raids": clean_list(getattr(self, "session_raids", [])),
                    "subs": clean_list(getattr(self, "session_subs", [])),
                    "kicks": clean_list(getattr(self, "session_kicks", [])),
                    "powerchat": clean_list([
                        p for p in getattr(self, "session_powerchat", [])
                        if float(p.get("amount", 0.0) or 0.0) > 0.0
                    ])
                }
            with open("stream_session.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            try:
                self.append_system_log(f"\n[System] Warning: Could not save stream_session.json: {e}\n")
            except Exception:
                pass

    def on_chat_message(self, user, text, command, badges=None):
        """Callback fired by the real-time WebSocket client thread"""
        try:
            self._on_chat_message_inner(user, text, command, badges)
        except Exception as e:
            import traceback
            err = traceback.format_exc()
            self.after(0, self.append_system_log, f"\n[System] FATAL ERROR IN CHAT MESSAGE HANDLER: {e}\n{err}\n")

    def _on_chat_message_inner(self, user, text, command, badges=None):
        if user == "System":
            msg = f"[System] {text}\n"
            self.after(0, self.append_system_log, msg)
            if "[Error]" in text or "lookup failed" in text.lower() or "could not find chatroom" in text.lower():
                self.after(0, self.on_chat_monitoring_error)
            return

        self.record_stream_activity()

        import re
        import datetime
        import threading
        import json

        if user == "SystemNativeDono" or str(user).startswith("SystemNative"):
            # Process internal system event payloads without emoji stripping or text body pruning
            pass
        else:
            # Track active chatters (unique usernames who typed a message or emoji in past duration).
            # Usernames on the block list / banned list are explicitly included in Active Chatters when they chat,
            # even though their messages are suppressed from displaying in the Live Chat.
            raw_user = str(user or "").lstrip('@').strip()
            clean_u = self.remove_emojis(raw_user).lstrip('@').strip() or raw_user
            self.record_active_chatter(clean_u)

            # Immediately strip emojis and Kick emote tags from user, command, and text
            user = self.remove_emojis(user).lstrip('@').strip()
            if not user:
                user = "User"
            text = self.remove_emojis(text)
            if command:
                command = self.remove_emojis(command).strip()

            # If the chat message body (excluding command) is empty after removing emojis/emotes, do not display or process (unless it's a command)
            text_body = text
            if command and text_body.lower().startswith(command.lower()):
                text_body = text_body[len(command):].strip()

            if not text_body and not command and not (badges and (badges.get("bubbles") or badges.get("is_channel_points"))):
                # The chat was only emojis/emotes with no remaining text and no command
                return
        
        # Check blocked users (case insensitive)
        clean_user = user.strip().lstrip("@").lower()
        banned_users = [u.strip().lstrip("@").lower() for u in self.settings.get("banned_users", []) if u.strip()]
        is_banned = clean_user in banned_users
        if is_banned:
            has_active_vm = False
            if self.settings.get("voice_mapping_enabled", True):
                for mapping in self.settings.get("voice_mappings", []):
                    if mapping.get("username", "").strip().lstrip("@").lower() == clean_user and mapping.get("enabled", True):
                        has_active_vm = True
                        break
            if not has_active_vm:
                return
        
        # Check timed-out users
        is_timed_out = False
        d_user = user.lower()
        if d_user in self.timeouts:
            try:
                expires = datetime.datetime.fromisoformat(self.timeouts[d_user]["expires_at"])
                if datetime.datetime.now() < expires:
                    is_timed_out = True
                else:
                    del self.timeouts[d_user]
                    self.save_timeouts()
            except:
                pass

        # --- Handle Roleplay commands first ---
        rp_enabled = self.settings.get("roleplay_enabled", False)
        is_broadcaster = badges and (badges.get("broadcaster") or badges.get("streamer") or badges.get("host"))
        if (rp_enabled or is_broadcaster) and hasattr(self, 'roleplay_manager') and not (is_banned or is_timed_out):
            clean_raw = re.sub(r'[\u200b\u200c\u200d\uFEFF]', '', text).strip()
            match = re.match(r'^(\?[a-zA-Z0-9_]+)', clean_raw)
            if match:
                rp_mentions = re.findall(r'(?<!\S)\?[a-zA-Z0-9_]+', clean_raw, flags=re.IGNORECASE)
                is_valid = True
                for m in rp_mentions:
                    m_tts = "!" + m[1:].lower()
                    v = next((v for v in self.voices if v["command"].lower() == m_tts), None)
                    if not v or (not v.get('rp_active', True) and not is_broadcaster):
                        is_valid = False
                        break
                
                if is_valid:
                    self.after(0, self.process_roleplay_command, user, text)
                    return # Do not process further in regular chat
                else:
                    self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Ignored Roleplay for message starting with '{match.group(1)}' (a voice was disabled or missing)\n")
                    return # Do not process further in regular chat

        # Handle Kick Donos & Raids BEFORE other logic
        kd_settings = self.settings.get("kick_donos", {})
        
        # Check SystemNativeRaid or Raid in chat
        raid_user = None
        raid_viewers = 0
        raid_id = ""

        if user == "SystemNativeRaid":
            try:
                r_data = json.loads(text)
                raid_user = str(r_data.get("raider", "")).strip().lstrip('@')
                raid_viewers = int(r_data.get("viewers", 0) or 0)
                raid_id = r_data.get("id", "")
                self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Received Kick Raid event: raider={raid_user}, viewers={raid_viewers}\n")
            except Exception as e:
                self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Failed to parse SystemNativeRaid JSON: {e}\n")
        elif text and ("raided with" in text.lower() or "hosting with" in text.lower() or "hosted with" in text.lower()):
            match_r = re.search(r'^(?:\[.*?\]\s*)?([a-zA-Z0-9_\-]+)\s+(?:just\s+)?(?:is\s+hosting\s+with|raided\s+with|hosted\s+with)\s+(\d+)\s+viewers', text, re.IGNORECASE)
            if match_r:
                raid_user = match_r.group(1).strip().lstrip('@')
                try:
                    raid_viewers = int(match_r.group(2))
                except Exception:
                    raid_viewers = 0

        if raid_user:
            raid_user = str(raid_user).strip().lstrip('@')
            streamer_name = str(self.settings.get("kick_username", "")).strip().lstrip('@').lower()

            # Ignore 0-viewer / uninitialized / unhost events, placeholder names, or channel self-raids
            if (
                raid_viewers <= 0
                or not raid_user
                or raid_user.lower() in ("someone", "unknown", "none", "null")
                or (streamer_name and raid_user.lower() == streamer_name)
            ):
                return

            if not hasattr(self, '_processed_raid_keys'):
                self._processed_raid_keys = {}
            r_key = f"raid::{raid_user.lower()}"
            now_ts = datetime.datetime.now().timestamp()
            # Deduplicate any duplicate raid notifications for the same user within 60 seconds
            if r_key in self._processed_raid_keys and (now_ts - self._processed_raid_keys[r_key] < 60):
                return
            self._processed_raid_keys[r_key] = now_ts
            if len(self._processed_raid_keys) > 2000:
                for k in list(self._processed_raid_keys.keys())[:500]:
                    del self._processed_raid_keys[k]

            # Record raid for Stream Summary
            if hasattr(self, '_summary_lock') and hasattr(self, 'session_raids'):
                with self._summary_lock:
                    self.session_raids.append({
                        "user": raid_user,
                        "viewers": raid_viewers,
                        "timestamp": datetime.datetime.now().isoformat()
                    })
                self.record_stream_summary_activity()
                self.after(0, self.refresh_stream_summary_if_open)

            if not self.settings.get("raid_alerts_enabled", True):
                self.after(0, self.append_system_log, f"\n[System] Raid by {raid_user} ({raid_viewers} viewers) detected but Raid Alerts are disabled.\n")
                return

            tts_cmd = self.get_voice_command_for_alert("raid_alerts_voice", kd_settings.get("tts_voice", ""))
            raid_msg = f"Raid alert! {raid_user} just raided with {raid_viewers} viewers!"
            translated_raid_user = self.translate_username(raid_user)
            tts_raid_msg = f"Raid alert! {translated_raid_user} just raided with {raid_viewers} viewers!"

            self.after(0, self.append_system_log, f"\n[Raid Alert] {raid_user} just raided with {raid_viewers} viewers!\n")
            self.after(0, self.append_raid_chat_message, raid_user, raid_viewers)

            # Play alert sound if enabled in Kick Donos settings
            if kd_settings.get("sound_enabled", False):
                sound_path = kd_settings.get("sound_file", "")
                import os
                if sound_path and os.path.exists(sound_path):
                    self.tts.play_audio_file(sound_path, command="Alert", category="RAID", bypass_mute=False, user=translated_raid_user)

            # Speak TTS message using the Kick Donos Voice Command
            if tts_cmd:
                self.tts.generate_and_play(tts_cmd, tts_raid_msg, self.on_chat_message, user=translated_raid_user, category="RAID")

            return

        # Check SystemNativeGiftedSubs
        if user == "SystemNativeGiftedSubs":
            try:
                g_data = json.loads(text)
                g_user = str(g_data.get("user", "")).strip().lstrip('@')
                if g_user.lower() == "kickbot":
                    return
                g_count = int(g_data.get("count", 1) or 1)
                g_id = g_data.get("id", "")
                is_comm = bool(g_data.get("is_community", False) or g_count > 1)
                recip = str(g_data.get("recipient", "")).strip().lstrip('@')
                self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Received Gifted Subs event: user={g_user}, count={g_count}, is_community={is_comm}, recipient={recip}\n")
                self.process_gifted_subs_alert(g_user, g_count, g_id, is_community=is_comm, recipient=recip)
                return
            except Exception as e:
                self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Failed to parse SystemNativeGiftedSubs JSON: {e}\n")
                return

        # Check SystemNativeSub
        if user == "SystemNativeSub":
            try:
                s_data = json.loads(text)
                s_user = str(s_data.get("user", "")).strip().lstrip('@')
                s_id = s_data.get("id", "")
                s_months = int(s_data.get("months", 1) or 1)
                self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Received Native Sub event: user={s_user}, months={s_months}\n")
                if s_months > 1:
                    self.process_sub_celebration_alert(s_user, s_months, "", s_id)
                else:
                    self.process_sub_alert(s_user, s_id)
                return
            except Exception as e:
                self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Failed to parse SystemNativeSub JSON: {e}\n")
                return

        # Check SystemNativeSubCelebration
        if user == "SystemNativeSubCelebration":
            try:
                c_data = json.loads(text)
                c_user = str(c_data.get("user", "")).strip().lstrip('@')
                c_months = int(c_data.get("months", 1) or 1)
                c_msg = str(c_data.get("message", "") or "")
                c_id = c_data.get("id", "")

                # Check if this sub celebration event is a KickBot sub alert
                if str(c_user).strip().lstrip('@').lower() == "kickbot" or (not c_user and any(k in c_msg.lower() for k in ("subbed", "subscribed", "subscription", "resub")) and ("month" in c_msg.lower() or "months" in c_msg.lower())):
                    kb_u, kb_m = self.parse_kickbot_sub_message(c_msg)
                    if kb_u:
                        self.process_kickbot_sub_alert(kb_u, kb_m, c_msg)
                        return

                # Double-check if c_msg or any field has an explicit higher milestone month
                m_txt = re.search(
                    r'\b(?:subscription\s+for\s+(\d+)\s+months?|(\d+)\s+months?\s+(?:subscription|sub|renewal)|'
                    r'(?:celebrates|celebrated|celebrating|is\s+celebrating|resubscribed|resubbed|subbed|subscribed|renewed)\s+(?:their\s+|a\s+)?(?:subscription\s+for\s+)?(\d+)\s+months?|'
                    r'for\s+(\d+)\s+months?|(\d+)\s+months?!)\b',
                    c_msg, re.I
                )
                if m_txt:
                    for g in m_txt.groups():
                        if g and g.isdigit() and int(g) > c_months:
                            c_months = int(g)

                self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Received Sub Celebration event: user={c_user}, months={c_months}, msg='{c_msg}'\n")
                self.process_sub_celebration_alert(c_user, c_months, c_msg, c_id)
                return
            except Exception as e:
                self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Failed to parse SystemNativeSubCelebration JSON: {e}\n")
                return

        # Check SystemNativeChannelPoints
        if user == "SystemNativeChannelPoints":
            if not self.settings.get("enable_points_tts", True):
                return
            try:
                cp_data = json.loads(text)
                cp_u = str(cp_data.get("user", "")).strip().lstrip('@')
                cp_reward = str(cp_data.get("reward", "")).strip()
                cp_id = cp_data.get("id", "")
                self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Received Channel Points event: user={cp_u}, reward={cp_reward}\n")
                self.process_channel_point_redemption(cp_u, cp_reward, event_id=cp_id)
                return
            except Exception as e:
                self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Failed to parse SystemNativeChannelPoints JSON: {e}\n")
                return

        # Check KickBot messages in chat feed:
        # KickBot posts should ONLY be used for the "Enable Sub Alerts" switch.
        clean_user_check = str(user or "").strip().lstrip('@').lower()
        if clean_user_check == "kickbot":
            clean_text_check = str(text or "").lower()
            if any(k in clean_text_check for k in ("subbed", "subscribed", "subscription", "resubbed", "resubscribed", "renew")):
                kb_user, kb_months = self.parse_kickbot_sub_message(text)
                if kb_user:
                    clean_kb_user = str(kb_user).strip().lstrip('@').lower()
                    if clean_kb_user in banned_users or self.is_user_timed_out(clean_kb_user):
                        return
                    self.process_kickbot_sub_alert(kb_user, kb_months, text)
                    return

            # All other KickBot posts (e.g. commands, alerts, gifted sub duplicate notices)
            # are suppressed from appearing in the Live Chat to avoid clutter.
            return

        # Check Channel Points redemption in chat feed
        clean_chat_text = str(text or "").strip()
        is_user_cmd = bool(command or clean_chat_text.startswith("!") or clean_chat_text.startswith("-v ") or clean_chat_text == "-v")
        has_question = "?" in clean_chat_text
        has_bubbles_badge = bool(badges and (badges.get("bubbles") or badges.get("is_channel_points")))

        if not is_user_cmd:
            # 1. Regex check for "<User> has redeemed <Reward>" or "has redeemed <Reward>" or "redeemed <Reward>"
            m_cp_chat = re.search(
                r'^\s*(?:\[.*?\]\s*)*(?:@?[\w\-]+\s+)?(?:has\s+redeemed|redeemed)\s*(?:a\s+|the\s+)?[:\s]*(.+)$',
                clean_chat_text,
                re.IGNORECASE
            )
            matched_cp_item = None
            matched_reward_text = ""

            if m_cp_chat:
                cand = m_cp_chat.group(1).strip().strip('"\'!.: ')
                for it in getattr(self, "channel_points", []):
                    it_name = str(it.get("tts_name", "")).strip()
                    if it_name and (cand.lower() == it_name.lower() or cand.lower().startswith(it_name.lower()) or it_name.lower() in cand.lower()):
                        matched_cp_item = it
                        matched_reward_text = it_name
                        break

            # 2. If bubbles badge is present or direct match against configured items
            if not matched_cp_item:
                for it in getattr(self, "channel_points", []):
                    it_name = str(it.get("tts_name", "")).strip()
                    if not it_name:
                        continue
                    # Direct match e.g. "Kiss", "[Kiss]", "Kiss!"
                    clean_no_punct = clean_chat_text.strip().strip('"\'!.:[]() ')
                    if clean_no_punct.lower() == it_name.lower() or (has_bubbles_badge and it_name.lower() in clean_no_punct.lower()):
                        matched_cp_item = it
                        matched_reward_text = it_name
                        break
                    # Or starts with "Kiss:" or "Kiss -"
                    if clean_chat_text.lower().startswith(f"{it_name.lower()}:") or clean_chat_text.lower().startswith(f"{it_name.lower()} -"):
                        matched_cp_item = it
                        matched_reward_text = it_name
                        break

            if matched_cp_item:
                if not self.settings.get("enable_points_tts", True):
                    self.after(0, self.append_system_log, f"\n[Points] Channel point redemption '{matched_reward_text}' by {user} ignored (Points TTS disabled)\n")
                    return
                self.after(0, self.append_system_log, f"\n[Points] Triggering Channel Points TTS: user={user}, reward='{matched_reward_text}'\n")
                if self.process_channel_point_redemption(user, matched_reward_text):
                    return

        # Check Gifted Subs in chat feed (Image 1) - ONLY for real non-command, non-question announcements
        if not is_user_cmd and not has_question:
            # 0. Clean lifetime stats sentence (e.g. "They've gifted 25 subscriptions in the channel.") so it does not block the gift alert or corrupt the count
            clean_sub_text = re.sub(r"(?:they've|they\s+have)\s+gifted\s+\d+\s+subscriptions?\s+in\s+the\s+channel\.?", "", text, flags=re.I).strip()
            if not clean_sub_text and re.search(r"(?:they've|they\s+have)\s+gifted\s+\d+\s+subscriptions?\s+in\s+the\s+channel", text, re.I):
                return  # Message was purely the lifetime stats sentence without any gift notification
            sub_text_to_check = clean_sub_text if clean_sub_text else text

            # 1. Community Gift: "Gifted [X] subscription(s) to the community!..."
            m_comm_gift = re.search(r'^\s*(?:\[.*?\]\s*)*(?:(?!just\b)([a-zA-Z0-9_\-]+)\s+)?(?:just\s+)?gifted\s+(\d+)\s+(?:subscriptions?|subs?)\s+to\s+the\s+community\s*!?$', sub_text_to_check, re.I)
            if not m_comm_gift:
                m_comm_gift_single = re.search(r'^\s*(?:\[.*?\]\s*)*(?:(?!just\b)([a-zA-Z0-9_\-]+)\s+)?(?:just\s+)?gifted\s+(?:a|1)\s+(?:subscription|sub)\s+to\s+the\s+community\s*!?$', sub_text_to_check, re.I)
                if m_comm_gift_single:
                    gifter = m_comm_gift_single.group(1) or user
                    if gifter.lower() == "just":
                        gifter = user
                    self.process_gifted_subs_alert(gifter, 1, is_community=True)
                    return
            else:
                gifter = m_comm_gift.group(1) or user
                if gifter.lower() == "just":
                    gifter = user
                try:
                    g_count = int(m_comm_gift.group(2))
                except Exception:
                    g_count = 1
                self.process_gifted_subs_alert(gifter, g_count, is_community=True)
                return

            # 2. General gifted subs text: "[User] gifted [5] subs..." or "[User] gifted [5] subscriptions..."
            m_gift_subs = re.search(r'^\s*(?:\[.*?\]\s*)*(?:(?!just\b)([a-zA-Z0-9_\-]+)\s+)?(?:just\s+)?gifted\s+(\d+)\s+(?:subs?|subscriptions?)\s*!?$', sub_text_to_check, re.I)
            if m_gift_subs:
                gifter = m_gift_subs.group(1) or user
                if gifter.lower() == "just":
                    gifter = user
                try:
                    g_count = int(m_gift_subs.group(2))
                except Exception:
                    g_count = 1
                self.process_gifted_subs_alert(gifter, g_count, is_community=(g_count > 1))
                return

            # 3. Individual gift recipient row: "[User] gifted a sub to [Recipient]" or "...gifted a subscription to..."
            m_indiv_gift = re.search(r'^\s*(?:\[.*?\]\s*)*(?:(?!just\b)([a-zA-Z0-9_\-]+)\s+)?(?:just\s+)?gifted\s+(?:a|1|\d+)?\s*(?:subs?|subscriptions?)\s+to\s+@?([a-zA-Z0-9_\-]+)\s*!?$', sub_text_to_check, re.I)
            if m_indiv_gift:
                recip = m_indiv_gift.group(2).strip().lstrip('@')
                if recip.lower() in ("the community", "community"):
                    return  # Handled by community regex above
                gifter = m_indiv_gift.group(1) or user
                if gifter.lower() == "just":
                    gifter = user
                if hasattr(self, '_processed_sub_keys'):
                    agg_key = f"recent_agg_giftsub::{gifter.lower()}"
                    comm_key = f"community_gift::{gifter.lower()}"
                    now_ts = datetime.datetime.now().timestamp()
                    if (agg_key in self._processed_sub_keys and (now_ts - self._processed_sub_keys[agg_key] < 4.0)) or \
                       (comm_key in self._processed_sub_keys and (now_ts - self._processed_sub_keys[comm_key] < 4.0)):
                        return  # Suppress individual recipient row from community gift batch
                self.process_gifted_subs_alert(gifter, 1, is_community=False, recipient=recip)
                return

            # Check Sub Celebration in chat feed (Image 2)
            m_cel = re.search(
                r'^\s*(?:\[\d+\]\s*)*(?:\[?(?!just\b)([a-zA-Z0-9_\-]+)\]?\s*)?:?\s*'
                r'(?:celebrates|celebrated|celebrating|is\s+celebrating|resubscribed|resubbed|subbed|subscribed|renewed)\s+(?:their\s+|a\s+)?'
                r'(?:subscription\s+for\s+(\d+)\s+months?|(\d+)\s+months?\s+(?:subscription|sub|renewal)|(?:for\s+)?(\d+)\s+months?)\s*!?'
                r'(?:\s*[:\-—,]?\s*(.*))?$',
                text,
                re.IGNORECASE
            )
            if m_cel:
                cel_user = m_cel.group(1) or user
                months_str = m_cel.group(2) or m_cel.group(3) or m_cel.group(4) or "1"
                try:
                    cel_months = int(months_str)
                except Exception:
                    cel_months = 1
                cel_msg = (m_cel.group(5) or "").strip()
                self.process_sub_celebration_alert(cel_user, cel_months, cel_msg)
                return

        dono_user = None
        dono_amount = 0
        dono_msg = ""
        dono_cid = ""
        dono_idx = ""
        
        # Check for Powerchat donation announcement in chat (e.g. from a Powerchat bot or chat integration)
        if self.settings.get("powerchat_enabled", True) and clean_chat_text:
            pc_match = re.search(r'(?:\[Powerchat\]|Powerchat:?)\s*(?:@?([\w\-]+)\s+)?(?:sent|tipped|donated)?\s*\$([0-9]+(?:\.[0-9]{1,2})?)\s*[:\-—]?\s*(.*)', clean_chat_text, re.I)
            if not pc_match:
                pc_match = re.search(r'^(?:@?([\w\-]+)\s+)?(?:just\s+)?(?:sent|tipped|donated)\s*\$([0-9]+(?:\.[0-9]{1,2})?)\s*(?:via|on|through)\s+Powerchat\s*[:\-—]?\s*(.*)', clean_chat_text, re.I)
            if pc_match:
                pc_u = (pc_match.group(1) or "").strip().lstrip('@')
                if not pc_u or pc_u.lower() in ("anonymous", "anon", "someone", "unknown", "null", "none"):
                    pc_u = "Anonymous"
                try:
                    pc_amt = float(pc_match.group(2))
                    pc_memo = (pc_match.group(3) or "").strip()
                    if pc_amt > 0:
                        pc_cid = f"chat_pc_{pc_u.lower()}_{pc_amt:.2f}_{pc_memo.lower()}"
                        if not hasattr(self, '_processed_dono_keys'):
                            self._processed_dono_keys = {}
                        now_ts = datetime.datetime.now().timestamp()
                        if pc_cid not in self._processed_dono_keys or (now_ts - self._processed_dono_keys[pc_cid] > 10):
                            self._processed_dono_keys[pc_cid] = now_ts
                            self.on_powerchat_donation(pc_u, pc_amt, pc_memo, pc_cid)
                            return
                except Exception:
                    pass

        if user == "SystemNativeDono":
            try:
                data = json.loads(text)
                dono_user = str(data.get("user", "")).strip().lstrip('@')
                if not dono_user or dono_user.lower() in ("anonymous", "anon", "someone", "unknown", "null", "none", "undefined", "n/a"):
                    dono_user = "Anonymous"
                raw_amt = data.get("amount", 0)
                if raw_amt is None: raw_amt = 0
                dono_amount = int(raw_amt)
                dono_msg = str(data.get("msg", "") or "").strip()
                dono_cid = data.get("cid", "")
                
                # Safety checks: ignore subscription notifications
                if re.search(r'\b(?:subscribed|resubscribed|subbed\s+for|gifted\s+\d+\s+sub|gifted\s+a\s+sub|tier\s*\d+\s*sub)\b', dono_msg, re.I):
                    return
                if re.search(r'^@[\w\-]+\s+(?:just\s+)?(?:gifted|sent|tipped|donated)\b', dono_msg, re.I):
                    return
                    
                self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Received Kick Dono event: user={dono_user}, amount={dono_amount}, msg='{dono_msg}'\n")
            except Exception as e:
                self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Failed to parse SystemNativeDono JSON: {e}\n")
                
        if (dono_user or dono_amount > 0) and dono_amount > 0:
            if not dono_user:
                dono_user = "Anonymous"
            # Secondary safety deduplication in main UI thread
            if not hasattr(self, '_processed_dono_keys'):
                self._processed_dono_keys = {}
            
            dono_key = f"dono::{dono_cid}" if dono_cid else f"dono_fb::{dono_user.strip().lower()}::{dono_amount}::{dono_msg.strip().lower()}"
            now_ts = datetime.datetime.now().timestamp()
            if dono_key in self._processed_dono_keys:
                ttl = 86400 if dono_cid else 1.5
                if now_ts - self._processed_dono_keys[dono_key] < ttl:
                    # Duplicate dono detected from virtual scroll remount or multi-channel dispatch, ignore
                    return
            self._processed_dono_keys[dono_key] = now_ts
            if len(self._processed_dono_keys) > 5000:
                for k in list(self._processed_dono_keys.keys())[:1000]:
                    del self._processed_dono_keys[k]
            
            # Log donation
            donos = self.load_donos()
            donos.append({
                "type": "kicks",
                "timestamp": datetime.datetime.now().isoformat(),
                "user": dono_user,
                "amount": dono_amount,
                "subs": 0,
                "message": dono_msg
            })
            self.save_donos(donos)
            self.after(0, self.refresh_dono_log)
            
            # Record kick dono for Stream Summary
            if hasattr(self, '_summary_lock') and hasattr(self, 'session_kicks'):
                with self._summary_lock:
                    self.session_kicks.append({
                        "user": dono_user,
                        "amount": dono_amount,
                        "timestamp": datetime.datetime.now().isoformat()
                    })
                self.record_stream_summary_activity()
                self.after(0, self.refresh_stream_summary_if_open)
            
            alerts_enabled = kd_settings.get("alerts_enabled", True)
            min_alert = kd_settings.get("min_alert_amount", 0)
            if not alerts_enabled or dono_amount < min_alert:
                return

            # Display in Live Chat if alerts enabled and meets min alert amount
            self.after(0, self.append_dono_chat_message, dono_user, dono_amount)
            
            # Setup TTS logic to run after sound (or immediately)
            def run_tts():
                if kd_settings.get("tts_enabled", False):
                    tts_msg = kd_settings.get("tts_message", "Thank you [user] for the [amount] kicks! [message]")
                    # Clean and format dono message (e.g. "sent Rage Quit: Wow W stream i love u" -> "Rage Quit! Wow W stream i love u")
                    clean_dono_msg = dono_msg.strip() if dono_msg else ""
                    m_sent = re.search(r'^(?:@?[\w\-]+\s+)?(?:sent|gifted|tipped|donated)\s+(.+)$', clean_dono_msg, re.DOTALL | re.IGNORECASE)
                    if m_sent:
                        after_sent = m_sent.group(1).strip()
                        lines = [l.strip() for l in after_sent.split('\n') if l.strip()]
                        while lines and re.match(r'^(?:\d+\s*(?:kicks?|tokens?)|\d+)$', lines[-1], re.IGNORECASE):
                            lines.pop()
                        if lines:
                            first_line = lines[0]
                            sep_m = re.search(r'^(.+?)\s*[:\-—]\s*(.*)$', first_line)
                            if sep_m:
                                cand = sep_m.group(1).strip()
                                rest = sep_m.group(2).strip()
                                if not re.match(r'^(?:\d+\s*(?:kicks?|tokens?)|\d+)$', cand, re.IGNORECASE):
                                    g_part = re.sub(r'[\.!]+$', '', cand).strip()
                                    rem_lines = ([rest] if rest else []) + lines[1:]
                                    rem_comm = ' '.join(rem_lines).strip()
                                    clean_dono_msg = f"{g_part}! {rem_comm}" if rem_comm else f"{g_part}!"
                                else:
                                    rem_lines = ([rest] if rest else []) + lines[1:]
                                    clean_dono_msg = ' '.join(rem_lines).strip()
                            else:
                                if re.match(r'^(?:\d+\s*(?:kicks?|tokens?)|\d+)$', first_line, re.IGNORECASE):
                                    clean_dono_msg = ' '.join(lines[1:]).strip()
                                else:
                                    g_part = re.sub(r'[\.!]+$', '', first_line).strip()
                                    rem_comm = ' '.join(lines[1:]).strip()
                                    clean_dono_msg = f"{g_part}! {rem_comm}" if rem_comm else f"{g_part}!"
                    else:
                        clean_dono_msg = re.sub(r'^sent\b\s*[:\-—,]?\s*', '', clean_dono_msg, flags=re.IGNORECASE).strip()
                    translated_dono_user = self.translate_username(dono_user)
                    clean_tts_dono_msg = self.translate_usernames_in_text(clean_dono_msg, self.get_active_chatters_list())
                    tts_msg = tts_msg.replace("[user]", translated_dono_user).replace("[amount]", str(dono_amount)).replace("[message]", clean_tts_dono_msg)
                    tts_cmd = kd_settings.get("tts_voice", "")
                    if tts_cmd:
                        self.append_system_log(f"\n[System] MAIN.PY: Triggering Kick Dono TTS using '{tts_cmd}'\n")
                        self.tts.generate_and_play(tts_cmd, tts_msg, self.on_chat_message, user=translated_dono_user, category="DONO")
            
            # Check sound file
            if kd_settings.get("sound_enabled", False):
                sound_path = kd_settings.get("sound_file", "")
                import os
                if sound_path and os.path.exists(sound_path):
                    self.tts.play_audio_file(sound_path, command="Alert", category="DONO", bypass_mute=False, user=self.translate_username(dono_user))

            # Queue TTS immediately (it will play after the alert sound because of queue order)
            run_tts()
            
            # Return immediately since it's a dono
            return

        if is_banned or is_timed_out:
            if command:
                text = re.sub(r'[\u200b\u200c\u200d\uFEFF]', '', text).strip()
                pattern = r'^' + re.escape(command) + r'\b\s*'
                text = re.sub(pattern, '', text, flags=re.IGNORECASE).strip()
                if text.lower().startswith(command.lower()):
                    text = text[len(command):].strip()
                command = None
                
            has_mapping = False
            if self.settings.get("voice_mapping_enabled", True):
                for mapping in self.settings.get("voice_mappings", []):
                    if mapping.get("username", "").strip().lstrip("@").lower() == clean_user and mapping.get("enabled", True):
                        has_mapping = True
                        break
                    
            if not has_mapping:
                return  # Completely ignore their messages

        # Handle AdBots 
        adbot_users = [u.strip().lstrip("@").lower() for u in self.settings.get("adbot_usernames", []) if u.strip()]
        if self.settings.get("ad_tts_enabled", False) and user.lower() in adbot_users:
            ad_cmd = self.settings.get("ad_tts_command", "").strip()
            if self.settings.get("ad_custom_message_enabled", False):
                ad_msg = self.settings.get("ad_custom_text", "").strip()
            else:
                ad_msg = text
                
            self.after(0, self.append_chat_message, user, ad_msg, ad_cmd)
            self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Triggering Ad TTS for {user} using '{ad_cmd}'\n")
            if ad_cmd:
                translated_user = self.translate_username(user)
                ad_tts_msg = ad_msg.replace("[user]", translated_user)
                ad_tts_msg = self.translate_usernames_in_text(ad_tts_msg, self.get_active_chatters_list())
                self.tts.generate_and_play(ad_cmd, ad_tts_msg, self.on_chat_message, user=translated_user, category="AD")
            return

        # Handle Voice Mappings (Do this first so mapped users can bypass roles)
        mapped_command = None
        if self.settings.get("voice_mapping_enabled", True):
            mappings = self.settings.get("voice_mappings", [])
            for mapping in mappings:
                if mapping.get("username", "").strip().lstrip("@").lower() == clean_user:
                    if mapping.get("enabled", True):
                        mapped_command = mapping.get("command", "")
                    break

        # Check permissions for explicitly passed commands
        if command:
            who_can_use = self.settings.get("who_can_use", {"ALL": True, "OG": False, "VIP": False, "SUB": False, "MOD": False})
            can_use_native = False
            
            # Check Streamer / Broadcaster (always has permission)
            if badges and (badges.get("broadcaster") or badges.get("streamer") or badges.get("host")):
                can_use_native = True
            # Check ALL allowed
            elif who_can_use.get("ALL", True):
                can_use_native = True
            # Check mapped users (bypass role restrictions)
            elif mapped_command:
                can_use_native = True
            # Check specific roles enabled in WHO CAN USE
            elif badges:
                has_og = badges.get("og") or badges.get("is_og")
                has_mod = badges.get("mod") or badges.get("moderator") or badges.get("is_mod")
                has_vip = badges.get("vip") or badges.get("is_vip")
                has_sub = badges.get("sub") or badges.get("subscriber") or badges.get("is_sub")

                if who_can_use.get("OG") and has_og:
                    can_use_native = True
                if who_can_use.get("MOD") and has_mod:
                    can_use_native = True
                if who_can_use.get("VIP") and has_vip:
                    can_use_native = True
                if who_can_use.get("SUB") and has_sub:
                    can_use_native = True
                
            if not can_use_native:
                self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Command '{command}' by {user} ignored: lacking roles.\n")
                command = None

        skip_tts = False
        if mapped_command:
            if command and command.lower() == mapped_command.lower():
                skip_tts = True
            elif text.lower().startswith("-v ") or text.lower() == "-v":
                skip_tts = True
                if text.lower() == "-v":
                    text = mapped_command
                else:
                    text = mapped_command + text[2:]

        if not command:
            command = mapped_command

        # Truncate to nearest word based on max char limit
        max_chars = int(self.settings.get("max_chars", 300))
        if len(text) > max_chars:
            truncated = text[:max_chars]
            last_space = truncated.rfind(' ')
            if last_space > 0:
                text = truncated[:last_space]
            else:
                text = truncated

        if not is_banned:
            self.after(0, self.append_chat_message, user, text, command)

        # Trigger TTS engine if a specific command was detected
        if skip_tts:
            self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Ignored TTS for {user} due to matching self-mapped command '{command}'.\n")
        elif command:
            # Check if command is active in UI
            is_active = False
            actual_command = command
            for v in self.voices:
                if v["command"].lower() == command.lower():
                    is_active = v.get("active", True)
                    actual_command = v["command"]
                    break
            
            if is_active:
                self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Triggering generate_and_play for '{actual_command}'\n")
                
                # Determine category
                cat = "VM" if (mapped_command and actual_command.lower() == mapped_command.lower()) else "Standard"
                tts_text = self.translate_usernames_in_text(text, self.get_active_chatters_list())
                translated_user = self.translate_username(user)
                self.tts.generate_and_play(actual_command, tts_text, self.on_chat_message, category=cat, user=translated_user)

                # If user is on the block list but in an active voicemapping and triggered TTS, display chat in app
                if is_banned:
                    self.after(0, self.append_chat_message, user, text, actual_command, True)
            else:
                self.after(0, self.append_system_log, f"\n[System] MAIN.PY: Ignored TTS for '{actual_command}' (disabled in Voice Library)\n")

    def remove_emojis(self, text):
        import re
        if not text:
            return ""
        # 1. Remove Kick emote tags like [emote:5748002:collectiblesGoldenL]
        t = re.sub(r'\[emote:[^\]]*\]', '', str(text), flags=re.IGNORECASE)
        # 2. Remove characters outside the Basic Multilingual Plane (BMP) (Unicode emojis)
        t = re.sub(r'[^\u0000-\uFFFF]', '', t)
        # 3. Remove misc emoji symbols, variation selectors, zero-width spaces/joiners
        t = re.sub(r'[\u2600-\u27BF\u2B50-\u2B55\uFE00-\uFE0F\u200B-\u200D\uFEFF]', '', t)
        # 4. Collapse multiple spaces into single space
        t = re.sub(r'[ \t]+', ' ', t).strip()
        return t

    def append_system_log(self, msg):
        print(msg.strip())

    def on_chat_scrollbar_press(self, event):
        self.is_user_scrolling = True
        if hasattr(self, 'resume_scrolling_timer') and self.resume_scrolling_timer:
            self.after_cancel(self.resume_scrolling_timer)
            self.resume_scrolling_timer = None

    def on_chat_scrollbar_release(self, event):
        if hasattr(self, 'resume_scrolling_timer') and self.resume_scrolling_timer:
            self.after_cancel(self.resume_scrolling_timer)
        self.resume_scrolling_timer = self.after(3000, self.resume_chat_scrolling)

    def on_chat_mouse_wheel(self, event):
        self.is_user_scrolling = True
        if hasattr(self, 'resume_scrolling_timer') and self.resume_scrolling_timer:
            self.after_cancel(self.resume_scrolling_timer)
        self.resume_scrolling_timer = self.after(3000, self.resume_chat_scrolling)

    def resume_chat_scrolling(self):
        self.is_user_scrolling = False
        self.resume_scrolling_timer = None
        self.after(10, lambda: self.chat_box.see("end"))


    def update_tts_queue_ui(self):
        # Only update if TTS queue frame is visible
        if not hasattr(self, "tts_queue_frame") or not self.tts_queue_frame.winfo_ismapped():
            return
            
        queue_state = self.tts.get_queue_state()
        
        # Keep track of widgets to update without flickering
        if not hasattr(self, "_tts_queue_widgets"):
            self._tts_queue_widgets = {}

        current_state_signature = [(item['group_id'], item['status'], len(item.get('commands', []))) for item in queue_state]
        
        # Check if structure changed
        structure_changed = False
        if getattr(self, "_last_queue_signature", None) != current_state_signature:
            structure_changed = True
            self._last_queue_signature = current_state_signature
            
            # Clear existing
            for widget in self.tts_queue_scrollable.winfo_children():
                widget.destroy()
            self._tts_queue_widgets = {}
            if hasattr(self.tts_queue_scrollable, "_parent_canvas"):
                self.tts_queue_scrollable._parent_canvas.yview_moveto(0)
        
        if not structure_changed:
            # Just update progress bars
            for item in queue_state:
                gid = item['group_id']
                if gid in self._tts_queue_widgets:
                    prog = item.get("progress", 0.0)
                    w = self._tts_queue_widgets[gid]
                    w["progress"] = prog
                    c = w["canvas"]
                    width = c.winfo_width()
                    height = c.winfo_height()
                    if width > 1: # ensure it's mapped
                        c.coords(w["rect_id"], 0, 0, int(width * prog), height)
                    
                    status = item['status']
                    cat = item['category']
                    status_indicator = ""
                    if status == "Playing":
                        status_indicator = "♪ "
                    elif status == "Processing":
                        status_indicator = "⚙ "
                    elif status == "Waiting to Play":
                        status_indicator = "⏳ "
                    elif status == "Generating LLM":
                        status_indicator = "🧠 "
                    elif status == "Waiting for LLM":
                        status_indicator = "💤 "
                    cmds_str = ", ".join(item.get("commands", [item.get("command", "")]))
                    display_text = f"{status_indicator}[{'TTS' if cat == 'Standard' else cat}] {item.get('user', 'Unknown')} - {cmds_str}"
                    if len(display_text) > 80:
                        display_text = display_text[:77] + "..."
                    c.itemconfig(w["text_id"], text=display_text)
            return
            
        pass
            
        if not queue_state:
            import customtkinter as ctk
            ctk.CTkLabel(self.tts_queue_scrollable, text="TTS Queue is empty.", text_color="#888888", font=("Arial", 12)).pack(pady=20)
            return
            
        import customtkinter as ctk
        import tkinter as tk
        
        # Build Context Menu
        if not hasattr(self, "tts_queue_menu"):
            self.tts_queue_menu = tk.Menu(self.rp_panel, tearoff=False)
            self.tts_queue_menu.add_command(label="Move to Top", command=lambda: self.tts.move_to_top(self._context_group_id))
            self.tts_queue_menu.add_command(label="Delete", command=lambda: self.tts.delete_from_queue(self._context_group_id))
        
        def show_context(event, gid):
            self._context_group_id = gid
            self.tts_queue_menu.tk_popup(event.x_root, event.y_root)

        for item in queue_state:
            gid = item['group_id']
            status = item['status']
            cat = item['category']
            
            # Format display
            prefix = "[TTS] "
            color = "#00FF00" # Green
            if cat == "RP":
                prefix = "[RP] "
                color = "#00BFFF" # Light Blue
            elif cat == "DONO":
                prefix = "[DONO] "
                color = "#FF4444" # Red
            elif cat in ("SUB", "RESUB", "GIFTSUB", "SUB_CELEBRATION", "SUBSCRIPTION"):
                prefix = "[SUB] "
                color = "#9B59B6" # Purple
            elif cat == "RAID":
                prefix = "[RAID] "
                color = "#E67E22" # Amber / Orange
            elif cat == "VM":
                prefix = "[VM] "
                color = "#FFA500" # Orange
            elif cat == "AD":
                prefix = "[AD] "
                color = "#F1C40F" # Yellow
                
            status_indicator = ""
            if status == "Playing":
                status_indicator = "♪ "
            elif status == "Processing":
                status_indicator = "⚙ "
            elif status == "Waiting to Play":
                status_indicator = "⏳ "
            elif status == "Generating LLM":
                status_indicator = "🧠 "
            elif status == "Waiting for LLM":
                status_indicator = "💤 "
                
            cmds_str = ", ".join(item.get("commands", [item.get("command", "")]))
            display_text = f"{status_indicator}{prefix} {item.get('user', 'Unknown')} - {cmds_str}"
            if len(display_text) > 110:
                display_text = display_text[:107] + "..."
                
            if cat == "RP":
                bg_color = "#003355"
                fill_color = "#0088CC"
            elif cat == "VM":
                bg_color = "#552200"
                fill_color = "#CC5500"
            elif cat == "DONO":
                bg_color = "#550000"
                fill_color = "#CC0000"
            elif cat in ("SUB", "RESUB", "GIFTSUB", "SUB_CELEBRATION", "SUBSCRIPTION"):
                bg_color = "#3A1A4A"
                fill_color = "#8E44AD"
            elif cat == "RAID":
                bg_color = "#4A2800"
                fill_color = "#D35400"
            elif cat == "AD":
                bg_color = "#4A4000"
                fill_color = "#B7950B"
            else:
                bg_color = "#004400"
                fill_color = "#00BB00"
                
            frame = ctk.CTkFrame(self.tts_queue_scrollable, fg_color="transparent", height=32)
            frame.pack(fill="x", pady=2, padx=5)
            frame.pack_propagate(False)
            
            canvas = tk.Canvas(frame, bg=bg_color, highlightthickness=0)
            canvas.pack(fill="both", expand=True)
            
            progress = item.get("progress", 0.0)
            rect_id = canvas.create_rectangle(0, 0, 0, 32, fill=fill_color, outline="")
            text_id = canvas.create_text(10, 16, text=display_text, fill="white", font=("Arial", 9, "bold"), anchor="w")
            
            widgets = {"canvas": canvas, "rect_id": rect_id, "text_id": text_id, "progress": progress}
            self._tts_queue_widgets[gid] = widgets
            
            canvas.bind("<Button-1>", lambda e, g=gid: show_context(e, g))
            canvas.bind("<Button-3>", lambda e, g=gid: show_context(e, g))

    def _make_textbox_readonly(self, textbox):
        """Makes a CTkTextbox read-only (prevents typing/editing/cutting/pasting, allows selecting & copying)."""
        def _block_keys(event):
            # Allow Ctrl+C / Cmd+C (copy), Ctrl+A (select all), Ctrl+Insert (copy)
            is_ctrl = bool(event.state & 0x0004)
            if is_ctrl and (event.keysym.lower() in ('c', 'a', 'insert')):
                return None
            # Allow navigation keys: Up, Down, Left, Right, Home, End, Prior (PageUp), Next (PageDown), modifiers
            if event.keysym in ('Up', 'Down', 'Left', 'Right', 'Home', 'End', 'Prior', 'Next', 'Shift_L', 'Shift_R', 'Control_L', 'Control_R', 'Alt_L', 'Alt_R'):
                return None
            return "break"

        try:
            textbox.bind("<Key>", _block_keys, add="+")
            textbox.bind("<<Paste>>", lambda e: "break", add="+")
            textbox.bind("<<Cut>>", lambda e: "break", add="+")
            textbox.bind("<<Clear>>", lambda e: "break", add="+")
            if hasattr(textbox, "_textbox"):
                textbox._textbox.bind("<Key>", _block_keys, add="+")
                textbox._textbox.bind("<<Paste>>", lambda e: "break", add="+")
                textbox._textbox.bind("<<Cut>>", lambda e: "break", add="+")
                textbox._textbox.bind("<<Clear>>", lambda e: "break", add="+")
        except Exception:
            pass

    def append_rp_user_message(self, username, user_message):
        """Safely append user roleplay trigger prompt to the Roleplay Chats Feed"""
        try:
            self.rp_chat_box.insert("end", f"\n{username}: ", "user")
            self.rp_chat_box.insert("end", f"{user_message}\n")

            # Prevent UI lag by truncating chat history to 500 lines
            try:
                line_count = int(self.rp_chat_box.index("end-1c").split(".")[0])
                if line_count > 500:
                    self.rp_chat_box.delete("1.0", f"{line_count - 500 + 1}.0")
            except Exception:
                pass

            self.rp_chat_box.see("end")
        except Exception as e:
            self.append_system_log(f"\n[Roleplay Feed Error] {e}")

    def append_rp_dialogue(self, command, text):
        """Safely append character dialogue to the Roleplay Chats Feed and return (start_line, end_line)"""
        start_line = 1
        end_line = 1
        try:
            try:
                start_line = int(self.rp_chat_box.index("end-1c").split(".")[0])
            except Exception:
                start_line = 1

            self.rp_chat_box.insert("end", f"{command}: ", "command")
            self.rp_chat_box.insert("end", f"{text}\n")
            self.rp_chat_box.see("end")

            try:
                end_line = int(self.rp_chat_box.index("end-1c").split(".")[0])
            except Exception:
                end_line = start_line
        except Exception as e:
            self.append_system_log(f"\n[Roleplay Feed Error] {e}")

        return start_line, end_line

    def append_chat_message(self, user, text, command, allow_banned=False):
        clean_user = str(user or "").strip().lstrip("@").lower()
        if clean_user == "kickbot":
            return

        banned_users = [u.strip().lstrip("@").lower() for u in self.settings.get("banned_users", []) if u.strip()]
        if clean_user in banned_users and not allow_banned:
            return

        clean_text = self.remove_emojis(text).strip()
        msg_body = clean_text
        if command and msg_body.lower().startswith(command.lower()):
            msg_body = msg_body[len(command):].strip()
        
        # If there is no message body and no command, do not display
        if not msg_body and not command:
            return

        tag_name = f"user_{user}"
        self.chat_box.configure(state="normal")
        self.chat_box.tag_config(tag_name, foreground="#A970FF")
        self.chat_box.tag_bind(tag_name, "<Button-1>", lambda event, u=user: self.show_user_context_menu(event, u))
        
        self.chat_box.insert("end", f"[{user}] ", tag_name)
        if command:
            self.chat_box.insert("end", f"{command}", "command")
            if msg_body:
                self.chat_box.insert("end", f" {msg_body}\n")
            else:
                self.chat_box.insert("end", "\n")
        else:
            self.chat_box.insert("end", f"{msg_body}\n")
        
        # Prevent UI lag by truncating chat history to 300 lines
        line_count = int(float(self.chat_box.index("end-1c")))
        if line_count > 300:
            self.chat_box.delete("1.0", f"{line_count - 300 + 1}.0")

        self.chat_box.configure(state="disabled")

        if not getattr(self, "is_user_scrolling", False):
            self.after(10, lambda: self.chat_box.see("end"))

    def append_dono_chat_message(self, user, amount):
        banned_users = [u.strip().lstrip("@").lower() for u in self.settings.get("banned_users", []) if u.strip()]
        if str(user or "").strip().lstrip("@").lower() in banned_users:
            return

        tag_name = f"user_{user}"
        self.chat_box.configure(state="normal")
        self.chat_box.tag_config(tag_name, foreground="#A970FF")
        self.chat_box.tag_bind(tag_name, "<Button-1>", lambda event, u=user: self.show_user_context_menu(event, u))
        
        self.chat_box.insert("end", f"[{user}] ", tag_name)
        self.chat_box.insert("end", "sent ")
        self.chat_box.insert("end", f"{amount} Kicks", "dono_lime")
        self.chat_box.insert("end", "\n")
        
        # Prevent UI lag by truncating chat history to 300 lines
        line_count = int(float(self.chat_box.index("end-1c")))
        if line_count > 300:
            self.chat_box.delete("1.0", f"{line_count - 300 + 1}.0")

        self.chat_box.configure(state="disabled")

        if not getattr(self, "is_user_scrolling", False):
            self.after(10, lambda: self.chat_box.see("end"))

    def _should_suppress_duplicate_sub_chat(self, user):
        user_clean = str(user or "").strip().lstrip('@').lower()
        if not user_clean:
            return True
        import datetime
        now_ts = datetime.datetime.now().timestamp()
        if not hasattr(self, '_displayed_user_subs'):
            self._displayed_user_subs = {}
        if user_clean in self._displayed_user_subs and (now_ts - self._displayed_user_subs[user_clean] < 15.0):
            return True
        self._displayed_user_subs[user_clean] = now_ts
        if len(self._displayed_user_subs) > 500:
            for k in list(self._displayed_user_subs.keys())[:100]:
                del self._displayed_user_subs[k]
        return False

    def append_sub_chat_message(self, user):
        banned_users = [u.strip().lstrip("@").lower() for u in self.settings.get("banned_users", []) if u.strip()]
        if str(user or "").strip().lstrip("@").lower() in banned_users:
            return
        if self._should_suppress_duplicate_sub_chat(user):
            return

        tag_name = f"user_{user}"
        self.chat_box.configure(state="normal")
        self.chat_box.tag_config(tag_name, foreground="#A970FF")
        self.chat_box.tag_bind(tag_name, "<Button-1>", lambda event, u=user: self.show_user_context_menu(event, u))
        
        self.chat_box.insert("end", f"[{user}] ", tag_name)
        self.chat_box.insert("end", "just ")
        self.chat_box.insert("end", "subscribed", "dono_lime")
        self.chat_box.insert("end", "\n")
        
        # Prevent UI lag by truncating chat history to 300 lines
        line_count = int(float(self.chat_box.index("end-1c")))
        if line_count > 300:
            self.chat_box.delete("1.0", f"{line_count - 300 + 1}.0")

        self.chat_box.configure(state="disabled")

        if not getattr(self, "is_user_scrolling", False):
            self.after(10, lambda: self.chat_box.see("end"))

    def append_resub_chat_message(self, user):
        banned_users = [u.strip().lstrip("@").lower() for u in self.settings.get("banned_users", []) if u.strip()]
        if str(user or "").strip().lstrip("@").lower() in banned_users:
            return
        if self._should_suppress_duplicate_sub_chat(user):
            return

        tag_name = f"user_{user}"
        self.chat_box.configure(state="normal")
        self.chat_box.tag_config(tag_name, foreground="#A970FF")
        self.chat_box.tag_bind(tag_name, "<Button-1>", lambda event, u=user: self.show_user_context_menu(event, u))
        
        self.chat_box.insert("end", f"[{user}] ", tag_name)
        self.chat_box.insert("end", "renewed their ")
        self.chat_box.insert("end", "subscription", "dono_lime")
        self.chat_box.insert("end", "\n")
        
        # Prevent UI lag by truncating chat history to 300 lines
        line_count = int(float(self.chat_box.index("end-1c")))
        if line_count > 300:
            self.chat_box.delete("1.0", f"{line_count - 300 + 1}.0")

        self.chat_box.configure(state="disabled")

        if not getattr(self, "is_user_scrolling", False):
            self.after(10, lambda: self.chat_box.see("end"))

    def append_gifted_subs_chat_message(self, user, count):
        banned_users = [u.strip().lstrip("@").lower() for u in self.settings.get("banned_users", []) if u.strip()]
        if str(user or "").strip().lstrip("@").lower() in banned_users:
            return

        tag_name = f"user_{user}"
        self.chat_box.configure(state="normal")
        self.chat_box.tag_config(tag_name, foreground="#A970FF")
        self.chat_box.tag_bind(tag_name, "<Button-1>", lambda event, u=user: self.show_user_context_menu(event, u))
        
        sub_text = f"{count} sub" if count == 1 else f"{count} subs"
        self.chat_box.insert("end", f"[{user}] ", tag_name)
        self.chat_box.insert("end", f"gifted {sub_text}", "dono_lime")
        self.chat_box.insert("end", "\n")
        
        # Prevent UI lag by truncating chat history to 300 lines
        line_count = int(float(self.chat_box.index("end-1c")))
        if line_count > 300:
            self.chat_box.delete("1.0", f"{line_count - 300 + 1}.0")

        self.chat_box.configure(state="disabled")

        if not getattr(self, "is_user_scrolling", False):
            self.after(10, lambda: self.chat_box.see("end"))

    def append_sub_celebration_chat_message(self, user, months, message=""):
        banned_users = [u.strip().lstrip("@").lower() for u in self.settings.get("banned_users", []) if u.strip()]
        if str(user or "").strip().lstrip("@").lower() in banned_users:
            return
        if not (message and str(message).strip()) and self._should_suppress_duplicate_sub_chat(user):
            return

        tag_name = f"user_{user}"
        self.chat_box.configure(state="normal")
        self.chat_box.tag_config(tag_name, foreground="#A970FF")
        self.chat_box.tag_bind(tag_name, "<Button-1>", lambda event, u=user: self.show_user_context_menu(event, u))
        
        month_sub_text = f"{months} month sub"
        self.chat_box.insert("end", f"[{user}] ", tag_name)
        self.chat_box.insert("end", "celebrated their ")
        self.chat_box.insert("end", month_sub_text, "dono_lime")
        if message and str(message).strip():
            self.chat_box.insert("end", f": {str(message).strip()}")
        self.chat_box.insert("end", "\n")
        
        # Prevent UI lag by truncating chat history to 300 lines
        line_count = int(float(self.chat_box.index("end-1c")))
        if line_count > 300:
            self.chat_box.delete("1.0", f"{line_count - 300 + 1}.0")

        self.chat_box.configure(state="disabled")

        if not getattr(self, "is_user_scrolling", False):
            self.after(10, lambda: self.chat_box.see("end"))

    def append_sub_renewal_message_chat_message(self, user, message=""):
        banned_users = [u.strip().lstrip("@").lower() for u in self.settings.get("banned_users", []) if u.strip()]
        if str(user or "").strip().lstrip("@").lower() in banned_users:
            return

        tag_name = f"user_{user}"
        self.chat_box.configure(state="normal")
        self.chat_box.tag_config(tag_name, foreground="#A970FF")
        self.chat_box.tag_bind(tag_name, "<Button-1>", lambda event, u=user: self.show_user_context_menu(event, u))
        
        self.chat_box.insert("end", f"[{user}] ", tag_name)
        self.chat_box.insert("end", "sent a ")
        self.chat_box.insert("end", "re-sub message", "dono_lime")
        if message and str(message).strip():
            self.chat_box.insert("end", f": {str(message).strip()}")
        self.chat_box.insert("end", "\n")
        
        # Prevent UI lag by truncating chat history to 300 lines
        line_count = int(float(self.chat_box.index("end-1c")))
        if line_count > 300:
            self.chat_box.delete("1.0", f"{line_count - 300 + 1}.0")

        self.chat_box.configure(state="disabled")

        if not getattr(self, "is_user_scrolling", False):
            self.after(10, lambda: self.chat_box.see("end"))

    def append_kickbot_sub_chat_message(self, user, months):
        banned_users = [u.strip().lstrip("@").lower() for u in self.settings.get("banned_users", []) if u.strip()]
        if str(user or "").strip().lstrip("@").lower() in banned_users:
            return

        user_clean = str(user or "").strip().lstrip('@')
        if not user_clean:
            return

        if self._should_suppress_duplicate_sub_chat(user_clean):
            return

        try:
            m_int = int(months)
        except Exception:
            m_int = 1

        tag_name = f"user_{user_clean}"
        self.chat_box.configure(state="normal")
        self.chat_box.tag_config(tag_name, foreground="#A970FF")
        self.chat_box.tag_bind(tag_name, "<Button-1>", lambda event, u=user_clean: self.show_user_context_menu(event, u))

        m_word = "month" if m_int == 1 else "months"

        self.chat_box.insert("end", f"[{user_clean}] ", tag_name)
        self.chat_box.insert("end", "just ")
        self.chat_box.insert("end", f"subscribed for {m_int} {m_word}", "dono_lime")
        self.chat_box.insert("end", "\n")

        # Prevent UI lag by truncating chat history to 300 lines
        line_count = int(float(self.chat_box.index("end-1c")))
        if line_count > 300:
            self.chat_box.delete("1.0", f"{line_count - 300 + 1}.0")

        self.chat_box.configure(state="disabled")

        if not getattr(self, "is_user_scrolling", False):
            self.after(10, lambda: self.chat_box.see("end"))

    def append_raid_chat_message(self, user, viewers):
        banned_users = [u.strip().lstrip("@").lower() for u in self.settings.get("banned_users", []) if u.strip()]
        if str(user or "").strip().lstrip("@").lower() in banned_users:
            return

        tag_name = f"user_{user}"
        self.chat_box.configure(state="normal")
        self.chat_box.tag_config(tag_name, foreground="#A970FF")
        self.chat_box.tag_bind(tag_name, "<Button-1>", lambda event, u=user: self.show_user_context_menu(event, u))
        
        viewers_str = f"raided with {viewers} viewers" if viewers != 1 else f"raided with {viewers} viewer"
        self.chat_box.insert("end", f"[{user}] ", tag_name)
        self.chat_box.insert("end", "just ")
        self.chat_box.insert("end", viewers_str, "raid_blue")
        self.chat_box.insert("end", "\n")
        
        # Prevent UI lag by truncating chat history to 300 lines
        line_count = int(float(self.chat_box.index("end-1c")))
        if line_count > 300:
            self.chat_box.delete("1.0", f"{line_count - 300 + 1}.0")

        self.chat_box.configure(state="disabled")

        if not getattr(self, "is_user_scrolling", False):
            self.after(10, lambda: self.chat_box.see("end"))

    def load_timeouts(self):
        import json, os
        if os.path.exists("timeouts.json"):
            try:
                with open("timeouts.json", "r") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        return data
                    return {}
            except:
                return {}
        return {}

    def save_timeouts(self):
        import json
        try:
            with open("timeouts.json", "w") as f:
                json.dump(self.timeouts, f, indent=4)
        except Exception as e:
            print(f"[Error] Failed to save timeouts: {e}")

    def check_timeouts_loop(self):
        import datetime
        now = datetime.datetime.now()
        changed = False
        expired_users = []
        for d_user, t in list(self.timeouts.items()):
            try:
                expires = datetime.datetime.fromisoformat(t["expires_at"])
                if now >= expires:
                    expired_users.append(d_user)
                    changed = True
            except:
                expired_users.append(d_user)
                changed = True
                
        for u in expired_users:
            del self.timeouts[u]
            
        if changed:
            self.save_timeouts()
            if hasattr(self, 'timeouts_window') and self.timeouts_window is not None and self.timeouts_window.winfo_exists():
                self.refresh_timeouts_ui()
                
        self.after(60000, self.check_timeouts_loop)

    def is_user_timed_out(self, username):
        if not username:
            return False
        clean = str(username).strip().lstrip("@").lower()
        if not hasattr(self, "timeouts") or not isinstance(self.timeouts, dict):
            return False
        if clean in self.timeouts:
            try:
                import datetime
                expires = datetime.datetime.fromisoformat(self.timeouts[clean]["expires_at"])
                if datetime.datetime.now() < expires:
                    return True
                else:
                    del self.timeouts[clean]
                    self.save_timeouts()
            except Exception:
                pass
        return False

    def get_active_timed_out_usernames(self):
        if not hasattr(self, "timeouts") or not isinstance(self.timeouts, dict):
            return []
        import datetime
        now = datetime.datetime.now()
        active = []
        for d_user, t in list(self.timeouts.items()):
            try:
                expires = datetime.datetime.fromisoformat(t["expires_at"])
                if now < expires:
                    active.append(d_user)
            except Exception:
                pass
        return active

    def apply_timeout(self, user, minutes):
        user = user.strip().lstrip('@')
        import datetime
        now = datetime.datetime.now()
        expires = now + datetime.timedelta(minutes=minutes)
        self.timeouts[user.lower()] = {
            "username": user,
            "timestamp": now.isoformat(),
            "expires_at": expires.isoformat()
        }
        self.save_timeouts()
        self.after(0, self.append_system_log, f"\n[System] User '{user}' timed out for {minutes} minute(s).")
        if hasattr(self, 'timeouts_window') and self.timeouts_window is not None and self.timeouts_window.winfo_exists():
            self.refresh_timeouts_ui()

    def apply_perma(self, user):
        user = user.strip().lstrip('@')
        banned = self.settings.get("banned_users", [])
        if user.lower() not in [u.strip().lstrip('@').lower() for u in banned]:
            banned.append(user.lower())
            self.settings["banned_users"] = banned
            self.save_settings()
            
        d_user = user.lower()
        if d_user in self.timeouts:
            del self.timeouts[d_user]
            self.save_timeouts()
            if hasattr(self, 'timeouts_window') and self.timeouts_window is not None and self.timeouts_window.winfo_exists():
                self.refresh_timeouts_ui()
                
        self.after(0, self.append_system_log, f"\n[System] User '{user}' has been permanently banned.")

    def show_user_context_menu(self, event, user):
        from tkinter import Menu
        menu = Menu(self, tearoff=0)
        menu.add_command(label=f"Time out {user}", state="disabled")
        menu.add_separator()
        menu.add_command(label="5 minutes", command=lambda: self.apply_timeout(user, 5))
        menu.add_command(label="1 hour", command=lambda: self.apply_timeout(user, 60))
        menu.add_command(label="1 day", command=lambda: self.apply_timeout(user, 1440))
        menu.add_command(label="1 week", command=lambda: self.apply_timeout(user, 10080))
        menu.add_command(label="perma", command=lambda: self.apply_perma(user))
        
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            menu.grab_release()

    def open_timeouts_window(self):
        if hasattr(self, 'timeouts_window') and self.timeouts_window is not None and self.timeouts_window.winfo_exists():
            self.timeouts_window.focus()
            return
            
        self.timeouts_window = ctk.CTkToplevel(self)
        self.center_toplevel(self.timeouts_window, 400, 500)
        self.timeouts_window.title("Timed Out Users")
        self.apply_dark_title_bar(self.timeouts_window)
        self.timeouts_window.grab_set()
        
        main_frame = ctk.CTkFrame(self.timeouts_window, fg_color="#0A0A0E")
        main_frame.pack(fill="both", expand=True)
        
        ctk.CTkLabel(main_frame, text="Current Timeouts", font=("Arial", 16, "bold")).pack(pady=10)
        
        self.timeouts_list_frame = ctk.CTkScrollableFrame(main_frame, fg_color="#1e1e24")
        self.timeouts_list_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.refresh_timeouts_ui()
        
        entry_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        entry_frame.pack(fill="x", padx=10, pady=10)
        
        self.timeout_user_var = ctk.StringVar()
        ctk.CTkEntry(entry_frame, textvariable=self.timeout_user_var, placeholder_text="Username").pack(side="left", fill="x", expand=True, padx=(0, 5))
        
        self.timeout_duration_var = ctk.StringVar(value="5 minutes")
        ctk.CTkOptionMenu(entry_frame, variable=self.timeout_duration_var, values=["5 minutes", "1 hour", "1 day", "1 week"], width=100).pack(side="left", padx=(0, 5))
        
        ctk.CTkButton(entry_frame, text="Add", width=60, command=self.manual_add_timeout).pack(side="left")

    def manual_add_timeout(self):
        user = self.timeout_user_var.get().strip()
        if not user: return
        dur_str = self.timeout_duration_var.get()
        mins = 5
        if dur_str == "1 hour": mins = 60
        elif dur_str == "1 day": mins = 1440
        elif dur_str == "1 week": mins = 10080
        self.apply_timeout(user, mins)
        self.timeout_user_var.set("")

    def remove_timeout(self, user):
        d_user = user.lower()
        if d_user in self.timeouts:
            del self.timeouts[d_user]
            self.save_timeouts()
            self.refresh_timeouts_ui()

    def refresh_timeouts_ui(self):
        if not hasattr(self, 'timeouts_list_frame') or not self.timeouts_list_frame.winfo_exists():
            return
            
        for child in self.timeouts_list_frame.winfo_children():
            child.destroy()
            
        import datetime
        now = datetime.datetime.now()
        
        sorted_timeouts = sorted(self.timeouts.values(), key=lambda x: x.get("timestamp", ""), reverse=True)
        
        for t in sorted_timeouts:
            try:
                expires = datetime.datetime.fromisoformat(t["expires_at"])
                if now >= expires:
                    continue
                    
                delta = expires - now
                days = delta.days
                hours, remainder = divmod(delta.seconds, 3600)
                minutes, _ = divmod(remainder, 60)
                
                time_str = ""
                if days > 0: time_str += f"{days}d "
                if hours > 0: time_str += f"{hours}h "
                time_str += f"{minutes}m"
                if time_str == "0m" and delta.seconds > 0:
                    time_str = "<1m"
                elif time_str == "0m":
                    time_str = "Expired"
            except:
                time_str = "Unknown"
            
            row = ctk.CTkFrame(self.timeouts_list_frame, fg_color="transparent")
            row.pack(fill="x", pady=2)
            
            ctk.CTkLabel(row, text=t["username"], font=("Arial", 12, "bold")).pack(side="left", padx=5)
            ctk.CTkLabel(row, text=time_str, font=("Arial", 11), text_color="#8E9299").pack(side="left", padx=10)
            
            ctk.CTkButton(row, text="X", width=30, fg_color="#FF4444", hover_color="#cc0000",
                         command=lambda u=t['username']: self.remove_timeout(u)).pack(side="right", padx=5)

    def on_closing(self):
        try:
            self.unregister_hotkeys()
        except Exception:
            pass
        if hasattr(self, "kickbot_listener") and self.kickbot_listener:
            try:
                self.kickbot_listener.stop()
            except Exception:
                pass
        if hasattr(self, "powerchat_listener") and self.powerchat_listener:
            try:
                self.powerchat_listener.stop()
            except Exception:
                pass
        try:
            current_geom = self.geometry()
            parts = current_geom.split("+")
            size_part = parts[0].split("x")
            curr_h = size_part[1] if len(size_part) > 1 else "961"
            x_pos = parts[1] if len(parts) > 1 else "278"
            y_pos = parts[2] if len(parts) > 2 else "302"
            self.settings["geometry"] = f"{self.APP_WIDTH}x{curr_h}+{x_pos}+{y_pos}"
        except Exception:
            self.settings["geometry"] = f"{self.APP_WIDTH}x961+278+302"
        try:
            if hasattr(self, "vol_slider"):
                self.settings["app_volume"] = int(round(self.vol_slider.get()))
            self.settings["manual_tts_command"] = self.manual_cmd_var.get().strip()
            self.settings["manual_tts_message"] = self.manual_text_box.get("1.0", "end-1c").strip()
        except:
            pass
        self.save_settings(immediate=True)
        self.save_voices(immediate=True)
        try:
            self.record_stream_summary_activity()
        except Exception:
            pass
        self.destroy()


def disable_quickedit():
    import os
    if os.name == 'nt':
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            hStdIn = kernel32.GetStdHandle(-10)
            mode = ctypes.c_uint32()
            kernel32.GetConsoleMode(hStdIn, ctypes.byref(mode))
            mode.value &= ~0x0040
            mode.value |= 0x0080
            kernel32.SetConsoleMode(hStdIn, mode)
        except Exception:
            pass

disable_quickedit()

if __name__ == "__main__":

    app = App()
    app.mainloop()

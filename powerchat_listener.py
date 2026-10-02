import json
import time
import threading
import re
import uuid
from kick_websocket_client import PureWebSocket


def extract_powerchat_username(link_or_text: str) -> str:
    """
    Extracts the Powerchat username from a URL or raw string.
    Supports:
      - https://powerchat.live/niles/tts
      - http://powerchat.live/niles/tts/
      - powerchat.live/niles/tts
      - https://powerchat.live/niles
      - niles
    """
    if not link_or_text:
        return ""
    text = link_or_text.strip()

    # Search for powerchat.live/<user> pattern
    match = re.search(r'powerchat\.live/([^/?#\s]+)', text, re.IGNORECASE)
    if match:
        user = match.group(1).strip()
        if user.lower() == "tts":
            return ""
        return user

    # Raw username provided directly (without url schema or slashes)
    if not text.startswith("http://") and not text.startswith("https://") and "/" not in text:
        return text.strip().lstrip('@')

    return ""


class PowerchatListener:
    """
    Monitors powerchat.live WebSocket notifications for incoming donations.
    Extracts donator username, dollar amount, and message, and triggers callbacks.
    """
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

                # Handshake log required by powerchat server
                ws.send_text(f"Remote client log - {username} - Open TTS - {username}")

                # Start keepalive ping thread for this connection
                ping_thread = threading.Thread(
                    target=self._ping_worker,
                    args=(ws, username),
                    daemon=True
                )
                ping_thread.start()

                # Main message receive loop
                while self.is_running and not ws.closed:
                    opcode, payload = ws.recv_frame()
                    if opcode is None:
                        if ws.closed or not self.is_running:
                            break
                        continue

                    # Server Ping frame (0x09)
                    if opcode == 0x09:
                        ws.send_pong(payload)
                        continue

                    # Text frame (0x01)
                    if opcode == 0x01:
                        try:
                            text = payload.decode("utf-8", errors="ignore").strip()
                            if text and text != "pong":
                                self._handle_raw_message(text)
                        except Exception as e:
                            self.log(f"Error handling message payload: {e}")

                    # Close frame (0x08)
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
        """Sends Powerchat heartbeat every 25 seconds."""
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
            # Check all common field names used across Powerchat and alert APIs
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
        # If ID is missing or a common dummy value, generate a unique ID
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

        # Prune deduplication caches periodically
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

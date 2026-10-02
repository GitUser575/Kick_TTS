import json
import time
import socket
import ssl
import struct
import os
import base64
import threading
import re
import secrets


class PureWebSocket:
    """Minimal, self-contained WebSocket client built with standard library sockets & SSL."""
    def __init__(self, host, port=443, path="/", ssl_wrap=True):
        self.host = host
        self.port = port
        self.path = path
        self.ssl_wrap = ssl_wrap
        self.sock = None
        self.closed = False
        self._read_buffer = bytearray()

    def connect(self, timeout=12):
        self.closed = False
        self._read_buffer.clear()
        raw_sock = socket.create_connection((self.host, self.port), timeout=timeout)
        if self.ssl_wrap:
            context = ssl.create_default_context()
            self.sock = context.wrap_socket(raw_sock, server_hostname=self.host)
        else:
            self.sock = raw_sock

        key = base64.b64encode(os.urandom(16)).decode("utf-8")
        req = (
            f"GET {self.path} HTTP/1.1\r\n"
            f"Host: {self.host}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n"
            f"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n\r\n"
        )
        self.sock.sendall(req.encode("utf-8"))

        # Read HTTP upgrade response
        response = b""
        while b"\r\n\r\n" not in response:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("Handshake failed: connection closed prematurely")
            response += chunk

        headers_part, rest = response.split(b"\r\n\r\n", 1)
        if rest:
            self._read_buffer.extend(rest)

        status_line = headers_part.split(b"\r\n")[0]
        if b" 101 " not in status_line:
            raise ConnectionError(f"WebSocket handshake rejected: {status_line.decode('utf-8', errors='ignore')}")

        # Set a recurring socket timeout after handshake to allow non-blocking checks
        self.sock.settimeout(1.0)

    def _read_exact(self, n):
        while len(self._read_buffer) < n:
            if self.closed or not self.sock:
                return None
            try:
                chunk = self.sock.recv(max(4096, n - len(self._read_buffer)))
                if not chunk:
                    return None
                self._read_buffer.extend(chunk)
            except (socket.timeout, ssl.SSLError) as e:
                err_str = str(e).lower()
                if "timed out" in err_str or "time out" in err_str:
                    if self.closed or not self.sock:
                        return None
                    continue
                return None
            except Exception:
                return None
        res = bytes(self._read_buffer[:n])
        del self._read_buffer[:n]
        return res

    def send_text(self, text):
        if self.closed or not self.sock:
            return
        payload = text.encode("utf-8")
        length = len(payload)
        header = bytearray([0x81])  # FIN + Text

        mask_key = os.urandom(4)
        if length <= 125:
            header.append(0x80 | length)
        elif length <= 65535:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", length))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", length))

        header.extend(mask_key)
        masked = bytearray(payload)
        for i in range(len(masked)):
            masked[i] ^= mask_key[i % 4]

        try:
            self.sock.sendall(header + masked)
        except Exception:
            self.close()

    def send_pong(self, payload=b""):
        if self.closed or not self.sock:
            return
        header = bytearray([0x8A])  # FIN + Pong opcode
        mask_key = os.urandom(4)
        length = len(payload)
        if length <= 125:
            header.append(0x80 | length)
        elif length <= 65535:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", length))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", length))
        header.extend(mask_key)
        masked = bytearray(payload)
        for i in range(len(masked)):
            masked[i] ^= mask_key[i % 4]
        try:
            self.sock.sendall(header + masked)
        except Exception:
            self.close()

    def send_ping(self):
        if self.closed or not self.sock:
            return
        header = bytearray([0x89, 0x80])  # Ping opcode + mask bit
        mask_key = os.urandom(4)
        header.extend(mask_key)
        try:
            self.sock.sendall(header)
        except Exception:
            self.close()

    def recv_frame(self):
        if self.closed or not self.sock:
            return None, None

        b1_b2 = self._read_exact(2)
        if not b1_b2:
            return None, None

        b1, b2 = b1_b2[0], b1_b2[1]
        opcode = b1 & 0x0F
        has_mask = bool(b2 & 0x80)
        payload_len = b2 & 0x7F

        if payload_len == 126:
            ext = self._read_exact(2)
            if not ext: return None, None
            payload_len = struct.unpack("!H", ext)[0]
        elif payload_len == 127:
            ext = self._read_exact(8)
            if not ext: return None, None
            payload_len = struct.unpack("!Q", ext)[0]

        mask_key = None
        if has_mask:
            mask_key = self._read_exact(4)
            if not mask_key: return None, None

        payload = self._read_exact(payload_len) if payload_len > 0 else b""
        if payload is None:
            return None, None

        if has_mask and mask_key:
            unmasked = bytearray(payload)
            for i in range(len(unmasked)):
                unmasked[i] ^= mask_key[i % 4]
            payload = bytes(unmasked)

        return opcode, payload

    def close(self):
        self.closed = True
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None


_RECENT_COMMUNITY_GIFTS = {}


def _record_recent_community_gift(user):
    """Tracks a recent community gift to suppress subsequent recipient rows."""
    if not user:
        return
    _RECENT_COMMUNITY_GIFTS[str(user).strip().lower()] = time.time()
    # Clean up old keys
    if len(_RECENT_COMMUNITY_GIFTS) > 200:
        now = time.time()
        for k in list(_RECENT_COMMUNITY_GIFTS.keys())[:50]:
            if now - _RECENT_COMMUNITY_GIFTS[k] > 20:
                del _RECENT_COMMUNITY_GIFTS[k]


def _is_recent_community_gift(user):
    """Checks if the user completed a community gift within the last 4 seconds."""
    if not user:
        return False
    u = str(user).strip().lower()
    if u in _RECENT_COMMUNITY_GIFTS:
        if time.time() - _RECENT_COMMUNITY_GIFTS[u] < 4.0:
            return True
        else:
            del _RECENT_COMMUNITY_GIFTS[u]
    return False


def _extract_user_str(val):
    """Safely extracts a username string whether val is a string or a dict."""
    if not val:
        return ""
    if isinstance(val, str):
        return val.strip().lstrip('@')
    if isinstance(val, dict):
        return str(val.get("username") or val.get("slug") or val.get("name") or val.get("channel_slug") or "").strip().lstrip('@')
    return ""


def _get_any_user(data, *keys):
    """Safely searches data dictionary for the first non-empty username across candidate keys."""
    if not isinstance(data, dict):
        return ""
    for k in keys:
        if k in data and data[k]:
            u = _extract_user_str(data[k])
            if u:
                return u
    return ""


def _extract_gift_count(data, meta=None):
    """Safely extracts the count of gifted subscriptions from data or metadata."""
    meta = meta if isinstance(meta, dict) else {}
    data = data if isinstance(data, dict) else {}

    # Check list of recipient usernames
    for obj in (meta, data):
        for key in ("gifted_usernames", "recipients", "gifted_users", "users"):
            val = obj.get(key)
            if isinstance(val, list) and len(val) > 0:
                return len(val)
            elif isinstance(val, (int, float)) and val > 0:
                return int(val)

    # Check count / quantity / amount fields (do NOT include "months" as months represents subscription tenure)
    for obj in (meta, data):
        for key in ("count", "gift_count", "quantity", "amount", "total", "gift_quantity", "gifts"):
            val = obj.get(key)
            if val is not None:
                try:
                    c = int(val)
                    if c > 0:
                        return c
                except Exception:
                    pass
    return 1


def _extract_sub_celebration_months(data=None, meta=None, content="", raw_badges=None, sender=None):
    """
    Robustly extracts the subscription milestone / tenure months for a sub celebration or renewal.
    Prioritizes explicit celebration text across content/meta/data, milestone tenure fields
    (monthsSubscribed, months_subscribed, streak, total_months, etc.), and only falls back
    to subscriber badge counts, duration, or 1.
    """
    data = data if isinstance(data, dict) else {}
    meta = meta if isinstance(meta, dict) else {}
    sender = sender if isinstance(sender, dict) else {}
    raw_badges = raw_badges if isinstance(raw_badges, list) else []

    # Gather all strings across data, meta, sender, content to scan for milestone text
    strings_to_check = []
    if content:
        strings_to_check.append(str(content))

    def _collect_strings(obj, depth=0):
        if depth > 4:
            return
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, str) and len(v) > 2:
                    strings_to_check.append(v)
                elif isinstance(v, dict):
                    _collect_strings(v, depth + 1)
                elif isinstance(v, (list, tuple)):
                    for item in v:
                        if isinstance(item, str) and len(item) > 2:
                            strings_to_check.append(item)
                        elif isinstance(item, dict):
                            _collect_strings(item, depth + 1)

    _collect_strings(meta)
    _collect_strings(data)
    _collect_strings(sender)

    # 1. Search milestone text in all collected strings (highest fidelity)
    celebration_regexes = [
        re.compile(
            r'\b(?:celebrated|celebrates|celebrating|is\s+celebrating|resubscribed|resubbed|subbed|subscribed|renewed)\s+(?:their\s+|a\s+)?'
            r'(?:subscription\s+for\s+(\d+)\s+months?|(\d+)\s+months?\s+(?:subscription|sub|renewal)|(?:for\s+)?(\d+)\s+months?)\b',
            re.I
        ),
        re.compile(r'\b(?:subscription\s+for\s+(\d+)\s+months?|(\d+)\s+months?\s+(?:subscription|sub)|(\d+)\s+month\s+sub)\b', re.I),
        re.compile(r'\b(?:celebrated|celebrates|celebrating|is\s+celebrating)\s+(?:their\s+)?(\d+)\s+months?\b', re.I),
        re.compile(r'\b(?:subscribed|subbed|resubscribed|resubbed)\s+for\s+(\d+)\s+months?\b', re.I),
        re.compile(r'\b(?:has|have)\s+been\s+subscribed\s+for\s+(\d+)\s+months?\b', re.I),
        re.compile(r'\b(?:subscribed|subbed|resubscribed|resubbed)\s+(?:for\s+)?(\d+)\s+months?\b', re.I),
        re.compile(r'\bfor\s+(\d+)\s+months!?\b', re.I),
        re.compile(r'\b(\d+)\s+months?\b', re.I),
    ]

    for s in strings_to_check:
        for reg in celebration_regexes:
            m = reg.search(s)
            if m:
                for g in m.groups():
                    if g and g.isdigit() and int(g) > 0:
                        return int(g)

    # 2. Check tenure / streak / milestone fields across all nested dicts
    tenure_keys = (
        "monthsSubscribed", "months_subscribed",
        "subscribedMonths", "subscribed_months",
        "totalMonths", "total_months",
        "cumulativeMonths", "cumulative_months",
        "streakMonths", "streak_months",
        "subMonths", "sub_months",
        "celebrationMonths", "celebration_months",
        "milestoneMonths", "milestone_months",
        "subscriptionMonths", "subscription_months",
        "durationMonths", "duration_months",
        "monthsCount", "months_count",
        "numMonths", "num_months",
        "streak",
        "tenure", "sub_tenure", "subTenure",
        "milestone", "anniversary",
        "months", "month",
        "duration", "total_duration", "sub_duration", "months_total", "total_sub_months"
    )

    candidates = []

    def _collect_numeric_tenure(obj, depth=0):
        if depth > 4:
            return
        if not isinstance(obj, dict):
            return
        for k, v in obj.items():
            if k in tenure_keys and v is not None:
                try:
                    iv = int(v)
                    if iv > 0:
                        candidates.append(iv)
                except Exception:
                    pass
            if k in ("celebration", "sub_celebration", "stream_celebration") and isinstance(v, (int, str)):
                try:
                    iv = int(v)
                    if iv > 0:
                        candidates.append(iv)
                except Exception:
                    pass
            if isinstance(v, dict):
                _collect_numeric_tenure(v, depth + 1)
            elif isinstance(v, (list, tuple)):
                for item in v:
                    if isinstance(item, dict):
                        _collect_numeric_tenure(item, depth + 1)

    _collect_numeric_tenure(meta)
    _collect_numeric_tenure(data)
    _collect_numeric_tenure(sender)

    # If any tenure milestone candidates were found, pick the largest (e.g. 19 months over 1 month renewal package)
    if candidates:
        return max(candidates)

    # 3. Check subscriber badge count/text (only if no explicit milestone field was found)
    badge_candidates = []
    for b in raw_badges:
        if isinstance(b, dict):
            b_type = str(b.get("type") or b.get("name") or "").lower()
            if ("sub" in b_type or "subscriber" in b_type) and "founder" not in b_type:
                for k in ("count", "months", "duration"):
                    val = b.get(k)
                    if val is not None:
                        try:
                            iv = int(val)
                            if iv > 0:
                                badge_candidates.append(iv)
                        except Exception:
                            pass
                b_text = str(b.get("text") or "")
                m_b = re.search(r'(\d+)', b_text)
                if m_b:
                    try:
                        iv = int(m_b.group(1))
                        if iv > 0:
                            badge_candidates.append(iv)
                    except Exception:
                        pass
    if badge_candidates:
        return max(badge_candidates)

    # 4. Fallback: check duration / quantity
    for obj in (meta, data, sender):
        if not isinstance(obj, dict):
            continue
        for k in ("duration", "quantity", "count"):
            val = obj.get(k)
            if val is not None:
                try:
                    iv = int(val)
                    if iv > 0:
                        return iv
                except Exception:
                    pass

    return 1


def parse_pusher_payload(raw_json, callback, target_username=""):
    """
    Parses a raw Pusher JSON frame string and dispatches appropriate system callbacks.
    Shared by both KickWebSocketClient and KickScraper (via browser WebSocket interception).
    """
    try:
        msg = json.loads(raw_json)
    except Exception:
        return

    event_name = msg.get("event")
    if not event_name:
        return

    # Handle or ignore all internal Pusher control events
    if event_name.startswith("pusher:") or event_name.startswith("pusher_internal:"):
        return

    data_str = msg.get("data")
    if not data_str:
        return

    try:
        data = json.loads(data_str) if isinstance(data_str, str) else data_str
    except Exception:
        return

    if not isinstance(data, dict):
        return

    # Ensure metadata is parsed if it's a JSON string
    if isinstance(data.get("metadata"), str):
        try:
            data["metadata"] = json.loads(data["metadata"])
        except Exception:
            pass

    meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    meta_dict = meta

    # Unpack nested data from activity feed or generic event envelopes if present
    if isinstance(data.get("data"), dict):
        nested = data["data"]
        for k, v in nested.items():
            if k not in data or not data[k]:
                data[k] = v

    ev_lower = str(event_name).lower()
    activity_type = str(data.get("type") or data.get("event") or data.get("action") or "").lower()
    is_activity_event = any(k in ev_lower for k in ("activity", "feed", "channel_event", "channelevent"))

    is_raid_event = (
        any(k in ev_lower for k in ("streamhost", "hostevent", "raidevent"))
        or any(k in activity_type for k in ("raid", "host", "stream_host"))
        or (("host" in ev_lower or "raid" in ev_lower) and not any(k in ev_lower for k in ("chatmessage", "chat.message", "gift", "sub", "tip", "dono", "token", "kicks")))
    )

    # 1. Native Gifted Subscriptions Events (e.g. App\Events\GiftedSubscriptionsEvent, channel.subscription.gifts, SubscriptionGiftEvent)
    is_gifted_sub_event = (
        any(k in ev_lower for k in (
            "giftedsubscription", "gifted_subscription", "giftedsubscriptions", "giftsub", "gift_sub",
            "subscriptiongift", "subscription_gift", "subscriptiongifts", "sub_gift", "subgift",
            "giftedsub", "giftedsubs", "community_gift", "mass_gift", "communitygift",
            "communitysubgift", "community_sub_gift"
        ))
        or ("subscription" in ev_lower and "gift" in ev_lower)
        or ("sub" in ev_lower and "gift" in ev_lower)
        or any(k in activity_type for k in (
            "gifted_subscriptions", "subscription_gift", "sub_gift", "gifted_subs", "gift_sub",
            "community_gift", "mass_gift", "gifted_subscription", "subgift", "community_sub_gift"
        ))
        or str(data.get("type", "")).lower() in (
            "gifted_subscriptions", "giftsub", "gift_sub", "subscription_gift", "sub_gift",
            "community_gift", "mass_gift", "gifted_sub", "community_sub_gift"
        )
        or str(meta.get("type", "")).lower() in (
            "gifted_subscriptions", "giftsub", "gift_sub", "subscription_gift", "sub_gift",
            "community_gift", "mass_gift", "gifted_sub", "community_sub_gift"
        )
        or bool(data.get("is_gift") or meta.get("is_gift") or data.get("gifted_usernames") or meta.get("gifted_usernames") or data.get("recipients") or meta.get("recipients") or data.get("gifter_username") or meta.get("gifter_username"))
    )

    # 2. Native Sub Celebration Events (MUST be an explicit celebration event, not a regular subscription)
    is_sub_celebration_event = (
        not is_gifted_sub_event
        and (
            any(k in ev_lower for k in ("subscriptioncelebration", "subcelebration", "subscription_celebration", "streamcelebration"))
            or any(k in activity_type for k in ("sub_celebration", "subscription_celebration", "stream_celebration", "celebration"))
            or str(data.get("type", "")).lower() in ("sub_celebration", "subscription_celebration")
        )
    )

    # 2b. Native Direct Subscription / Resubscription Events
    is_direct_sub_event = (
        not is_gifted_sub_event
        and not is_sub_celebration_event
        and (
            any(k in ev_lower for k in ("subscriptionevent", "subscription_event", "newsubscriber", "new_subscription", "resub", "subscription_renewed", "resubscription"))
            or ("subscription" in ev_lower and "event" in ev_lower)
            or (ev_lower in ("channel.subscription", "chatrooms.subscription", "chatroom.subscription"))
            or str(data.get("type", "")).lower() in ("subscription", "new_subscription", "sub", "resub", "resubscription", "subscription_renewed")
            or str(meta.get("type", "")).lower() in ("subscription", "new_subscription", "sub", "resub", "resubscription", "subscription_renewed")
        )
    )

    # Sub-related events (Gifted subs & Sub celebrations are alerted; regular subscriptions/resubscriptions are ignored)
    is_subscription_related = (
        is_gifted_sub_event
        or is_sub_celebration_event
        or is_direct_sub_event
        or any(k in ev_lower for k in ("sub", "subscription", "resub", "subrenew"))
        or any(k in activity_type for k in ("sub", "subscription", "resub", "subrenew"))
        or str(data.get("type", "")).lower() in ("sub", "subscription", "resub", "subscription_renewed", "sub_celebration")
    )

    # 3. Native Channel Points Redemption Events
    is_channel_points_event = (
        any(k in ev_lower for k in (
            "reward_redeemed", "rewardredeemed", "channel_points", "channelpoints",
            "point_redemption", "pointredemption", "reward", "bubbles", "bubble",
            "community_point", "point_redeem", "custom_reward"
        ))
        or str(data.get("type", "")).lower() in (
            "reward_redeemed", "channel_points", "point_redemption", "channel_points_redeemed",
            "reward", "bubbles", "bubble", "community_points", "custom_reward", "point_redemption"
        )
        or str(meta.get("type", "")).lower() in (
            "reward_redeemed", "channel_points", "point_redemption", "channel_points_redeemed",
            "reward", "bubbles", "bubble", "community_points", "custom_reward"
        )
        or bool(data.get("reward") or meta.get("reward") or data.get("reward_title") or meta.get("reward_title"))
        or str(data.get("icon", "")).lower() in ("bubbles", "bubble")
        or str(meta.get("icon", "")).lower() in ("bubbles", "bubble")
        or "data-ds-icon=\"bubbles\"" in str(data).lower()
        or "data-ds-icon='bubbles'" in str(data).lower()
    )

    is_dono_event = (
        not is_subscription_related
        and not is_channel_points_event
        and (
            any(k in ev_lower for k in ("kick", "dono", "tip", "token", "bits"))
            or ("gift" in ev_lower and not is_subscription_related)
            or any(k in activity_type for k in ("kick", "gift", "dono", "tip", "token", "bits"))
        )
        and (
            not ("chatmessage" in ev_lower or "chat.message" in ev_lower)
            or any(k in ev_lower for k in ("kick", "dono", "tip", "tokens"))
        )
    )

    # 0. Stream Host / Raid Events
    if is_raid_event:
        raider = _get_any_user(data, "host_username", "hoster_username", "hoster", "raider_username", "raider", "user", "sender", "username")
        raider = str(raider).strip().lstrip('@')
        viewers = data.get("number_viewers") or data.get("viewers_count") or data.get("viewers") or data.get("count") or data.get("amount") or 0
        try:
            viewers = int(viewers)
        except Exception:
            viewers = 0

        # Ignore uninitialized / preparation / 0-viewer / unhost events, invalid raiders, or channel's own name
        target_norm = (target_username or "").strip().lower()
        if (
            viewers <= 0
            or not raider
            or raider.lower() in ("someone", "unknown", "none", "null")
            or (target_norm and raider.lower() == target_norm)
        ):
            return

        raid_id = f"raid::{raider.lower()}"
        callback("SystemNativeRaid", json.dumps({"raider": str(raider), "viewers": viewers, "id": raid_id}), None)
        return

    # 1. Native Gifted Subscriptions Events (e.g. App\Events\GiftedSubscriptionsEvent, channel.subscription.gifts)
    if is_gifted_sub_event:
        meta_dict = meta if isinstance(meta, dict) else {}
        gifter = (
            _get_any_user(meta_dict, "gifter_username", "gifter", "original_sender", "sender", "user", "username")
            or _get_any_user(data, "gifter_username", "gifter", "original_sender", "sender", "username", "user", "subscriber", "name")
        )
        gifter = str(gifter).strip().lstrip('@')
        count = _extract_gift_count(data, meta_dict)
        is_comm = (count > 1) or bool(data.get("gifted_usernames") or meta_dict.get("gifted_usernames"))
        recipient = _extract_user_str(data.get("recipient") or meta_dict.get("recipient") or data.get("subscriber") or "")

        if is_comm and gifter:
            _record_recent_community_gift(gifter)

        if gifter and count > 0:
            g_id = f"giftsub::{gifter.lower()}::{count}::{data.get('id', '')}"
            callback("SystemNativeGiftedSubs", json.dumps({"user": str(gifter), "count": count, "id": g_id, "is_community": is_comm, "recipient": recipient}), None)
            return

    # 2. Native Sub Celebration Events
    if is_sub_celebration_event:
        sub_user = _get_any_user(data, "username", "user", "sender", "subscriber", "name")
        sub_user = str(sub_user).strip().lstrip('@')
        raw_msg = data.get("message")
        if raw_msg is None:
            raw_msg = data.get("content")
        if isinstance(raw_msg, dict):
            msg = str(raw_msg.get("text") or raw_msg.get("message") or raw_msg.get("content") or raw_msg.get("body") or "")
        elif isinstance(raw_msg, (list, tuple)):
            msg = " ".join(str(x) for x in raw_msg if x is not None)
        elif raw_msg is None:
            msg = ""
        else:
            msg = str(raw_msg)
        meta_dict = meta if isinstance(meta, dict) else {}
        raw_badges = data.get("badges") or meta_dict.get("badges") or []
        sender_obj = data.get("sender") or meta_dict.get("sender") or {}
        months = _extract_sub_celebration_months(data=data, meta=meta_dict, content=msg, raw_badges=raw_badges, sender=sender_obj)
        if sub_user:
            c_id = f"subcel::{sub_user.lower()}::{months}::{data.get('id', '')}"
            callback("SystemNativeSubCelebration", json.dumps({"user": str(sub_user), "months": months, "message": str(msg), "id": c_id}), None)
            return

    # 2b. Native Direct Subscription / Resubscription Events
    if is_direct_sub_event:
        meta_dict = meta if isinstance(meta, dict) else {}
        sub_user = (
            _get_any_user(data, "username", "user", "sender", "subscriber", "name")
            or _get_any_user(meta_dict, "username", "user", "sender", "subscriber")
        )
        sub_user = str(sub_user).strip().lstrip('@') if sub_user else ""
        if sub_user:
            raw_msg = data.get("message") or data.get("content") or data.get("body") or ""
            if isinstance(raw_msg, dict):
                raw_msg = raw_msg.get("text") or raw_msg.get("message") or ""
            sender_obj = data.get("sender") or meta_dict.get("sender") or {}
            raw_badges = data.get("badges") or meta_dict.get("badges") or []
            months = _extract_sub_celebration_months(data=data, meta=meta_dict, content=str(raw_msg), raw_badges=raw_badges, sender=sender_obj)
            is_resub = (
                months > 1
                or any(k in ev_lower for k in ("resub", "renew"))
                or any(k in activity_type for k in ("resub", "renew"))
                or any(k in str(data.get("type", "")).lower() for k in ("resub", "renew"))
            )
            if is_resub and months > 1:
                c_id = f"subcel::{sub_user.lower()}::{months}::{data.get('id', '')}"
                callback("SystemNativeSubCelebration", json.dumps({"user": str(sub_user), "months": months, "message": str(raw_msg), "id": c_id}), None)
                return
            else:
                s_id = f"sub::{sub_user.lower()}::{data.get('id', '')}"
                callback("SystemNativeSub", json.dumps({"user": str(sub_user), "months": months, "id": s_id}), None)
                return

    # 3. Native Channel Points Redemption Events
    if is_channel_points_event:
        cp_user = (
            _get_any_user(data, "username", "user", "sender", "subscriber", "name", "chatter")
            or _get_any_user(meta_dict, "username", "user", "sender")
        )
        if not cp_user and isinstance(data.get("sender"), dict):
            cp_user = data["sender"].get("username") or data["sender"].get("slug")
        if not cp_user and isinstance(data.get("user"), dict):
            cp_user = data["user"].get("username") or data["user"].get("slug")
        if not cp_user and isinstance(meta_dict.get("sender"), dict):
            cp_user = meta_dict["sender"].get("username") or meta_dict["sender"].get("slug")
        if not cp_user and isinstance(meta_dict.get("user"), dict):
            cp_user = meta_dict["user"].get("username") or meta_dict["user"].get("slug")
        cp_user = str(cp_user).strip().lstrip('@') if cp_user else ""

        reward_obj = data.get("reward") if isinstance(data.get("reward"), dict) else (meta_dict.get("reward") if isinstance(meta_dict.get("reward"), dict) else {})
        cp_reward = (
            data.get("reward_title")
            or data.get("reward_name")
            or data.get("title")
            or reward_obj.get("title")
            or reward_obj.get("name")
            or meta_dict.get("reward_title")
            or meta_dict.get("reward_name")
            or meta_dict.get("title")
            or data.get("content")
            or data.get("message")
            or data.get("prompt")
            or data.get("user_input")
            or meta_dict.get("content")
            or meta_dict.get("message")
            or ""
        )
        if cp_user:
            ev_id = (
                data.get("redemption_id")
                or data.get("claim_id")
                or data.get("uuid")
                or data.get("id")
                or meta_dict.get("id")
                or ""
            )
            cp_id = f"cp::{str(cp_user).lower()}::{str(cp_reward).lower()}::{ev_id}"
            callback("SystemNativeChannelPoints", json.dumps({"user": str(cp_user), "reward": str(cp_reward), "id": cp_id, "data": data}), None)
            return

    return data, event_name, ev_lower, is_dono_event


class KickWebSocketClient:
    """
    Pure WebSocket Client for Kick.com.
    """
    def __init__(self, callback, chatroom_id=None, channel_id=None, target_username=""):
        self.callback = callback
        self.chatroom_id = chatroom_id
        self.channel_id = channel_id
        self.target_username = target_username
        self.is_running = False
        self.ws = None
        self.thread = None
        self.pusher_app_key = "32cbd69e4b950bf97679"
        self.pusher_cluster = "us2"
        self.seen_event_hashes = {}
        self.processed_dono_cids = {}
        self.processed_chat_ids = {}
        self.seen_message_ids = {}
        self.recent_texts = {}

    def _handle_pusher_message(self, raw_json):
        try:
            msg = json.loads(raw_json)
        except Exception:
            return

        event_name = msg.get("event")
        if not event_name:
            return

        # Handle Pusher ping/pong
        if event_name == "pusher:ping":
            if self.ws and not self.ws.closed:
                try:
                    self.ws.send_text(json.dumps({"event": "pusher:pong", "data": {}}))
                except Exception:
                    pass
            return
        if event_name.startswith("pusher:") or event_name.startswith("pusher_internal:"):
            return

        res = parse_pusher_payload(raw_json, self.callback, getattr(self, "target_username", ""))
        if not res:
            return
        data, event_name, ev_lower, is_dono_event = res

        # 3. Kicks / Donations / Gifts (Excludes subscriptions & gifted subs)
        if is_dono_event:
            self._process_dono_event(event_name, data)

        # 4. Standard Chat Messages (which may also contain KICKs, gifts, or channel points)
        elif "chatmessage" in ev_lower or "chat.message" in ev_lower or event_name in ("App\\Events\\ChatMessageEvent", "ChatMessageEvent", "chat.message.sent", "App\\Events\\ChatMessageSentEvent") or any(k in ev_lower for k in ("reward", "channel_point", "point", "bubble", "redeem")):
            self._process_chat_event(data)

        # 5. Fallback: check if unknown event data contains direct dono / gift payload
        elif isinstance(data, dict) and (data.get("kicks") or data.get("tokens") or (data.get("gift") and not isinstance(data.get("gift"), str))):
            self._process_dono_event(event_name, data)

    def _process_chat_event(self, data):
        sender = data.get("sender", {}) if isinstance(data.get("sender"), dict) else {}
        username = sender.get("username") or sender.get("slug") or data.get("username") or "Anonymous"
        raw_content = data.get("content")
        if raw_content is None:
            raw_content = data.get("message")
        if isinstance(raw_content, dict):
            content = str(raw_content.get("text") or raw_content.get("message") or raw_content.get("content") or raw_content.get("body") or "")
        elif isinstance(raw_content, (list, tuple)):
            content = " ".join(str(x) for x in raw_content if x is not None)
        elif raw_content is None:
            content = ""
        else:
            content = str(raw_content)

        msg_id = str(data.get("id") or f"{username}::{content}")
        msg_type = str(data.get("type", "")).lower()

        if isinstance(data.get("metadata"), str):
            try:
                data["metadata"] = json.loads(data["metadata"])
            except Exception:
                pass
        meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}

        # Extract Badges & Roles (OG, VIP, Subscriber, Moderator, Broadcaster/Streamer, Founder)
        raw_identity = sender.get("identity", {}) if isinstance(sender.get("identity"), dict) else {}
        raw_badges = raw_identity.get("badges", []) or sender.get("badges", []) or data.get("badges", [])
        badge_types = []
        for b in raw_badges:
            if isinstance(b, dict):
                for k in ("type", "text", "name", "badge", "id", "icon", "tag"):
                    val = b.get(k)
                    if val is not None:
                        badge_types.append(str(val).lower())
            elif isinstance(b, str):
                badge_types.append(b.lower())

        raw_roles = sender.get("roles", []) or raw_identity.get("roles", []) or data.get("roles", [])
        if isinstance(raw_roles, list):
            for r in raw_roles:
                if isinstance(r, dict):
                    for k in ("name", "type", "slug"):
                        val = r.get(k)
                        if val is not None:
                            badge_types.append(str(val).lower())
                elif isinstance(r, str):
                    badge_types.append(r.lower())

        # Check Founder role (strictly separate role, NOT subscriber, NOT VIP, NOT OG)
        is_founder = (
            any("founder" in b for b in badge_types)
            or bool(sender.get("is_founder") or raw_identity.get("is_founder"))
        )

        # Check if user has subscriber role/badge (Founder badge is NOT treated as subscriber)
        is_sub = (
            any((b == "sub" or "subscriber" in b or "channel_subscriber" in b) and "founder" not in b for b in badge_types)
            or bool((sender.get("is_subscriber") or sender.get("is_sub") or data.get("is_subscriber") or data.get("is_sub")) and not is_founder)
            or bool((raw_identity.get("is_subscriber") or raw_identity.get("is_sub")) and not is_founder)
        )
        
        # Check if user is the streamer / broadcaster (microphone icon / channel owner / host / creator)
        clean_user_norm = username.strip().lstrip('@').lower()
        target_norm = (self.target_username or "").strip().lstrip('@').lower()
        is_broadcaster = (
            any(k in b for b in badge_types for k in ("broadcaster", "streamer", "host", "creator", "owner", "channel_owner", "mic", "microphone"))
            or (bool(target_norm) and clean_user_norm == target_norm)
            or (bool(target_norm) and clean_user_norm.replace('_', '-') == target_norm.replace('_', '-'))
            or bool(sender.get("is_broadcaster") or sender.get("is_streamer") or sender.get("is_owner"))
            or bool(raw_identity.get("is_broadcaster") or raw_identity.get("is_streamer") or raw_identity.get("is_owner"))
        )
        
        # Check moderator role
        is_mod = (
            any(b == "mod" or "moderator" in b or "channel_moderator" in b or re.search(r'\bmod\b', b) for b in badge_types)
            or bool(sender.get("is_moderator") or sender.get("is_mod") or data.get("is_moderator") or data.get("is_mod"))
            or bool(raw_identity.get("is_moderator") or raw_identity.get("is_mod"))
        )
        
        # Check VIP role (strictly VIP, excluding founder)
        is_vip = (
            any((b == "vip" or "vip_badge" in b or "channel_vip" in b or re.search(r'\bvip\b', b)) and "sub" not in b and "founder" not in b for b in badge_types)
            or bool(sender.get("is_vip") or data.get("is_vip"))
            or bool(raw_identity.get("is_vip"))
        )
        
        # Check OG role (strictly OG, excluding founder / early subscriber)
        is_og = (
            any((b == "og" or "og_badge" in b or "channel_og" in b or re.search(r'\bog\b', b)) and "founder" not in b and "dialog" not in b and "program" not in b for b in badge_types)
            or bool(sender.get("is_og") or data.get("is_og"))
            or bool(raw_identity.get("is_og"))
        )

        # Check Verified role (the green checkmark badge provided by Kick to verified accounts/streamers)
        is_verified = (
            any(b == "verified" or "verified_badge" in b or "channel_verified" in b or re.search(r'\bverified\b', b) for b in badge_types)
            or bool(sender.get("is_verified") or raw_identity.get("is_verified") or data.get("is_verified"))
        )

        # Pre-extract user command from message content upfront so user commands (e.g. !destiny)
        # are NEVER falsely intercepted as gifted subs, sub celebrations, or donos
        clean_raw = re.sub(r'[\u200b\u200c\u200d\uFEFF]', '', str(content or "")).strip()
        command = None
        words = clean_raw.split()
        if words:
            first_word = words[0]
            if first_word.startswith("!") and not first_word.startswith("!!") and not all(c == '!' for c in first_word):
                command = first_word.lower()
        is_user_cmd = bool(command or clean_raw.startswith("!") or clean_raw.startswith("-v ") or clean_raw == "-v")

        # Check Bubbles icon / Channel Points / Reward indicators
        is_bubbles_icon = (
            any("bubble" in b for b in badge_types)
            or str(data.get("icon", "")).lower() in ("bubbles", "bubble")
            or str(meta.get("icon", "")).lower() in ("bubbles", "bubble")
            or "data-ds-icon=\"bubbles\"" in str(data).lower()
            or "data-ds-icon='bubbles'" in str(data).lower()
            or "bubbles" in str(raw_identity).lower()
            or "bubbles" in str(data.get("badges", "")).lower()
        )
        is_chat_channel_points = not is_user_cmd and (
            is_bubbles_icon
            or msg_type in (
                "reward", "reward_redeemed", "channel_points", "channel_point",
                "point_redemption", "bubbles", "bubble", "custom_reward", "point_redeem"
            )
            or str(meta.get("type", "")).lower() in (
                "reward", "reward_redeemed", "channel_points", "channel_point",
                "point_redemption", "bubbles", "bubble", "custom_reward", "point_redeem"
            )
            or bool(data.get("reward") or meta.get("reward") or data.get("reward_title") or meta.get("reward_title"))
            or bool(re.search(r'^\s*(?:\[.*?\]\s*)*(?:@?[\w\-]+\s+)?(?:has\s+redeemed|redeemed)\s+(?:a\s+|the\s+)?[:\s]*(.+)$', str(content), re.I))
        )

        badges = {
            "sub": is_sub,
            "subscriber": is_sub,
            "broadcaster": is_broadcaster,
            "streamer": is_broadcaster,
            "mod": is_mod,
            "moderator": is_mod,
            "vip": is_vip,
            "og": is_og,
            "founder": is_founder,
            "verified": is_verified,
            "bubbles": is_bubbles_icon,
            "is_channel_points": is_chat_channel_points
        }

        # Check if this chat message is a Channel Point Redemption
        if is_chat_channel_points:
            reward_obj = data.get("reward") if isinstance(data.get("reward"), dict) else (meta.get("reward") if isinstance(meta.get("reward"), dict) else {})
            cp_reward_from_data = (
                data.get("reward_title")
                or data.get("reward_name")
                or data.get("title")
                or reward_obj.get("title")
                or reward_obj.get("name")
                or meta.get("reward_title")
                or meta.get("title")
                or ""
            )
            if not cp_reward_from_data:
                m_cp = re.search(r'^\s*(?:\[.*?\]\s*)*(?:@?[\w\-]+\s+)?(?:has\s+redeemed|redeemed)\s+(?:a\s+|the\s+)?[:\s]*(.+)$', str(content), re.I)
                if m_cp:
                    cp_reward_from_data = m_cp.group(1).strip()
                else:
                    cp_reward_from_data = str(content).strip()

            cp_id = f"cp_chat::{username.lower()}::{str(cp_reward_from_data).lower()}::{msg_id}"
            self.callback("SystemNativeChannelPoints", json.dumps({"user": username, "reward": cp_reward_from_data, "id": cp_id, "content": content}), None)
            return

        # Check if this chat message is a Host / Raid event
        if not is_user_cmd and (msg_type in ("host", "raid", "streamhost") or re.search(r'^([a-zA-Z0-9_\-]+)\s+(?:is\s+hosting\s+with|raided\s+with|hosted\s+with)\s+(\d+)\s+viewers', str(content), re.I)):
            raider = username
            m = re.search(r'^([a-zA-Z0-9_\-]+)\s+(?:is\s+hosting\s+with|raided\s+with|hosted\s+with)\s+(\d+)\s+viewers', str(content), re.I)
            if m:
                raider = m.group(1).strip()
                v_count = int(m.group(2))
            else:
                m_v = re.search(r'(\d+)\s+viewers', str(content), re.I)
                v_count = int(m_v.group(1)) if m_v else 0
            self.callback("SystemNativeRaid", json.dumps({"raider": str(raider), "viewers": v_count, "id": str(msg_id)}), None)
            return

        # Check if message is a third-party notification formatted as "@User just gifted/sent..."
        is_bot_notification = not is_user_cmd and bool(
            re.search(r'^@[\w\-]+\s+(?:just\s+)?(?:gifted|sent|tipped|donated)\s+\d+\s*kicks?', str(content), re.I)
            or re.search(r'^@[\w\-]+\s+(?:just\s+)?gifted\s+\d+\s+subs?\b', str(content), re.I)
            or re.search(r'^@[\w\-]+\s+(?:just\s+)?gifted\s+a\s+(?:sub|subscription)\b', str(content), re.I)
        )

        # Check if message is a gifted subscription or sub celebration (THESE ARE NOT KICKS DONOS)
        is_sub_message = (
            msg_type in (
                "gifted_subscriptions", "gift_sub", "giftsub",
                "subscription_gift", "sub_gift", "gifted_sub", "gifted_subs", "community_gift", "mass_gift",
                "channel_subscription_gift", "channel_subscription_gifts", "sub_celebration", "subscription_celebration",
                "stream_celebration", "celebration", "resub"
            )
            or (not is_user_cmd and not ("?" in str(content)) and (
                bool(re.search(r'^\s*(?:\[.*?\]\s*)*(?:@?[\w\-]+\s+)?(?:just\s+)?gifted\s+(?:\d+|a|1)\s+(?:subscriptions?|subs?)\b', str(content), re.I))
                or bool(re.search(r'^\s*(?:\[.*?\]\s*)*(?:@?[\w\-]+\s+)?(?:celebrated|celebrates|is\s+celebrating|resubscribed|resubbed|subbed)\b', str(content), re.I))
            ))
        )

        # Exclude bot notifications (KickBot posts should only be used for the KickBot sub alert switch)
        is_from_kickbot = (
            str(username or "").strip().lstrip('@').lower() == "kickbot"
            or str(_get_any_user(sender, "username", "slug") or "").strip().lstrip('@').lower() == "kickbot"
            or str(_get_any_user(meta, "username", "user") or "").strip().lstrip('@').lower() == "kickbot"
        )

        # 1. Gifted Subscriptions Detection in Chat Message (ONLY from real Kick events, NOT KickBot)
        # Clean away the secondary lifetime stats line so it does not corrupt the gift count
        clean_content = re.sub(r"(?:they've|they\s+have)\s+gifted\s+\d+\s+subscriptions?\s+in\s+the\s+channel\.?", "", str(content), flags=re.I).strip()
        has_question = "?" in clean_content

        m_chat_comm_gift = re.search(r'^\s*(?:\[.*?\]\s*)*(?:@?[\w\-]+\s+)?(?:just\s+)?gifted\s+(?:\d+|a|1)\s+(?:subscriptions?|subs?)\s+to\s+the\s+community\s*!?$', clean_content, re.I)
        m_chat_indiv_gift = re.search(r'^\s*(?:\[.*?\]\s*)*(?:@?[\w\-]+\s+)?(?:just\s+)?gifted\s+(?:a|1|\d+)?\s*(?:subs?|subscriptions?)\s+to\s+@?([a-zA-Z0-9_\-]+)\s*!?$', clean_content, re.I)
        m_chat_mass_gift = re.search(r'^\s*(?:\[.*?\]\s*)*(?:@?[\w\-]+\s+)?(?:just\s+)?gifted\s+(\d+)\s+(?:subs?|subscriptions?)\s*!?$', clean_content, re.I)

        is_chat_giftsub = not is_from_kickbot and not is_user_cmd and not has_question and (
            msg_type in (
                "gifted_subscriptions", "giftsub", "gift_sub", "subscription_gift", "sub_gift",
                "gifted_sub", "gifted_subs", "community_gift", "mass_gift",
                "channel_subscription_gift", "channel_subscription_gifts",
                "community_sub_gift", "community_sub_gifts"
            )
            or str(meta.get("type", "")).lower() in (
                "gifted_subscriptions", "giftsub", "gift_sub", "subscription_gift", "sub_gift",
                "community_gift", "mass_gift", "gifted_sub", "community_sub_gift"
            )
            or bool(meta and (meta.get("gifted_usernames") or meta.get("recipients") or (meta.get("recipient") and meta.get("is_gift")) or meta.get("gift_count") or meta.get("quantity") or meta.get("is_gift") or meta.get("gift") or meta.get("gifter") or meta.get("gifter_username")))
            or bool(data.get("gifted_usernames") or data.get("recipients") or (data.get("recipient") and data.get("is_gift")) or data.get("gifter_username") or data.get("is_gift"))
            or bool(m_chat_comm_gift or m_chat_indiv_gift or m_chat_mass_gift)
        )

        if is_chat_giftsub:
            # Determine gifter username
            gifter = (
                _get_any_user(meta, "gifter", "gifter_username", "original_sender")
                or _get_any_user(data, "gifter_username", "gifter", "original_sender", "sender", "username", "user")
                or username
            )
            # If notification text says "@User just gifted...", extract the tagged user as the real gifter
            m_bot_gifter = re.search(r'^\s*@?([a-zA-Z0-9_\-]+)\s+(?:just\s+)?gifted\s+(\d+)?', clean_content, re.I)
            if m_bot_gifter and m_bot_gifter.group(1):
                cand = m_bot_gifter.group(1).strip().lstrip('@')
                if cand:
                    gifter = cand

            # Determine gift count
            g_count = _extract_gift_count(data, meta)
            m_cnt = re.search(r'(?:just\s+)?gifted\s+(\d+)\s+(?:subs?|subscriptions?)', clean_content, re.I)
            if m_cnt and m_cnt.group(1):
                try:
                    c_val = int(m_cnt.group(1))
                    if c_val > 0:
                        g_count = c_val
                except Exception:
                    pass
            elif re.search(r'(?:just\s+)?gifted\s+(?:a|1)\s+(?:sub|subscription)', clean_content, re.I):
                g_count = 1

            is_comm = bool(
                re.search(r'gifted\s+(?:\d+|a|1)\s+subscriptions?\s+to\s+the\s+community', clean_content, re.I)
                or msg_type in ("community_gift", "mass_gift", "community_sub_gift", "community_sub_gifts")
                or str(meta.get("type", "")).lower() in ("community_gift", "mass_gift", "community_sub_gift")
                or (g_count > 1)
                or bool(data.get("gifted_usernames") or meta.get("gifted_usernames"))
            )
            m_recip = re.search(r'(?:just\s+)?gifted\s+(?:a|1|\d+)?\s*(?:subs?|subscriptions?)\s+to\s+@?([a-zA-Z0-9_\-]+)', clean_content, re.I)
            recip_name = ""
            if m_recip and m_recip.group(1).strip().lower() not in ("the community", "community"):
                recip_name = m_recip.group(1).strip().lstrip('@')
            elif meta.get("recipient") or data.get("recipient"):
                recip_name = _extract_user_str(meta.get("recipient") or data.get("recipient"))

            is_recipient = bool(
                recip_name
                or (bool(meta.get("recipient") or data.get("recipient")) and not is_comm)
            )

            # Only process if this is confirmed as a community gift, individual recipient gift, or native gift sub event
            if gifter and (is_comm or is_recipient or bool(data.get("is_gift") or meta.get("is_gift"))):
                if is_comm:
                    _record_recent_community_gift(gifter)
                elif is_recipient:
                    if _is_recent_community_gift(gifter):
                        return  # Suppress individual recipient row from recent community gift batch

                if g_count > 0:
                    g_id = f"giftsub_chat::{gifter.lower()}::{recip_name.lower()}::{g_count}::{msg_id}"
                    self.callback("SystemNativeGiftedSubs", json.dumps({"user": str(gifter), "count": g_count, "id": g_id, "is_community": is_comm, "is_recipient": is_recipient, "recipient": recip_name}), None)
                    return

        # 2. Sub Celebration Detection in Chat Message (ONLY explicit celebrations from viewers, not bot announcements)
        m_chat_subcel = re.search(
            r'^\s*(?:\[.*?\]\s*)*(?:@?[\w\-]+\s+)?(?:celebrated|celebrates|is\s+celebrating|resubscribed|resubbed|subbed|subscribed|renewed)\s+(?:their\s+|a\s+)?'
            r'(?:subscription\s+for\s+(\d+)\s+months?|(\d+)\s+months?\s+(?:subscription|sub|renewal)|(?:for\s+)?(\d+)\s+months?)\s*!?'
            r'(?:\s*[:\-—,]?\s*(.*))?$',
            str(content),
            re.I
        )
        is_chat_subcel = not is_from_kickbot and not is_user_cmd and not has_question and (
            msg_type in ("sub_celebration", "subscription_celebration", "stream_celebration", "celebration", "resub", "subscription_renewed")
            or str(meta.get("type", "")).lower() in ("sub_celebration", "subscription_celebration", "celebration", "resub")
            or bool(meta.get("celebration") or data.get("celebration") or meta.get("sub_celebration") or data.get("sub_celebration"))
            or bool(meta.get("monthsSubscribed") or data.get("monthsSubscribed") or meta.get("streak") or data.get("streak"))
            or bool(m_chat_subcel)
        )
        if is_chat_subcel:
            sub_user = _get_any_user(meta, "user", "username") or _get_any_user(data, "username", "user", "sender", "subscriber") or username
            months = _extract_sub_celebration_months(data=data, meta=meta, content=content, raw_badges=raw_badges, sender=sender)
            c_msg = str(content or (meta or {}).get("message") or "")
            if sub_user:
                c_id = f"subcel_chat::{sub_user.lower()}::{months}::{msg_id}"
                self.callback("SystemNativeSubCelebration", json.dumps({"user": str(sub_user), "months": months, "message": c_msg, "id": c_id}), None)
                return

        # Check if this chat message is a REAL KICKs / Gift / Donation event from a donor user
        has_kicks_in_data = (
            not is_bot_notification
            and not is_sub_message
            and not is_user_cmd
            and not has_question
            and (
                msg_type in ("gift", "kicks", "tip", "dono", "donation")
                or (data.get("gift") is not None and not is_sub_message)
                or (data.get("kicks") is not None and int(data.get("kicks") or 0) > 0)
                or (data.get("tokens") is not None and int(data.get("tokens") or 0) > 0)
                or (data.get("amount") is not None and int(data.get("amount") or 0) > 0 and msg_type in ("gift", "kicks", "tip", "dono", "donation"))
                or bool(meta.get("kicks") or meta.get("kicks_amount") or meta.get("tokens"))
                or bool(re.search(r'\[(?:kicks|tokens)[^\]]*:\s*\d+\]', str(content), re.I))
                or bool(re.search(r'\[(?:gift|kicks)[^\]]*\]', str(content), re.I))
                or bool(re.search(r'^\s*(?:\[.*?\]\s*)*(?:@?[\w\-]+\s+)?(?:sent|tipped|donated)\s+(?:.*?|\s*)(\d+)\s*(?:kicks?|tokens?)(?:\s*!|\s*$)', str(content), re.I))
            )
        )
        if has_kicks_in_data:
            delivered = self._process_dono_event("ChatMessageKicksDonation", data)
            if delivered:
                return

        # Check for message deduplication
        now = time.time()
        has_server_id = bool(data.get("id"))
        # If server message ID exists, it's unique per message (safe to cache for 120s)
        # If server ID is missing, fallback to username+content which should only suppress rapid duplicate frame echoes (1.0s)
        msg_ttl = 120 if has_server_id else 1.0
        if is_chat_channel_points or badges.get("is_channel_points"):
            msg_ttl = 1.0

        if msg_id in self.seen_message_ids:
            if now - self.seen_message_ids[msg_id] < msg_ttl:
                return
        self.seen_message_ids[msg_id] = now

        # Spam deduplication (do not block channel points redemptions)
        if not (is_chat_channel_points or badges.get("is_channel_points")):
            norm_text = re.sub(r'http[s]?://\S+', '', str(content or "")).strip().lower()
            text_id = f"{username.lower()}::{norm_text}"
            is_adbot = hasattr(self, 'adbot_usernames') and username.lstrip('@').lower() in [u.lower() for u in self.adbot_usernames]
            limit = 60 if is_adbot else 1.5
            if text_id in self.recent_texts:
                if now - self.recent_texts[text_id] < limit:
                    return
            self.recent_texts[text_id] = now

        # Clean old cache keys
        if len(self.seen_message_ids) > 5000:
            for k in list(self.seen_message_ids.keys())[:1000]:
                del self.seen_message_ids[k]
        if len(self.recent_texts) > 5000:
            for k in list(self.recent_texts.keys())[:1000]:
                del self.recent_texts[k]

        # Ensure command is set if not already extracted
        if command is None and words:
            first_word = words[0]
            if first_word.startswith("!") and not first_word.startswith("!!") and not all(c == '!' for c in first_word):
                command = first_word.lower()

        # Deliver to App chat message callback
        self.callback(username, content, command, badges)

    def _process_dono_event(self, event_type, data):
        # Dono scanning is always active so that donations are captured for Stream Summary and logs
        # regardless of whether sound/chat alerts are enabled or what minimum alert amount is configured
        if not isinstance(data, dict):
            return False

        # Exclude ALL subscription, resubscription, and gifted subscription events completely
        ev_type_str = str(event_type).lower()
        msg_type_str = str(data.get("type", "")).lower()
        if any(k in ev_type_str for k in ("subscription", "subscri", "subevent", "giftedsubscription", "giftsub", "resub")):
            return False
        if any(k in msg_type_str for k in ("subscription", "subscri", "subevent", "giftedsubscription", "giftsub", "resub", "gift_sub", "gifted_subscriptions")):
            return False

        # Ensure metadata is decoded if string
        if isinstance(data.get("metadata"), str):
            try:
                data["metadata"] = json.loads(data["metadata"])
            except Exception:
                pass
        meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}

        raw_content = str(data.get("content") or data.get("message") or data.get("comment") or data.get("text") or data.get("note") or "")

        # Exclude text containing subscription/resubscription messages
        if re.search(r'\b(?:subscribed|resubscribed|subbed\s+for|gifted\s+\d+\s+sub|gifted\s+a\s+sub|tier\s*\d+\s*sub)\b', raw_content, re.I):
            return False

        # Exclude bot notifications (e.g. KickBot: "@Dr_BWC just gifted 1 KICKS!")
        if re.search(r'^@[\w\-]+\s+(?:just\s+)?(?:gifted|sent|tipped|donated)\b', raw_content, re.I):
            return False

        # 1. Extract Donor User
        donor_user = None
        for key in ("gifter_username", "username", "slug", "donor", "user_name", "sender_username", "author"):
            val = data.get(key)
            if val and isinstance(val, str) and val.strip():
                donor_user = val.strip()
                break

        if not donor_user:
            for obj_key in ("gifter", "sender", "user", "donor", "author"):
                obj = data.get(obj_key)
                if isinstance(obj, dict):
                    donor_user = obj.get("username") or obj.get("slug") or obj.get("name")
                    if donor_user:
                        break
                elif isinstance(obj, str) and obj.strip():
                    donor_user = obj.strip()
                    break

        if not donor_user:
            for obj_key in ("original_sender", "sender", "user", "gifter"):
                obj = meta.get(obj_key)
                if isinstance(obj, dict):
                    donor_user = obj.get("username") or obj.get("slug") or obj.get("name")
                    if donor_user:
                        break
                elif isinstance(obj, str) and obj.strip():
                    donor_user = obj.strip()
                    break

        if not donor_user and meta.get("username"):
            donor_user = str(meta.get("username")).strip()

        if not donor_user and raw_content:
            match = re.search(r'^([a-zA-Z0-9_]{3,30})\s+(?:sent|tipped|donated)\b', raw_content, re.I)
            if match:
                donor_user = match.group(1).strip()

        if not donor_user:
            donor_user = "Anonymous"

        # Clean donor username
        donor_user = donor_user.strip().lstrip('@')
        if not donor_user or donor_user.lower() in ("anonymous", "anon", "someone", "unknown", "null", "none", "undefined", "n/a"):
            donor_user = "Anonymous"

        # 2. Extract Amount (strictly kicks / tokens / gifts, NOT months or gifted subs)
        amount = 0
        raw_amt = (
            data.get("kicks")
            or data.get("tokens")
            or data.get("kicks_amount")
            or data.get("gift_amount")
            or data.get("amount")
            or data.get("quantity")
            or data.get("count")
            or data.get("total")
            or data.get("price")
            or data.get("cost")
        )
        if raw_amt is not None:
            try:
                amount = int(raw_amt)
            except Exception:
                amount = 0

        # Check nested gift dictionary (e.g. Kick item gift like "Hell Yeah" (K) 1)
        if amount <= 0 and isinstance(data.get("gift"), dict):
            g = data["gift"]
            raw_g = g.get("kicks") or g.get("kicks_amount") or g.get("amount") or g.get("cost") or g.get("price") or g.get("quantity") or g.get("count")
            if raw_g is not None:
                try:
                    amount = int(raw_g)
                except Exception:
                    pass

        # Check metadata dictionary
        if amount <= 0 and meta:
            raw_m = (
                meta.get("kicks")
                or meta.get("kicks_amount")
                or meta.get("tokens")
                or meta.get("amount")
                or meta.get("gift_amount")
                or meta.get("quantity")
                or meta.get("count")
            )
            if raw_m is not None:
                try:
                    amount = int(raw_m)
                except Exception:
                    pass
            if amount <= 0 and isinstance(meta.get("gift"), dict):
                g = meta["gift"]
                raw_g = g.get("kicks") or g.get("kicks_amount") or g.get("amount") or g.get("cost") or g.get("price") or g.get("quantity") or g.get("count")
                if raw_g is not None:
                    try:
                        amount = int(raw_g)
                    except Exception:
                        pass

        # Regex search in raw content for kicks/tokens
        if amount <= 0 and raw_content:
            match = re.search(r'\[(?:gift|kicks|tokens)[^\]]*:\s*(\d+)\]', raw_content, re.I)
            if not match:
                match = re.search(r'(?:sent|tipped|donated)\s+(?:.*?|\s*)(\d+)\s*(?:kicks?|tokens?)', raw_content, re.I)
            if not match:
                match = re.search(r'(\d+)\s*(?:kicks|tokens)\b', raw_content, re.I)
            if match:
                try:
                    amount = int(match.group(1))
                except Exception:
                    pass

        if amount <= 0:
            return False

        # 3. Extract Gift Name & Message
        gift_name = None
        if isinstance(data.get("gift"), dict):
            gift_name = data["gift"].get("name") or data["gift"].get("title") or data["gift"].get("slug")
        elif isinstance(data.get("gift"), str) and data.get("gift").strip():
            gift_name = data.get("gift").strip()
        elif isinstance(meta.get("gift"), dict):
            gift_name = meta["gift"].get("name") or meta["gift"].get("title") or meta["gift"].get("slug")
        elif isinstance(meta.get("gift"), str) and meta.get("gift").strip():
            gift_name = meta.get("gift").strip()
        elif data.get("gift_name"):
            gift_name = data.get("gift_name")
        elif data.get("gift_title"):
            gift_name = data.get("gift_title")

        clean_msg = re.sub(r'\[(?:emote|gift|kicks|tokens)[^\]]*\]', '\n', raw_content).strip()

        extracted_gift = str(gift_name).strip() if (gift_name and str(gift_name).strip()) else ""
        extracted_comment = ""

        # Check if text contains '[user] sent ...' or 'sent ...'
        m_sent = re.search(r'(?:^|\n)(?:@?[\w\-]+\s+)?(?:sent|gifted|tipped|donated)\s+(.+)$', clean_msg, re.DOTALL | re.IGNORECASE)
        if m_sent:
            after_sent = m_sent.group(1).strip()
            lines = [l.strip() for l in after_sent.split('\n') if l.strip()]
            while lines and re.match(r'^(?:\d+\s*(?:kicks?|tokens?)|\d+)$', lines[-1], re.IGNORECASE):
                lines.pop()

            if lines:
                first_line = lines[0]
                sep_m = re.search(r'^(.+?)\s*[:\-—]\s*(.*)$', first_line)
                if sep_m:
                    candidate_gift = sep_m.group(1).strip()
                    rest_of_first = sep_m.group(2).strip()
                    if not re.match(r'^(?:\d+\s*(?:kicks?|tokens?)|\d+)$', candidate_gift, re.IGNORECASE):
                        if not extracted_gift:
                            extracted_gift = candidate_gift
                        rest_lines = ([rest_of_first] if rest_of_first else []) + lines[1:]
                        extracted_comment = ' '.join(rest_lines).strip()
                    else:
                        rest_lines = ([rest_of_first] if rest_of_first else []) + lines[1:]
                        extracted_comment = ' '.join(rest_lines).strip()
                else:
                    if re.match(r'^(?:\d+\s*(?:kicks?|tokens?)|\d+)$', first_line, re.IGNORECASE):
                        extracted_comment = ' '.join(lines[1:]).strip()
                    else:
                        if len(lines) > 1:
                            if not extracted_gift:
                                extracted_gift = first_line
                            extracted_comment = ' '.join(lines[1:]).strip()
                        else:
                            if not extracted_gift:
                                extracted_gift = first_line
        else:
            extracted_comment = clean_msg

        # Clean extracted_gift
        if extracted_gift:
            extracted_gift = re.sub(r'[\.!]+$', '', extracted_gift).strip()
            extracted_gift = re.sub(r'\s+\d+\s*(?:kicks?|tokens?)?$', '', extracted_gift, flags=re.IGNORECASE).strip()

        # Clean extracted_comment
        if extracted_comment:
            extracted_comment = re.sub(r'^(?:sent|gifted|tipped|donated)\s*(?:\d+\s*(?:kicks?|tokens?)|[a-zA-Z0-9_\s]+)?\s*[:!\-—,]\s*', '', extracted_comment, flags=re.IGNORECASE).strip()
            extracted_comment = re.sub(r'^(?:sent|gifted|tipped|donated)\s+', '', extracted_comment, flags=re.IGNORECASE).strip()
            if re.match(r'^(?:\d+\s*(?:kicks?|tokens?)|\d+)$', extracted_comment, re.IGNORECASE):
                extracted_comment = ""

        # Build final message (e.g. "Rage Quit! Wow W stream i love u" or "Rage Quit!")
        if extracted_gift and extracted_comment:
            if extracted_comment.lower().startswith(extracted_gift.lower()):
                clean_comm = extracted_comment[len(extracted_gift):].strip().lstrip(':!-—, ').strip()
                message = f"{extracted_gift}! {clean_comm}".strip() if clean_comm else f"{extracted_gift}!"
            else:
                message = f"{extracted_gift}! {extracted_comment}"
        elif extracted_gift:
            message = f"{extracted_gift}!"
        elif extracted_comment:
            message = extracted_comment
        else:
            message = f"{amount} KICKs"

        # 4. Deduplication
        now = time.time()
        meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
        extracted_id = (
            data.get("id")
            or data.get("cid")
            or data.get("message_id")
            or data.get("tx_id")
            or data.get("transaction_id")
            or data.get("uuid")
            or data.get("chat_id")
            or data.get("gift_id")
            or meta.get("id")
            or meta.get("cid")
            or meta.get("message_id")
            or meta.get("tx_id")
            or meta.get("transaction_id")
            or meta.get("uuid")
            or meta.get("gift_id")
        )
        dono_id = str(extracted_id).strip() if extracted_id else ""
        created_at = str(data.get("created_at") or meta.get("created_at") or data.get("timestamp") or meta.get("timestamp") or "").strip()

        if dono_id:
            dono_key = f"dono_id::{dono_id}"
            ttl = 86400
        elif created_at:
            dono_key = f"dono_ts::{donor_user.strip().lower()}::{amount}::{created_at}::{message.strip().lower()}"
            ttl = 86400
        else:
            # Fallback for events with no ID or timestamp: only suppress rapid duplicate frame echoes across multi-channel subscriptions (1.5s)
            dono_key = f"dono_fb::{donor_user.strip().lower()}::{amount}::{message.strip().lower()}"
            ttl = 1.5

        if dono_key in self.seen_dono_ids and (now - self.seen_dono_ids[dono_key] < ttl):
            return True
        self.seen_dono_ids[dono_key] = now

        if len(self.seen_dono_ids) > 5000:
            for k in list(self.seen_dono_ids.keys())[:1000]:
                del self.seen_dono_ids[k]

        # 5. Deliver as SystemNativeDono with unique CID
        unique_cid = dono_id if dono_id else f"ws_{donor_user.strip().lower()}_{amount}_{now:.4f}"
        native_dono_payload = json.dumps({
            "cid": unique_cid,
            "user": donor_user.strip().lstrip('@'),
            "amount": amount,
            "msg": message
        })
        self.callback("SystemNativeDono", native_dono_payload, None, None)
        return True


class KickRealtimeChatClient(KickWebSocketClient):
    """
    Connects to Kick's Pusher WebSocket cluster and streams chat messages and donation events.
    Drop-in replacement for the KickScraper browser scraper.
    """
    PUSHER_HOST = "ws-us2.pusher.com"
    PUSHER_PORT = 443
    PUSHER_APP_KEY = "32cbd69e4b950bf97679"
    PUSHER_PATH = f"/app/{PUSHER_APP_KEY}?protocol=7&client=js&version=7.6.0&flash=false&cluster=us2"

    def __init__(self, callback):
        super().__init__(callback=callback)
        self.callback = callback
        self.is_running = False
        self.thread = None
        self.ws = None
        self.target_username = ""
        self.adbot_usernames = []
        self.enable_dono_scanning = True
        self.seen_message_ids = {}
        self.recent_texts = {}
        self.seen_dono_ids = {}
        self.seen_event_hashes = {}
        self.processed_dono_cids = {}
        self.processed_chat_ids = {}

    def start(self, channel_target, adbot_usernames=None, access_token=None):
        """
        Starts the real-time client.
        `channel_target` can be either a Kick username (e.g. 'jimboz') or a full kick URL.
        """
        if self.is_running:
            return

        self.is_running = True
        self.adbot_usernames = adbot_usernames or []
        self.seen_message_ids = {}
        self.recent_texts = {}
        self.seen_dono_ids = {}
        self.seen_event_hashes = {}
        self.processed_dono_cids = {}
        self.processed_chat_ids = {}

        # Extract username if given a URL or raw username string
        username = channel_target.strip()
        if "kick.com/" in username:
            parts = username.split("kick.com/")[-1].split("/")
            if parts[0] == "popout" and len(parts) > 1:
                username = parts[1]
            else:
                username = parts[0]
            username = username.split("?")[0].split("#")[0]

        self.target_username = username.strip().lstrip('@').lower()

        self.thread = threading.Thread(
            target=self._run_loop,
            args=(username, access_token),
            daemon=True
        )
        self.thread.start()

    def stop(self):
        self.is_running = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

    def _run_loop(self, username, access_token):
        from kick_api import KickAPIClient
        self.callback("System", f"Resolving Kick channel info for '{username}'...", None)

        channel_info, err = KickAPIClient.resolve_channel_info(username, access_token)
        if not channel_info:
            self.callback("System", f"[Error] {err or 'Channel lookup failed'}. Please check the username or URL.", None)
            self.is_running = False
            return

        chatroom_id = channel_info.get("chatroom_id")
        channel_id = channel_info.get("channel_id")
        clean_name = channel_info.get("username", username)
        self.chatroom_id = chatroom_id
        self.channel_id = channel_id
        self.target_username = clean_name.strip().lstrip('@').lower()

        self.callback("System", f"Connecting to Kick Live Chat ({clean_name} | Chatroom: {chatroom_id})...", None)

        reconnect_delay = 2
        has_announced_connected = False
        while self.is_running:
            try:
                self.ws = PureWebSocket(self.PUSHER_HOST, self.PUSHER_PORT, self.PUSHER_PATH, ssl_wrap=True)
                self.ws.connect(timeout=10)
                if not has_announced_connected:
                    self.callback("System", f"Connected to Kick Live Chat for {clean_name}!", None)
                    has_announced_connected = True
                reconnect_delay = 2

                # Subscribe to all relevant channel and chatroom endpoints
                channels_to_sub = [
                    f"chatrooms.{chatroom_id}.v2",
                    f"chatrooms.{chatroom_id}",
                    f"chatroom.{chatroom_id}.v2",
                    f"chatroom.{chatroom_id}",
                    f"chatroom_{chatroom_id}",
                    f"chatrooms.{chatroom_id}.gifts",
                    f"chatrooms.{chatroom_id}.subscriptions",
                    f"chatroom.{chatroom_id}.gifts",
                    f"chatroom.{chatroom_id}.subscriptions",
                    f"chatrooms.{chatroom_id}.kicks",
                    f"chatroom.{chatroom_id}.kicks",
                    f"chatrooms.{chatroom_id}.channel-points",
                    f"chatrooms.{chatroom_id}.channel_points",
                    f"chatrooms.{chatroom_id}.rewards",
                    f"chatrooms.{chatroom_id}.points",
                    f"chatroom.{chatroom_id}.channel-points",
                    f"chatroom.{chatroom_id}.channel_points",
                    f"chatroom.{chatroom_id}.rewards",
                    f"chatroom.{chatroom_id}.points"
                ]

                # Fallback: if channel_id is not yet known or different from chatroom_id, subscribe to candidate IDs
                candidate_chan_ids = [channel_id] if channel_id else []
                if chatroom_id and chatroom_id not in candidate_chan_ids:
                    candidate_chan_ids.append(chatroom_id)

                for cid in candidate_chan_ids:
                    channels_to_sub.extend([
                        f"channel.{cid}",
                        f"channel.{cid}.v2",
                        f"channel_{cid}",
                        f"channel.{cid}.gifts",
                        f"channel.{cid}.kicks",
                        f"channel.{cid}.subscriptions",
                        f"channel.{cid}.community-sub-gifts",
                        f"channel.{cid}.sub-gifts",
                        f"channel.{cid}.channel-points",
                        f"channel.{cid}.channel_points",
                        f"channel.{cid}.rewards",
                        f"channel.{cid}.points",
                        f"channel.{cid}.community-points"
                    ])

                for ch in set(channels_to_sub):
                    sub_payload = {
                        "event": "pusher:subscribe",
                        "data": {"auth": "", "channel": ch}
                    }
                    self.ws.send_text(json.dumps(sub_payload))

                # Background ping loop
                def _ping_loop(ws_ref):
                    while self.is_running and ws_ref == self.ws and not ws_ref.closed:
                        time.sleep(25)
                        try:
                            ws_ref.send_text(json.dumps({"event": "pusher:ping", "data": {}}))
                        except Exception:
                            break

                threading.Thread(target=_ping_loop, args=(self.ws,), daemon=True).start()

                # Read message frames
                while self.is_running and not self.ws.closed:
                    opcode, payload = self.ws.recv_frame()
                    if opcode is None:
                        break
                    if opcode == 0x8:  # Close opcode
                        break
                    if opcode == 0x9:  # Ping
                        # Respond with Pong
                        try:
                            self.ws.send_pong(payload)
                        except Exception:
                            pass
                        continue
                    if opcode == 0xA:  # Pong response from server
                        continue
                    if opcode == 0x1:  # Text frame
                        try:
                            text_data = payload.decode("utf-8", errors="ignore")
                            self._handle_pusher_message(text_data)
                        except Exception as e:
                            import traceback
                            print(f"[WebSocket] Error handling Pusher frame: {e}\n{traceback.format_exc()}")

            except Exception as e:
                if self.is_running:
                    self.callback("System", f"[Warning] Connection lost ({e}). Reconnecting in {reconnect_delay}s...", None)
            finally:
                if self.ws:
                    self.ws.close()

            if self.is_running:
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 1.5, 30)

        self.callback("System", f"Chat monitoring stopped for {clean_name}.", None)


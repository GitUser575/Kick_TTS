import json
import os
import re
import urllib.request

try:
    import requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

KICK_API_BASE = "https://api.kick.com/public/v1"
CACHE_FILE = "kick_channel_cache.json"


class KickAPIClient:
    """Helper class for resolving Kick channel and chatroom metadata."""

    @classmethod
    def _load_cache(cls):
        try:
            if os.path.exists(CACHE_FILE):
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    @classmethod
    def _save_cache(cls, username, data):
        try:
            cache = cls._load_cache()
            cache[str(username).lower()] = data
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(cache, f, indent=2)
        except Exception:
            pass

    @staticmethod
    def _fetch_url(url, headers, timeout=8):
        if _HAS_REQUESTS:
            try:
                resp = requests.get(url, headers=headers, timeout=timeout)
                return resp.status_code, resp.text
            except Exception:
                pass
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                body = response.read().decode("utf-8", errors="ignore")
                return response.status, body
        except Exception:
            return None, None

    @classmethod
    def resolve_channel_info(cls, username, access_token=None):
        """
        Resolves channel metadata (chatroom_id, channel_id, slug) for a given Kick username.
        Uses public channels endpoint, Kick v2/v1 direct API, local persistent cache, and HTML fallback.
        Also supports direct numeric chatroom IDs.
        """
        raw_clean = str(username).strip().lstrip("@").lower()
        if not raw_clean:
            return None, "Username cannot be empty"

        # 1. Clean URL prefixes if user pasted full channel or popout URL
        if "kick.com/" in raw_clean:
            parts = raw_clean.split("kick.com/")[-1].split("/")
            if parts[0] == "popout" and len(parts) > 1:
                raw_clean = parts[1]
            elif parts[0] == "chatroom" and len(parts) > 1:
                raw_clean = parts[1]
            else:
                raw_clean = parts[0]
            raw_clean = raw_clean.split("?")[0].split("#")[0].strip()

        # 2. Check if user provided direct numeric Chatroom/Channel ID
        if raw_clean.isdigit():
            cid = int(raw_clean)
            res = {
                "username": raw_clean,
                "channel_id": cid,
                "chatroom_id": cid,
                "slug": raw_clean
            }
            cls._save_cache(raw_clean, res)
            return res, None

        m_num = re.search(r'chatroom[s]?[\._\-](\d+)', raw_clean)
        if m_num:
            cid = int(m_num.group(1))
            res = {
                "username": raw_clean,
                "channel_id": cid,
                "chatroom_id": cid,
                "slug": raw_clean
            }
            cls._save_cache(raw_clean, res)
            return res, None

        headers = {
            "Accept": "application/json, text/html, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
            "sec-ch-ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none"
        }
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"

        candidates = [raw_clean]
        hyphenated = raw_clean.replace("_", "-")
        if hyphenated != raw_clean and hyphenated not in candidates:
            candidates.append(hyphenated)

        for candidate in candidates:
            endpoints = [
                f"https://kick.com/api/v2/channels/{candidate}",
                f"https://kick.com/api/v2/channels/{candidate}/chatroom",
                f"https://kick.com/api/v1/channels/{candidate}",
                f"{KICK_API_BASE}/channels/{candidate}",
                f"https://kick.com/{candidate}"
            ]
            for url in endpoints:
                status, body = cls._fetch_url(url, headers=headers, timeout=6)
                if status == 200 and body:
                    body_str = body.strip()
                    if body_str.startswith("{"):
                        try:
                            d = json.loads(body_str)
                            if "data" in d and isinstance(d["data"], dict):
                                d = d["data"]
                            chatroom = d.get("chatroom", {}) if isinstance(d.get("chatroom"), dict) else {}
                            chatroom_id = chatroom.get("id") or d.get("chatroom_id")
                            if not chatroom_id and "slow_mode" in d and "id" in d:
                                chatroom_id = d.get("id")
                            channel_id = d.get("id") or d.get("channel_id")
                            slug = d.get("slug") or candidate
                            if chatroom_id:
                                res = {
                                    "username": candidate,
                                    "channel_id": channel_id,
                                    "chatroom_id": int(chatroom_id),
                                    "slug": slug
                                }
                                cls._save_cache(candidate, res)
                                cls._save_cache(raw_clean, res)
                                return res, None
                        except Exception:
                            pass
                    else:
                        # Direct HTML page regex fallback & Next.js __NEXT_DATA__
                        if "__NEXT_DATA__" in body_str:
                            m_next = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', body_str, re.DOTALL)
                            if m_next:
                                try:
                                    next_json = json.loads(m_next.group(1))
                                    page_props = next_json.get("props", {}).get("pageProps", {})
                                    chan = page_props.get("channel", {})
                                    c_room = chan.get("chatroom", {})
                                    cr_id = c_room.get("id") or chan.get("chatroom_id")
                                    if cr_id:
                                        res = {
                                            "username": candidate,
                                            "channel_id": chan.get("id"),
                                            "chatroom_id": int(cr_id),
                                            "slug": candidate
                                        }
                                        cls._save_cache(candidate, res)
                                        cls._save_cache(raw_clean, res)
                                        return res, None
                                except Exception:
                                    pass

                        m_chat = re.search(r'\"chatroom\":\{\"id\":(\d+)', body_str) or re.search(r'\"chatroom_id\":(\d+)', body_str)
                        m_chan = re.search(r'\"channel\":\{\"id\":(\d+)', body_str) or re.search(r'\"channel_id\":(\d+)', body_str)
                        if m_chat:
                            c_id = int(m_chat.group(1))
                            ch_id = int(m_chan.group(1)) if m_chan else None
                            res = {
                                "username": candidate,
                                "channel_id": ch_id,
                                "chatroom_id": c_id,
                                "slug": candidate
                            }
                            cls._save_cache(candidate, res)
                            cls._save_cache(raw_clean, res)
                            return res, None

        # Check persistent cache if network request was blocked by Cloudflare anti-bot
        cache = cls._load_cache()
        if raw_clean in cache:
            return cache[raw_clean], None
        for candidate in candidates:
            if candidate in cache:
                return cache[candidate], None

        return None, (
            f"Could not resolve chatroom for Kick user '{raw_clean}'. "
            "Kick's Cloudflare anti-bot may be blocking automated lookups. "
            "You can enter your numeric Chatroom ID directly into the channel box to connect."
        )

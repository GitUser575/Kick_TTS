import json
import os
import requests
import re
import uuid
import threading

class RoleplayManager:
    def __init__(self, app_ref):
        self.app = app_ref
        self.universe_lore_path = "universe_lore.json"
        self.character_lore_path = "character_lore.json"
        self.global_rules_path = "global_rules.json"
        
        self.universe_lore = self.load_json(self.universe_lore_path, default="")
        self.character_lore = self.load_json(self.character_lore_path, default={})
        self.global_rules = self.load_json(self.global_rules_path, default="")
        
        self.audio_dir = "rp_audio"
        os.makedirs(self.audio_dir, exist_ok=True)
        # Clear out any old files from previous sessions
        for f in os.listdir(self.audio_dir):
            if f.endswith('.wav'):
                try:
                    os.remove(os.path.join(self.audio_dir, f))
                except:
                    pass
        self.rp_history = []
        self.pending_llm_queue = []
        
        # Start queue monitor
        threading.Thread(target=self._llm_queue_monitor, daemon=True).start()

    def load_json(self, path, default=None):
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                self.app.append_system_log(f"\n[System] Warning: Could not load {path}: {e}")
        return default

    def reload_lore(self):
        self.universe_lore = self.load_json(self.universe_lore_path, default="")
        self.character_lore = self.load_json(self.character_lore_path, default={})
        self.global_rules = self.load_json(self.global_rules_path, default="")
        self.app.append_system_log("\n[System] In-memory character lore and rules reloaded from disk.")

    def save_json(self, path, data):
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
        except Exception:
            pass

    def update_voice_command(self, old_cmd, new_cmd):
        import re
        
        # Update keys
        if old_cmd in self.character_lore:
            old_lore = self.character_lore.pop(old_cmd)
            
            def is_empty(l):
                if isinstance(l, dict):
                    return not l.get('backstory', '').strip() and not l.get('rules', '').strip()
                elif isinstance(l, str):
                    return not l.strip()
                return True
                
            if new_cmd not in self.character_lore:
                self.character_lore[new_cmd] = old_lore
            else:
                # If renaming to a command that already has orphaned lore, preserve the existing lore
                # unless it is empty and the old command had actual lore.
                if is_empty(self.character_lore[new_cmd]) and not is_empty(old_lore):
                    self.character_lore[new_cmd] = old_lore
            
        def safe_replace(text, old, new):
            if not isinstance(text, str): return text
            # Replace 'old' only if it's not immediately followed by a word character
            # so !john doesn't match !johnny
            pattern = re.escape(old) + r'(?![a-zA-Z0-9_])'
            return re.sub(pattern, new, text)
        
        # Update references in lore values
        for cmd, lore in self.character_lore.items():
            if isinstance(lore, dict):
                if 'backstory' in lore:
                    lore['backstory'] = safe_replace(lore['backstory'], old_cmd, new_cmd)
                if 'rules' in lore:
                    lore['rules'] = safe_replace(lore['rules'], old_cmd, new_cmd)
            elif isinstance(lore, str):
                self.character_lore[cmd] = safe_replace(lore, old_cmd, new_cmd)
                
        self.save_json(self.character_lore_path, self.character_lore)
        
        # Update references in universe lore
        if isinstance(self.universe_lore, str):
            self.universe_lore = safe_replace(self.universe_lore, old_cmd, new_cmd)
            self.save_json(self.universe_lore_path, self.universe_lore)
            
        # Update references in global rules
        if isinstance(self.global_rules, str):
            self.global_rules = safe_replace(self.global_rules, old_cmd, new_cmd)
            self.save_json(self.global_rules_path, self.global_rules)
            
    def get_api_key(self):
        return self.app.settings.get("deepseek_api_key", "")

    def get_model_name(self):
        model = self.app.settings.get("deepseek_model", "deepseek-flash")
        if isinstance(model, str) and model.strip():
            return model.strip()
        return "deepseek-flash"

    def _llm_queue_monitor(self):
        import time
        while True:
            try:
                self._check_llm_queue()
            except Exception as e:
                pass
            time.sleep(1.0)

    def _check_llm_queue(self):
        if not hasattr(self, 'pending_llm_queue') or not self.pending_llm_queue:
            return
            
        to_start = []
        with self.app.tts.queue_lock:
            # Get active groups
            active_groups = []
            for g in self.app.tts.queue_groups:
                if g.get("deleted", False):
                    continue
                if g["group_id"] == getattr(self.app.tts, "skip_group_id", None):
                    continue
                if g.get("last_chunk_added", False) and g.get("chunks_played", 0) >= g.get("chunks_added", 1):
                    continue
                active_groups.append(g)
                
            # Sort by priority and order (matches TTS playback order)
            active_groups.sort(key=lambda x: (x.get("priority", 2), x.get("order", 0)))
            
            active_rp_count = 0
            for g in active_groups:
                if g.get("category") == "RP":
                    if not g.get("is_pending_llm", False) or g.get("is_processing_llm", False):
                        active_rp_count += 1
                    elif g.get("is_pending_llm", False) and not g.get("is_processing_llm", False):
                        if active_rp_count < 5:
                            # Start this LLM generation
                            g["is_processing_llm"] = True
                            g["text"] = "[Generating Response...]"
                            to_start.append(g["group_id"])
                            self.app.tts.queue_condition.notify_all()
                            active_rp_count += 1
                        else:
                            # It's pending but there are already 5 or more RPs ahead of it
                            active_rp_count += 1

        for group_id in to_start:
            # Find in pending_llm_queue
            idx = -1
            for i, item in enumerate(self.pending_llm_queue):
                if item[0] == group_id:
                    idx = i
                    break
            
            if idx != -1:
                _, run_func = self.pending_llm_queue.pop(idx)
                import threading
                threading.Thread(target=run_func, daemon=True).start()

    def generate_roleplay(self, username, user_message, characters):
        if not self.get_api_key():
            self.app.append_system_log("\n[System] DeepSeek API Key not set. Cannot perform roleplay.")
            return
            
        import uuid
        group_id = str(uuid.uuid4())
        
        with self.app.tts.queue_lock:
            self.app.tts.group_counter += 1
            import time
            placeholder_group = {
                "group_id": group_id,
                "commands": [c[0] for c in characters],
                "text": "[Pending LLM Generation]",
                "category": "RP",
                "user": username,
                "generated_text_length": 0,
                "total_audio_duration": 0.0,
                "played_audio_duration": 0.0,
                "current_playing_start_time": None,
                "current_playing_duration": 0.0,
                "priority": 2,
                "order": time.time(),
                "chunks_added": 0,
                "chunks_played": 0,
                "last_chunk_added": False,
                "is_pending_llm": True
            }
            self.app.tts.queue_groups.append(placeholder_group)
            self.app.tts.queue_condition.notify_all()
            
        def _thread():
            nonlocal user_message
            import re
                        # Helper to download image, resize down to 600px max dimension, and convert to Base64 data URI
            def download_image_as_data_uri(img_url, max_dim=600, timeout=5):
                try:
                    import urllib.request
                    import base64
                    import io
                    req = urllib.request.Request(
                        img_url,
                        headers={
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                            'Referer': 'https://twitter.com/'
                        }
                    )
                    with urllib.request.urlopen(req, timeout=timeout) as img_resp:
                        img_bytes = img_resp.read()
                        if not img_bytes or len(img_bytes) < 100:
                            return None
                        content_type = img_resp.headers.get('Content-Type', '').lower()
                        mime = 'image/jpeg'
                        if 'png' in content_type or img_bytes.startswith(b'\x89PNG'):
                            mime = 'image/png'
                        elif 'webp' in content_type or (img_bytes[:4] == b'RIFF' and img_bytes[8:12] == b'WEBP'):
                            mime = 'image/webp'
                        elif 'gif' in content_type or img_bytes.startswith(b'GIF8'):
                            mime = 'image/gif'
                        
                        # Resize image so the longest side is at most max_dim (600px) while preserving aspect ratio
                        try:
                            from PIL import Image, ImageOps
                            img = Image.open(io.BytesIO(img_bytes))
                            try:
                                img = ImageOps.exif_transpose(img)
                            except Exception:
                                pass
                            
                            w, h = img.size
                            if max(w, h) > max_dim:
                                ratio = max_dim / float(max(w, h))
                                new_w = max(1, int(round(w * ratio)))
                                new_h = max(1, int(round(h * ratio)))
                                
                                resample = getattr(Image, 'Resampling', Image).LANCZOS
                                img = img.resize((new_w, new_h), resample=resample)
                                
                                out_io = io.BytesIO()
                                if mime == 'image/png':
                                    img.save(out_io, format='PNG', optimize=True)
                                elif mime == 'image/webp':
                                    img.save(out_io, format='WEBP', quality=85)
                                else:
                                    if img.mode in ('RGBA', 'LA', 'P'):
                                        img = img.convert('RGB')
                                    img.save(out_io, format='JPEG', quality=85)
                                    mime = 'image/jpeg'
                                
                                img_bytes = out_io.getvalue()
                        except Exception:
                            # Fall back safely if Pillow is unavailable or image cannot be decoded
                            pass

                        b64_str = base64.b64encode(img_bytes).decode('utf-8')
                        return f"data:{mime};base64,{b64_str}"
                except Exception as e:
                    return None

            # --- Handle Twitter / X.com Links & Images (Vision API) ---
            llm_user_message = user_message
            attached_image_urls = []
            attached_data_uris = []
            
            # Match all common variations of X / Twitter URLs
            match = re.search(r'https?://(?:www\.|mobile\.)?(?:twitter\.com|x\.com|vxtwitter\.com|fxtwitter\.com|fixupx\.com)/([a-zA-Z0-9_]+)/status/([0-9]+)', user_message, re.IGNORECASE)
            if match:
                screen_name = match.group(1)
                status_id = match.group(2)
                
                # Fetch tweet data with fallback across FixupX and VxTwitter
                data = None
                endpoints = [
                    f"https://api.fxtwitter.com/{screen_name}/status/{status_id}",
                    f"https://api.vxtwitter.com/{screen_name}/status/{status_id}"
                ]
                
                import urllib.request
                import json
                
                for api_url in endpoints:
                    try:
                        req = urllib.request.Request(api_url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
                        with urllib.request.urlopen(req, timeout=4) as response:
                            if response.status == 200:
                                parsed = json.loads(response.read().decode())
                                if parsed and isinstance(parsed, dict) and ('tweet' in parsed or 'text' in parsed):
                                    data = parsed
                                    break
                    except Exception:
                        pass
                
                # If the tweet was deleted, private, rate-limited, or unreachable, invalidate and cancel this roleplay request
                if not data:
                    self.app.after(0, self.app.append_system_log, f"\n[Roleplay Cancelled] Invalid or unreachable X/Twitter post: https://x.com/{screen_name}/status/{status_id}")
                    with self.app.tts.queue_lock:
                        g = next((x for x in self.app.tts.queue_groups if x["group_id"] == group_id), None)
                        if g:
                            g["deleted"] = True
                            g["is_pending_llm"] = False
                    return
                
                # Extract tweet fields
                tweet_obj = data.get('tweet', data)
                tweet_text = tweet_obj.get('text', '').strip()
                author_name = tweet_obj.get('author', {}).get('name') or tweet_obj.get('user_name', screen_name)
                
                injections = []
                if tweet_text:
                    injections.append(f"[Attached X/Twitter Post by @{screen_name} ({author_name})]:\n\"{tweet_text}\"")
                
                # Extract image attachments (up to first 2 images)
                extracted_photos = []
                media = tweet_obj.get('media', {})
                if isinstance(media, dict) and 'photos' in media:
                    for p in media['photos']:
                        p_url = p.get('url') if isinstance(p, dict) else str(p)
                        if p_url and p_url.startswith('http'):
                            extracted_photos.append(p_url)
                elif 'mediaURLs' in tweet_obj and isinstance(tweet_obj['mediaURLs'], list):
                    for p_url in tweet_obj['mediaURLs']:
                        if isinstance(p_url, str) and p_url.startswith('http') and not any(p_url.lower().endswith(ext) for ext in ('.mp4', '.m3u8', '.mov')):
                            extracted_photos.append(p_url)
                
                attached_image_urls = extracted_photos[:2]
                for raw_url in attached_image_urls:
                    d_uri = download_image_as_data_uri(raw_url)
                    if d_uri:
                        attached_data_uris.append(d_uri)
                if attached_data_uris:
                    injections.append(f"[{len(attached_data_uris)} image(s) attached from this post for visual reference]")
                
                # Handle quoted tweet context if present
                qrt_obj = tweet_obj.get('quote') or tweet_obj.get('qrt')
                if isinstance(qrt_obj, dict):
                    qrt_text = qrt_obj.get('text', '').strip()
                    qrt_author = qrt_obj.get('author', {}).get('name') or qrt_obj.get('user_name', '')
                    if qrt_text:
                        injections.append(f"[Quoted Tweet Context by {qrt_author}]:\n\"{qrt_text}\"")
                
                if injections:
                    llm_user_message = llm_user_message + "\n\n" + "\n\n".join(injections)
            # -----------------------------------------------------------

            # --- Handle #chat Tag (Active Stream Chatters) ---
            has_chat_tag = bool(re.search(r'(?<![#\w])#chat\b', user_message, re.IGNORECASE))
            active_chatters = []
            if has_chat_tag:
                excluded_bots = {"kickbot", "botrix"}
                raw_chatters = self.app.get_active_chatters_list() if hasattr(self.app, 'get_active_chatters_list') else []
                active_chatters = [
                    name for name in raw_chatters
                    if name and str(name).strip().lstrip('@').lower() not in excluded_bots
                ]

                def _norm_chat(m):
                    txt = m.group(0)
                    return "Chat" if len(txt) > 1 and txt[1].isupper() else "chat"

                llm_user_message = re.sub(r'(?<![#\w])#chat\b', _norm_chat, llm_user_message, flags=re.IGNORECASE)
                user_message = re.sub(r'(?<![#\w])#chat\b', _norm_chat, user_message, flags=re.IGNORECASE)
                self.app.append_system_log(f"\n[Roleplay] Resolved #chat tag with {len(active_chatters)} active chatter(s): {', '.join(active_chatters) if active_chatters else 'None'}")
            # ------------------------------------------------
            
            cmd_names = [c[0] for c in characters]
            self.app.append_system_log(f"\n[Roleplay] Requesting LLM response for {', '.join(cmd_names)}...")

            sys_prompt = "You are participating in a character roleplay.\n"
            if self.global_rules:
                sys_prompt += f"Global Rules:\n{self.global_rules}\n\n"
            if self.universe_lore:
                sys_prompt += f"Universe Lore:\n{self.universe_lore}\n\n"
                
            primary_cmds = set([c[0].lower() for c in characters])
            def format_lore(cmd, name, lore, is_primary):
                role_label = "PRIMARY CHARACTER (You are acting as this character)" if is_primary else "SECONDARY CHARACTER (Context only, DO NOT act as this character)"
                return f"Character: {name} ({cmd}) - {role_label}\nBackstory: {lore.get('backstory', '')}\nRules: {lore.get('rules', '')}\n"

            import re
            
            processed_cmds_lower = set()
            cmds_to_process = [c[0] for c in characters]
                
            # Also find mentions in user_message
            user_mentions = list(set(re.findall(r'(?<!\S)![a-zA-Z0-9_]+', user_message, flags=re.IGNORECASE)))
            cmds_to_process.extend(user_mentions)
            
            lore_blocks = []
            
            while cmds_to_process:
                curr_cmd = cmds_to_process.pop(0)
                curr_cmd_lower = curr_cmd.lower()
                
                if curr_cmd_lower in processed_cmds_lower:
                    continue
                    
                processed_cmds_lower.add(curr_cmd_lower)
                
                # Check if this command is valid
                voice_match = next((v for v in self.app.voices if v["command"].lower() == curr_cmd_lower), None)
                if not voice_match:
                    continue
                    
                c_name = voice_match.get("name", curr_cmd)
                c_cmd = voice_match["command"]
                
                lore_key = next((k for k in self.character_lore.keys() if k.lower() == curr_cmd_lower), c_cmd)
                c_lore = self.character_lore.get(lore_key, {})
                if isinstance(c_lore, str):
                    c_lore = {"backstory": c_lore, "rules": ""}
                    
                is_primary = curr_cmd_lower in primary_cmds
                lore_blocks.append(format_lore(c_cmd, c_name, c_lore, is_primary))
                
                # Replace mention in user_message (case-insensitive)
                pattern = re.escape(curr_cmd) + r'(?![a-zA-Z0-9_])'
                llm_user_message = re.sub(pattern, c_name, llm_user_message, flags=re.IGNORECASE)
                user_message = re.sub(pattern, c_name, user_message, flags=re.IGNORECASE)
                
                # Scan this lore for additional commands to include
                lore_text = c_lore.get('backstory', '') + " " + c_lore.get('rules', '')
                lore_mentions = list(set(re.findall(r'(?<!\S)![a-zA-Z0-9_]+', lore_text, flags=re.IGNORECASE)))
                for m in lore_mentions:
                    if m.lower() not in processed_cmds_lower:
                        cmds_to_process.append(m)
                        
            if lore_blocks:
                sys_prompt += "\n".join(lore_blocks)

            if has_chat_tag:
                if active_chatters:
                    chatters_str = ", ".join(active_chatters)
                    sys_prompt += f"\nActive Stream Chatters / Viewers in Chat:\n{chatters_str}\n\n"
                    sys_prompt += (
                        "Active Chatters Context Rule: The user's prompt references 'chat' (representing the active stream chatters and viewers listed above). "
                        "Each name listed in Active Stream Chatters is a real person and active viewer currently watching and participating in the live chat. "
                        "When answering questions, making choices, picking favorites, or speaking to/about 'chat', treat these names as people and pick or refer to them naturally as individuals from the chat audience.\n\n"
                    )
                else:
                    sys_prompt += (
                        "\nActive Stream Chatters / Viewers in Chat:\n(None currently active / chat is empty)\n\n"
                        "Active Chatters Context Rule: The user's prompt references 'chat', but there are currently no other active chatters detected in the chat.\n\n"
                    )

            sys_prompt += "\nConstraints:\n"
            sys_prompt += "- Do not use em dashes or asterisks.\n"
            sys_prompt += "- You are strictly forbidden from using any bracketed emotives other than this exact list of 5 allowed emotives: [clear throat], [cough], [groan], [chuckle], [laugh]. Do not invent new ones. Using these emotives is entirely optional; responses without them are completely fine.\n"
            sys_prompt += "- Never break character. Fully assume the character(s) and their full lore.\n"
            sys_prompt += "- Be absolutely sure to keep the roleplay rules and character traits for each character separate. Do not apply one character's rules or personality to another character.\n"
            sys_prompt += "- Context Grounding: Listeners only hear your response, not the chat message. Ensure listeners immediately grasp the topic by naturally weaving the subject or premise into your opening sentence. You do not need to mention the user's username.\n"
            sys_prompt += "- Variety & Voice: Never use repetitive formulaic openings like 'Oh, you want to know...', 'So you're asking...', or 'You want to hear about...'. Instead, seamlessly embed the context through the character's unique personality (e.g., reacting with amusement or skepticism, echoing a key word or phrase, offering a sharp retort, or jumping straight into an opinion on the topic).\n"
            sys_prompt += "- Treat this request as a completely independent and isolated interaction. Do not hallucinate or assume any past chat history exists. Rely strictly on the provided lore and the provided Active Stream Chatters list (if applicable).\n"
            
            max_words = self.app.settings.get("rp_max_words", 60)
            
            if len(characters) > 1:
                iterations = self.app.settings.get("rp_iterations", 1)
                char_cmds_str = "' or '".join([c[0] for c in characters])
                sys_prompt += f"\nReturn your response strictly as a JSON array of objects, where each object has 'character' (one of: '{char_cmds_str}') and 'response' (the spoken text).\n"
                sys_prompt += f"Provide {iterations} back-and-forth iteration(s). Each response must be strictly under {max_words} words."
            else:
                name1 = characters[0][1]
                sys_prompt += f"\nCRITICAL INSTRUCTION: You MUST respond ONLY as {name1}. Do NOT act as any of the secondary characters provided for context. Ensure your response is strictly under {max_words} words."
            headers = {
                "Authorization": f"Bearer {self.get_api_key()}",
                "Content-Type": "application/json"
            }
            
            user_prompt_text = f"User '{username}' says: {llm_user_message}"
            
            # Construct OpenAI-compatible multimodal content for deepseek-v4-flash-vision-exp
            if attached_data_uris:
                user_content = [{"type": "text", "text": user_prompt_text}]
                for data_uri in attached_data_uris:
                    user_content.append({
                        "type": "image_url",
                        "image_url": {
                            "url": data_uri,
                            "detail": "low"
                        }
                    })
            else:
                user_content = user_prompt_text
            
            deepseek_model = self.get_model_name()
            payload = {
                "model": deepseek_model,
                "messages": [
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": user_content}
                ],
                "response_format": {"type": "json_object"} if len(characters) > 1 else {"type": "text"}
            }
            def _cleanup_failed_rp(err_msg=None):
                if err_msg:
                    self.app.after(0, self.app.append_system_log, err_msg)
                with self.app.tts.queue_lock:
                    g = next((x for x in self.app.tts.queue_groups if x["group_id"] == group_id), None)
                    if g:
                        g["deleted"] = True
                        g["is_pending_llm"] = False
                        g["is_processing_llm"] = False
                        g["last_chunk_added"] = True
                    self.app.tts.queue_condition.notify_all()
                self.pending_llm_queue = [item for item in self.pending_llm_queue if item[0] != group_id]
                self._check_llm_queue()

            max_retries = 2
            resp = None
            for attempt in range(1, max_retries + 1):
                # If item was explicitly cancelled by user while waiting, abort
                with self.app.tts.queue_lock:
                    g = next((x for x in self.app.tts.queue_groups if x["group_id"] == group_id), None)
                    if g and g.get("user_cancelled", False):
                        return

                try:
                    resp = requests.post("https://api.deepseek.com/chat/completions", headers=headers, json=payload, timeout=(5, 30))
                    if resp.status_code != 200:
                        code = resp.status_code
                        err_detail = ""
                        try:
                            err_json = resp.json()
                            if "error" in err_json and "message" in err_json["error"]:
                                err_detail = err_json["error"]["message"]
                        except Exception:
                            err_detail = resp.text[:200] if resp.text else ""

                        # Transient retryable status codes (e.g. 429 rate limit / server busy or 5xx server error)
                        if attempt < max_retries and (code == 429 or 500 <= code <= 599):
                            self.app.after(0, self.app.append_system_log, f"\n[Roleplay] DeepSeek returned HTTP {code} on attempt {attempt}/{max_retries}. Retrying request in 1.5s...")
                            with self.app.tts.queue_lock:
                                g = next((x for x in self.app.tts.queue_groups if x["group_id"] == group_id), None)
                                if g:
                                    g["text"] = "[Retrying Response...]"
                                    self.app.tts.queue_condition.notify_all()
                            import time
                            time.sleep(1.5)
                            continue

                        if code in (401, 403):
                            msg = f"\n[DeepSeek Error] Invalid or unauthorized API key (HTTP {code}). {err_detail}".strip()
                        elif code == 402:
                            msg = f"\n[DeepSeek Error] Insufficient balance on API key (HTTP 402). {err_detail}".strip()
                        elif code == 410:
                            msg = f"\n[DeepSeek Error] DeepSeek service or endpoint unavailable (HTTP 410). {err_detail}".strip()
                        elif code == 429:
                            msg = f"\n[DeepSeek Error] Rate limit exceeded or server busy (HTTP 429). Skipping item in TTS queue. {err_detail}".strip()
                        elif 500 <= code <= 599:
                            msg = f"\n[DeepSeek Error] DeepSeek server error (HTTP {code}). Skipping item in TTS queue. {err_detail}".strip()
                        else:
                            msg = f"\n[DeepSeek Error] Request failed with HTTP {code}: {err_detail}".strip()

                        _cleanup_failed_rp(msg)
                        return

                    # Successful response received (HTTP 200)
                    break

                except requests.exceptions.Timeout:
                    if attempt < max_retries:
                        self.app.after(0, self.app.append_system_log, f"\n[Roleplay] DeepSeek read timed out (attempt {attempt}/{max_retries}). Retrying roleplay request...")
                        with self.app.tts.queue_lock:
                            g = next((x for x in self.app.tts.queue_groups if x["group_id"] == group_id), None)
                            if g:
                                g["text"] = "[Retrying Response...]"
                                self.app.tts.queue_condition.notify_all()
                        import time
                        time.sleep(1.0)
                        continue
                    else:
                        _cleanup_failed_rp(f"\n[Roleplay] DeepSeek timed out again on retry (attempt {attempt}/{max_retries}). Skipping item in TTS queue.")
                        return
                except Exception as e:
                    if attempt < max_retries and "timed out" in str(e).lower():
                        self.app.after(0, self.app.append_system_log, f"\n[Roleplay] DeepSeek connection timed out (attempt {attempt}/{max_retries}). Retrying roleplay request...")
                        with self.app.tts.queue_lock:
                            g = next((x for x in self.app.tts.queue_groups if x["group_id"] == group_id), None)
                            if g:
                                g["text"] = "[Retrying Response...]"
                                self.app.tts.queue_condition.notify_all()
                        import time
                        time.sleep(1.0)
                        continue
                    _cleanup_failed_rp(f"\n[Roleplay Error] {str(e)}")
                    return

            if not resp or resp.status_code != 200:
                return

            try:
                result = resp.json()
                content = result['choices'][0]['message']['content']
                
                usage = result.get('usage', {})
                prompt_tokens = usage.get('prompt_tokens', 0)
                completion_tokens = usage.get('completion_tokens', 0)
                total_tokens = usage.get('total_tokens', 0)
                
                # Cache hit info is sometimes provided by DeepSeek
                cache_hits = usage.get('prompt_cache_hit_tokens', 0)
                cache_misses = prompt_tokens - cache_hits
                
                # Estimate cost (deepseek-chat current pricing approx):
                # Input cache hit: $0.014 / 1M, Input miss: $0.14 / 1M, Output: $0.28 / 1M
                est_cost = (cache_hits * 0.014 + cache_misses * 0.14 + completion_tokens * 0.28) / 1000000
                
                usage_msg = f"\n[LLM Usage] Model: {deepseek_model} | Prompt: {prompt_tokens} (Cache Hit: {cache_hits}), Completion: {completion_tokens}, Total: {total_tokens}, Est. Cost: ${est_cost:.6f}"
                self.app.after(0, self.app.append_system_log, usage_msg)
                
                # Clean up placeholder text
                with self.app.tts.queue_lock:
                    g = next((x for x in self.app.tts.queue_groups if x["group_id"] == group_id), None)
                    if g:
                        g["text"] = ""
                        g["is_pending_llm"] = False
                        g["is_processing_llm"] = False
                
                # Update UI in main thread
                self.app.after(0, self._handle_success, username, user_message, content, characters, group_id)
            except Exception as e:
                _cleanup_failed_rp(f"\n[Roleplay Error] Failed to process LLM response: {str(e)}")
                
        self.pending_llm_queue.append((group_id, _thread))
        self._check_llm_queue()

    def chunk_message(self, text):
        if hasattr(self.app, 'chunk_message'):
            return self.app.chunk_message(text)

        import re
        text = (text or "").strip()
        if not text:
            return []
        max_chars = int(self.app.settings.get("max_chars", 300)) if hasattr(self.app, "settings") else 300
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

    def _handle_success(self, username, user_message, content, characters, group_id):
        import time

        # We write original request to RP feed
        if hasattr(self.app, 'append_rp_user_message'):
            self.app.append_rp_user_message(username, user_message)
        else:
            self.app.rp_chat_box.insert("end", f"\n{username}: ", "user")
            self.app.rp_chat_box.insert("end", f"{user_message}\n")
            self.app.rp_chat_box.see("end")

        # Check if this group was previously timed out or removed from the active TTS Queue
        was_late = False
        with self.app.tts.queue_lock:
            g = next((x for x in self.app.tts.queue_groups if x["group_id"] == group_id), None)
            if not g or g.get("deleted", False) or g.get("timed_out_from_queue", False):
                was_late = True
                if g:
                    # Restore and un-delete the group in the TTS queue
                    g["deleted"] = False
                    g["user_cancelled"] = False
                    g["timed_out_from_queue"] = False
                    g["is_pending_llm"] = False
                    g["is_processing_llm"] = False
                    g["last_chunk_added"] = False
                    g["chunks_added"] = 0
                    g["chunks_played"] = 0
                    g["order"] = time.time()
                    g["priority"] = self.app.tts.get_category_priority("RP")
                    g["text"] = ""
                if getattr(self.app.tts, "skip_group_id", None) == group_id:
                    self.app.tts.skip_group_id = None
                self.app.tts.playback_chunks = [c for c in self.app.tts.playback_chunks if c.get("group_id") != group_id]
                self.app.tts.queue_condition.notify_all()

        if was_late:
            self.app.append_system_log(f"\n[Roleplay] Received late DeepSeek response for '{username}'. Adding back into TTS Queue...")

        items = []
        if len(characters) > 1:
            try:
                import json
                responses = json.loads(content)
                if isinstance(responses, dict) and 'responses' in responses:
                    responses = responses['responses']
                
                valid_items = [it for it in responses if isinstance(it, dict) and it.get('response', '').strip()]
                if not valid_items:
                    self.app.append_system_log(f"\n[Roleplay Warning] No valid response dialogue found in multi-character response.")
                    with self.app.tts.queue_lock:
                        g = next((x for x in self.app.tts.queue_groups if x["group_id"] == group_id), None)
                        if g:
                            g["deleted"] = True
                            g["is_pending_llm"] = False
                            g["is_processing_llm"] = False
                            g["last_chunk_added"] = True
                        self.app.tts.queue_condition.notify_all()
                    self._check_llm_queue()
                    return

                for item in valid_items:
                    raw_cmd = item.get('character', characters[0][0])
                    cmd = characters[0][0]
                    for c in characters:
                        if raw_cmd.lower() == c[0].lower():
                            cmd = c[0]
                            break
                    
                    resp_text = item.get('response', '')
                    items.append((cmd, resp_text))
            except Exception as e:
                self.app.append_system_log(f"\n[Roleplay Error] Failed to parse JSON response: {str(e)}\nRaw: {content}")
                with self.app.tts.queue_lock:
                    g = next((x for x in self.app.tts.queue_groups if x["group_id"] == group_id), None)
                    if g:
                        g["deleted"] = True
                        g["is_pending_llm"] = False
                        g["is_processing_llm"] = False
                        g["last_chunk_added"] = True
                    self.app.tts.queue_condition.notify_all()
                self._check_llm_queue()
                return
        else:
            items.append((characters[0][0], content))

        self._queue_conversation(items, group_id, username, user_message)

    def _queue_conversation(self, items, group_id, username="Unknown", user_message=""):
        import time
        import uuid
        import re

        with self.app.tts.queue_lock:
            g = next((x for x in self.app.tts.queue_groups if x["group_id"] == group_id), None)
            if g:
                g["deleted"] = False
                g["user_cancelled"] = False
                g["timed_out_from_queue"] = False
                g["is_pending_llm"] = False
                g["is_processing_llm"] = False
                g["order"] = time.time()
            if getattr(self.app.tts, "skip_group_id", None) == group_id:
                self.app.tts.skip_group_id = None
            self.app.tts.queue_condition.notify_all()

        prepared_items = []
        unique_run_id = uuid.uuid4().hex[:8]

        for item_idx, (command, text) in enumerate(items):
            # Write dialogue to RP feed and determine start line for context menu mapping
            if hasattr(self.app, 'append_rp_dialogue'):
                start_line, end_line = self.app.append_rp_dialogue(command, text)
            else:
                try:
                    start_line = int(self.app.rp_chat_box.index("end-1c").split(".")[0])
                except Exception:
                    start_line = 1
                self.app.rp_chat_box.insert("end", f"{command}: ", "command")
                self.app.rp_chat_box.insert("end", f"{text}\n")
                self.app.rp_chat_box.see("end")
                try:
                    end_line = int(self.app.rp_chat_box.index("end-1c").split(".")[0])
                except Exception:
                    end_line = start_line

            chunks = self.chunk_message(text)
            valid_chunks = []
            for c in chunks:
                c = c.strip()
                if re.search(r'[a-zA-Z0-9]', c):
                    valid_chunks.append(c)

            if not valid_chunks:
                self.app.append_system_log(f"\n[Roleplay Warning] No valid spoken dialogue generated for '{command}'. Skipping item.")
                continue

            if len(valid_chunks) > 1:
                max_c = int(self.app.settings.get("max_chars", 300)) if hasattr(self.app, "settings") else 300
                self.app.append_system_log(f"\n[Roleplay] Splitting long dialogue ({len(text)} chars, limit {max_c}) into {len(valid_chunks)} TTS segment(s) for '{command}'\n")

            audio_files = []
            for i in range(len(valid_chunks)):
                audio_filename = f"{group_id}_{unique_run_id}_{item_idx}_{i}.wav"
                audio_path = os.path.join(self.audio_dir, audio_filename)
                audio_files.append(audio_path)

            prepared_items.append({
                "group_id": group_id,
                "start_line": start_line,
                "end_line": end_line,
                "audio_files": audio_files,
                "command": command,
                "chunks": valid_chunks,
                "text": text,
                "user": username
            })

            # Store in history
            self.rp_history.append(prepared_items[-1])
            if len(self.rp_history) > 20:
                old_item = self.rp_history.pop(0)
                for fpath in old_item["audio_files"]:
                    try:
                        if os.path.exists(fpath):
                            os.remove(fpath)
                    except Exception:
                        pass

        total_chunks = sum(len(it["chunks"]) for it in prepared_items)
        if total_chunks == 0:
            with self.app.tts.queue_lock:
                g = next((x for x in self.app.tts.queue_groups if x["group_id"] == group_id), None)
                if g:
                    g["deleted"] = True
                    g["is_pending_llm"] = False
                    g["is_processing_llm"] = False
                    g["last_chunk_added"] = True
                self.app.tts.queue_condition.notify_all()
            self._check_llm_queue()
            return

        commands_list = [it["command"] for it in prepared_items]
        conversation_collector = {
            "commands": commands_list,
            "prompt": user_message,
            "user": username,
            "chunks_total": total_chunks,
            "chunks_received": 0,
            "audio_parts": [None] * total_chunks,
        }

        def _make_conversation_chunk_callback(global_idx, fpath, col):
            def _on_generated(audio_numpy):
                try:
                    import soundfile as sf
                    os.makedirs(os.path.dirname(os.path.abspath(fpath)), exist_ok=True)
                    sf.write(fpath, audio_numpy, 24000)
                except Exception:
                    pass

                col["audio_parts"][global_idx] = audio_numpy
                col["chunks_received"] += 1

                if col["chunks_received"] == col["chunks_total"]:
                    if hasattr(self.app, "save_roleplay_audio"):
                        self.app.save_roleplay_audio(
                            command=col["commands"],
                            text=col["prompt"],
                            user=col["user"],
                            audio_numpy_or_parts=col["audio_parts"]
                        )
            return _on_generated

        global_chunk_idx = 0
        for item in prepared_items:
            for i, chunk in enumerate(item["chunks"]):
                is_final_in_group = (global_chunk_idx == total_chunks - 1)
                is_contiguous = not is_final_in_group
                audio_path = item["audio_files"][i]

                def _log_callback(user, log_text, cmd=None, badges=None):
                    self.app.append_system_log(f"[{user}] {log_text}")

                translated_user = self.app.translate_username(username) if hasattr(self.app, "translate_username") else username
                chunk_tts = self.app.translate_usernames_in_text(chunk, self.app.get_active_chatters_list()) if hasattr(self.app, "translate_usernames_in_text") else chunk

                self.app.tts.generate_and_play(
                    item["command"],
                    chunk_tts,
                    _log_callback,
                    bypass_mute=False,
                    is_contiguous=is_contiguous,
                    group_id=group_id,
                    save_audio_path=None,
                    is_roleplay=True,
                    category="RP",
                    user=translated_user,
                    on_audio_generated=_make_conversation_chunk_callback(global_chunk_idx, audio_path, conversation_collector)
                )
                global_chunk_idx += 1

    def _queue_tts(self, command, text, group_id, username="Unknown", is_final_in_group=True):
        self._queue_conversation([(command, text)], group_id, username, text)

    def regenerate_audio_for_line(self, line_num):
        target_group_id = None
        for item in self.rp_history:
            if (item["start_line"] - 2) <= line_num <= item["end_line"]:
                target_group_id = item["group_id"]
                break
                
        if not target_group_id:
            self.app.append_system_log("\n[System] No history found for this line to regenerate.")
            return

        items_to_regenerate = [it for it in self.rp_history if it["group_id"] == target_group_id]
        total_items = len(items_to_regenerate)
        if total_items == 0:
            self.app.append_system_log("\n[System] No items found to regenerate audio.")
            return

        import time
        # Prepare the TTS queue: un-delete the group, reset chunk counters, clear stale playback chunks, update order timestamp
        with self.app.tts.queue_lock:
            self.app.tts.playback_chunks = [c for c in self.app.tts.playback_chunks if c.get("group_id") != target_group_id]
            
            g = next((x for x in self.app.tts.queue_groups if x["group_id"] == target_group_id), None)
            if g:
                g["deleted"] = False
                g["user_cancelled"] = False
                g["timed_out_from_queue"] = False
                g["is_pending_llm"] = False
                g["is_processing_llm"] = False
                g["last_chunk_added"] = False
                g["chunks_added"] = 0
                g["chunks_played"] = 0
                g["order"] = time.time()
                g["priority"] = self.app.tts.get_category_priority("RP")
                g["text"] = ""
            if getattr(self.app.tts, "skip_group_id", None) == target_group_id:
                self.app.tts.skip_group_id = None
            self.app.tts.queue_condition.notify_all()

        total_chunks = sum(len(it["chunks"]) for it in items_to_regenerate)
        if total_chunks == 0:
            self.app.append_system_log("\n[System] No chunks found to regenerate audio.")
            return

        first_item = items_to_regenerate[0]
        commands_list = [it["command"] for it in items_to_regenerate]
        conversation_collector = {
            "commands": commands_list,
            "prompt": first_item.get("text", ""),
            "user": first_item.get("user", "Unknown"),
            "chunks_total": total_chunks,
            "chunks_received": 0,
            "audio_parts": [None] * total_chunks,
        }

        def _make_regen_callback(global_idx, fpath, col):
            def _on_generated(audio_numpy):
                try:
                    import soundfile as sf
                    os.makedirs(os.path.dirname(os.path.abspath(fpath)), exist_ok=True)
                    sf.write(fpath, audio_numpy, 24000)
                except Exception:
                    pass

                col["audio_parts"][global_idx] = audio_numpy
                col["chunks_received"] += 1

                if col["chunks_received"] == col["chunks_total"]:
                    if hasattr(self.app, "save_roleplay_audio"):
                        self.app.save_roleplay_audio(
                            command=col["commands"],
                            text=col["prompt"],
                            user=col["user"],
                            audio_numpy_or_parts=col["audio_parts"]
                        )
            return _on_generated

        self.app.append_system_log(f"\n[Roleplay] Regenerating audio for dialogue. Queuing into TTS...")
        global_chunk_idx = 0
        for item in items_to_regenerate:
            for i, chunk in enumerate(item["chunks"]):
                is_last = (global_chunk_idx == total_chunks - 1)
                is_contiguous = not is_last
                audio_path = item["audio_files"][i]
                def _log_callback(user, log_text, cmd=None, badges=None):
                    self.app.append_system_log(f"[{user}] {log_text}")
                rp_user = self.app.translate_username(item.get("user", "Unknown")) if hasattr(self.app, "translate_username") else item.get("user", "Unknown")
                chunk_tts = self.app.translate_usernames_in_text(chunk, self.app.get_active_chatters_list()) if hasattr(self.app, "translate_usernames_in_text") else chunk
                self.app.tts.generate_and_play(
                    item["command"],
                    chunk_tts,
                    _log_callback,
                    bypass_mute=False,
                    is_contiguous=is_contiguous,
                    group_id=target_group_id,
                    save_audio_path=None,
                    is_roleplay=True,
                    category="RP",
                    user=rp_user,
                    on_audio_generated=_make_regen_callback(global_chunk_idx, audio_path, conversation_collector)
                )
                global_chunk_idx += 1

    def play_audio_for_line(self, line_num):
        target_group_id = None
        for item in self.rp_history:
            if (item["start_line"] - 2) <= line_num <= item["end_line"]:
                target_group_id = item["group_id"]
                break
                
        if not target_group_id:
            self.app.append_system_log("\n[System] No history found for this line to play.")
            return

        # Clear skip_group_id in case this group was previously skipped
        if getattr(self.app.tts, "skip_group_id", None) == target_group_id:
            self.app.tts.skip_group_id = None

        items_to_play = [it for it in self.rp_history if it["group_id"] == target_group_id]
        
        def _play_thread():
            import soundfile as sf
            import sounddevice as sd
            import threading
            import time
            for item in items_to_play:
                for fpath in item["audio_files"]:
                    if os.path.exists(fpath):
                        try:
                            data, fs = sf.read(fpath)
                            self.app.tts.current_playing_group_id = item["group_id"]
                            import numpy as np
                            audio_2d = np.ascontiguousarray(data.reshape(-1, 1) if len(data.shape) == 1 else data, dtype=np.float32)
                            current_idx = 0
                            stream_finished = threading.Event()

                            def callback(outdata, frames, time_info, status):
                                nonlocal current_idx
                                if getattr(self.app.tts, "is_paused", False):
                                    outdata.fill(0)
                                    return
                                if getattr(self.app.tts, "is_muted", False):
                                    outdata.fill(0)
                                    chunksize = min(len(audio_2d) - current_idx, frames)
                                    current_idx += chunksize
                                    if chunksize < frames:
                                        raise sd.CallbackStop()
                                    return
                                chunksize = min(len(audio_2d) - current_idx, frames)
                                outdata[:chunksize] = audio_2d[current_idx:current_idx + chunksize]
                                if chunksize < frames:
                                    outdata[chunksize:] = 0
                                    raise sd.CallbackStop()
                                current_idx += chunksize

                            with sd.OutputStream(samplerate=fs, channels=audio_2d.shape[1], callback=callback, finished_callback=stream_finished.set):
                                while not stream_finished.is_set():
                                    if getattr(self.app.tts, "skip_group_id", None) == item["group_id"]:
                                        break
                                    time.sleep(0.05)
                                    
                            self.app.tts.current_playing_group_id = None
                            
                            # Check if skipped
                            if getattr(self.app.tts, "skip_group_id", None) == item["group_id"]:
                                return
                        except Exception as e:
                            self.app.append_system_log(f"\n[System] Failed to play audio: {e}")
                    else:
                        self.app.append_system_log("\n[System] Audio file not found. Try regenerating.")
        
        threading.Thread(target=_play_thread, daemon=True).start()

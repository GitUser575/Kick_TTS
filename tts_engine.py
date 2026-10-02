import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning, module="diffusers")
warnings.filterwarnings("ignore", module="diffusers")
warnings.filterwarnings("ignore", message=".*LoRACompatibleLinear.*")

import time
import threading
import subprocess
import os
import queue

class TTSEngine:
    def __init__(self, on_model_status=None):
        self.ready = False
        self.is_muted = False
        self.on_audio_play_hook = None
        self.on_model_status = on_model_status
        self.has_neural_tts = False
        self.clone_model = None
        
        self.queue_lock = threading.Lock()
        self.queue_condition = threading.Condition(self.queue_lock)
        self.generation_queue = queue.PriorityQueue()
        self.playback_chunks = []
        self.queue_groups = []
        self.group_counter = 0
        self.task_counter = 0
        self.current_generating_group_id = None
        self.current_playing_group_id = None
        self.current_epoch = 0
        self.cached_conds = {}
        
        self.active_voices = {
            "!snoop": {"path": "snoop_sample.mp3", "temperature": 0.455},
            "!gamer": {"path": "gamer_sample.mp3", "temperature": 0.455},
            "!narrator": {"path": "narrator_sample.mp3", "temperature": 0.455},
            "!robot": {"path": "robot_sample.mp3", "temperature": 0.455}
        }
        
        # Start generation and playback queue worker threads
        threading.Thread(target=self._generation_worker, daemon=True).start()
        threading.Thread(target=self._playback_worker, daemon=True).start()

        # Asynchronously load neural voice cloning weights in background
        threading.Thread(target=self._load_neural_model_worker, daemon=True).start()

    def _load_neural_model_worker(self):
        def notify(text, progress, is_ready=False):
            if self.on_model_status:
                try:
                    self.on_model_status(text, progress, is_ready)
                except Exception:
                    pass

        notify("Loading AI Model into VRAM (10%)...", 0.10, False)
        stop_ticker = threading.Event()

        def _progress_ticker():
            pct = 15
            while not stop_ticker.is_set() and pct < 90:
                time.sleep(0.35)
                if stop_ticker.is_set():
                    break
                pct += 5 if pct < 50 else (3 if pct < 80 else 1)
                notify(f"Loading AI Model into VRAM ({pct}%)...", pct / 100.0, False)

        ticker = threading.Thread(target=_progress_ticker, daemon=True)
        ticker.start()

        try:
            import os
            import torch
            torch.set_grad_enabled(False) # Globally disable gradient calculation to save VRAM
            torch_threads = int(os.environ.get("TORCH_NUM_THREADS", 4))
            torch.set_num_threads(torch_threads) # Restrict CPU thread usage
            device = "cuda" if torch.cuda.is_available() else "cpu"
            
            from chatterbox.tts_turbo import ChatterboxTurboTTS
            print("[System] Loading Neural Voice Cloning Model (Chatterbox Turbo). This may take a moment...")
            self.clone_model = ChatterboxTurboTTS.from_pretrained(device=device)
            
            self.has_neural_tts = True
            print(f"[System] Neural Voice Cloning Loaded successfully on {device.upper()}!")
        except Exception as e:
            print(f"[Warning] Neural Voice disabled (error or missing): {e}. Falling back to basic synthesis.")
            self.has_neural_tts = False
        finally:
            stop_ticker.set()
            self.ready = True
            notify("AI Model Loaded into VRAM (100%)", 1.0, True)
        


    def add_voice(self, command, filepath, temperature=0.5):
        self.active_voices[command] = {
            "path": filepath,
            "temperature": float(temperature),
        }
        
    def get_category_priority(self, category):
        """
        Determines queue priority for TTS messages and audio clips.
        Priority 0 (Highest): Raid alerts, subscription-related alerts, kicks dono alerts
        Priority 1 (Next highest): Voice Mapping and Ad messages
        Priority 2 (Standard): Roleplay, standard chat TTS, KickBot, and others
        """
        cat_upper = str(category or "").strip().upper()
        if cat_upper in (
            "RAID", "RAID_ALERT", "RAIDS",
            "SUB", "RESUB", "GIFTSUB", "GIFTEDSUB", "GIFTED_SUB", "GIFTED_SUBS", "GIFTSUBS", "GIFTEDSUBS",
            "SUBSCRIPTION", "SUBSCRIPTIONS", "SUB_CELEBRATION", "SUB_CELEBRATIONS", "SUBCELEBRATION", "SUB_RENEWAL", "SUBRENEWAL",
            "DONO", "DONATION", "KICKS"
        ):
            return 0
        elif cat_upper in ("VM", "VOICE_MAPPING", "AD", "ADBOT", "ADS"):
            return 1
        elif cat_upper in ("RP", "STANDARD", "KICKBOT"):
            return 2
        else:
            return 2

    def play_audio_file(self, file_path, command="Alert", category="DONO", bypass_mute=False, text="Alert Sound", user="System"):
        if user and isinstance(user, str):
            import re
            s = re.sub(r'\d+$', '', user)
            if s:
                c = s.rstrip('_-')
                user = c if c else s
            user = user.replace('0', 'o').replace('3', 'e')
        if hasattr(self, "on_audio_play_hook") and self.on_audio_play_hook:
            try:
                self.on_audio_play_hook()
            except Exception:
                pass
        import uuid
        import time
        import os
        
        is_mp3 = file_path.lower().endswith(".mp3")
        duration = 3.0
        data = None
        fs = None
        
        if not is_mp3:
            import soundfile as sf
            try:
                data, fs = sf.read(file_path)
                duration = len(data) / float(fs)
            except Exception as e:
                pass
        else:
            try:
                file_size = os.path.getsize(file_path)
                duration = max(1.0, round(file_size / 16000.0, 1))
            except Exception:
                duration = 3.0

        group_id = str(uuid.uuid4())
        
        with self.queue_lock:
            self.group_counter += 1
            priority = self.get_category_priority(category)
            
            group = {
                "group_id": group_id,
                "commands": [command],
                "text": text,
                "category": category,
                "user": user,
                "generated_text_length": len(text),
                "total_audio_duration": duration,
                "played_audio_duration": 0.0,
                "current_playing_start_time": None,
                "current_playing_duration": 0.0,
                "priority": priority,
                "order": time.time(),
                "chunks_added": 1,
                "chunks_played": 0,
                "last_chunk_added": True
            }
            self.queue_groups.append(group)
            
            if data is not None and fs is not None:
                chunk = {
                    'type': 'audio_file',
                    'audio_numpy': data,
                    'samplerate': fs,
                    'command': command,
                    'callback_log': lambda sender, msg, param: None,
                    'bypass_mute': bypass_mute,
                    'is_contiguous': False,
                    'group_id': group_id,
                    'epoch': self.current_epoch,
                    'duration': duration
                }
            else:
                chunk = {
                    'type': 'subprocess_audio',
                    'file_path': file_path,
                    'command': command,
                    'callback_log': lambda sender, msg, param: None,
                    'bypass_mute': bypass_mute,
                    'is_contiguous': False,
                    'group_id': group_id,
                    'epoch': self.current_epoch,
                    'duration': duration
                }
            self.playback_chunks.append(chunk)
            self.queue_condition.notify_all()

    @staticmethod
    def get_dynamic_duration_limit(word_count):
        wc = max(0, int(word_count or 0))
        # Dynamic duration limit with +30% expanded tolerance (1.75 * 1.30 = 2.275x tolerance multiplier)
        # Allows naturally slower speaking voices ample time without being cut off early (e.g. 68 words: ((68 / 2.0) + 4.0) * 2.275 = ~86.5s)
        return max(6.5, ((wc / 2.0) + 4.0) * 2.275)

    def generate_and_play(self, command, text, callback_log, bypass_mute=False, is_contiguous=False, group_id=None, save_audio_path=None, is_roleplay=False, category="Standard", user="Unknown", save_only=False, on_audio_generated=None):
        if user and isinstance(user, str):
            import re
            s = re.sub(r'\d+$', '', user)
            if s:
                c = s.rstrip('_-')
                user = c if c else s
            user = user.replace('0', 'o').replace('3', 'e')
        if group_id is None:
            import uuid
            group_id = str(uuid.uuid4())

        # Add to queue_groups if not exists
        with self.queue_lock:
            group = next((g for g in getattr(self, "queue_groups", []) if g["group_id"] == group_id), None)
            if not group:
                self.group_counter += 1
                priority = self.get_category_priority(category)
                    
                import time
                group = {
                    "group_id": group_id,
                    "commands": [command],
                    "text": text,
                    "category": category,
                    "user": user,
                    "generated_text_length": 0,
                    "total_audio_duration": 0.0,
                    "played_audio_duration": 0.0,
                    "current_playing_start_time": None,
                    "current_playing_duration": 0.0,
                    "priority": priority,
                    "order": time.time(),
                    "chunks_added": 0,
                    "chunks_played": 0,
                    "last_chunk_added": False
                }
                self.queue_groups.append(group)
            else:
                if command not in group.get("commands", []):
                    group.setdefault("commands", []).append(command)
                group["text"] += " " + text
                # If this group was previously marked deleted or completed (e.g. from watchdog timeout or prior playback),
                # un-delete and re-enable it so tasks are synthesized and played
                if group.get("deleted", False):
                    group["deleted"] = False
                    group["user_cancelled"] = False
                    group["timed_out_from_queue"] = False
                    group["last_chunk_added"] = False
            
            if getattr(self, "skip_group_id", None) == group_id:
                self.skip_group_id = None

            group["chunks_added"] += 1
            if not is_contiguous:
                group["last_chunk_added"] = True

        def drop_chunk():
            with self.queue_lock:
                g = next((x for x in getattr(self, "queue_groups", []) if x["group_id"] == group_id), None)
                if g:
                    g["chunks_played"] = g.get("chunks_played", 0) + 1
                    self.queue_condition.notify_all()

        if command not in self.active_voices:
            callback_log("System", f"[TTS Debug] Command '{command}' rejected! Active voices are: {list(self.active_voices.keys())}", None)
            drop_chunk()
            return
            
        def _run():
            if getattr(self, "skip_group_id", None) == group_id and group_id is not None:
                drop_chunk()
                return # Skip generation if this group was already skipped
            
            callback_log("System", f"[TTS] Generating processing for {command}...", None)
            
            # 1. Strip the command robustly (case-insensitive, matched as a full word if possible)
            import re
            
            def _normalize_speech_text(raw_input):
                t = raw_input
                
                # Translate @mentions (usernames) by removing trailing numbers and converting 0 to o and 3 to e
                def _clean_mention(m):
                    uname = m.group(1)
                    s = re.sub(r'\d+$', '', uname)
                    if s:
                        c = s.rstrip('_-')
                        uname = c if c else s
                    return '@' + uname.replace('0', 'o').replace('3', 'e')

                t = re.sub(r'@([a-zA-Z0-9_\-]+)', _clean_mention, t)

                # Replace underscores with spaces so e.g. "Mr_TTS" becomes "Mr TTS"
                t = t.replace('_', ' ')
                
                # 0. Check if most of the entire message is uppercase / all-caps
                alpha_chars = [c for c in t if c.isalpha()]
                total_alpha = len(alpha_chars)
                upper_count = sum(1 for c in alpha_chars if c.isupper()) if total_alpha else 0
                is_mostly_uppercase = (total_alpha >= 4 and (upper_count / total_alpha >= 0.50))

                # If "US" or "USA" is in the message and most of the entire message isn't uppercase,
                # convert to "U-S" or "U-S-A" so they are pronounced as individual letters and not "us" or "usa"
                if not is_mostly_uppercase:
                    t = re.sub(r'\bUS\b', 'U-S', t)
                    t = re.sub(r'\bUSA\b', 'U-S-A', t)
                    t = re.sub(r'(?i)\bu\.s\.a\.?(?!\w)', 'U-S-A', t)
                    t = re.sub(r'(?i)\bu\.s\.?(?!\w)', 'U-S', t)
                    t = re.sub(r'(?i)\busa\b', 'U-S-A', t)
                else:
                    t = re.sub(r'(?i)\bu\.s\.a\.?(?!\w)', 'U-S-A', t)
                    t = re.sub(r'(?i)\bu\.s\.?(?!\w)', 'U-S', t)
                    t = t.lower()

                # Convert several uppercase words in a row (2 or more) to lowercase (preserve hyphenated acronyms like U-S and U-S-A)
                t = re.sub(r'\b[A-Z]{2,}(?:[\s_]+[A-Z]{2,})+\b', lambda m: m.group(0).lower(), t)
                t = re.sub(r'\b[A-Z]+(?:[\s_]+[A-Z]+){2,}\b', lambda m: m.group(0).lower(), t)

                # Normalize numbers with "k" like "$2.5k" or "2.5k" to spoken text: "two point five k"
                _DIGIT_WORDS = {
                    0: "zero", 1: "one", 2: "two", 3: "three", 4: "four",
                    5: "five", 6: "six", 7: "seven", 8: "eight", 9: "nine"
                }
                _TEENS = {
                    10: "ten", 11: "eleven", 12: "twelve", 13: "thirteen", 14: "fourteen",
                    15: "fifteen", 16: "sixteen", 17: "seventeen", 18: "eighteen", 19: "nineteen"
                }
                _TENS = {
                    20: "twenty", 30: "thirty", 40: "forty", 50: "fifty",
                    60: "sixty", 70: "seventy", 80: "eighty", 90: "ninety"
                }

                def _int_to_words(n):
                    if n < 10:
                        return _DIGIT_WORDS[n]
                    if n < 20:
                        return _TEENS[n]
                    if n < 100:
                        ten, rem = divmod(n, 10)
                        return _TENS[ten * 10] + (" " + _DIGIT_WORDS[rem] if rem else "")
                    if n < 1000:
                        hundred, rem = divmod(n, 100)
                        return _DIGIT_WORDS[hundred] + " hundred" + (" " + _int_to_words(rem) if rem else "")
                    if n < 1000000:
                        thousand, rem = divmod(n, 1000)
                        return _int_to_words(thousand) + " thousand" + (" " + _int_to_words(rem) if rem else "")
                    return str(n)

                def _k_replacer(m):
                    num_str = m.group(1)
                    if "." in num_str:
                        int_str, dec_str = num_str.split(".", 1)
                        int_val = int(int_str) if int_str.isdigit() else 0
                        int_word = _int_to_words(int_val)
                        dec_words = " ".join(_DIGIT_WORDS.get(int(d), d) for d in dec_str if d.isdigit())
                        return f"{int_word} point {dec_words} k"
                    else:
                        int_val = int(num_str) if num_str.isdigit() else 0
                        return f"{_int_to_words(int_val)} k"

                t = re.sub(r'\$?(\d+(?:\.\d+)?)\s*[kK]\b', _k_replacer, t)

                # Currency expansion
                t = re.sub(r'\$(\d+)\.(\d{2})\b', r'\1 dollars and \2 cents', t)
                t = re.sub(r'\$(\d+)\.(\d)\b', r'\1 dollars and \g<2>0 cents', t)
                t = re.sub(r'\$(\d+)\b', r'\1 dollars', t)
                
                # Decimal points in numbers
                t = re.sub(r'(\d+)\.(\d+)', r'\1 point \2', t)
                
                # Acronyms with periods
                t = re.sub(r'(?i)\bu\.s\.a\.?\b', 'U-S-A', t)
                t = re.sub(r'(?i)\bu\.s\.?\b', 'U-S', t)
                t = re.sub(r'(?i)\be\.g\.?\b', 'for example', t)
                t = re.sub(r'(?i)\bi\.e\.?\b', 'that is', t)
                t = re.sub(r'(?i)\betc\.?\b', 'etcetera', t)
                t = re.sub(r'(?i)\bvs\.?\b', 'versus', t)
                t = re.sub(r'(?i)\bapprox\.?\b', 'approximately', t)
                t = re.sub(r'(?i)\bavg\.?\b', 'average', t)
                
                # Common honorifics and titles
                t = re.sub(r'(?i)\bdr\.\s*', 'Doctor ', t)
                t = re.sub(r'(?i)\bmr\.\s*', 'Mister ', t)
                t = re.sub(r'(?i)\bmrs\.\s*', 'Missus ', t)
                t = re.sub(r'(?i)\bms\.\s*', 'Miz ', t)
                t = re.sub(r'(?i)\bprof\.\s*', 'Professor ', t)
                t = re.sub(r'(?i)\bcapt\.\s*', 'Captain ', t)
                t = re.sub(r'(?i)\bgen\.\s*', 'General ', t)
                t = re.sub(r'(?i)\blt\.\s*', 'Lieutenant ', t)
                t = re.sub(r'(?i)\bsr\.\s*', 'Senior ', t)
                t = re.sub(r'(?i)\bjr\.\s*', 'Junior ', t)
                t = re.sub(r'(?i)\bst\.\s*', 'Saint ', t)
                
                # Slang / Gaming / Streamer acronyms (hyphenate for crisp multi-letter pronunciation)
                t = re.sub(r'(?i)\bb[\s\.\-]*w[\s\.\-]*c\b\.?', 'B-W-C', t)
                t = re.sub(r'(?i)\bb[\s\.\-]*b[\s\.\-]*c\b\.?', 'bee-bee-see', t)
                t = re.sub(r'(?i)\ba[\s\.\-]*o[\s\.\-]*c\b\.?', 'A-O see', t)
                
                # Only hyphenate isolated true acronyms, converting common words to lowercase
                common_words = {
                    "THE", "AND", "FOR", "ARE", "YOU", "NOT", "CAN", "ALL", "GET", "WAS", "OUT", "SEE",
                    "NOW", "DAY", "WAY", "NEW", "ONE", "TWO", "HER", "HIS", "HIM", "SHE", "WHO", "WHY",
                    "HOW", "WHAT", "WHEN", "WITH", "THIS", "THAT", "FROM", "THEY", "HAVE", "SOME", "MORE",
                    "BEEN", "GOOD", "MUCH", "TIME", "JUST", "KNOW", "TAKE", "COME", "LOOK", "ONLY", "THEN",
                    "ALSO", "BACK", "EVEN", "WELL", "MAKE", "GIVE", "VERY", "STILL", "OVER", "INTO", "LAST",
                    "SAID", "LIKE", "WILL", "YOUR", "THEM", "WANT", "HERE", "WERE", "DOES", "WENT", "MANY",
                    "THAN", "FIND", "DOWN", "LONG", "MADE", "SAY", "USE", "MAN", "MEN", "BOY", "BAD", "BIG",
                    "TOP", "HOT", "FAR", "RUN", "OFF", "PUT", "SET", "END", "LET", "SIT", "ASK", "WIN", "YES",
                    "NO", "MAY", "TRY", "FEW", "BUY", "PAY", "DOG", "CAT", "CAR", "BUS", "JOB", "SUN", "BED",
                    "CUP", "BOX", "BAG", "HAT", "PEN", "PIG", "COW", "EAT", "FLY", "HIT", "FIT", "CUT", "BIT",
                    "BET", "RED", "OLD", "LOW", "WAR", "AIR", "OIL", "ART", "LAW", "GOD", "ICE", "LOVE", "HATE",
                    "FOOD", "GAME", "CHAT", "COOL", "NICE", "STOP", "HELP", "WAIT", "FAST", "SLOW", "HARD", "EASY",
                    "PLAY", "REAL", "FAKE", "TRUE", "FREE", "FULL", "DONE", "SICK", "LATE", "SOON", "SAFE", "TEAM",
                    "SHOW", "FIRE", "HOME", "LIFE", "HEAD", "HAND", "EYE", "FACE", "ROOM", "DOOR", "ROAD", "CITY",
                    "TOWN", "BODY", "MIND", "SON", "DAD", "MOM", "BRO", "SIS", "KID", "GUY", "PAL", "DOC", "FAN",
                    "PET", "BOT", "MOD", "SUB", "PUB", "BAR", "GYM", "SPA", "LAB", "GAS", "TAX", "FEE", "CASH",
                    "CARD", "COIN", "BANK", "SHOP", "MALL", "SALE", "DEAL", "COST", "PRICE", "RICH", "POOR", "LOSS",
                    "TIE", "DRAW", "BEAT", "KICK", "PUNCH", "HURT", "HEAL", "CURE", "REST", "SLEEP", "WALK", "JUMP",
                    "FALL", "DROP", "PICK", "HOLD", "GRAB", "PUSH", "PULL", "OPEN", "SHUT", "LOCK", "TURN", "MOVE",
                    "STAY", "LEAVE", "WATCH", "HEAR", "TALK", "SPEAK", "TELL", "BEG", "CALL", "CRY", "SMILE", "FEEL",
                    "CARE", "HOPE", "WISH", "FEAR", "NEED", "MUST", "SHALL", "COULD", "WOULD", "HAS", "HAD", "DID",
                    "GOT", "TOOK", "GAVE", "FOUND", "KEPT", "SEEK", "HIDE", "MEET", "MET", "JOIN", "PASS", "FAIL",
                    "TEST", "PLAN", "IDEA", "FACT", "LIE", "NEWS", "POST", "READ", "SING", "SONG", "DANCE", "ACT",
                    "FILM", "BOOK", "PAGE", "WORD", "NAME", "SIGN", "MARK", "NOTE", "LIST", "FORM", "LINE", "SIDE",
                    "HALF", "BASE", "CORE", "PEAK", "EDGE", "ZONE", "AREA", "SITE", "SPOT", "HOUR", "MIN", "SEC",
                    "YEAR", "WEEK", "NOON", "DAWN", "DUSK", "PAST", "NEXT", "EVER", "ONCE", "LESS", "MOST", "NONE",
                    "BOTH", "EACH", "SAME", "SUCH", "THUS", "TOO", "QUITE", "BEST", "GREAT", "EVIL", "HUGE", "TINY",
                    "TALL", "WIDE", "DEEP", "HIGH", "DARK", "DIM", "WARM", "COLD", "DRY", "WET", "PURE", "RAW",
                    "SOFT", "WEAK", "CALM", "WILD", "KIND", "MEAN", "RUDE", "SALT", "FAT", "SLIM", "THIN", "BORN",
                    "RISK", "LOST", "SURE", "WISE", "FOOL", "DUMB", "BUSY", "IDLE", "LOUD", "MUTE", "DEAF", "KEEN",
                    "DULL", "FLAT", "BENT", "FINE", "BARE", "NEAR", "AWAY", "THEIR", "OURS", "MINE", "ANY"
                }
                known_acronyms = {
                    "EBT", "VIP", "MVP", "BRB", "AFK", "POV", "WIP", "NPC", "FPS", "RPG", "GG", "EZ", "BTW", "TBA",
                    "TBD", "FYI", "FAQ", "DM", "PM", "IRL", "TBH", "IDK", "OMG", "SMH", "RN", "FR", "NGL",
                    "IMO", "IMHO", "IKR", "ROFL", "LMFAO", "LMAO", "LOL", "FTW", "AKA", "ETA", "ASAP", "DIY",
                    "CEO", "CFO", "CTO", "FBI", "CIA", "NASA", "USA", "US", "UK", "EU", "UN", "GPS", "SMS", "URL",
                    "IP", "PC", "TV", "DVD", "CD", "USB", "AI", "VR", "AR", "UI", "UX", "OS", "RAM", "CPU",
                    "GPU", "HDD", "SSD", "LED", "LCD", "RGB", "HD", "HQ", "GIF", "PNG", "JPG", "MP3", "MP4",
                    "PDF", "IQ", "EQ", "ATM", "EMT", "DMV", "PTSD", "ADHD", "STD", "HIV", "CPR", "ICU", "LLC",
                    "INC", "CPA", "IRS", "TSA", "DOJ", "DOD", "ATF", "EPA", "FDA", "USDA", "FAA", "FCC", "SEC",
                    "FTC", "DEA", "NSA", "CDC", "GOP", "DNC", "PAC", "MTG", "AOC", "BWC", "BBC", "GOAT", "OG",
                    "XD", "UFC", "WWE", "NBA", "NFL", "MLB", "NHL", "KFC", "BLT", "OLED", "HDMI", "VGA", "PSU",
                    "UPS", "LAN", "WAN", "VPN", "DNS", "TCP", "UDP", "HTML", "CSS", "PHP", "SQL", "API", "SDK",
                    "CLI", "GUI", "DOC", "TXT", "CSV", "WAV", "SVG", "NFT", "BTC", "ETH", "SOL", "DEX", "CEX",
                    "POS", "PIN", "OTP", "SSN", "DOB", "ETD", "TBC", "TTYL", "GTG", "IDC", "WTF", "STFU", "DOT",
                    "AOE", "PVP", "PVE", "XP", "HP", "MP", "FWIW", "TLDR", "WTH", "OMW", "ASL", "OOTD", "NSFW",
                    "SFW", "RIP", "SOS", "RSVP", "BLVD", "AVE", "APT", "EST", "PST", "CST", "MST", "UTC", "GMT",
                    "AC", "DC"
                }

                def _normalize_isolated_acronym(m):
                    word = m.group(0)
                    if is_mostly_uppercase:
                        return word.lower()
                    if word in common_words and word not in known_acronyms:
                        return word.lower()
                    if word == "US":
                        return "U-S"
                    # Hyphenate uppercase acronyms/initialisms (e.g. EBT -> E-B-T, ATM -> A-T-M)
                    return '-'.join(list(word))

                t = re.sub(r'\b[A-Z]{2,4}\b', _normalize_isolated_acronym, t)

                # Reduce 4 or more consecutive periods to 3 periods (single '.', double '..', and triple '...' are preserved)
                t = re.sub(r'\.{4,}', '...', t)

                return t

            if is_roleplay:
                clean_text = _normalize_speech_text(text)
                clean_text = re.sub(r'\b[a-z](?:-[a-z])+\b', lambda m: m.group(0).upper(), clean_text)
                clean_text = re.sub(r'\bw\b', 'W', clean_text)
            else:
                # Strip zero width spaces just in case
                clean_text = re.sub(r'[\u200b\u200c\u200d\uFEFF]', '', text).strip()
                
                # Remove the command from the very beginning of the string ONLY (with optional trailing space)
                pattern = r'^' + re.escape(command) + r'\b\s*'
                clean_text = re.sub(pattern, '', clean_text, flags=re.IGNORECASE).strip()
                
                # If the command didn't get stripped because it wasn't at the start (due to weird spaces), do a fallback replace
                if clean_text.lower().startswith(command):
                    clean_text = clean_text[len(command):].strip()
                    
                # Filter out URLs to prevent them from being spoken
                clean_text = re.sub(r'(https?://|www\.)\S+', '', clean_text).strip()
                
                # Strip any accidental username: prefix the DOM might have leaked during scraping
                clean_text = re.sub(r'^[^:\s]{1,30}:\s*', '', clean_text).strip()
                
                # Apply intelligent acronym, abbreviation, and currency normalization
                clean_text = _normalize_speech_text(clean_text)
                
                # Sanitize double quotes to prevent injection
                clean_text = clean_text.replace('"', '')
                
                # Reduce multiple single quotes to a single quote
                clean_text = re.sub(r"'+", "'", clean_text)
                
                # Convert special symbols to words
                clean_text = clean_text.replace('&', ' and ')
                clean_text = clean_text.replace('@', ' at ')
                clean_text = clean_text.replace('#', ' hashtag ')
                clean_text = clean_text.replace('%', ' percent ')
                
                # Replace anything that is not an alphabet character (a-z), number (0-9), exclamation mark (!), square brackets ([]), question mark (?), dollar sign ($), single quote ('), period (.), or hyphen (-) with a space
                clean_text = re.sub(r"[^a-zA-Z0-9 !\[\]\?\$'\.\-]", ' ', clean_text)
                
                # Reduce 4 or more periods in a row down to 3 periods (single '.', double '..', and triple '...' are preserved)
                clean_text = re.sub(r'\.{4,}', '...', clean_text)

                # Reduce multiple exclamation marks to a single one
                clean_text = re.sub(r'!+', '!', clean_text)
                
                # Reduce multiple square brackets to a single one
                clean_text = re.sub(r'\[+', '[', clean_text)
                clean_text = re.sub(r'\]+', ']', clean_text)
                
                # Reduce multiple spaces to a single space
                clean_text = re.sub(r' +', ' ', clean_text)
                
                # Record isolated capital letters before lowercasing so their letter-name pronunciation is preserved (e.g. 'W', 'L')
                isolated_caps = set(re.findall(r'\b[A-Z]\b', clean_text))
                
                # Convert text to lowercase
                clean_text = clean_text.lower()
                
                # Reduce 4 or more identical letters in a row to a single letter
                clean_text = re.sub(r'([a-z])\1{3,}', r'\1', clean_text)
                
                # Restore hyphenated acronyms/initialisms to uppercase (e.g. e-b-t -> E-B-T, u-s-a -> U-S-A, a-t-m -> A-T-M)
                clean_text = re.sub(r'\b[a-z](?:-[a-z])+\b', lambda m: m.group(0).upper(), clean_text)
                
                # Restore isolated capital letters that were originally uppercase (except standard lowercase English words 'a' and 'i')
                for cap in isolated_caps:
                    if cap not in ('A', 'I'):
                        clean_text = re.sub(r'\b' + cap.lower() + r'\b', cap, clean_text)
                
                # Ensure standalone 'w' is always capitalized to 'W' so the TTS engine pronounces it as saying the letter ("double-u")
                clean_text = re.sub(r'\bw\b', 'W', clean_text)
                
            if not clean_text.strip() or len(clean_text.strip()) < 2:
                callback_log("System", f"[TTS] Text too short to synthesize for {command}.", None)
                drop_chunk()
                return

            callback_log("System", f"[TTS] Added {command} to processing queue...", None)
            words = clean_text.strip().split()
            word_count = len(words)
            task = {
                'command': command,
                'clean_text': clean_text,
                'word_count': word_count,
                'callback_log': callback_log,
                'bypass_mute': bypass_mute,
                'is_contiguous': is_contiguous,
                'group_id': group_id,
                'save_audio_path': save_audio_path,
                'save_only': save_only,
                'on_audio_generated': on_audio_generated,
                'epoch': self.current_epoch
            }
            with self.queue_lock:
                self.task_counter += 1
                self.generation_queue.put((group["priority"], group["order"], self.task_counter, task))
                self.queue_condition.notify_all()

        _run()

    
    def skip_all(self):
        try:
            # Increment epoch to discard anything currently generating or queued
            self.current_epoch += 1
            
            # Clear generation queue
            with self.generation_queue.mutex:
                self.generation_queue.queue.clear()
                
            with self.queue_lock:
                for g in self.queue_groups:
                    g["deleted"] = True
                    g["user_cancelled"] = True
                self.playback_chunks = []
                self.queue_condition.notify_all()
                
            # If fallback process is running, kill it
            if hasattr(self, "current_ps_process") and self.current_ps_process:
                try:
                    self.current_ps_process.kill()
                except Exception:
                    pass
                    
            # Stop MCI audio if active
            if hasattr(self, "current_mci_alias") and self.current_mci_alias:
                try:
                    import ctypes
                    ctypes.windll.winmm.mciSendStringW(f'stop {self.current_mci_alias}', None, 0, None)
                    ctypes.windll.winmm.mciSendStringW(f'close {self.current_mci_alias}', None, 0, None)
                except Exception:
                    pass
                self.current_mci_alias = None
                    
            # Also mark current group as skipped just in case
            if hasattr(self, 'current_playing_group_id') and self.current_playing_group_id is not None:
                self.skip_group_id = self.current_playing_group_id
            else:
                self.skip_group_id = getattr(self, 'last_added_group_id', None)
        except Exception as e:
            print(f"[TTS Skip All Error] {e}")

    def skip_current(self):
        try:
            # Mark the current group as skipped
            if hasattr(self, 'current_playing_group_id') and self.current_playing_group_id is not None:
                self.skip_group_id = self.current_playing_group_id
            elif hasattr(self, 'current_generating_group_id') and self.current_generating_group_id is not None:
                self.skip_group_id = self.current_generating_group_id
            else:
                self.skip_group_id = getattr(self, 'last_added_group_id', None)
                
            if self.skip_group_id:
                with self.queue_lock:
                    for g in getattr(self, "queue_groups", []):
                        if g["group_id"] == self.skip_group_id:
                            g["user_cancelled"] = True
                            break
                    self.queue_condition.notify_all()

            # If fallback process is running, kill it
            if hasattr(self, "current_ps_process") and self.current_ps_process:
                try:
                    self.current_ps_process.kill()
                except Exception:
                    pass

            # Stop MCI audio if active
            alias = getattr(self, "current_mci_alias", None)
            if alias:
                self.current_mci_alias = None
                try:
                    import ctypes
                    ctypes.windll.winmm.mciSendStringW(f'stop {alias}', None, 0, None)
                    ctypes.windll.winmm.mciSendStringW(f'close {alias}', None, 0, None)
                except Exception:
                    pass
        except Exception as e:
            print(f"[TTS Skip Current Error] {e}")

    def delete_from_queue(self, group_id):
        with self.queue_lock:
            for g in getattr(self, "queue_groups", []):
                if g["group_id"] == group_id:
                    g["deleted"] = True
                    g["user_cancelled"] = True
                    break
                    
            if getattr(self, "current_playing_group_id", None) == group_id:
                self.skip_current()
                
            self.queue_condition.notify_all()

    def move_to_top(self, group_id):
        with self.queue_lock:
            for g in getattr(self, "queue_groups", []):
                if g["group_id"] == group_id:
                    g["priority"] = -1
                    break
            self.queue_condition.notify_all()

    def set_paused(self, is_paused):
        with self.queue_lock:
            self.is_paused = is_paused
            import time
            group = next((g for g in getattr(self, "queue_groups", []) if g["group_id"] == getattr(self, "current_playing_group_id", None)), None)
            if group:
                if is_paused:
                    group["pause_start_time"] = time.time()
                else:
                    if "pause_start_time" in group:
                        group["current_playing_start_time"] += (time.time() - group["pause_start_time"])
                        del group["pause_start_time"]

    def _generation_worker(self):
        while True:
            t = self.generation_queue.get()
            if t is None: break
            if isinstance(t, tuple) and len(t) == 4:
                _, _, _, task = t
            elif isinstance(t, dict):
                task = t
            else:
                self.generation_queue.task_done()
                continue
            
            import time
            group_id = task.get('group_id')
            
            with self.queue_lock:
                group = next((g for g in getattr(self, "queue_groups", []) if g["group_id"] == group_id), None)
                if group and group.get("deleted", False):
                    self.generation_queue.task_done()
                    continue
                    
            self.current_generating_group_id = group_id
            self.current_generating_start_time = time.time()
            command = task['command']
            clean_text = task['clean_text']
            callback_log = task['callback_log']
            chunk_text_length = len(clean_text)
            words = clean_text.strip().split()
            word_count = task.get('word_count') or len(words)
            max_allowed_duration = self.get_dynamic_duration_limit(word_count)
            
            try:
                # If model is still loading into VRAM in the background, wait for it to finish
                while not getattr(self, "ready", False):
                    time.sleep(0.1)

                if self.has_neural_tts:
                    voice_settings = self.active_voices[command]
                    sample_mp3 = voice_settings["path"]
                    temperature = voice_settings.get("temperature", 0.4)
                    
                    if not os.path.exists(sample_mp3):
                        callback_log("System", f"[TTS Error] Missing voice sample file: {sample_mp3} - Click 'Add New Voice Sample' to map it!", None)
                    else:
                        callback_log("System", f"[TTS] Cloning voice from {os.path.basename(sample_mp3)}...", None)
                        
                        import torch
                        with torch.no_grad():
                            kwargs = {'temperature': temperature}
                            kwargs['norm_loudness'] = True

                            cache_key = sample_mp3
                            if hasattr(self.clone_model, 'prepare_conditionals'):
                                if cache_key not in self.cached_conds:
                                    callback_log("System", f"[TTS] Preparing conditionals for voice...", None)
                                    self.clone_model.prepare_conditionals(sample_mp3)
                                    self.cached_conds[cache_key] = self.clone_model.conds
                                else:
                                    self.clone_model.conds = self.cached_conds[cache_key]

                            audio_tensor = self.clone_model.generate(
                                text=clean_text,
                                **kwargs
                            )
                            
                        audio_numpy = audio_tensor.squeeze().cpu().numpy()
                        
                        # Aggressive VRAM cleanup: immediately release scratchpad VRAM on every chunk
                        del audio_tensor
                        if torch.cuda.is_available():
                            torch.cuda.empty_cache()
                        
                        import numpy as np
                        raw_duration = len(audio_numpy) / 24000.0

                        # Checkpoint A: Audio File Inspection
                        # Trim the audio to the dynamic limit instead of skipping the entire audio
                        if raw_duration > max_allowed_duration:
                            callback_log("System", f"[TTS] Audio duration ({raw_duration:.1f}s) exceeded limit ({max_allowed_duration:.1f}s) for {word_count} words. Trimming audio to dynamic limit.", None)
                            max_samples = int(max_allowed_duration * 24000)
                            audio_numpy = np.ascontiguousarray(audio_numpy[:max_samples], dtype=np.float32)
                            duration = len(audio_numpy) / 24000.0
                        else:
                            duration = raw_duration
                            audio_numpy = np.ascontiguousarray(audio_numpy, dtype=np.float32)
                        
                        group = next((g for g in getattr(self, "queue_groups", []) if g["group_id"] == task.get('group_id')), None)
                        if group:
                            group["generated_text_length"] = group.get("generated_text_length", 0) + chunk_text_length
                            group["total_audio_duration"] = group.get("total_audio_duration", 0.0) + duration

                        if task.get('on_audio_generated'):
                            try:
                                task['on_audio_generated'](audio_numpy)
                            except Exception as e:
                                callback_log("System", f"[TTS Error] Audio callback error: {e}", None)

                        save_only = task.get('save_only', False)
                        save_path = task.get('save_audio_path')

                        if save_path:
                            if group:
                                group.setdefault("collected_audio", []).append(audio_numpy)
                            # When the final chunk of the message is reached, write the complete audio
                            if not task.get('is_contiguous', False):
                                try:
                                    import soundfile as sf
                                    if group and group.get("collected_audio"):
                                        full_audio = np.concatenate(group["collected_audio"])
                                    else:
                                        full_audio = audio_numpy
                                    os.makedirs(os.path.dirname(os.path.abspath(save_path)), exist_ok=True)
                                    sf.write(save_path, full_audio, 24000)
                                    if save_only:
                                        callback_log("System", f"[TTS] Audio successfully saved to: {save_path}", None)
                                except Exception as e:
                                    callback_log("System", f"[TTS Error] Failed to write audio file: {e}", None)

                        if save_only:
                            with self.queue_lock:
                                if group:
                                    group["played_audio_duration"] = group.get("played_audio_duration", 0.0) + duration
                                    group["chunks_played"] = group.get("chunks_played", 0) + 1
                                self.queue_condition.notify_all()
                        else:
                            chunk = {
                                'type': 'neural',
                                'audio_numpy': audio_numpy,
                                'command': command,
                                'clean_text': clean_text,
                                'word_count': word_count,
                                'callback_log': callback_log,
                                'bypass_mute': task.get('bypass_mute', False),
                                'is_contiguous': task.get('is_contiguous', False),
                                'group_id': task.get('group_id'),
                                'epoch': task.get('epoch', 0),
                                'duration': duration,
                                'max_allowed_duration': max_allowed_duration
                            }
                            with self.queue_lock:
                                self.playback_chunks.append(chunk)
                                self.queue_condition.notify_all()
                else:
                    duration = min(len(clean_text) / 15.0, max_allowed_duration)
                    
                    group = next((g for g in getattr(self, "queue_groups", []) if g["group_id"] == task.get('group_id')), None)
                    if group:
                        group["generated_text_length"] = group.get("generated_text_length", 0) + chunk_text_length
                        group["total_audio_duration"] = group.get("total_audio_duration", 0.0) + duration
                    
                    save_only = task.get('save_only', False)
                    save_path = task.get('save_audio_path')
                    if save_only:
                        if save_path:
                            try:
                                import sys, subprocess
                                os.makedirs(os.path.dirname(os.path.abspath(save_path)), exist_ok=True)
                                if sys.platform == "win32":
                                    ps_script = f'Add-Type -AssemblyName System.speech; $s = New-Object System.Speech.Synthesis.SpeechSynthesizer; $s.SetOutputToWaveFile(\'{save_path}\'); $s.Speak(\'{task["clean_text"]}\'); $s.Dispose();'
                                    subprocess.Popen(["powershell", "-Command", ps_script], creationflags=subprocess.CREATE_NO_WINDOW).wait()
                                callback_log("System", f"[TTS] Audio saved to: {save_path}", None)
                            except Exception as e:
                                callback_log("System", f"[TTS Error] Failed saving audio: {e}", None)
                        with self.queue_lock:
                            if group:
                                group["played_audio_duration"] = group.get("played_audio_duration", 0.0) + duration
                                group["chunks_played"] = group.get("chunks_played", 0) + 1
                            self.queue_condition.notify_all()
                    else:
                        chunk = {
                            'type': 'fallback',
                            'clean_text': clean_text,
                            'command': command,
                            'word_count': word_count,
                            'callback_log': callback_log,
                            'bypass_mute': task.get('bypass_mute', False),
                            'is_contiguous': task.get('is_contiguous', False),
                            'group_id': task.get('group_id'),
                            'epoch': task.get('epoch', 0),
                            'duration': duration,
                            'max_allowed_duration': max_allowed_duration
                        }
                        with self.queue_lock:
                            self.playback_chunks.append(chunk)
                            self.queue_condition.notify_all()
            except Exception as e:
                error_msg = str(e)
                if "backend" in error_msg.lower() or "ffmpeg" in error_msg.lower():
                     callback_log("System", f"[TTS CRITICAL] Windows failed to read the audio file format! Try using a .WAV file instead of .MP3 for your voice sample.", None)
                else:
                     callback_log("System", f"[TTS Generation Error] {error_msg}", None)
                with self.queue_lock:
                    group = next((g for g in getattr(self, "queue_groups", []) if g["group_id"] == task.get('group_id')), None)
                    if group:
                        group["chunks_played"] = group.get("chunks_played", 0) + 1
                        if not any(c.get("group_id") == task.get('group_id') for c in self.playback_chunks):
                            group["deleted"] = True
                    self.queue_condition.notify_all()
            finally:
                self.current_generating_group_id = None
                self.generation_queue.task_done()

    def _playback_worker(self):
        import sounddevice as sd
        import numpy as np
        import time

        while True:
            with self.queue_lock:
                task = None
                while True:
                    # Clean up any orphaned chunks belonging to deleted groups
                    deleted_ids = {g["group_id"] for g in self.queue_groups if g.get("deleted", False)}
                    if deleted_ids:
                        self.playback_chunks = [c for c in self.playback_chunks if c.get("group_id") not in deleted_ids]

                    active_groups = []
                    for g in self.queue_groups:
                        if g.get("deleted", False):
                            continue
                        if g["group_id"] == getattr(self, "skip_group_id", None):
                            continue
                        if g.get("last_chunk_added", False) and g.get("chunks_played", 0) >= g.get("chunks_added", 1):
                            continue
                        active_groups.append(g)
                    
                    if not active_groups:
                        self.queue_condition.wait(timeout=1.0)
                        continue
                        
                    active_groups.sort(key=lambda x: (x.get("priority", 2), x.get("order", 0)))
                    highest_priority_group = active_groups[0]
                    
                    chunk_idx = next((i for i, c in enumerate(self.playback_chunks) if c["group_id"] == highest_priority_group["group_id"]), -1)
                    
                    if chunk_idx != -1:
                        task = self.playback_chunks.pop(chunk_idx)
                        break
                    else:
                        now = time.time()
                        group_order_time = highest_priority_group.get("order", now)
                        
                        # Safety net for stuck LLM or generation items
                        if highest_priority_group.get("is_pending_llm") or highest_priority_group.get("is_processing_llm"):
                            llm_start = highest_priority_group.get("llm_start_time", group_order_time)
                            if (now - llm_start) > 25.0:
                                highest_priority_group["deleted"] = True
                                highest_priority_group["last_chunk_added"] = True
                                highest_priority_group["is_pending_llm"] = False
                                highest_priority_group["is_processing_llm"] = False
                                highest_priority_group["timed_out_from_queue"] = True
                                if hasattr(self, "app") and hasattr(self.app, "append_system_log"):
                                    self.app.append_system_log(f"\n[System] DeepSeek response taking longer than usual (>25s) for {highest_priority_group.get('user', 'Unknown')}. Advancing TTS queue...")
                                self.queue_condition.notify_all()
                                continue
                        elif highest_priority_group["group_id"] == getattr(self, "current_generating_group_id", None):
                            # Actively synthesizing neural TTS audio in _generation_worker
                            gen_start = getattr(self, "current_generating_start_time", now)
                            if (now - gen_start) > 120.0:
                                highest_priority_group["deleted"] = True
                                highest_priority_group["last_chunk_added"] = True
                                if hasattr(self, "app") and hasattr(self.app, "append_system_log"):
                                    self.app.append_system_log(f"\n[TTS Watchdog] Generation timed out (>120s) for {highest_priority_group.get('user', 'Unknown')}. Skipping item.")
                                self.queue_condition.notify_all()
                                continue
                        elif (now - group_order_time) > 60.0:
                            highest_priority_group["deleted"] = True
                            highest_priority_group["last_chunk_added"] = True
                            if hasattr(self, "app") and hasattr(self.app, "append_system_log"):
                                self.app.append_system_log(f"\n[TTS Watchdog] Generation timed out for {highest_priority_group.get('user', 'Unknown')}. Skipping item.")
                            self.queue_condition.notify_all()
                            continue

                        self.queue_condition.wait(timeout=1.0)
                        
            group_id = task.get('group_id')
            if task.get('epoch', 0) < getattr(self, 'current_epoch', 0) or (getattr(self, "skip_group_id", None) == group_id and group_id is not None):
                with self.queue_lock:
                    group = next((g for g in getattr(self, "queue_groups", []) if g["group_id"] == group_id), None)
                    if group:
                        group["chunks_played"] = group.get("chunks_played", 0) + 1
                    self.queue_condition.notify_all()
                continue
                
            self.current_playing_group_id = group_id
            
            duration = task.get('duration', 0.0)
            command = task['command']
            callback_log = task['callback_log']
            
            # Checkpoint A: Audio File Inspection (Before Playback)
            # Trim the audio to the dynamic limit instead of skipping the entire audio
            wc = task.get('word_count') or len(task.get('clean_text', '').strip().split())
            dynamic_limit = self.get_dynamic_duration_limit(wc) if wc > 0 else max(5.0, duration)
            
            if task.get('type') in ('neural', 'audio_file') and 'audio_numpy' in task:
                audio_numpy = task['audio_numpy']
                sr = 24000 if task['type'] == 'neural' else task.get('samplerate', 24000)
                if len(audio_numpy) > 0:
                    actual_dur = len(audio_numpy) / float(sr)
                    if actual_dur > (dynamic_limit + 0.05):
                        max_samples = int(dynamic_limit * sr)
                        task['audio_numpy'] = np.ascontiguousarray(audio_numpy[:max_samples], dtype=np.float32)
                        duration = len(task['audio_numpy']) / float(sr)
                        task['duration'] = duration
                        callback_log("System", f"[TTS] Audio duration ({actual_dur:.1f}s) exceeded limit ({dynamic_limit:.1f}s) for {wc} words. Trimmed to dynamic limit before playback.", None)
                    else:
                        task['audio_numpy'] = np.ascontiguousarray(audio_numpy, dtype=np.float32)

            import time
            with self.queue_lock:
                group = next((g for g in getattr(self, "queue_groups", []) if g["group_id"] == group_id), None)
                if group:
                    group["current_playing_duration"] = duration
                    group["current_playing_start_time"] = time.time()
                    if getattr(self, "is_paused", False):
                        group["pause_start_time"] = time.time()
            
            bypass_mute = task.get('bypass_mute', False)
            is_contiguous = task.get('is_contiguous', False)
            
            try:
                # If paused, wait until unpaused or skipped before beginning audio output
                while getattr(self, "is_paused", False):
                    if getattr(self, "skip_group_id", None) == group_id:
                        break
                    time.sleep(0.05)
                    
                if getattr(self, "skip_group_id", None) == group_id:
                    continue

                if hasattr(self, "on_audio_play_hook") and self.on_audio_play_hook:
                    try:
                        self.on_audio_play_hook()
                    except Exception:
                        pass
                callback_log("System", f"[TTS] Playing audio for {command}...", None)
                if task['type'] == 'audio_file':
                    audio_numpy = task['audio_numpy']
                    fs = task['samplerate']
                    if len(audio_numpy) > 0:
                        import threading
                        import sounddevice as sd
                        current_idx = 0
                        stream_finished = threading.Event()
                        audio_2d = audio_numpy.reshape(-1, 1) if len(audio_numpy.shape) == 1 else audio_numpy
                        
                        def callback(outdata, frames, time_info, status):
                            nonlocal current_idx
                            if getattr(self, "skip_group_id", None) == group_id or task.get('epoch', 0) < getattr(self, 'current_epoch', 0):
                                outdata.fill(0)
                                raise sd.CallbackStop()
                            if getattr(self, "is_paused", False):
                                outdata.fill(0)
                                return
                            if getattr(self, "is_muted", False) and not bypass_mute:
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

                        play_start = time.time()
                        playback_timeout = max(5.0, duration + 2.0)
                        try:
                            with sd.OutputStream(samplerate=fs, channels=audio_2d.shape[1], callback=callback, finished_callback=stream_finished.set) as stream:
                                while not stream_finished.is_set():
                                    if getattr(self, "skip_group_id", None) == group_id or task.get('epoch', 0) < getattr(self, 'current_epoch', 0):
                                        # Callback will raise CallbackStop on next block and set stream_finished
                                        if not stream_finished.wait(timeout=0.15):
                                            try:
                                                if stream.active:
                                                    stream.abort()
                                            except Exception:
                                                pass
                                            stream_finished.wait(timeout=0.25)
                                        break
                                    if getattr(self, "is_paused", False):
                                        play_start += 0.05
                                    elif (time.time() - play_start) > playback_timeout:
                                        callback_log("System", f"[TTS Watchdog] Audio playback timed out ({playback_timeout:.1f}s). Advancing queue.", None)
                                        try:
                                            if stream.active:
                                                stream.abort()
                                        except Exception:
                                            pass
                                        stream_finished.wait(timeout=0.25)
                                        break
                                    time.sleep(0.02)
                                # Graceful wait to ensure PortAudio callback thread exits cleanly before stream closes
                                stream_finished.wait(timeout=0.4)
                        except Exception as e:
                            callback_log("System", f"[TTS Audio Output Error] {e}", None)
                                
                    if not is_contiguous:
                        time.sleep(1.0)
                elif task['type'] == 'subprocess_audio':
                    if not getattr(self, "is_muted", False) or bypass_mute:
                        import subprocess
                        import sys
                        import os
                        abs_path = os.path.abspath(task['file_path'])
                        if sys.platform == "win32":
                            import ctypes
                            import uuid
                            alias = "mp3alert_" + str(uuid.uuid4().hex)[:8]
                            self.current_mci_alias = alias
                            try:
                                ctypes.windll.winmm.mciSendStringW(f'open "{abs_path}" type mpegvideo alias {alias}', None, 0, None)
                                ctypes.windll.winmm.mciSendStringW(f'play {alias}', None, 0, None)
                                
                                status_buffer = ctypes.create_unicode_buffer(128)
                                is_mci_paused = False
                                play_start = time.time()
                                playback_timeout = max(5.0, duration + 3.0)
                                while True:
                                    if getattr(self, "is_paused", False):
                                        if not is_mci_paused:
                                            ctypes.windll.winmm.mciSendStringW(f'pause {alias}', None, 0, None)
                                            is_mci_paused = True
                                        play_start += 0.05
                                        time.sleep(0.05)
                                        if getattr(self, "skip_group_id", None) == group_id or task.get('epoch', 0) < getattr(self, 'current_epoch', 0):
                                            break
                                        continue
                                    elif is_mci_paused:
                                        ctypes.windll.winmm.mciSendStringW(f'play {alias}', None, 0, None)
                                        is_mci_paused = False

                                    ctypes.windll.winmm.mciSendStringW(f'status {alias} mode', status_buffer, 128, None)
                                    if status_buffer.value != "playing":
                                        break
                                    if getattr(self, "skip_group_id", None) == group_id or task.get('epoch', 0) < getattr(self, 'current_epoch', 0):
                                        break
                                    if (time.time() - play_start) > playback_timeout:
                                        callback_log("System", f"[TTS Watchdog] Media playback timed out after {playback_timeout:.1f}s. Advancing queue.", None)
                                        break
                                    time.sleep(0.05)
                            finally:
                                try:
                                    ctypes.windll.winmm.mciSendStringW(f'stop {alias}', None, 0, None)
                                    ctypes.windll.winmm.mciSendStringW(f'close {alias}', None, 0, None)
                                except Exception:
                                    pass
                                self.current_mci_alias = None
                        elif sys.platform == "darwin":
                            subprocess.run(["afplay", abs_path])
                        else:
                            subprocess.run(["ffplay", "-nodisp", "-autoexit", abs_path], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
                elif task['type'] == 'neural':
                    audio_numpy = task['audio_numpy']
                    
                    # Mute logic is handled in the playback callback to allow unmuting mid-playback
                    
                    if len(audio_numpy) > 0:
                        import threading
                        import sounddevice as sd
                        current_idx = 0
                        stream_finished = threading.Event()
                        
                        audio_2d = np.ascontiguousarray(audio_numpy.reshape(-1, 1) if len(audio_numpy.shape) == 1 else audio_numpy, dtype=np.float32)
                        
                        def callback(outdata, frames, time_info, status):
                            nonlocal current_idx
                            if getattr(self, "skip_group_id", None) == group_id or task.get('epoch', 0) < getattr(self, 'current_epoch', 0):
                                outdata.fill(0)
                                raise sd.CallbackStop()
                                
                            if getattr(self, "is_paused", False):
                                outdata.fill(0)
                                return
                                
                            if getattr(self, "is_muted", False) and not bypass_mute:
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
                        
                        # Dynamic playback watchdog: duration + buffer (scaled dynamically by word count)
                        wc = task.get('word_count') or len(task.get('clean_text', '').strip().split())
                        dynamic_limit = self.get_dynamic_duration_limit(wc)
                        playback_timeout = min(duration + 3.0, dynamic_limit + 3.0)
                        play_start = time.time()

                        try:
                            with sd.OutputStream(samplerate=24000, channels=audio_2d.shape[1], callback=callback, finished_callback=stream_finished.set) as stream:
                                while not stream_finished.is_set():
                                    if getattr(self, "skip_group_id", None) == group_id or task.get('epoch', 0) < getattr(self, 'current_epoch', 0):
                                        # Callback will raise CallbackStop on next block and set stream_finished
                                        if not stream_finished.wait(timeout=0.15):
                                            try:
                                                if stream.active:
                                                    stream.abort()
                                            except Exception:
                                                pass
                                            stream_finished.wait(timeout=0.25)
                                        break
                                    if getattr(self, "is_paused", False):
                                        play_start += 0.05
                                    elif (time.time() - play_start) > playback_timeout:
                                        callback_log("System", f"[TTS Watchdog] Audio playback timed out after {playback_timeout:.1f}s for {command}. Advancing queue.", None)
                                        try:
                                            if stream.active:
                                                stream.abort()
                                        except Exception:
                                            pass
                                        stream_finished.wait(timeout=0.25)
                                        break
                                    time.sleep(0.02)
                                # Graceful wait to ensure PortAudio callback thread exits cleanly before stream closes
                                stream_finished.wait(timeout=0.4)
                        except Exception as e:
                            callback_log("System", f"[TTS Audio Output Error] {e}", None)
                                
                    if not is_contiguous:
                        time.sleep(1.0)
                                
                    if not is_contiguous:
                        time.sleep(1.0)
                else:
                    if getattr(self, "is_muted", False) and not bypass_mute:
                        callback_log("System", r"[TTS] PyTorch missing fallback skipped due to App Mute state.", None)
                    else:
                        callback_log("System", r"[TTS Warning] True Voice Cloning disabled (PyTorch missing). Using fallback.", None)
                        while getattr(self, "is_paused", False):
                            time.sleep(0.1)
                            if getattr(self, "skip_group_id", None) == group_id:
                                break
                        
                        if getattr(self, "skip_group_id", None) != group_id:
                            ps_script = f'Add-Type -AssemblyName System.speech; (New-Object System.Speech.Synthesis.SpeechSynthesizer).Speak(\'{task["clean_text"]}\');'
                            self.current_ps_process = subprocess.Popen(["powershell", "-Command", ps_script], creationflags=subprocess.CREATE_NO_WINDOW)
                            wc = task.get('word_count') or len(task.get('clean_text', '').strip().split())
                            ps_timeout = self.get_dynamic_duration_limit(wc) + 3.0
                            try:
                                self.current_ps_process.wait(timeout=ps_timeout)
                            except subprocess.TimeoutExpired:
                                try:
                                    self.current_ps_process.kill()
                                except Exception:
                                    pass
                                callback_log("System", f"[TTS Watchdog] Fallback synthesis timed out ({ps_timeout:.1f}s). Advancing queue.", None)
                        if not is_contiguous:
                            time.sleep(1.0)
                
                callback_log("System", f"[TTS] Playback finished for {command}.", None)
            except Exception as e:
                callback_log("System", f"[TTS Playback Error] {str(e)}", None)
            finally:
                with self.queue_lock:
                    group = next((g for g in getattr(self, "queue_groups", []) if g["group_id"] == group_id), None)
                    if group:
                        group["played_audio_duration"] = group.get("played_audio_duration", 0.0) + task.get('duration', 0.0)
                        group["current_playing_start_time"] = None
                        group["chunks_played"] = group.get("chunks_played", 0) + 1
                    self.current_playing_group_id = None
                    self.queue_condition.notify_all()

    def get_queue_state(self):
        playing_id = getattr(self, "current_playing_group_id", None)
        skip_id = getattr(self, "skip_group_id", None)
        gen_id = getattr(self, "current_generating_group_id", None)
        
        with self.queue_lock:
            active_groups = []
            for g in getattr(self, "queue_groups", []):
                if g.get("deleted", False):
                    continue
                if g["group_id"] == skip_id:
                    continue
                if g.get("last_chunk_added", False) and g.get("chunks_played", 0) >= g.get("chunks_added", 1):
                    continue
                active_groups.append(g)
                
            active_groups.sort(key=lambda x: (x.get("priority", 2), x.get("order", 0)))
            
            state = []
            for g in active_groups:
                gid = g["group_id"]
                
                if gid == playing_id:
                    status = "Playing"
                elif any(c.get("group_id") == gid for c in self.playback_chunks):
                    status = "Waiting to Play"
                elif gid == gen_id:
                    status = "Processing"
                elif g.get("is_processing_llm"):
                    status = "Generating LLM"
                elif g.get("is_pending_llm"):
                    status = "Waiting for LLM"
                else:
                    status = "Waiting"
                
                item_state = {
                    "group_id": gid,
                    "commands": g.get("commands", [g.get("command")]),
                    "category": g.get("category", "Standard"),
                    "status": status,
                    "user": g.get("user", "Unknown"),
                    "text_length": len(g.get("text", ""))
                }
                   
                if status == "Processing":
                    import time
                    elapsed = time.time() - getattr(self, "current_generating_start_time", time.time())
                    unprocessed = item_state["text_length"] - g.get("generated_text_length", 0)
                    estimated_total = max(2.0, unprocessed / 15.0)
                    base_progress = g.get("generated_text_length", 0) / max(1, item_state["text_length"])
                    
                    chunk_progress = min(0.95, elapsed / estimated_total) if estimated_total > 0 else 0
                    total_progress = base_progress + (chunk_progress * (unprocessed / max(1, item_state["text_length"])))
                    item_state["progress"] = min(0.99, max(0.0, total_progress))
                elif status == "Playing":
                    import time
                    played = g.get("played_audio_duration", 0.0)
                    if g.get("current_playing_start_time"):
                        if g.get("pause_start_time"):
                            elapsed = g["pause_start_time"] - g["current_playing_start_time"]
                        else:
                            elapsed = time.time() - g["current_playing_start_time"]
                        played += elapsed
                       
                    total = g.get("total_audio_duration", 0.1)
                    if total < 0.1: total = 0.1
                       
                    unprocessed_length = item_state["text_length"] - g.get("generated_text_length", 0)
                    if unprocessed_length > 0:
                        total += unprocessed_length / 15.0
                           
                    progress = played / total
                    if progress > 1.0: progress = 1.0
                    item_state["progress"] = progress
                else:
                    item_state["progress"] = 0.0
                       
                state.append(item_state)
        return state

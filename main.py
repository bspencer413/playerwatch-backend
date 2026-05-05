from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from datetime import datetime, timedelta
from typing import Optional
import jwt
import bcrypt
import psycopg2
import os
import urllib.request
import urllib.parse
import json as json_lib
from contextlib import contextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# ── Config ─────────────────────────────────────────────────────────────────────

SECRET_KEY = os.environ.get("SECRET_KEY", "playerwatch-secret-2026")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 10080

# Reuses the MW Postgres but with prefixed tables for full data isolation.
# Set DATABASE_URL in Render env vars to override.
DB_HOST = os.environ.get("DB_HOST", "dpg-d6qhp3ngi27c73a3ivag-a.oregon-postgres.render.com")
DB_USER = os.environ.get("DB_USER", "memorial_watch_db_user")
DB_PASS = os.environ.get("DB_PASS", "9IkXRdY8NcZSKy0yw5b7viPdtIrVIITR")
DB_NAME = os.environ.get("DB_NAME", "memorial_watch_db")
DATABASE_URL = os.environ.get("DATABASE_URL",
    "postgresql://" + DB_USER + ":" + DB_PASS + "@" + DB_HOST + "/" + DB_NAME)

# ── Database ───────────────────────────────────────────────────────────────────

def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS pw_users (
        id SERIAL PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS pw_watchlist (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        location TEXT,
        dob TEXT,
        status TEXT DEFAULT 'active',
        is_deceased BOOLEAN DEFAULT FALSE,
        death_year TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES pw_users (id)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS pw_notifications (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        watchlist_id INTEGER NOT NULL,
        message TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES pw_users (id),
        FOREIGN KEY (watchlist_id) REFERENCES pw_watchlist (id)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS pw_snapshots (
        watchlist_id INTEGER PRIMARY KEY,
        snapshot_json TEXT NOT NULL,
        captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (watchlist_id) REFERENCES pw_watchlist (id)
    )""")
    conn.commit()
    conn.close()

@contextmanager
def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    try:
        yield conn
    finally:
        conn.close()

# ── Models ─────────────────────────────────────────────────────────────────────

class UserCreate(BaseModel):
    email: EmailStr
    password: str

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

class WatchlistItem(BaseModel):
    name: str
    location: Optional[str] = None
    dob: Optional[str] = None
    is_deceased: Optional[bool] = False
    death_year: Optional[str] = None

# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(title="Player Watch API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")

# ── Auth helpers ───────────────────────────────────────────────────────────────

def hash_password(p: str) -> str:
    return bcrypt.hashpw(p.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

def verify_password(p: str, h: str) -> bool:
    return bcrypt.checkpw(p.encode("utf-8"), h.encode("utf-8"))

def create_access_token(data: dict) -> str:
    to_encode = data.copy()
    to_encode.update({"exp": datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid authentication")
        return int(user_id)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# ── Health ─────────────────────────────────────────────────────────────────────

@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat(),
            "version": "0.2.0", "app": "Player Watch"}

# ── Auth ───────────────────────────────────────────────────────────────────────

@app.post("/auth/register", response_model=Token)
async def register(user: UserCreate):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM pw_users WHERE email = %s", (user.email,))
        if c.fetchone():
            raise HTTPException(status_code=400, detail="Email already registered")
        c.execute("INSERT INTO pw_users (email, password_hash) VALUES (%s, %s) RETURNING id",
                  (user.email, hash_password(user.password)))
        user_id = c.fetchone()[0]
        conn.commit()
        return {"access_token": create_access_token({"sub": str(user_id)}), "token_type": "bearer"}

@app.post("/auth/login", response_model=Token)
async def login(user: UserLogin):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id, password_hash FROM pw_users WHERE email = %s", (user.email,))
        result = c.fetchone()
        if not result or not verify_password(user.password, result[1]):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        return {"access_token": create_access_token({"sub": str(result[0])}), "token_type": "bearer"}

@app.delete("/account")
async def delete_account(user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""DELETE FROM pw_snapshots WHERE watchlist_id IN
                     (SELECT id FROM pw_watchlist WHERE user_id = %s)""", (user_id,))
        c.execute("DELETE FROM pw_notifications WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM pw_watchlist WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM pw_users WHERE id = %s", (user_id,))
        conn.commit()
        return {"message": "Account permanently deleted"}

# ── Watchlist ──────────────────────────────────────────────────────────────────

@app.get("/watchlist")
async def get_watchlist(user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""SELECT id, name, location, dob, status, created_at, is_deceased, death_year
                     FROM pw_watchlist WHERE user_id = %s AND status = 'active'
                     ORDER BY created_at DESC""", (user_id,))
        return [{"id": r[0], "name": r[1], "location": r[2], "dob": r[3],
                 "status": r[4], "created_at": str(r[5]),
                 "is_deceased": r[6] or False, "death_year": r[7]}
                for r in c.fetchall()]

@app.post("/watchlist")
async def add_to_watchlist(item: WatchlistItem, user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""INSERT INTO pw_watchlist (user_id, name, location, dob, is_deceased, death_year)
                     VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
                  (user_id, item.name, item.location, item.dob,
                   item.is_deceased or False, item.death_year))
        new_id = c.fetchone()[0]
        conn.commit()
        return {"id": new_id, "name": item.name, "location": item.location, "dob": item.dob}

@app.delete("/watchlist/{item_id}")
async def remove_from_watchlist(item_id: int, user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("UPDATE pw_watchlist SET status = 'deleted' WHERE id = %s AND user_id = %s",
                  (item_id, user_id))
        if c.rowcount == 0:
            conn.rollback()
            raise HTTPException(status_code=404, detail="Item not found")
        c.execute("DELETE FROM pw_snapshots WHERE watchlist_id = %s", (item_id,))
        conn.commit()
        return {"message": "Removed"}

# ── Notifications ──────────────────────────────────────────────────────────────

@app.get("/notifications")
async def get_notifications(user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""SELECT n.id, n.message, n.created_at, w.name, n.watchlist_id
                     FROM pw_notifications n
                     JOIN pw_watchlist w ON n.watchlist_id = w.id
                     WHERE n.user_id = %s
                     ORDER BY n.created_at DESC LIMIT 50""", (user_id,))
        return [{"id": r[0], "name": r[3], "message": r[1],
                 "created_at": str(r[2]), "watchlist_id": r[4]}
                for r in c.fetchall()]

@app.delete("/notifications/{notif_id}")
async def delete_notification(notif_id: int, user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM pw_notifications WHERE id = %s AND user_id = %s",
                  (notif_id, user_id))
        conn.commit()
        return {"deleted": True}

# ── ESPN proxy (bypasses browser CORS) ─────────────────────────────────────────

def fetch_url(url: str, timeout: int = 15):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json_lib.loads(resp.read().decode())
    except Exception as e:
        print("[espn] fetch error " + url + ": " + str(e))
        return None

@app.get("/espn/search")
async def espn_search(name: str, limit: int = 10):
    """Proxy ESPN search. Frontend calls this instead of ESPN directly."""
    url = ("https://site.web.api.espn.com/apis/search/v2"
           "?region=us&lang=en&query=" + urllib.parse.quote(name) +
           "&limit=" + str(limit))
    data = fetch_url(url)
    if data is None:
        raise HTTPException(status_code=502, detail="ESPN search failed")
    return data

# MLS uses ESPN's soccer/usa.1 path, not soccer/mls
LEAGUE_OVERRIDES = {"mls": "usa.1"}

@app.get("/espn/overview")
async def espn_overview(sport: str, league: str, id: str):
    """Proxy ESPN athlete data via site.web.api.espn.com/apis/common/v3.
    Validates the ID against the base athlete endpoint, then fetches overview.
    Always includes region=us&lang=en — ESPN treats these as required."""
    espn_league = LEAGUE_OVERRIDES.get(league, league)
    base = ("https://site.web.api.espn.com/apis/common/v3/sports/"
            + sport + "/" + espn_league + "/athletes/" + id)
    qs = "?region=us&lang=en"

    # Validate ID exists
    root = fetch_url(base + qs)
    if root is None or not isinstance(root, dict):
        raise HTTPException(status_code=404, detail="Athlete not found on ESPN")

    # Fetch sections; merge what we get
    result = {"athlete": root}
    for section in ["overview", "bio", "stats", "gamelog"]:
        sec = fetch_url(base + "/" + section + qs)
        if sec is not None and isinstance(sec, dict):
            result[section] = sec
    return result

@app.get("/espn/news")
async def espn_news(sport: str, league: str, id: str):
    """Proxy ESPN athlete news. Different host: site.api (no 'web')."""
    espn_league = LEAGUE_OVERRIDES.get(league, league)
    url = ("https://site.api.espn.com/apis/site/v2/sports/"
           + sport + "/" + espn_league + "/athletes/" + id + "/news?region=us&lang=en")
    data = fetch_url(url)
    if data is None:
        raise HTTPException(status_code=404, detail="No news found")
    return data

@app.get("/espn/debug")
async def espn_debug(name: str):
    """Returns raw ESPN search response so we can inspect field shapes."""
    url = ("https://site.web.api.espn.com/apis/search/v2"
           "?region=us&lang=en&query=" + urllib.parse.quote(name) + "&limit=10")
    data = fetch_url(url)
    if data is None:
        raise HTTPException(status_code=502, detail="ESPN search failed")
    return data

# ── Daily watchlist cron (snapshot diff → notifications) ──────────────────────

def build_alert_snapshot(meta_json):
    """Fetch ESPN data for a watched player and return a small alert-relevant
    snapshot. Returns None if we can't reach ESPN or the meta is missing IDs."""
    if not meta_json:
        return None
    try:
        meta = json_lib.loads(meta_json)
    except Exception:
        return None
    sport = meta.get("sport")
    league = meta.get("league")
    espn_id = meta.get("espnId")
    if not sport or not league or not espn_id:
        return None

    espn_league = LEAGUE_OVERRIDES.get(league, league)
    base = ("https://site.web.api.espn.com/apis/common/v3/sports/"
            + sport + "/" + espn_league + "/athletes/" + str(espn_id))
    root = fetch_url(base + "?region=us&lang=en")
    if root is None or not isinstance(root, dict):
        return None

    # Mirror frontend buildSnapshot's "athlete.athlete" unwrap
    inner = root.get("athlete") if isinstance(root.get("athlete"), dict) else root

    # Status
    status = root.get("status") or inner.get("status") or {}
    status_type = status.get("type") if isinstance(status, dict) else {}
    if not isinstance(status_type, dict):
        status_type = {}
    status_text = (status_type.get("description")
                   or status_type.get("name")
                   or (status.get("name") if isinstance(status, dict) else "")
                   or "")

    # Team
    team = root.get("team") or inner.get("team") or {}
    team_name = team.get("displayName") if isinstance(team, dict) else ""

    # First injury (if any)
    injuries = root.get("injuries") or inner.get("injuries") or []
    inj0 = injuries[0] if injuries and isinstance(injuries[0], dict) else {}
    injury_headline = (inj0.get("longComment") or inj0.get("shortComment")
                       or inj0.get("description") or "")
    injury_status = inj0.get("status") or ""
    if not injury_status:
        inj_type = inj0.get("type")
        if isinstance(inj_type, dict):
            injury_status = inj_type.get("description") or ""

    return {
        "status_text": (status_text or "").strip(),
        "team_name": (team_name or "").strip(),
        "injury_headline": (injury_headline or "").strip(),
        "injury_status": (injury_status or "").strip(),
    }


def diff_snapshots(old, new):
    """Compare two alert snapshots. Returns list of short alert message strings.
    First-run (old is None) returns []  — we never alert on the first capture."""
    if old is None or new is None:
        return []
    alerts = []

    old_status = old.get("status_text", "")
    new_status = new.get("status_text", "")
    if old_status and new_status and old_status != new_status:
        alerts.append("Status changed: " + old_status + " → " + new_status)

    old_team = old.get("team_name", "")
    new_team = new.get("team_name", "")
    if old_team and new_team and old_team != new_team:
        alerts.append("Trade: Now with " + new_team)

    old_inj = old.get("injury_headline", "")
    new_inj = new.get("injury_headline", "")
    if new_inj and new_inj != old_inj:
        msg = new_inj if len(new_inj) <= 140 else (new_inj[:137] + "...")
        alerts.append("New injury report: " + msg)

    return alerts


def check_all_watched_players():
    """Daily job: iterate active watchlist, fetch ESPN, diff, write notifications."""
    print("[cron] Starting daily watchlist check at " + datetime.now().isoformat())
    checked = 0
    alerted = 0
    skipped = 0
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""SELECT id, user_id, name, location FROM pw_watchlist
                         WHERE status = 'active'
                           AND location IS NOT NULL
                           AND location != ''""")
            rows = c.fetchall()

        for row in rows:
            watchlist_id, user_id, name, location = row
            new_snap = build_alert_snapshot(location)
            if new_snap is None:
                skipped += 1
                continue
            checked += 1

            # Read prior snapshot
            with get_db() as conn:
                c = conn.cursor()
                c.execute("SELECT snapshot_json FROM pw_snapshots WHERE watchlist_id = %s",
                          (watchlist_id,))
                prior = c.fetchone()
            old_snap = None
            if prior and prior[0]:
                try:
                    old_snap = json_lib.loads(prior[0])
                except Exception:
                    old_snap = None

            # Diff and write any alerts
            alerts = diff_snapshots(old_snap, new_snap)
            if alerts:
                with get_db() as conn:
                    c = conn.cursor()
                    for msg in alerts:
                        c.execute("""INSERT INTO pw_notifications
                                     (user_id, watchlist_id, message)
                                     VALUES (%s, %s, %s)""",
                                  (user_id, watchlist_id, msg))
                    conn.commit()
                alerted += 1

            # Always upsert the new snapshot
            with get_db() as conn:
                c = conn.cursor()
                c.execute("""INSERT INTO pw_snapshots (watchlist_id, snapshot_json, captured_at)
                             VALUES (%s, %s, CURRENT_TIMESTAMP)
                             ON CONFLICT (watchlist_id) DO UPDATE
                             SET snapshot_json = EXCLUDED.snapshot_json,
                                 captured_at = CURRENT_TIMESTAMP""",
                          (watchlist_id, json_lib.dumps(new_snap)))
                conn.commit()

        print("[cron] Done. Checked " + str(checked)
              + " players, " + str(alerted) + " with new alerts, "
              + str(skipped) + " skipped (no ESPN id or fetch failed)")
    except Exception as e:
        print("[cron] Fatal error: " + str(e))


@app.post("/admin/run-cron")
async def run_cron_manually(secret: str):
    """Manually trigger the daily watchlist check. Useful for testing without
    waiting for the 14:00 UTC schedule. Pass ?secret=... matching ADMIN_SECRET."""
    if secret != os.environ.get("ADMIN_SECRET", "playerwatch-cron-2026"):
        raise HTTPException(status_code=403, detail="Forbidden")
    check_all_watched_players()
    return {"status": "completed", "ran_at": datetime.now().isoformat()}


# Module-level scheduler — kept in scope so it isn't garbage-collected
_pw_scheduler = None

# ── Startup ────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    init_db()
    print("Player Watch DB initialized")

    # Schedule daily watchlist check at 14:00 UTC
    # = 4am HST / 9am EST winter / 10am EDT summer
    global _pw_scheduler
    _pw_scheduler = BackgroundScheduler(timezone="UTC")
    _pw_scheduler.add_job(
        check_all_watched_players,
        CronTrigger(hour=14, minute=0),
        id="pw_daily_check",
        replace_existing=True,
    )
    _pw_scheduler.start()
    print("Player Watch cron scheduled: daily at 14:00 UTC")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

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

app = FastAPI(title="Player Watch API", version="0.1.2")

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
            "version": "0.1.2", "app": "Player Watch"}

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
        conn.commit()
        if c.rowcount == 0:
            raise HTTPException(status_code=404, detail="Item not found")
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
        req = urllib.request.Request(url, headers={"User-Agent": "PlayerWatch/0.1"})
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

@app.get("/espn/overview")
async def espn_overview(sport: str, league: str, id: str):
    """Proxy ESPN athlete overview. Tries multiple endpoints — paths vary by sport."""
    urls = [
        "https://site.api.espn.com/apis/common/v3/sports/" + sport + "/" + league + "/athletes/" + id + "/overview",
        "https://site.web.api.espn.com/apis/common/v3/sports/" + sport + "/" + league + "/athletes/" + id + "/overview",
        "https://site.api.espn.com/apis/site/v2/sports/" + sport + "/" + league + "/athletes/" + id,
        "https://sports.core.api.espn.com/v2/sports/" + sport + "/leagues/" + league + "/athletes/" + id,
    ]
    for u in urls:
        data = fetch_url(u)
        if data is not None and isinstance(data, dict):
            return data
    raise HTTPException(status_code=404, detail="No ESPN data found for this athlete")

# ── Startup ────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    init_db()
    print("Player Watch DB initialized")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

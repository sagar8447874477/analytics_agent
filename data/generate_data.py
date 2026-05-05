"""
generate_data.py
Generates a synthetic analytics SQLite database with ~30K rows total.
Tables: users, sessions, transactions, content_views
"""

import sqlite3
import random
import json
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

DB_PATH = "analytics.db"

# ── Config ──────────────────────────────────────────────────────────────────
NUM_USERS        = 5_000
NUM_SESSIONS     = 20_000
NUM_TRANSACTIONS = 6_000
NUM_VIEWS        = 30_000

COUNTRIES   = ["US","IN","GB","CA","AU","DE","FR","BR","MX","SG"]
WEIGHTS_C   = [0.30,0.18,0.10,0.07,0.05,0.05,0.05,0.05,0.05,0.10]
PLATFORMS   = ["iOS","Android","Web","Desktop"]
WEIGHTS_P   = [0.35,0.30,0.25,0.10]
PLANS       = ["free","basic","pro","enterprise"]
WEIGHTS_PL  = [0.55,0.25,0.15,0.05]
CONTENT     = ["article","video","podcast","tutorial","webinar"]
CHANNELS    = ["organic","paid_search","social","email","referral","direct"]

START_DATE  = datetime(2024, 1, 1)
END_DATE    = datetime(2024, 12, 31)

def rand_date(start=START_DATE, end=END_DATE):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def rand_date_after(dt, max_days=365):
    end = min(dt + timedelta(days=max_days), END_DATE)
    if dt >= end:
        return None
    return dt + timedelta(seconds=random.randint(0, int((end - dt).total_seconds())))

# ── Build DB ─────────────────────────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)
cur  = conn.cursor()

cur.executescript("""
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS sessions;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS content_views;

CREATE TABLE users (
    user_id        INTEGER PRIMARY KEY,
    created_at     TEXT NOT NULL,
    country        TEXT NOT NULL,
    platform       TEXT NOT NULL,
    plan           TEXT NOT NULL,
    age            INTEGER,
    referral_channel TEXT
);

CREATE TABLE sessions (
    session_id     INTEGER PRIMARY KEY,
    user_id        INTEGER NOT NULL,
    started_at     TEXT NOT NULL,
    duration_sec   INTEGER,
    platform       TEXT,
    country        TEXT,
    is_bounce      INTEGER DEFAULT 0,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
);

CREATE TABLE transactions (
    txn_id         INTEGER PRIMARY KEY,
    user_id        INTEGER NOT NULL,
    created_at     TEXT NOT NULL,
    amount_usd     REAL NOT NULL,
    product        TEXT,
    currency       TEXT DEFAULT 'USD',
    status         TEXT DEFAULT 'completed',
    FOREIGN KEY(user_id) REFERENCES users(user_id)
);

CREATE TABLE content_views (
    view_id        INTEGER PRIMARY KEY,
    user_id        INTEGER NOT NULL,
    viewed_at      TEXT NOT NULL,
    content_type   TEXT,
    content_id     INTEGER,
    duration_sec   INTEGER,
    completed      INTEGER DEFAULT 0,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
);
""")

# ── Users ────────────────────────────────────────────────────────────────────
print("Generating users …")
users = []
for uid in range(1, NUM_USERS + 1):
    created = rand_date()
    country  = random.choices(COUNTRIES, WEIGHTS_C)[0]
    platform = random.choices(PLATFORMS, WEIGHTS_P)[0]
    plan     = random.choices(PLANS,     WEIGHTS_PL)[0]
    age      = random.randint(16, 65)
    channel  = random.choices(CHANNELS)[0]
    users.append((uid, created.isoformat(), country, platform, plan, age, channel))

cur.executemany(
    "INSERT INTO users VALUES (?,?,?,?,?,?,?)", users
)

# ── Sessions ─────────────────────────────────────────────────────────────────
print("Generating sessions …")
sessions = []
for sid in range(1, NUM_SESSIONS + 1):
    uid      = random.randint(1, NUM_USERS)
    u_created = datetime.fromisoformat(users[uid-1][1])
    started  = rand_date(start=u_created)
    duration = int(random.expovariate(1/300))          # avg 5 min
    duration = max(5, min(duration, 7200))
    platform = random.choices(PLATFORMS, WEIGHTS_P)[0]
    country  = users[uid-1][3-1]                        # same country as user
    country  = users[uid-1][2]
    is_bounce = 1 if duration < 10 else 0
    sessions.append((sid, uid, started.isoformat(), duration, platform, country, is_bounce))

cur.executemany(
    "INSERT INTO sessions VALUES (?,?,?,?,?,?,?)", sessions
)

# ── Transactions ──────────────────────────────────────────────────────────────
print("Generating transactions …")
products    = ["Pro Monthly","Pro Annual","Enterprise","Add-on Storage","API Credits"]
product_prices = {"Pro Monthly":9.99,"Pro Annual":99,"Enterprise":499,"Add-on Storage":4.99,"API Credits":19.99}
txns = []
# bias toward paying users
paying_users = [u[0] for u in users if u[4] in ("basic","pro","enterprise")]
for tid in range(1, NUM_TRANSACTIONS + 1):
    uid     = random.choice(paying_users)
    u_created = datetime.fromisoformat(users[uid-1][1])
    created = rand_date(start=u_created)
    product = random.choice(products)
    base    = product_prices[product]
    amount  = round(base * random.uniform(0.9, 1.1), 2)
    status  = random.choices(["completed","refunded","failed"],[0.88,0.07,0.05])[0]
    txns.append((tid, uid, created.isoformat(), amount, product, "USD", status))

cur.executemany(
    "INSERT INTO transactions VALUES (?,?,?,?,?,?,?)", txns
)

# ── Content Views ─────────────────────────────────────────────────────────────
print("Generating content views …")
views = []
for vid in range(1, NUM_VIEWS + 1):
    uid       = random.randint(1, NUM_USERS)
    u_created = datetime.fromisoformat(users[uid-1][1])
    viewed_at = rand_date(start=u_created)
    ctype     = random.choices(CONTENT)[0]
    cid       = random.randint(1, 200)
    duration  = random.randint(5, 3600)
    completed = 1 if duration > 120 and random.random() > 0.4 else 0
    views.append((vid, uid, viewed_at.isoformat(), ctype, cid, duration, completed))

cur.executemany(
    "INSERT INTO content_views VALUES (?,?,?,?,?,?,?)", views
)

# ── Indexes ───────────────────────────────────────────────────────────────────
cur.executescript("""
CREATE INDEX IF NOT EXISTS idx_sessions_user   ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_date   ON sessions(started_at);
CREATE INDEX IF NOT EXISTS idx_txns_user       ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_txns_date       ON transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_views_user      ON content_views(user_id);
CREATE INDEX IF NOT EXISTS idx_views_date      ON content_views(viewed_at);
""")

conn.commit()
conn.close()

print(f"""
✅  Database created: {DB_PATH}
    users          : {NUM_USERS:,}
    sessions       : {NUM_SESSIONS:,}
    transactions   : {NUM_TRANSACTIONS:,}
    content_views  : {NUM_VIEWS:,}
""")

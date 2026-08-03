from fastapi import FastAPI, Depends, HTTPException, Header
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

# == Config ===================================================================

SECRET_KEY = os.environ.get("SECRET_KEY", "candidatewatch-secret-2026")
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

# External API keys
FEC_API_KEY = os.environ.get("FEC_API_KEY", "")
CONGRESS_API_KEY = os.environ.get("CONGRESS_API_KEY", "")

# == Cycle helpers ============================================================

def current_election_cycle() -> int:
    """FEC cycles are even years. 2026, 2028, etc."""
    y = datetime.utcnow().year
    return y if y % 2 == 0 else y + 1

# == Database =================================================================

def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS cw_users (
        id SERIAL PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS cw_watchlist (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        location TEXT,
        dob TEXT,
        status TEXT DEFAULT 'active',
        is_memory BOOLEAN DEFAULT FALSE,
        in_my_candidates BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES cw_users (id)
    )""")
    # Migration: add column if table existed without it
    c.execute("""ALTER TABLE cw_watchlist ADD COLUMN IF NOT EXISTS in_my_candidates BOOLEAN DEFAULT FALSE""")
    c.execute("""CREATE TABLE IF NOT EXISTS cw_notifications (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        watchlist_id INTEGER NOT NULL,
        message TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES cw_users (id),
        FOREIGN KEY (watchlist_id) REFERENCES cw_watchlist (id)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS cw_snapshots (
        watchlist_id INTEGER PRIMARY KEY,
        snapshot_json TEXT NOT NULL,
        captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (watchlist_id) REFERENCES cw_watchlist (id)
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

# == Models ===================================================================

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

# == App ======================================================================

app = FastAPI(title="Candidate Watch API", version="0.3.6")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")

# == Auth helpers =============================================================

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

# == Health ===================================================================

@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat(),
            "version": "0.3.6", "app": "Candidate Watch",
            "fec_configured": bool(FEC_API_KEY),
            "congress_configured": bool(CONGRESS_API_KEY),
            "cycle": current_election_cycle()}

# == Auth =====================================================================

@app.post("/auth/register", response_model=Token)
async def register(user: UserCreate):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM cw_users WHERE email = %s", (user.email,))
        if c.fetchone():
            raise HTTPException(status_code=400, detail="Email already registered")
        c.execute("INSERT INTO cw_users (email, password_hash) VALUES (%s, %s) RETURNING id",
                  (user.email, hash_password(user.password)))
        user_id = c.fetchone()[0]
        conn.commit()
        return {"access_token": create_access_token({"sub": str(user_id)}), "token_type": "bearer"}

@app.post("/auth/login", response_model=Token)
async def login(user: UserLogin):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id, password_hash FROM cw_users WHERE email = %s", (user.email,))
        result = c.fetchone()
        if not result or not verify_password(user.password, result[1]):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        return {"access_token": create_access_token({"sub": str(result[0])}), "token_type": "bearer"}

@app.delete("/account")
async def delete_account(user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""DELETE FROM cw_snapshots WHERE watchlist_id IN
                     (SELECT id FROM cw_watchlist WHERE user_id = %s)""", (user_id,))
        c.execute("DELETE FROM cw_notifications WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM cw_watchlist WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM cw_users WHERE id = %s", (user_id,))
        conn.commit()
        return {"message": "Account permanently deleted"}

# == Watchlist ================================================================

@app.get("/watchlist")
async def get_watchlist(user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""SELECT id, name, location, dob, status, created_at, is_memory, in_my_candidates
                     FROM cw_watchlist WHERE user_id = %s AND status = 'active'
                     ORDER BY created_at DESC""", (user_id,))
        return [{"id": r[0], "name": r[1], "location": r[2], "dob": r[3],
                 "status": r[4], "created_at": str(r[5]),
                 "is_memory": r[6] or False,
                 "in_my_candidates": r[7] or False}
                for r in c.fetchall()]

@app.post("/watchlist")
async def add_to_watchlist(item: WatchlistItem, user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        # Reject duplicates within same user
        c.execute("""SELECT id FROM cw_watchlist
                     WHERE user_id = %s AND status = 'active' AND LOWER(name) = LOWER(%s)""",
                  (user_id, item.name))
        if c.fetchone():
            raise HTTPException(status_code=400, detail=item.name + " is already on your watchlist")
        c.execute("""INSERT INTO cw_watchlist (user_id, name, location, dob)
                     VALUES (%s, %s, %s, %s) RETURNING id""",
                  (user_id, item.name, item.location, item.dob))
        new_id = c.fetchone()[0]
        conn.commit()

    # Seed snapshot from FEC if we have a fecId in the meta.
    # v: guarded — the row is already saved+committed above. An FEC outage or
    # timeout here must NOT fail the save (previously it 500'd as "Failed to
    # save" on a candidate that was actually saved). Snapshot is best-effort.
    try:
        snap = build_alert_snapshot(item.location)
        if snap is not None:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("""INSERT INTO cw_snapshots (watchlist_id, snapshot_json, captured_at)
                             VALUES (%s, %s, CURRENT_TIMESTAMP)
                             ON CONFLICT (watchlist_id) DO UPDATE
                             SET snapshot_json = EXCLUDED.snapshot_json,
                                 captured_at = CURRENT_TIMESTAMP""",
                          (new_id, json_lib.dumps(snap)))
                conn.commit()
    except Exception as e:
        print("[watchlist] snapshot seed failed (save still ok):", e)

    return {"id": new_id, "name": item.name, "location": item.location, "dob": item.dob}

@app.delete("/watchlist/{item_id}")
async def remove_from_watchlist(item_id: int, user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("UPDATE cw_watchlist SET status = 'deleted' WHERE id = %s AND user_id = %s",
                  (item_id, user_id))
        if c.rowcount == 0:
            conn.rollback()
            raise HTTPException(status_code=404, detail="Item not found")
        c.execute("DELETE FROM cw_snapshots WHERE watchlist_id = %s", (item_id,))
        conn.commit()
        return {"message": "Removed"}

@app.get("/watchlist/{item_id}/refresh")
async def refresh_watchlist_item(item_id: int, user_id: int = Depends(get_current_user)):
    """Manual on-demand refresh of a single watchlist item.
    Re-fetches FEC, diffs against snapshot, writes any alerts, updates snapshot."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""SELECT id, location FROM cw_watchlist
                     WHERE id = %s AND user_id = %s AND status = 'active'""",
                  (item_id, user_id))
        row = c.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Not found")
    new_snap = build_alert_snapshot(row[1])
    if new_snap is None:
        return {"changed": False, "reason": "No FEC id linked or fetch failed"}

    # Read prior, diff, write alerts, upsert snapshot
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT snapshot_json FROM cw_snapshots WHERE watchlist_id = %s", (item_id,))
        prior = c.fetchone()
    old_snap = None
    if prior and prior[0]:
        try:
            old_snap = json_lib.loads(prior[0])
        except Exception:
            old_snap = None

    alerts = diff_snapshots(old_snap, new_snap)
    with get_db() as conn:
        c = conn.cursor()
        for msg in alerts:
            c.execute("""INSERT INTO cw_notifications (user_id, watchlist_id, message)
                         VALUES (%s, %s, %s)""", (user_id, item_id, msg))
        c.execute("""INSERT INTO cw_snapshots (watchlist_id, snapshot_json, captured_at)
                     VALUES (%s, %s, CURRENT_TIMESTAMP)
                     ON CONFLICT (watchlist_id) DO UPDATE
                     SET snapshot_json = EXCLUDED.snapshot_json,
                         captured_at = CURRENT_TIMESTAMP""",
                  (item_id, json_lib.dumps(new_snap)))
        conn.commit()

    return {"changed": len(alerts) > 0, "alerts": alerts, "snapshot": new_snap}

@app.post("/watchlist/{item_id}/promote")
async def promote_to_my_candidates(item_id: int, user_id: int = Depends(get_current_user)):
    """Flip in_my_candidates flag for an item already on the watchlist."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""UPDATE cw_watchlist SET in_my_candidates = TRUE
                     WHERE id = %s AND user_id = %s AND status = 'active'""",
                  (item_id, user_id))
        if c.rowcount == 0:
            conn.rollback()
            raise HTTPException(status_code=404, detail="Not found")
        conn.commit()
    return {"promoted": True, "id": item_id}

# == Notifications ============================================================

@app.get("/notifications")
async def get_notifications(user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""SELECT n.id, n.message, n.created_at, w.name, n.watchlist_id
                     FROM cw_notifications n
                     JOIN cw_watchlist w ON n.watchlist_id = w.id
                     WHERE n.user_id = %s
                     ORDER BY n.created_at DESC LIMIT 50""", (user_id,))
        return [{"id": r[0], "name": r[3], "message": r[1],
                 "created_at": str(r[2]), "watchlist_id": r[4]}
                for r in c.fetchall()]

@app.delete("/notifications/{notif_id}")
async def delete_notification(notif_id: int, user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM cw_notifications WHERE id = %s AND user_id = %s",
                  (notif_id, user_id))
        conn.commit()
        return {"deleted": True}

# == FEC OpenAPI client =======================================================

FEC_BASE = "https://api.open.fec.gov/v1"

def fetch_url(url: str, timeout: int = 15):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "CandidateWatch/0.2 (+https://candidatewatch.app)",
            "Accept": "application/json, text/plain, */*",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json_lib.loads(resp.read().decode())
    except Exception as e:
        print("[fetch] error " + url + ": " + str(e))
        return None

def fec_get(path: str, params: dict, timeout: int = 15) -> Optional[dict]:
    if not FEC_API_KEY:
        return None
    p = dict(params or {})
    p["api_key"] = FEC_API_KEY
    url = FEC_BASE + path + "?" + urllib.parse.urlencode(p, doseq=True)
    return fetch_url(url, timeout=timeout)

def fec_search_candidates(name: Optional[str] = None, office: str = "S",
                          state: Optional[str] = None, district: Optional[str] = None,
                          cycle: Optional[int] = None, limit: int = 20) -> list:
    params = {
        "office": office,
        "per_page": limit,
    }
    if name:
        params["name"] = name
    if state:
        params["state"] = state
    # election_year handling: pass-through if explicitly given; for nameless lookups
    # FEC needs a year bound to return results, so default to current cycle.
    if cycle:
        params["election_year"] = cycle
    elif not name:
        params["election_year"] = current_election_cycle()
    data = fec_get("/candidates/", params)
    results = (data.get("results", []) or []) if data else []
    # FEC's /candidates/ endpoint does not accept district as a filter parameter.
    # Filter client-side, zero-padded to two digits ("11" -> "11", "1" -> "01").
    if district:
        d = str(district).zfill(2)
        results = [r for r in results if str(r.get("district") or "") == d]
    return results

def fec_candidate_detail(candidate_id: str, cycle: Optional[int] = None) -> dict:
    cycle = cycle or current_election_cycle()
    data = fec_get("/candidate/" + candidate_id + "/", {"cycle": cycle})
    if not data:
        return {}
    results = data.get("results", []) or []
    return results[0] if results else {}

def fec_candidate_totals(candidate_id: str, cycle: Optional[int] = None) -> dict:
    cycle = cycle or current_election_cycle()
    data = fec_get("/candidate/" + candidate_id + "/totals/",
                   {"cycle": cycle, "election_full": "true", "per_page": 1})
    if not data:
        return {}
    results = data.get("results", []) or []
    return results[0] if results else {}

def fec_principal_committee(candidate_id: str, cycle: Optional[int] = None) -> dict:
    cycle = cycle or current_election_cycle()
    data = fec_get("/candidate/" + candidate_id + "/committees/",
                   {"cycle": cycle, "designation": "P", "per_page": 1})
    if not data:
        return {}
    results = data.get("results", []) or []
    return results[0] if results else {}

def fec_committee_filings(committee_id: str, cycle: Optional[int] = None, limit: int = 5) -> list:
    cycle = cycle or current_election_cycle()
    data = fec_get("/committee/" + committee_id + "/filings/",
                   {"cycle": cycle, "per_page": limit, "sort": "-receipt_date"})
    return (data.get("results", []) or []) if data else []

def fec_candidates_by_state(office: str, state: str, cycle: Optional[int] = None) -> list:
    cycle = cycle or current_election_cycle()
    params = {
        "office": office,
        "state": state,
        "election_year": cycle,
        "per_page": 50,
    }
    data = fec_get("/candidates/", params)
    return (data.get("results", []) or []) if data else []

# == Congress.gov client ======================================================

CONGRESS_BASE = "https://api.congress.gov/v3"

def congress_get(path: str, params: Optional[dict] = None, timeout: int = 15) -> Optional[dict]:
    if not CONGRESS_API_KEY:
        return None
    p = dict(params or {})
    p["api_key"] = CONGRESS_API_KEY
    p.setdefault("format", "json")
    url = CONGRESS_BASE + path + "?" + urllib.parse.urlencode(p, doseq=True)
    return fetch_url(url, timeout=timeout)

def congress_member(bioguide_id: str) -> dict:
    data = congress_get("/member/" + bioguide_id, {})
    return (data.get("member") or {}) if data else {}

def congress_sponsored(bioguide_id: str, limit: int = 10) -> list:
    data = congress_get("/member/" + bioguide_id + "/sponsored-legislation",
                        {"limit": limit})
    return (data.get("sponsoredLegislation") or []) if data else []

def congress_cosponsored(bioguide_id: str, limit: int = 10) -> list:
    data = congress_get("/member/" + bioguide_id + "/cosponsored-legislation",
                        {"limit": limit})
    return (data.get("cosponsoredLegislation") or []) if data else []

def congress_current_congress_number() -> int:
    """Current Congress number. 119th = Jan 2025 - Jan 2027. Each Congress = 2 years."""
    year = datetime.now().year
    # 119th Congress runs 2025-2026; 120th runs 2027-2028, etc.
    return 119 + ((year - 2025) // 2)

def congress_member_by_district(state: str, district: str) -> Optional[dict]:
    """Direct lookup of current House member for a state+district via the dedicated endpoint."""
    if not state or not district:
        return None
    cong = congress_current_congress_number()
    d = str(district).lstrip("0") or "0"
    data = congress_get("/member/congress/" + str(cong) + "/" + state.upper() + "/" + d, {"currentMember": "true"})
    members = (data.get("members") or []) if data else []
    return members[0] if members else None

def congress_current_senators_for_state(state: str) -> list:
    """Current senators from a state. Pulls state-list and filters to Senate via terms."""
    if not state:
        return []
    data = congress_get("/member/" + state.upper(), {"currentMember": "true", "limit": 50})
    members = (data.get("members") or []) if data else []
    out = []
    for m in members:
        terms = m.get("terms") or {}
        term_list = terms.get("item") if isinstance(terms, dict) else terms
        last = None
        if isinstance(term_list, list) and term_list:
            last = term_list[-1]
        elif isinstance(term_list, dict):
            last = term_list
        chamber = (last.get("chamber") if last else "") or ""
        if "senate" in chamber.lower():
            out.append(m)
    return out

# == FEC search & profile endpoints ===========================================

@app.get("/fec/search")
async def fec_search(name: Optional[str] = None, office: str = "S",
                     state: Optional[str] = None, district: Optional[str] = None,
                     cycle: Optional[int] = None, limit: int = 20):
    """Search FEC candidates by name, or by office/state/district (e.g., for ZIP-derived district lookup). Default office=S (Senate)."""
    if not FEC_API_KEY:
        raise HTTPException(status_code=503, detail="FEC_API_KEY not configured")
    has_name = bool(name and len(name.strip()) >= 2)
    has_district = bool(state and district)
    if not has_name and not has_district:
        raise HTTPException(status_code=400, detail="provide name (>= 2 chars) or state + district")
    results = fec_search_candidates(
        name=name.strip() if has_name else None,
        office=office, state=state, district=district,
        cycle=cycle, limit=limit,
    )
    out = []
    for r in results:
        out.append({
            "candidate_id": r.get("candidate_id"),
            "name": r.get("name"),
            "party": r.get("party_full") or r.get("party"),
            "office": r.get("office_full") or r.get("office"),
            "state": r.get("state"),
            "district": r.get("district"),
            "incumbent_challenge": r.get("incumbent_challenge_full") or r.get("incumbent_challenge"),
            "cycle": (r.get("election_years") or [None])[-1] if r.get("election_years") else None,
            "principal_committee": (r.get("principal_committees") or [{}])[0].get("committee_id") if r.get("principal_committees") else None,
        })
    return {"results": out, "cycle": cycle or current_election_cycle()}

@app.get("/fec/candidate/{candidate_id}")
async def fec_candidate(candidate_id: str, cycle: Optional[int] = None):
    """Full candidate profile: bio, totals, principal committee, recent filings."""
    if not FEC_API_KEY:
        raise HTTPException(status_code=503, detail="FEC_API_KEY not configured")
    cycle = cycle or current_election_cycle()
    detail = fec_candidate_detail(candidate_id, cycle=cycle)
    if not detail:
        raise HTTPException(status_code=404, detail="Candidate not found")
    totals = fec_candidate_totals(candidate_id, cycle=cycle) or {}
    pc = fec_principal_committee(candidate_id, cycle=cycle) or {}
    filings = []
    if pc.get("committee_id"):
        filings = fec_committee_filings(pc["committee_id"], cycle=cycle, limit=5) or []
    return {
        "candidate": {
            "candidate_id": detail.get("candidate_id"),
            "name": detail.get("name"),
            "party": detail.get("party_full") or detail.get("party"),
            "office": detail.get("office_full") or detail.get("office"),
            "state": detail.get("state"),
            "district": detail.get("district"),
            "incumbent_challenge": detail.get("incumbent_challenge_full"),
            "active_through": detail.get("active_through"),
        },
        "totals": {
            "cycle": cycle,
            "receipts": totals.get("receipts"),
            "disbursements": totals.get("disbursements"),
            "cash_on_hand_end_period": totals.get("cash_on_hand_end_period"),
            "debts_owed_by_committee": totals.get("debts_owed_by_committee"),
            "individual_contributions": totals.get("individual_contributions"),
            "other_political_committee_contributions": totals.get("other_political_committee_contributions"),
            "coverage_end_date": totals.get("coverage_end_date"),
        },
        "principal_committee": {
            "committee_id": pc.get("committee_id"),
            "name": pc.get("name"),
        } if pc else None,
        "recent_filings": [
            {
                "filing_id": f.get("file_number") or f.get("sub_id"),
                "form_type": f.get("form_type"),
                "receipt_date": f.get("receipt_date"),
                "coverage_end_date": f.get("coverage_end_date"),
                "total_receipts_period": f.get("total_receipts_period"),
                "total_disbursements_period": f.get("total_disbursements_period"),
            }
            for f in filings
        ],
    }

@app.get("/fec/race")
async def fec_race(office: str, state: str, district: Optional[str] = None,
                   cycle: Optional[int] = None):
    """All candidates in a given race (Senate state, House state+district)."""
    if not FEC_API_KEY:
        raise HTTPException(status_code=503, detail="FEC_API_KEY not configured")
    if office.upper() not in ("S", "H"):
        raise HTTPException(status_code=400, detail="office must be S or H")
    cycle = cycle or current_election_cycle()
    results = fec_candidates_by_state(office.upper(), state.upper(), cycle=cycle)
    if office.upper() == "H" and district:
        results = [r for r in results if str(r.get("district") or "") == str(district).zfill(2)]
    out = []
    for r in results:
        out.append({
            "candidate_id": r.get("candidate_id"),
            "name": r.get("name"),
            "party": r.get("party_full") or r.get("party"),
            "incumbent_challenge": r.get("incumbent_challenge_full") or r.get("incumbent_challenge"),
            "state": r.get("state"),
            "district": r.get("district"),
            "principal_committee": (r.get("principal_committees") or [{}])[0].get("committee_id") if r.get("principal_committees") else None,
        })
    return {"office": office.upper(), "state": state.upper(),
            "district": district, "cycle": cycle, "candidates": out}

# == Congress.gov endpoints (incumbents) ======================================

@app.get("/congress/lookup")
async def congress_lookup(office: str, state: str, district: Optional[str] = None):
    """Resolve an incumbent's bioguide_id from FEC office/state/district.

    Senate: office=S&state=XX returns the first matching current senator.
    House:  office=H&state=XX&district=NN returns the rep for that district (direct).
    """
    if not CONGRESS_API_KEY:
        raise HTTPException(status_code=503, detail="CONGRESS_API_KEY not configured")
    o_raw = (office or "").upper().strip()
    # Accept "H"/"S" or "HOUSE"/"SENATE" -- FEC returns the full-word form
    if o_raw in ("HOUSE", "H"):
        o = "H"
    elif o_raw in ("SENATE", "S"):
        o = "S"
    else:
        raise HTTPException(status_code=400, detail="office must be S or H")
    st = (state or "").upper().strip()
    if not st:
        raise HTTPException(status_code=400, detail="state required")
    chosen = None
    if o == "H":
        if not district:
            raise HTTPException(status_code=400, detail="district required for House lookup")
        chosen = congress_member_by_district(st, district)
    else:
        senators = congress_current_senators_for_state(st)
        if senators:
            chosen = senators[0]
    if not chosen:
        raise HTTPException(status_code=404, detail="no current incumbent matched")
    # Pull a sensible district display value for the response (Senate has none)
    out_district = None
    if o == "H" and district:
        try:
            out_district = str(int(district)).zfill(2)
        except Exception:
            out_district = district
    return {
        "bioguide_id": chosen.get("bioguideId"),
        "name": chosen.get("name") or chosen.get("directOrderName") or chosen.get("invertedOrderName"),
        "state": chosen.get("state") or st,
        "party": chosen.get("partyName"),
        "office": o,
        "district": out_district,
    }

@app.get("/congress/member/{bioguide_id}")
async def congress_member_endpoint(bioguide_id: str):
    """Bio + recent sponsored/cosponsored legislation for an incumbent."""
    if not CONGRESS_API_KEY:
        raise HTTPException(status_code=503, detail="CONGRESS_API_KEY not configured")
    bio = congress_member(bioguide_id) or {}
    sponsored = congress_sponsored(bioguide_id, limit=10) or []
    cosponsored = congress_cosponsored(bioguide_id, limit=10) or []
    return {
        "member": {
            "bioguide_id": bio.get("bioguideId") or bioguide_id,
            "name": bio.get("directOrderName") or bio.get("invertedOrderName"),
            "state": bio.get("state"),
            "party": (bio.get("partyHistory") or [{}])[-1].get("partyName") if bio.get("partyHistory") else bio.get("partyName"),
            "depiction": bio.get("depiction") or {},
            "terms": bio.get("terms") or [],
            "honorific": bio.get("honorificName"),
        },
        "sponsored": [
            {
                "title": b.get("title"),
                "type": b.get("type"),
                "number": b.get("number"),
                "introduced_date": b.get("introducedDate"),
                "latest_action": (b.get("latestAction") or {}).get("text"),
                "latest_action_date": (b.get("latestAction") or {}).get("actionDate"),
            }
            for b in sponsored
        ],
        "cosponsored": [
            {
                "title": b.get("title"),
                "type": b.get("type"),
                "number": b.get("number"),
                "introduced_date": b.get("introducedDate"),
            }
            for b in cosponsored
        ],
    }

# == Snapshot + diff (cron core) ==============================================

def build_alert_snapshot(meta_json):
    """Fetch FEC + Congress data for a watched candidate. Return a small
    alert-relevant snapshot. Returns None if we can't reach FEC or meta is missing."""
    if not meta_json:
        return None
    try:
        meta = json_lib.loads(meta_json)
    except Exception:
        return None
    fec_id = meta.get("fecId") or meta.get("candidate_id")
    if not fec_id:
        return None

    cycle = current_election_cycle()
    detail = fec_candidate_detail(fec_id, cycle=cycle) or {}
    totals = fec_candidate_totals(fec_id, cycle=cycle) or {}
    pc = fec_principal_committee(fec_id, cycle=cycle) or {}

    last_filing_id = ""
    last_filing_date = ""
    if pc.get("committee_id"):
        fls = fec_committee_filings(pc["committee_id"], cycle=cycle, limit=1) or []
        if fls:
            last_filing_id = str(fls[0].get("file_number") or fls[0].get("sub_id") or "")
            last_filing_date = str(fls[0].get("receipt_date") or "")

    snap = {
        "candidate_id": fec_id,
        "cycle": cycle,
        "incumbent_challenge": (detail.get("incumbent_challenge_full") or "").strip(),
        "last_filing_id": last_filing_id,
        "last_filing_date": last_filing_date,
        "receipts": totals.get("receipts"),
        "disbursements": totals.get("disbursements"),
        "cash_on_hand": totals.get("cash_on_hand_end_period"),
        "debts": totals.get("debts_owed_by_committee"),
    }

    # For incumbents with a bioguideId, capture latest sponsored bill
    bio_id = meta.get("bioguideId")
    is_incumbent = (snap["incumbent_challenge"] or "").lower().find("incumbent") >= 0
    if is_incumbent and bio_id and CONGRESS_API_KEY:
        sp = congress_sponsored(bio_id, limit=1) or []
        if sp:
            b = sp[0]
            snap["latest_bill"] = {
                "type": b.get("type") or "",
                "number": str(b.get("number") or ""),
                "title": (b.get("title") or "")[:200],
                "introduced_date": b.get("introducedDate") or "",
            }
        else:
            snap["latest_bill"] = None

    return snap

def fmt_money(v) -> str:
    try:
        n = float(v or 0)
    except Exception:
        return "$0"
    return "${:,.0f}".format(n)

def diff_snapshots(old, new):
    """Compare two snapshots. Returns list of short alert messages.
    First-run (old is None) returns [] -- never alert on the first capture."""
    if old is None or new is None:
        return []
    alerts = []

    # New FEC filing
    old_fid = old.get("last_filing_id") or ""
    new_fid = new.get("last_filing_id") or ""
    if new_fid and new_fid != old_fid:
        date_part = new.get("last_filing_date") or ""
        receipts_part = fmt_money(new.get("receipts"))
        msg = "New FEC filing posted"
        if date_part:
            msg += " (" + date_part + ")"
        msg += ". Cycle receipts now " + receipts_part + "."
        alerts.append(msg)

    # Incumbent challenge status change (e.g. challenger -> incumbent after winning)
    old_ic = (old.get("incumbent_challenge") or "").lower()
    new_ic = (new.get("incumbent_challenge") or "").lower()
    if old_ic and new_ic and old_ic != new_ic:
        alerts.append("Status changed: " + old.get("incumbent_challenge", "") + " -> " + new.get("incumbent_challenge", ""))

    # New sponsored bill (incumbents)
    old_bill = old.get("latest_bill") or {}
    new_bill = new.get("latest_bill") or {}
    old_num = (old_bill.get("type") or "") + (old_bill.get("number") or "")
    new_num = (new_bill.get("type") or "") + (new_bill.get("number") or "")
    if new_num and new_num != old_num:
        title = new_bill.get("title") or ""
        if len(title) > 120:
            title = title[:117] + "..."
        alerts.append("New bill sponsored: " + new_bill.get("type", "") + " "
                      + new_bill.get("number", "") + " -- " + title)

    return alerts

# == Daily watchlist cron =====================================================

def check_all_watched_candidates():
    """Daily job: iterate active watchlist, fetch FEC, diff, write notifications."""
    print("[cron] Starting daily watchlist check at " + datetime.now().isoformat())
    if not FEC_API_KEY:
        print("[cron] Skipped: FEC_API_KEY not configured")
        return
    checked = 0
    alerted = 0
    skipped = 0
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""SELECT id, user_id, name, location FROM cw_watchlist
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

            # Read prior
            with get_db() as conn:
                c = conn.cursor()
                c.execute("SELECT snapshot_json FROM cw_snapshots WHERE watchlist_id = %s",
                          (watchlist_id,))
                prior = c.fetchone()
            old_snap = None
            if prior and prior[0]:
                try:
                    old_snap = json_lib.loads(prior[0])
                except Exception:
                    old_snap = None

            alerts = diff_snapshots(old_snap, new_snap)
            if alerts:
                with get_db() as conn:
                    c = conn.cursor()
                    for msg in alerts:
                        c.execute("""INSERT INTO cw_notifications
                                     (user_id, watchlist_id, message)
                                     VALUES (%s, %s, %s)""",
                                  (user_id, watchlist_id, msg))
                    conn.commit()
                alerted += 1

            # Always upsert snapshot
            with get_db() as conn:
                c = conn.cursor()
                c.execute("""INSERT INTO cw_snapshots (watchlist_id, snapshot_json, captured_at)
                             VALUES (%s, %s, CURRENT_TIMESTAMP)
                             ON CONFLICT (watchlist_id) DO UPDATE
                             SET snapshot_json = EXCLUDED.snapshot_json,
                                 captured_at = CURRENT_TIMESTAMP""",
                          (watchlist_id, json_lib.dumps(new_snap)))
                conn.commit()

        print("[cron] Done. Checked " + str(checked)
              + " candidates, " + str(alerted) + " with new alerts, "
              + str(skipped) + " skipped (no FEC id or fetch failed)")
    except Exception as e:
        print("[cron] Fatal error: " + str(e))


@app.post("/admin/run-cron")
async def run_cron_manually(secret: str):
    """Manually trigger the daily check. Pass ?secret=... matching ADMIN_SECRET."""
    if secret != os.environ.get("ADMIN_SECRET", "candidatewatch-cron-2026"):
        raise HTTPException(status_code=403, detail="Forbidden")
    check_all_watched_candidates()
    return {"status": "completed", "ran_at": datetime.now().isoformat()}


# Module-level scheduler -- kept in scope so it isn't garbage-collected
_cw_scheduler = None

@app.get("/admin/signup-stats")
async def admin_signup_stats(x_admin_token: str = Header(None, alias="X-Admin-Token")):
    """Read-only signup metrics for the 3Brains scoreboard.
    Requires X-Admin-Token header matching ADMIN_STATS_TOKEN env var."""
    expected = os.environ.get("ADMIN_STATS_TOKEN")
    if not expected or x_admin_token != expected:
        raise HTTPException(status_code=403, detail="Forbidden")
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM cw_users")
        total_users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM cw_users WHERE created_at >= NOW() - INTERVAL '24 hours'")
        signups_24h = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM cw_users WHERE created_at >= NOW() - INTERVAL '7 days'")
        signups_7d = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM cw_users WHERE created_at >= NOW() - INTERVAL '30 days'")
        signups_30d = c.fetchone()[0]
        c.execute("SELECT MAX(created_at) FROM cw_users")
        latest_row = c.fetchone()
        latest = latest_row[0].isoformat() if latest_row and latest_row[0] else None
        return {
            "total_users": total_users,
            "signups_24h": signups_24h,
            "signups_7d": signups_7d,
            "signups_30d": signups_30d,
            "latest_signup_at": latest
        }


# == Startup ==================================================================

@app.on_event("startup")
async def startup_event():
    init_db()
    print("Candidate Watch DB initialized")

    # Schedule daily watchlist check at 14:00 UTC
    # = 4am HST / 9am EST winter / 10am EDT summer
    global _cw_scheduler
    _cw_scheduler = BackgroundScheduler(timezone="UTC")
    _cw_scheduler.add_job(
        check_all_watched_candidates,
        CronTrigger(hour=14, minute=0),
        id="cw_daily_check",
        replace_existing=True,
    )
    _cw_scheduler.start()
    print("Candidate Watch cron scheduled: daily at 14:00 UTC. Cycle=" + str(current_election_cycle()))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

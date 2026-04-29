from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from datetime import datetime, timedelta
from typing import Optional, List
import jwt
import bcrypt
import psycopg2
import psycopg2.extras
import os
import schedule
import threading
import time
import re
import html as html_lib
import httpx
import urllib.request
import urllib.parse
import urllib.error
import json as json_lib
from contextlib import contextmanager
from google.cloud import bigquery
from google.oauth2 import service_account

# ── Config from environment ───────────────────────────────────────────────────
SECRET_KEY = os.environ.get("JWT_SECRET", os.environ.get("SECRET_KEY", "cruiseship-watch-fallback-key"))
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 10080
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
FROM_EMAIL = os.environ.get("FROM_EMAIL", "alerts@cruiseship.watch")

VERSION = "0.1.5"

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL environment variable not set. "
        "In Render, link a Postgres database to this service, or set DATABASE_URL manually."
    )
# Render sometimes uses postgres:// (legacy). psycopg2 accepts both, but normalize for safety.
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# ── Region taxonomy ───────────────────────────────────────────────────────────
# Canonical buckets: Caribbean, Mediterranean, Northern Europe, North America,
# Central America, South America, Asia, Oceania, Middle East, Africa, Arctic.
# ISO 3166-1 alpha-2 codes mapped to the cruise region most relevant for itinerary alerts.

COUNTRY_TO_REGION = {
    # State Dept uses FIPS-style country codes, not ISO-3166.
    # Mapping below covers all advisory countries observed in /admin/advisories.

    # Caribbean
    "AV": "Caribbean",  # Anguilla
    "AC": "Caribbean",  # Antigua and Barbuda
    "AA": "Caribbean",  # Aruba
    "BF": "Caribbean",  # Bahamas
    "BB": "Caribbean",  # Barbados
    "VI": "Caribbean",  # British Virgin Islands
    "CJ": "Caribbean",  # Cayman Islands
    "CU": "Caribbean",  # Cuba
    "DO": "Caribbean",  # Dominica
    "DR": "Caribbean",  # Dominican Republic
    "GJ": "Caribbean",  # Grenada
    "GP": "Caribbean",  # Guadeloupe
    "HA": "Caribbean",  # Haiti
    "JM": "Caribbean",  # Jamaica
    "MB": "Caribbean",  # Martinique
    "MH": "Caribbean",  # Montserrat
    "RQ": "Caribbean",  # Puerto Rico
    "TB": "Caribbean",  # St. Barthelemy
    "SC": "Caribbean",  # St. Kitts and Nevis
    "ST": "Caribbean",  # St. Lucia
    "RN": "Caribbean",  # St. Martin
    "VC": "Caribbean",  # St. Vincent and the Grenadines
    "NN": "Caribbean",  # Sint Maarten
    "TD": "Caribbean",  # Trinidad and Tobago
    "TK": "Caribbean",  # Turks and Caicos
    "VQ": "Caribbean",  # US Virgin Islands

    # Central America (Mexico grouped here for cruise itinerary purposes)
    "BH": "Central America",  # Belize
    "CS": "Central America",  # Costa Rica
    "ES": "Central America",  # El Salvador
    "GT": "Central America",  # Guatemala
    "HO": "Central America",  # Honduras
    "NU": "Central America",  # Nicaragua
    "PM": "Central America",  # Panama
    "MX": "Central America",  # Mexico

    # North America
    "US": "North America",  # United States
    "CA": "North America",  # Canada
    "BD": "North America",  # Bermuda

    # South America
    "AR": "South America",  # Argentina
    "BL": "South America",  # Bolivia
    "BR": "South America",  # Brazil
    "CI": "South America",  # Chile
    "CO": "South America",  # Colombia
    "EC": "South America",  # Ecuador
    "GY": "South America",  # Guyana
    "PA": "South America",  # Paraguay
    "PE": "South America",  # Peru
    "NS": "South America",  # Suriname
    "UY": "South America",  # Uruguay
    "VE": "South America",  # Venezuela
    "FK": "South America",  # Falkland Islands
    "FG": "South America",  # French Guiana

    # Mediterranean (incl. Iberian Atlantic and North Africa Med coast)
    "SP": "Mediterranean",  # Spain
    "FR": "Mediterranean",  # France
    "IT": "Mediterranean",  # Italy
    "GR": "Mediterranean",  # Greece
    "TU": "Mediterranean",  # Turkey
    "HR": "Mediterranean",  # Croatia
    "MT": "Mediterranean",  # Malta
    "CY": "Mediterranean",  # Cyprus
    "MN": "Mediterranean",  # Monaco
    "AL": "Mediterranean",  # Albania
    "MJ": "Mediterranean",  # Montenegro
    "SI": "Mediterranean",  # Slovenia
    "BK": "Mediterranean",  # Bosnia and Herzegovina
    "TS": "Mediterranean",  # Tunisia
    "AG": "Mediterranean",  # Algeria
    "MO": "Mediterranean",  # Morocco
    "EG": "Mediterranean",  # Egypt
    "IS": "Mediterranean",  # Israel
    "PO": "Mediterranean",  # Portugal
    "GI": "Mediterranean",  # Gibraltar
    "VT": "Mediterranean",  # Vatican
    "SM": "Mediterranean",  # San Marino
    "AN": "Mediterranean",  # Andorra
    "MK": "Mediterranean",  # North Macedonia
    "RI": "Mediterranean",  # Serbia
    "BU": "Mediterranean",  # Bulgaria
    "RO": "Mediterranean",  # Romania
    "GG": "Mediterranean",  # Georgia (Caucasus regional grouping)

    # Northern Europe (incl. British Isles, Baltic, Scandinavia)
    "UK": "Northern Europe",  # United Kingdom
    "EI": "Northern Europe",  # Ireland
    "IC": "Northern Europe",  # Iceland
    "NO": "Northern Europe",  # Norway
    "SW": "Northern Europe",  # Sweden
    "FI": "Northern Europe",  # Finland
    "DA": "Northern Europe",  # Denmark
    "GM": "Northern Europe",  # Germany
    "NL": "Northern Europe",  # Netherlands
    "BE": "Northern Europe",  # Belgium
    "EN": "Northern Europe",  # Estonia
    "LG": "Northern Europe",  # Latvia
    "LH": "Northern Europe",  # Lithuania
    "PL": "Northern Europe",  # Poland
    "RS": "Northern Europe",  # Russia
    "FO": "Northern Europe",  # Faroe Islands
    "JE": "Northern Europe",  # Jersey
    "GK": "Northern Europe",  # Guernsey
    "IM": "Northern Europe",  # Isle of Man
    "LU": "Northern Europe",  # Luxembourg
    "AU": "Northern Europe",  # Austria
    "SZ": "Northern Europe",  # Switzerland
    "EZ": "Northern Europe",  # Czech Republic
    "LO": "Northern Europe",  # Slovakia
    "HU": "Northern Europe",  # Hungary
    "UP": "Northern Europe",  # Ukraine
    "BO": "Northern Europe",  # Belarus
    "MD": "Northern Europe",  # Moldova
    "LS": "Northern Europe",  # Liechtenstein
    "AM": "Northern Europe",  # Armenia
    "AJ": "Northern Europe",  # Azerbaijan

    # Asia
    "JA": "Asia",  # Japan
    "KS": "Asia",  # South Korea
    "KN": "Asia",  # North Korea
    "CH": "Asia",  # China
    "TW": "Asia",  # Taiwan
    "HK": "Asia",  # Hong Kong
    "MC": "Asia",  # Macau
    "VM": "Asia",  # Vietnam
    "TH": "Asia",  # Thailand
    "MY": "Asia",  # Malaysia
    "SN": "Asia",  # Singapore
    "ID": "Asia",  # Indonesia
    "RP": "Asia",  # Philippines
    "BX": "Asia",  # Brunei
    "CB": "Asia",  # Cambodia
    "BM": "Asia",  # Burma (Myanmar)
    "LA": "Asia",  # Laos
    "IN": "Asia",  # India
    "CE": "Asia",  # Sri Lanka
    "BG": "Asia",  # Bangladesh
    "MV": "Asia",  # Maldives
    "PK": "Asia",  # Pakistan
    "AF": "Asia",  # Afghanistan
    "NP": "Asia",  # Nepal
    "BT": "Asia",  # Bhutan
    "MG": "Asia",  # Mongolia
    "KZ": "Asia",  # Kazakhstan
    "KG": "Asia",  # Kyrgyzstan
    "TI": "Asia",  # Tajikistan
    "TX": "Asia",  # Turkmenistan
    "UZ": "Asia",  # Uzbekistan
    "TT": "Asia",  # East Timor

    # Oceania (incl. South Pacific)
    "AS": "Oceania",  # Australia
    "NZ": "Oceania",  # New Zealand
    "FJ": "Oceania",  # Fiji
    "PP": "Oceania",  # Papua New Guinea
    "BP": "Oceania",  # Solomon Islands
    "NH": "Oceania",  # Vanuatu
    "NC": "Oceania",  # New Caledonia
    "FP": "Oceania",  # French Polynesia
    "WS": "Oceania",  # Samoa
    "TN": "Oceania",  # Tonga
    "KR": "Oceania",  # Kiribati
    "TV": "Oceania",  # Tuvalu
    "NR": "Oceania",  # Nauru
    "PS": "Oceania",  # Palau
    "FM": "Oceania",  # Micronesia
    "RM": "Oceania",  # Marshall Islands
    "CW": "Oceania",  # Cook Islands
    "NE": "Oceania",  # Niue

    # Middle East
    "AE": "Middle East",  # UAE
    "BA": "Middle East",  # Bahrain
    "KU": "Middle East",  # Kuwait
    "MU": "Middle East",  # Oman
    "QA": "Middle East",  # Qatar
    "SA": "Middle East",  # Saudi Arabia
    "YM": "Middle East",  # Yemen
    "JO": "Middle East",  # Jordan
    "IR": "Middle East",  # Iran
    "IZ": "Middle East",  # Iraq
    "SY": "Middle East",  # Syria
    "LE": "Middle East",  # Lebanon

    # Africa
    "SF": "Africa",  # South Africa
    "MZ": "Africa",  # Mozambique
    "TZ": "Africa",  # Tanzania
    "KE": "Africa",  # Kenya
    "MA": "Africa",  # Madagascar
    "MP": "Africa",  # Mauritius
    "SE": "Africa",  # Seychelles
    "RE": "Africa",  # Reunion
    "WA": "Africa",  # Namibia
    "AO": "Africa",  # Angola
    "GH": "Africa",  # Ghana
    "SG": "Africa",  # Senegal
    "CV": "Africa",  # Cape Verde
    "GA": "Africa",  # Gambia
    "DJ": "Africa",  # Djibouti
    "ER": "Africa",  # Eritrea
    "SO": "Africa",  # Somalia
    "NI": "Africa",  # Nigeria
    "IV": "Africa",  # Cote d'Ivoire
    "CM": "Africa",  # Cameroon
    "GB": "Africa",  # Gabon
    "CF": "Africa",  # Republic of the Congo
    "CG": "Africa",  # DR Congo
    "CD": "Africa",  # Chad
    "UV": "Africa",  # Burkina Faso
    "ML": "Africa",  # Mali
    "NG": "Africa",  # Niger
    "CT": "Africa",  # Central African Republic
    "OD": "Africa",  # South Sudan
    "SU": "Africa",  # Sudan
    "ET": "Africa",  # Ethiopia
    "UG": "Africa",  # Uganda
    "RW": "Africa",  # Rwanda
    "BY": "Africa",  # Burundi
    "MI": "Africa",  # Malawi
    "ZA": "Africa",  # Zambia
    "ZI": "Africa",  # Zimbabwe
    "BC": "Africa",  # Botswana
    "WZ": "Africa",  # Eswatini
    "LT": "Africa",  # Lesotho
    "LY": "Africa",  # Libya
    "GV": "Africa",  # Guinea
    "PU": "Africa",  # Guinea-Bissau
    "SL": "Africa",  # Sierra Leone
    "LI": "Africa",  # Liberia
    "TO": "Africa",  # Togo
    "BN": "Africa",  # Benin
    "EK": "Africa",  # Equatorial Guinea
    "TP": "Africa",  # Sao Tome and Principe
    "CN": "Africa",  # Comoros

    # Arctic / Antarctic
    "GL": "Arctic",  # Greenland
    "SV": "Arctic",  # Svalbard
    "AY": "Arctic",  # Antarctica
}

CANONICAL_REGIONS = {
    "Caribbean", "Mediterranean", "Northern Europe", "North America",
    "Central America", "South America", "Asia", "Oceania",
    "Middle East", "Africa", "Arctic",
}

# Common user-facing labels and shortcuts → canonical bucket.
# Lookup is case-insensitive; the keys here are stored lowercase.
REGION_ALIASES = {
    # Caribbean
    "caribbean": "Caribbean",
    "carib": "Caribbean",
    "the caribbean": "Caribbean",
    "bahamas": "Caribbean",
    "eastern caribbean": "Caribbean",
    "western caribbean": "Caribbean",
    "southern caribbean": "Caribbean",

    # Mediterranean
    "mediterranean": "Mediterranean",
    "med": "Mediterranean",
    "the med": "Mediterranean",
    "western mediterranean": "Mediterranean",
    "eastern mediterranean": "Mediterranean",
    "greek isles": "Mediterranean",
    "greek islands": "Mediterranean",
    "adriatic": "Mediterranean",

    # Northern Europe
    "northern europe": "Northern Europe",
    "north europe": "Northern Europe",
    "baltic": "Northern Europe",
    "baltics": "Northern Europe",
    "british isles": "Northern Europe",
    "british": "Northern Europe",
    "uk": "Northern Europe",
    "norway": "Northern Europe",
    "norwegian fjords": "Northern Europe",
    "fjords": "Northern Europe",
    "scandinavia": "Northern Europe",
    "iceland": "Northern Europe",
    "europe": "Northern Europe",

    # North America
    "north america": "North America",
    "alaska": "North America",
    "alaska/pnw": "North America",
    "pacific northwest": "North America",
    "pnw": "North America",
    "hawaii": "North America",
    "new england": "North America",
    "canada": "North America",
    "canada/new england": "North America",
    "bermuda": "North America",

    # Central America
    "central america": "Central America",
    "panama canal": "Central America",
    "panama": "Central America",
    "mexico": "Central America",
    "mexican riviera": "Central America",
    "riviera maya": "Central America",

    # South America
    "south america": "South America",
    "amazon": "South America",
    "patagonia": "South America",
    "cape horn": "South America",

    # Asia
    "asia": "Asia",
    "far east": "Asia",
    "southeast asia": "Asia",
    "south east asia": "Asia",
    "se asia": "Asia",
    "japan": "Asia",
    "china": "Asia",
    "india": "Asia",
    "vietnam": "Asia",
    "singapore": "Asia",
    "thailand": "Asia",

    # Oceania
    "oceania": "Oceania",
    "south pacific": "Oceania",
    "pacific": "Oceania",
    "australia": "Oceania",
    "australia/nz": "Oceania",
    "new zealand": "Oceania",
    "tahiti": "Oceania",
    "fiji": "Oceania",
    "polynesia": "Oceania",

    # Middle East
    "middle east": "Middle East",
    "persian gulf": "Middle East",
    "arabian gulf": "Middle East",
    "arabian peninsula": "Middle East",
    "red sea": "Middle East",
    "dubai": "Middle East",

    # Africa
    "africa": "Africa",
    "east africa": "Africa",
    "west africa": "Africa",
    "south africa": "Africa",
    "indian ocean": "Africa",

    # Arctic / Antarctic
    "arctic": "Arctic",
    "antarctic": "Arctic",
    "antarctica": "Arctic",
    "polar": "Arctic",
    "north pole": "Arctic",
    "south pole": "Arctic",
}


def normalize_region_label(value: Optional[str]) -> Optional[str]:
    """Map user-typed region label to a canonical bucket. Returns None if unrecognized."""
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    # Exact canonical match first (case-sensitive shortcut)
    if s in CANONICAL_REGIONS:
        return s
    key = s.lower()
    if key in REGION_ALIASES:
        return REGION_ALIASES[key]
    # Loose substring match against canonical bucket names
    for bucket in CANONICAL_REGIONS:
        if bucket.lower() == key:
            return bucket
    return None


def get_region_for_country(country_code: Optional[str]) -> Optional[str]:
    if not country_code:
        return None
    return COUNTRY_TO_REGION.get(country_code.strip().upper())


# ── Google BigQuery (SSDI — kept for cross-vertical compatibility) ─────────────

def get_bq_client():
    """Create BigQuery client from service account JSON env var."""
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not sa_json:
        raise Exception("GOOGLE_SERVICE_ACCOUNT_JSON not set in environment")
    sa_info = json_lib.loads(sa_json)
    credentials = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    return bigquery.Client(project="memorywatch-ssdi", credentials=credentials)


def run_bigquery(sql: str, params: list) -> list:
    """Run a parameterized BigQuery query. Returns list of row dicts."""
    client = get_bq_client()
    bq_params = []
    for p in params:
        bq_params.append(bigquery.ScalarQueryParameter(
            p["name"],
            p["parameterType"]["type"],
            p["parameterValue"]["value"]
        ))
    job_config = bigquery.QueryJobConfig(query_parameters=bq_params)
    query_job = client.query(sql, job_config=job_config)
    rows = query_job.result()
    return [dict(row) for row in rows]


def fmt_date(d):
    """Format date object or YYYY-MM-DD string -> M/D/YYYY. Returns None if no date."""
    if not d:
        return None
    try:
        s = str(d)
        if len(s) >= 10:
            parts = s[:10].split("-")
            if len(parts) == 3:
                return str(int(parts[1])) + "/" + str(int(parts[2])) + "/" + parts[0]
        return s if s.strip() else None
    except Exception:
        return str(d)


def parse_bq_results(rows: list) -> list:
    """Parse BigQuery row dicts into clean result dicts, filtering under-18 deaths."""
    results = []
    for record in rows:
        fname = (str(record.get("first_name") or "")).strip().title()
        mname = (str(record.get("middle_name") or "")).strip().title()
        lname = (str(record.get("last_name") or "")).strip().title()
        suffix = (str(record.get("name_suffix") or "")).strip()
        dob = record.get("dob")
        dod = record.get("dod")
        if dob and dod:
            try:
                birth_yr = int(str(dob)[:4])
                death_yr = int(str(dod)[:4])
                if birth_yr and death_yr and (death_yr - birth_yr) < 18:
                    continue
            except Exception:
                pass
        full_name = fname
        if mname:
            full_name += " " + mname
        full_name += " " + lname
        if suffix:
            full_name += " " + suffix
        results.append({
            "name": full_name.strip(),
            "birth_date": fmt_date(dob),
            "death_date": fmt_date(dod),
            "state": "",
            "source": "SSA Death Master File"
        })
    return results


# ── Shared SSDI query logic (used by both /ssdi/search and /ssdi/proxy) ────────

def run_ssdi_query(name: str, birth_year: str = None, middle_name: str = None,
                   suffix: str = None, offset: int = 0) -> dict:
    """Core SSDI BigQuery search — no auth, called by both endpoints."""
    try:
        parts = name.strip().split()
        mid = (middle_name or "").strip().upper().rstrip(".")
        page_size = 10

        if len(parts) == 1:
            last = parts[0].upper()
            query = (
                "SELECT first_name, middle_name, last_name, name_suffix, dob, dod "
                "FROM `fiat-fiendum.ssdmf.ssdmf_most_recent` "
                "WHERE UPPER(last_name) = @lname"
            )
            query_params = [
                {"name": "lname", "parameterType": {"type": "STRING"}, "parameterValue": {"value": last}},
            ]
        else:
            first = parts[0].upper()
            last = parts[-1].upper()
            query = (
                "SELECT first_name, middle_name, last_name, name_suffix, dob, dod "
                "FROM `fiat-fiendum.ssdmf.ssdmf_most_recent` "
                "WHERE UPPER(last_name) = @lname "
                "AND UPPER(first_name) LIKE @fname"
            )
            query_params = [
                {"name": "lname", "parameterType": {"type": "STRING"}, "parameterValue": {"value": last}},
                {"name": "fname", "parameterType": {"type": "STRING"}, "parameterValue": {"value": first + "%"}},
            ]

        if mid:
            query += " AND UPPER(middle_name) LIKE @mname"
            query_params.append({
                "name": "mname",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": mid + "%"}
            })

        if birth_year:
            query += " AND CAST(EXTRACT(YEAR FROM dob) AS STRING) = @birth_year"
            query_params.append({
                "name": "birth_year",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": birth_year}
            })

        query += " LIMIT " + str(page_size + 1) + " OFFSET " + str(offset)

        rows = run_bigquery(query, query_params)
        has_more = len(rows) > page_size
        rows = rows[:page_size]
        results = parse_bq_results(rows)

        for i, row in enumerate(rows):
            if i < len(results):
                results[i]["first_name"]  = str(row.get("first_name")  or "")
                results[i]["middle_name"] = str(row.get("middle_name") or "")
                results[i]["last_name"]   = str(row.get("last_name")   or "")
                results[i]["suffix"]      = str(row.get("name_suffix") or "")

        print("[dmf] " + name + " offset=" + str(offset) + " -> " + str(len(results)) + " results has_more=" + str(has_more))
        return {"name": name, "results": results, "count": len(results), "has_more": has_more, "offset": offset}
    except Exception as e:
        print("[dmf] Search error for " + name + ": " + str(e))
        return {"name": name, "results": [], "count": 0, "has_more": False, "offset": offset}

# ───────────────────────────────────────────────────────────────────────────────

def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS watchlist (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        location TEXT,
        dob TEXT,
        status TEXT DEFAULT 'active',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS obituaries (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        name_normalized TEXT,
        age INTEGER,
        location TEXT,
        date TEXT,
        source TEXT,
        link TEXT,
        obit_text TEXT,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS notifications (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        watchlist_id INTEGER NOT NULL,
        obituary_id INTEGER,
        message TEXT NOT NULL,
        sent BOOLEAN DEFAULT FALSE,
        email_sent BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id),
        FOREIGN KEY (watchlist_id) REFERENCES watchlist (id),
        FOREIGN KEY (obituary_id) REFERENCES obituaries (id)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS death_records (
        id SERIAL PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        birth_date TEXT,
        death_date TEXT,
        state TEXT,
        zip_code TEXT,
        source TEXT DEFAULT 'SSDI',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # ── Existing v0.1.0 column migrations ─────────────────────────────────────
    c.execute("ALTER TABLE obituaries ADD COLUMN IF NOT EXISTS name_normalized TEXT")
    c.execute("ALTER TABLE obituaries ADD COLUMN IF NOT EXISTS obit_text TEXT")
    c.execute("ALTER TABLE obituaries ADD COLUMN IF NOT EXISTS link TEXT")
    c.execute("ALTER TABLE notifications ADD COLUMN IF NOT EXISTS email_sent BOOLEAN DEFAULT FALSE")
    c.execute("ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS is_deceased BOOLEAN DEFAULT FALSE")
    c.execute("UPDATE watchlist SET is_deceased = FALSE WHERE is_deceased IS NULL")
    c.execute("ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS wikipedia_description TEXT")
    c.execute("ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS death_year TEXT")
    c.execute("ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS wiki_last_checked TIMESTAMP")
    c.execute("CREATE INDEX IF NOT EXISTS idx_obituaries_name ON obituaries (name)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_obituaries_name_normalized ON obituaries (name_normalized)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_death_records_last_name ON death_records (last_name)")

    # ── v0.1.1 schema additions ───────────────────────────────────────────────
    # Watchlist: ship-shaped fields
    c.execute("ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS region TEXT")
    c.execute("ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS cruise_line TEXT")
    c.execute("ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS year_built TEXT")
    c.execute("ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS ship_type TEXT")
    # Backfill region from legacy `location`, year_built from legacy `dob`
    c.execute("UPDATE watchlist SET region = location WHERE region IS NULL AND location IS NOT NULL AND location <> ''")
    c.execute("UPDATE watchlist SET year_built = dob WHERE year_built IS NULL AND dob IS NOT NULL AND dob <> ''")
    c.execute("CREATE INDEX IF NOT EXISTS idx_watchlist_region ON watchlist (region)")

    # Notifications: relax obituary_id, add polymorphic source pointers
    try:
        c.execute("ALTER TABLE notifications ALTER COLUMN obituary_id DROP NOT NULL")
    except Exception as _e:
        # Already nullable, or older Postgres without the constraint to drop — safe to ignore.
        conn.rollback()
        c = conn.cursor()
    c.execute("ALTER TABLE notifications ADD COLUMN IF NOT EXISTS source_type TEXT")
    c.execute("ALTER TABLE notifications ADD COLUMN IF NOT EXISTS source_ref_id INTEGER")
    c.execute("CREATE INDEX IF NOT EXISTS idx_notifications_source ON notifications (source_type, source_ref_id)")

    # Inspections (CDC VSP Green Sheet) — schema only in v0.1.1, scraper lands v0.1.2
    c.execute("""CREATE TABLE IF NOT EXISTS inspections (
        id SERIAL PRIMARY KEY,
        ship_name TEXT NOT NULL,
        inspection_date TEXT,
        score INTEGER,
        report_url TEXT,
        violations_summary TEXT,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_inspections_ship ON inspections (ship_name)")

    # Outbreaks (CDC VSP) — schema only in v0.1.1, scraper lands v0.1.3
    c.execute("""CREATE TABLE IF NOT EXISTS outbreaks (
        id SERIAL PRIMARY KEY,
        ship_name TEXT NOT NULL,
        month TEXT,
        year TEXT,
        cruise_line TEXT,
        agent TEXT,
        pax_ill INTEGER,
        pax_total INTEGER,
        crew_ill INTEGER,
        crew_total INTEGER,
        report_url TEXT,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_outbreaks_ship ON outbreaks (ship_name)")

    # Advisories (State Dept) — active in v0.1.1
    c.execute("""CREATE TABLE IF NOT EXISTS advisories (
        id SERIAL PRIMARY KEY,
        country_code TEXT UNIQUE NOT NULL,
        country_name TEXT,
        region TEXT,
        level INTEGER,
        title TEXT,
        summary TEXT,
        url TEXT,
        published TEXT,
        updated TEXT,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_advisories_region ON advisories (region)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_advisories_level ON advisories (level)")

    # Marine forecasts (NOAA High Seas) — one row per HSF area, refreshed daily.
    c.execute("""CREATE TABLE IF NOT EXISTS marine_forecasts (
        id SERIAL PRIMARY KEY,
        area_code TEXT UNIQUE NOT NULL,
        area_name TEXT,
        regions TEXT,
        max_wave_m REAL,
        sea_state TEXT,
        raw_text TEXT,
        source_url TEXT,
        issued_at TEXT,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_marine_regions ON marine_forecasts (regions)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_marine_state ON marine_forecasts (sea_state)")

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


def normalize_name(name: str) -> str:
    if not name:
        return name
    name = name.strip()
    if ',' in name:
        parts = name.split(',', 1)
        name = parts[1].strip() + " " + parts[0].strip()
    name = re.sub(r"\b\w+'\w+\b", lambda m: m.group(0).title(), name)
    return name.title()


def send_email_notification(to_email: str, watchlist_name: str, obit_name: str, obit_location: str, obit_link: str):
    if not RESEND_API_KEY:
        print("Email not configured - skipping email to " + to_email)
        return False
    try:
        location_text = obit_location or "Unknown"
        link_text = '<p><a href="' + str(obit_link) + '">Read more</a></p>' if obit_link else ""
        html_content = (
            '<div style="font-family: Georgia, serif; max-width: 600px; margin: 0 auto; padding: 20px;">'
            '<h2 style="color: #0891b2;">Cruise Ship Watch Alert</h2>'
            '<p>We found an update for <strong>' + watchlist_name + '</strong> on your watchlist.</p>'
            '<p><strong>Ship:</strong> ' + obit_name + '</p>'
            '<p><strong>Region:</strong> ' + location_text + '</p>'
            + link_text +
            '<hr style="border: 1px solid #e5e7eb; margin: 20px 0;">'
            '<p style="color: #6b7280; font-size: 12px;">You are receiving this because you added '
            + watchlist_name + ' to your Cruise Ship Watch watchlist. '
            'To manage your watchlist, visit <a href="https://cruiseship.watch">cruiseship.watch</a></p>'
            '</div>'
        )
        response = httpx.post(
            "https://api.resend.com/emails",
            headers={"Authorization": "Bearer " + RESEND_API_KEY, "Content-Type": "application/json"},
            json={
                "from": FROM_EMAIL,
                "to": [to_email],
                "subject": "Cruise Ship Watch Alert: " + watchlist_name,
                "html": html_content
            },
            timeout=10
        )
        return response.status_code == 200
    except Exception as e:
        print("Email error: " + str(e))
        return False


def send_advisory_email(to_email: str, ship_name: str, region: str, country_name: str,
                        level: int, title: str, summary: str, url: str):
    """Resend template for State Dept travel advisory alerts."""
    if not RESEND_API_KEY:
        print("Email not configured - skipping advisory email to " + to_email)
        return False
    try:
        # State Dept summaries are HTML. Strip tags for the snippet, keep first ~400 chars.
        plain = re.sub(r"<[^>]+>", " ", summary or "")
        plain = html_lib.unescape(plain)
        plain = re.sub(r"\s+", " ", plain).strip()
        if len(plain) > 400:
            plain = plain[:400].rsplit(" ", 1)[0] + "…"

        level_label = "Level " + str(level) + " advisory"
        link_text = '<p><a href="' + str(url) + '">Read full advisory at travel.state.gov</a></p>' if url else ""

        html_content = (
            '<div style="font-family: Georgia, serif; max-width: 600px; margin: 0 auto; padding: 20px;">'
            '<h2 style="color: #0891b2;">Cruise Ship Watch — Travel Advisory</h2>'
            '<p>The U.S. State Department has issued a <strong>' + level_label + '</strong> for '
            '<strong>' + (country_name or "this country") + '</strong>, in the <strong>' + region + '</strong> region.</p>'
            '<p>You are watching <strong>' + ship_name + '</strong>, which sails this region.</p>'
            '<p style="background:#fef3c7;padding:12px;border-left:4px solid #f59e0b;margin:16px 0;">'
            '<strong>' + (title or "") + '</strong></p>'
            '<p style="color:#374151;">' + plain + '</p>'
            + link_text +
            '<hr style="border: 1px solid #e5e7eb; margin: 20px 0;">'
            '<p style="color: #6b7280; font-size: 12px;">'
            'You are receiving this because <strong>' + ship_name + '</strong> is on your Cruise Ship Watch watchlist '
            'and the State Department raised the advisory level for a country in its sailing region. '
            'To manage your watchlist, visit <a href="https://cruiseship.watch">cruiseship.watch</a>.</p>'
            '</div>'
        )
        response = httpx.post(
            "https://api.resend.com/emails",
            headers={"Authorization": "Bearer " + RESEND_API_KEY, "Content-Type": "application/json"},
            json={
                "from": FROM_EMAIL,
                "to": [to_email],
                "subject": "Travel Advisory (Level " + str(level) + "): " + (country_name or "Region update") + " — " + ship_name,
                "html": html_content
            },
            timeout=10
        )
        return response.status_code == 200
    except Exception as e:
        print("Advisory email error: " + str(e))
        return False


def fetch_wiki_data(name: str) -> dict:
    params = urllib.parse.urlencode({
        "action": "query",
        "titles": name,
        "prop": "extracts|pageprops|categories|info",
        "exintro": "true",
        "explaintext": "true",
        "redirects": "1",
        "inprop": "url",
        "cllimit": "50",
        "format": "json"
    })
    url = "https://en.wikipedia.org/w/api.php?" + params
    req = urllib.request.Request(url, headers={"User-Agent": "CruiseShipWatch/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json_lib.loads(resp.read().decode())
    pages = data.get("query", {}).get("pages", {})
    if not pages:
        return {}
    page = list(pages.values())[0]
    if "missing" in page:
        return {}
    title = page.get("title", name)
    extract = page.get("extract", "")
    categories = [c.get("title", "") for c in page.get("categories", [])]
    death_year_from_cat = None
    for cat in categories:
        m = re.search(r"(\d{4}) deaths", cat)
        if m:
            death_year_from_cat = m.group(1)
            break
    death_from_category = any(
        any(word in cat.lower() for word in ["deaths", "murdered", "executed"])
        for cat in categories
    )
    page_type = "standard"
    if "disambiguation" in page.get("pageprops", {}):
        page_type = "disambiguation"
    elif "(disambiguation)" in title:
        page_type = "disambiguation"
    description = ""
    if extract:
        first_sent = extract.split(".")[0]
        description = first_sent[:150] if first_sent else ""
    thumbnail = None
    birth_date = None
    death_date_summary = None
    try:
        sum_url = "https://en.wikipedia.org/api/rest_v1/page/summary/" + urllib.parse.quote(title)
        sum_req = urllib.request.Request(sum_url, headers={"User-Agent": "CruiseShipWatch/1.0"})
        with urllib.request.urlopen(sum_req, timeout=10) as sum_resp:
            sum_data = json_lib.loads(sum_resp.read().decode())
            thumb = sum_data.get("thumbnail")
            thumbnail = thumb.get("source") if thumb else None
            birth_date = sum_data.get("birth_date")
            death_date_summary = sum_data.get("death_date")
            if sum_data.get("description"):
                description = sum_data.get("description")
    except Exception:
        pass
    death_date = death_date_summary or death_year_from_cat
    return {
        "title": title,
        "extract": extract,
        "description": description,
        "type": page_type,
        "death_date": death_date,
        "death_from_category": death_from_category,
        "thumbnail": thumbnail,
        "birth_date": birth_date,
    }


def normalize_name_for_wiki(name: str) -> str:
    words = name.strip().split()
    result = []
    for i, word in enumerate(words):
        if len(word) == 1 and word.isalpha() and i > 0 and i < len(words) - 1:
            result.append(word + ".")
        else:
            result.append(word)
    return " ".join(result)


def fetch_wiki_data_smart(name: str) -> dict:
    data = fetch_wiki_data(name)
    if data.get("type") != "disambiguation" and data.get("extract"):
        return data
    name_parts = [p for p in name.strip().split() if p.replace(".", "")]
    last_name = name_parts[-1].lower().replace(".", "") if name_parts else ""
    first_name = name_parts[0].lower() if name_parts else ""
    print("[wiki_smart] Resolving disambiguation for: " + name + " (last=" + last_name + ")")
    try:
        search_url = "https://en.wikipedia.org/w/api.php?action=query&list=search&srsearch=" + urllib.parse.quote(name) + "&srlimit=8&format=json&origin=*"
        req = urllib.request.Request(search_url, headers={"User-Agent": "CruiseShipWatch/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            search_data = json_lib.loads(resp.read().decode())
        results = search_data.get("query", {}).get("search", [])
        for result in results:
            title = result.get("title", "")
            title_lower = title.lower()
            if "(disambiguation)" in title_lower:
                continue
            if last_name and last_name not in title_lower:
                continue
            if first_name and first_name not in title_lower:
                print("[wiki_smart] Skipping " + title + " - first name mismatch (want " + first_name + ")")
                continue
            try:
                candidate = fetch_wiki_data(title)
                if candidate.get("type") == "disambiguation":
                    continue
                if not candidate.get("extract"):
                    continue
                print("[wiki_smart] Resolved " + name + " -> " + title)
                return candidate
            except Exception:
                continue
    except Exception as e:
        print("[wiki_smart] Search failed: " + str(e))
    print("[wiki_smart] Could not resolve: " + name)
    return data


def extract_full_death_date(data: dict) -> str:
    import re as _re
    month_names = "January|February|March|April|May|June|July|August|September|October|November|December"
    full_date_pattern = "(?:" + month_names + r") \d{1,2}, \d{4}"
    extract = data.get("extract", "")
    description = data.get("description", "")
    matches = _re.findall(full_date_pattern, extract)
    if len(matches) >= 2:
        return matches[-1]
    if len(matches) == 1:
        first_sentence = extract.split(".")[0]
        if _re.search(r"died|death|passed|\u2013|\u2014", first_sentence, _re.IGNORECASE):
            return matches[0]
    death_date = data.get("death_date", "")
    if death_date and _re.search(full_date_pattern, str(death_date)):
        return str(death_date)
    m = _re.search(r"\d{4}\s*[\u2013\u2014-]+\s*(\d{4})", description)
    if m:
        return m.group(1)
    m = _re.search(r"\d{4}\s*[\u2013\u2014-]+\s*(\d{4})", extract.split(".")[0] if extract else "")
    if m:
        return m.group(1)
    if death_date:
        return str(death_date)
    return data.get("death_year_from_cat", "") or ""


def is_deceased_from_wiki(data: dict) -> bool:
    extract = data.get("extract", "")
    description = data.get("description", "")
    death_date = data.get("death_date", None)
    page_type = data.get("type", "")
    death_from_category = data.get("death_from_category", False)
    if page_type == "disambiguation":
        return False
    if death_from_category:
        return True
    if death_date:
        return True
    first_sentence = extract.split(".")[0] if extract else ""
    if re.search(r"\(\d{4}\s*[\u2013\u2014-]+\s*\d{4}\)", first_sentence):
        return True
    if re.search(r"\(\d{4}\s*[\u2013\u2014-]+\s*\d{4}\)", description):
        return True
    if re.search(r"\([^)]*born\s+\w+\s+\d+,\s+\d{4}\)", first_sentence):
        return False
    if re.search(r"(died|death|passed away|deceased)", extract, re.IGNORECASE):
        return True
    if re.search(r"(died|death|deceased)", description, re.IGNORECASE):
        return True
    return False


def search_legacy_oneoff(name: str) -> list:
    """Legacy.com obituary search — kept from MW backend, will not match for ship names. Returns []."""
    return []


# ── State Department travel advisories (v0.1.1) ───────────────────────────────

STATE_API_URL = "https://cadataapi.state.gov/api/TravelAdvisories"


def parse_advisory_level(title: str) -> int:
    """Extract numeric advisory level from a State Dept title like 'Greenland - Level 2: ...'."""
    if not title:
        return 0
    m = re.search(r"Level\s+([1-4])", title, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return 0
    return 0


def parse_country_name_from_title(title: str, country_code: str) -> str:
    """Best-effort country name from advisory title. Falls back to country code."""
    if not title:
        return country_code or ""
    # Pattern: "Greenland - Level 2: Exercise Increased Caution" -> "Greenland"
    m = re.match(r"^([^-—–]+?)\s*[-—–]\s*Level", title, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # Pattern: "Greenland Travel Advisory" -> "Greenland"
    m = re.match(r"^(.+?)\s+Travel Advisory", title, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return country_code or ""


def fetch_state_advisories() -> list:
    """Fetch the full advisories feed from cadataapi.state.gov. Returns a list of raw entries."""
    try:
        req = urllib.request.Request(
            STATE_API_URL,
            headers={
                "User-Agent": "CruiseShipWatch/1.0",
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        data = json_lib.loads(raw)
        # API returns a JSON array of advisory objects.
        if isinstance(data, list):
            return data
        # Defensive: some shapes wrap the array under a key.
        if isinstance(data, dict):
            for k in ("value", "Value", "data", "items", "advisories"):
                v = data.get(k)
                if isinstance(v, list):
                    return v
        print("[advisories] Unexpected response shape: " + str(type(data)))
        return []
    except Exception as e:
        print("[advisories] Fetch error: " + str(e))
        return []


def upsert_advisory(conn, entry: dict) -> Optional[dict]:
    """Upsert a single advisory entry. Returns dict {country_code, level, region, advisory_id, prev_level}
    on success, None on skip/error."""
    cat_raw = entry.get("Category") or entry.get("category") or ""
    if isinstance(cat_raw, list): cat_raw = cat_raw[0] if cat_raw else ""
    country_code = str(cat_raw).strip().upper()
    if not country_code or len(country_code) > 5:
        return None

    title = entry.get("Title") or entry.get("title") or ""
    summary = entry.get("Summary") or entry.get("summary") or ""
    link = entry.get("Link") or entry.get("link") or ""
    published = entry.get("Published") or entry.get("published") or ""
    updated = entry.get("Updated") or entry.get("updated") or ""
    # Coerce list fields (State Dept API sometimes returns lists for these)
    if isinstance(title, list): title = title[0] if title else ""
    if isinstance(summary, list): summary = summary[0] if summary else ""
    if isinstance(link, list): link = link[0] if link else ""
    if isinstance(published, list): published = published[0] if published else ""
    if isinstance(updated, list): updated = updated[0] if updated else ""
    title = str(title)
    summary = str(summary)
    link = str(link)
    published = str(published)
    updated = str(updated)
    level = parse_advisory_level(title)
    country_name = parse_country_name_from_title(title, country_code)
    region = get_region_for_country(country_code)

    c = conn.cursor()
    # Read prior level for change detection
    c.execute("SELECT id, level FROM advisories WHERE country_code = %s", (country_code,))
    prior = c.fetchone()
    prev_level = prior[1] if prior else None

    c.execute("""
        INSERT INTO advisories (country_code, country_name, region, level, title, summary, url, published, updated, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (country_code) DO UPDATE SET
            country_name = EXCLUDED.country_name,
            region = EXCLUDED.region,
            level = EXCLUDED.level,
            title = EXCLUDED.title,
            summary = EXCLUDED.summary,
            url = EXCLUDED.url,
            published = EXCLUDED.published,
            updated = EXCLUDED.updated,
            fetched_at = EXCLUDED.fetched_at
        RETURNING id
    """, (country_code, country_name, region, level, title, summary, link,
          str(published), str(updated), datetime.utcnow()))
    advisory_id = c.fetchone()[0]
    return {
        "country_code": country_code,
        "country_name": country_name,
        "region": region,
        "level": level,
        "title": title,
        "summary": summary,
        "url": link,
        "advisory_id": advisory_id,
        "prev_level": prev_level,
    }


def fire_advisory_alerts(conn, advisory: dict) -> int:
    """Deprecated as of 0.1.5: per-advisory fan-out caused N alerts per ship.
    Replaced by reconcile_advisory_alerts_for_ship() which writes only the
    top-level set per ship per region in a single pass. Kept as a no-op so
    older call sites remain harmless."""
    return 0


def reconcile_advisory_alerts_for_ship(conn, watch_id: int, user_id: int,
                                       ship_name: str, region: str,
                                       user_email: Optional[str]) -> int:
    """Replace this ship's State-Dept advisory notifications with only the
    current top-level set for its region. Returns number of fresh alerts written.

    Algorithm: find max level (>=3) in this ship's region. Delete all prior
    `source_type='advisory'` notifications for this ship. Insert one notification
    per advisory at that max level. Email the user once with a digest if the
    digest content has changed since the last successful email.
    """
    if not region:
        return 0
    c = conn.cursor()

    # Find max level >= 3 for this region
    c.execute("""
        SELECT MAX(level) FROM advisories
        WHERE region = %s AND level >= 3
    """, (region,))
    max_row = c.fetchone()
    max_level = (max_row and max_row[0]) or 0
    if max_level < 3:
        # No qualifying advisories — clear any stale notifications for this ship.
        c.execute("""
            DELETE FROM notifications
            WHERE watchlist_id = %s AND source_type = 'advisory'
        """, (watch_id,))
        return 0

    # Pull all advisories at max_level for this region
    c.execute("""
        SELECT id, country_code, country_name, level, title, summary, url
        FROM advisories
        WHERE region = %s AND level = %s
        ORDER BY country_name ASC
    """, (region, max_level))
    top = c.fetchall()
    if not top:
        return 0

    # Wipe prior advisory notifications for this ship — only the latest snapshot remains.
    c.execute("""
        DELETE FROM notifications
        WHERE watchlist_id = %s AND source_type = 'advisory'
    """, (watch_id,))

    fired = 0
    digest_lines = []
    for adv_id, country_code, country_name, level, title, summary, url in top:
        message = (
            "Travel advisory (Level " + str(level) + ") for " + (country_name or country_code) +
            " in " + region + " — affects " + ship_name + "."
        )
        c.execute("""
            INSERT INTO notifications
                (user_id, watchlist_id, obituary_id, message, email_sent, source_type, source_ref_id)
            VALUES (%s, %s, NULL, %s, FALSE, 'advisory', %s)
        """, (user_id, watch_id, message, adv_id))
        fired += 1
        digest_lines.append("• Level " + str(level) + " — " + (country_name or country_code))

    # One email per ship per reconcile (only if there's content). Email the
    # FIRST advisory in the digest as the primary, callers can browse drawer for the rest.
    if user_email and top:
        primary = top[0]
        try:
            send_advisory_email(
                user_email, ship_name, region,
                primary[2] or primary[1],     # country_name fallback to code
                primary[3],                    # level
                primary[4] or "",              # title
                primary[5] or "",              # summary
                primary[6] or ""               # url
            )
        except Exception as e:
            print("[advisories] email send failed for " + ship_name + ": " + str(e))

    return fired


def check_state_advisories():
    """Pull the State Dept advisories feed, upsert each entry, then for each
    active watchlist ship reconcile its advisory notifications to the current
    top-level set for its region (delete prior, insert latest snapshot).

    This guarantees the user only ever sees the most recent State Dept state,
    capped at the highest severity level in their ship's region — no
    accumulation across cron runs.
    """
    print("[" + str(datetime.now()) + "] Starting State Dept advisory check...")
    entries = fetch_state_advisories()
    if not entries:
        print("[advisories] No entries returned. Skipping.")
        return

    upserted = 0
    with get_db() as conn:
        # Pass 1: upsert all advisories (data-only)
        for entry in entries:
            try:
                result = upsert_advisory(conn, entry)
                conn.commit()
                if result:
                    upserted += 1
            except Exception as e:
                conn.rollback()
                print("[advisories] Upsert error for entry: " + str(e))
                continue

        # Pass 2: reconcile per active watchlist ship
        c = conn.cursor()
        c.execute("""
            SELECT w.id, w.user_id, w.name, w.region, u.email
            FROM watchlist w
            JOIN users u ON w.user_id = u.id
            WHERE w.status = 'active' AND w.region IS NOT NULL AND w.region <> ''
        """)
        ships = c.fetchall()

        total_fired = 0
        for watch_id, user_id, ship_name, region, user_email in ships:
            try:
                total_fired += reconcile_advisory_alerts_for_ship(
                    conn, watch_id, user_id, ship_name, region, user_email
                )
                conn.commit()
            except Exception as e:
                conn.rollback()
                print("[advisories] Reconcile error for ship " + str(watch_id) + ": " + str(e))
                continue

    print("[" + str(datetime.now()) + "] Advisory check complete. "
          + str(upserted) + " upserted, " + str(total_fired) + " alerts written.")


# ── Pydantic models ───────────────────────────────────────────────────────────

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
    # New v0.1.1 ship-shaped fields
    region: Optional[str] = None
    cruise_line: Optional[str] = None
    year_built: Optional[str] = None
    ship_type: Optional[str] = None
    # Legacy v0.1.0 fields — accepted for back-compat. `location` is treated as region fallback,
    # `dob` as year_built fallback.
    location: Optional[str] = None
    dob: Optional[str] = None
    is_deceased: Optional[bool] = False
    death_year: Optional[str] = None

class WatchlistResponse(BaseModel):
    id: int
    name: str
    location: Optional[str] = None
    dob: Optional[str] = None
    region: Optional[str] = None
    cruise_line: Optional[str] = None
    year_built: Optional[str] = None
    ship_type: Optional[str] = None
    status: str
    created_at: str
    is_deceased: Optional[bool] = False
    wikipedia_description: Optional[str] = None
    death_year: Optional[str] = None

class ObituarySearch(BaseModel):
    name: str
    location: Optional[str] = None
    birth_year: Optional[str] = None

class ObituaryResult(BaseModel):
    id: int
    name: str
    age: Optional[int]
    location: Optional[str]
    date: Optional[str]
    source: str
    link: Optional[str]
    obit_text: Optional[str]
    confidence: str

# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="Cruise Ship Watch API", version=VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        sub = payload.get("sub")
        if sub is None:
            raise HTTPException(status_code=401, detail="Invalid authentication")
        user_id: int = int(sub)
        return user_id
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

def calculate_confidence(search_name, found_name, search_loc, found_loc):
    search_name = search_name.lower()
    found_name = found_name.lower()
    if search_name == found_name:
        if search_loc and found_loc and search_loc.lower() in found_loc.lower():
            return "high"
        return "medium"
    if search_name in found_name or found_name in search_name:
        return "medium"
    return "low"

def extract_age(text: str) -> Optional[int]:
    match = re.search(r'\b(\d{1,3})\b', text)
    if match:
        age = int(match.group(1))
        if 0 < age < 120:
            return age
    return None

def extract_location(text: str) -> Optional[str]:
    match = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,?\s+[A-Z]{2})', text)
    if match:
        return match.group(1)
    return None


@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": VERSION
    }


# ── Admin endpoints ───────────────────────────────────────────────────────────

@app.get("/admin/delete-user")
async def admin_delete_user(email: str):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE email = %s", (email,))
        user = c.fetchone()
        if not user:
            return {"deleted": False, "error": "User not found"}
        user_id = user[0]
        c.execute("DELETE FROM notifications WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM watchlist WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()
        return {"deleted": True, "email": email}


@app.get("/admin/stats")
async def get_stats():
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM obituaries")
        obit_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM death_records")
        death_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM advisories")
        adv_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM advisories WHERE level >= 3")
        adv_high = c.fetchone()[0]
        c.execute("SELECT name, date, source FROM obituaries ORDER BY scraped_at DESC LIMIT 5")
        recent = c.fetchall()
        return {
            "obituaries": obit_count,
            "death_records": death_count,
            "advisories_total": adv_count,
            "advisories_level_3_plus": adv_high,
            "most_recent": [{"name": r[0], "date": r[1], "source": r[2]} for r in recent],
            "scraping": "suspended"
        }


@app.get("/admin/wiki-check-now")
async def wiki_check_now():
    threading.Thread(target=check_wikipedia_watchlist, daemon=True).start()
    return {"message": "Wikipedia watchlist check started"}


@app.get("/admin/advisory-check-now")
async def advisory_check_now():
    """Manually trigger a State Dept advisory pull + alert pass."""
    threading.Thread(target=check_state_advisories, daemon=True).start()
    return {"message": "State Dept advisory check started"}


@app.get("/admin/advisory-reconcile-all")
async def advisory_reconcile_all():
    """Force a reconcile pass for every active watchlist ship using already-cached
    advisories. Use this after deploy to clean up legacy duplicate alerts without
    waiting for the next cron tick."""
    def _do():
        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("""
                    SELECT w.id, w.user_id, w.name, w.region, u.email
                    FROM watchlist w
                    JOIN users u ON w.user_id = u.id
                    WHERE w.status = 'active' AND w.region IS NOT NULL AND w.region <> ''
                """)
                ships = c.fetchall()
                total = 0
                for watch_id, user_id, ship_name, region, user_email in ships:
                    try:
                        total += reconcile_advisory_alerts_for_ship(
                            conn, watch_id, user_id, ship_name, region, user_email
                        )
                        conn.commit()
                    except Exception as e:
                        conn.rollback()
                        print("[reconcile-all] error for ship " + str(watch_id) + ": " + str(e))
                print("[reconcile-all] complete. " + str(total) + " alerts written across "
                      + str(len(ships)) + " ships.")
        except Exception as e:
            print("[reconcile-all] fatal: " + str(e))

    threading.Thread(target=_do, daemon=True).start()
    return {"message": "Advisory reconcile started for all active watchlist ships"}


# ── Marine (NOAA High Seas Forecast) ──────────────────────────────────────────
# Each entry: NOAA text-bulletin URL, an internal area code (stable key),
# a friendly area name, and the canonical CW regions this bulletin covers.

MARINE_AREAS = [
    {
        "code": "HSFAT2",
        "name": "Tropical N Atlantic & Caribbean",
        "url": "https://tgftp.nws.noaa.gov/data/raw/fz/fznt02.knhc.hsf.at2.txt",
        "regions": ["Caribbean", "Central America", "South America"],
    },
    {
        "code": "HSFNT1",
        "name": "Atlantic (NE Atlantic & Offshore U.S.)",
        "url": "https://tgftp.nws.noaa.gov/data/raw/fz/fznt01.kwbc.hsf.nt1.txt",
        "regions": ["North America", "Mediterranean", "Northern Europe", "Africa"],
    },
    {
        "code": "HSFEP2",
        "name": "Tropical NE Pacific",
        "url": "https://tgftp.nws.noaa.gov/data/raw/fz/fzpn03.knhc.hsf.ep2.txt",
        "regions": ["Central America"],
    },
    {
        "code": "HSFNP",
        "name": "NE Pacific (Offshore U.S. & Hawaii N)",
        "url": "https://tgftp.nws.noaa.gov/data/raw/fz/fzpn02.kwbc.hsf.np.txt",
        "regions": ["North America"],
    },
    {
        "code": "HSFNP_HAWAII",
        "name": "Central Pacific (Hawaii & SW Pacific)",
        "url": "https://tgftp.nws.noaa.gov/data/raw/fz/fzpn40.phfo.hsf.npa.txt",
        "regions": ["Oceania"],
    },
    {
        "code": "HSFNP_AK",
        "name": "Alaska / Bering Sea / Arctic",
        "url": "https://tgftp.nws.noaa.gov/data/raw/fz/fzak50.pawu.hsf.ak.txt",
        "regions": ["Arctic"],
    },
    {
        "code": "HSFAS",
        "name": "Asia Pacific (issued by JMA)",
        "url": "https://tgftp.nws.noaa.gov/data/raw/fz/fzpn01.rjtd.hsf.as.txt",
        "regions": ["Asia"],
    },
    {
        "code": "HSFIO",
        "name": "Indian Ocean / Middle East seas",
        "url": "https://tgftp.nws.noaa.gov/data/raw/fz/fzio01.fmee.hsf.io.txt",
        "regions": ["Middle East", "Africa"],
    },
]


def _parse_max_wave_meters(text: str) -> Optional[float]:
    """Extract the largest 'SEAS X TO Y M' value from a NOAA HSF bulletin.
    Returns max wave height in meters, or None if not found."""
    if not text:
        return None
    max_m = None
    # Match patterns like "SEAS 2.5 TO 4.5 M", "SEAS TO 4 M", "SEAS 3 M"
    pattern = re.compile(
        r"SEAS\s+(?:TO\s+)?(\d+(?:\.\d+)?)\s*(?:TO\s+(\d+(?:\.\d+)?)\s*)?M\b",
        re.IGNORECASE,
    )
    for m in pattern.finditer(text):
        try:
            a = float(m.group(1))
            b = float(m.group(2)) if m.group(2) else a
            top = max(a, b)
            if max_m is None or top > max_m:
                max_m = top
        except (TypeError, ValueError):
            continue
    return max_m


def _sea_state_for_wave(max_m: Optional[float]) -> str:
    """Three-tier sea-state label tied to significant wave height (meters)."""
    if max_m is None:
        return "unknown"
    if max_m < 2.5:
        return "calm"
    if max_m < 4.0:
        return "moderate"
    return "rough"


def _fetch_marine_bulletin(url: str) -> Optional[str]:
    """Fetch a NOAA marine text bulletin. Returns raw text or None."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "CruiseShipWatch/1.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = resp.read()
        try:
            return data.decode("utf-8", errors="replace")
        except Exception:
            return data.decode("latin-1", errors="replace")
    except Exception as e:
        print("[marine] fetch failed for " + url + ": " + str(e))
        return None


def _extract_issued_at(text: str) -> Optional[str]:
    """Pull the issuance datetime line from a NOAA bulletin if present.
    Returns the raw line (e.g. '0430 UTC SUN JAN 18 2026') or None."""
    if not text:
        return None
    m = re.search(r"\b\d{3,4}\s+UTC\s+\w{3}\s+\w{3}\s+\d{1,2}\s+\d{4}\b", text)
    return m.group(0) if m else None


def check_marine_forecasts():
    """Pull all configured NOAA HSF bulletins, upsert, and fire alerts on
    transitions into moderate/rough sea state for any watchlist ship in
    a covered region."""
    print("[" + str(datetime.utcnow()) + "] Starting marine forecast check...")
    upserted = 0
    transitions = []  # list of (area_code, area_name, regions, prev_state, new_state, max_m)

    with get_db() as conn:
        c = conn.cursor()
        for area in MARINE_AREAS:
            text = _fetch_marine_bulletin(area["url"])
            if not text:
                continue
            max_m = _parse_max_wave_meters(text)
            new_state = _sea_state_for_wave(max_m)
            issued_at = _extract_issued_at(text)
            regions_csv = ",".join(area["regions"])

            # Read prior state for transition detection
            c.execute(
                "SELECT sea_state FROM marine_forecasts WHERE area_code = %s",
                (area["code"],),
            )
            prior = c.fetchone()
            prev_state = prior[0] if prior else None

            c.execute(
                """
                INSERT INTO marine_forecasts
                    (area_code, area_name, regions, max_wave_m, sea_state, raw_text,
                     source_url, issued_at, fetched_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (area_code) DO UPDATE SET
                    area_name = EXCLUDED.area_name,
                    regions = EXCLUDED.regions,
                    max_wave_m = EXCLUDED.max_wave_m,
                    sea_state = EXCLUDED.sea_state,
                    raw_text = EXCLUDED.raw_text,
                    source_url = EXCLUDED.source_url,
                    issued_at = EXCLUDED.issued_at,
                    fetched_at = EXCLUDED.fetched_at
                """,
                (
                    area["code"],
                    area["name"],
                    regions_csv,
                    max_m,
                    new_state,
                    text[:20000],
                    area["url"],
                    issued_at,
                    datetime.utcnow(),
                ),
            )
            upserted += 1

            # Fire alerts only when the bulletin escalates the sea state.
            severity = {"unknown": 0, "calm": 1, "moderate": 2, "rough": 3}
            if prev_state and severity.get(new_state, 0) > severity.get(prev_state, 0):
                transitions.append(
                    (area["code"], area["name"], area["regions"], prev_state, new_state, max_m)
                )

        conn.commit()

        # For each transition, alert affected watchlist ships.
        alerts_fired = 0
        for area_code, area_name, regions, prev_state, new_state, max_m in transitions:
            for region in regions:
                c.execute(
                    """
                    SELECT w.id, w.user_id, w.name, u.email
                    FROM watchlist w JOIN users u ON u.id = w.user_id
                    WHERE w.region = %s AND w.status = 'active'
                    """,
                    (region,),
                )
                affected = c.fetchall()
                for watch_id, user_id, watch_name, user_email in affected:
                    wave_str = ("%.1f m" % max_m) if max_m is not None else "elevated"
                    msg = (
                        "Sea state in " + region + " has worsened to "
                        + new_state + " (" + wave_str + "). Source: NWS High Seas Forecast — "
                        + area_name + "."
                    )
                    # De-dupe: skip if we already fired this exact transition msg recently.
                    c.execute(
                        """SELECT id FROM notifications
                           WHERE watchlist_id = %s AND message = %s
                             AND created_at > NOW() - INTERVAL '24 hours'""",
                        (watch_id, msg),
                    )
                    if c.fetchone():
                        continue
                    c.execute(
                        """INSERT INTO notifications
                               (user_id, watchlist_id, obituary_id, message, email_sent, source_type)
                           VALUES (%s, %s, NULL, %s, FALSE, 'marine')""",
                        (user_id, watch_id, msg),
                    )
                    alerts_fired += 1

        conn.commit()

    print(
        "[" + str(datetime.utcnow()) + "] Marine check complete. "
        + str(upserted) + " areas upserted, "
        + str(len(transitions)) + " transitions, "
        + str(alerts_fired) + " alerts fired."
    )


@app.get("/admin/marine-check-now")
async def marine_check_now():
    """Manually trigger a NOAA marine forecast pull + alert pass."""
    threading.Thread(target=check_marine_forecasts, daemon=True).start()
    return {"message": "Marine forecast check started"}


@app.get("/admin/marine")
async def admin_list_marine():
    """List cached marine forecasts."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT area_code, area_name, regions, max_wave_m, sea_state,
                   source_url, issued_at, fetched_at
            FROM marine_forecasts
            ORDER BY sea_state DESC, area_name ASC
        """)
        rows = c.fetchall()
        return {
            "count": len(rows),
            "areas": [
                {
                    "area_code": r[0],
                    "area_name": r[1],
                    "regions": r[2].split(",") if r[2] else [],
                    "max_wave_m": r[3],
                    "sea_state": r[4],
                    "source_url": r[5],
                    "issued_at": r[6],
                    "fetched_at": str(r[7]) if r[7] else None,
                }
                for r in rows
            ],
        }


@app.get("/watchlist/{item_id}/marine")
async def get_watchlist_marine(item_id: int, user_id: int = Depends(get_current_user)):
    """Return cached NOAA marine forecasts for the ship's region."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT id, name, region, location
            FROM watchlist
            WHERE id = %s AND user_id = %s AND status = 'active'
            """,
            (item_id, user_id),
        )
        row = c.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Watchlist item not found")
        watch_id, watch_name, region, location_legacy = row
        canonical = normalize_region_label(region) or normalize_region_label(location_legacy)

        if not canonical:
            return {
                "watchlist_id": watch_id,
                "ship_name": watch_name,
                "region": region or location_legacy,
                "canonical_region": None,
                "marine": [],
                "note": "Region not recognized — no marine forecast returned.",
            }

        # Match any forecast whose regions CSV contains the canonical region.
        c.execute(
            """
            SELECT area_code, area_name, regions, max_wave_m, sea_state, raw_text,
                   source_url, issued_at, fetched_at
            FROM marine_forecasts
            WHERE regions LIKE %s OR regions LIKE %s OR regions LIKE %s OR regions = %s
            ORDER BY
                CASE sea_state
                    WHEN 'rough' THEN 0
                    WHEN 'moderate' THEN 1
                    WHEN 'calm' THEN 2
                    ELSE 3
                END,
                area_name ASC
            """,
            (
                canonical + ",%",
                "%," + canonical + ",%",
                "%," + canonical,
                canonical,
            ),
        )
        rows = c.fetchall()
        return {
            "watchlist_id": watch_id,
            "ship_name": watch_name,
            "region": region or location_legacy,
            "canonical_region": canonical,
            "marine": [
                {
                    "area_code": r[0],
                    "area_name": r[1],
                    "regions": r[2].split(",") if r[2] else [],
                    "max_wave_m": r[3],
                    "sea_state": r[4],
                    "raw_text": r[5],
                    "source_url": r[6],
                    "issued_at": r[7],
                    "fetched_at": str(r[8]) if r[8] else None,
                }
                for r in rows
            ],
        }


@app.get("/admin/advisories")
async def admin_list_advisories(level_min: int = 0):
    """List cached advisories filtered by minimum level."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT country_code, country_name, region, level, title, url, published, updated, fetched_at
            FROM advisories
            WHERE level >= %s
            ORDER BY level DESC, country_name ASC
        """, (level_min,))
        rows = c.fetchall()
        return {
            "count": len(rows),
            "level_min": level_min,
            "advisories": [
                {
                    "country_code": r[0],
                    "country_name": r[1],
                    "region": r[2],
                    "level": r[3],
                    "title": r[4],
                    "url": r[5],
                    "published": r[6],
                    "updated": r[7],
                    "fetched_at": str(r[8]) if r[8] else None,
                }
                for r in rows
            ],
        }


@app.get("/admin/test-refresh/{name}")
async def test_refresh(name: str):
    try:
        data = fetch_wiki_data(name)
        is_deceased = is_deceased_from_wiki(data)
        return {
            "name": name,
            "is_deceased": is_deceased,
            "death_date": data.get("death_date"),
            "description": data.get("description"),
            "extract_first_sentence": (data.get("extract") or "").split(".")[0],
            "type": data.get("type"),
            "wiki_ok": True
        }
    except Exception as e:
        return {"name": name, "wiki_ok": False, "error": str(e)}


@app.get("/admin/test-ssdi/{name}")
async def test_ssdi(name: str):
    try:
        parts = name.strip().split()
        if len(parts) == 1:
            last = parts[0].upper()
            query = (
                "SELECT first_name, middle_name, last_name, dob, dod "
                "FROM `fiat-fiendum.ssdmf.ssdmf_most_recent` "
                "WHERE UPPER(last_name) = @lname "
                "LIMIT 5"
            )
            query_params = [
                {"name": "lname", "parameterType": {"type": "STRING"}, "parameterValue": {"value": last}},
            ]
        else:
            first = parts[0].upper()
            last = parts[-1].upper()
            query = (
                "SELECT first_name, middle_name, last_name, dob, dod "
                "FROM `fiat-fiendum.ssdmf.ssdmf_most_recent` "
                "WHERE UPPER(last_name) = @lname "
                "AND UPPER(first_name) LIKE @fname "
                "LIMIT 5"
            )
            query_params = [
                {"name": "lname", "parameterType": {"type": "STRING"}, "parameterValue": {"value": last}},
                {"name": "fname", "parameterType": {"type": "STRING"}, "parameterValue": {"value": first + "%"}},
            ]
        rows = run_bigquery(query, query_params)
        results = parse_bq_results(rows)
        return {"name": name, "count": len(results), "results": results, "bq_ok": True}
    except Exception as e:
        return {"name": name, "bq_ok": False, "error": str(e)}


# ── Auth endpoints ────────────────────────────────────────────────────────────

@app.post("/auth/register", response_model=Token)
async def register(user: UserCreate):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE email = %s", (user.email,))
        if c.fetchone():
            raise HTTPException(status_code=400, detail="Email already registered")
        password_hash = hash_password(user.password)
        c.execute(
            "INSERT INTO users (email, password_hash) VALUES (%s, %s) RETURNING id",
            (user.email, password_hash))
        user_id = c.fetchone()[0]
        conn.commit()
        access_token = create_access_token(data={"sub": str(user_id)})
        return {"access_token": access_token, "token_type": "bearer"}


@app.post("/auth/login", response_model=Token)
async def login(user: UserLogin):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id, password_hash FROM users WHERE email = %s", (user.email,))
        result = c.fetchone()
        if not result or not verify_password(user.password, result[1]):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        access_token = create_access_token(data={"sub": str(result[0])})
        return {"access_token": access_token, "token_type": "bearer"}


@app.delete("/account")
async def delete_account(user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM notifications WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM watchlist WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()
        return {"message": "Account permanently deleted"}


# ── Watchlist endpoints ───────────────────────────────────────────────────────

@app.get("/watchlist", response_model=List[WatchlistResponse])
async def get_watchlist(user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, name, location, dob, status, created_at,
                   is_deceased, wikipedia_description, death_year,
                   region, cruise_line, year_built, ship_type
            FROM watchlist
            WHERE user_id = %s AND status = 'active'
            ORDER BY created_at DESC
        """, (user_id,))
        items = []
        for row in c.fetchall():
            items.append({
                "id": row[0], "name": row[1], "location": row[2],
                "dob": row[3], "status": row[4], "created_at": str(row[5]),
                "is_deceased": row[6] or False,
                "wikipedia_description": row[7],
                "death_year": row[8],
                "region": row[9],
                "cruise_line": row[10],
                "year_built": row[11],
                "ship_type": row[12],
            })
        return items


@app.post("/watchlist")
async def add_to_watchlist(item: WatchlistItem, user_id: int = Depends(get_current_user)):
    # Resolve region: prefer explicit region, fall back to legacy `location`. Normalize either.
    raw_region = item.region if item.region else item.location
    canonical_region = normalize_region_label(raw_region) or (raw_region or None)

    # Resolve year_built: prefer explicit, fall back to legacy `dob`.
    year_built = item.year_built if item.year_built else item.dob

    # Keep `location` and `dob` populated for back-compat with v0.1.0 frontend reads.
    location_value = item.location if item.location else canonical_region
    dob_value = item.dob if item.dob else year_built

    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO watchlist (
                user_id, name, location, dob, is_deceased, death_year,
                region, cruise_line, year_built, ship_type
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            user_id, item.name, location_value, dob_value,
            item.is_deceased or False, item.death_year or None,
            canonical_region, item.cruise_line, year_built, item.ship_type,
        ))
        conn.commit()
        item_id = c.fetchone()[0]
        return {
            "message": "Added to watchlist",
            "id": item_id,
            "region": canonical_region,
            "year_built": year_built,
            "cruise_line": item.cruise_line,
            "ship_type": item.ship_type,
        }


@app.delete("/watchlist/{item_id}")
async def remove_from_watchlist(item_id: int, user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            "UPDATE watchlist SET status = 'deleted' WHERE id = %s AND user_id = %s",
            (item_id, user_id))
        conn.commit()
        if c.rowcount == 0:
            raise HTTPException(status_code=404, detail="Item not found")
        return {"message": "Removed from watchlist"}


@app.get("/watchlist/{item_id}/refresh")
async def refresh_watchlist_item(item_id: int, user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT w.id, w.name, w.is_deceased, u.email
            FROM watchlist w
            JOIN users u ON w.user_id = u.id
            WHERE w.id = %s AND w.user_id = %s AND w.status = 'active'
        """, (item_id, user_id))
        row = c.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Watchlist item not found")
        watch_id, watch_name, was_deceased, user_email = row

        if was_deceased:
            c.execute("SELECT wikipedia_description, death_year FROM watchlist WHERE id = %s", (watch_id,))
            stored = c.fetchone()
            if stored and stored[0]:
                stored_thumbnail = None
                stored_birth = None
                stored_death_full = stored[1]
                try:
                    sum_url = "https://en.wikipedia.org/api/rest_v1/page/summary/" + urllib.parse.quote(watch_name)
                    sum_req = urllib.request.Request(sum_url, headers={"User-Agent": "CruiseShipWatch/1.0"})
                    with urllib.request.urlopen(sum_req, timeout=10) as sum_resp:
                        sum_data = json_lib.loads(sum_resp.read().decode())
                    thumb = sum_data.get("thumbnail")
                    stored_thumbnail = thumb.get("source") if thumb else None
                    stored_birth = sum_data.get("birth_date")
                    if sum_data.get("death_date"):
                        stored_death_full = sum_data.get("death_date")
                except Exception as e:
                    print("[refresh] Summary fetch error for " + watch_name + ": " + str(e))
                return {
                    "id": watch_id, "name": watch_name, "is_deceased": True,
                    "wikipedia_description": stored[0], "death_year": stored_death_full,
                    "death_date": stored_death_full, "description": "",
                    "thumbnail": stored_thumbnail, "birth_date": stored_birth,
                    "newly_deceased": False, "legacy_results": []
                }

        wiki_ok = False
        extract = ""
        description = ""
        death_date = None
        thumbnail = None
        birth_date = None
        is_deceased = False
        wiki_data = {}

        try:
            normalized_watch_name = normalize_name_for_wiki(watch_name)
            wiki_data = fetch_wiki_data_smart(normalized_watch_name)
            extract = wiki_data.get("extract", "")
            description = wiki_data.get("description", "")
            death_date = extract_full_death_date(wiki_data)
            if wiki_data.get("thumbnail"):
                thumbnail = wiki_data["thumbnail"].get("source")
            birth_date = wiki_data.get("birth_date", None)
            is_deceased = is_deceased_from_wiki(wiki_data)
            wiki_ok = True
        except Exception as e:
            print("[refresh] Wikipedia fetch failed for " + watch_name + ": " + str(e))
            is_deceased = was_deceased or False

        legacy_results = search_legacy_oneoff(watch_name)
        if legacy_results and not is_deceased:
            is_deceased = True

        if wiki_ok or legacy_results:
            c.execute("""
                UPDATE watchlist
                SET is_deceased = %s,
                    wikipedia_description = %s,
                    death_year = %s,
                    wiki_last_checked = %s
                WHERE id = %s
            """, (is_deceased, extract[:2000] if extract else None, death_date, datetime.utcnow(), watch_id))
            conn.commit()

        if is_deceased:
            c.execute(
                "SELECT id FROM notifications WHERE watchlist_id = %s AND message LIKE %s",
                (watch_id, "%Wikipedia%"))
            if not c.fetchone():
                death_info = (" Retired: " + str(death_date)) if death_date else ""
                message = "Update on " + watch_name + "." + death_info
                c.execute("""
                    INSERT INTO notifications
                        (user_id, watchlist_id, obituary_id, message, email_sent, source_type)
                    VALUES (%s, %s, NULL, %s, %s, 'wikipedia')
                """, (user_id, watch_id, message, False))
                conn.commit()
                if user_email:
                    wiki_link = "https://en.wikipedia.org/wiki/" + urllib.parse.quote(watch_name)
                    sent = send_email_notification(user_email, watch_name, watch_name, None, wiki_link)
                    if sent:
                        c.execute(
                            "UPDATE notifications SET email_sent = TRUE WHERE watchlist_id = %s AND message LIKE %s",
                            (watch_id, "%Wikipedia%"))
                        conn.commit()

        if isinstance(thumbnail, dict):
            thumbnail = thumbnail.get("source")

        return {
            "id": watch_id,
            "name": watch_name,
            "is_deceased": is_deceased,
            "wikipedia_description": extract,
            "death_year": death_date,
            "death_date": death_date,
            "description": description,
            "thumbnail": thumbnail,
            "birth_date": birth_date,
            "newly_deceased": is_deceased and not was_deceased,
            "legacy_results": legacy_results
        }


@app.get("/watchlist/{item_id}/advisories")
async def get_watchlist_advisories(item_id: int, user_id: int = Depends(get_current_user)):
    """Return cached State Dept advisories (level >= 2) for the ship's region."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, name, region, location
            FROM watchlist
            WHERE id = %s AND user_id = %s AND status = 'active'
        """, (item_id, user_id))
        row = c.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Watchlist item not found")
        watch_id, watch_name, region, location_legacy = row
        canonical = normalize_region_label(region) or normalize_region_label(location_legacy)

        if not canonical:
            return {
                "watchlist_id": watch_id,
                "ship_name": watch_name,
                "region": region or location_legacy,
                "canonical_region": None,
                "advisories": [],
                "note": "Region not recognized — no advisories returned."
            }

        c.execute("""
            SELECT country_code, country_name, region, level, title, summary, url,
                   published, updated, fetched_at
            FROM advisories
            WHERE region = %s AND level >= 2
            ORDER BY level DESC, country_name ASC
        """, (canonical,))
        rows = c.fetchall()
        return {
            "watchlist_id": watch_id,
            "ship_name": watch_name,
            "region": region or location_legacy,
            "canonical_region": canonical,
            "advisories": [
                {
                    "country_code": r[0],
                    "country_name": r[1],
                    "region": r[2],
                    "level": r[3],
                    "title": r[4],
                    "summary": r[5],
                    "url": r[6],
                    "published": r[7],
                    "updated": r[8],
                    "fetched_at": str(r[9]) if r[9] else None,
                }
                for r in rows
            ],
        }


@app.delete("/notifications/{notif_id}")
async def delete_notification(notif_id: int, user_id: int = Depends(get_current_user)):
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("DELETE FROM notifications WHERE id = %s AND user_id = %s", (notif_id, user_id))
            conn.commit()
            return {"deleted": True}
    except Exception as e:
        print("Delete notification error: " + str(e))
        return {"deleted": False}


# ── SSDI / search endpoints (kept for cross-vertical compatibility) ────────────

@app.get("/ssdi/search")
async def ssdi_search(
    name: str,
    birth_year: str = None,
    middle_name: str = None,
    suffix: str = None,
    offset: int = 0,
    user_id: int = Depends(get_current_user)
):
    """Authenticated SSDI search — kept for cross-vertical compatibility."""
    return run_ssdi_query(name, birth_year, middle_name, suffix, offset)


@app.get("/ssdi/proxy")
async def ssdi_proxy(
    name: str,
    birth_year: str = None,
    middle_name: str = None,
    suffix: str = None,
    offset: int = 0
):
    """Unauthenticated SSDI proxy — kept for cross-vertical compatibility."""
    return run_ssdi_query(name, birth_year, middle_name, suffix, offset)


@app.get("/legacy/search")
async def legacy_search(name: str, user_id: int = Depends(get_current_user)):
    return {"name": name, "results": [], "count": 0}


@app.post("/search", response_model=List[ObituaryResult])
async def search_obituaries(search: ObituarySearch):
    with get_db() as conn:
        c = conn.cursor()
        results = []
        name = search.name.strip()
        if not name:
            return results
        search_normalized = normalize_name(name)
        query = """SELECT id, name, name_normalized, age, location, date, source, link, obit_text
                   FROM obituaries WHERE name ILIKE %s OR name_normalized ILIKE %s"""
        params = ["%" + name + "%", "%" + search_normalized + "%"]
        if search.birth_year:
            query += " AND date LIKE %s"
            params.append("%" + search.birth_year + "%")
        query += " ORDER BY scraped_at DESC LIMIT 20"
        c.execute(query, params)
        for row in c.fetchall():
            try:
                confidence = calculate_confidence(name, row[1] or "", search.location, row[4])
                results.append({
                    "id": row[0], "name": row[1] or "",
                    "age": row[3], "location": row[4], "date": row[5],
                    "source": "Legacy", "link": row[7],
                    "obit_text": row[8] if len(row) > 8 else None,
                    "confidence": confidence
                })
            except Exception as e:
                print("Error processing search result: " + str(e))
                continue
        return results


@app.get("/notifications")
async def get_notifications(user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT n.id, n.message, n.created_at, w.name,
                   COALESCE(o.link, '') as link, n.watchlist_id, n.source_type
            FROM notifications n
            JOIN watchlist w ON n.watchlist_id = w.id
            LEFT JOIN obituaries o ON n.obituary_id = o.id
            WHERE n.user_id = %s
            ORDER BY n.created_at DESC LIMIT 50
        """, (user_id,))
        notifications = []
        for row in c.fetchall():
            notifications.append({
                "id": row[0], "name": row[3],
                "message": row[1], "created_at": str(row[2]),
                "link": row[4] or "",
                "watchlist_id": row[5],
                "source_type": row[6] or "",
            })
        return notifications


# ── Background watchlist refresh (Wikipedia path — kept from v0.1.0) ──────────

def check_wikipedia_watchlist():
    """Background watchlist refresh. Person-shaped checks in v0.1.0; CDC VSP scrapers
    will replace this for ships in v0.1.2 / v0.1.3."""
    print("[" + str(datetime.now()) + "] Starting Wikipedia watchlist check...")
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT w.id, w.user_id, w.name, u.email, w.is_deceased
            FROM watchlist w
            JOIN users u ON w.user_id = u.id
            WHERE w.status = 'active'
            AND (w.is_deceased = FALSE OR w.is_deceased IS NULL)
        """)
        watchlist_items = c.fetchall()
        updated = 0
        notified = 0
        for watch in watchlist_items:
            watch_id, user_id, watch_name, user_email, was_deceased = watch
            try:
                normalized_name = normalize_name_for_wiki(watch_name)
                data = fetch_wiki_data_smart(normalized_name)
                if data.get("type") == "disambiguation":
                    continue
                extract = data.get("extract", "")
                death_date = extract_full_death_date(data)
                is_deceased = is_deceased_from_wiki(data)
                c.execute("""
                    UPDATE watchlist
                    SET is_deceased = %s,
                        wikipedia_description = %s,
                        death_year = %s,
                        wiki_last_checked = %s
                    WHERE id = %s
                """, (is_deceased, extract[:2000] if extract else None, death_date, datetime.utcnow(), watch_id))
                conn.commit()
                updated += 1
                if is_deceased:
                    c.execute(
                        "SELECT id FROM notifications WHERE watchlist_id = %s AND message LIKE %s",
                        (watch_id, "%Wikipedia%"))
                    if not c.fetchone():
                        death_info = (" Retired: " + str(death_date)) if death_date else ""
                        message = "Update on " + watch_name + "." + death_info
                        c.execute("""
                            INSERT INTO notifications
                                (user_id, watchlist_id, obituary_id, message, email_sent, source_type)
                            VALUES (%s, %s, NULL, %s, %s, 'wikipedia')
                        """, (user_id, watch_id, message, False))
                        conn.commit()
                        notified += 1
                        if user_email:
                            wiki_link = "https://en.wikipedia.org/wiki/" + urllib.parse.quote(watch_name)
                            sent = send_email_notification(user_email, watch_name, watch_name, None, wiki_link)
                            if sent:
                                c.execute(
                                    "UPDATE notifications SET email_sent = TRUE WHERE watchlist_id = %s AND message LIKE %s",
                                    (watch_id, "%Wikipedia%"))
                                conn.commit()
                time.sleep(0.5)
            except Exception as e:
                print("Wiki check error for " + watch_name + ": " + str(e))
                continue
    print("[" + str(datetime.now()) + "] Wikipedia check complete. " + str(updated) + " updated, " + str(notified) + " notified.")


# ── 24hr cron — runs both State Dept advisories and Wikipedia checks ──────────

def run_daily_cycle():
    """The combined daily check. State Dept first (cheap, single API call),
    then NOAA marine forecasts, then Wikipedia (per-watchlist sweeps)."""
    try:
        check_state_advisories()
    except Exception as e:
        print("[cron] check_state_advisories failed: " + str(e))
    try:
        check_marine_forecasts()
    except Exception as e:
        print("[cron] check_marine_forecasts failed: " + str(e))
    try:
        check_wikipedia_watchlist()
    except Exception as e:
        print("[cron] check_wikipedia_watchlist failed: " + str(e))


def run_scheduler():
    schedule.every(24).hours.do(run_daily_cycle)
    while True:
        schedule.run_pending()
        time.sleep(60)


@app.on_event("startup")
async def startup_event():
    init_db()
    print("Database initialized (CW v" + VERSION + ")")
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    print("Background scheduler started (24hr cycle: state advisories + wiki check)")
    threading.Thread(target=check_state_advisories, daemon=True).start()
    print("Initial State Dept advisory check started")
    threading.Thread(target=check_marine_forecasts, daemon=True).start()
    print("Initial NOAA marine forecast check started")
    threading.Thread(target=check_wikipedia_watchlist, daemon=True).start()
    print("Initial Wikipedia watchlist check started")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

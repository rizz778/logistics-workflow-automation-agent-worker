import os
from pathlib import Path
import psycopg
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

base_url = os.getenv("DATABASE_URL", "")

print(f"Loaded .env from: {env_path}")
if not base_url:
    print("ERROR: DATABASE_URL is not set!")
    exit(1)

# Mask password for display
masked = base_url
if "@" in base_url and ":" in base_url:
    parts = base_url.split("@")
    cred_parts = parts[0].split(":")
    masked = f"{cred_parts[0]}:****@{parts[1]}"
print(f"Testing URL: {masked}\n")

# Test 1: As configured
print("--- [Test 1] Testing configured DATABASE_URL ---")
try:
    conn = psycopg.connect(base_url, connect_timeout=8)
    print(">>> SUCCESS: Connected to Supabase PostgreSQL! <<<")
    with conn.cursor() as cur:
        cur.execute("SELECT current_user, current_database(), version();")
        res = cur.fetchone()
        print(f"Connected as: user='{res[0]}', db='{res[1]}'")
    conn.close()
    exit(0)
except Exception as e:
    print(f"Test 1 Failed: {e}\n")

# Test 2: Try switching port (5432 <-> 6543)
alt_url = None
if ":5432" in base_url:
    alt_url = base_url.replace(":5432", ":6543")
    print("--- [Test 2] Testing Transaction Pooler (Port 6543) ---")
elif ":6543" in base_url:
    alt_url = base_url.replace(":6543", ":5432")
    print("--- [Test 2] Testing Session Pooler (Port 5432) ---")

if alt_url:
    try:
        conn = psycopg.connect(alt_url, connect_timeout=8)
        print(">>> SUCCESS on alternate port! <<<")
        with conn.cursor() as cur:
            cur.execute("SELECT current_user, current_database();")
            res = cur.fetchone()
            print(f"Connected as: user='{res[0]}', db='{res[1]}'")
        conn.close()
        print(f"\nTIP: Update DATABASE_URL in .env to use this port!")
        exit(0)
    except Exception as e:
        print(f"Test 2 Failed: {e}\n")

#!/usr/bin/env python3
"""Script to upload existing JSON data to Supabase."""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# Supabase credentials
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://mzwkpzsepmvwegsxnala.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im16d2twenNlcG12d2Vnc3huYWxhIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NTkzNTIzNCwiZXhwIjoyMDgxNTExMjM0fQ.xITF8T3RjMWZYJfxb8vH8lOqrJOuVNrfIts6sLaEkqI")

DATA_PATH = Path(__file__).parent / "court_data.json"


def clean_and_upload(json_data: list) -> None:
    """Clean and upload JSON data to Supabase."""
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    clean_rows = []
    
    for item in json_data:
        # 1. Convert "2025-12-18 08:00 AM" to Python datetime object
        time_str = item.get('time', '')
        if not time_str:
            continue
        
        # Try different time formats
        dt_object = None
        time_formats = [
            "%Y-%m-%d %I:%M %p",  # "2025-12-18 08:00 AM"
            "%Y-%m-%d %H:%M",     # "2025-12-18 08:00"
        ]
        
        for fmt in time_formats:
            try:
                dt_object = datetime.strptime(time_str, fmt)
                break
            except ValueError:
                continue
        
        if dt_object is None:
            print(f"Warning: Could not parse time: {time_str}")
            continue
        
        # 2. Map JSON keys to your new SQL Schema columns
        row = {
            "court_name": item.get('court', ''),
            "start_time": dt_object.isoformat(),  # Supabase likes ISO strings
            "status": item.get('status', ''),
            "booking_link": item.get('link', ''),
            "raw_text": item.get('raw_text', ''),
            "updated_at": datetime.now(datetime.timezone.utc).isoformat()  # Mark when we last saw it
        }
        clean_rows.append(row)
    
    if not clean_rows:
        print("No rows to upload")
        return
    
    # 3. Upsert (The on_conflict parameter uses that Unique Constraint we made)
    response = supabase.table('court_slots').upsert(
        clean_rows,
        on_conflict='court_name,start_time'
    ).execute()
    
    print(f"Synced {len(clean_rows)} slots to Supabase.")


def main():
    """Main function to upload JSON data to Supabase."""
    if not DATA_PATH.exists():
        print(f"Error: {DATA_PATH} not found")
        sys.exit(1)
    
    print(f"Loading data from {DATA_PATH}...")
    with open(DATA_PATH, 'r') as f:
        json_data = json.load(f)
    
    print(f"Found {len(json_data)} slots in JSON file")
    print("Uploading to Supabase...")
    
    clean_and_upload(json_data)
    
    print("Done!")


if __name__ == "__main__":
    main()


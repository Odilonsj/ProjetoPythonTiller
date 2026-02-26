"""
BRONZE LAYER - Fetch and Save Brazilian National Holidays
API: Nager.Date (free, no authentication required)
Uses only Python standard library - no external packages needed!

This script fetches holidays from API and saves to JSON file.
Run this periodically (e.g., once per year) to update holiday data.
"""
import urllib.request
import json
from datetime import datetime
from pathlib import Path

def get_brazilian_holidays(year=None):
    if year is None:
        year = datetime.now().year

    url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/BR"

# Urlopen sends a GET request to the specified URL and returns a response object.
# The timeout is set to 10 seconds to prevent hanging if the API is unresponsive.
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            data = response.read()
            holidays = json.loads(data)

            print(f"\nBR Brazilian National Holidays - {year}")
            print("=" * 60)

        for holiday in holidays:
            date = holiday['date']
            name = holiday['localName']
            print(f"{date} - {name}")
        
        return holidays

    except Exception as e:
        print("=" * 60)
        print(f"Error fetching holidays: {e}")
        return []

# The save_holidays_to_file function takes the list of holidays and an optional filename.
# If no filename is provided, it defaults to "data/bronze/holidays.json"
def save_holidays_to_file(holidays, filename=None):
    try:
        if filename is None:
            filename = Path(__file__).resolve().parents[2] / "data" / "bronze" / "holidays.json"

        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(holidays, f, ensure_ascii=False, indent=2)
            
        print("=" * 60)
        print(f"✓ Holidays saved to {filename}")
        return True
    
    except Exception as e:
        print(f"Error saving holidays to file: {e}")
        return False

# The main block checks if the script is being run directly (not imported as a module).

if __name__ == "__main__":
    
    holidays = get_brazilian_holidays()
    if holidays:
        save_holidays_to_file(holidays)
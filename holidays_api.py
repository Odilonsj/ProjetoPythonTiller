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

def get_brazilian_holidays(year=None):
    if year is None:
        year = datetime.now().year

    url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/BR"

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

def save_holidays_to_file(holidays, filename="holidays.json"):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(holidays, f, ensure_ascii=False, indent=2)
            
        print("=" * 60)
        print(f"✓ Holidays saved to {filename}")
        return True
    
    except Exception as e:
        print(f"Error saving holidays to file: {e}")
        return False


if __name__ == "__main__":
    
    holidays = get_brazilian_holidays()
    if holidays:
        save_holidays_to_file(holidays, "holidays.json")
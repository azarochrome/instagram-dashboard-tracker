import os
import requests
from pyairtable import Table
from datetime import datetime

AIRTABLE_TOKEN = os.environ['AIRTABLE_API_KEY']
BASE_ID = os.environ['BASE_ID']
PROFILES_TABLE = "Instagram Profiles"
REELS_TABLE = "Reels"
SCRAPECREATORS_API_KEY = os.environ['SCRAPECREATORS_API_KEY']

usernames = [
    'emiladrisse', 'emilaphonia', 'emilaphyssa', 'emilaphyxia', 'emilarentha',
    'emilarionda', 'emilarionth', 'emilarionyx', 'emilarithia', 'emilarthesia',
    'emilarthona', 'emilaryaeth', 'emilasteria', 'emilastriva', 'emilathione',
    'emilavareth', 'emilavarethra', 'emilavessiaz', 'emilavessra', 'emilavindra',
    'emilavionae', 'emilavostra', 'emilaxireth', 'emilaylaraz', 'emilayrissa',
    'emilayzora', 'emilazarethra', 'emilazirion', 'emilazuvara', 'emilazyrel'
]

profiles_table = Table(AIRTABLE_TOKEN, BASE_ID, PROFILES_TABLE)
reels_table = Table(AIRTABLE_TOKEN, BASE_ID, REELS_TABLE)

HEADERS = {
    "x-api-key": SCRAPECREATORS_API_KEY
}

BASE_URL = "https://api.scrapecreators.com/v1/instagram"

def get_profile_data(username):
    """Get Instagram profile data using ScrapeCreators API"""
    url = f"{BASE_URL}/profile"
    params = {"handle": username}
    
    try:
        response = requests.get(url, headers=HEADERS, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch profile for {username}: {e}")
        return None

def get_reels_data(username):
    """Get Instagram reels data using ScrapeCreators API"""
    url = f"{BASE_URL}/reels"
    params = {"handle": username, "count": 20}  # Adjust count as needed
    
    try:
        response = requests.get(url, headers=HEADERS, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch reels for {username}: {e}")
        return None

def update_profile_in_airtable(username, profile_data):
    """Update or create profile record in Airtable"""
    if not profile_data:
        return
    
    try:
        # Extract relevant data from ScrapeCreators response
        follower_count = profile_data.get('follower_count', 0)
        full_name = profile_data.get('full_name', '')
        biography = profile_data.get('biography', '')
        is_verified = profile_data.get('is_verified', False)
        
        # Check if profile exists
        existing_records = profiles_table.all(formula=f"{{Instagram Handle}} = '{username}'")
        
        record_data = {
            "Instagram Handle": username,
            "Profile Name": full_name,
            "Follower Count": follower_count,
            "Biography": biography,
            "Verified": is_verified,
            "Last Checked": datetime.now().isoformat()
        }
        
        if existing_records:
            # Update existing record
            record_id = existing_records[0]['id']
            profiles_table.update(record_id, record_data)
            print(f"Updated profile for {username}: {follower_count} followers")
        else:
            # Create new record
            profiles_table.create(record_data)
            print(f"Created new profile for {username}: {follower_count} followers")
            
    except Exception as e:
        print(f"Failed to update profile for {username} in Airtable: {e}")

def update_reels_in_airtable(username, reels_data):
    """Update or create reel records in Airtable"""
    if not reels_data or 'items' not in reels_data:
        return
    
    try:
        for reel in reels_data['items']:
            reel_id = reel.get('id', '')
            if not reel_id:
                continue
                
            # Extract reel data
            view_count = reel.get('view_count', 0)
            like_count = reel.get('like_count', 0)
            comment_count = reel.get('comment_count', 0)
            caption = reel.get('caption', {}).get('text', '')
            reel_url = f"https://instagram.com/reel/{reel.get('code', '')}"
            created_at = reel.get('taken_at', '')
            
            # Check if reel exists
            existing_records = reels_table.all(formula=f"{{Reel ID}} = '{reel_id}'")
            
            record_data = {
                "Reel ID": reel_id,
                "Instagram Handle": username,
                "Reel URL": reel_url,
                "Caption": caption[:1000] if caption else '',  # Limit caption length
                "Views": view_count,
                "Likes": like_count,
                "Comments": comment_count,
                "Date Posted": created_at,
                "Last Checked": datetime.now().isoformat()
            }
            
            if existing_records:
                # Update existing record
                record_id = existing_records[0]['id']
                reels_table.update(record_id, record_data)
                print(f"Updated reel {reel_id}: {view_count} views, {like_count} likes")
            else:
                # Create new record
                reels_table.create(record_data)
                print(f"Created new reel {reel_id}: {view_count} views, {like_count} likes")
                
    except Exception as e:
        print(f"Failed to update reels for {username} in Airtable: {e}")

def main():
    """Main function to fetch data and update Airtable"""
    print("Starting Instagram data collection with ScrapeCreators...")
    
    for username in usernames:
        print(f"\nProcessing {username}...")
        
        # Get profile data
        profile_data = get_profile_data(username)
        if profile_data:
            update_profile_in_airtable(username, profile_data)
        
        # Get reels data
        reels_data = get_reels_data(username)
        if reels_data:
            update_reels_in_airtable(username, reels_data)
        
        # Small delay to be respectful to the API
        import time
        time.sleep(1)
    
    print("\nData collection and Airtable update complete.")

if __name__ == "__main__":
    main()
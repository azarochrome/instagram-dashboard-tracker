import os
import requests
from pyairtable import Api
from datetime import datetime
import time

AIRTABLE_TOKEN = os.environ['AIRTABLE_API_KEY']
BASE_ID = os.environ['BASE_ID']
PROFILES_TABLE = "Instagram Profiles"
REELS_TABLE = "Reels"
SCRAPECREATORS_API_KEY = os.environ['SCRAPECREATORS_API_KEY']

# PRODUCTION MODE: Complete list of all usernames (90 accounts)
# This will process all users for complete data collection
usernames = [
    'emiladrisse', 'emilaphonia', 'emilaphyssa', 'emilaphyxia', 'emilarentha',
    'emilarionda', 'emilarionth', 'emilarionyx', 'emilarithia', 'emilarthesia',
    'emilarthona', 'emilaryaeth', 'emilasteria', 'emilastriva', 'emilathione',
    'emilavareth', 'emilavarethra', 'emilavessiaz', 'emilavessra', 'emilavindra',
    'emilavionae', 'emilavostra', 'emilaxireth', 'emilaylaraz', 'emilayrissa',
    'emilayzora', 'emilazaireth', 'emilazareen', 'emilazauria', 'emilazavyn',
    'emilazelyra', 'emilazentha', 'emilazenthe', 'emilazenthae', 'emilazenya',
    'emilazetra', 'emilaziona', 'emilazionyx', 'emilazirexa', 'emilaznara',
    'emilazoniah', 'emilazrya', 'emilazrynna', 'emilazythra', 'emilenthessa',
    'emilenthira', 'emilestine', 'emilethoria', 'emileveraith', 'emilevontra',
    'emilevorae', 'emilevura', 'emilezariah', 'emilezethra', 'emilindrya',
    'emilionthra', 'emiliorixa', 'emilistraen', 'emilithyana', 'emilixarya',
    'emilixurina', 'emilondara', 'emilondraxa', 'emilonexra', 'emilorenxia',
    'emiloresta', 'emilorexia', 'emiloryllia', 'emiloryneth', 'emilorynxa',
    'emilourithia', 'emilovandria', 'emilovaxa', 'emilovendra', 'emilovessra',
    'emilovetha', 'emilozanna', 'emilozuria', 'emiltharion', 'emilundraeth',
    'emilunessa', 'emilunethra', 'emilunetra', 'emilurayna', 'emilurellia',
    'emilurethia', 'emilurithae', 'emiluryona', 'emilustraxa', 'emiluvetha',
    'emilazarethra', 'emilazirion', 'emilazuvara', 'emilazyrel'
]

# TESTING MODE (commented out for production)
# When testing, uncomment this and comment out the above list:
"""
usernames = [
    'emiladrisse', 
    'emilaphonia', 
    'emilaphyssa'
]
"""

# Initialize API and tables using the new syntax
api = Api(AIRTABLE_TOKEN)
profiles_table = api.table(BASE_ID, PROFILES_TABLE)
reels_table = api.table(BASE_ID, REELS_TABLE)

HEADERS = {
    "x-api-key": SCRAPECREATORS_API_KEY
}

BASE_URL = "https://api.scrapecreators.com/v1/instagram"

def get_profile_data(username):
    """Get Instagram profile data using ScrapeCreators API"""
    url = f"{BASE_URL}/profile"
    params = {"handle": username}
    
    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=30)
        
        if response.status_code == 402:
            print(f"Payment required for {username} - API quota exceeded")
            return None
        elif response.status_code == 404:
            print(f"Profile not found for {username}")
            return None
        elif response.status_code == 429:
            print(f"Rate limited for {username} - waiting 60 seconds")
            time.sleep(60)
            return None
        
        response.raise_for_status()
        data = response.json()
        
        # Check if the API returned success but with an error message
        if data.get('success') and data.get('error'):
            error_message = data.get('message', 'Unknown error')
            print(f"API error for {username}: {error_message}")
            return None
        
        # Check if we have valid user data
        if not data.get('success') or not data.get('data', {}).get('user'):
            print(f"No valid user data for {username}")
            return None
        
        return data
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch profile for {username}: {e}")
        return None

def get_reels_data(username):
    """Get Instagram reels data using ScrapeCreators API - Updated endpoint"""
    url = f"{BASE_URL}/user/reels/simple"
    params = {"handle": username, "amount": 20, "trim": False}
    
    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=60)  # Increased timeout for reels API
        
        if response.status_code == 402:
            print(f"Payment required for reels of {username} - API quota exceeded")
            return None
        elif response.status_code == 404:
            print(f"Reels not found for {username}")
            return None
        elif response.status_code == 429:
            print(f"Rate limited for reels of {username} - waiting 60 seconds")
            time.sleep(60)
            return None
        elif response.status_code == 400:
            print(f"Bad request for reels of {username}: {response.text}")
            return None
        elif response.status_code == 401:
            print(f"Unauthorized for reels of {username} - check API key")
            return None
        
        response.raise_for_status()
        data = response.json()
        
        # Validate that we got a list of reels
        if not isinstance(data, list):
            print(f"Invalid reels data format for {username}: expected list, got {type(data)}")
            return None
        
        return data
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch reels for {username}: {e}")
        return None
    except ValueError as e:
        print(f"Failed to parse JSON response for {username}: {e}")
        return None

def update_profile_in_airtable(username, profile_data):
    """Update or create profile record in Airtable"""
    if not profile_data:
        return
    
    try:
        # Extract relevant data from ScrapeCreators response
        # The API returns data in a nested structure: {'success': True, 'data': {'user': {...}}}
        user_data = profile_data.get('data', {}).get('user', {}) if isinstance(profile_data, dict) else {}
        
        # Extract follower count from the correct path
        follower_count = (
            user_data.get('edge_followed_by', {}).get('count', 0) or
            user_data.get('follower_count', 0) or 
            user_data.get('followers', 0) or 
            user_data.get('followers_count', 0) or
            0
        )
        
        # Extract full name
        full_name = (
            user_data.get('full_name', '') or 
            user_data.get('name', '') or 
            user_data.get('display_name', '') or
            ''
        )
        
        # Extract biography
        bio = user_data.get('biography', '') or user_data.get('bio', '')
        
        # Check if profile exists
        existing_records = profiles_table.all(formula=f"{{Instagram Handle}} = '{username}'")
        
        # Use field names from README - removed Profile Picture field completely
        record_data = {
            "Instagram Handle": username,
            "Profile Name": full_name,
            "Follower Count": follower_count,
            "Biography": bio,  # Changed from "Bio" to "Biography"
            "Last Checked": datetime.now().isoformat()  # Use ISO format for Airtable compatibility
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
        import traceback
        traceback.print_exc()

def update_reels_in_airtable(username, reels_data):
    """Update or create reel records in Airtable - Updated for new API structure"""
    if not reels_data:
        print(f"No reels data for {username}")
        return
    
    # The new API returns an array of objects with 'media' property
    if not isinstance(reels_data, list):
        print(f"Invalid reels data format for {username}: expected list, got {type(reels_data)}")
        return
    
    if not reels_data:
        print(f"No reels found for {username}")
        return
    
    try:
        for reel_item in reels_data:
            # The API response structure has changed - data is directly in reel_item, not nested under 'media'
            # Check if we have the old structure (with 'media') or new structure (direct data)
            if 'media' in reel_item:
                # Old structure - extract from media object
                media = reel_item.get('media')
                if not media:
                    print(f"No media object found in reel item for {username}")
                    continue
            else:
                # New structure - data is directly in reel_item
                media = reel_item
                
            # Extract reel data from the media object
            reel_id = media.get('pk', '') or media.get('id', '')
            if not reel_id:
                print(f"No reel ID found for {username}")
                continue
                
            # Extract engagement metrics from the correct paths
            view_count = (
                media.get('play_count', 0) or 
                media.get('ig_play_count', 0) or 
                media.get('video_view_count', 0) or 
                0
            )
            like_count = (
                media.get('edge_liked_by', {}).get('count', 0) or
                media.get('like_count', 0) or 
                0
            )
            comment_count = (
                media.get('edge_media_to_comment', {}).get('count', 0) or
                media.get('comment_count', 0) or 
                0
            )
            
            # Extract caption from the correct path in the API response
            caption = ''
            if media.get('edge_media_to_caption', {}).get('edges'):
                edges = media['edge_media_to_caption']['edges']
                if edges and len(edges) > 0:
                    caption = edges[0].get('node', {}).get('text', '')
            elif media.get('caption'):
                caption = media['caption']
            elif media.get('reusable_text_info'):
                # Try to get text from reusable_text_info if available
                text_info = media['reusable_text_info']
                if isinstance(text_info, list) and text_info:
                    caption = text_info[0].get('text', '')
            
            # Clean up caption - if it's a dict, try to extract text
            if isinstance(caption, dict):
                if 'text' in caption:
                    caption = caption['text']
                else:
                    caption = str(caption)  # Convert to string if it's a complex object
            
            # Create reel URL using the code
            reel_code = media.get('code', '')
            reel_url = f"https://instagram.com/reel/{reel_code}" if reel_code else ''
            
            # Extract creation date
            created_at = media.get('taken_at', '')
            if created_at:
                # Convert timestamp to datetime if needed
                try:
                    created_at = datetime.fromtimestamp(created_at).isoformat()
                except (ValueError, TypeError):
                    created_at = str(created_at)
            
            # Check if reel exists
            existing_records = reels_table.all(formula=f"{{Reel ID}} = '{reel_id}'")
            
            # Use field names from README - add username for reference
            record_data = {
                "Reel ID": reel_id,
                "Profile": username,  # Add username to show which profile this reel belongs to
                "Reel URL": reel_url,
                "Caption": str(caption)[:1000] if caption else '',  # Limit caption length and ensure it's a string
                "Views": view_count,
                "Likes": like_count,
                "Comments": comment_count,
                "Date Posted": created_at,
                "Last Checked": datetime.now().isoformat()  # Use ISO format for Airtable compatibility
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
        import traceback
        traceback.print_exc()

def main():
    """Main function to fetch data and update Airtable"""
    print("Starting Instagram data collection with ScrapeCreators...")
    print(f"🚀 PRODUCTION MODE: Processing all {len(usernames)} usernames for complete data collection")
    
    successful_profiles = 0
    successful_reels = 0
    failed_profiles = 0
    failed_reels = 0
    
    for i, username in enumerate(usernames, 1):
        print(f"\nProcessing {username} ({i}/{len(usernames)})...")
        
        # Get profile data
        profile_data = get_profile_data(username)
        if profile_data:
            try:
                update_profile_in_airtable(username, profile_data)
                successful_profiles += 1
            except Exception as e:
                print(f"Failed to update profile for {username}: {e}")
                failed_profiles += 1
        else:
            failed_profiles += 1
        
        # Get reels data
        reels_data = get_reels_data(username)
        if reels_data:
            try:
                update_reels_in_airtable(username, reels_data)
                successful_reels += 1
            except Exception as e:
                print(f"Failed to update reels for {username}: {e}")
                failed_reels += 1
        else:
            failed_reels += 1
        
        # Small delay to be respectful to the API
        time.sleep(2)
    
    print(f"\nData collection and Airtable update complete.")
    print(f"✅ Successfully processed {successful_profiles} profiles and {successful_reels} reels")
    print(f"❌ Failed to process {failed_profiles} profiles and {failed_reels} reels")
    print(f"📊 Total usernames processed: {len(usernames)}")

if __name__ == "__main__":
    main()
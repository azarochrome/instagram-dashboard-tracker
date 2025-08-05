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

# TESTING MODE: Only 3 usernames for faster testing
# This should process only 3 users, not 30!
usernames = [
    'emiladrisse', 
    'emilaphonia', 
    'emilaphyssa'
]

# FULL LIST (commented out for testing)
# When ready for production, uncomment this and comment out the above list:
"""
usernames = [
    'emiladrisse', 'emilaphonia', 'emilaphyssa', 'emilaphyxia', 'emilarentha',
    'emilarionda', 'emilarionth', 'emilarionyx', 'emilarithia', 'emilarthesia',
    'emilarthona', 'emilaryaeth', 'emilasteria', 'emilastriva', 'emilathione',
    'emilavareth', 'emilavarethra', 'emilavessiaz', 'emilavessra', 'emilavindra',
    'emilavionae', 'emilavostra', 'emilaxireth', 'emilaylaraz', 'emilayrissa',
    'emilayzora', 'emilazarethra', 'emilazirion', 'emilazuvara', 'emilazyrel'
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
        
        # Debug: Print the response structure
        print(f"Profile API response for {username}: {data}")
        
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
        
        # Debug: Print the response structure
        print(f"Reels API response for {username}: {len(data) if isinstance(data, list) else 'Not a list'} items")
        
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
            "Last Checked": datetime.now().strftime("%Y-%m-%d")  # Use date format instead of ISO
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
            # Extract the media object from each reel item
            media = reel_item.get('media')
            if not media:
                print(f"No media object found in reel item for {username}")
                continue
                
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
            
            # Use field names from README - temporarily remove Profile linking to debug
            record_data = {
                "Reel ID": reel_id,
                "Reel URL": reel_url,
                "Caption": str(caption)[:1000] if caption else '',  # Limit caption length and ensure it's a string
                "Views": view_count,
                "Likes": like_count,
                "Comments": comment_count,
                "Date Posted": created_at
            }
            
            # TODO: Fix Profile linking once we confirm the field structure
            # For now, let's get the reels working without the Profile link
            
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

def test_api_response(username):
    """Test function to debug API responses"""
    print(f"\n=== Testing API for {username} ===")
    
    # Test profile endpoint
    profile_url = f"{BASE_URL}/profile"
    profile_params = {"handle": username}
    
    try:
        profile_response = requests.get(profile_url, headers=HEADERS, params=profile_params, timeout=30)
        print(f"Profile Status: {profile_response.status_code}")
        print(f"Profile Headers: {dict(profile_response.headers)}")
        if profile_response.status_code == 200:
            profile_data = profile_response.json()
            print(f"Profile Data: {profile_data}")
        else:
            print(f"Profile Error: {profile_response.text}")
    except Exception as e:
        print(f"Profile Exception: {e}")
    
    # Test reels endpoint - Updated to new endpoint
    reels_url = f"{BASE_URL}/user/reels/simple"
    reels_params = {"handle": username, "amount": 5, "trim": False}
    
    try:
        reels_response = requests.get(reels_url, headers=HEADERS, params=reels_params, timeout=60)  # Increased timeout
        print(f"Reels Status: {reels_response.status_code}")
        print(f"Reels Headers: {dict(reels_response.headers)}")
        if reels_response.status_code == 200:
            reels_data = reels_response.json()
            print(f"Reels Data: {len(reels_data) if isinstance(reels_data, list) else 'Not a list'} items")
            if isinstance(reels_data, list) and reels_data:
                print(f"First reel structure: {reels_data[0]}")
        else:
            print(f"Reels Error: {reels_response.text}")
    except Exception as e:
        print(f"Reels Exception: {e}")
    
    print("=== End Test ===\n")

def test_reels_api_call(username, api_key=None):
    """
    Standalone function to test the Instagram reels API call
    
    Args:
        username (str): Instagram username/handle
        api_key (str, optional): API key (uses environment variable if not provided)
    
    Returns:
        dict: Response data or error information
    """
    import os
    
    # Use provided API key or environment variable
    if api_key is None:
        api_key = os.environ.get('SCRAPECREATORS_API_KEY')
        if not api_key:
            return {"error": "No API key provided or found in environment variables"}
    
    # API configuration
    base_url = "https://api.scrapecreators.com/v1/instagram"
    endpoint = "/user/reels/simple"
    url = f"{base_url}{endpoint}"
    
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json"
    }
    
    params = {
        "handle": username,
        "amount": 10,  # Number of reels to fetch
        "trim": False  # Get full response
    }
    
    print(f"Making API call to: {url}")
    print(f"Parameters: {params}")
    print(f"Headers: {dict(headers)}")
    
    try:
        # Make the API request
        response = requests.get(url, headers=headers, params=params, timeout=60)  # Increased timeout
        
        print(f"\nResponse Status: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        
        # Handle different status codes
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"✅ Success! Retrieved {len(data) if isinstance(data, list) else 'data'} items")
                
                # Parse and display reel information
                if isinstance(data, list) and data:
                    print(f"\n📊 Reels Summary for @{username}:")
                    print(f"Total reels found: {len(data)}")
                    
                    for i, reel_item in enumerate(data[:3], 1):  # Show first 3 reels
                        media = reel_item.get('media', {})
                        reel_id = media.get('pk', 'N/A')
                        play_count = media.get('play_count', 0)
                        like_count = media.get('like_count', 0)
                        comment_count = media.get('comment_count', 0)
                        code = media.get('code', 'N/A')
                        
                        print(f"  Reel {i}:")
                        print(f"    ID: {reel_id}")
                        print(f"    Code: {code}")
                        print(f"    Views: {play_count:,}")
                        print(f"    Likes: {like_count:,}")
                        print(f"    Comments: {comment_count:,}")
                        print(f"    URL: https://instagram.com/reel/{code}")
                        print()
                
                return {
                    "success": True,
                    "status_code": response.status_code,
                    "data": data,
                    "reel_count": len(data) if isinstance(data, list) else 0
                }
                
            except ValueError as e:
                error_msg = f"Failed to parse JSON response: {e}"
                print(f"❌ {error_msg}")
                return {"error": error_msg, "status_code": response.status_code, "raw_response": response.text}
        
        elif response.status_code == 400:
            error_msg = f"Bad request - check parameters"
            print(f"❌ {error_msg}: {response.text}")
            return {"error": error_msg, "status_code": response.status_code, "details": response.text}
        
        elif response.status_code == 401:
            error_msg = f"Unauthorized - check API key"
            print(f"❌ {error_msg}")
            return {"error": error_msg, "status_code": response.status_code}
        
        elif response.status_code == 402:
            error_msg = f"Payment required - API quota exceeded"
            print(f"❌ {error_msg}")
            return {"error": error_msg, "status_code": response.status_code}
        
        elif response.status_code == 404:
            error_msg = f"Reels not found for user @{username}"
            print(f"❌ {error_msg}")
            return {"error": error_msg, "status_code": response.status_code}
        
        elif response.status_code == 429:
            error_msg = f"Rate limited - too many requests"
            print(f"❌ {error_msg}")
            return {"error": error_msg, "status_code": response.status_code}
        
        else:
            error_msg = f"Unexpected status code: {response.status_code}"
            print(f"❌ {error_msg}: {response.text}")
            return {"error": error_msg, "status_code": response.status_code, "details": response.text}
    
    except requests.exceptions.Timeout:
        error_msg = "Request timed out"
        print(f"❌ {error_msg}")
        return {"error": error_msg}
    
    except requests.exceptions.ConnectionError:
        error_msg = "Connection error - check internet connection"
        print(f"❌ {error_msg}")
        return {"error": error_msg}
    
    except requests.exceptions.RequestException as e:
        error_msg = f"Request failed: {e}"
        print(f"❌ {error_msg}")
        return {"error": error_msg}
    
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        print(f"❌ {error_msg}")
        return {"error": error_msg}


def main():
    """Main function to fetch data and update Airtable"""
    print("Starting Instagram data collection with ScrapeCreators...")
    print(f"🧪 TESTING MODE: Processing only {len(usernames)} usernames for faster testing")
    print(f"🔍 DEBUG: Usernames list contains: {usernames}")
    print(f"🔍 DEBUG: Total usernames: {len(usernames)}")
    
    # Test the new reels API with the first username to ensure it's working
    test_username = usernames[0] if usernames else "emiladrisse"
    print(f"Testing new reels API with username: {test_username}")
    
    # Test the new reels API
    result = test_reels_api_call(test_username)
    if result.get("success"):
        print("✅ Reels API test successful! Proceeding with data collection...")
    else:
        print(f"❌ Reels API test failed: {result.get('error')}")
        print("Continuing with profile data collection only...")
    
    successful_profiles = 0
    successful_reels = 0
    
    for i, username in enumerate(usernames, 1):
        print(f"\nProcessing {username} ({i}/{len(usernames)})...")
        
        # Get profile data
        profile_data = get_profile_data(username)
        if profile_data:
            update_profile_in_airtable(username, profile_data)
            successful_profiles += 1
        
        # Get reels data
        reels_data = get_reels_data(username)
        if reels_data:
            update_reels_in_airtable(username, reels_data)
            successful_reels += 1
        
        # Small delay to be respectful to the API
        time.sleep(2)
    
    print(f"\nData collection and Airtable update complete.")
    print(f"Successfully processed {successful_profiles} profiles and {successful_reels} reels out of {len(usernames)} usernames.")

if __name__ == "__main__":
    main()
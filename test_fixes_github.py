#!/usr/bin/env python3
"""
Test script for GitHub Actions to verify the fixes
"""

import os
import sys
from datetime import datetime

# Add the current directory to the path so we can import our module
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scrapecreators_fetch_and_update import (
    get_profile_data, 
    get_reels_data, 
    update_profile_in_airtable, 
    update_reels_in_airtable
)

def test_single_user_github(username):
    """Test the data extraction and Airtable update for a single user in GitHub Actions"""
    print(f"🧪 Testing fixes for user: {username}")
    print("=" * 50)
    
    # Test profile data
    print("📊 Testing profile data extraction...")
    profile_data = get_profile_data(username)
    
    if profile_data:
        print("✅ Profile data fetched successfully")
        print(f"   Data structure: {type(profile_data)}")
        if isinstance(profile_data, dict):
            print(f"   Success: {profile_data.get('success', 'N/A')}")
            if profile_data.get('success'):
                user_data = profile_data.get('data', {}).get('user', {})
                print(f"   Follower count: {user_data.get('edge_followed_by', {}).get('count', 'N/A')}")
                print(f"   Biography: {user_data.get('biography', 'N/A')[:50]}...")
        
        # Test profile update
        print("\n📝 Testing profile update to Airtable...")
        try:
            update_profile_in_airtable(username, profile_data)
            print("✅ Profile update completed")
        except Exception as e:
            print(f"❌ Profile update failed: {e}")
    else:
        print("❌ Profile data fetch failed")
    
    # Test reels data
    print("\n🎬 Testing reels data extraction...")
    reels_data = get_reels_data(username)
    
    if reels_data:
        print("✅ Reels data fetched successfully")
        print(f"   Data structure: {type(reels_data)}")
        if isinstance(reels_data, list):
            print(f"   Number of reels: {len(reels_data)}")
            if reels_data:
                first_reel = reels_data[0]
                media = first_reel.get('media', {})
                print(f"   First reel ID: {media.get('pk', 'N/A')}")
                print(f"   First reel views: {media.get('play_count', 'N/A')}")
        
        # Test reels update
        print("\n📝 Testing reels update to Airtable...")
        try:
            update_reels_in_airtable(username, reels_data)
            print("✅ Reels update completed")
        except Exception as e:
            print(f"❌ Reels update failed: {e}")
    else:
        print("❌ Reels data fetch failed")
    
    print("\n" + "=" * 50)
    print("🎉 Test completed!")

if __name__ == "__main__":
    # Test with a single username
    test_username = "emiladrisse"
    test_single_user_github(test_username) 
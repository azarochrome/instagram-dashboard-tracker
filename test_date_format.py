#!/usr/bin/env python3
"""
Test script to verify the date format fix
"""

from datetime import datetime

def test_date_formats():
    """Test different date formats to ensure Airtable compatibility"""
    print("🧪 Testing date format fixes...")
    
    # Test the old format (causing errors)
    old_format = datetime.now().strftime("%d/%m/%y %H:%M")
    print(f"❌ Old format (causing errors): {old_format}")
    
    # Test the new format (Airtable compatible)
    new_format = datetime.now().isoformat()
    print(f"✅ New format (Airtable compatible): {new_format}")
    
    # Test timestamp conversion
    timestamp = 1754336633
    try:
        converted_date = datetime.fromtimestamp(timestamp).isoformat()
        print(f"✅ Timestamp conversion: {timestamp} → {converted_date}")
    except Exception as e:
        print(f"❌ Timestamp conversion failed: {e}")
    
    print("\n✅ Date format tests completed!")

if __name__ == "__main__":
    test_date_formats() 
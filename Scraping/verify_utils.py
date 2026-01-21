"""Utility verification script to ensure all required utils exist."""

import sys
from pathlib import Path

def verify_utils():
    """Check if all required utility modules are present."""
    required_utils = [
        'utils/db.py',
        'utils/state.py',
        'utils/retry.py',
        'utils/logging_config.py',
        'utils/json_flattener.py',
        'utils/http.py'
    ]
    
    missing = []
    found = []
    
    for util_path in required_utils:
        path = Path(util_path)
        if path.exists():
            found.append(util_path)
            print(f"✓ Found: {util_path}")
        else:
            missing.append(util_path)
            print(f"✗ Missing: {util_path}")
    
    print("\n" + "="*60)
    print(f"Found: {len(found)}/{len(required_utils)}")
    
    if missing:
        print("\n⚠️  WARNING: Missing required utilities:")
        for m in missing:
            print(f"  - {m}")
        print("\nThe scraper requires these utilities to function correctly.")
        return False
    else:
        print("\n✅ All required utilities are present!")
        return True

if __name__ == "__main__":
    success = verify_utils()
    sys.exit(0 if success else 1)
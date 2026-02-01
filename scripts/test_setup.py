"""
Quick test script for local development

Run this to verify your setup before deploying to GitHub Actions
"""
import os
import sys
from datetime import datetime, timedelta


def check_env_variables():
    """Check if all required environment variables are set"""
    print("🔍 Checking environment variables...")
    
    required_vars = {
        'TWELVE_DATA_API_KEY': 'Twelve Data API Key',
        'GCS_BUCKET_NAME': 'GCS Bucket Name',
    }
    
    missing = []
    for var, description in required_vars.items():
        value = os.getenv(var)
        if value:
            # Mask sensitive values
            masked = value[:4] + "..." + value[-4:] if len(value) > 8 else "***"
            print(f"  ✓ {description}: {masked}")
        else:
            print(f"  ✗ {description}: NOT SET")
            missing.append(var)
    
    if missing:
        print(f"\n❌ Missing variables: {', '.join(missing)}")
        print("\nSet them in .env file or as environment variables:")
        for var in missing:
            print(f"  $env:{var}='your_value'")
        return False
    
    return True


def check_credentials():
    """Check if GCP credentials are available"""
    print("\n🔍 Checking GCP credentials...")
    
    creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if creds_path and os.path.exists(creds_path):
        print(f"  ✓ Credentials file found: {creds_path}")
        return True
    else:
        print("  ⚠️  GOOGLE_APPLICATION_CREDENTIALS not set")
        print("     Set it for local development:")
        print("     $env:GOOGLE_APPLICATION_CREDENTIALS='path/to/key.json'")
        return False


def test_imports():
    """Test if all required packages are installed"""
    print("\n🔍 Testing Python packages...")
    
    packages = [
        ('pandas', 'Data processing'),
        ('twelvedata', 'Twelve Data API'),
        ('google.cloud.storage', 'Google Cloud Storage'),
        ('pyarrow', 'Parquet support'),
    ]
    
    missing = []
    for package, description in packages:
        try:
            __import__(package)
            print(f"  ✓ {description}: installed")
        except ImportError:
            print(f"  ✗ {description}: NOT INSTALLED")
            missing.append(package)
    
    if missing:
        print(f"\n❌ Missing packages. Install with:")
        print(f"  pip install {' '.join(missing)}")
        return False
    
    return True


def test_gcs_connection():
    """Test connection to GCS"""
    print("\n🔍 Testing GCS connection...")
    
    try:
        from google.cloud import storage
        
        bucket_name = os.getenv('GCS_BUCKET_NAME')
        if not bucket_name:
            print("  ⚠️  GCS_BUCKET_NAME not set, skipping")
            return True
        
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        if bucket.exists():
            print(f"  ✓ Bucket '{bucket_name}' is accessible")
            return True
        else:
            print(f"  ✗ Bucket '{bucket_name}' not found")
            return False
            
    except Exception as e:
        print(f"  ✗ GCS connection failed: {e}")
        return False


def test_api_connection():
    """Test connection to Twelve Data API"""
    print("\n🔍 Testing Twelve Data API connection...")
    
    try:
        from twelvedata import TDClient
        
        api_key = os.getenv('TWELVE_DATA_API_KEY')
        if not api_key:
            print("  ⚠️  TWELVE_DATA_API_KEY not set, skipping")
            return True
        
        td = TDClient(apikey=api_key)
        
        # Simple test call - get latest quote
        ts = td.time_series(
            symbol="EUR/USD",
            interval="1day",
            outputsize=1
        )
        
        data = ts.as_json()
        print(f"  ✓ API connection successful")
        print(f"     Test fetch: 1 record for EUR/USD")
        return True
        
    except Exception as e:
        print(f"  ✗ API connection failed: {e}")
        return False


def run_full_test():
    """Run a full test of the extraction script"""
    print("\n🚀 Running full extraction test...")
    print("   This will fetch data for yesterday and upload to GCS")
    
    # Import the extraction script
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
    
    try:
        from extract_forex import run_daily_extraction
        
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        success = run_daily_extraction(
            symbol='EUR/USD',
            interval='5min',  # Use 5min as default interval
            date=yesterday,
            bucket_name=os.getenv('GCS_BUCKET_NAME')
        )
        
        if success:
            print("\n✅ FULL TEST PASSED!")
            return True
        else:
            print("\n❌ FULL TEST FAILED!")
            return False
            
    except Exception as e:
        print(f"\n❌ FULL TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main test runner"""
    print("=" * 80)
    print("🧪 Forex Data Pipeline - Local Setup Test")
    print("=" * 80)
    
    # Load .env file if it exists
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("✓ Loaded .env file\n")
    except ImportError:
        print("⚠️  python-dotenv not installed, using system environment variables\n")
    
    # Run all checks
    checks = [
        ("Environment Variables", check_env_variables),
        ("GCP Credentials", check_credentials),
        ("Python Packages", test_imports),
        ("GCS Connection", test_gcs_connection),
        ("API Connection", test_api_connection),
    ]
    
    results = []
    for name, check_func in checks:
        try:
            result = check_func()
            results.append(result)
        except Exception as e:
            print(f"\n❌ {name} check crashed: {e}")
            results.append(False)
    
    # Summary
    print("\n" + "=" * 80)
    print("📊 Test Summary")
    print("=" * 80)
    
    passed = sum(results)
    total = len(results)
    
    for i, (name, _) in enumerate(checks):
        status = "✅ PASS" if results[i] else "❌ FAIL"
        print(f"{status} - {name}")
    
    print("=" * 80)
    
    if all(results):
        print("🎉 All checks passed!")
        
        # Ask if user wants to run full test
        print("\n" + "=" * 80)
        response = input("Run full extraction test? (y/n): ").lower()
        
        if response == 'y':
            success = run_full_test()
            sys.exit(0 if success else 1)
        else:
            print("Skipping full test. Run manually:")
            print("  python scripts/extract_forex.py --date 2026-01-31")
            sys.exit(0)
    else:
        print("❌ Some checks failed. Fix issues above before deploying.")
        sys.exit(1)


if __name__ == "__main__":
    main()

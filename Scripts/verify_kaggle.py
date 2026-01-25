import sys
from dotenv import load_dotenv
load_dotenv()

try:
    import kaggle
    print("Kaggle module found.")
except ImportError:
    print("Kaggle module NOT found.")
    sys.exit(1)

try:
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()
    print("Kaggle authenticated successfully.")
except Exception as e:
    print(f"Kaggle authentication failed: {e}")
    sys.exit(1)

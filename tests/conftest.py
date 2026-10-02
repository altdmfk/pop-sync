import os
import sys

# Add project root to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# Set environment variables for tests
os.environ["ADMIN_SECRET_TOKEN"] = "super-secret-admin"

import os
import sys
import pytest

# Add the backend directory to the Python path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, backend_dir)

# Register custom marks
def pytest_configure(config):
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "asyncio: mark test as async test")

# Setup AWS credentials for tests
@pytest.fixture(autouse=True)
def setup_aws_credentials():
    """Setup AWS credentials for all tests."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
# Add the project root directory to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root) 
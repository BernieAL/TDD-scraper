"""

for the script we want to test - identify all things it does:

    we want to write a test for each function 
    receives events/triggers
    validates input
    calls scraperOrchestrator
    handles responses/errors
    sends notifications
    return responses
    

"""

#create ficture that will be usedimport pytest
import pytest
from pathlib import Path
from unittest.mock import Mock, patch

class TestScraperWorker:

    @pytest.fixture
    def test():
        pass
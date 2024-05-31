import os

import pytest


@pytest.fixture
def monitor_input_json():
    return os.path.abspath("monitor_input.json")

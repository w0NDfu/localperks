"""Load the adjacent service without installing or writing into its source tree."""
import os
import sys
from pathlib import Path

import pytest

SERVICE = Path(os.environ.get('AGENT_SERVICE_ROOT', Path(__file__).resolve().parents[1] / 'agent-service'))
sys.path[:0] = [str(SERVICE), str(SERVICE / 'tests'), str(Path(__file__).parent)]



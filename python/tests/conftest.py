import os
import sys
sys.path.append(os.path.abspath(".."))

import pytest

from src.scraper import Scraper
from src.scraper_ligas import ScraperLigas
from src.config import ENDPOINT_LIGAS

@pytest.fixture
def scraper():

	return Scraper(ENDPOINT_LIGAS)

@pytest.fixture
def scraper_ligas():

	return ScraperLigas(ENDPOINT_LIGAS)
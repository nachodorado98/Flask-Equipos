import pytest
import pandas as pd

from src.extraer import extraerData
from src.excepciones import LigasError

def test_extraer_data_error_endpoint():

	assert extraerData("/en/players") is None

def test_extraer_data():

	data=extraerData()

	assert isinstance(data, pd.DataFrame)
	assert not data.empty
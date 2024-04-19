"""BNMP testing suite."""

from typing import Dict
import pytest

from mapper import mapper
from scraper import BulkScraper, DetailsScraper
from workflow_setup import WorkflowSetup


@pytest.mark.skip()
def test_cookie_checker():
    """Test cookie checker."""
    pass


def test_mapper():
    """Test mapper."""
    api_map = mapper({"state_id": 1})

    # is a dictionary
    assert isinstance(api_map, Dict)

    # must contain a "api_map" key
    assert api_map.get("api_map")

    # must have only one value (specific for state_id 1) on its list
    assert len(api_map["api_map"]) == 1

    # its list must have the all the fundamental keys
    assert set(["state", "state_probe", "include_desc"]).issubset(
        api_map["api_map"][0]
    )


def test_scrapers():
    """Test scrapers."""
    bulk = BulkScraper()
    assert bulk
    
    details = DetailsScraper()
    assert details


@pytest.mark.skip()
def test_parser():
    """Test parser."""
    pass


@pytest.mark.skip()
def test_identifier():
    """Test identifier."""
    pass


def test_workflow_cleanup():
    """Test workflow cleanup."""
    wsetup = WorkflowSetup()
    assert wsetup


def test_workflow_setup():
    """Test workflow setup."""
    wsetup = WorkflowSetup()
    assert wsetup

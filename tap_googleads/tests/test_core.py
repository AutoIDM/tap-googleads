"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests
from tap_googleads.tap import TapGoogleAds


SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    ),
    "client_id": "12345",
    "client_secret": "12345",
    "developer_token": "12345",
    "refresh_token": "12345",
    "customer_id": "12345",
    "login_customer_id": "12345",
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapGoogleAds, config=SAMPLE_CONFIG)
    for test in tests:
        test()

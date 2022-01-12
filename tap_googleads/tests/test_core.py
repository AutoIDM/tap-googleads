"""Tests standard tap features using the built-in SDK tests library."""

import datetime
from pathlib import Path

from singer_sdk.testing import get_standard_tap_tests
from singer_sdk.tap_base import Tap
from singer_sdk.streams.core import Stream
from singer_sdk.exceptions import FatalAPIError
from tap_googleads.tap import TapGoogleAds
import pytest
import json

import responses
import requests

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
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

@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps

def test_customer_not_enabled(mocked_responses):

    auth_response = {
        "access_token": "access_granted",
        "expires_in": "10000000",
    }
    
    customer_not_enabled_body = {
        "error": {
            "code": 403,
            "message": "The caller does not have permission",
            "status": "PERMISSION_DENIED",
            "details": [
                {
                    "@type": "type.googleapis.com/google.ads.googleads.v8.errors.GoogleAdsFailure",
                    "errors": [
                        {
                            "errorCode": {"authorizationError": "CUSTOMER_NOT_ENABLED"},
                            "message": "The customer can't be used because it isn't enabled.",
                        }
                    ],
                    "requestId": "y5qNuM6d1jVb6km9FEa1GG",
                }
            ],
        }
    }
    mocked_responses.add(
        responses.POST,
        #TODO cleanup long url, googleads.googleapis.com/* would suffice
        "https://googleads.googleapis.com/v8/customers/12345/googleAds:search?pageSize=10000&query=%0A%20%20%20%20%20%20%20%20SELECT%20campaign.id,%20campaign.name%20FROM%20campaign%20ORDER%20BY%20campaign.id%0A%20%20%20%20%20%20%20%20",
        body=json.dumps(customer_not_enabled_body).encode("utf-8"),
        status=403,
        content_type="application/json",
    )
    mocked_responses.add(
        responses.POST,
        "https://www.googleapis.com/oauth2/v4/token?refresh_token=12345&client_id=12345&client_secret=12345&grant_type=refresh_token", #TODO Clean up
        body=json.dumps(auth_response).encode("utf-8"),
        status=200,
        content_type="application/json"
    )
    tap: Tap = TapGoogleAds(config=SAMPLE_CONFIG, parse_env_config=True)
    campaign_stream: Stream = tap.streams["campaign"]
    context = {
            "client_id":"12345"
    }
    campaign_stream.sync(context)
#    with pytest.raises(FatalAPIError):
#        campaign_stream.sync(context)


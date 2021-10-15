"""GoogleAds tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_googleads.streams import (
    CustomerStream,
    CampaignsStream,
    AdGroupsStream,
    AdGroupsPerformance,
)
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    CustomerStream,
    CampaignsStream,
    AdGroupsStream,
    AdGroupsPerformance,
]


class TapGoogleAds(Tap):
    """GoogleAds tap class."""
    name = "tap-googleads"

    # TODO: Add Descriptions
    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
        ),
        th.Property(
            "developer_token",
            th.StringType,
            required=True,
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True,
        ),
        th.Property(
            "customer_id",
            th.StringType,
            required=True,
        ),
        th.Property(
            "login_customer_id",
            th.StringType,
            required=True,
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

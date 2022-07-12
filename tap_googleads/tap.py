"""GoogleAds tap class."""

import datetime
from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from datetime import date, timedelta

from tap_googleads.streams import (
    CampaignsStream,
    CustomerStream,
    AdGroupsStream,
    AdGroupsPerformance,
    AccessibleCustomers,
    CustomerHierarchyStream,
    CampaignPerformance,
    CampaignPerformanceByAgeRangeAndDevice,
    CampaignPerformanceByGenderAndDevice,
    CampaignPerformanceByLocation,
    GeotargetsStream,
    ConversionsByLocation,
    CampaignLabel,
)

STREAM_TYPES = [
    CampaignsStream,
    CustomerStream,
    AdGroupsStream,
    AdGroupsPerformance,
    AccessibleCustomers,
    CustomerHierarchyStream,
    CampaignPerformance,
    CampaignPerformanceByAgeRangeAndDevice,
    CampaignPerformanceByGenderAndDevice,
    CampaignPerformanceByLocation,
    GeotargetsStream,
    ConversionsByLocation,
    CampaignLabel,
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
            description="ClientID from Oauth Setup",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            description="ClientSecret from Oauth Setup",
        ),
        th.Property(
            "developer_token",
            th.StringType,
            required=True,
            description="Developer Token from Google Ads Console",
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True,
            description="Refresh Token from Oauth dance",
        ),
        th.Property(
            "customer_id",
            th.StringType,
            required=True,
            description="Customer ID from Google Ads Console, note this should be the top level client. This tap will pull all subaccounts",
        ),
        th.Property(
            "login_customer_id",
            th.StringType,
            required=True,
            description="Customer ID that has access to the customer_id, note that they can be the same, but they don't have to be as this could be a Manager account",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            default=(date.today() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            required=True,
            description="Date to start our search from, applies to Streams where there is a filter date. Note that Google responds to Data in buckets of 1 Day increments",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            default=date.today().strftime("%Y-%m-%dT%H:%M:%SZ"),
            required=True,
            description="Date to end our search on, applies to Streams where there is a filter date. Note that the query is BETWEEN start_date AND end_date",
        ),
        th.Property(
            "start_date",
            th.StringType,
            required=False,
            default=datetime.date.today().isoformat(),
        ),
        th.Property(
            "end_date",
            th.StringType,
            required=False,
            default=datetime.date.today().isoformat(),
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

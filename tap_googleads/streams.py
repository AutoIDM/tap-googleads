"""Stream type classes for tap-googleads."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_googleads.client import GoogleAdsStream
from tap_googleads.auth import GoogleAdsAuthenticator

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


def flatten(row: dict, keys_w_prefix=None, keys_no_prefix=None) -> dict:
    """Flatten one level for typical Google Ads row."""
    keys_w_prefix = keys_w_prefix if keys_w_prefix else []
    keys_no_prefix = keys_no_prefix if keys_no_prefix else []

    new_row = {}
    for prefix in keys_w_prefix:
        subitem = row.pop(prefix)
        for k, v in subitem.items():
            new_row[f"{prefix}_{k}"] = v

    for key in keys_no_prefix:
        subitem = row.pop(key)
        new_row.update(subitem)

    new_row.update(row)
    return new_row




class CustomerStream(GoogleAdsStream):
    """Define custom stream."""

    @property
    def path(self):
        return "/customers/" + self.config["customer_id"]

    name = "customers"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "customer.json"


class AccessibleCustomers(GoogleAdsStream):
    """Accessible Customers"""

    path = "/customers:listAccessibleCustomers"
    name = "accessible_customers"
    primary_keys = None
    replication_key = None
    # TODO add an assert for one record
    #    schema_filepath = SCHEMAS_DIR / "customer.json"
    schema = th.PropertiesList(
        th.Property("resourceNames", th.ArrayType(th.StringType))
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"resourceNames": record["resourceNames"]}


class CustomerHierarchyStream(GoogleAdsStream):
    """
    Customer Hierarchy, inspiration from Google here
    https://developers.google.com/google-ads/api/docs/account-management/get-account-hierarchy.

    This stream is stictly to be the Parent Stream, to let all Child Streams
    know when to query the down stream apps.

    """

    # TODO add a seperate stream to get the Customer information and return i
    rest_method = "POST"

    @property
    def path(self):
        # Paramas
        path = "/customers/{client_id}"
        path = path + "/googleAds:search"
        path = path + "?pageSize=10000"
        path = path + f"&query={self.gaql}"
        return path

    @property
    def gaql(self):
        return """
	        SELECT customer_client.client_customer
                 , customer_client.level
                 , customer_client.manager
                 , customer_client.descriptive_name
                 , customer_client.currency_code
                 , customer_client.time_zone
                 , customer_client.id
            FROM customer_client
            WHERE customer_client.level <= 1
	    """

    records_jsonpath = "$.results[*]"
    name = "customer_hierarchystream"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = AccessibleCustomers
    # schema_filepath = SCHEMAS_DIR / "campaign.json"
    schema = th.PropertiesList(
        th.Property("resourceName", th.StringType),
        th.Property("clientCustomer", th.StringType),
        th.Property("level", th.StringType),
        th.Property("timeZone", th.StringType),
        th.Property("manager", th.BooleanType),
        th.Property("descriptiveName", th.StringType),
        th.Property("currencyCode", th.StringType),
        th.Property("id", th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        return row["customerClient"]

    # Goal of this stream is to send to children stream a dict of
    # login-customer-id:customer-id to query for all queries downstream
    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        client_ids = []
        if self.config["login_customer_id"]:
            client_ids = [self.config["login_customer_id"]]
        else:
            # TODO when implementing this the headers need to be set properly
            client_ids = context["resourceNames"]

        for client in client_ids:
            client_id = client.split("/")[-1]
            context["client_id"] = client_id
            for row in self.request_records(context):
                row = self.post_process(row, context)
                # Don't search Manager accounts as we can't query them for everything
                if row["manager"] == True:
                    continue
                yield row

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"client_id": record["id"]}


class GeotargetsStream(GoogleAdsStream):
    """Geotargets, worldwide, constant across all customers"""

    rest_method = "POST"

    @property
    def path(self):
        # Paramas
        path = "/customers/{login_customer_id}"
        path = path + "/googleAds:search"
        path = path + "?pageSize=10000"
        path = path + f"&query={self.gaql}"
        return path

    gaql = """
        SELECT geo_target_constant.canonical_name
             , geo_target_constant.country_code
             , geo_target_constant.id
             , geo_target_constant.name
             , geo_target_constant.status
             , geo_target_constant.target_type 
        FROM geo_target_constant
    """
    records_jsonpath = "$.results[*]"
    name = "geo_target_constant"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "geo_target_constant.json"
    parent_stream_type = None  # Override ReportsStream default as this is a constant

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        return row["geoTargetConstant"]


class ReportsStream(GoogleAdsStream):
    rest_method = "POST"
    parent_stream_type = CustomerHierarchyStream

    @property
    def gaql(self):
        raise NotImplementedError

    @property
    def path(self):
        # Paramas
        path = "/customers/{client_id}"
        path = path + "/googleAds:search"
        path = path + "?pageSize=10000"
        path = path + f"&query={self.gaql}"
        return path


class CampaignsStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return """
            SELECT campaign.id
                 , campaign.name 
            FROM campaign 
            ORDER BY campaign.id
        """

    records_jsonpath = "$.results[*]"
    name = "campaign"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign.json"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        return flatten(row, keys_no_prefix=['campaign'])


class AdGroupsStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return """
            SELECT ad_group.url_custom_parameters
                 , ad_group.type
                 , ad_group.tracking_url_template
                 , ad_group.targeting_setting.target_restrictions
                 , ad_group.target_roas
                 , ad_group.target_cpm_micros
                 , ad_group.status
                 , ad_group.target_cpa_micros
                 , ad_group.resource_name
                 , ad_group.percent_cpc_bid_micros
                 , ad_group.name
                 , ad_group.labels
                 , ad_group.id
                 , ad_group.final_url_suffix
                 , ad_group.explorer_auto_optimizer_setting.opt_in
                 , ad_group.excluded_parent_asset_field_types
                 , ad_group.effective_target_roas_source
                 , ad_group.effective_target_roas
                 , ad_group.effective_target_cpa_source
                 , ad_group.effective_target_cpa_micros
                 , ad_group.display_custom_bid_dimension
                 , ad_group.cpv_bid_micros
                 , ad_group.cpm_bid_micros
                 , ad_group.cpc_bid_micros
                 , ad_group.campaign
                 , ad_group.base_ad_group
                 , ad_group.ad_rotation_mode
            FROM ad_group 
        """

    records_jsonpath = "$.results[*]"
    name = "adgroups"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "ad_group.json"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        return row["adGroup"]


class AdGroupsPerformance(ReportsStream):
    """AdGroups Performance"""

    gaql = """
        SELECT campaign.id
             , ad_group.id
             , metrics.impressions
             , metrics.clicks
             , metrics.cost_micros
             , segments.date
        FROM ad_group
        WHERE segments.date DURING LAST_7_DAYS
    """
    records_jsonpath = "$.results[*]"
    name = "adgroupsperformance"
    primary_keys = ["campaign_id", "adGroup_id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "adgroups_performance.json"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        return flatten(
            row,
            keys_w_prefix=["campaign", "adGroup"],
            keys_no_prefix=["segments", "metrics"]
        )


class CampaignPerformance(ReportsStream):
    """Campaign Performance"""

    gaql = """
        SELECT 
              campaign.id
            , campaign.name
            , campaign.status
            , segments.device
            , segments.date
            , metrics.impressions
            , metrics.clicks
            , metrics.ctr
            , metrics.average_cpc
            , metrics.cost_micros 
        FROM campaign 
        WHERE segments.date DURING LAST_7_DAYS
    """
    records_jsonpath = "$.results[*]"
    name = "campaign_performance"
    primary_keys = ["id", "date"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_performance.json"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        return flatten(row, keys_no_prefix=["campaign", "metrics", "segments"])


class CampaignPerformanceByAgeRangeAndDevice(ReportsStream):
    """Campaign Performance By Age Range and Device"""

    gaql = """
        SELECT ad_group_criterion.age_range.type
             , campaign.name
             , campaign.id
             , campaign.status
             , ad_group.name
             , ad_group.id
             , segments.date
             , segments.device
             , ad_group_criterion.system_serving_status
             , ad_group_criterion.bid_modifier
             , metrics.clicks
             , metrics.impressions
             , metrics.ctr
             , metrics.average_cpc
             , metrics.cost_micros
             , campaign.advertising_channel_type 
        FROM age_range_view 
        WHERE segments.date DURING LAST_7_DAYS
    """
    records_jsonpath = "$.results[*]"
    name = "campaign_performance_by_age_range_and_device"
    primary_keys = ["campaign_id", "adGroup_id", "adGroupCriterion_ageRange_type", "device", "date"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_performance_by_age_range_and_device.json"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        new_row = flatten(
            row,
            keys_w_prefix=["campaign", "adGroup", "adGroupCriterion", "ageRangeView"],
            keys_no_prefix=["metrics", "segments"],
        )
        age_rage_type = new_row.pop("adGroupCriterion_ageRange")
        new_row["adGroupCriterion_ageRange_type"] = age_rage_type["type"]
        return new_row


class CampaignPerformanceByGenderAndDevice(ReportsStream):
    """Campaign Performance By Age Range and Device"""

    gaql = """
        SELECT ad_group_criterion.gender.type
             , campaign.name
             , campaign.status
             , campaign.id
             , ad_group.name
             , ad_group.id
             , segments.date
             , segments.device
             , ad_group_criterion.system_serving_status
             , ad_group_criterion.bid_modifier
             , metrics.clicks
             , metrics.impressions
             , metrics.ctr
             , metrics.average_cpc
             , metrics.cost_micros
             , campaign.advertising_channel_type 
        FROM gender_view 
        WHERE segments.date DURING LAST_7_DAYS
    """
    records_jsonpath = "$.results[*]"
    name = "campaign_performance_by_gender_and_device"
    primary_keys = ["campaign_id", "adGroup_id", "adGroupCriterion_gender_type", "device", "date"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_performance_by_gender_and_device.json"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        new_row = flatten(
            row,
            keys_w_prefix=["campaign", "adGroup", "adGroupCriterion", "genderView"],
            keys_no_prefix=["metrics", "segments"],
        )
        return flatten(
            new_row,
            keys_w_prefix=["adGroupCriterion_gender"],
        )



class CampaignPerformanceByLocation(ReportsStream):
    """Campaign Performance By Age Range and Device"""

    gaql = """
        SELECT campaign_criterion.location.geo_target_constant
             , campaign.name
             , campaign.id
             , campaign_criterion.bid_modifier
             , segments.date
             , metrics.clicks
             , metrics.impressions
             , metrics.ctr
             , metrics.average_cpc
             , metrics.cost_micros 
        FROM location_view 
        WHERE segments.date DURING LAST_7_DAYS 
          AND campaign_criterion.status != 'REMOVED'
    """
    records_jsonpath = "$.results[*]"
    name = "campaign_performance_by_location"
    primary_keys = ["campaign_id", "date", "campaignCriterion_location_geoTargetConstant"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_performance_by_location.json"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        new_row = flatten(
            row,
            keys_w_prefix=["campaign", "campaignCriterion", "locationView"],
            keys_no_prefix=["metrics", "segments"],
        )
        return flatten(
            new_row,
            keys_w_prefix=["campaignCriterion_location"],
        )


class ConversionsByLocation(ReportsStream):
    """Conversions By Location"""

    gaql = """
        SELECT campaign_criterion.location.geo_target_constant
             , campaign.name
             , campaign.id
             , campaign_criterion.bid_modifier
             , segments.date
             , segments.conversion_action_category
             , metrics.conversions 
        FROM location_view 
        WHERE segments.date DURING LAST_7_DAYS 
          AND campaign_criterion.status != 'REMOVED'
    """
    records_jsonpath = "$.results[*]"
    name = "conversion_by_location"
    primary_keys = ["campaign_id", "date", "campaignCriterion_location_geoTargetConstant"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "conversion_by_location.json"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        new_row = flatten(
            row,
            keys_w_prefix=["campaign", "campaignCriterion", "locationView"],
            keys_no_prefix=["metrics", "segments"],
        )
        return flatten(
            new_row,
            keys_w_prefix=["campaignCriterion_location"],
        )

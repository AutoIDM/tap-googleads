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
	SELECT
          customer_client.client_customer,
          customer_client.level,
          customer_client.manager,
          customer_client.descriptive_name,
          customer_client.currency_code,
          customer_client.time_zone,
          customer_client.id
        FROM customer_client
        WHERE customer_client.level <= 1
	"""

    records_jsonpath = "$.results[*]"
    name = "customer_hierarchystream"
    primary_keys = ["customer_client.id"]
    replication_key = None
    parent_stream_type = AccessibleCustomers
    # schema_filepath = SCHEMAS_DIR / "campaign.json"
    schema = th.PropertiesList(
        th.Property(
            "customerClient",
            th.ObjectType(
                th.Property("resourceName", th.StringType),
                th.Property("clientCustomer", th.StringType),
                th.Property("level", th.StringType),
                th.Property("timeZone", th.StringType),
                th.Property("manager", th.BooleanType),
                th.Property("descriptiveName", th.StringType),
                th.Property("currencyCode", th.StringType),
                th.Property("id", th.StringType),
            ),
        )
    ).to_dict()

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
                if row["customerClient"]["manager"] == True:
                    continue
                yield row

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"client_id": record["customerClient"]["id"]}


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
    SELECT geo_target_constant.canonical_name, geo_target_constant.country_code, geo_target_constant.id, geo_target_constant.name, geo_target_constant.status, geo_target_constant.target_type FROM geo_target_constant
    """
    records_jsonpath = "$.results[*]"
    name = "geo_target_constant"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "geo_target_constant.json"
    parent_stream_type = None  # Override ReportsStream default as this is a constant


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
        SELECT campaign.id, campaign.name FROM campaign ORDER BY campaign.id
        """

    records_jsonpath = "$.results[*]"
    name = "campaign"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign.json"


class AdGroupsStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return """
       SELECT ad_group.url_custom_parameters, 
       ad_group.type, 
       ad_group.tracking_url_template, 
       ad_group.targeting_setting.target_restrictions,
       ad_group.target_roas,
       ad_group.target_cpm_micros,
       ad_group.status,
       ad_group.target_cpa_micros,
       ad_group.resource_name,
       ad_group.percent_cpc_bid_micros,
       ad_group.name,
       ad_group.labels,
       ad_group.id,
       ad_group.final_url_suffix,
       ad_group.explorer_auto_optimizer_setting.opt_in,
       ad_group.excluded_parent_asset_field_types,
       ad_group.effective_target_roas_source,
       ad_group.effective_target_roas,
       ad_group.effective_target_cpa_source,
       ad_group.effective_target_cpa_micros,
       ad_group.display_custom_bid_dimension,
       ad_group.cpv_bid_micros,
       ad_group.cpm_bid_micros,
       ad_group.cpc_bid_micros,
       ad_group.campaign,
       ad_group.base_ad_group,
       ad_group.ad_rotation_mode
       FROM ad_group 
       """

    records_jsonpath = "$.results[*]"
    name = "adgroups"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "ad_group.json"


class AdGroupsPerformance(ReportsStream):
    """AdGroups Performance"""

    gaql = """
        SELECT campaign.id, ad_group.id, metrics.impressions, metrics.clicks,
               metrics.cost_micros
               FROM ad_group
               WHERE segments.date DURING LAST_7_DAYS
        """
    records_jsonpath = "$.results[*]"
    name = "adgroupsperformance"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "adgroups_performance.json"


class CampaignPerformance(ReportsStream):
    """Campaign Performance"""

    gaql = """
    SELECT campaign.name, campaign.status, segments.device, segments.date, metrics.impressions, metrics.clicks, metrics.ctr, metrics.average_cpc, metrics.cost_micros FROM campaign WHERE segments.date DURING LAST_7_DAYS
    """
    records_jsonpath = "$.results[*]"
    name = "campaign_performance"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_performance.json"


class CampaignPerformanceByAgeRangeAndDevice(ReportsStream):
    """Campaign Performance By Age Range and Device"""

    gaql = """
    SELECT ad_group_criterion.age_range.type, campaign.name, campaign.status, ad_group.name, segments.date, segments.device, ad_group_criterion.system_serving_status, ad_group_criterion.bid_modifier, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros, campaign.advertising_channel_type FROM age_range_view WHERE segments.date DURING LAST_7_DAYS
    """
    records_jsonpath = "$.results[*]"
    name = "campaign_performance_by_age_range_and_device"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_performance_by_age_range_and_device.json"


class CampaignPerformanceByGenderAndDevice(ReportsStream):
    """Campaign Performance By Age Range and Device"""

    gaql = """
    SELECT ad_group_criterion.gender.type, campaign.name, campaign.status, ad_group.name, segments.date, segments.device, ad_group_criterion.system_serving_status, ad_group_criterion.bid_modifier, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros, campaign.advertising_channel_type FROM gender_view WHERE segments.date DURING LAST_7_DAYS
    """
    records_jsonpath = "$.results[*]"
    name = "campaign_performance_by_gender_and_device"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_performance_by_gender_and_device.json"


class CampaignPerformanceByLocation(ReportsStream):
    """Campaign Performance By Age Range and Device"""

    gaql = """
    SELECT campaign_criterion.location.geo_target_constant, campaign.name, campaign_criterion.bid_modifier, segments.date, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros FROM location_view WHERE segments.date DURING LAST_7_DAYS AND campaign_criterion.status != 'REMOVED'
    """
    records_jsonpath = "$.results[*]"
    name = "campaign_performance_by_location"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_performance_by_location.json"


class ConversionsByLocation(ReportsStream):
    """Conversions By Location"""

    gaql = """
    SELECT campaign_criterion.location.geo_target_constant, campaign.name, campaign_criterion.bid_modifier, segments.date, segments.conversion_action_category, metrics.conversions FROM location_view WHERE segments.date DURING LAST_7_DAYS AND campaign_criterion.status != 'REMOVED'
    """
    records_jsonpath = "$.results[*]"
    name = "conversion_by_location"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "conversion_by_location.json"


class CampaignLabel(ReportsStream):
    """Conversions By Location"""

    gaql = """
    SELECT campaign_label.campaign, campaign_label.label, campaign_label.resource_name, campaign.id, campaign.name, campaign.resource_name, campaign.status, customer.id, customer.resource_name, label.name, label.resource_name, label.id, label.status 
    FROM campaign_label
    """
    records_jsonpath = "$.results[*]"
    name = "campaign_label"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_label.json"

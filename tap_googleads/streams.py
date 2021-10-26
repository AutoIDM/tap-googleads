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
        return "/customers/"+self.config["customer_id"]
    
    name = "customers"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "customer.json"

class AccessibleCustomers(GoogleAdsStream):
    """Accessible Customers"""
    path="/customers:listAccessibleCustomers"
    name = "accessible_customers"
    primary_keys = None
    replication_key = None
#    schema_filepath = SCHEMAS_DIR / "customer.json"
    schema = th.PropertiesList(
            th.Property("resourceNames", th.ArrayType(th.StringType))
            ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return record

class ReportsStream(GoogleAdsStream):
    rest_method = "POST"
    @property
    def gaql(self):
        raise NotImplementedError
    
    @property
    def path(self):
        #Paramas
        path = "/customers/"+self.config["customer_id"]
        path = path + "/googleAds:search"
        path = path + "?pageSize=10000"
        path = path + f"&query={self.gaql}"
        return path
    
class CustomerHierarchyStream(ReportsStream):
    """Customer Hierarchy, inspiration from Google here 
	https://developers.google.com/google-ads/api/docs/account-management/get-account-hierarchy."""
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
    name = "campaign"
    primary_keys = ["customer_client.id"]
    replication_key = None
    #schema_filepath = SCHEMAS_DIR / "campaign.json"
    schema = th.PropertiesList(
            th.Property("customer_client.client_customer", th.StringType),
            th.Property("customer_client.level", th.StringType),
            th.Property("customer_client.manager", th.StringType),
            th.Property("customer_client.descriptive_name", th.StringType),
            th.Property("customer_client.currency_code", th.StringType),
            th.Property("customer_client.time_zone", th.StringType),
            th.Property("customer_client.id", th.StringType),
            ).to_dict()
    parent_stream_type = AccessibleCustomers
    
    @property
    def path(self):
        #Paramas
        path = "/customers/"+self.config["customer_id"]
        path = path + "/googleAds:search"
        path = path + "?pageSize=10000"
        path = path + f"&query={self.gaql}"
        return path

    #Goal of this stream is to send to children stream a dict of
    #login-customer-id:customer-id to query for all queries downstream
    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for row in self.request_records(context):
            row = self.post_process(row, context)
            yield row

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

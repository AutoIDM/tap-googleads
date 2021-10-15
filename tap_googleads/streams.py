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

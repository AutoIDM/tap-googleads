""" Utilities used in this module """
import singer
from tap_googleads.tap import TapGoogleAds

from singer_sdk.helpers._singer import Catalog
from singer_sdk.helpers import _catalog

accessible_customer_return_data = {
    "resourceNames": ["customers/1234512345", "customers/5432154321"]
}


def set_up_tap_with_custom_catalog(mock_config, stream_list):

    tap = TapGoogleAds(config=mock_config)
    # Run discovery
    tap.run_discovery()
    # Get catalog from tap
    catalog = Catalog.from_dict(tap.catalog_dict)
    # Reset and re-initialize with an input catalog
    _catalog.deselect_all_streams(catalog=catalog)
    for stream in stream_list:
        _catalog.set_catalog_stream_selected(
            catalog=catalog,
            stream_name=stream,
            selected=True,
        )
    # Initialise tap with new catalog
    return TapGoogleAds(config=mock_config, catalog=catalog.to_dict())

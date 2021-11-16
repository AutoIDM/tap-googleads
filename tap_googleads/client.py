"""REST client handling, including GoogleAdsStream base class."""

from pathlib import Path
from typing import Any, Dict, Optional
import requests
import singer

from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import OAuthAuthenticator

from tap_googleads.auth import GoogleAdsAuthenticator, ProxyGoogleAdsAuthenticator


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

LOGGER = singer.get_logger()


class GoogleAdsStream(RESTStream):
    """GoogleAds stream class."""

    url_base = "https://googleads.googleapis.com/v8"

    records_jsonpath = "$[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.nextPageToken"  # Or override `get_next_page_token`.
    _LOG_REQUEST_METRIC_URLS: bool = True

    @property
    @cached
    def authenticator(self) -> OAuthAuthenticator:
        """Return a new authenticator object."""
        base_auth_url = "https://www.googleapis.com/oauth2/v4/token"
        # Silly way to do parameters but it works
        auth_url = (
            base_auth_url + f"?refresh_token={self.config.get('refresh_token', None)}"
        )
        auth_url = auth_url + f"&client_id={self.config.get('client_id')}"
        auth_url = auth_url + f"&client_secret={self.config.get('client_secret')}"
        auth_url = auth_url + "&grant_type=refresh_token"

        oauth_credentials = self.config.get("oauth_credentials", {})

        if not oauth_credentials:
            return GoogleAdsAuthenticator(stream=self, auth_endpoint=auth_url)

        auth_body = {}
        auth_headers = {}

        auth_body["refresh_token"] = oauth_credentials.get("refresh_token")
        auth_body["grant_type"] = "refresh_token"

        auth_headers["authorization"] = oauth_credentials.get("refresh_proxy_url_auth")
        auth_headers["Content-Type"] = "application/json"
        auth_headers["Accept"] = "application/json"

        return ProxyGoogleAdsAuthenticator(
            stream=self,
            auth_endpoint=oauth_credentials.get("refresh_proxy_url"),
            auth_body=auth_body,
            auth_headers=auth_headers,
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        headers["developer-token"] = self.config["developer_token"]
        headers["login-customer-id"] = self.config["login_customer_id"]
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        # TODO: If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        #       pagination loop.
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = None

        return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["pageToken"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

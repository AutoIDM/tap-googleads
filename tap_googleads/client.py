"""REST client handling, including GoogleAdsStream base class."""

from urllib.parse import urlencode, urljoin
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.pagination import JSONPathPaginator

from tap_googleads.auth import GoogleAdsAuthenticator
from tap_googleads.utils import replicate_pk_at_root


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class GoogleAdsStream(RESTStream):
    """GoogleAds stream class."""

    url_base = "https://googleads.googleapis.com/v14"

    records_jsonpath = "$[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.nextPageToken"
    primary_keys_jsonpaths = None
    _LOG_REQUEST_METRIC_URLS: bool = True

    @property
    @cached
    def authenticator(self) -> GoogleAdsAuthenticator:
        """Return a new authenticator object."""
        base_auth_url = "https://www.googleapis.com/oauth2/v4/token"
        auth_params = {
            "refresh_token": self.config["refresh_token"],
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "grant_type": "refresh_token",
        }
        auth_url = urljoin(base_auth_url, "?" + urlencode(auth_params))
        return GoogleAdsAuthenticator(stream=self, auth_endpoint=auth_url)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        headers["developer-token"] = self.config["developer_token"]
        headers["login-customer-id"] = self.config["login_customer_id"]
        return headers

    def get_new_paginator(self) -> JSONPathPaginator:
        return JSONPathPaginator(self.next_page_token_jsonpath)

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

    def validate_response(self, response):
        # Still catch error status codes
        if response.status_code == 403:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for url: {response.url}"
            )
            data = response.json()
            details: dict = data.get("error").get("details")

            if (
                details
                and details[0]["errors"][0]["errorCode"]["authorizationError"]
                == "CUSTOMER_NOT_ENABLED"
            ):
                raise CustomerNotEnabledError(msg)

        if response.status_code == 429:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}."
                f"response.json() {response.json()}:"
            )
            raise RetriableAPIError(msg)

        if 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}."
                f"response.json() {response.json()}:"
            )
            raise FatalAPIError(msg)

        elif 500 <= response.status_code < 600:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise RetriableAPIError(msg)

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        try:
            for record in self.request_records(context):
                transformed_record = self.post_process(record, context)
                if transformed_record is None:
                    # Record filtered out during post_process()
                    continue
                yield transformed_record
        except CustomerNotEnabledError as e:
            self.logger.warning(
                "We hit the Customer Not Enabled error. "
                "Happens when we get a customer from the hierarchy list that "
                "isn't enabled anymore, most likely due to a customer being "
                f"disabled after the API that lists customers is called.  {e=}"
            )

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """As needed, append or transform raw data to match expected structure."""
        return replicate_pk_at_root(row, self.primary_keys_jsonpaths)


class CustomerNotEnabledError(Exception):
    """
    Customer Not Enabled, sometimes googles cache gives us customers that
    are not enabled.
    """

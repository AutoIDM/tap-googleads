"""GoogleAds Authentication."""


import json
from datetime import datetime
from typing import Optional
import requests


from singer import utils
from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from singer_sdk.helpers._util import utc_now
from singer_sdk.streams import Stream as RESTStreamBase


class ProxyGoogleAdsAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """API Authenticator for Proxy OAuth 2.0 flows."""

    def __init__(
        self,
        stream: RESTStreamBase,
        auth_endpoint: Optional[str] = None,
        oauth_scopes: Optional[str] = None,
        auth_headers: Optional[dict] = None,
        auth_body: Optional[dict] = None,
    ) -> None:
        """Create a new authenticator.

        Args:
            stream: The stream instance to use with this authenticator.
            auth_endpoint: API username.
            oauth_scopes: API password.
        """
        super().__init__(stream=stream)
        self._auth_endpoint = auth_endpoint
        self._oauth_scopes = oauth_scopes
        self._auth_headers = auth_headers
        self._auth_body = auth_body

        # Initialize internal tracking attributes
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.last_refreshed: Optional[datetime] = None
        self.expires_in: Optional[int] = None

    def is_token_valid(self) -> bool:
        """Check if token is valid.

        Returns:
            True if the token is valid (fresh).
        """
        if self.last_refreshed is None:
            return False
        if not self.expires_in:
            return True
        if self.expires_in > (utils.now() - self.last_refreshed).total_seconds():
            return True
        return False

    # Authentication and refresh
    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in`.

        Raises:
            RuntimeError: When OAuth login fails.
        """
        request_time = utc_now()

        token_response = requests.post(
            self.auth_endpoint,
            headers=self._auth_headers,
            data=json.dumps(self._auth_body),
        )
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            )
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        self.expires_in = token_json["expires_in"]
        self.last_refreshed = request_time

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the GoogleAds API."""
        return {}


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class GoogleAdsAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for GoogleAds."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the GoogleAds API."""
        return {}

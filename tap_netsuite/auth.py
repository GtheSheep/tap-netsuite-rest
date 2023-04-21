"""Netsuite Authentication."""

from __future__ import annotations

import requests
from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from singer_sdk.helpers._util import utc_now


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class NetsuiteAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for Netsuite."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the Netsuite API.

        Returns:
            A dict with the request body
        """
        return {
            'refresh_token': self.config["refresh_token"],
            'grant_type': 'refresh_token',
        }

    @classmethod
    def create_for_stream(cls, stream) -> "NetsuiteAuthenticator":
        """Instantiate an authenticator for a specific Singer stream.

        Args:
            stream: The Singer stream instance.

        Returns:
            A new authenticator.
        """
        return cls(
            stream=stream,
        )

    # Authentication and refresh
    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in`.

        Raises:
            RuntimeError: When OAuth login fails.
        """
        request_time = utc_now()
        auth_request_payload = self.oauth_request_payload
        token_response = requests.post(
            "https://{account_identifier}.suitetalk.api.netsuite.com/services/rest/auth/oauth2/v1/token".format(account_identifier=self.config["account_identifier"]),
            data=auth_request_payload,
            timeout=60,
            auth=(self.config["client_id"], self.config["client_secret"])
        )
        try:
            token_response.raise_for_status()
        except requests.HTTPError as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}",
            ) from ex

        self.logger.info("OAuth authorization attempt was successful.")

        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        self.expires_in = int(token_json.get("expires_in", self._default_expiration))
        if self.expires_in is None:
            self.logger.debug(
                "No expires_in receied in OAuth response and no "
                "default_expiration set. Token will be treated as if it never "
                "expires.",
            )
        self.last_refreshed = request_time

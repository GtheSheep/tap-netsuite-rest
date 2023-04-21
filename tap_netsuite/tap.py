"""Netsuite tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_netsuite import streams


class TapNetsuite(Tap):
    """Netsuite tap class."""

    name = "tap-netsuite"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            secret=True,
            description="Client ID"
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            secret=True,
            description="App secret key"
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True,
            secret=True,
            description="Refresh token obtained from the OAuth2 user flow"
        ),
        th.Property(
            "account_identifier",
            th.StringType,
            required=True,
            secret=True,
            description="Account identifier, taken from your Netsuite URL",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.NetsuiteStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.CustomersStream(self),
            streams.InventoryItemsStream(self),
            streams.PurchaseOrdersStream(self)
        ]


if __name__ == "__main__":
    TapNetsuite.cli()

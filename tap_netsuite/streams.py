"""Stream type classes for tap-netsuite."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_netsuite.client import NetsuiteStream


class IDStream(NetsuiteStream):
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("links", th.ArrayType(
            th.ObjectType(
                th.Property("rel", th.StringType),
                th.Property("href", th.StringType),
            )
        )),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {"id": record["id"]}


class _IDCustomerStream(IDStream):
    name = "_id_customer"
    path = "/customer"


class CustomersStream(NetsuiteStream):
    name = "customers"
    path = "/customer/{id}"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = _IDCustomerStream
    ignore_parent_replication_keys = True
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("displayName", th.StringType),
    ).to_dict()

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        return {}

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,
    ) -> Any | None:
        """Return a token for identifying next page or None if no more pages.

        Args:
            response: The HTTP ``requests.Response`` object.
            previous_token: The previous page token value.

        Returns:
            The next pagination token.
        """
        return None

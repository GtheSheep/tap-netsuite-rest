"""Stream type classes for tap-netsuite."""

from __future__ import annotations

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
        response_json = response.json()
        next_page_token = None
        if response_json['hasMore']:
            next_page_token = response_json['offset'] + response_json['count']
        return next_page_token

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
        params: dict = {
            "limit": 1000
        }
        if next_page_token:
            params["offset"] = next_page_token
        return params


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
        th.Property("companyName", th.StringType),
        th.Property("dateCreated", th.DateTimeType),
        th.Property("entityId", th.StringType),
        th.Property("isInactive", th.BooleanType),
        th.Property("isPerson", th.BooleanType),
        th.Property("lastModifiedDate", th.DateTimeType),
    ).to_dict()


class _IDInventoryItemStream(IDStream):
    name = "_id_inventory_item"
    path = "/inventoryItem"


class InventoryItemsStream(NetsuiteStream):
    name = "inventory_items"
    path = "/inventoryItem/{id}"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = _IDInventoryItemStream
    ignore_parent_replication_keys = True
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("createdDate", th.DateTimeType),
        th.Property("displayName", th.StringType),
        th.Property("externalId", th.StringType),
        th.Property("internalId", th.IntegerType),
        th.Property("itemType", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("refName", th.StringType),
        )),
    ).to_dict()

"""Stream type classes for tap-netsuite."""

from __future__ import annotations

from typing import Any, Iterable, Optional

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_netsuite.client import NetsuiteStream


links_schema = th.ArrayType(
    th.ObjectType(
        th.Property("rel", th.StringType),
        th.Property("href", th.StringType),
    ),
)


class NetsuiteRESTBaseStream(NetsuiteStream):
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.items[*]"
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("links", links_schema),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "id": record["id"]
        }

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
        if self.replication_key:
            start_date = self.get_starting_timestamp(context)
            start_date_formatted = start_date.strftime("%d/%m/%Y")
            params['q'] = f'{self.replication_key} AFTER "{start_date_formatted}"'
        if next_page_token:
            params["offset"] = next_page_token
        return params

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            context = context or {}
            context["id"] = record["id"]
            substream = self.substream(self._tap)
            for sub_record in substream.request_records(context):
                transformed_record = substream.post_process(sub_record, context)
                if transformed_record is None:
                    # Record filtered out during post_process()
                    continue
                yield transformed_record


customers_schema = th.PropertiesList(
    th.Property("links", links_schema),
    th.Property("addressBook", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("items", th.ArrayType(
            th.ObjectType(
                th.Property("links", links_schema),
                th.Property("addressBookAddress", th.ObjectType(
                    th.Property("links", links_schema),
                    th.Property("addr1", th.StringType),
                    th.Property("addressee", th.StringType),
                    th.Property("addrText", th.StringType),
                    th.Property("city", th.StringType),
                    th.Property("country", th.ObjectType(
                        th.Property("id", th.StringType),
                        th.Property("refName", th.StringType),
                    )),
                    th.Property("override", th.BooleanType),
                    th.Property("zip", th.StringType),
                )),
                th.Property("addressBookAddress_text", th.StringType),
                th.Property("addressId", th.StringType),
                th.Property("defaultBilling", th.BooleanType),
                th.Property("defaultShipping", th.BooleanType),
                th.Property("id", th.IntegerType),
                th.Property("internalId", th.IntegerType),
                th.Property("isResidential", th.BooleanType),
                th.Property("label", th.StringType),
            )
        )),
        th.Property("totalResults", th.IntegerType),
    )),
    th.Property("alcoholRecipientType", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("balance", th.NumberType),
    th.Property("companyName", th.StringType),
    th.Property("contactList", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("count", th.IntegerType),
        th.Property("hasMore", th.BooleanType),
        th.Property("items", th.ArrayType(
            th.ObjectType()
        )),
        th.Property("offset", th.IntegerType),
        th.Property("totalResults", th.IntegerType),
    )),
    th.Property("contactRoles", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("items", th.ArrayType(
            th.ObjectType()
        )),
        th.Property("totalResults", th.IntegerType),
    )),
    th.Property("creditHoldOverride", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("currency", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("currencyList", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("items", th.ArrayType(
            th.ObjectType(
                th.Property("links", links_schema),
                th.Property("balance", th.NumberType),
                th.Property("currency", th.ObjectType(
                    th.Property("links", links_schema),
                    th.Property("id", th.StringType),
                    th.Property("refName", th.StringType),
                )),
                th.Property("depositBalance", th.NumberType),
                th.Property("displaySymbol", th.StringType),
                th.Property("overdueBalance", th.NumberType),
                th.Property("overrideCurrencyFormat", th.BooleanType),
                th.Property("symbolPlacement", th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("refName", th.StringType),
                )),
                th.Property("unbilledOrders", th.NumberType),
            ),
        )),
        th.Property("totalResults", th.IntegerType),
    )),
    th.Property("customForm", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("dateCreated", th.DateTimeType),
    th.Property("defaultAddress", th.StringType),
    th.Property("depositBalance", th.NumberType),
    th.Property("emailPreference", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("emailTransactions", th.BooleanType),
    th.Property("entityId", th.StringType),
    th.Property("entityStatus", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("faxTransactions", th.BooleanType),
    th.Property("giveAccess", th.BooleanType),
    th.Property("groupPricing", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("items", th.ArrayType(
            th.ObjectType()
        )),
        th.Property("totalResults", th.IntegerType),
    )),
    th.Property("id", th.StringType),
    th.Property("isAutogeneratedRepresentingEntity", th.BooleanType),
    th.Property("isBudgetApproved", th.BooleanType),
    th.Property("isInactive", th.BooleanType),
    th.Property("isPerson", th.BooleanType),
    th.Property("itemPricing", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("items", th.ArrayType(
            th.ObjectType()
        )),
        th.Property("totalResults", th.IntegerType),
    )),
    th.Property("lastModifiedDate", th.DateTimeType),
    th.Property("numberFormat", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("overdueBalance", th.NumberType),
    th.Property("printTransactions", th.BooleanType),
    th.Property("receivablesAccount", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("sendEmail", th.BooleanType),
    th.Property("shipComplete", th.BooleanType),
    th.Property("shippingCarrier", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("subsidiary", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("terms", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("unbilledOrders", th.NumberType),
    th.Property("url", th.StringType),
    th.Property("custentity", th.ArrayType(th.ObjectType())),
).to_dict()


class CustomersSubStream(NetsuiteStream):
    name = "_customers"
    path = "/customer/{id}"
    primary_keys = ["id"]
    custom_attribute_prefix = "custentity"
    ignore_parent_replication_keys = True
    schema = customers_schema


class CustomersStream(NetsuiteRESTBaseStream):
    name = "customers"
    path = "/customer"
    primary_keys = ["id"]
    replication_key = "lastModifiedDate"
    substream = CustomersSubStream
    schema = customers_schema


inventory_items_schema = th.PropertiesList(
    th.Property("id", th.StringType),
    th.Property("createdDate", th.DateTimeType),
    th.Property("displayName", th.StringType),
    th.Property("externalId", th.StringType),
    th.Property("internalId", th.IntegerType),
    th.Property("itemType", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("lastModifiedDate", th.DateTimeType),
).to_dict()


class InventoryItemsSubStream(NetsuiteStream):
    name = "_inventory_items"
    path = "/inventoryItem/{id}"
    primary_keys = ["id"]
    custom_attribute_prefix = "custitem"
    ignore_parent_replication_keys = True
    schema = inventory_items_schema


class InventoryItemsStream(NetsuiteRESTBaseStream):
    name = "inventory_items"
    path = "/inventoryItem"
    primary_keys = ["id"]
    custom_attribute_prefix = "custitem"
    replication_key = "lastModifiedDate"
    substream = InventoryItemsSubStream
    schema = inventory_items_schema


purchase_orders_schema = th.PropertiesList(
    th.Property("id", th.StringType),
    th.Property("createdDate", th.DateTimeType),
    th.Property("lastModifiedDate", th.DateTimeType),
    th.Property("memo", th.StringType),
    th.Property("subtotal", th.NumberType),
    th.Property("total", th.NumberType),
    th.Property("tranDate", th.DateTimeType),
    th.Property("tranId", th.StringType),
).to_dict()


class PurchaseOrdersSubStream(NetsuiteStream):
    name = "_purchase_orders"
    path = "/purchaseOrder/{id}"
    primary_keys = ["id"]
    ignore_parent_replication_keys = True
    schema = purchase_orders_schema


class PurchaseOrdersStream(NetsuiteRESTBaseStream):
    name = "purchase_orders"
    path = "/purchaseOrder"
    primary_keys = ["id"]
    replication_key = "lastModifiedDate"
    substream = PurchaseOrdersSubStream
    schema = purchase_orders_schema

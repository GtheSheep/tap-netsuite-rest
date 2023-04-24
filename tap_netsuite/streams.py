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
    th.Property("links", links_schema),
    th.Property("assetAccount", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("atpMethod", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("autoLeadTime", th.BooleanType),
    th.Property("autoPreferredStockLevel", th.BooleanType),
    th.Property("autoReorderPoint", th.BooleanType),
    th.Property("averageCost", th.NumberType),
    th.Property("billExchRateVarianceAcct", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("class", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("cogsAccount", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("costEstimateType", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("costingMethod", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("countryOfManufacture", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("createdDate", th.DateTimeType),
    th.Property("createRevenuePlansOn", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("currency", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("customForm", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("deferredRevenueAccount", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("deferRevRec", th.BooleanType),
    th.Property("directRevenuePosting", th.BooleanType),
    th.Property("displayName", th.StringType),
    th.Property("dontShowPrice", th.BooleanType),
    th.Property("enforceminqtyinternally", th.BooleanType),
    th.Property("excludeFromSiteMap", th.BooleanType),
    th.Property("externalId", th.StringType),
    th.Property("froogleProductFeed", th.BooleanType),
    th.Property("gainLossAccount", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("id", th.StringType),
    th.Property("includeChildren", th.BooleanType),
    th.Property("incomeAccount", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("intercoIncomeAccount", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("internalId", th.IntegerType),
    th.Property("isGCoCompliant", th.BooleanType),
    th.Property("isInactive", th.BooleanType),
    th.Property("isLotItem", th.BooleanType),
    th.Property("isOnline", th.BooleanType),
    th.Property("itemId", th.StringType),
    th.Property("itemType", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("itemVendor", th.ObjectType(
        th.Property("links", links_schema),
    )),
    th.Property("lastModifiedDate", th.DateTimeType),
    th.Property("lastPurchasePrice", th.NumberType),
    th.Property("manufacturer", th.StringType),
    th.Property("matchBillToReceipt", th.BooleanType),
    th.Property("nexTagProductFeed", th.BooleanType),
    th.Property("presentationItem", th.ObjectType(
        th.Property("links", links_schema),
    )),
    th.Property("price", th.ObjectType(
        th.Property("links", links_schema),
    )),
    th.Property("purchaseDescription", th.StringType),
    th.Property("revenueRecognitionRule", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("revRecForecastRule", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("roundUpAsComponent", th.BooleanType),
    th.Property("salesDescription", th.StringType),
    th.Property("seasonalDemand", th.BooleanType),
    th.Property("shoppingProductFeed", th.BooleanType),
    th.Property("shopzillaProductFeed", th.BooleanType),
    th.Property("siteCategory", th.ObjectType(
        th.Property("links", links_schema),
    )),
    th.Property("subsidiary", th.ObjectType(
        th.Property("links", links_schema),
    )),
    th.Property("supplyReplenishmentMethod", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("totalValue", th.NumberType),
    th.Property("trackLandedCost", th.BooleanType),
    th.Property("upcCode", th.StringType),
    th.Property("vendorName", th.StringType),
    th.Property("VSOEDelivered", th.BooleanType),
    th.Property("VSOESopGroup", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("weight", th.NumberType),
    th.Property("weightUnit", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("yahooProductFeed", th.BooleanType),
    th.Property("custitem", th.ArrayType(th.ObjectType())),
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
    replication_key = "lastModifiedDate"
    substream = InventoryItemsSubStream
    schema = inventory_items_schema


purchase_orders_schema = th.PropertiesList(
    th.Property("links", links_schema),
    th.Property("approvalStatus", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("balance", th.NumberType),
    th.Property("billAddress", th.StringType),
    th.Property("billAddressList", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("billingAddress", th.ObjectType(
        th.Property("links", links_schema),
    )),
    th.Property("billingAddress_text", th.StringType),
    th.Property("createdDate", th.DateTimeType),
    th.Property("currency", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("customForm", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("department", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("dueDate", th.DateTimeType),
    th.Property("email", th.StringType),
    th.Property("employee", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("entity", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("exchangeRate", th.NumberType),
    th.Property("expense", th.ObjectType(
        th.Property("links", links_schema),
    )),
    th.Property("externalId", th.StringType),
    th.Property("id", th.StringType),
    th.Property("item", th.ObjectType(
        th.Property("links", links_schema),
    )),
    th.Property("lastModifiedDate", th.DateTimeType),
    th.Property("location", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("memo", th.StringType),
    th.Property("nextApprover", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("orderStatus", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("prevDate", th.DateTimeType),
    th.Property("shipAddress", th.StringType),
    th.Property("shipAddressList", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("shipDate", th.DateTimeType),
    th.Property("shipIsResidential", th.BooleanType),
    th.Property("shipOverride", th.BooleanType),
    th.Property("shippingAddress", th.ObjectType(
        th.Property("links", links_schema),
    )),
    th.Property("shippingAddress_text", th.StringType),
    th.Property("status", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("subsidiary", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("subtotal", th.NumberType),
    th.Property("terms", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("toBeEmailed", th.BooleanType),
    th.Property("toBeFaxed", th.BooleanType),
    th.Property("toBePrinted", th.BooleanType),
    th.Property("total", th.NumberType),
    th.Property("tranDate", th.DateTimeType),
    th.Property("tranId", th.StringType),
    th.Property("unbilledOrders", th.NumberType),
    th.Property("custbody", th.ArrayType(th.ObjectType())),
).to_dict()


class PurchaseOrdersSubStream(NetsuiteStream):
    name = "_purchase_orders"
    path = "/purchaseOrder/{id}"
    primary_keys = ["id"]
    custom_attribute_prefix = "custbody"
    ignore_parent_replication_keys = True
    schema = purchase_orders_schema


class PurchaseOrdersStream(NetsuiteRESTBaseStream):
    name = "purchase_orders"
    path = "/purchaseOrder"
    primary_keys = ["id"]
    replication_key = "lastModifiedDate"
    substream = PurchaseOrdersSubStream
    schema = purchase_orders_schema


sales_orders_schema = th.PropertiesList(
    th.Property("links", links_schema),
    th.Property("billAddress", th.StringType),
    th.Property("billAddressList", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("billingAddress", th.ObjectType(
        th.Property("links", links_schema),
    )),
    th.Property("billingAddress_text", th.StringType),
    th.Property("canBeUnapproved", th.BooleanType),
    th.Property("createdDate", th.DateTimeType),
    th.Property("currency", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("customForm", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("department", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("email", th.StringType),
    th.Property("entity", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("estGrossProfit", th.NumberType),
    th.Property("exchangeRate", th.NumberType),
    th.Property("id", th.StringType),
    th.Property("isCrossSubTransaction", th.BooleanType),
    th.Property("item", th.ObjectType(
        th.Property("links", links_schema),
    )),
    th.Property("lastModifiedDate", th.DateTimeType),
    th.Property("location", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("memo", th.StringType),
    th.Property("needsPick", th.BooleanType),
    th.Property("nextBill", th.DateTimeType),
    th.Property("orderStatus", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("prevDate", th.DateTimeType),
    th.Property("salesEffectiveDate", th.DateTimeType),
    th.Property("shipAddress", th.StringType),
    th.Property("shipAddressList", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("shipComplete", th.BooleanType),
    th.Property("shipDate", th.DateTimeType),
    th.Property("shipIsResidential", th.BooleanType),
    th.Property("shipOverride", th.BooleanType),
    th.Property("shippingAddress", th.ObjectType(
        th.Property("links", links_schema),
    )),
    th.Property("shippingAddress_text", th.StringType),
    th.Property("status", th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("storeOrder", th.StringType),
    th.Property("subsidiary", th.ObjectType(
        th.Property("links", links_schema),
        th.Property("id", th.StringType),
        th.Property("refName", th.StringType),
    )),
    th.Property("subtotal", th.NumberType),
    th.Property("toBeEmailed", th.BooleanType),
    th.Property("toBeFaxed", th.BooleanType),
    th.Property("toBePrinted", th.BooleanType),
    th.Property("total", th.NumberType),
    th.Property("totalCostEstimate", th.NumberType),
    th.Property("tranDate", th.DateTimeType),
    th.Property("tranId", th.StringType),
    th.Property("webStore", th.StringType),
    th.Property("custbody", th.ArrayType(th.ObjectType())),
).to_dict()


class SalesOrdersSubStream(NetsuiteStream):
    name = "_sales_orders"
    path = "/salesOrder/{id}"
    primary_keys = ["id"]
    custom_attribute_prefix = "custbody"
    ignore_parent_replication_keys = True
    schema = sales_orders_schema


class SalesOrdersStream(NetsuiteRESTBaseStream):
    name = "sales_orders"
    path = "/salesOrder"
    primary_keys = ["id"]
    replication_key = "lastModifiedDate"
    substream = SalesOrdersSubStream
    schema = sales_orders_schema

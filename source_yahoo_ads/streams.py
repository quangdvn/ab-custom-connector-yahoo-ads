from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.sources.streams.http import HttpStream

from source_yahoo_ads.api import YAHOO_ADS_SEARCH


class YahooSearchAdsStream(HttpStream, ABC):
  def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
    return None

  def request_params(
      self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
  ) -> MutableMapping[str, Any]:
    return {}


class YahooDisplayAdsStream(HttpStream, ABC):
  """
  TODO remove this comment

  This class represents a stream output by the connector.
  This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
  parsing responses etc..

  Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

  Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
  contains the endpoints
      - GET v1/customers
      - GET v1/employees

  then you should have three classes:
  `class YahooAdsStream(HttpStream, ABC)` which is the current class
  `class Customers(YahooAdsStream)` contains behavior to pull data for customers using v1/customers
  `class Employees(YahooAdsStream)` contains behavior to pull data for employees using v1/employees

  If some streams implement incremental sync, it is typical to create another class
  `class IncrementalYahooAdsStream((YahooAdsStream), ABC)` then have concrete stream implementations extend it. An example
  is provided below.

  See the reference docs for the full list of configurable options.
  """

  # TODO: Fill in the url base. Required.
  url_base = "https://ads-display.yahooapis.jp/api/v10/ReportDefinitionService"

  def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
    """
    TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

    This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
    to most other methods in this class to help you form headers, request bodies, query params, etc..

    For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
    'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
    The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

    :param response: the most recent response from the API
    :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
            If there are no more pages in the result, return None.
    """
    return None

  def request_params(
      self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
  ) -> MutableMapping[str, Any]:
    """
    TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
    Usually contains common params e.g. pagination size etc.
    """
    return {}

  def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
    """
    TODO: Override this method to define how a response is parsed.
    :return an iterable containing each record in the response
    """
    yield {}


class IncrementalYahooSearchAdsStream(YahooSearchAdsStream, ABC):
  """
  TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
        if you do not need to implement incremental sync for any streams, remove this class.
  """

  # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
  state_checkpoint_interval = None

  @property
  def cursor_field(self) -> str:
    """
    TODO
    Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
    usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

    :return str: The name of the cursor field.
    """
    return []

  def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
    """
    Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
    the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
    """
    return {}


class IncrementalYahooDisplayAdsStream(YahooDisplayAdsStream, ABC):
  state_checkpoint_interval = None

  @property
  def cursor_field(self) -> str:
    return []

  def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
    return {}


class YssAd(IncrementalYahooSearchAdsStream):
  url_base = "https://ads-search.yahooapis.jp/api/v10/ReportDefinitionService/"
  http_method = "POST"

  cursor_field = "日"
  primary_key = ["広告ID", "日"]

  def __init__(self, account_id: str, report_job_id: str, ** kwargs):
    super().__init__(**kwargs)
    self.account_id = account_id
    self.report_job_id = report_job_id

  def path(self, **kwargs) -> str:
    return "download"

  def request_body_json(
      self,
      stream_state: Mapping[str, Any],
      stream_slice: Mapping[str, Any] = None,
      next_page_token: Mapping[str, Any] = None,
  ) -> Optional[Mapping]:
    body = {
        "accountId": self.account_id,
        "reportJobId": self.report_job_id
    }
    return body

  def request_headers(self, *args, **kvargs) -> MutableMapping[str, Any]:
    return {"Content-Type": "application/json"}

  # TODO: Parse download response into file
  def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
    print('content', response.content)
    yield {}


#   def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
#     raise NotImplementedError(
#         "Implement stream slices for YssAd or delete this method! ")


class YssAdConversion(IncrementalYahooSearchAdsStream):
  """
  TODO: Change class name to match the table/data source this stream corresponds to.
  """

  # TODO: Fill in the cursor_field. Required.
  cursor_field = "DAY"

  # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
  primary_key = ["AD_ID", "DAY"]

  def path(self, **kwargs) -> str:
    """
    TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
    return "single". Required.
    """
    return "yss_ad"

  def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
    """
    TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

    Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
    This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
    section of the docs for more information.

    The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
    necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
    This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

    An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
    craft that specific request.

    For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
    this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
    till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
    the date query param.
    """
    raise NotImplementedError(
        "Implement stream slices or delete this method!")


class YssKeywords(IncrementalYahooSearchAdsStream):
  """
  TODO: Change class name to match the table/data source this stream corresponds to.
  """

  # TODO: Fill in the cursor_field. Required.
  cursor_field = "DAY"

  # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
  primary_key = ["ADGROUP_ID", "DAY"]

  def path(self, **kwargs) -> str:
    """
    TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
    return "single". Required.
    """
    return "yss_ad"

  def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
    """
    TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

    Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
    This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
    section of the docs for more information.

    The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
    necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
    This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

    An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
    craft that specific request.

    For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
    this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
    till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
    the date query param.
    """
    raise NotImplementedError(
        "Implement stream slices or delete this method!")


class YdnAd(IncrementalYahooDisplayAdsStream):
  """
  TODO: Change class name to match the table/data source this stream corresponds to.
  """

  # TODO: Fill in the cursor_field. Required.
  cursor_field = "DAY"

  # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
  primary_key = ["AD_ID", "DAY"]

  def path(self, **kwargs) -> str:
    """
    TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
    return "single". Required.
    """
    return "yss_ad"

  def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
    """
    TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

    Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
    This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
    section of the docs for more information.

    The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
    necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
    This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

    An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
    craft that specific request.

    For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
    this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
    till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
    the date query param.
    """
    raise NotImplementedError(
        "Implement stream slices or delete this method!")

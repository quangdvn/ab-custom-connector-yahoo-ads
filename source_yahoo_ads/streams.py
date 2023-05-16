import csv
import os
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.sources.streams.http import HttpStream

from source_yahoo_ads.api import YAHOO_ADS_DISPLAY, YAHOO_ADS_SEARCH

from .utils import generate_temp_download


class YahooAdsStream(HttpStream, ABC):
  def __init__(self, account_id: str, report_job_id: str, ** kwargs):
    super().__init__(**kwargs)
    self.account_id = account_id
    self.report_job_id = report_job_id

  def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
    return None

  def request_params(
      self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
  ) -> MutableMapping[str, Any]:
    return {}

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

  def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
    # Use the generator function to iterate over the rows of data
    csv_rows_generator = generate_temp_download(response)

    # Convert the generator to a list
    report_response = list(csv_rows_generator)

    yield from report_response


class YahooSearchAdsStream(YahooAdsStream, ABC):
  url_base = YAHOO_ADS_SEARCH["BASE_URL"]
  http_method = "POST"

  def path(self, **kwargs) -> str:
    return "download"


class YahooDisplayAdsStream(HttpStream, ABC):
  url_base = YAHOO_ADS_DISPLAY["BASE_URL"]
  http_method = "POST"

  def path(self, **kwargs) -> str:
    return "download"


class IncrementalYahooSearchAdsStream(YahooSearchAdsStream, ABC):
  state_checkpoint_interval = None

  @property
  def cursor_field(self) -> str:
    return []

  def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
    return {}


class IncrementalYahooDisplayAdsStream(YahooDisplayAdsStream, ABC):
  state_checkpoint_interval = None

  @property
  def cursor_field(self) -> str:
    return []

  def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
    return {}


class YssAd(IncrementalYahooSearchAdsStream):
  cursor_field = "日"
  primary_key = ["広告ID", "日"]


class YssAdConversion(IncrementalYahooSearchAdsStream):
  cursor_field = "日"
  primary_key = ["広告ID", "日"]


class YssKeywords(IncrementalYahooSearchAdsStream):
  cursor_field = "日"
  primary_key = ["広告グループID", "日"]


class YdnAd(IncrementalYahooDisplayAdsStream):
  cursor_field = "日"
  primary_key = ["広告ID", "日"]

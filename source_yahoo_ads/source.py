import logging
import time
from typing import Any, Iterator, List, Mapping, MutableMapping, Tuple, Union

import requests
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.models import (AirbyteMessage, AirbyteStateMessage,
                                ConfiguredAirbyteCatalog)
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

from source_yahoo_ads.api import YAHOO_ADS_SEARCH, YahooAds
from source_yahoo_ads.streams import YdnAd, YssAd, YssAdConversion, YssKeywords


class AirbyteStopSync(AirbyteTracedException):
  pass


# Hard code the value for now
# TODO: Implement this idea (use `/get` to check reportJobStatus and recursion until report is created)
# https://github.com/yahoojp-marketing/ads-search-api-python-samples/blob/master/report_sample.py#L68
REPORT_PREPARE_TIME = 10

YAHOO_ADS_SEARCH_AD_STREAM = 0
YAHOO_ADS_SEARCH_AD_CONVERSION_STREAM = 1
YAHOO_ADS_SEARCH_KEYWORDS_STREAM = 2
YAHOO_ADS_DISPLAY_AD_STREAM = 3

DESIRED_STREAMS = [
    {'ads_type': 'YSS', 'stream': 'AD'},
    {'ads_type': 'YSS', 'stream': 'AD_CONVERSION'},
    {'ads_type': 'YSS', 'stream': 'KEYWORDS'}
    # {'ads_type': 'YDN', 'stream': 'AD'}
]


class SourceYahooAds(AbstractSource):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.catalog = None
    self.config = None
    self.report_jobs = []

  @staticmethod
  def _get_yahoo_ads_object(config: Mapping[str, Any]) -> YahooAds:
    yahoo_ads = YahooAds(**config)
    yahoo_ads.login()
    return yahoo_ads

  def check_connection(self, logger: AirbyteLogger, config) -> Tuple[bool, any]:
    try:
      yahoo_ads_object = self._get_yahoo_ads_object(config)
      if hasattr(yahoo_ads_object, 'access_token'):
        return True, None
      return False, "Invalid access token"
    except requests.exceptions.HTTPError as error:
      error_data = error.response.json()[0]
      error_code = error_data.get("errorCode")
      if error.response.status_code == requests.codes.FORBIDDEN and error_code == "REQUEST_LIMIT_EXCEEDED":
        logger.warn(
            f"API Call limit is exceeded. Error message: '{error_data.get('message')}'")
        return False, "API Call limit is exceeded"

  def streams(self, config: Mapping[str, Any]) -> List[Stream]:
    yahoo_ads_object = self._get_yahoo_ads_object(config)
    authenticator = TokenAuthenticator(token=yahoo_ads_object.access_token)

    for item in DESIRED_STREAMS:
      report_job = yahoo_ads_object.add_report(
          ads_type=item['ads_type'],
          stream=item['stream'],
          start_date=config['start_date'])
      self.report_jobs.append(report_job)
    stream_args = []
    for report_job in self.report_jobs:
      stream_args.append({
          "authenticator": authenticator,
          "account_id": config["account_id"],
          "report_job_id": report_job['report_job_id']
      })
    # For checking report jobs, delete later
    print('ids', self.report_jobs)

    time.sleep(REPORT_PREPARE_TIME)

    return [
        YssAd(**stream_args[YAHOO_ADS_SEARCH_AD_STREAM]),
        YssAdConversion(
            **stream_args[YAHOO_ADS_SEARCH_AD_CONVERSION_STREAM]),
        YssKeywords(**stream_args[YAHOO_ADS_SEARCH_KEYWORDS_STREAM])
        # YdnAd(**stream_args[YAHOO_ADS_DISPLAY_AD_STREAM])
    ]

  def read(
      self,
      logger: logging.Logger,
      config: Mapping[str, Any],
      catalog: ConfiguredAirbyteCatalog,
      state: Union[List[AirbyteStateMessage], MutableMapping[str, Any]] = None,
  ) -> Iterator[AirbyteMessage]:
    yahoo_ads_object = self._get_yahoo_ads_object(config)
    try:
      yield from super().read(logger, config, catalog, state)
      logger.info(f"Finished syncing {self.name} successfully")
    except AirbyteStopSync:
      logger.info(f"Finished syncing {self.name} with error")
    finally:
      for report_job in self.report_jobs:
        yahoo_ads_object.remove_report(
            ads_type=report_job['ads_type'],
            report_job_id=report_job['report_job_id']
        )
      logger.info(f"Removed reports successfully after syncing")

from typing import Any, List, Mapping, Tuple

import requests
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

from source_yahoo_ads.api import YAHOO_ADS_SEARCH, YahooAds
from source_yahoo_ads.streams import YdnAd, YssAd, YssAdConversion, YssKeywords


class SourceYahooAds(AbstractSource):
  @staticmethod
  def _get_yahoo_object(self, config: Mapping[str, Any]) -> YahooAds:
    yahoo_ads = YahooAds(**config)
    yahoo_ads.login()
    return yahoo_ads

  def check_connection(self, logger: AirbyteLogger, config) -> Tuple[bool, any]:
    try:
      yahoo_object = self._get_yahoo_object(config)
      if hasattr(yahoo_object, 'access_token'):
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
    # TODO remove the authenticator if not required.
    # Oauth2Authenticator is also available if you need oauth support
    # auth = TokenAuthenticator(token="api_key")
    return [YssAd()]
    # return [YssAd(), YssAdConversion(), YssKeywords(), YdnAd()]

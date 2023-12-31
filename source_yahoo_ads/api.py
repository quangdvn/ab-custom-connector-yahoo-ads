# import concurrent.futures
import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, List, Mapping, Optional, Tuple

import requests  # type: ignore[import]
from airbyte_cdk.models import ConfiguredAirbyteCatalog
from requests import adapters as request_adapters
# type: ignore[import]
from requests.exceptions import HTTPError, RequestException

from .exceptions import TypeYahooAdsException
from .rate_limiting import default_backoff_handler

YAHOO_ADS_DISPLAY = {
    'BASE_URL': "https://ads-display.yahooapis.jp/api/v10/ReportDefinitionService/",
    'AD': [
        {'request_name': "ACCOUNT_ID", 'api_name': "アカウントID"},
        {'request_name': "ACCOUNT_NAME", 'api_name': "アカウント名"},
        {'request_name': "DAY", 'api_name': "日"},
        {'request_name': "DEVICE", 'api_name': "デバイス"},
        {'request_name': "CAMPAIGN_ID", 'api_name': "キャンペーンID"},
        {'request_name': "CAMPAIGN_NAME", 'api_name': "キャンペーン名"},
        {'request_name': "ADGROUP_ID", 'api_name': "広告グループID"},
        {'request_name': "ADGROUP_NAME", 'api_name': "広告グループ名"},
        {'request_name': "AD_ID", 'api_name': "広告ID"},
        {'request_name': "AD_NAME", 'api_name': "広告名"},
        {'request_name': "SEARCHKEYWORD_ID", 'api_name': "サーチキーワードID"},
        {'request_name': "SEARCHKEYWORD", 'api_name': "サーチキーワード"},
        {'request_name': "COST", 'api_name': "コスト"},
        {'request_name': "IMPS", 'api_name': "インプレッション数"},
        {'request_name': "VIEWABLE_IMPS", 'api_name': "ビューアブルインプレッション数"},
        {'request_name': "CLICK", 'api_name': "クリック数"},
        {'request_name': "CLICK_RATE", 'api_name': "クリック率"},
        {'request_name': "CONVERSIONS", 'api_name': "コンバージョン数"},
        {'request_name': "CONV_RATE", 'api_name': "コンバージョン率"},
        {'request_name': "AVG_CPC", 'api_name': "平均CPC"},
        {'request_name': "AVG_CPM", 'api_name': "平均CPM"},
        {'request_name': "AVG_DELIVER_RANK", 'api_name': "平均掲載順位"},
    ]
}

YAHOO_ADS_SEARCH = {
    'BASE_URL': "https://ads-search.yahooapis.jp/api/v10/ReportDefinitionService/",
    'AD': [
        {'request_name': "ACCOUNT_ID", 'api_name': "アカウントID"},
        {'request_name': "ACCOUNT_NAME", 'api_name': "アカウント名"},
        {'request_name': "DAY", 'api_name': "日"},
        {'request_name': "DEVICE", 'api_name': "デバイス"},
        {'request_name': "CAMPAIGN_ID", 'api_name': "キャンペーンID"},
        {'request_name': "CAMPAIGN_NAME", 'api_name': "キャンペーン名"},
        {'request_name': "ADGROUP_ID", 'api_name': "広告グループID"},
        {'request_name': "ADGROUP_NAME", 'api_name': "広告グループ名"},
        {'request_name': "AD_ID", 'api_name': "広告ID"},
        {'request_name': "AD_NAME", 'api_name': "広告名"},
        {'request_name': "COST", 'api_name': "コスト"},
        {'request_name': "IMPS", 'api_name': "インプレッション数"},
        {'request_name': "CLICKS", 'api_name': "クリック数"},
        {'request_name': "CLICK_RATE", 'api_name': "クリック率"},
        {'request_name': "AVG_CPC", 'api_name': "平均CPC"},
        {'request_name': "CONVERSIONS", 'api_name': "コンバージョン数"},
        {'request_name': "CONV_RATE", 'api_name': "コンバージョン率"},
    ],
    "AD_CONVERSION": [
        {'request_name': "ACCOUNT_ID", 'api_name': "アカウントID"},
        {'request_name': "ACCOUNT_NAME", 'api_name': "アカウント名"},
        {'request_name': "DAY", 'api_name': "日"},
        {'request_name': "DEVICE", 'api_name': "デバイス"},
        {'request_name': "CAMPAIGN_ID", 'api_name': "キャンペーンID"},
        {'request_name': "ADGROUP_ID", 'api_name': "広告グループID"},
        {'request_name': "AD_ID", 'api_name': "広告ID"},
        {'request_name': "CONVERSION_NAME", 'api_name': "コンバージョン名"},
        {'request_name': "CONVERSIONS", 'api_name': "コンバージョン数"},
    ],
    "KEYWORDS": [
        {'request_name': "ACCOUNT_ID", 'api_name': "アカウントID"},
        {'request_name': "ACCOUNT_NAME", 'api_name': "アカウント名"},
        {'request_name': "DAY", 'api_name': "日"},
        {'request_name': "DEVICE", 'api_name': "デバイス"},
        {'request_name': "CAMPAIGN_ID", 'api_name': "キャンペーンID"},
        {'request_name': "CAMPAIGN_NAME", 'api_name': "キャンペーン名"},
        {'request_name': "ADGROUP_ID", 'api_name': "広告グループID"},
        {'request_name': "ADGROUP_NAME", 'api_name': "広告グループ名"},
        {'request_name': "KEYWORD_ID", 'api_name': "キーワードID"},
        {'request_name': "KEYWORD", 'api_name': "キーワード"},
        {'request_name': "COST", 'api_name': "コスト"},
        {'request_name': "IMPS", 'api_name': "インプレッション数"},
        {'request_name': "CLICKS", 'api_name': "クリック数"},
        {'request_name': "CLICK_RATE", 'api_name': "クリック率"},
        {'request_name': "AVG_CPC", 'api_name': "平均CPC"},
        {'request_name': "CONVERSIONS", 'api_name': "コンバージョン数"},
        {'request_name': "CONV_RATE", 'api_name': "コンバージョン率"}
    ]
}

# https://github.com/yahoojp-marketing/ads-search-api-python-samples/blob/master/report_sample.py#L68


class YahooAds:
  logger = logging.getLogger("airbyte")
  parallel_tasks_size = 100
  REPORT_PREPARE_TIME = 5

  def __init__(
      self,
      refresh_token: str = None,
      client_id: str = None,
      client_secret: str = None,
      sync_option: dict[str, str] = None,
      start_date: str = None,
      **kwargs: Any,
  ) -> None:
    self.refresh_token = refresh_token
    self.client_id = client_id
    self.client_secret = client_secret
    # self.sync_option = sync_option
    self.yss_account_id = sync_option.get('yss_account_id', None)
    self.ydn_account_id = sync_option.get('ydn_account_id', None)
    self.start_date = start_date
    self.access_token = None

    self.session = requests.Session()
    # Change the connection pool size. Default value is not enough for parallel tasks
    adapter = request_adapters.HTTPAdapter(
        pool_connections=self.parallel_tasks_size, pool_maxsize=self.parallel_tasks_size)
    self.session.mount("https://", adapter)

  def login(self):
    login_url = f"https://biz-oauth.yahoo.co.jp/oauth/v1/token"
    login_body = {
        "grant_type": "refresh_token",
        "client_id": self.client_id,
        "client_secret": self.client_secret,
        "refresh_token": self.refresh_token,
    }
    resp = self._make_request(http_method="POST",
                              url=login_url,
                              body=login_body,
                              headers={
                                  "Content-Type": "application/x-www-form-urlencoded"
                              }
                              )

    auth = resp.json()
    self.access_token = auth["access_token"]

  def add_report(self, ads_type: str, stream: str, start_date: str) -> dict[str, str]:
    end_date = (datetime.today() + timedelta(hours=9) +
                timedelta(days=-1)).strftime('%Y%m%d')
    account_id = self.yss_account_id if ads_type == "YSS" else self.ydn_account_id
    add_config = {
        "accountId": account_id,
        "operand": [
            {
                "dateRange": {
                    "startDate": start_date,
                    "endDate": end_date
                },
                "fields": self._extract_report_fields(ads_type, stream),
                "reportDateRangeType": "CUSTOM_DATE",
                "reportDownloadEncode": "UTF8",
                "reportDownloadFormat": "CSV",
                "reportLanguage": "JA",
                "reportName": f"YahooReport_{ads_type}_{stream}_{end_date}_{uuid.uuid4()}",
                "reportSkipReportSummary": "TRUE"
            }
        ]
    }

    if ads_type == 'YDN':
      add_url = f"{YAHOO_ADS_DISPLAY['BASE_URL']}add"
      get_url = f"{YAHOO_ADS_DISPLAY['BASE_URL']}get"

    elif ads_type == 'YSS':
      add_url = f"{YAHOO_ADS_SEARCH['BASE_URL']}add"
      get_url = f"{YAHOO_ADS_SEARCH['BASE_URL']}get"
      add_config["operand"][0]["reportType"] = stream if stream == "KEYWORDS" else "AD"

    headers = self._get_standard_headers()

    add_report_resp = self._make_request(
        http_method='POST',
        url=add_url,
        body=json.dumps(add_config),
        headers=headers).json()

    if not add_report_resp['rval']['values'][0]['operationSucceeded']:
      error = add_report_resp['rval']['values'][0]['errors']
      raise Exception(f'InvalidEnumError: {json.dumps(error)}')

    get_config = {
        "accountId": account_id,
        "reportJobIds": [
            str(add_report_resp['rval']['values'][0]
                ['reportDefinition']['reportJobId'])
        ]
    }

    sleep_duration = self.REPORT_PREPARE_TIME
    while True:
      get_report_resp = self._make_request(
          http_method='POST',
          url=get_url,
          body=json.dumps(get_config),
          headers=headers).json()
      report_job_status = str(
          get_report_resp['rval']['values'][0]['reportDefinition']['reportJobStatus'])

      # Break the loop after finishing create the report regardless of completed or failed
      # WAIT -- Please wait for report request to complete.
      # COMPLETED -- Report request completed successfully.
      # IN_PROGRESS -- Report is in creating process.
      # FAILED -- Report request failed.
      # UNKNOWN -- Unknown Value
      if report_job_status not in ['WAIT', 'IN_PROGRESS']:
        break
      time.sleep(sleep_duration)
      # Double the prepare time for the next iteration to reduce hit to Yahoo server
      sleep_duration *= 2

    return {
        'ads_type': ads_type,
        'stream': stream,
        'account_id': account_id,
        'report_job_id': str(get_report_resp['rval']['values'][0]['reportDefinition']['reportJobId']),
        'report_job_status': str(get_report_resp['rval']['values'][0]['reportDefinition']['reportJobStatus']),
    }

  def remove_report(self, ads_type: str, report_job_id: str) -> bool:
    if ads_type == 'YDN':
      remove_url = f"{YAHOO_ADS_DISPLAY['BASE_URL']}remove"
    elif ads_type == 'YSS':
      remove_url = f"{YAHOO_ADS_SEARCH['BASE_URL']}remove"
    remove_config = {
        "accountId": self.yss_account_id if ads_type == "YSS" else self.ydn_account_id,
        "operand": [
            {
                "reportJobId": report_job_id
            }
        ]
    }
    headers = self._get_standard_headers()

    resp = self._make_request(
        http_method='POST',
        url=remove_url,
        body=json.dumps(remove_config),
        headers=headers).json()
    if not resp['rval']['values'][0]['operationSucceeded']:
      error = resp['rval']['values'][0]['errors']
      raise Exception(f'InvalidEnumError: {json.dumps(error)}')
    return resp['rval']['values'][0]['operationSucceeded']

  @default_backoff_handler(max_tries=5, factor=5)
  def _make_request(
      self,
      http_method: str,
      url: str,
      headers: dict = None,
      body: dict = None,
      stream: bool = False,
      params: dict = None
  ) -> requests.models.Response:
    try:
      if http_method == "GET":
        resp = self.session.get(
            url, headers=headers, stream=stream, params=params)
      elif http_method == "POST":
        resp = self.session.post(url, headers=headers, data=body)
      resp.raise_for_status()
    except HTTPError as err:
      self.logger.warn(f"http error body: {err.response.text}")
      raise
    return resp

  def _extract_report_fields(self, ads_type: str, stream: str):
    if ads_type == 'YDN':
      return [item['request_name'] for item in YAHOO_ADS_DISPLAY[stream]]
    elif ads_type == 'YSS':
      return [item['request_name'] for item in YAHOO_ADS_SEARCH[stream]]

  def _get_standard_headers(self) -> Mapping[str, str]:
    return {
        "Content-Type": "application/json",
        "Authorization": "Bearer {}".format(self.access_token)
    }

import csv
import io

import requests


def generate_temp_download(response: requests.models.Response):
  # Create a buffer for the response data
  buffer = io.BytesIO()

  # Write the response data to the buffer
  for chunk in response.iter_content(chunk_size=None):
    buffer.write(chunk)

  # Reset the buffer position to the beginning
  buffer.seek(0)

  # Parse the CSV data using csv.DictReader()
  reader = csv.DictReader(io.TextIOWrapper(buffer, encoding='utf-8'),
                          delimiter=',',
                          quotechar='"',
                          quoting=csv.QUOTE_MINIMAL,
                          skipinitialspace=True,
                          escapechar='\\',
                          doublequote=True,
                          strict=True)

  # Yield each row from the CSV data
  for row in reader:
    yield row

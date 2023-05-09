import os
import secrets
import tempfile
from datetime import date

import requests


def generate_temp_download(response: requests.models.Response) -> str:
  path = os.path.join(tempfile.gettempdir(), f"download-{date.today()}")
  os.makedirs(path, exist_ok=True)
  file_path = os.path.join(path, f"{secrets.token_hex(10)}.bin")
  with open(file_path, "wb") as f:
    for chunk in response.iter_content(chunk_size=None):
      f.write(chunk)

  return file_path


def skip_last_line(iterator):
  prev = next(iterator)
  for item in iterator:
    yield prev
    prev = item

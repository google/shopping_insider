# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# python3
"""Config parser module.

This module retrieves config values.
"""

import functools
import yaml


@functools.lru_cache()
def _get_config(config_key: str) -> str:
  """Returns value for a given config key.

  The config values are retrieved from "config.yaml" file for the
  first invocation of config key. The subsequent invocation returns
  the value from cache.

  Args:
    config_key: The key to retrieve a value for.

  Returns:
    Value for key from config file.
  """

  with open('config.yaml') as config_file:
    configs = yaml.safe_load(config_file)
    return configs[config_key]


def get_dataset_location() -> str:
  """Returns the dataset location."""
  return _get_config('LOCATION')

def get_market_insights_locale() -> str:
  """Returns the locale settings for Market Insights."""
  return _get_config('MARKET_INSIGHTS_LOCALE')
